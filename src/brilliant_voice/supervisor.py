"""Process supervisor for the on-panel voice agent.

Starts and supervises child processes (LVA, and optionally an AEC daemon),
restarting any that exit after a backoff delay.

All constants are on-panel paths; this module must not import any heavy or
panel-only libraries at module scope — pure stdlib only, as with the rest of
``brilliant_voice``.

On-panel paths
--------------
VOICE_ROOT  = /var/brilliant-voice
BUNDLED_PY  = bundled py3.11 (runs LVA)
PANEL_PY    = panel py3.10 (runs AEC daemon; needs audio_dsp)
SITE        = LVA's py3.11 dependencies
LIBS        = vendored native libs (openblas, libstdc++)
LVA_DIR     = forked linux_voice_assistant package directory
AEC_SCRIPT  = aec_daemon.py script
RUN_DIR     = FIFO directory on tmpfs (provided by RuntimeDirectory; daemon makedirs defensively)

Mid-run restart note
--------------------
On a child-by-child restart the FIFO dependency ordering is not re-coordinated:
each child restarts independently. The AEC daemon opens its FIFOs O_RDWR, so a
brief LVA-ahead race self-heals on LVA's next restart. Pilot validation will
confirm.
"""

from __future__ import annotations

import logging
import os
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from subprocess import TimeoutExpired
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from brilliant_voice.config import VoiceSettings

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# On-panel path constants
# ---------------------------------------------------------------------------

VOICE_ROOT = "/var/brilliant-voice"
BUNDLED_PY = VOICE_ROOT + "/python/bin/python3"
PANEL_PY = "/data/switch-embedded/env/bin/python3"
SITE = VOICE_ROOT + "/site"
LIBS = VOICE_ROOT + "/libs"
LVA_DIR = VOICE_ROOT + "/lva"
AEC_SCRIPT = VOICE_ROOT + "/aec/aec_daemon.py"
RUN_DIR = "/run/brilliant-voice"
REF_FIFO = RUN_DIR + "/ref.fifo"
CLEAN_FIFO = RUN_DIR + "/clean.fifo"

_INTER_START_DELAY_S = 0.5  # give AEC daemon time to create FIFOs before LVA
_BACKOFF_S = 5.0


# ---------------------------------------------------------------------------
# ChildSpec (pure data; the testable core)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ChildSpec:
    """Immutable description of a supervised child process."""

    name: str  # "lva" | "aec"
    argv: list[str]
    env: dict[str, str]  # OVERRIDES merged over os.environ by the spawner


# ---------------------------------------------------------------------------
# Pure child-spec builder
# ---------------------------------------------------------------------------


def child_specs(settings: VoiceSettings) -> list[ChildSpec]:
    """Return the ordered list of child specs for the given voice settings.

    AEC comes first when enabled so it can create the FIFOs that LVA reads.

    Parameters
    ----------
    settings:
        A ``VoiceSettings`` instance.
    """
    enable_aec: bool = settings.enable_aec
    mic_device: str = settings.mic_device
    snd_device: str = settings.snd_device
    wake_word: str = settings.wake_word
    name: str = settings.name
    aec_mic_device: str = settings.aec_mic_device
    aec_delay_ms: int = settings.aec_delay_ms
    aec_type: int = settings.aec_type

    specs: list[ChildSpec] = []

    # AEC daemon (optional, must start BEFORE LVA)
    if enable_aec:
        aec_argv = [
            PANEL_PY,
            AEC_SCRIPT,
            "--mic-device",
            aec_mic_device,
            "--ref-fifo",
            REF_FIFO,
            "--clean-fifo",
            CLEAN_FIFO,
            "--aec-delay-ms",
            str(aec_delay_ms),
            "--aec-type",
            str(aec_type),
        ]
        specs.append(ChildSpec(name="aec", argv=aec_argv, env={}))

    # LVA (linux-voice-assistant) — always present
    lva_argv = [
        BUNDLED_PY,
        "-m",
        "linux_voice_assistant",
        "--name",
        name,
        "--audio-input-device",
        mic_device,
        "--audio-input-channels",
        "1",
        "--audio-output-device",
        snd_device,
        "--wake-model",
        wake_word,
    ]
    if enable_aec:
        lva_argv += ["--stream-input-device", f"fifo:{CLEAN_FIFO}"]

    lva_env: dict[str, str] = {
        "LD_LIBRARY_PATH": LIBS,
        "PYTHONPATH": f"{LVA_DIR}:{SITE}",
        "LVA_INSTANT_WAKE": "1",
    }
    if enable_aec:
        lva_env["LVA_REF_FIFO"] = REF_FIFO

    specs.append(ChildSpec(name="lva", argv=lva_argv, env=lva_env))
    return specs


# ---------------------------------------------------------------------------
# Proc Protocol + Spawn type
# ---------------------------------------------------------------------------


class Proc(Protocol):
    """Minimal subprocess.Popen-like surface required by the supervisor."""

    def poll(self) -> int | None: ...

    def wait(self, timeout: float | None = None) -> int: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...


Spawn = Callable[[ChildSpec], Proc]


def _default_spawn(spec: ChildSpec) -> Proc:
    """Start a child process using subprocess.Popen.

    Env overrides are merged over the current environment.
    """
    merged_env = {**os.environ, **spec.env}
    return subprocess.Popen(spec.argv, env=merged_env)


# ---------------------------------------------------------------------------
# Supervisor loop
# ---------------------------------------------------------------------------

#: A callable that returns the current monotonic time (injectable for tests).
Clock = Callable[[], float]
#: A callable that sleeps for the given number of seconds (injectable for tests).
Sleep = Callable[[float], None]
#: A no-argument callable that returns True while the supervisor should keep running.
KeepRunning = Callable[[], bool]


def supervise(
    specs: list[ChildSpec],
    *,
    spawn: Spawn = _default_spawn,
    clock: Clock = time.monotonic,
    sleep: Sleep = time.sleep,
    keep_running: KeepRunning = lambda: True,
    inter_start_delay: float = _INTER_START_DELAY_S,
    backoff: float = _BACKOFF_S,
) -> None:
    """Supervise child processes, restarting any that exit after a backoff.

    Parameters
    ----------
    specs:
        Ordered list of child specs to launch.  AEC must precede LVA when AEC
        is enabled (``child_specs()`` guarantees this).
    spawn:
        Factory that starts a child and returns a ``Proc``.  Injected for tests.
    clock:
        Returns the current monotonic time.  Injected for tests.
    sleep:
        Sleeps for the given number of seconds.  Injected for tests.
    keep_running:
        Predicate called each loop iteration; returns ``False`` to stop.
    inter_start_delay:
        Seconds to wait between starting successive children on initial launch
        so the AEC daemon can create the FIFOs before LVA tries to open them.
    backoff:
        Seconds to wait before restarting a child that exited.
    """
    if not specs:
        log.warning("supervise called with empty spec list — nothing to run")
        return

    # ``running`` is index-aligned with ``specs``: running[idx] is the live Proc
    # for specs[idx].  A restart reuses specs[idx] unchanged.  The whole body is
    # wrapped so the teardown ALWAYS runs — including when a spawn() raises during
    # the initial start or a restart, which must not orphan already-running
    # children (the AEC daemon holding the mic, LVA holding the API port).
    running: list[Proc] = []
    try:
        # Initial start: launch all children in order with inter-start delays.
        for i, spec in enumerate(specs):
            log.info("starting child %s: %s", spec.name, spec.argv[0])
            running.append(spawn(spec))
            if i < len(specs) - 1:
                sleep(inter_start_delay)

        # restart_after: keyed by list index; value is the monotonic time after
        # which the child may be restarted. Set when the child first exits.
        restart_after: dict[int, float] = {}

        while keep_running():
            sleep(0.1)  # poll cadence — injected sleep makes tests instant

            now = clock()

            for idx, proc in enumerate(running):
                rc = proc.poll()
                if rc is None:
                    # Child is still running.
                    continue

                # Child has exited.
                if idx not in restart_after:
                    log.warning(
                        "child %s exited with code %s; restarting after %.1fs backoff",
                        specs[idx].name,
                        rc,
                        backoff,
                    )
                    restart_after[idx] = now + backoff

                if now >= restart_after[idx]:
                    log.info("restarting child %s", specs[idx].name)
                    running[idx] = spawn(specs[idx])
                    del restart_after[idx]

    except KeyboardInterrupt:
        log.info("KeyboardInterrupt received — stopping supervisor")
    finally:
        _teardown(running, specs)

    log.info("supervisor exited cleanly")


def _teardown(running: list[Proc], specs: list[ChildSpec]) -> None:
    """Terminate→wait→SIGKILL every started child, swallowing per-child errors.

    Always runs (clean stop, KeyboardInterrupt, OR a spawn failure) so a partial
    start never leaves a child orphaned.  ``running`` is index-aligned with
    ``specs``; only children started so far are present.
    """
    log.info("stopping; terminating %d child(ren)", len(running))
    for idx, proc in enumerate(running):
        try:
            proc.terminate()
        except Exception:
            log.exception("error terminating child %s", specs[idx].name)

    # Best-effort wait for all children to exit cleanly; SIGKILL if they linger.
    for idx, proc in enumerate(running):
        try:
            proc.wait(timeout=5.0)
        except TimeoutExpired:
            log.warning("child %s did not exit after 5 s — sending SIGKILL", specs[idx].name)
            try:
                proc.kill()
                proc.wait()
            except Exception:
                log.exception("error killing child %s", specs[idx].name)
        except Exception:
            log.exception("error waiting for child %s to exit", specs[idx].name)
