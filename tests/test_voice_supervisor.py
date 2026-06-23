"""Tests for the voice-agent supervisor: child_specs and supervise loop.

All tests run off-panel — no real subprocesses, no real clocks.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from subprocess import TimeoutExpired

import pytest

from brilliant_voice.config import VoiceSettings
from brilliant_voice.supervisor import (
    AEC_SCRIPT,
    BUNDLED_PY,
    CLEAN_FIFO,
    LIBS,
    LVA_DIR,
    PANEL_PY,
    REF_FIFO,
    SITE,
    ChildSpec,
    child_specs,
    supervise,
)

# ---------------------------------------------------------------------------
# Helpers: fake settings
# ---------------------------------------------------------------------------


def _settings(
    *,
    enable_aec: bool = False,
    name: str = "Brilliant office",
    mic_device: str = "default",
    snd_device: str = "plug:dmix_48000",
    wake_word: str = "okay_nabu",
    aec_mic_device: str = "plug:dsnoop_48000",
    aec_delay_ms: int = 0,
    aec_type: int = 1,
) -> VoiceSettings:
    """Return a VoiceSettings with test defaults, overriding only the fields the
    supervisor reads. Every parameter is explicitly typed so construction is
    well-typed (no ``# type: ignore``)."""
    return VoiceSettings(
        panel="office",
        name=name,
        api_port=6053,
        wake_word=wake_word,
        mic_device=mic_device,
        snd_device=snd_device,
        enable_aec=enable_aec,
        aec_mic_device=aec_mic_device,
        aec_delay_ms=aec_delay_ms,
        aec_type=aec_type,
        ha_host="",
        log_level="INFO",
    )


# ---------------------------------------------------------------------------
# child_specs: AEC disabled (default)
# ---------------------------------------------------------------------------


class TestChildSpecsNoAec:
    def test_returns_exactly_one_lva_spec(self) -> None:
        specs = child_specs(_settings(enable_aec=False))
        assert len(specs) == 1
        assert specs[0].name == "lva"

    def test_lva_argv_structure(self) -> None:
        s = _settings(
            enable_aec=False,
            name="Test Panel",
            mic_device="hw:1,0",
            snd_device="hw:1,1",
            wake_word="hey_jarvis",
        )
        lva = child_specs(s)[0]
        assert lva.argv == [
            BUNDLED_PY,
            "-m",
            "linux_voice_assistant",
            "--name",
            "Test Panel",
            "--audio-input-device",
            "hw:1,0",
            "--audio-input-channels",
            "1",
            "--audio-output-device",
            "hw:1,1",
            "--wake-model",
            "hey_jarvis",
        ]

    def test_lva_argv_no_stream_input_device(self) -> None:
        lva = child_specs(_settings(enable_aec=False))[0]
        assert "--stream-input-device" not in lva.argv

    def test_lva_env_no_ref_fifo(self) -> None:
        lva = child_specs(_settings(enable_aec=False))[0]
        assert "LVA_REF_FIFO" not in lva.env

    def test_lva_env_has_required_keys(self) -> None:
        lva = child_specs(_settings(enable_aec=False))[0]
        assert lva.env == {
            "LD_LIBRARY_PATH": LIBS,
            "PYTHONPATH": f"{LVA_DIR}:{SITE}",
            "LVA_INSTANT_WAKE": "1",
        }


# ---------------------------------------------------------------------------
# child_specs: AEC enabled
# ---------------------------------------------------------------------------


class TestChildSpecsWithAec:
    def test_returns_two_specs_in_order(self) -> None:
        specs = child_specs(_settings(enable_aec=True))
        assert len(specs) == 2
        assert specs[0].name == "aec"
        assert specs[1].name == "lva"

    def test_aec_argv(self) -> None:
        s = _settings(
            enable_aec=True,
            aec_mic_device="hw:2,0",
            aec_delay_ms=150,
            aec_type=2,
        )
        aec = child_specs(s)[0]
        assert aec.argv == [
            PANEL_PY,
            AEC_SCRIPT,
            "--mic-device",
            "hw:2,0",
            "--ref-fifo",
            REF_FIFO,
            "--clean-fifo",
            CLEAN_FIFO,
            "--aec-delay-ms",
            "150",
            "--aec-type",
            "2",
        ]

    def test_aec_env_is_empty(self) -> None:
        aec = child_specs(_settings(enable_aec=True))[0]
        assert aec.env == {}

    def test_lva_argv_has_stream_input_device(self) -> None:
        lva = child_specs(_settings(enable_aec=True))[1]
        idx = lva.argv.index("--stream-input-device")
        assert lva.argv[idx + 1] == f"fifo:{CLEAN_FIFO}"

    def test_lva_env_has_ref_fifo(self) -> None:
        lva = child_specs(_settings(enable_aec=True))[1]
        assert lva.env.get("LVA_REF_FIFO") == REF_FIFO

    def test_lva_env_complete(self) -> None:
        lva = child_specs(_settings(enable_aec=True))[1]
        assert lva.env == {
            "LD_LIBRARY_PATH": LIBS,
            "PYTHONPATH": f"{LVA_DIR}:{SITE}",
            "LVA_INSTANT_WAKE": "1",
            "LVA_REF_FIFO": REF_FIFO,
        }

    def test_aec_type_default_serialised_as_string(self) -> None:
        # aec_type=1 (default Speex) must be passed as "1" on the argv.
        aec = child_specs(_settings(enable_aec=True, aec_type=1))[0]
        idx = aec.argv.index("--aec-type")
        assert aec.argv[idx + 1] == "1"


# ---------------------------------------------------------------------------
# Fake subprocess helpers for the supervise loop tests
# ---------------------------------------------------------------------------


@dataclass
class FakeProc:
    """A fake Proc whose exit can be controlled from the test."""

    name: str
    _poll_result: int | None = None
    terminate_calls: int = field(default=0, repr=False)
    wait_calls: int = field(default=0, repr=False)
    kill_calls: int = field(default=0, repr=False)
    # When True, the first wait() call raises TimeoutExpired; subsequent calls succeed.
    wait_raises_timeout: bool = field(default=False, repr=False)

    def poll(self) -> int | None:
        return self._poll_result

    def wait(self, timeout: float | None = None) -> int:
        self.wait_calls += 1
        if self.wait_raises_timeout and self.wait_calls == 1:
            raise TimeoutExpired(cmd="fake", timeout=timeout or 5.0)
        return self._poll_result if self._poll_result is not None else 0

    def terminate(self) -> None:
        self.terminate_calls += 1

    def kill(self) -> None:
        self.kill_calls += 1

    def make_exit(self, code: int = 0) -> None:
        """Signal that this process has exited."""
        self._poll_result = code


@dataclass
class FakeSpawner:
    """Records spawn calls and hands out controllable FakeProcs."""

    _procs: list[FakeProc] = field(default_factory=list)
    _spawn_log: list[str] = field(default_factory=list)

    def __call__(self, spec: ChildSpec) -> FakeProc:
        proc = FakeProc(name=spec.name)
        self._procs.append(proc)
        self._spawn_log.append(spec.name)
        return proc

    @property
    def spawn_log(self) -> list[str]:
        return list(self._spawn_log)

    def proc_for(self, name: str, occurrence: int = 0) -> FakeProc:
        """Return the Nth spawned proc with the given name (0-indexed)."""
        matches = [p for p in self._procs if p.name == name]
        return matches[occurrence]


class FakeClock:
    """A monotonic fake clock that advances only when told to."""

    def __init__(self) -> None:
        self._now = 0.0

    def __call__(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


def _fake_sleep(_seconds: float) -> None:
    """A no-op sleep for tests — the FakeClock drives time instead."""


# ---------------------------------------------------------------------------
# supervise: initial start ordering
# ---------------------------------------------------------------------------


class TestSuperviseInitialStart:
    def test_single_spec_spawned_once(self) -> None:
        spawner = FakeSpawner()
        calls = iter([True, False])  # run one iteration then stop
        supervise(
            [ChildSpec(name="lva", argv=["lva"], env={})],
            spawn=spawner,
            clock=FakeClock(),
            sleep=_fake_sleep,
            keep_running=lambda: next(calls),
            inter_start_delay=0.0,
            backoff=5.0,
        )
        assert spawner.spawn_log == ["lva"]

    def test_two_specs_spawned_in_order(self) -> None:
        spawner = FakeSpawner()
        calls = iter([True, False])
        specs = [
            ChildSpec(name="aec", argv=["aec"], env={}),
            ChildSpec(name="lva", argv=["lva"], env={}),
        ]
        supervise(
            specs,
            spawn=spawner,
            clock=FakeClock(),
            sleep=_fake_sleep,
            keep_running=lambda: next(calls),
            inter_start_delay=0.0,
            backoff=5.0,
        )
        assert spawner.spawn_log == ["aec", "lva"]

    def test_inter_start_delay_called_between_children(self) -> None:
        sleep_calls: list[float] = []

        def recording_sleep(s: float) -> None:
            sleep_calls.append(s)

        spawner = FakeSpawner()
        calls = iter([True, False])
        specs = [
            ChildSpec(name="aec", argv=["aec"], env={}),
            ChildSpec(name="lva", argv=["lva"], env={}),
        ]
        supervise(
            specs,
            spawn=spawner,
            clock=FakeClock(),
            sleep=recording_sleep,
            keep_running=lambda: next(calls),
            inter_start_delay=0.5,
            backoff=5.0,
        )
        # The inter-start delay (0.5) must appear before the poll sleep (0.1).
        assert 0.5 in sleep_calls

    def test_empty_specs_is_noop(self) -> None:
        spawner = FakeSpawner()
        supervise(
            [],
            spawn=spawner,
            clock=FakeClock(),
            sleep=_fake_sleep,
            keep_running=lambda: True,  # would loop forever if not caught
        )
        assert spawner.spawn_log == []


# ---------------------------------------------------------------------------
# supervise: restart after exit
# ---------------------------------------------------------------------------


class TestSuperviseRestart:
    def _run_to_restart(
        self,
        spec: ChildSpec,
        backoff: float = 5.0,
    ) -> tuple[FakeSpawner, FakeClock]:
        """Drive the supervisor until the first child has been restarted once."""
        spawner = FakeSpawner()
        clock = FakeClock()

        # We'll control the loop via a shared counter.
        iteration: list[int] = [0]

        def keep_running() -> bool:
            # Stop after enough iterations that:
            # iteration 0 → first keep_running check (initial spawn already done)
            # iteration 1 → child exits, backoff set
            # ...advance clock past backoff...
            # iteration N → restart occurs
            # iteration N+1 → stop
            iteration[0] += 1
            if iteration[0] == 1:
                # Let the child exit on next poll.
                spawner.proc_for(spec.name).make_exit(1)
                return True
            if iteration[0] <= 3:
                # Advance clock past backoff each tick.
                clock.advance(backoff + 1)
                return True
            # One final iteration so the restart spawn has been recorded.
            return iteration[0] <= 4

        supervise(
            [spec],
            spawn=spawner,
            clock=clock,
            sleep=_fake_sleep,
            keep_running=keep_running,
            inter_start_delay=0.0,
            backoff=backoff,
        )
        return spawner, clock

    def test_exited_child_is_restarted(self) -> None:
        spec = ChildSpec(name="lva", argv=["lva"], env={})
        spawner, _ = self._run_to_restart(spec)
        # Initial spawn + at least one restart.
        assert spawner.spawn_log.count("lva") >= 2

    def test_restart_is_for_same_spec(self) -> None:
        spec = ChildSpec(name="lva", argv=["lva"], env={})
        spawner, _ = self._run_to_restart(spec)
        assert all(name == "lva" for name in spawner.spawn_log)

    def test_running_child_not_restarted(self) -> None:
        """A child that stays alive is never re-spawned."""
        spawner = FakeSpawner()
        clock = FakeClock()
        iteration: list[int] = [0]

        def keep_running() -> bool:
            iteration[0] += 1
            return iteration[0] <= 5

        spec = ChildSpec(name="lva", argv=["lva"], env={})
        supervise(
            [spec],
            spawn=spawner,
            clock=clock,
            sleep=_fake_sleep,
            keep_running=keep_running,
            inter_start_delay=0.0,
            backoff=5.0,
        )
        # Only the initial spawn — never exited, never restarted.
        assert spawner.spawn_log == ["lva"]


# ---------------------------------------------------------------------------
# supervise: stop terminates all children
# ---------------------------------------------------------------------------


class TestSuperviseStop:
    def _run_and_stop(self, specs: list[ChildSpec], stop_after: int = 1) -> FakeSpawner:
        spawner = FakeSpawner()
        iteration: list[int] = [0]

        def keep_running() -> bool:
            iteration[0] += 1
            return iteration[0] <= stop_after

        supervise(
            specs,
            spawn=spawner,
            clock=FakeClock(),
            sleep=_fake_sleep,
            keep_running=keep_running,
            inter_start_delay=0.0,
            backoff=5.0,
        )
        return spawner

    def test_single_child_terminated_on_stop(self) -> None:
        spec = ChildSpec(name="lva", argv=["lva"], env={})
        spawner = self._run_and_stop([spec])
        assert spawner.proc_for("lva").terminate_calls == 1

    def test_multiple_children_all_terminated_on_stop(self) -> None:
        specs = [
            ChildSpec(name="aec", argv=["aec"], env={}),
            ChildSpec(name="lva", argv=["lva"], env={}),
        ]
        spawner = self._run_and_stop(specs)
        assert spawner.proc_for("aec").terminate_calls == 1
        assert spawner.proc_for("lva").terminate_calls == 1

    def test_wait_called_after_terminate(self) -> None:
        spec = ChildSpec(name="lva", argv=["lva"], env={})
        spawner = self._run_and_stop([spec])
        proc = spawner.proc_for("lva")
        assert proc.terminate_calls == 1
        assert proc.wait_calls == 1


# ---------------------------------------------------------------------------
# supervise: two-child scenario (aec + lva) restart ordering
# ---------------------------------------------------------------------------


class TestSuperviseTwoChildren:
    def test_only_exited_child_restarted_other_untouched(self) -> None:
        """When only LVA exits, only LVA is restarted; AEC is left alone."""
        spawner = FakeSpawner()
        clock = FakeClock()
        iteration: list[int] = [0]

        specs = [
            ChildSpec(name="aec", argv=["aec"], env={}),
            ChildSpec(name="lva", argv=["lva"], env={}),
        ]

        def keep_running() -> bool:
            iteration[0] += 1
            if iteration[0] == 1:
                # LVA exits; AEC keeps running.
                spawner.proc_for("lva").make_exit(0)
                return True
            if iteration[0] <= 3:
                clock.advance(10.0)  # past backoff
                return True
            return iteration[0] <= 4

        supervise(
            specs,
            spawn=spawner,
            clock=clock,
            sleep=_fake_sleep,
            keep_running=keep_running,
            inter_start_delay=0.0,
            backoff=5.0,
        )

        assert spawner.spawn_log.count("aec") == 1  # never restarted
        assert spawner.spawn_log.count("lva") >= 2  # initial + at least one restart


# ---------------------------------------------------------------------------
# ChildSpec immutability
# ---------------------------------------------------------------------------


def test_childspec_is_frozen() -> None:
    spec = ChildSpec(name="lva", argv=["lva"], env={})
    # A frozen-dataclass field assignment raises FrozenInstanceError at runtime.
    # setattr() with a NON-constant attribute name keeps both checkers happy:
    # mypy can't statically resolve the dynamic field, and ruff B010 only flags
    # setattr() with a constant literal (which it would rewrite to spec.name = …,
    # re-triggering mypy's misc error on the frozen field).
    field_name = "name"
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(spec, field_name, "other")


# ---------------------------------------------------------------------------
# child_specs returns ChildSpec instances with correct types
# ---------------------------------------------------------------------------


def test_child_specs_types() -> None:
    for spec in child_specs(_settings(enable_aec=True)):
        assert isinstance(spec, ChildSpec)
        assert isinstance(spec.name, str)
        assert isinstance(spec.argv, list)
        assert isinstance(spec.env, dict)


# ---------------------------------------------------------------------------
# supervise: SIGKILL fallback when wait() times out on stop
# ---------------------------------------------------------------------------


class TestSuperviseKillFallback:
    """Verify that a child whose wait() raises TimeoutExpired gets kill()ed."""

    def test_kill_called_after_timeout(self) -> None:
        # A spawner that always returns a FakeProc configured to time out on
        # the first wait() call (simulating a child that ignores SIGTERM).
        lingering_proc: list[FakeProc] = []

        def stubborn_spawn(spec: ChildSpec) -> FakeProc:
            proc = FakeProc(name=spec.name, wait_raises_timeout=True)
            lingering_proc.append(proc)
            return proc

        supervise(
            [ChildSpec(name="lva", argv=["lva"], env={})],
            spawn=stubborn_spawn,
            clock=FakeClock(),
            sleep=_fake_sleep,
            keep_running=iter([True, False]).__next__,
            inter_start_delay=0.0,
            backoff=5.0,
        )

        assert len(lingering_proc) == 1
        proc = lingering_proc[0]
        assert proc.terminate_calls == 1, "terminate() must have been called"
        assert proc.kill_calls == 1, "kill() must be called after TimeoutExpired"
        # wait() should be called at least twice: once timing out, once after kill()
        assert proc.wait_calls >= 2, "wait() must be called again after kill()"
