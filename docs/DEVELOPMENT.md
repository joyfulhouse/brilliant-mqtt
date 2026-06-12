# Development

How to set up a development environment for brilliant-mqtt.

## Prerequisites

- [`uv`](https://docs.astral.sh/uv/) — uv provisions the right interpreter
  automatically.
- **Python 3.10 — exactly.** The bridge runs under the panel's bundled
  Python 3.10.9, so `requires-python = ">=3.10,<3.11"`. The panel interpreter
  is the runtime and cannot be upgraded — do not bump the pin or use 3.11+
  syntax.

## Setup

```bash
git clone https://github.com/joyfulhouse/brilliant-mqtt.git
cd brilliant-mqtt
uv sync
```

## Quality Checks

All four must pass before any commit:

```bash
uv run ruff check --fix          # lint
uv run ruff format               # format
uv run mypy --strict src tests   # type check
uv run pytest                    # tests
```

Run a single test file or test while iterating:

```bash
uv run pytest tests/test_model.py -v
uv run pytest -k brightness -v
```

## Testing Rules

- **TDD:** failing test → minimal implementation → green → commit. Small,
  frequent commits.
- **The unit suite must run on any machine.** Only `src/brilliant_mqtt/bus.py`
  may import the panel-only `lib.message_bus_api`; everything else depends on
  the `BusClient`/`MqttClient` Protocols and is tested against `FakeBus` /
  `FakeMqtt` (`tests/fakes.py`).
- Never disable linters (`# noqa`, `# type: ignore`) — fix the root cause.

## On-Panel Work

Live-panel probing and deploys need credentials kept in a local **gitignored**
file (this repo's convention: `CREDENTIALS.local.md` — SSH details, broker
credentials) and must follow the safety rules in [`../CLAUDE.md`](../CLAUDE.md)
— these are production in-wall touchscreens. Pilot one panel; read-only by
default.

## Releasing

There is no PyPI release: the bridge deploys straight to panels (see
[INSTALL.md](../INSTALL.md) and
[reference/deployment.md](reference/deployment.md); automate with your own
configuration management). Tag versions in git and record them in
[CHANGELOG.md](../CHANGELOG.md).

See [CONTRIBUTING](https://github.com/joyfulhouse/.github/blob/main/CONTRIBUTING.md)
for the org-wide contribution workflow.
