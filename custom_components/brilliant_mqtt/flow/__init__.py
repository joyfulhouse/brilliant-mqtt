"""Config-flow step logic for the Brilliant MQTT integration.

Home Assistant imports the flow classes from ``config_flow.py``; that module
stays a thin entry point while the actual step implementations live here:

- ``schemas``: every form schema/normalizer shared by configure and reconfigure
- ``gateway``: the SSH/broker runtime boundary (patch point for tests)
- ``support``: failure states, placeholders, and fleet-entry invariants
- ``onboarding``: the shared panel connect/confirm/provision mixin
- ``fleet``: the top-level config flow (broker validation, fleet creation)
- ``reconfigure``: the per-panel legacy reconfigure steps
- ``subentry``: the Add panel / day-two panel subentry flow
- ``options``: the fleet and per-panel options flows
"""
