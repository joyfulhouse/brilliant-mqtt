# Enabling root SSH on a Brilliant panel

The bridge runs **on the panel itself** — the message bus it reads is only
reachable over a local unix socket — so each panel needs root SSH so you can
deploy and manage the agent. Brilliant ships an **official, supported** way to
enable it; no jailbreak is needed, but it is opt-in per device.

## Steps

1. On the panel (or in the Brilliant app for that panel), open **Settings →
   Advanced Settings → Root SSH Login**.
2. Enabling it requires **verifying your identity by email**. Complete the
   verification and the panel activates SSH with **per-device root credentials**
   (password authentication only — there is no authorized-keys mechanism).
3. Record the credentials somewhere safe and **out of git**. The password can
   contain shell-hostile characters, so connect with `sshpass` and password-only
   auth:

   ```bash
   SSHPASS='<root password>' sshpass -e ssh \
     -o PreferredAuthentications=password -o PubkeyAuthentication=no \
     -o NumberOfPasswordPrompts=1 root@<panel-ip>
   ```

   `NumberOfPasswordPrompts=1` makes a wrong password fail fast instead of
   retrying (avoid lockouts).

## Read Brilliant's caveats first

Review the official
[RootSSH support article](https://support.brilliant.tech/hc/en-us/articles/23152790775195-RootSSH)
before enabling. In short:

- It is intended for knowledgeable command-line users.
- Changes you make can break updates or functionality.
- Once enabled, the device is **permanently flagged as possibly manipulated** —
  don't enable it on a device you plan to transfer.
- Brilliant will never ask you to enable it or to share the credentials.

## Treat the panel as production

These are production in-wall touchscreens. Treat SSH as **read-only** except for
the deliberate install steps, **pilot ONE panel first**, and note that the bridge
only calls the same message-bus APIs Brilliant's own HomeKit peripheral uses,
under a resource-capped systemd unit.

---

Back to the [install overview](../../INSTALL.md).
