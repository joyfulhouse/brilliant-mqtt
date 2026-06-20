# Default HACS Store Submission Guide

## ARTWORK CAVEAT

**The icon and logo files in `custom_integrations/brilliant_mqtt/` are UNOFFICIAL
PLACEHOLDERS generated as drafts.** They are a generic wall-panel / signal-wave
glyph and plain wordmark — they do not reproduce Brilliant's trademarked artwork.

Before submitting any external PR, replace these files with operator-approved
artwork. Ideally this means the official Brilliant icon used with explicit
permission from Brilliant NextGen, Inc., or a custom icon designed by the
operator. The placeholder art is deliberately generic so there is zero trademark
risk if it is accidentally published, but it is not suitable for a default-store
submission aimed at end-users.

---

## Preconditions checklist (complete in order)

- [ ] Repo has a GitHub description set (required by HACS default store)
- [ ] Repo has the `home-assistant` topic set plus any relevant extras
  (e.g. `hacs`, `mqtt`, `brilliant`, `smart-home`)
- [ ] GitHub Issues are enabled on the repo
- [ ] **At least one published GitHub Release exists** — this is a hard blocker
  for default-store listing; HACS requires a tagged release. The operator chose
  automation-only distribution, so a release must be cut first (e.g. `v0.2.0`
  matching `manifest.json`). Run the `release.yml` workflow or push a `v*` tag.
- [ ] `hassfest` CI job passes green (checks `ha/custom_components/`)
- [ ] `hacs` CI job passes green — currently runs with `ignore: brands` because
  brand assets are not yet in the `home-assistant/brands` repo. Once the brands
  PR is merged (step below), remove `ignore: brands` from
  `.github/workflows/validate.yml` and confirm the HACS job still passes.
- [ ] Replace placeholder artwork with approved icons (see caveat above)
- [ ] `home-assistant/brands` PR merged (step 1 below)
- [ ] Then open `hacs/default` PR (step 2 below)

---

## Step 1 — home-assistant/brands PR

### Asset spec

| File | Dimensions | Notes |
|------|-----------|-------|
| `icon.png` | 256 × 256 px | Required; square; PNG; transparent background; trimmed |
| `icon@2x.png` | 512 × 512 px | Recommended hDPI version |
| `logo.png` | landscape; shortest side 128–256 px | Optional wordmark |
| `logo@2x.png` | landscape; shortest side 256–512 px | Optional hDPI wordmark |

All files: PNG only, lossless-compressed, interlaced preferred, transparency
preferred, trimmed of surrounding whitespace. Custom integrations must NOT use
Home Assistant branded images. Symlinks are not allowed.

### Steps

1. Fork `https://github.com/home-assistant/brands`
2. In your fork, create the directory:
   `custom_integrations/brilliant_mqtt/`
3. Copy the four files from `docs/brands/custom_integrations/brilliant_mqtt/`
   in this repo into that directory (replacing placeholder art with approved
   art first).
4. Commit with a short message such as:
   `Add brilliant_mqtt custom integration brand assets`
5. Open a PR against `home-assistant/brands` `master` branch.

### Ready-to-paste PR title

```
Add brilliant_mqtt custom integration brand assets
```

### Ready-to-paste PR description

```markdown
## Brand assets for `brilliant_mqtt`

**Integration:** Brilliant MQTT Panel Manager
**Repository:** https://github.com/joyfulhouse/brilliant-mqtt
**Domain:** `brilliant_mqtt`
**HACS category:** integration

### Files added

- `custom_integrations/brilliant_mqtt/icon.png` — 256 × 256 px, transparent PNG
- `custom_integrations/brilliant_mqtt/icon@2x.png` — 512 × 512 px, transparent PNG
- `custom_integrations/brilliant_mqtt/logo.png` — 400 × 120 px, transparent PNG
- `custom_integrations/brilliant_mqtt/logo@2x.png` — 800 × 240 px, transparent PNG

### Checklist

- [x] Images are PNG with transparency
- [x] Icons are square (1:1)
- [x] Logo is landscape
- [x] Images are trimmed of surrounding whitespace
- [x] No Home Assistant branded images used
- [x] No symlinks
- [x] Domain matches integration manifest (`brilliant_mqtt`)
```

---

## Step 2 — hacs/default PR

The HACS default store is maintained in `https://github.com/hacs/default`. To
list `brilliant-mqtt` under the `integration` category, add one line to the
`integration` file in that repo.

### Steps

1. Fork `https://github.com/hacs/default`
2. Open `integration` (plain text file, one repo per line, alphabetical order)
3. Add the line:
   ```
   joyfulhouse/brilliant-mqtt
   ```
   Insert it in the correct alphabetical position among the `j` entries.
4. Commit and open a PR against `hacs/default` `master`.

### Ready-to-paste PR title

```
Add joyfulhouse/brilliant-mqtt
```

### Ready-to-paste PR description

```markdown
## Add joyfulhouse/brilliant-mqtt

**Integration:** Brilliant MQTT Panel Manager
**Repository:** https://github.com/joyfulhouse/brilliant-mqtt
**Category:** integration

### About

Local MQTT bridge for Brilliant Smart Home Control in-wall touchscreen panels.
Exposes panel controls (lights, switches, scenes, motion) as Home Assistant
MQTT-Discovery entities. On-panel agent runs under systemd on each Brilliant
panel; no cloud dependency.

### Requirements met

- [x] Integration has a published GitHub Release
- [x] `hassfest` validation passes
- [x] `hacs` validation passes (with brand assets in `home-assistant/brands`)
- [x] GitHub Issues enabled
- [x] Repository description set
- [x] Relevant GitHub topics set
- [x] Brand assets submitted / merged in `home-assistant/brands`
```

---

## After brands PR merges: remove the `ignore: brands` flag

Once the `home-assistant/brands` PR is merged, edit
`.github/workflows/validate.yml` and remove the `ignore: brands` line from the
`hacs` job:

```yaml
      - uses: hacs/action@main
        with:
          category: integration
          # Remove the next line once brands PR is merged:
          # ignore: brands
```

Run the workflow to confirm the HACS validation job passes without the flag.
