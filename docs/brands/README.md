# docs/brands — Default HACS Store Submission Staging

This directory stages brand assets and submission instructions for eventually
listing `brilliant-mqtt` in the HACS default store. It is not part of the
shipped integration and is not deployed to panels.

See [`SUBMISSION.md`](SUBMISSION.md) for the complete step-by-step guide
covering: the brand-asset provenance/trademark note, the `home-assistant/brands` PR,
the `hacs/default` PR, required preconditions (including cutting a GitHub
Release, which is a hard blocker), and how to remove the `ignore: brands` flag
from CI once the brands PR merges.
