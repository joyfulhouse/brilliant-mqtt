"""The integration ships its own brand images (HA 2026.3+ bundled-brand API).

Since Home Assistant 2026.3.0, custom integrations provide brand icons/logos by
bundling them under ``<integration>/brand/`` instead of submitting them to the
``home-assistant/brands`` repository (that repo now auto-closes custom-integration
PRs). HA serves these via ``/api/brands/integration/{domain}/{image}``, taking
priority over the CDN. These assets must keep matching the brands image spec, so
this guard pins their presence, format, and dimensions.
"""

from __future__ import annotations

import struct
from pathlib import Path

import custom_components.brilliant_mqtt as integration

BRAND = Path(integration.__file__).parent / "brand"

_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def _png_size(path: Path) -> tuple[int, int]:
    """Return (width, height) read straight from the PNG IHDR header.

    Avoids a Pillow dependency: a PNG is the 8-byte signature, a 4-byte chunk
    length, the ``IHDR`` chunk type, then width and height as big-endian uint32.
    """
    data = path.read_bytes()
    assert data[:8] == _PNG_SIGNATURE, f"{path.name} is not a PNG"
    assert data[12:16] == b"IHDR", f"{path.name} has no leading IHDR chunk"
    width, height = struct.unpack(">II", data[16:24])
    return width, height


def test_brand_directory_holds_the_four_assets() -> None:
    assert BRAND.is_dir(), "custom_components/brilliant_mqtt/brand/ is missing"
    names = {p.name for p in BRAND.glob("*.png")}
    assert {"icon.png", "icon@2x.png", "logo.png", "logo@2x.png"} <= names


def test_icons_are_square_and_correctly_sized() -> None:
    # Icons must be exactly 256x256 (1x) and 512x512 (2x), 1:1 square.
    assert _png_size(BRAND / "icon.png") == (256, 256)
    assert _png_size(BRAND / "icon@2x.png") == (512, 512)


def test_logos_are_landscape_within_the_brands_size_range() -> None:
    # Logos must be landscape; shortest side 128-256 (1x) and 256-512 (2x).
    width, height = _png_size(BRAND / "logo.png")
    assert width > height, "logo.png must be landscape"
    assert 128 <= min(width, height) <= 256

    width2, height2 = _png_size(BRAND / "logo@2x.png")
    assert width2 > height2, "logo@2x.png must be landscape"
    assert 256 <= min(width2, height2) <= 512
