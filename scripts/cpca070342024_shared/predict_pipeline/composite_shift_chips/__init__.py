"""Composite and shifted-chip generation for the chip-chunked prediction pipeline.

Step 3 of the pipeline (`create_before_after_composites`) collapses a
(N_TS, 10, BLOCK_H, BLOCK_W) uint8 chip-block down to per-target-date
before/after composites by picking, per pixel, the most-recent non-nodata
observation before the target date and the oldest non-nodata observation
after it.

Step 4 (`generate_shifted_chips`) takes those composites and produces
81 chip pairs per target date (16 original + 20 H-shifts + 20 V-shifts +
25 diagonals) ready to feed to the model. The block layout has a 128-px
ghost ring on all 4 sides of the 4x4 live area, enabling shifts at all
edges so every live-area pixel ends up covered by exactly 4 chips (one
per shift kind) — the key invariant for downstream pixel-level voting.
"""
from .composite import (
    create_before_after_composites,
    cascading_select,
    cascading_select_flat,  # backwards-compatible alias
)
from .shift_chips import (
    ChipPair,
    ChipBundle,
    generate_shifted_chips,
    generate_shifted_chips_bundled,
    chip_nw_pixel_offset,
    BUNDLE_SIZE,
    N_ORIGINALS,
    N_H_SHIFTS,
    N_V_SHIFTS,
    N_DIAGONALS,
)

__all__ = [
    "create_before_after_composites",
    "cascading_select",
    "cascading_select_flat",
    "ChipPair",
    "ChipBundle",
    "generate_shifted_chips",
    "generate_shifted_chips_bundled",
    "chip_nw_pixel_offset",
    "BUNDLE_SIZE",
    "N_ORIGINALS",
    "N_H_SHIFTS",
    "N_V_SHIFTS",
    "N_DIAGONALS",
]
