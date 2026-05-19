"""Composite and shifted-chip generation for the chip-chunked prediction pipeline.

Step 3 of the pipeline (`create_before_after_composites`) collapses a
(N_TS, 10, n_chips * 65_536) uint8 chip-block down to per-target-date
before/after composites by picking, per pixel, the most-recent non-nodata
observation before the target date and the oldest non-nodata observation
after it.

Step 4 (`generate_shifted_chips`) takes those composites and produces
64 chip pairs per target date (16 original 256x256 chips + 16 H-shifts +
16 V-shifts + 16 diagonals) ready to feed to the model. The 5th row and
5th column of the 5x5 block are ghost — they supply neighbour pixels for
shifts that extend past the live 4x4 area but are not themselves
predicted; adjacent blocks overlap by 1 chip so each chip is predicted
exactly once.
"""
from .composite import create_before_after_composites, cascading_select_flat
from .shift_chips import (
    ChipPair,
    ChipBundle,
    generate_shifted_chips,
    generate_shifted_chips_bundled,
)

__all__ = [
    "create_before_after_composites",
    "cascading_select_flat",
    "ChipPair",
    "ChipBundle",
    "generate_shifted_chips",
    "generate_shifted_chips_bundled",
]
