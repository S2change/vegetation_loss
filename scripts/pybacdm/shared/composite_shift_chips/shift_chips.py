"""Generate model-ready chip pairs from a 2-D composite block.

Step 4 of the chip-chunked prediction pipeline. Takes the
(2, |D|, 10, BLOCK_H, BLOCK_W) uint8 composite array produced by step 3
and yields ChipPair named-tuples, each containing a (before, after)
256x256 chip pair ready for the model.

Block layout (matches input_setup.hdf5_reader):
  - BLOCK_H = BLOCK_W = 1280 (= LIVE + 2*GHOST = 1024 + 256)
  - Live 4x4 area sits at block[..., GHOST:GHOST+LIVE_H, GHOST:GHOST+LIVE_W]
  - 128-px ghost ring surrounds it on all 4 sides
  - 128x128 corner squares at the 4 corners

Per target date, 81 chip pairs are yielded:
  - 16 originals      (4x4 live positions, no shift)
  - 20 H-shifts       (4 rows x 5 between-col gap positions, c_gap in -1..3)
  - 20 V-shifts       (5 between-row gap positions x 4 cols, r_gap in -1..3)
  - 25 diagonals      (5x5 between-corner positions, both gaps in -1..3)

Negative-gap shifts extend into the ghost ring. Each live-area pixel ends
up covered by exactly 4 chips (one of each kind) — this is the design
intent for the downstream pixel-level voting.
"""
from collections import namedtuple
from typing import Iterator

import numpy as np

# ============================================================================
# CONFIGURATION
# ============================================================================

CHIP_H = 256
CHIP_W = 256
HALF = 128                     # half-chip in pixels (50% overlap stride)

LIVE_ROWS = 4                  # 4x4 live area
LIVE_COLS = 4
LIVE_H = LIVE_ROWS * CHIP_H    # 1024
LIVE_W = LIVE_COLS * CHIP_W    # 1024
GHOST = 128                    # ghost ring thickness on all 4 sides
BLOCK_H = LIVE_H + 2 * GHOST   # 1280
BLOCK_W = LIVE_W + 2 * GHOST   # 1280

# Per-kind shift counts in the new layout:
#   originals:  4 x 4         = 16
#   h_shifts:   4 x 5         = 20  (5 gaps: x = -128, 128, 384, 640, 896)
#   v_shifts:   5 x 4         = 20
#   diagonals:  5 x 5         = 25
# Total per target date: 81.
N_ORIGINALS = LIVE_ROWS * LIVE_COLS
N_H_SHIFTS  = LIVE_ROWS * (LIVE_COLS + 1)
N_V_SHIFTS  = (LIVE_ROWS + 1) * LIVE_COLS
N_DIAGONALS = (LIVE_ROWS + 1) * (LIVE_COLS + 1)
BUNDLE_SIZE = N_ORIGINALS + N_H_SHIFTS + N_V_SHIFTS + N_DIAGONALS    # 81


ChipPair = namedtuple("ChipPair", [
    "before",         # (10, 256, 256) uint8
    "after",          # (10, 256, 256) uint8
    "date_idx",       # int — index into the target_dates array
    "date_ordinal",   # int — the ordinal date itself
    "chip_kind",      # 'original' | 'h_shift' | 'v_shift' | 'diagonal'
    "grid_position",  # (row, col) within the relevant sub-grid
])


# ============================================================================
# SHIFT-OFFSET MATH
# ============================================================================

def chip_nw_pixel_offset(chip_kind: str,
                         grid_row: int,
                         grid_col: int,
                         ) -> tuple[int, int]:
    """Return (px_y, px_x) of the chip's NW corner relative to the LIVE area's
    NW corner.

    Values may be negative (chips that extend into the ghost ring) — e.g. an
    h_shift at grid_col = -1 has NW at (256·grid_row, -128).

    Per-kind formulas:
      - original  (r, c):     ( 256r,            256c        )
      - h_shift   (r, c_gap): ( 256r,            256·c_gap + 128 )
      - v_shift   (r_gap, c): ( 256·r_gap + 128, 256c        )
      - diagonal  (r_gap, c_gap): ( 256·r_gap + 128, 256·c_gap + 128 )
    """
    if chip_kind == "original":
        return grid_row * CHIP_H, grid_col * CHIP_W
    if chip_kind == "h_shift":
        return grid_row * CHIP_H, grid_col * CHIP_W + HALF
    if chip_kind == "v_shift":
        return grid_row * CHIP_H + HALF, grid_col * CHIP_W
    if chip_kind == "diagonal":
        return grid_row * CHIP_H + HALF, grid_col * CHIP_W + HALF
    raise ValueError(f"unknown chip_kind {chip_kind!r}")


def _slice_chip(side_2d: np.ndarray, nw_y: int, nw_x: int) -> np.ndarray:
    """Slice a 256x256 chip from a 2-D block-side array.

    Parameters
    ----------
    side_2d : (10, BLOCK_H, BLOCK_W) uint8
        One side (before or after) of one target date's composite, in 2-D layout.
    nw_y, nw_x : int
        NW pixel offset of the chip relative to the LIVE area's NW corner.
        Can be negative (-128 for shifts that extend into the ghost ring).

    Returns
    -------
    (10, 256, 256) uint8
    """
    # Translate to block-local coords by adding GHOST (the live area's NW
    # corner is at block[GHOST, GHOST]).
    by = GHOST + nw_y
    bx = GHOST + nw_x
    return side_2d[:, by:by + CHIP_H, bx:bx + CHIP_W]


# ============================================================================
# SHIFT POSITION ENUMERATION (gap index ranges)
# ============================================================================

def _iter_shift_positions(chip_kind: str):
    """Yield (grid_row, grid_col) for every shift position of the given kind.

    Originals: 4 x 4 = 16    (r, c)        in [0,4) x [0,4)
    H-shifts:  4 x 5 = 20    (r, c_gap)    in [0,4) x [-1,4)
    V-shifts:  5 x 4 = 20    (r_gap, c)    in [-1,4) x [0,4)
    Diagonals: 5 x 5 = 25    (r_gap, c_gap) in [-1,4) x [-1,4)
    """
    if chip_kind == "original":
        for r in range(LIVE_ROWS):
            for c in range(LIVE_COLS):
                yield r, c
        return
    if chip_kind == "h_shift":
        for r in range(LIVE_ROWS):
            for c_gap in range(-1, LIVE_COLS):
                yield r, c_gap
        return
    if chip_kind == "v_shift":
        for r_gap in range(-1, LIVE_ROWS):
            for c in range(LIVE_COLS):
                yield r_gap, c
        return
    if chip_kind == "diagonal":
        for r_gap in range(-1, LIVE_ROWS):
            for c_gap in range(-1, LIVE_COLS):
                yield r_gap, c_gap
        return
    raise ValueError(f"unknown chip_kind {chip_kind!r}")


# ============================================================================
# PER-PAIR GENERATOR
# ============================================================================

def generate_shifted_chips(composites: np.ndarray,
                           target_dates: np.ndarray,
                           valid_dates_mask: np.ndarray,
                           verbose: bool = True,
                           ) -> Iterator[ChipPair]:
    """Yield ChipPair instances for every valid (target_date, sub-grid position).

    Parameters
    ----------
    composites : (2, |D|, 10, BLOCK_H, BLOCK_W) uint8
        Output of `create_before_after_composites` for a 2-D block. axis 0:
        0 = before, 1 = after. axis 1: target dates.
    target_dates : (|D|,) int
        Ordinal dates aligned to composites' axis 1.
    valid_dates_mask : (|D|,) bool
        Output of step 3 alongside `composites`. False entries are skipped.
    verbose : bool
        Print a one-line note per skipped target date.

    Yields
    ------
    ChipPair
        81 instances per valid target date: 16 originals + 20 H-shifts +
        20 V-shifts + 25 diagonals.
    """
    _validate_composites(composites, target_dates, valid_dates_mask)

    for k, target in enumerate(target_dates):
        if not valid_dates_mask[k]:
            if verbose:
                print(f"[note] Skipping date_idx={k} ordinal={int(target)} "
                      f"in shift generator (already skipped by step 3).")
            continue

        before_side = composites[0, k]   # (10, BLOCK_H, BLOCK_W)
        after_side  = composites[1, k]
        date_ordinal = int(target)

        for kind in ("original", "h_shift", "v_shift", "diagonal"):
            for gr, gc in _iter_shift_positions(kind):
                nw_y, nw_x = chip_nw_pixel_offset(kind, gr, gc)
                yield ChipPair(
                    before=_slice_chip(before_side, nw_y, nw_x).copy(),
                    after=_slice_chip(after_side, nw_y, nw_x).copy(),
                    date_idx=k, date_ordinal=date_ordinal,
                    chip_kind=kind, grid_position=(gr, gc),
                )


def _validate_composites(composites: np.ndarray,
                         target_dates: np.ndarray,
                         valid_dates_mask: np.ndarray) -> None:
    """Shape checks shared by both generators."""
    if composites.ndim != 5 or composites.shape[0] != 2 or composites.shape[2] != 10:
        raise ValueError(
            "composites must have shape (2, |D|, 10, BLOCK_H, BLOCK_W); "
            f"got {composites.shape}"
        )
    if composites.shape[3] != BLOCK_H or composites.shape[4] != BLOCK_W:
        raise ValueError(
            f"composites' spatial dims must be ({BLOCK_H}, {BLOCK_W}); "
            f"got ({composites.shape[3]}, {composites.shape[4]})"
        )
    if target_dates.shape != (composites.shape[1],):
        raise ValueError(
            f"target_dates shape {target_dates.shape} must equal (|D|,) = "
            f"({composites.shape[1]},)"
        )
    if valid_dates_mask.shape != (composites.shape[1],):
        raise ValueError(
            f"valid_dates_mask shape {valid_dates_mask.shape} must equal "
            f"(|D|,) = ({composites.shape[1]},)"
        )


# ============================================================================
# BUNDLED VARIANT (one yield per target date, all 81 chips at once)
# ============================================================================

ChipBundle = namedtuple("ChipBundle", [
    "before",          # (BUNDLE_SIZE, 256, 256, 10) uint8 — predictor-native layout
    "after",           # (BUNDLE_SIZE, 256, 256, 10) uint8
    "chip_kinds",      # list[str] length BUNDLE_SIZE — parallel to batch axis
    "grid_positions",  # list[tuple[int, int]] length BUNDLE_SIZE
    "date_idx",        # int — index into target_dates
    "date_ordinal",    # int — the ordinal date itself
])


def _fill_bundle_side(side_2d: np.ndarray, dst: np.ndarray) -> tuple[list, list]:
    """Fill a (BUNDLE_SIZE, 256, 256, 10) uint8 array `dst` from one composite
    side. Returns the parallel (chip_kinds, grid_positions) metadata.

    `dst` is filled in place. Each 256x256 chip is sliced from the 2-D block
    and transposed (C, H, W) -> (H, W, C) to match the predictor's input
    layout.
    """
    chip_kinds: list[str] = []
    grid_positions: list[tuple[int, int]] = []
    i = 0
    for kind in ("original", "h_shift", "v_shift", "diagonal"):
        for gr, gc in _iter_shift_positions(kind):
            nw_y, nw_x = chip_nw_pixel_offset(kind, gr, gc)
            chip = _slice_chip(side_2d, nw_y, nw_x)            # (10, 256, 256)
            dst[i] = chip.transpose(1, 2, 0)                   # (256, 256, 10)
            chip_kinds.append(kind)
            grid_positions.append((gr, gc))
            i += 1
    assert i == BUNDLE_SIZE
    return chip_kinds, grid_positions


def generate_shifted_chips_bundled(composites: np.ndarray,
                                   target_dates: np.ndarray,
                                   valid_dates_mask: np.ndarray,
                                   verbose: bool = True,
                                   ) -> Iterator[ChipBundle]:
    """Bundled variant: yield one ChipBundle per valid target date.

    Same geometry as `generate_shifted_chips` (81 chips per date in
    originals -> h_shift -> v_shift -> diagonal order). Differences:

    - One yield per target date (not 81).
    - `before`/`after` are pre-allocated `(BUNDLE_SIZE, 256, 256, 10)` uint8
      arrays in the predictor's native (H, W, C) layout.
    - Memory cost per active bundle: 81 * 2 * 256 * 256 * 10 = ~106 MB.
    """
    _validate_composites(composites, target_dates, valid_dates_mask)

    for k, target in enumerate(target_dates):
        if not valid_dates_mask[k]:
            if verbose:
                print(f"[note] Skipping date_idx={k} ordinal={int(target)} "
                      f"in shift generator (already skipped by step 3).")
            continue

        before_side = composites[0, k]
        after_side  = composites[1, k]
        date_ordinal = int(target)

        before_bundle = np.empty(
            (BUNDLE_SIZE, CHIP_H, CHIP_W, 10), dtype=np.uint8)
        after_bundle  = np.empty(
            (BUNDLE_SIZE, CHIP_H, CHIP_W, 10), dtype=np.uint8)

        kinds_b, positions_b = _fill_bundle_side(before_side, before_bundle)
        kinds_a, positions_a = _fill_bundle_side(after_side,  after_bundle)
        assert kinds_b == kinds_a and positions_b == positions_a

        yield ChipBundle(
            before=before_bundle,
            after=after_bundle,
            chip_kinds=kinds_b,
            grid_positions=positions_b,
            date_idx=k,
            date_ordinal=date_ordinal,
        )
