"""Generate model-ready chip pairs from a 25-chip block of composites.

Step 4 of the chip-chunked prediction pipeline. Takes the (2, |D|, 10, P)
uint8 composite array produced by step 3 and yields ChipPair named-tuples,
each containing a (before, after) 256x256 chip pair ready for the model.

The 25-chip block is laid out as a 5x5 grid; the upper-left 4x4 (rows 0-3,
cols 0-3) is the "live" area that this block is responsible for predicting.
The 5th row and 5th column (R=4 and C=4) form a ghost border that supplies
neighbour pixels for shifts extending past the live area's right/bottom
edge. Adjacent blocks overlap by 1 chip — the right neighbour block's
column 0 is this block's column 4 — so each chip is predicted by exactly
one block (no duplicated work across blocks).

Per target date, 64 chip pairs are yielded:
  - 16 originals      (4x4 live positions, no shift)
  - 16 H-shifts       (4 between-col gaps x 4 rows — gap 3 uses ghost col 4)
  - 16 V-shifts       (4 between-row gaps x 4 cols — gap 3 uses ghost row 4)
  - 16 diagonals      (4x4 between-corner positions — last row/col use ghost)
"""
from collections import namedtuple
from typing import Iterator

import numpy as np

CHIP_H = 256
CHIP_W = 256
CHIP_PIXELS = CHIP_H * CHIP_W
HALF = 128                     # half-chip in pixels (50% overlap stride)

BLOCK_GRID_ROWS = 5
BLOCK_GRID_COLS = 5
LIVE_ROWS = 4                  # rows 0..3 are predicted; row 4 is ghost
LIVE_COLS = 4                  # cols 0..3 are predicted; col 4 is ghost


ChipPair = namedtuple("ChipPair", [
    "before",         # (10, 256, 256) uint8
    "after",          # (10, 256, 256) uint8
    "date_idx",       # int — index into the target_dates array
    "date_ordinal",   # int — the ordinal date itself
    "chip_kind",      # 'original' | 'h_shift' | 'v_shift' | 'diagonal'
    "grid_position",  # (row, col) within the relevant sub-grid
])


def _extract_chip(side: np.ndarray, R: int, C: int) -> np.ndarray:
    """Extract chip at grid position (R, C) from a flat pixel axis.

    Parameters
    ----------
    side : (10, P) uint8
        One side (before or after) of one target date's composite.
    R, C : int
        Grid row and column in the 5x5 block (0..4 each).

    Returns
    -------
    (10, 256, 256) uint8
    """
    flat_idx = R * BLOCK_GRID_COLS + C
    start = flat_idx * CHIP_PIXELS
    end = start + CHIP_PIXELS
    return side[:, start:end].reshape(10, CHIP_H, CHIP_W)


def _h_shift(side: np.ndarray, R: int, c_gap: int) -> np.ndarray:
    """Build an H-shifted chip from inner-grid chips (R, c_gap) and (R, c_gap+1).

    Output cols 0..127 come from cols 128..255 of (R, c_gap).
    Output cols 128..255 come from cols 0..127 of (R, c_gap+1).
    """
    left  = _extract_chip(side, R, c_gap)
    right = _extract_chip(side, R, c_gap + 1)
    out = np.empty((10, CHIP_H, CHIP_W), dtype=np.uint8)
    out[:, :, :HALF]  = left[:, :, HALF:]
    out[:, :, HALF:]  = right[:, :, :HALF]
    return out


def _v_shift(side: np.ndarray, r_gap: int, C: int) -> np.ndarray:
    """Build a V-shifted chip from inner-grid chips (r_gap, C) and (r_gap+1, C).

    Output rows 0..127 come from rows 128..255 of (r_gap, C).
    Output rows 128..255 come from rows 0..127 of (r_gap+1, C).
    """
    top    = _extract_chip(side, r_gap, C)
    bottom = _extract_chip(side, r_gap + 1, C)
    out = np.empty((10, CHIP_H, CHIP_W), dtype=np.uint8)
    out[:, :HALF, :]  = top[:, HALF:, :]
    out[:, HALF:, :]  = bottom[:, :HALF, :]
    return out


def _diagonal(side: np.ndarray, r_gap: int, c_gap: int) -> np.ndarray:
    """Build a diagonal-shifted chip from the 4 inner chips around (r_gap, c_gap).

    Output is split into 4 quadrants, one from each of:
      top-left     (r_gap,   c_gap)        bottom-right quadrant of source
      top-right    (r_gap,   c_gap+1)      bottom-left  quadrant of source
      bottom-left  (r_gap+1, c_gap)        top-right    quadrant of source
      bottom-right (r_gap+1, c_gap+1)      top-left     quadrant of source
    """
    tl_src = _extract_chip(side, r_gap,     c_gap)
    tr_src = _extract_chip(side, r_gap,     c_gap + 1)
    bl_src = _extract_chip(side, r_gap + 1, c_gap)
    br_src = _extract_chip(side, r_gap + 1, c_gap + 1)
    out = np.empty((10, CHIP_H, CHIP_W), dtype=np.uint8)
    out[:, :HALF, :HALF] = tl_src[:, HALF:, HALF:]
    out[:, :HALF, HALF:] = tr_src[:, HALF:, :HALF]
    out[:, HALF:, :HALF] = bl_src[:, :HALF, HALF:]
    out[:, HALF:, HALF:] = br_src[:, :HALF, :HALF]
    return out


def generate_shifted_chips(composites: np.ndarray,
                           target_dates: np.ndarray,
                           valid_dates_mask: np.ndarray,
                           verbose: bool = True,
                           ) -> Iterator[ChipPair]:
    """Yield ChipPair instances for every valid (target_date, sub-grid position).

    Parameters
    ----------
    composites : (2, |D|, 10, n_chips * 65_536) uint8
        Output of `create_before_after_composites`. composites[0] is before,
        composites[1] is after.
    target_dates : (|D|,) int
        Ordinal dates aligned to composites' axis 1.
    valid_dates_mask : (|D|,) bool
        Output of step 3 alongside `composites`. False entries are skipped
        here with a note explaining why (matching step 3's earlier warning).
    verbose : bool
        Print a one-line note per skipped target date.

    Yields
    ------
    ChipPair
        49 instances per valid target date: 16 originals + 12 H-shifts +
        12 V-shifts + 9 diagonals.
    """
    if composites.ndim != 4 or composites.shape[0] != 2 or composites.shape[2] != 10:
        raise ValueError(
            "composites must have shape (2, |D|, 10, P); "
            f"got {composites.shape}"
        )
    if target_dates.shape != (composites.shape[1],):
        raise ValueError(
            f"target_dates shape {target_dates.shape} must equal (|D|,) = "
            f"({composites.shape[1]},)"
        )
    if valid_dates_mask.shape != (composites.shape[1],):
        raise ValueError(
            f"valid_dates_mask shape {valid_dates_mask.shape} must equal (|D|,) = "
            f"({composites.shape[1]},)"
        )

    for k, target in enumerate(target_dates):
        if not valid_dates_mask[k]:
            if verbose:
                print(f"[note] Skipping date_idx={k} ordinal={int(target)} "
                      f"in shift generator (already skipped by step 3).")
            continue

        before_side = composites[0, k]  # (10, P)
        after_side  = composites[1, k]  # (10, P)
        date_ordinal = int(target)

        # Originals: 4x4 live positions
        for r in range(LIVE_ROWS):
            for c in range(LIVE_COLS):
                yield ChipPair(
                    before=_extract_chip(before_side, r, c),
                    after=_extract_chip(after_side, r, c),
                    date_idx=k, date_ordinal=date_ordinal,
                    chip_kind="original", grid_position=(r, c),
                )

        # H-shifts: 4 rows x 4 between-col gaps (gap 3 uses ghost col 4)
        for r in range(LIVE_ROWS):
            for c_gap in range(LIVE_COLS):
                yield ChipPair(
                    before=_h_shift(before_side, r, c_gap),
                    after=_h_shift(after_side, r, c_gap),
                    date_idx=k, date_ordinal=date_ordinal,
                    chip_kind="h_shift", grid_position=(r, c_gap),
                )

        # V-shifts: 4 between-row gaps x 4 cols (gap 3 uses ghost row 4)
        for r_gap in range(LIVE_ROWS):
            for c in range(LIVE_COLS):
                yield ChipPair(
                    before=_v_shift(before_side, r_gap, c),
                    after=_v_shift(after_side, r_gap, c),
                    date_idx=k, date_ordinal=date_ordinal,
                    chip_kind="v_shift", grid_position=(r_gap, c),
                )

        # Diagonals: 4x4 between-corner positions (last row/col use ghost)
        for r_gap in range(LIVE_ROWS):
            for c_gap in range(LIVE_COLS):
                yield ChipPair(
                    before=_diagonal(before_side, r_gap, c_gap),
                    after=_diagonal(after_side, r_gap, c_gap),
                    date_idx=k, date_ordinal=date_ordinal,
                    chip_kind="diagonal", grid_position=(r_gap, c_gap),
                )
