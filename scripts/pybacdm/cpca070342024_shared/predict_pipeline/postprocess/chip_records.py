"""Encode one ChipPredictionRecord per (chip, target_date).

Step 5 of the chip-chunked prediction pipeline. For each chip's (256, 256)
prediction label map we emit a single record carrying the chip's identity
(6-tuple), date, world position, and per-class binary masks RLE-encoded.

No connected-component enumeration happens here — downstream pixel-level
voting (multiple shifted predictions per pixel in the live 4x4 area) is the
natural unit of analysis, so storing per-component records would just be
work to undo. predict.py's `postprocess_prediction` already runs
morphological closing + small-component removal at the model-output level
before we ever see the labels here.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Iterator

import numpy as np

# ============================================================================
# CONFIGURATION
# ============================================================================

CHIP_H = 256
CHIP_W = 256
HALF = 128

# Background class index — its mask is never stored (it's the complement of
# every other class's masks unioned together).
BACKGROUND_CLASS = 0


# ============================================================================
# DATA TYPE
# ============================================================================

@dataclass
class ChipPredictionRecord:
    """One chip's prediction map, stored as per-class RLE binary masks."""
    # chip_id 6-tuple
    tile_id:        str
    block_row:      int
    block_col:      int
    chip_kind:      str          # 'original' | 'h_shift' | 'v_shift' | 'diagonal'
    grid_row:       int          # original: 0..3
                                  # h_shift:  0..3   (r)
                                  # v_shift:  -1..3  (r_gap)
                                  # diagonal: -1..3  (r_gap)
    grid_col:       int          # original: 0..3
                                  # h_shift:  -1..3  (c_gap)
                                  # v_shift:  0..3   (c)
                                  # diagonal: -1..3  (c_gap)

    # Date
    date_ordinal:   int
    date_iso:       str

    # World position metadata: tells consumers where to place this chip's
    # (256, 256) prediction in tile-pixel coordinates without re-deriving the
    # shift offsets. Reference frame is the LIVE area's NW corner — ghost-
    # using shifts (negative grid_row / grid_col) have negative chip_nw_px_*.
    block_world_origin_x: float
    block_world_origin_y: float
    chip_nw_px_y:         int    # pixel offset from LIVE NW to this chip's NW
    chip_nw_px_x:         int
    pixel_res:            float

    # Per-class pixel counts (cheap chip-level aggregates for filtering /
    # quick stats without decoding RLE).
    n_pixels_by_class: dict[int, int] = field(default_factory=dict)

    # Per-class binary masks, RLE-encoded.
    # Schema per class: a (2, n_runs) uint16 array, row 0 = starts (row-major
    # chip-local flat indices), row 1 = run lengths.
    # Missing class -> no pixels of that class in this chip; not stored.
    masks_by_class: dict[int, np.ndarray] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Plain-dict form for Parquet writing.

        RLE masks are flattened into per-class list[uint16] columns following
        the same `[start0, length0, start1, length1, ...]` convention as the
        old PatchRecord (one Parquet column per class).
        """
        d = asdict(self)
        # `dict[int, ...]` -> per-class columns named `cls_{id}_*`.
        d.pop("n_pixels_by_class")
        d.pop("masks_by_class")
        for cls, n in self.n_pixels_by_class.items():
            d[f"n_pixels_cls_{cls}"] = int(n)
        for cls, rle in self.masks_by_class.items():
            d[f"rle_cls_{cls}"] = rle.astype(np.uint16, copy=False).T.flatten().tolist()
        return d


# ============================================================================
# RLE
# ============================================================================

def _mask_to_rle(mask: np.ndarray) -> np.ndarray:
    """Encode a (H, W) bool mask as a (2, n_runs) uint16 array.

    Row 0 = run starts (chip-local flat indices, row-major).
    Row 1 = run lengths.
    """
    flat = mask.flatten().astype(np.uint8)
    if not flat.any():
        return np.empty((2, 0), dtype=np.uint16)
    pad = np.concatenate([[0], flat, [0]])
    diff = np.diff(pad.astype(np.int8))
    starts = np.where(diff == 1)[0]
    ends   = np.where(diff == -1)[0]
    lengths = ends - starts
    return np.stack([starts.astype(np.uint16), lengths.astype(np.uint16)], axis=0)


# ============================================================================
# CHIP NW PIXEL OFFSET (per chip_kind)
# ============================================================================

def chip_nw_pixel_offset(chip_kind: str,
                         grid_row: int,
                         grid_col: int,
                         ) -> tuple[int, int]:
    """Return (px_y, px_x) of the chip's NW corner relative to the LIVE
    area's NW corner.

    Values can be negative for shifts that extend into the ghost ring
    (h_shift c_gap=-1 -> px_x = -128; v_shift r_gap=-1 -> px_y = -128;
    diagonal with either gap = -1 -> negative on the respective axis).
    """
    base_y = grid_row * CHIP_H
    base_x = grid_col * CHIP_W
    if chip_kind == "original":
        return base_y, base_x
    if chip_kind == "h_shift":
        return base_y, base_x + HALF
    if chip_kind == "v_shift":
        return base_y + HALF, base_x
    if chip_kind == "diagonal":
        return base_y + HALF, base_x + HALF
    raise ValueError(f"unknown chip_kind {chip_kind!r}")


# ============================================================================
# PUBLIC: encode_chip_predictions
# ============================================================================

def encode_chip_predictions(label_map: np.ndarray,
                            *,
                            tile_id: str,
                            block_row: int,
                            block_col: int,
                            chip_kind: str,
                            grid_row: int,
                            grid_col: int,
                            date_ordinal: int,
                            date_iso: str,
                            block_world_origin_x: float,
                            block_world_origin_y: float,
                            pixel_res: float,
                            background_class: int = BACKGROUND_CLASS,
                            ) -> Iterator[ChipPredictionRecord]:
    """Yield 0 or 1 ChipPredictionRecord for one chip's label map.

    Yields 0 records if the label map is entirely background (skips storing
    chips that have nothing to vote with). Yields 1 record otherwise,
    carrying per-class RLE masks for every non-background class present.
    """
    if label_map.shape != (CHIP_H, CHIP_W):
        raise ValueError(
            f"label_map shape {label_map.shape} must equal ({CHIP_H}, {CHIP_W})"
        )

    unique_classes = np.unique(label_map)
    non_bg = [int(c) for c in unique_classes if int(c) != background_class]
    if not non_bg:
        return  # nothing to store

    n_pixels_by_class: dict[int, int] = {}
    masks_by_class: dict[int, np.ndarray] = {}
    for cls in non_bg:
        cls_mask = (label_map == cls)
        n_pixels_by_class[cls] = int(cls_mask.sum())
        masks_by_class[cls] = _mask_to_rle(cls_mask)

    nw_y, nw_x = chip_nw_pixel_offset(chip_kind, grid_row, grid_col)

    yield ChipPredictionRecord(
        tile_id=tile_id,
        block_row=block_row,
        block_col=block_col,
        chip_kind=chip_kind,
        grid_row=grid_row,
        grid_col=grid_col,
        date_ordinal=date_ordinal,
        date_iso=date_iso,
        block_world_origin_x=block_world_origin_x,
        block_world_origin_y=block_world_origin_y,
        chip_nw_px_y=nw_y,
        chip_nw_px_x=nw_x,
        pixel_res=pixel_res,
        n_pixels_by_class=n_pixels_by_class,
        masks_by_class=masks_by_class,
    )
