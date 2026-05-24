"""Encode per-chip prediction maps into per-connected-component PatchRecords.

Step 5 of the chip-chunked prediction pipeline. For each non-background
class, find connected components in the (256, 256) prediction map and
build a `PatchRecord` summarising each one.

Connectivity:
  - 8-connectivity (orthogonal + diagonal neighbours)
  - Per-class detection (class 1 components and class 2 components are
    found separately; they never merge)

Filtering:
  - Components with fewer than `MIN_COMPONENT_PIXELS` are dropped (default 4)

Encoding:
  - Mask is encoded as (start, length) pairs in chip-local row-major flat
    pixel indices, stored as a (2, n_runs) uint16 array.
  - World origin is the UTM (x, y) of the component's bounding-box NW corner,
    accounting for the chip's `chip_kind` (original / h_shift / v_shift /
    diagonal) which shifts the chip's UTM origin by half a chip's worth
    of pixels relative to the block grid.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Iterator

import numpy as np
from scipy import ndimage as ndi   # type: ignore[import-untyped]

# ============================================================================
# CONFIGURATION
# ============================================================================

CHIP_H = 256
CHIP_W = 256
HALF = 128
# 8-connectivity structuring element for scipy.ndimage.label.
EIGHT_CONNECTIVITY = np.ones((3, 3), dtype=np.int32)

# Drop connected components smaller than this. 1 keeps everything; 4 drops
# the most obvious noise (singletons + 2/3-pixel speckle).
MIN_COMPONENT_PIXELS = 4

# Background class index — never recorded as a PatchRecord.
BACKGROUND_CLASS = 0


# ============================================================================
# DATA TYPE
# ============================================================================

@dataclass
class PatchRecord:
    """One non-background connected component in one chip's prediction map."""
    # chip_id 6-tuple: (tile_id, block_row, block_col, chip_kind, grid_row, grid_col)
    tile_id:        str
    block_row:      int
    block_col:      int
    chip_kind:      str          # 'original' | 'h_shift' | 'v_shift' | 'diagonal'
    grid_row:       int          # 0..3
    grid_col:       int          # 0..3
    date_ordinal:   int          # ordinal date this prediction was made against
    date_iso:       str          # YYYY-MM-DD, for human readability
    label:          int          # class index (1 = Cuts, 2 = Fires)
    label_name:     str          # 'Cuts' / 'Fires' (or other, from class_names dict)
    n_pixels:       int          # count of pixels in the component
    bbox_chip_y0:   int          # bounding box (top-left, inclusive)
    bbox_chip_x0:   int
    bbox_chip_y1:   int          # bounding box (bottom-right, exclusive)
    bbox_chip_x1:   int
    world_origin_x: float        # UTM easting  of bbox NW corner
    world_origin_y: float        # UTM northing of bbox NW corner
    rle_mask:       np.ndarray   # (2, n_runs) uint16: row 0 = starts, row 1 = lengths

    def to_dict(self) -> dict:
        """Plain-dict form for Parquet writing. RLE flattened to a list."""
        d = asdict(self)
        # Flatten (2, n_runs) -> [start0, length0, start1, length1, ...] so
        # Parquet can store it as a list[uint16].
        rle = self.rle_mask.astype(np.uint16, copy=False)
        d["rle_mask"] = rle.T.flatten().tolist()
        return d


# ============================================================================
# RLE
# ============================================================================

def _mask_to_rle(mask: np.ndarray) -> np.ndarray:
    """Encode a (H, W) bool mask as a (2, n_runs) uint16 array.

    Row 0 holds run starts (chip-local flat pixel indices, row-major).
    Row 1 holds run lengths.
    """
    flat = mask.flatten().astype(np.uint8)
    if not flat.any():
        return np.empty((2, 0), dtype=np.uint16)
    # Find boundaries via diff: 1 = run starts here, -1 = run ends just before.
    pad = np.concatenate([[0], flat, [0]])
    diff = np.diff(pad.astype(np.int8))
    starts = np.where(diff == 1)[0]
    ends   = np.where(diff == -1)[0]
    lengths = ends - starts
    return np.stack([starts.astype(np.uint16), lengths.astype(np.uint16)], axis=0)


# ============================================================================
# WORLD-ORIGIN MATH (per chip_kind)
# ============================================================================

def _chip_nw_pixel_offset(chip_kind: str, grid_row: int, grid_col: int,
                          ) -> tuple[int, int]:
    """Return (px_y, px_x) of the chip's NW corner relative to the block's NW corner.

    The block is a 5x5 grid of CHIP_W x CHIP_H chips packed end-to-end. A
    shift moves the chip's NW corner by HALF px in the relevant axis (relative
    to where the unshifted chip at the same grid position would sit).
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


def _bbox_world_origin(chip_kind: str, grid_row: int, grid_col: int,
                       bbox_chip_y0: int, bbox_chip_x0: int,
                       block_world_origin_x: float,
                       block_world_origin_y: float,
                       pixel_res: float,
                       ) -> tuple[float, float]:
    """UTM (x, y) of the component bbox NW corner."""
    chip_nw_py, chip_nw_px = _chip_nw_pixel_offset(chip_kind, grid_row, grid_col)
    abs_px_x = chip_nw_px + bbox_chip_x0
    abs_px_y = chip_nw_py + bbox_chip_y0
    world_x = block_world_origin_x + abs_px_x * pixel_res
    world_y = block_world_origin_y - abs_px_y * pixel_res
    return world_x, world_y


# ============================================================================
# PUBLIC: encode_patches
# ============================================================================

def encode_patches(label_map: np.ndarray,
                   *,
                   tile_id: str,
                   block_row: int,
                   block_col: int,
                   chip_kind: str,
                   grid_row: int,
                   grid_col: int,
                   date_ordinal: int,
                   date_iso: str,
                   class_names: dict[int, str],
                   block_world_origin_x: float,
                   block_world_origin_y: float,
                   pixel_res: float,
                   min_component_pixels: int = MIN_COMPONENT_PIXELS,
                   background_class: int = BACKGROUND_CLASS,
                   ) -> Iterator[PatchRecord]:
    """Yield one PatchRecord per non-background connected component.

    Components below `min_component_pixels` are dropped (model noise).
    """
    if label_map.shape != (CHIP_H, CHIP_W):
        raise ValueError(
            f"label_map shape {label_map.shape} must equal ({CHIP_H}, {CHIP_W})"
        )

    unique_classes = np.unique(label_map)
    for cls in unique_classes:
        cls_int = int(cls)
        if cls_int == background_class:
            continue
        cls_mask = (label_map == cls_int)
        labeled, n_components = ndi.label(cls_mask, structure=EIGHT_CONNECTIVITY)
        if n_components == 0:
            continue

        # Pre-compute per-component metadata in one pass.
        slices = ndi.find_objects(labeled)
        component_sizes = ndi.sum_labels(
            cls_mask, labels=labeled, index=np.arange(1, n_components + 1),
        ).astype(np.int64)

        for comp_id in range(1, n_components + 1):
            n_pixels = int(component_sizes[comp_id - 1])
            if n_pixels < min_component_pixels:
                continue

            sl = slices[comp_id - 1]
            if sl is None:
                continue
            y0, y1 = int(sl[0].start), int(sl[0].stop)
            x0, x1 = int(sl[1].start), int(sl[1].stop)

            # Build a chip-sized boolean mask for this component, then RLE.
            # (Working in chip-sized makes the RLE indices chip-local.)
            comp_mask = (labeled == comp_id)
            rle = _mask_to_rle(comp_mask)

            world_x, world_y = _bbox_world_origin(
                chip_kind, grid_row, grid_col,
                bbox_chip_y0=y0, bbox_chip_x0=x0,
                block_world_origin_x=block_world_origin_x,
                block_world_origin_y=block_world_origin_y,
                pixel_res=pixel_res,
            )

            yield PatchRecord(
                tile_id=tile_id,
                block_row=block_row,
                block_col=block_col,
                chip_kind=chip_kind,
                grid_row=grid_row,
                grid_col=grid_col,
                date_ordinal=date_ordinal,
                date_iso=date_iso,
                label=cls_int,
                label_name=class_names.get(cls_int, f"class_{cls_int}"),
                n_pixels=n_pixels,
                bbox_chip_y0=y0,
                bbox_chip_x0=x0,
                bbox_chip_y1=y1,
                bbox_chip_x1=x1,
                world_origin_x=world_x,
                world_origin_y=world_y,
                rle_mask=rle,
            )
