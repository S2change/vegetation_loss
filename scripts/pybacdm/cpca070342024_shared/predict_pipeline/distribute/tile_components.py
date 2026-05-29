"""Connected-component extraction for tile-level voted label maps.

Used by aggregate_tile.py to turn a dense `(n_dates, TILE_H, TILE_W) uint8`
label map into a long-form per-component table (one row per detected
patch). The patches are the natural unit of downstream analysis — the
chip-chunked voting pipeline already collapses pixel-level overlap into
"how many chips agreed" per pixel; what survives at this stage is
patches of agreement.

Connectivity is 4-connected (no diagonal neighbours). `skimage.measure.label`
is the preferred backend (faster + bbox/centroid in one pass via
`regionprops`); if skimage isn't installed (some INCD venvs ship only
scipy) we fall back to `scipy.ndimage.label` + manual bbox/centroid.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

import numpy as np

# Prefer skimage. Fall back to scipy if skimage isn't installed.
try:
    from skimage.measure import label as _sk_label, regionprops as _sk_regionprops
    _HAVE_SKIMAGE = True
except ImportError:  # pragma: no cover — exercised on INCD only if skimage missing
    _HAVE_SKIMAGE = False
    from scipy.ndimage import label as _sp_label, find_objects as _sp_find_objects


@dataclass
class Component:
    """One connected component of one class on one target date.

    All coordinates are in tile-pixel space (origin at the tile's LIVE NW
    corner, +y = south, +x = east). Convert to world coords via
    `world_origin + pixel_res * (y, x)`.

    rle_starts/rle_lengths describe the component's pixel-level shape
    using the tile-relative row-major flat index. Flat-index dtype is
    int64 — a TILE_H=5120 tile has 26M pixels, so uint16 overflows and
    even uint32 is uncomfortably close (a single bbox + run wouldn't
    overflow but we want headroom for larger tiles).
    """
    date_ordinal: int
    class_id: int
    component_id: int       # unique within (date, class)
    n_pixels: int
    bbox_y0: int
    bbox_x0: int
    bbox_y1: int            # exclusive
    bbox_x1: int            # exclusive
    centroid_y: float
    centroid_x: float
    rle_starts: np.ndarray   # int64
    rle_lengths: np.ndarray  # int64


def _mask_to_tile_rle(mask: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Encode a (TILE_H, TILE_W) bool mask as (starts, lengths) using
    tile-relative row-major flat indices. int64 dtype throughout.

    Returns empty arrays for an all-False mask.
    """
    flat = mask.ravel().astype(np.uint8, copy=False)
    if not flat.any():
        empty = np.empty(0, dtype=np.int64)
        return empty, empty
    # Boundary-padding trick: diff of [0, flat, 0] makes +1 at run starts
    # and -1 at run ends. Same algorithm as postprocess.chip_records, but
    # with int64 indices for tile-scale flat addresses.
    pad = np.concatenate([[0], flat, [0]]).astype(np.int8)
    diff = np.diff(pad)
    starts = np.where(diff == 1)[0].astype(np.int64)
    ends = np.where(diff == -1)[0].astype(np.int64)
    return starts, ends - starts


def extract_components(label_map_2d: np.ndarray,
                       class_id: int,
                       date_ordinal: int,
                       ) -> Iterator[Component]:
    """Yield one Component per connected region of `class_id` in
    `label_map_2d` (one target date's tile-level voted labels).

    Parameters
    ----------
    label_map_2d : (TILE_H, TILE_W) uint8
        Per-pixel class labels for one target date (0 = no detection).
    class_id : int
        The class to enumerate. 0 (background) is rejected — voting
        already filtered those.
    date_ordinal : int
        Target date as a Python ordinal — stored on every emitted Component.

    Notes
    -----
    Centroids are 0-indexed pixel coordinates (matching skimage's
    convention). Bounding boxes use half-open intervals
    [y0, y1) x [x0, x1) on the same coordinate system.
    """
    if label_map_2d.ndim != 2:
        raise ValueError(
            f"label_map_2d must be 2-D, got shape {label_map_2d.shape}"
        )
    if class_id == 0:
        raise ValueError("class_id=0 is background — refuse to enumerate")

    class_mask = (label_map_2d == class_id)
    if not class_mask.any():
        return

    if _HAVE_SKIMAGE:
        # connectivity=1 -> 4-connected (no diagonals).
        labels = _sk_label(class_mask, connectivity=1, background=0)
        for region in _sk_regionprops(labels):
            comp_mask = (labels == region.label)
            y0, x0, y1, x1 = region.bbox  # half-open intervals, y0..y1
            starts, lengths = _mask_to_tile_rle(comp_mask)
            cy, cx = region.centroid  # (row, col)
            yield Component(
                date_ordinal=int(date_ordinal),
                class_id=int(class_id),
                component_id=int(region.label) - 1,  # 0-indexed for storage
                n_pixels=int(region.area),
                bbox_y0=int(y0), bbox_x0=int(x0),
                bbox_y1=int(y1), bbox_x1=int(x1),
                centroid_y=float(cy), centroid_x=float(cx),
                rle_starts=starts, rle_lengths=lengths,
            )
    else:
        # scipy.ndimage.label uses 4-connectivity by default (no structure
        # arg). Returns (labels, n_features); find_objects gives bboxes.
        labels, n = _sp_label(class_mask)
        slices = _sp_find_objects(labels)  # list of (slice_y, slice_x) per component
        for i, sl in enumerate(slices, start=1):
            if sl is None:  # find_objects can return None for "deleted" labels (shouldn't happen here)
                continue
            comp_mask = (labels == i)
            ys, xs = np.nonzero(comp_mask)
            n_pixels = int(ys.size)
            y0, y1 = int(ys.min()), int(ys.max()) + 1
            x0, x1 = int(xs.min()), int(xs.max()) + 1
            cy = float(ys.mean())
            cx = float(xs.mean())
            starts, lengths = _mask_to_tile_rle(comp_mask)
            yield Component(
                date_ordinal=int(date_ordinal),
                class_id=int(class_id),
                component_id=i - 1,
                n_pixels=n_pixels,
                bbox_y0=y0, bbox_x0=x0, bbox_y1=y1, bbox_x1=x1,
                centroid_y=cy, centroid_x=cx,
                rle_starts=starts, rle_lengths=lengths,
            )
