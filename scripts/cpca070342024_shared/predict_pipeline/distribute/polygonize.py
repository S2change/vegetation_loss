"""Vectorize voted label maps into per-patch polygons (in world coords).

Replaces the connected-component-on-stitched-raster approach with direct
raster->vector polygonization per block. Each block polygonizes its own
voted LIVE label map; the tile aggregator then dissolves edge-adjacent
polygons across block boundaries (a fire straddling two blocks produces
two edge-touching polygons that `unary_union` welds into one).

`rasterio.features.shapes()` does the heavy lifting: given a 2-D label
array and an affine transform, it yields (geometry, value) pairs already
in world coordinates. We run it per non-background class so each polygon
carries its class id. 4-connectivity matches the connected-component
behaviour the raster path used.

Output unit is one polygon per connected region of one class on one date.
Holes (background enclosed by a class region) are preserved natively by
`shapes()` as polygon interior rings.
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from rasterio.features import shapes as rio_shapes
from rasterio.transform import from_origin
from scipy.ndimage import binary_closing
from shapely.geometry import shape as shapely_shape
from shapely.geometry.base import BaseGeometry

# Background class — never polygonized.
BACKGROUND_CLASS = 0
# rasterio connectivity: 4 = rook (no diagonal). Matches the connected-
# component convention the raster path used before polygonization.
CONNECTIVITY = 4
# Per-class closing radii + fallback radius, shared with the chip-level
# close in <model>.predict.postprocess_prediction so the two stages can't
# drift. They come from the active model package — selected by the MODEL
# env var (same knob predict_block.py uses to pick the model), defaulting
# to bacdm. Model packages sit next to distribute/ under <shared>/, so put
# <shared>/ on the path here.
import importlib as _importlib
import os as _os
import sys as _sys
from pathlib import Path
_sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
_MODEL = _os.environ.get("MODEL", "bacdm")
try:
    _model_pkg = _importlib.import_module(_MODEL)
except ImportError as _exc:
    _available = sorted(
        p.parent.name
        for p in (Path(__file__).resolve().parent.parent).glob("*/predict.py")
    )
    raise SystemExit(
        f"[polygonize] Could not import model package '{_MODEL}': {_exc}\n"
        f"Available model packages: {_available}"
    )
DEFAULT_CLOSING_RADII = getattr(_model_pkg, "CLOSING_RADII", {})
# Fallback radius for classes absent from CLOSING_RADII.
DEFAULT_CLOSING_RADIUS = int(getattr(_model_pkg, "CLOSING_RADIUS", 3))
# Block-level minimum patch area (m^2). Patches smaller than this are
# dropped at the per-block stage. 2500 m^2 = 25 px at 10 m/px, matching
# the old per-chip MIN_PATCH_SIZE=25 floor.
DEFAULT_MIN_AREA_M2 = 2500.0


def close_labels(labels_2d: np.ndarray,
                 classes,
                 *,
                 closing_radius=None,
                 ) -> np.ndarray:
    """Morphological-close a voted label map, per non-background class.

    Runs `binary_closing` with a disk structuring element on each class's
    mask and writes the closed result back into background pixels only —
    so closing one class never overwrites another class's pixels. Mirrors
    the close half of the old `postprocess_prediction`, but applied once
    to the voted block result instead of per chip.

    Each class is closed with its own radius (Cuts → 3, Fires → 1), drawn
    from the active model package's `CLOSING_RADII` (shared with the
    chip-level close so the two stages can't drift). Classes absent from
    that dict fall back to `DEFAULT_CLOSING_RADIUS`.

    Parameters
    ----------
    labels_2d : (H, W) uint8
        Voted class labels (0 = background).
    classes : iterable of int
        Non-background class IDs to close. 0 is ignored.
    closing_radius : int, dict, or None
        Per-class radius control:
          - None (default): use DEFAULT_CLOSING_RADII, falling back to
            DEFAULT_CLOSING_RADIUS for unlisted classes.
          - dict {class_id: radius}: explicit per-class radii (same
            fallback for unlisted classes).
          - int: force one radius for every class (legacy behaviour).

    Returns
    -------
    (H, W) uint8 — closed labels (a copy; input is not mutated). A radius
    of 0 for a class skips closing that class.
    """
    if labels_2d.ndim != 2:
        raise ValueError(f"labels_2d must be 2-D, got {labels_2d.shape}")

    # Resolve the per-class radius lookup once.
    if isinstance(closing_radius, dict):
        radii = closing_radius
        fixed = None
    elif closing_radius is None:
        radii = DEFAULT_CLOSING_RADII
        fixed = None
    else:
        radii = None
        fixed = int(closing_radius)

    def _disk(r: int) -> np.ndarray:
        gy, gx = np.ogrid[-r:r + 1, -r:r + 1]
        return (gx ** 2 + gy ** 2) <= r ** 2

    out = labels_2d.copy()
    for cls in classes:
        cls_int = int(cls)
        if cls_int == BACKGROUND_CLASS:
            continue
        r = fixed if fixed is not None else int(
            radii.get(cls_int, DEFAULT_CLOSING_RADIUS))
        if r <= 0:
            # Radius 0 disables closing for this class.
            continue
        cls_mask = labels_2d == cls_int
        if not cls_mask.any():
            # Nothing of this class in the voted map — closing an all-False
            # mask is a no-op, so skip the (otherwise full-array) work.
            # Common on corner/edge blocks whose detections were all in the
            # NODATA ghost and got clipped out by voting.
            continue
        closed = binary_closing(cls_mask, structure=_disk(r))
        # Only fill pixels that are currently background, so we never
        # clobber a different class's votes.
        out[closed & (out == BACKGROUND_CLASS)] = cls_int
    return out


@dataclass
class PatchPolygon:
    """One vectorized patch of one class on one target date.

    Geometry is a shapely Polygon in world (UTM) coordinates. area_m2 and
    n_pixels are derived from the geometry + pixel_res. centroid_x/y are
    the polygon centroid in world coords.
    """
    date_ordinal: int
    class_id: int
    n_pixels: int
    area_m2: float
    centroid_x: float
    centroid_y: float
    geometry: BaseGeometry


def block_transform(world_origin_x: float,
                    world_origin_y: float,
                    pixel_res: float):
    """Affine transform for a block's LIVE area.

    NW corner (pixel (0,0)) maps to (world_origin_x, world_origin_y); x
    grows east at pixel_res, y shrinks south at pixel_res. Matches the
    convention used for the GeoTIFF outputs.
    """
    return from_origin(world_origin_x, world_origin_y, pixel_res, pixel_res)


def labels_to_polygons(labels_2d: np.ndarray,
                       date_ordinal: int,
                       classes,
                       *,
                       world_origin_x: float,
                       world_origin_y: float,
                       pixel_res: float,
                       min_area_m2: float = 0.0,
                       ) -> list[PatchPolygon]:
    """Polygonize one date's voted LIVE label map.

    Parameters
    ----------
    labels_2d : (H, W) uint8
        Voted class labels for one target date (0 = no detection).
    date_ordinal : int
        Target date as a Python ordinal — stored on every emitted patch.
    classes : iterable of int
        Non-background class IDs to vectorize. 0 is ignored even if passed.
    world_origin_x, world_origin_y, pixel_res : float
        Geo-reference for the block's LIVE NW corner + pixel size (metres).
    min_area_m2 : float (default 0.0)
        Drop polygons whose area is strictly less than this. The block-level
        size floor — boundary-straddling patches are measured per block here,
        so a patch split into two sub-threshold halves is dropped (firm
        floor, by design). The master applies a second, larger floor after
        cross-block merge.

    Returns
    -------
    list[PatchPolygon] — one per connected region of each class that meets
    the area floor.
    """
    if labels_2d.ndim != 2:
        raise ValueError(
            f"labels_2d must be 2-D, got shape {labels_2d.shape}"
        )

    transform = block_transform(world_origin_x, world_origin_y, pixel_res)
    px_area = float(pixel_res) * float(pixel_res)

    out: list[PatchPolygon] = []
    for cls in classes:
        cls_int = int(cls)
        if cls_int == BACKGROUND_CLASS:
            continue
        class_mask = (labels_2d == cls_int)
        if not class_mask.any():
            continue
        # `shapes` wants a value array + a same-shape mask of which cells to
        # vectorize. Feeding a uint8 mask (0/1) and masking to True cells
        # yields one set of shapes with value==1 per connected region.
        mask_u8 = class_mask.astype(np.uint8)
        for geom_json, val in rio_shapes(
            mask_u8, mask=class_mask, transform=transform,
            connectivity=CONNECTIVITY,
        ):
            if int(val) != 1:
                continue
            geom = shapely_shape(geom_json)
            area_m2 = float(geom.area)
            if area_m2 < min_area_m2:
                continue
            centroid = geom.centroid
            out.append(PatchPolygon(
                date_ordinal=int(date_ordinal),
                class_id=cls_int,
                n_pixels=int(round(area_m2 / px_area)),
                area_m2=area_m2,
                centroid_x=float(centroid.x),
                centroid_y=float(centroid.y),
                geometry=geom,
            ))
    return out


def polygons_to_records(patches: list[PatchPolygon],
                        tile_id: str) -> list[dict]:
    """Flatten PatchPolygons into row dicts for a GeoDataFrame.

    Geometry stays as a shapely object under the 'geometry' key so
    geopandas can build the GeoSeries directly.
    """
    rows: list[dict] = []
    from datetime import date as _date
    for p in patches:
        rows.append({
            "tile_id": tile_id,
            "date_ordinal": p.date_ordinal,
            "date_iso": _date.fromordinal(p.date_ordinal).isoformat(),
            "class_id": p.class_id,
            "n_pixels": p.n_pixels,
            "area_m2": p.area_m2,
            "centroid_x": p.centroid_x,
            "centroid_y": p.centroid_y,
            "geometry": p.geometry,
        })
    return rows
