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
from shapely.geometry import shape as shapely_shape
from shapely.geometry.base import BaseGeometry

# Background class — never polygonized.
BACKGROUND_CLASS = 0
# rasterio connectivity: 4 = rook (no diagonal). Matches the connected-
# component convention the raster path used before polygonization.
CONNECTIVITY = 4


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

    Returns
    -------
    list[PatchPolygon] — one per connected region of each class.
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
