"""Write before/after composite GeoTIFFs for one block (debug/inspection).

Optional output of the prediction pipeline: for each valid target date, dump
the 10-band before and after composites built by
`create_before_after_composites` as georeferenced GeoTIFFs so they can be
inspected in QGIS. One file per (date, side); each is the FULL block extent
(LIVE 1024x1024 + the 128-px ghost ring = 1280x1280), in NATIVE HDF5 band
order ([B2, B3, B4, B5, B6, B7, B8, B8a, B11, B12] — NOT the reversed order
the model is fed; that flip is a model-input quirk, not the data's identity).

Georeferencing
--------------
`BlockPosition.world_origin_x/y` is the NW corner of the LIVE area. The block
array stored/composited here also carries the ghost ring, which extends GHOST
pixels (128) further NW. So the GeoTIFF's NW corner is shifted by
GHOST * pixel_res metres up-and-left of the LIVE origin.

This is gated behind WRITE_COMPOSITE_TIFS in predict_block.py and writes to a
dedicated composite_tifs/ dir so it never collides with the aggregator's
block_outputs glob.
"""
from __future__ import annotations

import os
from datetime import date as _date

import numpy as np
import rasterio
from rasterio.transform import from_origin

# Ghost ring thickness (px) around the LIVE area, mirrored from
# input_setup.hdf5_reader.GHOST. Kept local to avoid importing the reader just
# for one constant.
GHOST = 128

# Native HDF5 band order (matches the source `band_names` attribute). Used only
# for band descriptions on the GeoTIFF.
DEFAULT_BAND_NAMES = (
    "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B8a", "B11", "B12",
)


def write_block_composite_tifs(
    composites: np.ndarray,
    target_dates: np.ndarray,
    valid_dates_mask: np.ndarray,
    *,
    out_dir: str,
    tile_id: str,
    block_row: int,
    block_col: int,
    world_origin_x: float,
    world_origin_y: float,
    pixel_res: float,
    crs=None,
    band_names=DEFAULT_BAND_NAMES,
    nodata: int = 255,
    ghost: int = GHOST,
) -> list[str]:
    """Write before/after composite GeoTIFFs for one block.

    Parameters
    ----------
    composites : (2, D, 10, H, W) uint8
        Output of create_before_after_composites. [0]=before, [1]=after.
    target_dates : (D,) int
        Ordinal target dates aligned to composites' axis 1.
    valid_dates_mask : (D,) bool
        Only dates flagged True are written (skipped ones are all-NODATA).
    out_dir : str
        Directory to write into (created if missing).
    tile_id, block_row, block_col : identity for the filename.
    world_origin_x, world_origin_y : float
        UTM NW corner of the LIVE area (BlockPosition.world_origin_*). The
        ghost ring extends `ghost` px NW of this, handled here.
    pixel_res : float
        Metres per pixel.
    crs : rasterio CRS or None
        Written as the GeoTIFF CRS when given.
    band_names : sequence[str]
        Per-band descriptions (native HDF5 order).
    nodata : int
        uint8 NODATA sentinel tagged on the GeoTIFF.
    ghost : int
        Ghost-ring thickness in px (block = LIVE + 2*ghost).

    Returns
    -------
    list[str] — paths written.
    """
    if composites.ndim != 5 or composites.shape[0] != 2:
        raise ValueError(
            f"composites must be (2, D, 10, H, W); got {composites.shape}")
    n_bands = composites.shape[2]

    os.makedirs(out_dir, exist_ok=True)

    # The block array (incl. ghost) starts ghost px NW of the LIVE origin.
    block_origin_x = float(world_origin_x) - ghost * float(pixel_res)
    block_origin_y = float(world_origin_y) + ghost * float(pixel_res)
    transform = from_origin(block_origin_x, block_origin_y,
                            pixel_res, pixel_res)

    _, _, _, h, w = composites.shape
    paths: list[str] = []
    for k, ordinal in enumerate(target_dates):
        if not bool(valid_dates_mask[k]):
            continue
        iso = _date.fromordinal(int(ordinal)).isoformat()
        for side_idx, side in ((0, "before"), (1, "after")):
            arr = composites[side_idx, k]  # (10, H, W) uint8
            out_path = os.path.join(
                out_dir,
                f"{tile_id}_block_{block_row:03d}_{block_col:03d}"
                f"_{iso}_{side}.tif",
            )
            profile = {
                "driver": "GTiff",
                "dtype": "uint8",
                "count": n_bands,
                "height": h,
                "width": w,
                "transform": transform,
                "nodata": nodata,
                "compress": "LZW",
                "tiled": True,
                "blockxsize": 256,
                "blockysize": 256,
            }
            if crs is not None:
                profile["crs"] = crs
            with rasterio.open(out_path, "w", **profile) as dst:
                dst.write(arr)  # (count, H, W)
                for b in range(n_bands):
                    name = band_names[b] if b < len(band_names) else f"band{b}"
                    dst.set_band_description(b + 1, f"{name}_{side}_{iso}")
            paths.append(out_path)
    return paths
