"""Merge per-block polygons + voted .npz shards into tile-level outputs.

Runs once after all array tasks for a tile complete (gated by the SLURM
`afterok` dependency in submit_tile.sh). Reads every
`{TILE_ID}_block_*.gpkg` (per-block polygons) and `{TILE_ID}_block_*.npz`
(per-block voted label maps) from `BLOCK_OUTPUT_DIR` (defaults to
`OUTPUT_DIR`), and writes the tile-level outputs below into
`FINAL_OUTPUT_DIR` (defaults to `OUTPUT_DIR`):

  - `{TILE_ID}_tile.gpkg` (PRIMARY) — one polygon per detected patch,
    class > 0. Patches straddling block boundaries are dissolved into a
    single geometry (per date + class) via unary_union of edge-adjacent
    polygons. Schema: tile_id, date_ordinal, date_iso, class_id,
    n_pixels, area_m2, centroid_x/y, geometry (Polygon, in the tile CRS).
  - `{TILE_ID}_tile.parquet` (PRIMARY, analysis) — the same patches as
    GeoParquet for fast pandas/geopandas reads.
  - `{TILE_ID}_tile.npz` (AUXILIARY) — the dense `(n_dates, TILE_H, TILE_W)`
    uint8 label map (blocks stitched into one canvas) + metadata.
  - `{TILE_ID}_tile_{YYYY-MM-DD}.tif` (GIS raster) — one LZW GeoTIFF per
    date, class 0 as NoData.
  - `{TILE_ID}_tile_{YYYY-MM-DD}_ndvi_before.tif` / `_ndvi_after.tif`
    (optional) — per-date before/after NDVI float32 rasters, written only
    when the predict step ran with OUTPUT_NDVI=1. NDVI is also added to the
    dense .npz (keys `ndvi_before` / `ndvi_after`). NoData = -9999.
  - `{TILE_ID}_block_grid.gpkg` (DEBUG) — one rectangle per block's LIVE
    extent (layer `block_grid`), drawn from each block's recorded
    world_origin. Overlay on the detections in QGIS to inspect block seams;
    `origin_drift_m` / `origin_ok` flag any block whose origin left the grid.

Boundary merge: LIVE areas tile with no gap, so a patch crossing a block
seam yields two edge-touching polygons in adjacent blocks. unary_union
welds touching/overlapping polygons, so the dissolved result is one patch
— no ghost output or overlap dedup needed.

Optional clip mask: set CLIP_MASK_GPKG to a polygon .gpkg to restrict the
final VECTOR map to a region of interest. The dissolved patches are
intersected with the (reprojected) mask before being written, so polygons
outside the mask are dropped and edge-straddling ones are cut to the inside.
This affects the .gpkg / .parquet outputs only; the dense .npz and per-date
.tif rasters are produced from the stitched label array and are not clipped.
"""
import os
import sys
import time
from datetime import date as _date
from pathlib import Path

import numpy as np
import geopandas as gpd
import pandas as pd
import rasterio
from rasterio.transform import from_origin
from rasterio.crs import CRS
from shapely.ops import unary_union

# h5py used only to grab the CRS attribute from the source HDF5. Optional
# so the aggregator can still run when TILE_HDF5_PATH isn't provided.
try:
    import h5py
    _HAVE_H5PY = True
except ImportError:
    _HAVE_H5PY = False

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))   # shared/
sys.path.insert(0, str(_HERE))          # distribute/

from postprocess.voted_output import read_voted_block
from postprocess.vote import LIVE_H, LIVE_W
from composite_shift_chips.ndvi import NDVI_NODATA


# Output column order for the dissolved tile vector (.gpkg / .parquet).
_VECTOR_COLUMNS = [
    "tile_id", "date_ordinal", "date_iso", "class_id",
    "n_pixels", "area_m2", "centroid_x", "centroid_y", "geometry",
]


def _required_env(name: str) -> str:
    v = os.environ.get(name)
    if v is None or v == "":
        raise SystemExit(f"[aggregate_tile] Missing required env var: {name}")
    return v


def _stitch_blocks(blocks: list[dict], n_rows: int, n_cols: int,
                   block_h: int, block_w: int, n_dates: int,
                   tile_origin_x: float, tile_origin_y: float,
                   pixel_res: float,
                   row_offset: int = 0, col_offset: int = 0) -> np.ndarray:
    """Place each block's labels at its cell in the (cropped) tile canvas.

    `row_offset`/`col_offset` are the block-grid coords of the canvas's
    top-left cell (the NW-most processed block) when only a sub-rectangle of
    the tile was processed; a block at grid (r, c) lands at canvas cell
    (r - row_offset, c - col_offset). For a full-tile run both offsets are 0.
    tile_origin_x/y is the world NW corner of that top-left cell. Warns on
    per-block world-origin drift > 0.5 m.
    """
    labels = np.zeros(
        (n_dates, n_rows * block_h, n_cols * block_w), dtype=np.uint8,
    )
    block_w_m = block_w * pixel_res
    block_h_m = block_h * pixel_res
    for b in blocks:
        r = int(b["block_row"]) - row_offset
        c = int(b["block_col"]) - col_offset
        expected_x = tile_origin_x + c * block_w_m
        expected_y = tile_origin_y - r * block_h_m
        got_x = float(b["world_origin_x"])
        got_y = float(b["world_origin_y"])
        if abs(got_x - expected_x) > 0.5 or abs(got_y - expected_y) > 0.5:
            print(f"  [warn] block ({b['block_row']},{b['block_col']}) world "
                  f"origin off: expected ({expected_x}, {expected_y}), "
                  f"got ({got_x}, {got_y})")
        y0 = r * block_h
        x0 = c * block_w
        labels[:, y0:y0 + block_h, x0:x0 + block_w] = b["labels"]
    return labels


def _stitch_float_key(blocks: list[dict], key: str,
                      n_rows: int, n_cols: int,
                      block_h: int, block_w: int, n_dates: int,
                      fill: float,
                      row_offset: int = 0, col_offset: int = 0) -> np.ndarray:
    """Stitch a per-block float array (e.g. ndvi_before) into the tile canvas.

    Same placement as `_stitch_blocks` but for a float32 key, pre-filled with
    `fill` so any block missing the key (or off-canvas gaps) reads as nodata.
    """
    canvas = np.full(
        (n_dates, n_rows * block_h, n_cols * block_w), fill, dtype=np.float32,
    )
    for b in blocks:
        if key not in b:
            continue
        r = int(b["block_row"]) - row_offset
        c = int(b["block_col"]) - col_offset
        y0 = r * block_h
        x0 = c * block_w
        canvas[:, y0:y0 + block_h, x0:x0 + block_w] = b[key]
    return canvas


def _dissolve_block_polygons(block_gdf: gpd.GeoDataFrame,
                             tile_id: str,
                             pixel_res: float,
                             min_area_m2: float = 0.0,
                             ) -> gpd.GeoDataFrame:
    """Dissolve per-block polygons into per-patch tile polygons.

    For each (date_ordinal, class_id) group, unary_union all member
    polygons — edge-touching patches from adjacent blocks weld into one —
    then explode the resulting (multi)polygon back to individual patches.
    Re-derives n_pixels / area_m2 / centroid per merged patch, and drops
    patches below `min_area_m2` (the master-level floor, applied AFTER the
    cross-block merge so boundary-straddling patches are measured at full
    size).

    Returns a GeoDataFrame with `_VECTOR_COLUMNS`, same CRS as the input.
    """
    px_area = float(pixel_res) * float(pixel_res)
    crs = block_gdf.crs

    if len(block_gdf) == 0:
        return gpd.GeoDataFrame(columns=_VECTOR_COLUMNS, geometry="geometry",
                                crs=crs)

    rows: list[dict] = []
    # Group by (date, class); within each, weld touching/overlapping polys.
    for (ordinal, cls), grp in block_gdf.groupby(["date_ordinal", "class_id"]):
        merged = unary_union(list(grp.geometry))
        # unary_union returns a Polygon or MultiPolygon; normalise to a list.
        geoms = list(merged.geoms) if merged.geom_type == "MultiPolygon" \
            else [merged]
        iso = _date.fromordinal(int(ordinal)).isoformat()
        for geom in geoms:
            if geom.is_empty:
                continue
            area_m2 = float(geom.area)
            if area_m2 < min_area_m2:
                continue
            centroid = geom.centroid
            rows.append({
                "tile_id": tile_id,
                "date_ordinal": int(ordinal),
                "date_iso": iso,
                "class_id": int(cls),
                "n_pixels": int(round(area_m2 / px_area)),
                "area_m2": area_m2,
                "centroid_x": float(centroid.x),
                "centroid_y": float(centroid.y),
                "geometry": geom,
            })

    if not rows:
        return gpd.GeoDataFrame(columns=_VECTOR_COLUMNS, geometry="geometry",
                                crs=crs)
    out = gpd.GeoDataFrame(rows, geometry="geometry", crs=crs)
    return out[_VECTOR_COLUMNS]


def _clip_to_mask(tile_gdf: gpd.GeoDataFrame,
                  mask_gpkg_path: str,
                  pixel_res: float,
                  ) -> gpd.GeoDataFrame:
    """Clip the dissolved tile polygons to a mask polygon .gpkg.

    Reads `mask_gpkg_path`, reprojects it to the tile CRS, unions all its
    features into a single mask geometry, and intersects every tile polygon
    with it. Polygons fully outside the mask are dropped; polygons straddling
    the mask edge are cut to the inside portion. n_pixels / area_m2 / centroid
    are re-derived from the clipped geometry so the attributes stay consistent
    with what's actually stored (same convention as _dissolve_block_polygons).

    Parameters
    ----------
    tile_gdf : GeoDataFrame
        Dissolved tile polygons (`_VECTOR_COLUMNS`), with a CRS set.
    mask_gpkg_path : str
        Path to a polygon .gpkg used as the clip mask. All layers/features are
        unioned into one mask.
    pixel_res : float
        Metres per pixel, for re-deriving n_pixels from the clipped area.

    Returns
    -------
    GeoDataFrame with `_VECTOR_COLUMNS`, same CRS as `tile_gdf`, containing
    only the inside-mask geometry. Empty if nothing overlaps the mask.
    """
    px_area = float(pixel_res) * float(pixel_res)
    crs = tile_gdf.crs

    mask_gdf = gpd.read_file(mask_gpkg_path)
    if len(mask_gdf) == 0:
        raise SystemExit(
            f"[aggregate_tile] clip mask {mask_gpkg_path} has no features."
        )
    # Reproject the mask to the tile CRS so the intersection is valid. Requires
    # both to have a CRS; the tile CRS comes from the HDF5, the mask from its
    # own file.
    if crs is not None and mask_gdf.crs is not None and mask_gdf.crs != crs:
        mask_gdf = mask_gdf.to_crs(crs)
    elif mask_gdf.crs is None:
        print("  [warn] clip mask has no CRS; assuming it matches the tile CRS.")

    mask_geom = unary_union(list(mask_gdf.geometry))

    if len(tile_gdf) == 0:
        return gpd.GeoDataFrame(columns=_VECTOR_COLUMNS, geometry="geometry",
                                crs=crs)

    rows: list[dict] = []
    for _, r in tile_gdf.iterrows():
        clipped = r["geometry"].intersection(mask_geom)
        if clipped.is_empty:
            continue
        geoms = list(clipped.geoms) if clipped.geom_type == "MultiPolygon" \
            else [clipped]
        for geom in geoms:
            # intersection can yield lines/points where geometries only touch;
            # keep polygonal pieces only.
            if geom.is_empty or geom.geom_type not in ("Polygon", "MultiPolygon"):
                continue
            area_m2 = float(geom.area)
            centroid = geom.centroid
            rows.append({
                "tile_id": r["tile_id"],
                "date_ordinal": int(r["date_ordinal"]),
                "date_iso": r["date_iso"],
                "class_id": int(r["class_id"]),
                "n_pixels": int(round(area_m2 / px_area)),
                "area_m2": area_m2,
                "centroid_x": float(centroid.x),
                "centroid_y": float(centroid.y),
                "geometry": geom,
            })

    if not rows:
        return gpd.GeoDataFrame(columns=_VECTOR_COLUMNS, geometry="geometry",
                                crs=crs)
    out = gpd.GeoDataFrame(rows, geometry="geometry", crs=crs)
    return out[_VECTOR_COLUMNS]


def _read_hdf5_crs(hdf5_path: str | None) -> CRS | None:
    """Return a rasterio CRS for the tile's HDF5, or None if unavailable.

    Looks up the `crs` attribute on the HDF5 root group (set by the
    upstream tile-builder). Accepts EPSG codes as ints/strings or full
    WKT / PROJ strings — whatever rasterio's `CRS.from_user_input`
    understands. Returns None silently if the HDF5 doesn't carry the
    attr, h5py isn't installed, or the path is missing — the GeoTIFF
    will then be written with no CRS tag (still spatially located).
    """
    if not hdf5_path:
        return None
    if not _HAVE_H5PY:
        print("  [note] h5py not installed; GeoTIFFs written without CRS.")
        return None
    if not os.path.exists(hdf5_path):
        print(f"  [note] TILE_HDF5_PATH not found ({hdf5_path}); "
              f"GeoTIFFs written without CRS.")
        return None
    with h5py.File(hdf5_path, "r") as h5f:
        raw = h5f.attrs.get("crs")
        if raw is None:
            print("  [note] HDF5 has no `crs` attribute; "
                  "GeoTIFFs written without CRS.")
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
    try:
        return CRS.from_user_input(raw)
    except Exception as e:
        print(f"  [warn] failed to parse CRS {raw!r}: {e}; "
              f"GeoTIFFs written without CRS.")
        return None


def _write_geotiffs_per_date(output_dir: str,
                             tile_id: str,
                             labels: np.ndarray,
                             target_dates: np.ndarray,
                             world_origin_x: float,
                             world_origin_y: float,
                             pixel_res: float,
                             crs: CRS | None) -> list[str]:
    """Write one LZW-compressed GeoTIFF per target date.

    Class 0 (background / no detection) is tagged as the GeoTIFF's nodata
    value so QGIS / ArcGIS render it transparent by default.

    Returns the list of paths written.
    """
    n_dates, tile_h, tile_w = labels.shape
    # rasterio's `from_origin(x, y, xres, yres)` builds an affine where
    # the upper-left pixel's NW corner is (x, y), x grows east at xres,
    # y shrinks south at yres. World origin in our metadata is the LIVE
    # NW corner of the tile, which matches that convention.
    transform = from_origin(world_origin_x, world_origin_y, pixel_res, pixel_res)

    paths: list[str] = []
    for i in range(n_dates):
        ordinal = int(target_dates[i])
        iso = _date.fromordinal(ordinal).isoformat()
        out_path = os.path.join(output_dir, f"{tile_id}_tile_{iso}.tif")
        profile = {
            "driver": "GTiff",
            "dtype": "uint8",
            "count": 1,
            "height": tile_h,
            "width": tile_w,
            "transform": transform,
            "nodata": 0,
            "compress": "LZW",
            "tiled": True,
            "blockxsize": 256,
            "blockysize": 256,
        }
        if crs is not None:
            profile["crs"] = crs
        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(labels[i], 1)
            # Tag the band with the date so gdalinfo + QGIS show it.
            dst.set_band_description(1, f"voted_labels_{iso}")
        paths.append(out_path)
    return paths


def _write_ndvi_geotiffs_per_date(output_dir: str,
                                  tile_id: str,
                                  ndvi_before: np.ndarray,
                                  ndvi_after: np.ndarray,
                                  target_dates: np.ndarray,
                                  world_origin_x: float,
                                  world_origin_y: float,
                                  pixel_res: float,
                                  crs: CRS | None) -> list[str]:
    """Write one float32 NDVI GeoTIFF per (date, side).

    Files: `{tile_id}_tile_{iso}_ndvi_before.tif` / `_ndvi_after.tif`.
    NDVI_NODATA is tagged as the GeoTIFF nodata value. Same affine/CRS as the
    label GeoTIFFs so they overlay pixel-for-pixel.
    """
    transform = from_origin(world_origin_x, world_origin_y, pixel_res, pixel_res)
    n_dates, tile_h, tile_w = ndvi_before.shape

    paths: list[str] = []
    for i in range(n_dates):
        iso = _date.fromordinal(int(target_dates[i])).isoformat()
        for side_name, arr in (("before", ndvi_before), ("after", ndvi_after)):
            out_path = os.path.join(
                output_dir, f"{tile_id}_tile_{iso}_ndvi_{side_name}.tif")
            profile = {
                "driver": "GTiff",
                "dtype": "float32",
                "count": 1,
                "height": tile_h,
                "width": tile_w,
                "transform": transform,
                "nodata": float(NDVI_NODATA),
                "compress": "LZW",
                "tiled": True,
                "blockxsize": 256,
                "blockysize": 256,
            }
            if crs is not None:
                profile["crs"] = crs
            with rasterio.open(out_path, "w", **profile) as dst:
                dst.write(arr[i], 1)
                dst.set_band_description(1, f"ndvi_{side_name}_{iso}")
            paths.append(out_path)
    return paths


def _write_block_grid(out_path: str,
                      blocks: list[dict],
                      tile_id: str,
                      block_h: int,
                      block_w: int,
                      pixel_res: float,
                      tile_origin_x: float,
                      tile_origin_y: float,
                      crs: CRS | None,
                      row_offset: int = 0,
                      col_offset: int = 0) -> int:
    """Write a debug vector layer outlining each block's LIVE extent.

    One rectangle polygon per block, so you can overlay it on the tile
    detections in QGIS and eyeball where blocks meet — handy for spotting
    aggregation seams, gaps, or a misplaced block.

    The rectangle is drawn from each block's OWN recorded world_origin (not
    just the computed grid), so a block whose origin drifted shows up as a
    rectangle that doesn't line up with its neighbours — exactly the "is
    anything strange" check. `origin_drift_m` records the offset from the
    expected grid position; `expected_*` flags whether the recorded origin
    matched the grid (within 0.5 m).

    Returns the number of block outlines written.
    """
    from shapely.geometry import box

    block_w_m = block_w * pixel_res
    block_h_m = block_h * pixel_res

    rows: list[dict] = []
    for b in blocks:
        r = int(b["block_row"])
        c = int(b["block_col"])
        ox = float(b["world_origin_x"])
        oy = float(b["world_origin_y"])
        # tile_origin is the NW-most processed block's corner; offset r/c to it.
        expected_x = tile_origin_x + (c - col_offset) * block_w_m
        expected_y = tile_origin_y - (r - row_offset) * block_h_m
        drift = float(np.hypot(ox - expected_x, oy - expected_y))
        # LIVE rectangle: NW corner (ox, oy), extends east + south.
        geom = box(ox, oy - block_h_m, ox + block_w_m, oy)
        rows.append({
            "tile_id": tile_id,
            "block_row": r,
            "block_col": c,
            "block_label": f"{r:03d}_{c:03d}",
            "world_origin_x": ox,
            "world_origin_y": oy,
            "origin_drift_m": round(drift, 3),
            "origin_ok": bool(drift <= 0.5),
            "geometry": geom,
        })

    grid_gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs=crs)
    grid_gdf.to_file(out_path, layer="block_grid", driver="GPKG")
    return len(rows)


def _write_dense_npz(out_path: str, *,
                     labels: np.ndarray, target_dates: np.ndarray,
                     classes: np.ndarray, tile_id: str,
                     world_origin_x: float, world_origin_y: float,
                     pixel_res: float, threshold: int,
                     n_block_rows: int, n_block_cols: int,
                     ndvi_before: np.ndarray | None = None,
                     ndvi_after: np.ndarray | None = None) -> None:
    extra = {}
    if ndvi_before is not None:
        extra["ndvi_before"] = ndvi_before.astype(np.float32, copy=False)
        extra["ndvi_after"] = ndvi_after.astype(np.float32, copy=False)
    np.savez_compressed(
        out_path,
        labels=labels,
        target_dates=target_dates.astype(np.int64),
        classes=classes.astype(np.uint8),
        tile_id=np.bytes_(tile_id),
        world_origin_x=np.float64(world_origin_x),
        world_origin_y=np.float64(world_origin_y),
        pixel_res=np.float64(pixel_res),
        threshold=np.uint8(threshold),
        n_block_rows=np.int64(n_block_rows),
        n_block_cols=np.int64(n_block_cols),
        **extra,
    )


def main() -> None:
    tile_id    = _required_env("TILE_ID")
    # Read from BLOCK_OUTPUT_DIR, write to FINAL_OUTPUT_DIR
    base_dir   = _required_env("OUTPUT_DIR")
    block_dir  = os.environ.get("BLOCK_OUTPUT_DIR") or base_dir
    output_dir = os.environ.get("FINAL_OUTPUT_DIR") or base_dir
    os.makedirs(output_dir, exist_ok=True)
    
    # Master-level patch-area floor (m^2)
    min_tile_patch_m2 = float(os.environ.get("MIN_TILE_PATCH_M2", "5000"))

    t_total = time.perf_counter()

    # ── Discover all block shards for this tile ───────────────────────────
    pattern = f"{tile_id}_block_*.npz"
    paths = sorted(Path(block_dir).glob(pattern))
    if not paths:
        raise SystemExit(
            f"[aggregate_tile] No shards matching {pattern} in {block_dir}"
        )
    print(f"Tile:        {tile_id}")
    print(f"Block dir:   {block_dir}")
    print(f"Output dir:  {output_dir}")
    print(f"Found {len(paths)} block shards.")

    # ── First pass: read all, validate consistency, infer grid extent ─────
    # The processed region may be a SUB-RECTANGLE of the tile (submit_tile.sh's
    # BLOCK_ROWS/BLOCK_COLS), so the grid we stitch spans the MIN..MAX of the
    # present blocks — not 0..max. Outputs are cropped to that sub-rectangle.
    blocks: list[dict] = []
    rows_seen: list[int] = []
    cols_seen: list[int] = []
    for p in paths:
        d = read_voted_block(str(p))
        blocks.append(d)
        rows_seen.append(int(d["block_row"]))
        cols_seen.append(int(d["block_col"]))

    min_row, max_row = min(rows_seen), max(rows_seen)
    min_col, max_col = min(cols_seen), max(cols_seen)

    # If submit_tile.sh selected a sub-region, cross-check it matches what we
    # actually found, so a half-finished selection fails loudly.
    def _env_int(name):
        v = os.environ.get(name)
        return int(v) if v not in (None, "") else None
    e_rlo = _env_int("PROCESS_ROW_LO"); e_rhi = _env_int("PROCESS_ROW_HI")
    e_clo = _env_int("PROCESS_COL_LO"); e_chi = _env_int("PROCESS_COL_HI")
    if None not in (e_rlo, e_rhi, e_clo, e_chi):
        if (min_row, max_row, min_col, max_col) != (e_rlo, e_rhi, e_clo, e_chi):
            raise SystemExit(
                f"[aggregate_tile] Processed blocks span rows "
                f"{min_row}-{max_row} cols {min_col}-{max_col}, but the "
                f"selection requested rows {e_rlo}-{e_rhi} cols {e_clo}-{e_chi}."
                f" Some selected blocks are missing — re-run them."
            )

    n_rows = max_row - min_row + 1
    n_cols = max_col - min_col + 1
    expected_n = n_rows * n_cols
    if len(blocks) != expected_n:
        present = {(int(b["block_row"]), int(b["block_col"])) for b in blocks}
        missing = [
            (r, c)
            for r in range(min_row, max_row + 1)
            for c in range(min_col, max_col + 1)
            if (r, c) not in present
        ]
        raise SystemExit(
            f"[aggregate_tile] Block grid is incomplete: found "
            f"{len(blocks)} shards, expected {expected_n} for the processed "
            f"rectangle rows {min_row}-{max_row} cols {min_col}-{max_col}.\n"
            f"Missing: {missing}\nRe-run those tasks before aggregating."
        )

    # Consistency: same target_dates / classes / threshold / pixel_res /
    # labels.shape across every shard.
    ref = blocks[0]
    ref_dates     = ref["target_dates"]
    ref_classes   = ref["classes"]
    ref_threshold = int(ref["threshold"])
    ref_pres      = float(ref["pixel_res"])
    ref_labels_shape = ref["labels"].shape   # (n_dates, H, W)
    for d in blocks[1:]:
        if not np.array_equal(d["target_dates"], ref_dates):
            raise SystemExit(
                f"[aggregate_tile] target_dates mismatch between shards "
                f"(saw {d['target_dates']} vs {ref_dates}). Re-run with a "
                f"consistent target-date list."
            )
        if not np.array_equal(d["classes"], ref_classes):
            raise SystemExit(
                f"[aggregate_tile] classes mismatch: {d['classes']} vs "
                f"{ref_classes}"
            )
        if int(d["threshold"]) != ref_threshold:
            raise SystemExit(
                f"[aggregate_tile] threshold mismatch: "
                f"{int(d['threshold'])} vs {ref_threshold}"
            )
        if float(d["pixel_res"]) != ref_pres:
            raise SystemExit(
                f"[aggregate_tile] pixel_res mismatch: "
                f"{float(d['pixel_res'])} vs {ref_pres}"
            )
        if d["labels"].shape != ref_labels_shape:
            raise SystemExit(
                f"[aggregate_tile] labels shape mismatch: "
                f"{d['labels'].shape} vs {ref_labels_shape}"
            )

    n_dates, block_h, block_w = ref_labels_shape
    if (block_h, block_w) != (LIVE_H, LIVE_W):
        print(f"  [note] block label shape is ({block_h}, {block_w}); "
              f"vote.py default is ({LIVE_H}, {LIVE_W}). Using the "
              f"observed values.")

    tile_h = n_rows * block_h
    tile_w = n_cols * block_w

    sub = "" if (min_row, min_col) == (0, 0) and (n_rows, n_cols) == \
        (max_row + 1, max_col + 1) else \
        f"  [sub-region rows {min_row}-{max_row} cols {min_col}-{max_col}]"
    print(f"Block grid:  {n_rows} x {n_cols}  (each block {block_h}x{block_w}){sub}")
    print(f"Tile size:   {tile_h} x {tile_w}  ({n_dates} target date(s))")
    print(f"Classes:     {ref_classes.tolist()}")
    print(f"Threshold:   {ref_threshold}")
    print(f"Pixel res:   {ref_pres} m")

    # ── Stitch ────────────────────────────────────────────────────────────
    # Origin = the NW-most processed block (min_row, min_col), not (0,0): the
    # processed region may be a sub-rectangle, and the canvas is cropped to it.
    origin_block = next(
        (b for b in blocks
         if int(b["block_row"]) == min_row and int(b["block_col"]) == min_col),
        None,
    )
    if origin_block is None:
        raise SystemExit(
            f"[aggregate_tile] No ({min_row}, {min_col}) shard found — the "
            f"NW-most processed block is required to fix the canvas world "
            f"origin. Re-run that block."
        )
    tile_origin_x = float(origin_block["world_origin_x"])
    tile_origin_y = float(origin_block["world_origin_y"])

    print(f"\nStitching {n_rows * n_cols} blocks into "
          f"({n_dates}, {tile_h}, {tile_w}) uint8...")
    t0 = time.perf_counter()
    labels = _stitch_blocks(
        blocks, n_rows, n_cols, block_h, block_w, n_dates,
        tile_origin_x, tile_origin_y, ref_pres,
        row_offset=min_row, col_offset=min_col,
    )
    print(f"  Stitch time: {time.perf_counter() - t0:.2f} s")

    # ── Optional: stitch per-pixel NDVI (present iff predict_block wrote it) ─
    # NDVI is opt-in (OUTPUT_NDVI in the predict step). Every block agrees
    # (all written by the same run), so detecting it on the reference block is
    # enough. Stitched as float32 with NDVI_NODATA fill.
    ndvi_before_tile = ndvi_after_tile = None
    has_ndvi = "ndvi_before" in ref
    if has_ndvi:
        print("\nStitching per-pixel NDVI (before/after)...")
        ndvi_before_tile = _stitch_float_key(
            blocks, "ndvi_before", n_rows, n_cols, block_h, block_w, n_dates,
            fill=float(NDVI_NODATA), row_offset=min_row, col_offset=min_col,
        )
        ndvi_after_tile = _stitch_float_key(
            blocks, "ndvi_after", n_rows, n_cols, block_h, block_w, n_dates,
            fill=float(NDVI_NODATA), row_offset=min_row, col_offset=min_col,
        )

    # ── Per-date detection summary ────────────────────────────────────────
    print("\nPer-date detections in the merged tile:")
    for i in range(n_dates):
        uniq, cnts = np.unique(labels[i], return_counts=True)
        post = {int(u): int(c) for u, c in zip(uniq, cnts) if u != 0}
        iso = _date.fromordinal(int(ref_dates[i])).isoformat()
        print(f"  {iso}: {post}")

    # ── Step A: read per-block polygons, dissolve, write vector outputs ───
    print("\nReading per-block polygons (.gpkg)...")
    t0 = time.perf_counter()
    gpkg_pattern = f"{tile_id}_block_*.gpkg"
    gpkg_paths = sorted(Path(block_dir).glob(gpkg_pattern))
    if not gpkg_paths:
        raise SystemExit(
            f"[aggregate_tile] No per-block polygons matching {gpkg_pattern} "
            f"in {block_dir}. Did predict_block write .gpkg files?"
        )
    block_gdfs = [gpd.read_file(str(p), layer="detections") for p in gpkg_paths]
    # Concatenate; preserve CRS from the first non-empty frame.
    block_gdf = gpd.GeoDataFrame(
        pd.concat(block_gdfs, ignore_index=True),
        geometry="geometry",
    )
    src_crs = next((g.crs for g in block_gdfs if g.crs is not None), None)
    block_gdf.set_crs(src_crs, inplace=True, allow_override=True)
    print(f"  Read {len(gpkg_paths)} block .gpkg ({len(block_gdf)} polygons) "
          f"in {time.perf_counter() - t0:.2f} s")

    print("\nDissolving boundary-straddling patches per (date, class)...")
    print(f"  Master patch-area floor: {min_tile_patch_m2} m^2 (post-merge)")
    t0 = time.perf_counter()
    tile_gdf = _dissolve_block_polygons(
        block_gdf, tile_id, ref_pres, min_area_m2=min_tile_patch_m2,
    )
    print(f"  {len(block_gdf)} block polygons -> {len(tile_gdf)} merged "
          f"patches in {time.perf_counter() - t0:.2f} s")
    if len(tile_gdf) > 0:
        per_class = tile_gdf.groupby(["class_id", "date_iso"])["n_pixels"].agg(
            ["count", "sum"]
        )
        print("  By (class, date) — count = n_patches, sum = total pixels:")
        print(per_class.to_string())

    # Stamp the tile CRS (from the source HDF5) if the block files lacked one.
    crs = _read_hdf5_crs(os.environ.get("TILE_HDF5_PATH"))
    if tile_gdf.crs is None and crs is not None:
        tile_gdf.set_crs(crs, inplace=True, allow_override=True)

    # ── Optional: clip the dissolved polygons to a mask .gpkg ─────────────
    # CLIP_MASK_GPKG (unset = no clip) restricts the final map to a region of
    # interest: polygons outside the mask are dropped, ones straddling its edge
    # are cut to the inside. Done after the cross-block dissolve so patches are
    # whole before clipping, and after the CRS stamp so the mask can reproject.
    clip_mask = os.environ.get("CLIP_MASK_GPKG")
    if clip_mask:
        if not Path(clip_mask).is_file():
            raise SystemExit(
                f"[aggregate_tile] CLIP_MASK_GPKG not found: {clip_mask}"
            )
        print(f"\nClipping to mask: {clip_mask}")
        t0 = time.perf_counter()
        n_before = len(tile_gdf)
        tile_gdf = _clip_to_mask(tile_gdf, clip_mask, ref_pres)
        print(f"  {n_before} -> {len(tile_gdf)} patches after clip "
              f"in {time.perf_counter() - t0:.2f} s")

    gpkg_path = os.path.join(output_dir, f"{tile_id}_tile.gpkg")
    parquet_path = os.path.join(output_dir, f"{tile_id}_tile.parquet")
    t0 = time.perf_counter()
    tile_gdf.to_file(gpkg_path, layer="detections", driver="GPKG")
    tile_gdf.to_parquet(parquet_path)
    print(f"\nWrote {gpkg_path}")
    print(f"      {parquet_path}")
    print(f"  ({len(tile_gdf)} patches in {time.perf_counter() - t0:.2f} s)")

    # ── Step B: dense .npz (auxiliary output) ─────────────────────────────
    npz_path = os.path.join(output_dir, f"{tile_id}_tile.npz")
    t0 = time.perf_counter()
    _write_dense_npz(
        npz_path,
        labels=labels, target_dates=ref_dates, classes=ref_classes,
        tile_id=tile_id,
        world_origin_x=tile_origin_x, world_origin_y=tile_origin_y,
        pixel_res=ref_pres, threshold=ref_threshold,
        n_block_rows=n_rows, n_block_cols=n_cols,
        ndvi_before=ndvi_before_tile, ndvi_after=ndvi_after_tile,
    )
    write_npz_s = time.perf_counter() - t0
    npz_bytes = os.path.getsize(npz_path)
    print(f"Wrote {npz_path}")
    print(f"  ({npz_bytes / 1024:.1f} KB in {write_npz_s:.2f} s)")

    # ── Step C: per-date GeoTIFFs (GIS-ready output) ──────────────────────
    print(f"\nWriting per-date GeoTIFFs...")
    crs = _read_hdf5_crs(os.environ.get("TILE_HDF5_PATH"))
    if crs is not None:
        print(f"  CRS:  {crs}")
    t0 = time.perf_counter()
    tif_paths = _write_geotiffs_per_date(
        output_dir, tile_id, labels, ref_dates,
        tile_origin_x, tile_origin_y, ref_pres, crs,
    )
    write_tif_s = time.perf_counter() - t0
    total_tif_bytes = sum(os.path.getsize(p) for p in tif_paths)
    print(f"  Wrote {len(tif_paths)} GeoTIFF(s) in {write_tif_s:.2f} s "
          f"(total {total_tif_bytes / 1024:.1f} KB):")
    for p in tif_paths:
        print(f"    {p}  ({os.path.getsize(p) / 1024:.1f} KB)")

    # Per-date NDVI GeoTIFFs (before/after), only when NDVI was produced.
    if has_ndvi:
        print(f"\nWriting per-date NDVI GeoTIFFs...")
        t0 = time.perf_counter()
        ndvi_paths = _write_ndvi_geotiffs_per_date(
            output_dir, tile_id, ndvi_before_tile, ndvi_after_tile, ref_dates,
            tile_origin_x, tile_origin_y, ref_pres, crs,
        )
        print(f"  Wrote {len(ndvi_paths)} NDVI GeoTIFF(s) in "
              f"{time.perf_counter() - t0:.2f} s")

    # ── Step D: block-grid outline (debug overlay) ────────────────────────
    # One rectangle per block's LIVE extent so you can overlay it on the
    # detections in QGIS and check where blocks meet / spot a misplaced block.
    # Note: NOT named "*_block_*" — that pattern is the per-block-polygon glob
    # (f"{tile_id}_block_*.gpkg"); a name collision would make a rerun read
    # this debug layer as if it were block detections.
    grid_path = os.path.join(output_dir, f"{tile_id}_blockgrid.gpkg")
    n_grid = _write_block_grid(
        grid_path, blocks, tile_id, block_h, block_w, ref_pres,
        tile_origin_x, tile_origin_y, crs,
        row_offset=min_row, col_offset=min_col,
    )
    n_drift = sum(1 for b in blocks
                  if np.hypot(
                      float(b["world_origin_x"])
                      - (tile_origin_x + (int(b["block_col"]) - min_col) * block_w * ref_pres),
                      float(b["world_origin_y"])
                      - (tile_origin_y - (int(b["block_row"]) - min_row) * block_h * ref_pres),
                  ) > 0.5)
    print(f"\nWrote {grid_path}")
    print(f"  ({n_grid} block outlines, layer 'block_grid'"
          + (f"; {n_drift} with origin drift > 0.5 m — check those seams"
             if n_drift else "; all origins on-grid") + ")")

    print(f"\nTotal aggregate time: {time.perf_counter() - t_total:.2f} s")


if __name__ == "__main__":
    main()
