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
    A `confidence` column (mean change-confidence 0–100 per patch) is added
    when the predict step ran with OUTPUT_CONFIDENCE=1.
  - `{TILE_ID}_tile.parquet` (PRIMARY, analysis) — the same patches as
    GeoParquet for fast pandas/geopandas reads.
  - `{TILE_ID}_tile.npz` (AUXILIARY) — the dense `(n_dates, TILE_H, TILE_W)`
    uint8 label map (blocks stitched into one canvas) + metadata.
  - `{TILE_ID}_tile_{YYYY-MM-DD}.tif` (GIS raster) — one LZW GeoTIFF per
    date, class 0 as NoData.
  - `{TILE_ID}_block_grid.gpkg` (DEBUG) — one rectangle per block's LIVE
    extent (layer `block_grid`), drawn from each block's recorded
    world_origin. Overlay on the detections in QGIS to inspect block seams;
    `origin_drift_m` / `origin_ok` flag any block whose origin left the grid.

Boundary merge: LIVE areas tile with no gap, so a patch crossing a block
seam yields two edge-touching polygons in adjacent blocks. unary_union
welds touching/overlapping polygons, so the dissolved result is one patch
— no ghost output or overlap dedup needed.
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
sys.path.insert(0, str(_HERE.parent))   # processes/ (for `postprocess` package)
sys.path.insert(0, str(_HERE))          # tile_postprocess/

from block_postprocess.voted_output import read_voted_block
from block_postprocess.vote import LIVE_H, LIVE_W


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


def _check_block_origins(blocks: list[dict], block_h: int, block_w: int,
                         tile_origin_x: float, tile_origin_y: float,
                         pixel_res: float,
                         row_offset: int = 0, col_offset: int = 0) -> None:
    """Warn for any block whose recorded world origin drifts > 0.5 m off-grid.

    Pulled out of stitching so the warning fires once (not once per date) now
    that the tile is stitched one date at a time.
    """
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


def _stitch_one_date(blocks: list[dict], date_idx: int,
                     n_rows: int, n_cols: int, block_h: int, block_w: int,
                     row_offset: int = 0, col_offset: int = 0) -> np.ndarray:
    """Stitch ONE date's label slice into a single (tile_h, tile_w) canvas.

    Builds a 2-D canvas for `date_idx` only, placing each block's
    `labels[date_idx]` at its grid cell. Stitching one date at a time (rather
    than the whole `(n_dates, tile_h, tile_w)` cube at once) is what keeps the
    aggregator's peak memory flat in the number of dates: a single date's
    canvas is `tile_h * tile_w` bytes (~127 MB at 11264²) regardless of how
    many target dates the run spans. This matters for multi-year runs (dozens
    of dates) where the full cube was several GiB and drove the OOM.

    `row_offset`/`col_offset` map a block at grid (r, c) to canvas cell
    (r - row_offset, c - col_offset) for sub-rectangle runs (0 for full tile).
    Does NOT free `b["labels"]` — the caller reuses every block across all
    date indices, then frees them after the date loop.
    """
    canvas = np.zeros((n_rows * block_h, n_cols * block_w), dtype=np.uint8)
    for b in blocks:
        r = int(b["block_row"]) - row_offset
        c = int(b["block_col"]) - col_offset
        y0 = r * block_h
        x0 = c * block_w
        canvas[y0:y0 + block_h, x0:x0 + block_w] = b["labels"][date_idx]
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
    # Carry confidence through only if the block polygons have it (i.e. the
    # predict step ran with OUTPUT_CONFIDENCE=1). When present, a merged patch's
    # confidence is the n_pixels-weighted mean of the source block patches that
    # compose it — so a large high-confidence patch isn't pulled down equally by
    # a tiny low-confidence sliver welded on at a block seam.
    has_conf = "confidence" in block_gdf.columns
    out_columns = _VECTOR_COLUMNS + (["confidence"] if has_conf else [])

    if len(block_gdf) == 0:
        return gpd.GeoDataFrame(columns=out_columns, geometry="geometry",
                                crs=crs)

    rows: list[dict] = []
    # Group by (date, class); within each, weld touching/overlapping polys.
    for (ordinal, cls), grp in block_gdf.groupby(["date_ordinal", "class_id"]):
        merged = unary_union(list(grp.geometry))
        # unary_union returns a Polygon or MultiPolygon; normalise to a list.
        geoms = list(merged.geoms) if merged.geom_type == "MultiPolygon" \
            else [merged]
        iso = _date.fromordinal(int(ordinal)).isoformat()
        # Source patches (with their confidence + n_pixels) for the weighted
        # average; computed once per group, reused per exploded geom.
        src = list(grp.itertuples(index=False)) if has_conf else None
        for geom in geoms:
            if geom.is_empty:
                continue
            area_m2 = float(geom.area)
            if area_m2 < min_area_m2:
                continue
            centroid = geom.centroid
            row = {
                "tile_id": tile_id,
                "date_ordinal": int(ordinal),
                "date_iso": iso,
                "class_id": int(cls),
                "n_pixels": int(round(area_m2 / px_area)),
                "area_m2": area_m2,
                "centroid_x": float(centroid.x),
                "centroid_y": float(centroid.y),
                "geometry": geom,
            }
            if has_conf:
                # n_pixels-weighted mean confidence over the source patches that
                # fall inside this merged geom (a source patch belongs to the
                # merged geom that contains its representative point).
                num = den = 0.0
                for s in src:
                    c = getattr(s, "confidence", None)
                    if c is None or (isinstance(c, float) and np.isnan(c)):
                        continue
                    if geom.intersects(s.geometry.representative_point()):
                        w = float(s.n_pixels)
                        num += float(c) * w
                        den += w
                row["confidence"] = (int(round(num / den)) if den > 0 else None)
            rows.append(row)

    if not rows:
        return gpd.GeoDataFrame(columns=out_columns, geometry="geometry",
                                crs=crs)
    out = gpd.GeoDataFrame(rows, geometry="geometry", crs=crs)
    return out[out_columns]


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


def _write_one_geotiff(output_dir: str,
                       tile_id: str,
                       date_canvas: np.ndarray,
                       ordinal: int,
                       world_origin_x: float,
                       world_origin_y: float,
                       pixel_res: float,
                       crs: CRS | None) -> str:
    """Write one date's (tile_h, tile_w) label canvas as an LZW GeoTIFF.

    Class 0 (background / no detection) is tagged as the GeoTIFF's nodata
    value so QGIS / ArcGIS render it transparent by default. Called once per
    date from the per-date loop so the full label cube never has to exist.
    Returns the path written.
    """
    tile_h, tile_w = date_canvas.shape
    iso = _date.fromordinal(int(ordinal)).isoformat()
    # rasterio's `from_origin(x, y, xres, yres)` builds an affine where the
    # upper-left pixel's NW corner is (x, y), x grows east at xres, y shrinks
    # south at yres — matching our LIVE-NW-corner world origin.
    transform = from_origin(world_origin_x, world_origin_y, pixel_res, pixel_res)
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
        dst.write(date_canvas, 1)
        dst.set_band_description(1, f"voted_labels_{iso}")
    return out_path


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
                     n_block_rows: int, n_block_cols: int) -> None:
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

    # Dense tile .npz ({TILE_ID}_tile.npz): the full (n_dates, tile_h, tile_w)
    # label cube. OFF by default — for a full-country tile that cube is several
    # GiB, and building it forces the whole stack into memory at once, which was
    # the aggregator's OOM driver. The per-date GeoTIFFs carry the same label
    # data in a GIS-ready, memory-friendly form, and nothing in the pipeline
    # reads this .npz back. Set WRITE_DENSE_NPZ=1 to opt in (e.g. for a small
    # tile where a single dense array is convenient for analysis); doing so
    # restores the multi-GiB peak, so size the aggregator's memory accordingly.
    write_dense_npz = (os.environ.get("WRITE_DENSE_NPZ", "0")
                       not in ("0", "", "false", "False"))

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

    # One-time origin drift check (was previously done inside the stitch, which
    # now runs per date — so do it once here instead of n_dates times).
    _check_block_origins(
        blocks, block_h, block_w, tile_origin_x, tile_origin_y, ref_pres,
        row_offset=min_row, col_offset=min_col,
    )

    # ── Step A: read per-block polygons, dissolve, write vector outputs ───
    # Done before the label stitch/TIF loop: the polygon GeoDataFrames are the
    # other large transient, and there's no reason for them to coexist with the
    # label canvases.
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
    del block_gdfs   # release the per-block frames; concat holds the data now

    print("\nDissolving boundary-straddling patches per (date, class)...")
    print(f"  Master patch-area floor: {min_tile_patch_m2} m^2 (post-merge)")
    t0 = time.perf_counter()
    tile_gdf = _dissolve_block_polygons(
        block_gdf, tile_id, ref_pres, min_area_m2=min_tile_patch_m2,
    )
    print(f"  {len(block_gdf)} block polygons -> {len(tile_gdf)} merged "
          f"patches in {time.perf_counter() - t0:.2f} s")
    del block_gdf    # the merged tile_gdf is all we need from here
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

    gpkg_path = os.path.join(output_dir, f"{tile_id}_tile.gpkg")
    parquet_path = os.path.join(output_dir, f"{tile_id}_tile.parquet")
    t0 = time.perf_counter()
    tile_gdf.to_file(gpkg_path, layer="detections", driver="GPKG")
    tile_gdf.to_parquet(parquet_path)
    print(f"\nWrote {gpkg_path}")
    print(f"      {parquet_path}")
    print(f"  ({len(tile_gdf)} patches in {time.perf_counter() - t0:.2f} s)")
    del tile_gdf

    # ── Step B: per-date stitch + GeoTIFF (+ optional dense .npz) ──────────
    # Stitch ONE date at a time into a (tile_h, tile_w) canvas, print its
    # detection summary, and write its GeoTIFF — so the full (n_dates, tile_h,
    # tile_w) cube never exists. Peak label memory is the scattered per-block
    # arrays (freed after this loop) plus one date's canvas, flat in n_dates.
    crs = _read_hdf5_crs(os.environ.get("TILE_HDF5_PATH"))
    print(f"\nStitching + writing {n_dates} per-date GeoTIFF(s) "
          f"({tile_h} x {tile_w} each)...")
    if crs is not None:
        print(f"  CRS:  {crs}")
    if write_dense_npz:
        print(f"  WRITE_DENSE_NPZ=1: also accumulating the dense "
              f"({n_dates}, {tile_h}, {tile_w}) cube for {tile_id}_tile.npz "
              f"— this restores the multi-GiB memory peak.")

    # Per-date detection summary (printed inline as we stitch each date).
    print("\nPer-date detections in the merged tile:")
    t0 = time.perf_counter()
    tif_paths: list[str] = []
    # Only allocate the dense cube when the user opted in.
    dense_labels = (np.zeros((n_dates, tile_h, tile_w), dtype=np.uint8)
                    if write_dense_npz else None)
    for i in range(n_dates):
        date_canvas = _stitch_one_date(
            blocks, i, n_rows, n_cols, block_h, block_w,
            row_offset=min_row, col_offset=min_col,
        )
        uniq, cnts = np.unique(date_canvas, return_counts=True)
        post = {int(u): int(c) for u, c in zip(uniq, cnts) if u != 0}
        iso = _date.fromordinal(int(ref_dates[i])).isoformat()
        print(f"  {iso}: {post}")
        if dense_labels is not None:
            dense_labels[i] = date_canvas
        tif_paths.append(_write_one_geotiff(
            output_dir, tile_id, date_canvas, int(ref_dates[i]),
            tile_origin_x, tile_origin_y, ref_pres, crs,
        ))
    write_tif_s = time.perf_counter() - t0
    # Per-block labels are fully consumed now; free them before the block-grid
    # step (which only needs metadata).
    for b in blocks:
        b.pop("labels", None)
    total_tif_bytes = sum(os.path.getsize(p) for p in tif_paths)
    print(f"\n  Wrote {len(tif_paths)} GeoTIFF(s) in {write_tif_s:.2f} s "
          f"(total {total_tif_bytes / 1024:.1f} KB)")

    # ── Optional dense .npz (auxiliary; off by default — see WRITE_DENSE_NPZ) ─
    if dense_labels is not None:
        npz_path = os.path.join(output_dir, f"{tile_id}_tile.npz")
        t0 = time.perf_counter()
        _write_dense_npz(
            npz_path,
            labels=dense_labels, target_dates=ref_dates, classes=ref_classes,
            tile_id=tile_id,
            world_origin_x=tile_origin_x, world_origin_y=tile_origin_y,
            pixel_res=ref_pres, threshold=ref_threshold,
            n_block_rows=n_rows, n_block_cols=n_cols,
        )
        npz_bytes = os.path.getsize(npz_path)
        print(f"Wrote {npz_path}")
        print(f"  ({npz_bytes / 1024:.1f} KB in {time.perf_counter() - t0:.2f} s)")
        del dense_labels

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
    # Emit this process's peak RSS so the wrapper can report aggregator memory in
    # the run summary. sacct can't: the summary is written by the aggregator job
    # while it's still running, so its own MaxRSS isn't flushed yet. ru_maxrss is
    # the kernel's peak RSS for this process — in KiB on Linux (the cluster). The
    # wrapper greps this exact "AGGREGATOR_PEAK_KB=" marker line.
    import resource
    print(f"AGGREGATOR_PEAK_KB={resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}")
