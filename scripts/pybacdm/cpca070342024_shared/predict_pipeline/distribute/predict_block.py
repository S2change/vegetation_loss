"""Per-block predict driver for tile-wide distribution.

One invocation processes exactly one (block_row, block_col) of one tile's
chip-chunked HDF5 and writes one voted-output .npz. Designed to run as
an array task — `submit_tile.sh` maps `$SLURM_ARRAY_TASK_ID` to a
(block_row, block_col) pair via the tile's block grid shape and exports
both as env vars before invoking this script.

Parameters are passed via environment variables (so the SLURM script can
set them without re-templating Python source):

  Required
    TILE_HDF5_PATH    Path to the chip-chunked HDF5 for one tile.
    WEIGHTS_PATH      Path to the BACDM .pth checkpoint.
    OUTPUT_DIR        Base run directory (used as a fallback output location).
    TILE_ID           Tile name (e.g. T29TPG). Used in the .npz filename.
  Optional
    BLOCK_OUTPUT_DIR  Where to write the per-block .npz + .gpkg. Defaults to
                      OUTPUT_DIR (submit_tile.sh sets it to
                      OUTPUT_DIR/block_outputs).
    BLOCK_ROW         Block row index (0..N_BLOCK_ROWS-1).
    BLOCK_COL         Block col index (0..N_BLOCK_COLS-1).
    TARGET_DATES      Comma-separated YYYY-MM-DD list, e.g.
                      "2025-11-15,2025-12-01".

  Optional
    BATCH_SIZE         Model batch size (default 8).
    VOTE_CLASSES       Comma-separated non-bg class IDs (default "1,2").
    VOTE_THRESHOLD     Min votes per pixel to keep a detection (default 2).

Everything else (model architecture, ghost geometry, etc.) is fixed by
the modules being imported.
"""
import os
import sys
import time
from collections import Counter
from datetime import date
from pathlib import Path

import numpy as np
import psutil
import torch

# Make the bacdm/ subpackage importable. bacdm/ sits next to distribute/
# under <shared>/, and its modules use `from bacdm.X import ...` internally,
# so we put <shared>/ (the parent of bacdm/) on the path.
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))                          # shared/ (for bacdm.*)

from input_setup import read_block, get_block_grid_shape
from composite_shift_chips import (
    create_before_after_composites,
    generate_shifted_chips,
)
# Imported from the submodule (not the package __init__) so rasterio stays off
# the core composite import path; only pulled in when actually writing TIFs.
from composite_shift_chips.write_composite_tifs import write_block_composite_tifs
from postprocess import (
    chip_nw_pixel_offset,
    VoteAccumulator,
    write_voted_block,
)
from polygonize import (
    labels_to_polygons, polygons_to_records, close_labels,
)
from bacdm.predict import load_model, predict_before_after_chips


def rss_mb() -> float:
    return psutil.Process().memory_info().rss / 1e6


# ============================================================================
# CONFIG (from env)
# ============================================================================

def _required_env(name: str) -> str:
    v = os.environ.get(name)
    if v is None or v == "":
        raise SystemExit(f"[predict_block] Missing required env var: {name}")
    return v


def _int_env(name: str) -> int:
    return int(_required_env(name))


def _classes_env(default: str = "1,2") -> tuple[int, ...]:
    raw = os.environ.get("VOTE_CLASSES", default)
    return tuple(int(c.strip()) for c in raw.split(",") if c.strip())


def _dates_env() -> np.ndarray:
    raw = _required_env("TARGET_DATES")
    items = [s.strip() for s in raw.split(",") if s.strip()]
    return np.array(
        [date.fromisoformat(s).toordinal() for s in items],
        dtype=np.int64,
    )


# ============================================================================
# MAIN
# ============================================================================

def main() -> None:
    hdf5_path     = _required_env("TILE_HDF5_PATH")
    weights_path  = _required_env("WEIGHTS_PATH")
    # Per-block .npz/.gpkg go to BLOCK_OUTPUT_DIR (submit_tile.sh sets this to
    # OUTPUT_DIR/block_outputs); fall back to OUTPUT_DIR for standalone runs.
    output_dir    = os.environ.get("BLOCK_OUTPUT_DIR") or _required_env("OUTPUT_DIR")
    tile_id       = _required_env("TILE_ID")
    block_row     = _int_env("BLOCK_ROW")
    block_col     = _int_env("BLOCK_COL")
    target_dates  = _dates_env()
    batch_size    = int(os.environ.get("BATCH_SIZE", "8"))
    vote_classes  = _classes_env()
    vote_threshold = int(os.environ.get("VOTE_THRESHOLD", "2"))
    # Symmetric day-window around the break date for before/after compositing.
    # Unset/empty = unbounded (use any timestep before/after the target).
    _mcd_env = os.environ.get("MAX_COMPOSITE_DAYS")
    max_composite_days = (int(_mcd_env)
                          if _mcd_env not in (None, "") else None)
    # Post-vote morphological close radius (disk). Per-class radii come from
    # AAA_Configs.CLOSING_RADII (Cuts → 3, Fires → 1); leaving CLOSING_RADIUS
    # unset uses those. Setting CLOSING_RADIUS forces one radius for every
    # class (override); CLOSING_RADIUS=0 disables closing entirely.
    _cr_env = os.environ.get("CLOSING_RADIUS")
    closing_radius = int(_cr_env) if _cr_env is not None else None
    # Block-level patch-area floor (m^2). Dropped at this stage; the master
    # applies a second, larger floor after cross-block merge.
    min_patch_m2 = float(os.environ.get("MIN_PATCH_M2", "2500"))

    # Bounds check the block coordinates against the HDF5's grid shape so
    # a misconfigured array index fails fast with a clear message instead
    # of read_block raising an opaque slice-bounds error.
    n_rows, n_cols = get_block_grid_shape(hdf5_path)
    if not (0 <= block_row < n_rows and 0 <= block_col < n_cols):
        raise SystemExit(
            f"[predict_block] block=({block_row}, {block_col}) is out of "
            f"range for grid shape ({n_rows}, {n_cols}) of {hdf5_path}"
        )

    os.makedirs(output_dir, exist_ok=True)

    print(f"Tile:           {tile_id}")
    print(f"HDF5:           {hdf5_path}")
    print(f"Block:          ({block_row}, {block_col}) of grid "
          f"({n_rows}, {n_cols})")
    print(f"Weights:        {weights_path}")
    print(f"Output dir:     {output_dir}")
    print(f"Target dates:   {[date.fromordinal(int(d)).isoformat() for d in target_dates]}")
    print(f"Max comp. days: {max_composite_days if max_composite_days is not None else 'unbounded'}")
    print(f"Batch size:     {batch_size}")
    print(f"Vote classes:   {vote_classes}")
    print(f"Vote threshold: {vote_threshold}")
    print(f"Closing radius: "
          f"{'per-class (AAA_Configs.CLOSING_RADII)' if closing_radius is None else closing_radius}")
    print(f"Min patch m^2:  {min_patch_m2}")
    print(f"\n[RSS] After imports:                   {rss_mb():7.1f} MB")

    # ── Step 1: read chip block ───────────────────────────────────────────
    print(f"\nStep 1: reading chip-block from HDF5...")
    t0 = time.perf_counter()
    block, ts, position = read_block(hdf5_path, block_row, block_col)
    print(f"  block: shape={block.shape}  dtype={block.dtype}  "
          f"{block.nbytes / 1e6:.1f} MB")
    print(f"  ts:    {date.fromordinal(int(ts[0]))} -> "
          f"{date.fromordinal(int(ts[-1]))}  ({len(ts)} timesteps)")
    print(f"  Step 1 time: {time.perf_counter() - t0:.2f} s")
    print(f"[RSS] After chip-block:                {rss_mb():7.1f} MB")

    # ── Step 3: per-pixel before/after compositing ────────────────────────
    print(f"\nStep 3: compositing for {len(target_dates)} target date(s)...")
    t0 = time.perf_counter()
    composites, valid_dates_mask = create_before_after_composites(
        block, ts, target_dates, verbose=True,
        max_days_from_break=max_composite_days,
    )
    n_valid = int(valid_dates_mask.sum())
    print(f"  composites: shape={composites.shape}  dtype={composites.dtype}  "
          f"{composites.nbytes / 1e6:.1f} MB")
    print(f"  valid dates: {n_valid} / {len(target_dates)}")
    print(f"  Step 3 time: {time.perf_counter() - t0:.2f} s")
    print(f"[RSS] After composites:                {rss_mb():7.1f} MB")

    # ── Optional: dump before/after composite GeoTIFFs for inspection ──────
    # Gated by WRITE_COMPOSITE_TIFS (default off) — these are 10-band 1280x1280
    # rasters per (date, side), not needed for the production pipeline. Written
    # to a dedicated composite_tifs/ dir so they never collide with the
    # aggregator's block_outputs glob.
    if os.environ.get("WRITE_COMPOSITE_TIFS", "0") not in ("0", "", "false", "False"):
        comp_dir = os.environ.get(
            "COMPOSITE_TIF_DIR",
            os.path.join(os.environ.get("OUTPUT_DIR", output_dir),
                         "composite_tifs"),
        )
        print(f"\nWriting composite GeoTIFFs -> {comp_dir} ...")
        t0 = time.perf_counter()
        comp_paths = write_block_composite_tifs(
            composites, target_dates, valid_dates_mask,
            out_dir=comp_dir, tile_id=tile_id,
            block_row=block_row, block_col=block_col,
            world_origin_x=position.world_origin_x,
            world_origin_y=position.world_origin_y,
            pixel_res=position.pixel_res,
            crs=_read_hdf5_crs(hdf5_path),
        )
        print(f"  wrote {len(comp_paths)} composite TIF(s) "
              f"in {time.perf_counter() - t0:.2f} s")

    if n_valid == 0:
        # Empty output is still useful: aggregator can detect missing/empty
        # blocks. Write a .npz with zero-filled labels for any dates the
        # user asked for so the file shape stays predictable, plus an empty
        # .gpkg so every block has both outputs (keeps the aggregator's
        # complete-grid check symmetric).
        print("\n  No valid target dates for this block. Writing empty .npz "
              "+ empty .gpkg and exiting.")
        labels = np.zeros(
            (len(target_dates),
             1024, 1024),  # default LIVE size — postprocess.vote.LIVE_H/W
            dtype=np.uint8,
        )
        write_voted_block(
            output_dir, tile_id, block_row, block_col,
            labels=labels,
            target_dates=target_dates.astype(np.int64),
            classes=tuple(int(c) for c in vote_classes),
            world_origin_x=position.world_origin_x,
            world_origin_y=position.world_origin_y,
            pixel_res=position.pixel_res,
            threshold=vote_threshold,
        )
        _write_empty_block_gpkg(
            output_dir, tile_id, block_row, block_col,
            crs=_read_hdf5_crs(hdf5_path),
        )
        return

    # ── Load model ────────────────────────────────────────────────────────
    # Pin PyTorch's intra-op thread pool. Precedence: explicit THREADS env
    # (used by the thread-sweep experiment and to tune in production) >
    # SLURM_CPUS_PER_TASK > 1. The SLURM wrapper also exports OMP/MKL/
    # OpenBLAS caps before Python starts (those size at import time); this is
    # the matching torch-side cap and covers running outside the wrapper.
    n_threads = int(os.environ.get("THREADS",
                                   os.environ.get("SLURM_CPUS_PER_TASK", "1")))
    torch.set_num_threads(n_threads)
    print(f"\ntorch threads:  {torch.get_num_threads()}")

    print("\nLoading model...")
    t0 = time.perf_counter()
    model = load_model(weights_path)
    print(f"  Loaded in {time.perf_counter() - t0:.2f} s")
    print(f"[RSS] After model loaded:              {rss_mb():7.1f} MB")

    # ── Step 4: generate shifted chips + predict; stream votes ────────────
    print(f"\nStep 4: generating shifted chips + predicting...")

    rss_before_infer = rss_mb()
    t_inference_total = 0.0
    t_vote_total = 0.0
    n_pairs = 0
    class_counts: Counter[int] = Counter()
    kind_counts: Counter[str] = Counter()

    voters: dict[int, VoteAccumulator] = {
        int(d): VoteAccumulator(classes=vote_classes)
        for d in target_dates[valid_dates_mask]
    }

    def vote_one(label_map: np.ndarray, chip_kind: str,
                 grid_position: tuple[int, int], date_ordinal: int) -> None:
        nonlocal t_vote_total
        gr, gc = grid_position
        nw_y, nw_x = chip_nw_pixel_offset(chip_kind, gr, gc)
        t0 = time.perf_counter()
        voters[date_ordinal].add(label_map, nw_y, nw_x)
        t_vote_total += time.perf_counter() - t0

    pair_iter = generate_shifted_chips(
        composites, target_dates, valid_dates_mask, verbose=True,
    )
    batch: list = []

    def flush(batch: list) -> None:
        nonlocal t_inference_total, n_pairs
        if not batch:
            return
        before = np.stack([p.before.transpose(1, 2, 0) for p in batch])
        after  = np.stack([p.after.transpose(1, 2, 0)  for p in batch])
        t0 = time.perf_counter()
        labels = predict_before_after_chips(before, after, model)
        t_inference_total += time.perf_counter() - t0
        n_pairs += len(batch)
        for p, label in zip(batch, labels):
            kind_counts[p.chip_kind] += 1
            uniq, cnts = np.unique(label, return_counts=True)
            for u, c in zip(uniq, cnts):
                class_counts[int(u)] += int(c)
            vote_one(label, p.chip_kind, p.grid_position, p.date_ordinal)
        batch.clear()

    for pair in pair_iter:
        batch.append(pair)
        if len(batch) >= batch_size:
            flush(batch)
    flush(batch)

    rss_after_infer = rss_mb()
    print(f"\n  Pairs predicted:   {n_pairs}")
    print(f"  By chip kind:      {dict(sorted(kind_counts.items()))}")
    print(f"  Total infer time:  {t_inference_total:.2f} s  "
          f"({t_inference_total / max(n_pairs, 1) * 1000:.1f} ms/chip)")
    print(f"  Total vote time:   {t_vote_total:.2f} s")
    print(f"[RSS] After all inference:             {rss_after_infer:7.1f} MB  "
          f"(delta {rss_after_infer - rss_before_infer:+6.1f} MB)")

    total_pixels = sum(class_counts.values())
    print("\nPer-class pixel counts (aggregated across all chip pairs):")
    for cls in sorted(class_counts):
        cnt = class_counts[cls]
        print(f"  class {cls}: {cnt:>12,} pixels  "
              f"({100 * cnt / max(total_pixels, 1):5.2f}%)")

    # ── Step 5b + 6: finalize voted labels, write .npz ────────────────────
    # The voted .npz only carries dates that were valid — but downstream
    # tile aggregation expects one labels slice per requested target date.
    # Resolution: write per-target-date labels, filling invalid dates with
    # zeros (matching the early-exit path above). Same shape regardless
    # of which dates had data.
    voted_labels = np.zeros(
        (len(target_dates),
         voters[next(iter(voters))].live_h,
         voters[next(iter(voters))].live_w),
        dtype=np.uint8,
    )
    print(f"\nStep 5b: voting (threshold={vote_threshold}, classes={vote_classes})")
    for i, d in enumerate(target_dates):
        ordinal = int(d)
        if ordinal in voters:
            acc = voters[ordinal]
            n_votes = acc.n_votes_by_class()
            voted_labels[i] = acc.finalize(threshold=vote_threshold)
            uniq, cnts = np.unique(voted_labels[i], return_counts=True)
            post = {int(u): int(c) for u, c in zip(uniq, cnts) if u != 0}
            iso = date.fromordinal(ordinal).isoformat()
            print(f"  {iso}: pre-threshold votes={n_votes}  "
                  f"post-threshold detections={post}")
        else:
            iso = date.fromordinal(ordinal).isoformat()
            print(f"  {iso}: skipped (no valid pre/post timesteps); "
                  f"writing zeros")

    print(f"\nStep 6: writing voted .npz...")
    t0 = time.perf_counter()
    npz_path = write_voted_block(
        output_dir, tile_id,
        position.block_row, position.block_col,
        labels=voted_labels,
        target_dates=target_dates.astype(np.int64),
        classes=tuple(int(c) for c in vote_classes),
        world_origin_x=position.world_origin_x,
        world_origin_y=position.world_origin_y,
        pixel_res=position.pixel_res,
        threshold=vote_threshold,
    )
    write_s = time.perf_counter() - t0
    npz_bytes = os.path.getsize(npz_path)
    print(f"  Wrote {npz_path} in {write_s:.2f} s  "
          f"({npz_bytes / 1024:.1f} KB)")

    # ── Step 6b: close + polygonize voted labels -> per-block GeoPackage ──
    # Post-vote morphological close (per class) smooths vote-boundary
    # roughness on the voted block result, then polygonize each date into
    # one polygon per connected patch of each class, in world (UTM) coords,
    # dropping patches below the block-level area floor. The tile aggregator
    # dissolves edge-adjacent polygons across block boundaries and applies
    # a second, larger floor. Stamped with the HDF5's CRS so each .gpkg is
    # self-contained.
    print(f"\nStep 6b: closing + polygonizing voted labels...")
    t0 = time.perf_counter()
    crs = _read_hdf5_crs(hdf5_path)
    classes_t = tuple(int(c) for c in vote_classes)
    rows: list = []
    for i, d in enumerate(target_dates):
        closed = (voted_labels[i]
                  if closing_radius == 0
                  else close_labels(voted_labels[i], classes_t,
                                    closing_radius=closing_radius))
        patches = labels_to_polygons(
            closed, date_ordinal=int(d),
            classes=classes_t,
            world_origin_x=position.world_origin_x,
            world_origin_y=position.world_origin_y,
            pixel_res=position.pixel_res,
            min_area_m2=min_patch_m2,
        )
        rows.extend(polygons_to_records(patches, tile_id))

    gpkg_path = _write_block_gpkg(
        rows, output_dir, tile_id,
        position.block_row, position.block_col, crs=crs,
    )
    print(f"  Wrote {gpkg_path} in {time.perf_counter() - t0:.2f} s  "
          f"({len(rows)} polygons)")

    print(f"\n[RSS] Final:                           {rss_mb():7.1f} MB")


# Column order for the per-block GeoPackage. Shared by the populated and
# empty writers so the schema is identical whether or not a block had
# detections.
_GPKG_COLUMNS = [
    "tile_id", "date_ordinal", "date_iso", "class_id",
    "n_pixels", "area_m2", "centroid_x", "centroid_y", "geometry",
]


def _write_block_gpkg(rows: list, output_dir: str, tile_id: str,
                      block_row: int, block_col: int, *, crs) -> str:
    """Write one block's polygon rows to a GeoPackage; return its path.

    Empty `rows` writes a valid empty layer so every block has a .gpkg.
    """
    import geopandas as gpd
    if rows:
        gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs=crs)
        gdf = gdf[_GPKG_COLUMNS]
    else:
        gdf = gpd.GeoDataFrame(
            columns=_GPKG_COLUMNS, geometry="geometry", crs=crs,
        )
    gpkg_path = os.path.join(
        output_dir,
        f"{tile_id}_block_{block_row:03d}_{block_col:03d}.gpkg",
    )
    gdf.to_file(gpkg_path, layer="detections", driver="GPKG")
    return gpkg_path


def _write_empty_block_gpkg(output_dir: str, tile_id: str,
                            block_row: int, block_col: int, *, crs) -> str:
    """Write an empty per-block GeoPackage (no detections)."""
    return _write_block_gpkg([], output_dir, tile_id, block_row, block_col,
                             crs=crs)


def _read_hdf5_crs(hdf5_path: str):
    """Return the tile's CRS (EPSG string/int or WKT) from the HDF5 `crs`
    attr, or None if absent. geopandas accepts any pyproj-parsable form.
    """
    try:
        import h5py
    except ImportError:
        return None
    try:
        with h5py.File(hdf5_path, "r") as h5f:
            raw = h5f.attrs.get("crs")
    except (OSError, KeyError):
        return None
    if raw is None:
        return None
    return raw.decode("utf-8") if isinstance(raw, bytes) else raw


if __name__ == "__main__":
    main()
