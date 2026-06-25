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
    OUTPUT_DIR        Base run directory (used as a fallback output location).
    TILE_ID           Tile name (e.g. T29TPG). Used in the .npz filename.
    BLOCK_ROW         Block row index (0..N_BLOCK_ROWS-1).
    BLOCK_COL         Block col index (0..N_BLOCK_COLS-1).
    TARGET_DATES      Comma-separated YYYY-MM-DD list, e.g.
                      "2025-11-15,2025-12-01".

  Optional
    MODEL             Model package directory name under <shared>/models/
                      (default "bacdm"; e.g. "enet_8bit"). The package must
                      expose predict.load_model / predict_before_after_chips
                      and DEFAULT_WEIGHTS — see bacdm/__init__.py for the
                      interface contract.
    WEIGHTS_PATH      Path to the model's .pth checkpoint. Defaults to the
                      model package's DEFAULT_WEIGHTS.
    DATA_DTYPE        "u8" (default) reads blocks with the q02/q98 stretch
                      (uint8, nodata 255) — bacdm / enet_8bit. "u16"
                      keeps raw uint16 reflectance (nodata 65535) — for
                      models that scale natively (efficientnet_b2_16bit_
                      pipeline). Controls the read->composite->shift chain,
                      not the model (each model handles its own input).
    BLOCK_OUTPUT_DIR  Where to write the per-block .npz + .gpkg. Defaults to
                      OUTPUT_DIR (submit_tile.sh sets it to
                      OUTPUT_DIR/block_outputs).
    BATCH_SIZE        Model batch size (default 8).
    VOTE_CLASSES      Comma-separated non-bg class IDs (default "1,2").
    VOTE_THRESHOLD    Min votes per pixel to keep a detection (default 2).
    CLOSING_RADIUS    Post-vote close radius. Normally UNSET (submit_tile.sh
                      does not set it) so the model package's per-class
                      CLOSING_RADII is the single source of truth. If present
                      in the environment it still forces one radius for all
                      classes (0 = off) — a low-level escape hatch only.
    MIN_PATCH_M2      Block-level patch-area floor in m^2 (default 2500).
    MAX_COMPOSITE_DAYS  Symmetric day-window around the break date for
                      before/after compositing (unset = unbounded).
    READ_START_DATE / READ_END_DATE  Clip the raw HDF5 timestep read to this
                      ISO date range before loading the block, so out-of-window
                      timesteps aren't pulled into memory. Empty/unset on either
                      side = no bound there. submit_tile.sh defaults these to
                      START_DATE / END_DATE (the cluster window).
    DATE_CLUSTERS     Serialized date clusters (set by submit_tile.sh when
                      USE_DATE_CLUSTERS=1). Format: clusters ';'-separated,
                      ISO dates within a cluster ','-separated, e.g.
                      "2023-01-01,2023-01-03;2023-02-10,2023-02-12". When set,
                      the block's raw timesteps are collapsed to one min-
                      composite per cluster before compositing. Unset = use
                      every raw timestep.

  Optional — debug composite GeoTIFFs
    WRITE_COMPOSITE_TIFS    Set to 1 to create the before & after time-
                      composites for each valid target date. Off by default.
    COMPOSITE_TIF_DIR       Defaults to OUTPUT_DIR/composite_tifs

Everything else (model architecture, ghost geometry, etc.) is fixed by
the modules being imported.
"""
import importlib
import os
import sys
import time
from collections import Counter
from datetime import date
from pathlib import Path

import numpy as np
import psutil
import torch

# Layout: predict_pipeline/{processes/{distribute, input_setup,
# composite_shift_chips, postprocess}, models/{bacdm, enet_8bit, ...}}.
# Put processes/ on the path for the shared subpackages, and models/ (one level
# above processes/) for the model packages, which are imported by their bare
# name via the MODEL env var.
_HERE = Path(__file__).resolve().parent                        # processes/distribute
_PROCESSES = _HERE.parent                                      # processes/
_MODELS_DIR = _PROCESSES.parent / "models"                     # predict_pipeline/models
sys.path.insert(0, str(_PROCESSES))                            # shared subpackages
sys.path.insert(0, str(_MODELS_DIR))                           # model packages

from input_setup import (
    read_block, get_block_grid_shape,
    aggregate_block_dates, parse_date_clusters,
)
from composite_shift_chips import (
    create_before_after_composites,
    generate_shifted_chips,
)
# Imported from the submodule (not the package __init__) so rasterio stays off
# the core composite import path; only pulled in when actually writing TIFs.
from composite_shift_chips.write_composite_tifs import write_block_composite_tifs
from postprocess import (
    chip_nw_pixel_offset,
    postprocess_prediction,
    VoteAccumulator,
    write_voted_block,
)
from polygonize import (
    labels_to_polygons, polygons_to_records, close_labels,
)

# ── Model selection ──────────────────────────────────────────────────────────
# MODEL names a model package directory under <shared>/models/ (default bacdm).
# Every model package exposes the same interface — predict.load_model,
# predict.predict_before_after_chips, DEFAULT_WEIGHTS, CLOSING_RADII — see
# bacdm/__init__.py for the contract. polygonize (imported above) reads the
# same env var, so the block-level close uses the matching CLOSING_RADII.
MODEL = os.environ.get("MODEL", "bacdm")
try:
    _model_pkg = importlib.import_module(MODEL)
    _model_predict = importlib.import_module(f"{MODEL}.predict")
except ImportError as exc:
    _available = sorted(
        p.parent.name for p in _MODELS_DIR.glob("*/predict.py")
    )
    raise SystemExit(
        f"[predict_block] Could not import model package '{MODEL}': {exc}\n"
        f"Available model packages under {_MODELS_DIR}: {_available}"
    )
load_model = _model_predict.load_model
predict_before_after_chips = _model_predict.predict_before_after_chips


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
    # Checkpoint: explicit WEIGHTS_PATH wins; otherwise the model package's
    # DEFAULT_WEIGHTS. Checked up front so a missing file fails fast with a
    # clear message instead of a torch.load traceback after the HDF5 read.
    weights_path  = (os.environ.get("WEIGHTS_PATH")
                     or str(getattr(_model_pkg, "DEFAULT_WEIGHTS", "")))
    if not weights_path:
        raise SystemExit(
            f"[predict_block] WEIGHTS_PATH is unset and model package "
            f"'{MODEL}' has no DEFAULT_WEIGHTS"
        )
    if not Path(weights_path).is_file():
        raise SystemExit(
            f"[predict_block] Model weights not found: {weights_path}"
        )
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
    # the model package's CLOSING_RADII (Cuts → 3, Fires → 1); leaving
    # CLOSING_RADIUS unset uses those. Setting CLOSING_RADIUS forces one
    # radius for every class (override); CLOSING_RADIUS=0 disables closing.
    _cr_env = os.environ.get("CLOSING_RADIUS")
    closing_radius = int(_cr_env) if _cr_env is not None else None
    # Block-level patch-area floor (m^2). Dropped at this stage; the master
    # applies a second, larger floor after cross-block merge.
    min_patch_m2 = float(os.environ.get("MIN_PATCH_M2", "2500"))

    # Input data dtype for the whole read->composite->shift chain. "u8"
    # (default) reads blocks with the per-band q02/q98 percentile stretch
    # (uint8, nodata 255) — what the bacdm / enet_8bit models expect.
    # "u16" keeps raw uint16 reflectance (nodata 65535) — for models trained
    # on native reflectance (e.g. enet_16bit), which scale
    # by 10000 internally. The model itself doesn't read this knob; it only
    # controls how the block is read + carried up to the model call.
    _dtype_env = os.environ.get("DATA_DTYPE", "u8").strip().lower()
    if _dtype_env in ("u8", "uint8", "8"):
        stretch, input_nodata = True, 255
    elif _dtype_env in ("u16", "uint16", "16"):
        stretch, input_nodata = False, 65535
    else:
        raise SystemExit(
            f"[predict_block] DATA_DTYPE={_dtype_env!r} invalid "
            f"(expected 'u8' or 'u16')"
        )

    # Optional temporal cluster-aggregation. submit_tile.sh computes the date
    # clusters once (from the tile's acquisition calendar over START/END) and
    # exports them as DATE_CLUSTERS; the TARGET_DATES it also derives are the
    # cluster-gap midpoints. When DATE_CLUSTERS is set, each block's raw
    # timesteps are collapsed to one min-composite per cluster (ts -> cluster
    # medians) before before/after compositing — cloud-suppressing and
    # shrinking the time axis. Unset (default) = use every raw timestep.
    date_clusters = parse_date_clusters(os.environ.get("DATE_CLUSTERS", ""))

    # Optional per-pixel / per-patch change confidence. When OUTPUT_CONFIDENCE
    # is on, the model returns a per-pixel change-prob (0–100), votes carry it
    # through, and finalize emits a per-pixel mean confidence that polygonize
    # averages into a per-patch confidence attribute. Only enet_16bit supports
    # the model-side return today; guard so other models fail loudly rather
    # than silently dropping it. Off by default.
    output_confidence = (os.environ.get("OUTPUT_CONFIDENCE", "0")
                         not in ("0", "", "false", "False"))
    if output_confidence and MODEL != "enet_16bit":
        raise SystemExit(
            f"[predict_block] OUTPUT_CONFIDENCE=1 is only supported by "
            f"MODEL=enet_16bit, not '{MODEL}'."
        )

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
    print(f"Model:          {MODEL}")
    print(f"Input dtype:    {'uint16 (raw, nodata 65535)' if not stretch else 'uint8 (stretched, nodata 255)'}")
    print(f"Weights:        {weights_path}")
    print(f"Output dir:     {output_dir}")
    print(f"Target dates:   {[date.fromordinal(int(d)).isoformat() for d in target_dates]}")
    print(f"Max comp. days: {max_composite_days if max_composite_days is not None else 'unbounded'}")
    print(f"Batch size:     {batch_size}")
    print(f"Vote classes:   {vote_classes}")
    print(f"Vote threshold: {vote_threshold}")
    print(f"Closing radius: "
          f"{f'per-class ({MODEL}.CLOSING_RADII)' if closing_radius is None else closing_radius}")
    print(f"Min patch m^2:  {min_patch_m2}")
    print(f"Date clusters:  {len(date_clusters) if date_clusters else 'off (raw timesteps)'}")
    print(f"Confidence:     {'on (0-100 per pixel/patch)' if output_confidence else 'off'}")
    print(f"\n[RSS] After imports:                   {rss_mb():7.1f} MB")

    # ── Step 1: read chip block ───────────────────────────────────────────
    # Read window: clip the raw timestep read to [READ_START_DATE, READ_END_DATE]
    # before loading, so timesteps outside the window aren't pulled into memory.
    # submit_tile.sh defaults these to START_DATE/END_DATE (the cluster window);
    # empty/unset on either side means "no bound on that side".
    def _read_ordinal(name):
        v = os.environ.get(name)
        return date.fromisoformat(v).toordinal() if v not in (None, "") else None
    read_start_ord = _read_ordinal("READ_START_DATE")
    read_end_ord   = _read_ordinal("READ_END_DATE")
    print(f"\nStep 1: reading chip-block from HDF5...")
    print(f"  Read window:  "
          f"{os.environ.get('READ_START_DATE') or 'unbounded'} -> "
          f"{os.environ.get('READ_END_DATE') or 'unbounded'}")
    t0 = time.perf_counter()
    block, ts, position = read_block(hdf5_path, block_row, block_col,
                                     ts_start_ordinal=read_start_ord,
                                     ts_end_ordinal=read_end_ord,
                                     stretch=stretch)
    print(f"  block: shape={block.shape}  dtype={block.dtype}  "
          f"{block.nbytes / 1e6:.1f} MB")
    print(f"  ts:    {date.fromordinal(int(ts[0]))} -> "
          f"{date.fromordinal(int(ts[-1]))}  ({len(ts)} timesteps)")
    print(f"  Step 1 time: {time.perf_counter() - t0:.2f} s")
    print(f"[RSS] After chip-block:                {rss_mb():7.1f} MB")

    # ── Step 2b: temporal cluster-aggregation (optional) ──────────────────
    # Collapse the block's raw timesteps into one min-composite per date
    # cluster (ts -> cluster medians) before compositing. The clusters were
    # computed once by submit_tile.sh from the tile calendar and exported as
    # DATE_CLUSTERS. Clusters whose dates fall entirely outside this block's
    # kept timesteps are dropped here so a block with a narrower ts window
    # still aggregates cleanly.
    if date_clusters:
        kept = set(int(t) for t in ts)
        block_clusters = [
            [d for d in cl if int(d) in kept] for cl in date_clusters
        ]
        block_clusters = [cl for cl in block_clusters if cl]
        if not block_clusters:
            raise SystemExit(
                "[predict_block] DATE_CLUSTERS set but none of the clustered "
                "dates are in this block's kept timesteps — check the "
                "START/END window matches the tile."
            )
        print(f"\nStep 2b: aggregating {len(ts)} timesteps into "
              f"{len(block_clusters)} date-cluster composite(s)...")
        t0 = time.perf_counter()
        block, ts, position = aggregate_block_dates(
            block, ts, position, block_clusters, nodata=input_nodata,
        )
        print(f"  block: shape={block.shape}  dtype={block.dtype}  "
              f"{block.nbytes / 1e6:.1f} MB")
        print(f"  ts:    {[date.fromordinal(int(t)).isoformat() for t in ts]}")
        print(f"  Step 2b time: {time.perf_counter() - t0:.2f} s")
        print(f"[RSS] After cluster-aggregation:       {rss_mb():7.1f} MB")

    # ── Step 3: per-pixel before/after compositing ────────────────────────
    print(f"\nStep 3: compositing for {len(target_dates)} target date(s)...")
    t0 = time.perf_counter()
    composites, valid_dates_mask = create_before_after_composites(
        block, ts, target_dates, verbose=True,
        max_days_from_break=max_composite_days,
        nodata=input_nodata,
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
            nodata=input_nodata,
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
    t_postprocess_total = 0.0
    t_vote_total = 0.0
    n_pairs = 0
    class_counts: Counter[int] = Counter()
    kind_counts: Counter[str] = Counter()

    voters: dict[int, VoteAccumulator] = {
        int(d): VoteAccumulator(classes=vote_classes,
                                track_confidence=output_confidence)
        for d in target_dates[valid_dates_mask]
    }

    def vote_one(label_map: np.ndarray, chip_kind: str,
                 grid_position: tuple[int, int], date_ordinal: int,
                 prob_map: np.ndarray | None = None) -> None:
        nonlocal t_vote_total
        gr, gc = grid_position
        nw_y, nw_x = chip_nw_pixel_offset(chip_kind, gr, gc)
        t0 = time.perf_counter()
        voters[date_ordinal].add(label_map, nw_y, nw_x, prob_map=prob_map)
        t_vote_total += time.perf_counter() - t0

    pair_iter = generate_shifted_chips(
        composites, target_dates, valid_dates_mask, verbose=True,
    )
    batch: list = []

    def flush(batch: list) -> None:
        nonlocal t_inference_total, t_postprocess_total, n_pairs
        if not batch:
            return
        before = np.stack([p.before.transpose(1, 2, 0) for p in batch])
        after  = np.stack([p.after.transpose(1, 2, 0)  for p in batch])
        t0 = time.perf_counter()
        # predict_before_after_chips now returns RAW model output. Chip-level
        # per-class morphological closing is applied here, after prediction,
        # via the shared model-agnostic postprocess_prediction. remove_small=
        # False: close only — chips vote on closed-but-unfiltered output, with
        # size filtering deferred to the block/master stages where patches are
        # whole. Per-class radii come from the active MODEL package, matching
        # the block-level close (polygonize.close_labels).
        if output_confidence:
            labels, confs = predict_before_after_chips(
                before, after, model, return_confidence=True)
        else:
            labels = predict_before_after_chips(before, after, model)
            confs = None
        t_inference_total += time.perf_counter() - t0
        t0 = time.perf_counter()
        labels = np.stack([
            postprocess_prediction(labels[i], remove_small=False)
            for i in range(len(labels))
        ])
        t_postprocess_total += time.perf_counter() - t0
        n_pairs += len(batch)
        for i, (p, label) in enumerate(zip(batch, labels)):
            kind_counts[p.chip_kind] += 1
            uniq, cnts = np.unique(label, return_counts=True)
            for u, c in zip(uniq, cnts):
                class_counts[int(u)] += int(c)
            prob_map = confs[i] if confs is not None else None
            vote_one(label, p.chip_kind, p.grid_position, p.date_ordinal,
                     prob_map=prob_map)
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
    print(f"  Total close time:  {t_postprocess_total:.2f} s  "
          f"(chip-level postprocess)")
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
    _live_h = voters[next(iter(voters))].live_h
    _live_w = voters[next(iter(voters))].live_w
    voted_labels = np.zeros((len(target_dates), _live_h, _live_w), dtype=np.uint8)
    # Per-pixel confidence (0–100, 255=nodata) parallel to voted_labels, only
    # when tracking. Invalid/zero-filled dates stay all-nodata.
    voted_confidence = (
        np.full((len(target_dates), _live_h, _live_w), 255, dtype=np.uint8)
        if output_confidence else None
    )
    print(f"\nStep 5b: voting (threshold={vote_threshold}, classes={vote_classes})")
    for i, d in enumerate(target_dates):
        ordinal = int(d)
        if ordinal in voters:
            acc = voters[ordinal]
            n_votes = acc.n_votes_by_class()
            if output_confidence:
                voted_labels[i], voted_confidence[i] = acc.finalize(
                    threshold=vote_threshold, return_confidence=True)
            else:
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
        confidence=voted_confidence,
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
            confidence_2d=(voted_confidence[i]
                           if voted_confidence is not None else None),
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
