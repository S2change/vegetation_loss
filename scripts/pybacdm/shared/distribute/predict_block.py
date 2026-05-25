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
    OUTPUT_DIR        Directory to write the voted .npz into.
    TILE_ID           Tile name (e.g. T29TPG). Used in the .npz filename.
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

# Make the bacdm/ subpackage importable. Same path setup as
# predict_testing/run_predict.py — on INCD the model code lives under
# `prediction_model/bacdm/` (we point sys.path at `prediction_model/`).
# Locally for dev, callers must symlink or otherwise wire it up the same.
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))                          # shared/
sys.path.insert(0, str(_HERE / "prediction_model"))            # for bacdm.*

from input_setup import read_block, get_block_grid_shape
from composite_shift_chips import (
    create_before_after_composites,
    generate_shifted_chips,
)
from postprocess import (
    chip_nw_pixel_offset,
    VoteAccumulator,
    write_voted_block,
)
from predict import load_model, predict_before_after_chips


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
    output_dir    = _required_env("OUTPUT_DIR")
    tile_id       = _required_env("TILE_ID")
    block_row     = _int_env("BLOCK_ROW")
    block_col     = _int_env("BLOCK_COL")
    target_dates  = _dates_env()
    batch_size    = int(os.environ.get("BATCH_SIZE", "8"))
    vote_classes  = _classes_env()
    vote_threshold = int(os.environ.get("VOTE_THRESHOLD", "2"))

    # Bounds check the block coordinates against the HDF5's grid shape so
    # a misconfigured array index fails fast with a clear message instead
    # of read_block raising an opaque slice-bounds error.
    n_rows, n_cols = get_block_grid_shape(hdf5_path)
    if not (0 <= block_row < n_rows and 0 <= block_col < n_cols):
        raise SystemExit(
            f"[predict_block] block=({block_row}, {block_col}) is out of "
            f"range for grid shape ({n_rows}, {n_cols}) of {hdf5_path}"
        )

    print(f"Tile:           {tile_id}")
    print(f"HDF5:           {hdf5_path}")
    print(f"Block:          ({block_row}, {block_col}) of grid "
          f"({n_rows}, {n_cols})")
    print(f"Weights:        {weights_path}")
    print(f"Output dir:     {output_dir}")
    print(f"Target dates:   {[date.fromordinal(int(d)).isoformat() for d in target_dates]}")
    print(f"Batch size:     {batch_size}")
    print(f"Vote classes:   {vote_classes}")
    print(f"Vote threshold: {vote_threshold}")
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
    )
    n_valid = int(valid_dates_mask.sum())
    print(f"  composites: shape={composites.shape}  dtype={composites.dtype}  "
          f"{composites.nbytes / 1e6:.1f} MB")
    print(f"  valid dates: {n_valid} / {len(target_dates)}")
    print(f"  Step 3 time: {time.perf_counter() - t0:.2f} s")
    print(f"[RSS] After composites:                {rss_mb():7.1f} MB")

    if n_valid == 0:
        # Empty output is still useful: aggregator can detect missing/empty
        # blocks. Write a .npz with zero-filled labels for any dates the
        # user asked for so the file shape stays predictable.
        print("\n  No valid target dates for this block. Writing empty .npz "
              "(all-bg labels) and exiting.")
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
        return

    # ── Load model ────────────────────────────────────────────────────────
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
    print(f"\n[RSS] Final:                           {rss_mb():7.1f} MB")


if __name__ == "__main__":
    main()
