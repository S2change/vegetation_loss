"""End-to-end smoke test for the chip-chunked prediction pipeline.

Chains together steps 1-6 of the planned pipeline. Input source is selectable
via USE_REAL_DATA in the configuration section:

  USE_REAL_DATA = True   read_block      (input_setup.hdf5_reader)
  USE_REAL_DATA = False  make_chip_block (np_creation)               — dummy

Then in both modes:

  2. (skipped: input is already uint8 from step 1 / read_block fuses the stretch)
  3. create_before_after_composites (composite_shift_chips.composite)
  4. generate_shifted_chips (composite_shift_chips.shift_chips)
     → predict_before_after_chips (bacdm.predict)
  5a. (optional) encode_chip_predictions  (postprocess.chip_records) — debug
  5b. VoteAccumulator.add per chip         (postprocess.vote)
  6.  VoteAccumulator.finalize + write_voted_block (postprocess.voted_output)
      and/or write_task_shard               (postprocess.shard)

Prints stats throughout. Step 6 writes one .npz of voted label maps per
(tile, block) by default; the per-chip Parquet path is gated behind
SAVE_CHIP_RECORDS for debugging.

Assumes the bacdm model code lives under ./prediction_model/bacdm/ (see
sys.path insert below) and that composite_shift_chips/, input_setup/,
postprocess/ are reachable via the parent directory.

Usage:
    python run_predict.py
"""
import os
import time
import sys
from collections import Counter
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import psutil

# Make the bacdm model code importable.
sys.path.insert(0, str(Path(__file__).resolve().parent / "prediction_model" / "bacdm"))
# Make composite_shift_chips importable (lives one directory up).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from np_creation import make_chip_block
from input_setup import read_block, BlockPosition
from composite_shift_chips import (
    create_before_after_composites,
    generate_shifted_chips,
    generate_shifted_chips_bundled,
)
from postprocess import (
    encode_chip_predictions,
    chip_nw_pixel_offset,
    write_task_shard,
    read_shards,
    VoteAccumulator,
    write_voted_block,
    read_voted_block,
)
from predict import load_model, predict_before_after_chips


def rss_mb():
    """Process resident memory in MB."""
    return psutil.Process().memory_info().rss / 1e6


# ============================================================================
# CONFIGURATION
# ============================================================================

# edit before running
WEIGHTS_PATH = "/users1/cpca070342024/shared/model_weights/teste20260429163505_best.pth"

# Step-1 input source:
#   True  -> read one 5x5 block from a chip-chunked HDF5 via input_setup.read_block
#   False -> generate a dummy block via np_creation.make_chip_block
USE_REAL_DATA = True

# Real-data parameters (used when USE_REAL_DATA = True)
HDF5_PATH = "/users1/cpca070342024/shared/hdf5/T29TPG_48ts_20251028_20251229.h5"
BLOCK_ROW = 0   # 0..N_BLOCK_ROWS-1 (see input_setup.get_block_grid_shape)
BLOCK_COL = 0   # 0..N_BLOCK_COLS-1

# Dummy-data parameters (used when USE_REAL_DATA = False)
N_TS = 48
NODATA_FRAC = 0.05
REVISIT_DAYS = 5
START_DATE = (2024, 1, 1)
SEED = 42

# Target dates for before/after compositing. Each valid target date produces
# 81 chip pairs (16 originals + 20 H-shifts + 20 V-shifts + 25 diagonals)
# in the new 4-sided-ghost layout. Every live-area pixel gets exactly 4
# overlapping predictions (one per shift kind) for downstream voting.
#
# Defaults below fit the real HDF5's 2025-10-28 -> 2025-12-29 window. For the
# dummy path (START_DATE=2024-01-01, 48 ts x 5d) you'll want dates inside
# 2024-01-01..2024-08-23 instead — e.g., "2024-03-01", "2024-05-15".
TARGET_DATES_YYYYMMDD = ("2025-11-15", "2025-12-01")

# Model batching
BATCH_SIZE = 8

# Chip generation strategy.
#   False -> generate_shifted_chips           (yields one ChipPair at a time;
#                                              caller stacks BATCH_SIZE of
#                                              them before each forward pass)
#   True  -> generate_shifted_chips_bundled   (yields one ChipBundle of 64
#                                              pre-stacked chips per target
#                                              date; caller slices into batches)
USE_BUNDLED = False

# Step 5b + 6 (default): accumulate votes per chip and write one .npz of
# voted label maps per (tile, block). Set False to skip voting + .npz
# writing entirely (useful for pure inference timing).
SAVE_OUTPUT = True
OUTPUT_DIR  = "/users1/cpca070342024/shared/predict_outputs"

# Non-background classes tracked by the voter. For the current 3-class
# BACDM model (0=Background, 1=Cuts, 2=Fires) this is (1, 2).
VOTE_CLASSES = (1, 2)

# Vote threshold: at each LIVE pixel, keep the winning non-bg class only
# if it received at least this many votes (out of 4 per pixel under the
# current 4-sided-ghost geometry).
VOTE_THRESHOLD = 2

# Step 5 (debug): emit one ChipPredictionRecord per non-empty chip and
# write a per-(tile, block) Parquet shard. Held behind a flag — useful
# for debugging the per-chip predictions but not the production output.
SAVE_CHIP_RECORDS = False

# Tile ID used in the shard filename and ChipPredictionRecord.tile_id. For
# real data this should match the tile the HDF5 covers (e.g. "T29TPG"). For
# dummy data it's a placeholder.
TILE_ID = "T29TPG"
DUMMY_TILE_ID = "DUMMY"

# Note: small-component filtering (was MIN_COMPONENT_PIXELS) is now done
# exclusively upstream by predict.postprocess_prediction (MIN_PATCH_SIZE,
# CLOSING_RADIUS defaults from AAA_Configs). encode_chip_predictions just
# stores whatever non-background pixels survive that filter.


# ============================================================================
# HELPERS
# ============================================================================

def parse_target_dates(strings: tuple[str, ...]) -> np.ndarray:
    """('YYYY-MM-DD', ...) -> int ordinal array."""
    return np.array(
        [date.fromisoformat(s).toordinal() for s in strings],
        dtype=np.int64,
    )


# ============================================================================
# MAIN
# ============================================================================

def main():
    print(f"Weights:        {WEIGHTS_PATH}")
    print(f"Data source:    {'real (HDF5)' if USE_REAL_DATA else 'dummy (np_creation)'}")
    if USE_REAL_DATA:
        print(f"HDF5 path:      {HDF5_PATH}")
        print(f"Block:          (block_row={BLOCK_ROW}, block_col={BLOCK_COL})")
    else:
        print(f"Chip block:     N_TS={N_TS}  "
              f"nodata_frac={NODATA_FRAC}  revisit={REVISIT_DAYS}d")
        print(f"Start date:     {date(*START_DATE)}")
        print(f"Seed:           {SEED}")
    print(f"Target dates:   {TARGET_DATES_YYYYMMDD}")
    print(f"Model batch:    {BATCH_SIZE}")
    print(f"Bundled mode:   {USE_BUNDLED}")
    print(f"Save output:    {SAVE_OUTPUT}")
    if SAVE_OUTPUT:
        print(f"Output dir:     {OUTPUT_DIR}")
        print(f"Tile ID:        {TILE_ID if USE_REAL_DATA else DUMMY_TILE_ID}")
        print(f"Vote classes:   {VOTE_CLASSES}")
        print(f"Vote threshold: {VOTE_THRESHOLD}")
    print(f"Save chip recs: {SAVE_CHIP_RECORDS}")
    print(f"\n[RSS] After imports:                   {rss_mb():7.1f} MB")

    # ── Step 1: build / read chip-block ───────────────────────────────────
    if USE_REAL_DATA:
        print(f"\nStep 1: reading chip-block from HDF5 "
              f"(block_row={BLOCK_ROW}, block_col={BLOCK_COL})...")
        t0 = time.perf_counter()
        block, ts, position = read_block(HDF5_PATH, BLOCK_ROW, BLOCK_COL)
        print(f"  block: shape={block.shape}  dtype={block.dtype}  "
              f"{block.nbytes / 1e6:.1f} MB")
        print(f"  ts:    {date.fromordinal(int(ts[0]))} -> "
              f"{date.fromordinal(int(ts[-1]))}  ({len(ts)} timesteps)")
        print(f"  position: chip-grid origin (y={position.chip_y_start}, "
              f"x={position.chip_x_start})")
    else:
        print("\nStep 1: generating dummy chip-block...")
        t0 = time.perf_counter()
        block, ts = make_chip_block(
            n_ts=N_TS, nodata_frac=NODATA_FRAC,
            revisit_days=REVISIT_DAYS, start_date=START_DATE, seed=SEED,
        )
        # Fake a BlockPosition with zero world origin so steps 5/6 still work
        # for the dummy path. ChipPredictionRecord.block_world_origin_* will
        # be unmeaningful for downstream UTM-position math.
        position = BlockPosition(
            block_row=0, block_col=0,
            chip_y_start=0, chip_x_start=0,
            world_origin_x=0.0, world_origin_y=0.0,
            pixel_res=10.0,
        )
        print(f"  block: shape={block.shape}  dtype={block.dtype}  "
              f"{block.nbytes / 1e6:.1f} MB")
        print(f"  ts:    {date.fromordinal(int(ts[0]))} -> "
              f"{date.fromordinal(int(ts[-1]))}  ({len(ts)} timesteps)")
    print(f"  Step 1 time: {time.perf_counter() - t0:.2f} s")
    print(f"[RSS] After chip-block:                {rss_mb():7.1f} MB")

    # Tile identity used downstream by steps 5/6.
    tile_id = TILE_ID if USE_REAL_DATA else DUMMY_TILE_ID

    # ── Step 3: per-pixel before/after compositing ────────────────────────
    target_dates = parse_target_dates(TARGET_DATES_YYYYMMDD)
    print(f"\nStep 3: compositing for {len(target_dates)} target date(s)...")
    t0 = time.perf_counter()
    composites, valid_dates_mask = create_before_after_composites(
        block, ts, target_dates, verbose=True,
    )
    print(f"  composites: shape={composites.shape}  dtype={composites.dtype}  "
          f"{composites.nbytes / 1e6:.1f} MB")
    print(f"  valid dates: {valid_dates_mask.sum()} / {len(target_dates)}")
    print(f"  Step 3 time: {time.perf_counter() - t0:.2f} s")
    print(f"[RSS] After composites:                {rss_mb():7.1f} MB")

    # ── Load model ────────────────────────────────────────────────────────
    print("\nLoading model...")
    t0 = time.perf_counter()
    model = load_model(WEIGHTS_PATH)
    print(f"  Loaded in {time.perf_counter() - t0:.2f} s")
    print(f"[RSS] After model loaded:              {rss_mb():7.1f} MB")

    # ── Step 4: generate shifted chips & predict in batches ───────────────
    mode = "bundled" if USE_BUNDLED else "per-pair"
    print(f"\nStep 4: generating shifted chips + predicting (mode={mode})...")

    rss_before_infer = rss_mb()
    t_inference_total = 0.0
    t_encode_total = 0.0
    t_vote_total = 0.0
    n_pairs = 0
    class_counts: Counter[int] = Counter()
    kind_counts: Counter[str] = Counter()
    chip_records: list = []   # accumulator for step 5a (per-chip Parquet,
                              # gated behind SAVE_CHIP_RECORDS)

    # One VoteAccumulator per valid target date, keyed by ordinal date.
    # Only allocated if SAVE_OUTPUT — voting drives the .npz output.
    voters: dict[int, VoteAccumulator] = {}
    if SAVE_OUTPUT:
        for d in target_dates[valid_dates_mask]:
            voters[int(d)] = VoteAccumulator(classes=VOTE_CLASSES)

    def process_one(label_map: np.ndarray, chip_kind: str,
                    grid_position: tuple[int, int], date_ordinal: int):
        """Drop one chip's prediction into the voter (always, if SAVE_OUTPUT)
        and optionally encode it as a ChipPredictionRecord (if
        SAVE_CHIP_RECORDS). Updates the timing accumulators in place."""
        nonlocal t_vote_total, t_encode_total
        gr, gc = grid_position

        if SAVE_OUTPUT:
            nw_y, nw_x = chip_nw_pixel_offset(chip_kind, gr, gc)
            t0 = time.perf_counter()
            voters[date_ordinal].add(label_map, nw_y, nw_x)
            t_vote_total += time.perf_counter() - t0

        if SAVE_CHIP_RECORDS:
            t0 = time.perf_counter()
            records = list(encode_chip_predictions(
                label_map,
                tile_id=tile_id,
                block_row=position.block_row,
                block_col=position.block_col,
                chip_kind=chip_kind,
                grid_row=gr,
                grid_col=gc,
                date_ordinal=date_ordinal,
                date_iso=date.fromordinal(date_ordinal).isoformat(),
                block_world_origin_x=position.world_origin_x,
                block_world_origin_y=position.world_origin_y,
                pixel_res=position.pixel_res,
            ))
            chip_records.extend(records)
            t_encode_total += time.perf_counter() - t0

    if not USE_BUNDLED:
        # Per-pair path: yield one ChipPair at a time, stack BATCH_SIZE of
        # them before each forward pass.
        pair_iter = generate_shifted_chips(
            composites, target_dates, valid_dates_mask, verbose=True,
        )
        batch: list = []

        def flush(batch: list):
            nonlocal t_inference_total, n_pairs
            if not batch:
                return
            # Predictor expects (B, H, W, C) — our ChipPair holds (C, H, W).
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
                process_one(
                    label, p.chip_kind, p.grid_position, p.date_ordinal,
                )
            batch.clear()

        for pair in pair_iter:
            batch.append(pair)
            if len(batch) >= BATCH_SIZE:
                flush(batch)
        flush(batch)  # trailing partial batch
    else:
        # Bundled path: yield one ChipBundle (all 64 chips pre-stacked in
        # predictor-native layout) per target date. Slice into BATCH_SIZE
        # sub-batches and feed views (no copy) to the model.
        bundle_iter = generate_shifted_chips_bundled(
            composites, target_dates, valid_dates_mask, verbose=True,
        )
        for bundle in bundle_iter:
            n = bundle.before.shape[0]
            for i in range(0, n, BATCH_SIZE):
                before_view = bundle.before[i:i + BATCH_SIZE]
                after_view  = bundle.after[i:i + BATCH_SIZE]
                t0 = time.perf_counter()
                labels = predict_before_after_chips(
                    before_view, after_view, model)
                t_inference_total += time.perf_counter() - t0
                batch_kinds = bundle.chip_kinds[i:i + BATCH_SIZE]
                batch_positions = bundle.grid_positions[i:i + BATCH_SIZE]
                n_pairs += len(batch_kinds)
                for kind, gpos, label in zip(batch_kinds, batch_positions, labels):
                    kind_counts[kind] += 1
                    uniq, cnts = np.unique(label, return_counts=True)
                    for u, c in zip(uniq, cnts):
                        class_counts[int(u)] += int(c)
                    process_one(
                        label, kind, gpos, bundle.date_ordinal,
                    )

    rss_after_infer = rss_mb()

    print(f"\n  Pairs predicted:   {n_pairs}")
    print(f"  By chip kind:      "
          f"{dict(sorted(kind_counts.items()))}")
    print(f"  Total infer time:  {t_inference_total:.2f} s  "
          f"({t_inference_total / max(n_pairs, 1) * 1000:.1f} ms/chip)")
    print(f"[RSS] After all inference:             {rss_after_infer:7.1f} MB  "
          f"(delta {rss_after_infer - rss_before_infer:+6.1f} MB)")

    # ── Class-count summary across all chips ──────────────────────────────
    total_pixels = sum(class_counts.values())
    print("\nPer-class pixel counts (aggregated across all chip pairs):")
    for cls in sorted(class_counts):
        cnt = class_counts[cls]
        print(f"  class {cls}: {cnt:>12,} pixels  "
              f"({100 * cnt / total_pixels:5.2f}%)")

    # ── Step 5b: voting summary ───────────────────────────────────────────
    if SAVE_OUTPUT:
        print(f"\nStep 5b: voting "
              f"(threshold={VOTE_THRESHOLD}, classes={VOTE_CLASSES})")
        print(f"  Total vote time:   {t_vote_total:.2f} s "
              f"({t_vote_total / max(n_pairs, 1) * 1000:.2f} ms/chip)")

        # Per-date pre-threshold vote totals + post-threshold detection
        # counts. Useful for spotting dates where every pixel was bg.
        ordered_dates = sorted(voters)
        voted_labels = np.zeros(
            (len(ordered_dates), voters[ordered_dates[0]].live_h,
             voters[ordered_dates[0]].live_w),
            dtype=np.uint8,
        )
        t_finalize = 0.0
        for i, d in enumerate(ordered_dates):
            acc = voters[d]
            n_votes = acc.n_votes_by_class()
            t0 = time.perf_counter()
            voted = acc.finalize(threshold=VOTE_THRESHOLD)
            t_finalize += time.perf_counter() - t0
            voted_labels[i] = voted
            uniq, cnts = np.unique(voted, return_counts=True)
            post = {int(u): int(c) for u, c in zip(uniq, cnts) if u != 0}
            iso = date.fromordinal(d).isoformat()
            print(f"  {iso}: pre-threshold votes={n_votes}  "
                  f"post-threshold detections={post}")
        print(f"  Total finalize time: {t_finalize:.2f} s")

    # ── Step 5a (debug): chip-prediction encoding summary ─────────────────
    if SAVE_CHIP_RECORDS:
        print(f"\nStep 5a: chip records emitted:         {len(chip_records):,} "
              f"(of {n_pairs} chip predictions; skipped chips were "
              f"entirely background)")
        print(f"  Total encode time: {t_encode_total:.2f} s "
              f"({t_encode_total / max(n_pairs, 1) * 1000:.1f} ms/chip)")
        if chip_records:
            per_class_totals: Counter[int] = Counter()
            for r in chip_records:
                for cls, n in r.n_pixels_by_class.items():
                    per_class_totals[cls] += n
            print(f"  Per-class pixel totals (across all records): "
                  f"{dict(sorted(per_class_totals.items()))}")
        else:
            print("  (no chip had any non-background pixels)")

    # ── Step 6: write voted .npz and (optionally) per-chip Parquet ────────
    if SAVE_OUTPUT:
        print(f"\nStep 6: writing voted .npz...")
        t0 = time.perf_counter()
        npz_path = write_voted_block(
            OUTPUT_DIR, tile_id,
            position.block_row, position.block_col,
            labels=voted_labels,
            target_dates=np.asarray(ordered_dates, dtype=np.int64),
            classes=tuple(int(c) for c in VOTE_CLASSES),
            world_origin_x=position.world_origin_x,
            world_origin_y=position.world_origin_y,
            pixel_res=position.pixel_res,
            threshold=VOTE_THRESHOLD,
        )
        write_s = time.perf_counter() - t0
        npz_bytes = os.path.getsize(npz_path)
        print(f"  Wrote {npz_path} in {write_s:.2f} s  "
              f"({npz_bytes / 1024:.1f} KB)")

        # Read-back sanity check on the .npz.
        t0 = time.perf_counter()
        d = read_voted_block(npz_path)
        read_s = time.perf_counter() - t0
        print(f"\n.npz read-back ({read_s:.2f} s):")
        print(f"  labels:       shape={d['labels'].shape}  "
              f"dtype={d['labels'].dtype}")
        print(f"  target_dates: {d['target_dates'].tolist()}")
        print(f"  classes:      {d['classes'].tolist()}")
        print(f"  block:        ({int(d['block_row'])}, {int(d['block_col'])})  "
              f"origin=({float(d['world_origin_x'])}, "
              f"{float(d['world_origin_y'])})  "
              f"pixel_res={float(d['pixel_res'])}")
        print(f"  threshold:    {int(d['threshold'])}")
        if np.array_equal(d["labels"], voted_labels):
            print("  Labels match in-memory voted_labels — OK")
        else:
            print("  WARNING: labels mismatch on read-back")

    if SAVE_CHIP_RECORDS:
        print(f"\nStep 6 (debug): writing per-chip Parquet shard...")
        t0 = time.perf_counter()
        shard_path = write_task_shard(
            chip_records, OUTPUT_DIR, tile_id,
            position.block_row, position.block_col,
        )
        write_s = time.perf_counter() - t0
        shard_bytes = os.path.getsize(shard_path)
        print(f"  Wrote {shard_path} in {write_s:.2f} s  "
              f"({shard_bytes / 1024:.1f} KB)")

        t0 = time.perf_counter()
        df = read_shards(OUTPUT_DIR, tile_id=tile_id)
        read_s = time.perf_counter() - t0
        df_block = df[(df["block_row"] == position.block_row) &
                      (df["block_col"] == position.block_col)]
        print(f"  Shard read-back: {len(df_block)} rows "
              f"(read {len(df)} rows for tile {tile_id} in {read_s:.2f} s)")
        if len(df_block) == len(chip_records):
            print("  Row count matches in-memory ChipPredictionRecord count — OK")
        else:
            print(f"  WARNING: row count mismatch (shard={len(df_block)}, "
                  f"in-memory={len(chip_records)})")
        if len(df_block) > 0:
            base_cols = [
                "chip_kind", "grid_row", "grid_col", "date_iso",
                "chip_nw_px_y", "chip_nw_px_x",
                "block_world_origin_x", "block_world_origin_y",
            ]
            n_pixel_cols = sorted(
                c for c in df_block.columns if c.startswith("n_pixels_cls_")
            )
            preview_cols = base_cols + n_pixel_cols
            print("  First 5 rows (selected cols):")
            with pd.option_context("display.max_columns", None,
                                   "display.width", 160):
                print(df_block[preview_cols].head().to_string(index=False))

    print(f"\n[RSS] Final:                           {rss_mb():7.1f} MB")


if __name__ == "__main__":
    main()
