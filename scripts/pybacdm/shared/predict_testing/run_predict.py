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
  5. encode_patches              (postprocess.encode)
  6. write_task_shard            (postprocess.shard)

Prints stats throughout. Step 6 writes one Parquet shard per (tile, block);
controlled by SAVE_OUTPUT in the configuration section.

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
from postprocess import encode_patches, write_task_shard, read_shards
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
N_CHIPS = 25
NODATA_FRAC = 0.05
REVISIT_DAYS = 5
START_DATE = (2024, 1, 1)
SEED = 42

# Target dates for before/after compositing. Pick a few that fall inside the
# data range so each one produces 64 chip pairs (16 originals + 16 H-shifts
# + 16 V-shifts + 16 diagonals; the 5th row/col of the block is ghost,
# supplying neighbour pixels for shifts at the live area's right/bottom edge).
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

# Step 5 + 6: encode connected components into PatchRecords and write a
# per-(tile, block) Parquet shard. Set False to skip patch encoding and
# shard writing entirely (useful for pure inference timing).
SAVE_OUTPUT = True
OUTPUT_DIR  = "/users1/cpca070342024/shared/predict_outputs"

# Tile ID used in the shard filename and PatchRecord.tile_id. For real data
# this should match the tile the HDF5 covers (e.g. "T29TPG"). For dummy
# data it's a placeholder.
TILE_ID = "T29TPG"
DUMMY_TILE_ID = "DUMMY"

# Connected-component noise filter: drop components smaller than this many
# pixels. 4 is a sensible default; 1 keeps everything, 16+ is aggressive.
MIN_COMPONENT_PIXELS = 4

# Class names — only the non-background classes generate PatchRecords.
# Must align with the model's output class scheme (see AAA_Configs.CLASS_NAMES).
CLASS_NAMES = {0: "Background", 1: "Cuts", 2: "Fires"}


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
        print(f"Chip block:     N_TS={N_TS}  N_CHIPS={N_CHIPS}  "
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
        print(f"Min component:  {MIN_COMPONENT_PIXELS} pixels")
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
            n_ts=N_TS, n_chips=N_CHIPS, nodata_frac=NODATA_FRAC,
            revisit_days=REVISIT_DAYS, start_date=START_DATE, seed=SEED,
        )
        # Fake a BlockPosition with zero world origin so steps 5/6 still work
        # for the dummy path. PatchRecord.world_origin will be unmeaningful.
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
    n_pairs = 0
    class_counts: Counter[int] = Counter()
    kind_counts: Counter[str] = Counter()
    patch_records: list = []   # accumulator for step 6 (step 5 emits per label)

    def encode_one(label_map: np.ndarray, chip_kind: str,
                   grid_position: tuple[int, int], date_ordinal: int):
        """Run step 5 on one chip's label map; append PatchRecords to the
        shared buffer; return wall time."""
        if not SAVE_OUTPUT:
            return 0.0
        t0 = time.perf_counter()
        records = list(encode_patches(
            label_map,
            tile_id=tile_id,
            block_row=position.block_row,
            block_col=position.block_col,
            chip_kind=chip_kind,
            grid_row=grid_position[0],
            grid_col=grid_position[1],
            date_ordinal=date_ordinal,
            date_iso=date.fromordinal(date_ordinal).isoformat(),
            class_names=CLASS_NAMES,
            block_world_origin_x=position.world_origin_x,
            block_world_origin_y=position.world_origin_y,
            pixel_res=position.pixel_res,
            min_component_pixels=MIN_COMPONENT_PIXELS,
        ))
        patch_records.extend(records)
        return time.perf_counter() - t0

    if not USE_BUNDLED:
        # Per-pair path: yield one ChipPair at a time, stack BATCH_SIZE of
        # them before each forward pass.
        pair_iter = generate_shifted_chips(
            composites, target_dates, valid_dates_mask, verbose=True,
        )
        batch: list = []

        def flush(batch: list):
            nonlocal t_inference_total, t_encode_total, n_pairs
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
                t_encode_total += encode_one(
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
                    t_encode_total += encode_one(
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

    # ── Steps 5 + 6: patch encoding summary + shard write ─────────────────
    if SAVE_OUTPUT:
        print(f"\nStep 5: patches encoded:               {len(patch_records):,}")
        print(f"  Total encode time: {t_encode_total:.2f} s "
              f"({t_encode_total / max(n_pairs, 1) * 1000:.1f} ms/chip)")
        if patch_records:
            by_label: Counter[str] = Counter(r.label_name for r in patch_records)
            print(f"  By class: {dict(sorted(by_label.items()))}")
            sizes = np.array([r.n_pixels for r in patch_records])
            print(f"  Component sizes: min={int(sizes.min())}  "
                  f"median={int(np.median(sizes))}  max={int(sizes.max())}  "
                  f"mean={float(sizes.mean()):.1f}")
        else:
            print("  (no non-background components found)")

        print(f"\nStep 6: writing shard...")
        t0 = time.perf_counter()
        shard_path = write_task_shard(
            patch_records, OUTPUT_DIR, tile_id,
            position.block_row, position.block_col,
        )
        write_s = time.perf_counter() - t0
        shard_bytes = os.path.getsize(shard_path)
        print(f"  Wrote {shard_path} in {write_s:.2f} s  "
              f"({shard_bytes / 1024:.1f} KB)")

        # Read-back sanity check: re-open the shard with read_shards and
        # verify row count + show a preview of the first few rows.
        t0 = time.perf_counter()
        df = read_shards(OUTPUT_DIR, tile_id=tile_id)
        read_s = time.perf_counter() - t0
        # Filter to this block in case OUTPUT_DIR holds other blocks' shards.
        df_block = df[(df["block_row"] == position.block_row) &
                      (df["block_col"] == position.block_col)]
        print(f"\nShard read-back: {len(df_block)} rows "
              f"(read {len(df)} rows for tile {tile_id} in {read_s:.2f} s)")
        if len(df_block) == len(patch_records):
            print("  Row count matches in-memory PatchRecord count — OK")
        else:
            print(f"  WARNING: row count mismatch (shard={len(df_block)}, "
                  f"in-memory={len(patch_records)})")
        if len(df_block) > 0:
            preview_cols = [
                "chip_kind", "grid_row", "grid_col", "date_iso",
                "label_name", "n_pixels",
                "bbox_chip_y0", "bbox_chip_x0",
                "world_origin_x", "world_origin_y",
            ]
            print("  First 5 rows (selected cols):")
            with pd.option_context("display.max_columns", None,
                                   "display.width", 160):
                print(df_block[preview_cols].head().to_string(index=False))

    print(f"\n[RSS] Final:                           {rss_mb():7.1f} MB")


if __name__ == "__main__":
    main()
