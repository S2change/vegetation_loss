"""End-to-end smoke test for the chip-chunked prediction pipeline.

Chains together steps 1-4 of the planned pipeline. Input source is selectable
via USE_REAL_DATA in the configuration section:

  USE_REAL_DATA = True   read_block      (input_setup.hdf5_reader)
  USE_REAL_DATA = False  make_chip_block (np_creation)               — dummy

Then in both modes:

  2. (skipped: input is already uint8 from step 1 / read_block fuses the stretch)
  3. create_before_after_composites (composite_shift_chips.composite)
  4. generate_shifted_chips (composite_shift_chips.shift_chips)
     → predict_before_after_chips (bacdm.predict)

Prints stats only — no outputs are saved.

Assumes the bacdm model code lives under ./prediction_model/bacdm/ (see
sys.path insert below) and that composite_shift_chips/ + input_setup/ are
reachable via the parent directory.

Usage:
    python run_predict.py
"""
import time
import sys
from collections import Counter
from datetime import date
from pathlib import Path

import numpy as np
import psutil

# Make the bacdm model code importable.
sys.path.insert(0, str(Path(__file__).resolve().parent / "prediction_model" / "bacdm"))
# Make composite_shift_chips importable (lives one directory up).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from np_creation import make_chip_block
from input_setup import read_block
from composite_shift_chips import (
    create_before_after_composites,
    generate_shifted_chips,
    generate_shifted_chips_bundled,
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
        print(f"  block: shape={block.shape}  dtype={block.dtype}  "
              f"{block.nbytes / 1e6:.1f} MB")
        print(f"  ts:    {date.fromordinal(int(ts[0]))} -> "
              f"{date.fromordinal(int(ts[-1]))}  ({len(ts)} timesteps)")
    print(f"  Step 1 time: {time.perf_counter() - t0:.2f} s")
    print(f"[RSS] After chip-block:                {rss_mb():7.1f} MB")

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
    n_pairs = 0
    class_counts: Counter[int] = Counter()
    kind_counts: Counter[str] = Counter()

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
                n_pairs += len(batch_kinds)
                for kind, label in zip(batch_kinds, labels):
                    kind_counts[kind] += 1
                    uniq, cnts = np.unique(label, return_counts=True)
                    for u, c in zip(uniq, cnts):
                        class_counts[int(u)] += int(c)

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

    print(f"\n[RSS] Final:                           {rss_mb():7.1f} MB")


if __name__ == "__main__":
    main()
