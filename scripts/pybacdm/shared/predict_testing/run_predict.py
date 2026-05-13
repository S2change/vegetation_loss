"""End-to-end smoke test for the BACDM inference path.

Generates dummy (before, after) chip batches via np_creation, loads the
real model, runs inference, and prints shape/timing/class diagnostics so
the prediction step can be exercised without HDF5 reads or compositing.

Assumes np_creation.py, predict.py, swin_ynet.py, AAA_Configs.py, and
data/dataset_swin_GZ.py all live in the same directory as this script
(or are otherwise on sys.path).

Usage:
    python run_predict.py
"""
import time

import numpy as np
import psutil

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent / "prediction_model" / "bacdm"))

from np_creation import make_before_after
from predict import load_model, predict_before_after_chips


def rss_mb():
    """Process resident memory in MB."""
    return psutil.Process().memory_info().rss / 1e6

# ============================================================================
# CONFIGURATION
# ============================================================================

# edit before running
WEIGHTS_PATH = "/users1/cpca070342024/shared/model_weights/teste20260429163505_best.pth"

BATCH_SIZE = 4
SEED = 42
NODATA_FRAC = 0.02


def main():
    print(f"Weights:    {WEIGHTS_PATH}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Seed:       {SEED}")
    print(f"NoData fraction: {NODATA_FRAC}")
    print(f"\n[RSS] After imports:                   {rss_mb():7.1f} MB")

    print("\nGenerating dummy chip batches...")
    before, after = make_before_after(
        batch_size=BATCH_SIZE, nodata_frac=NODATA_FRAC, seed=SEED,
    )
    print(f"  before: shape={before.shape}  dtype={before.dtype}")
    print(f"  after:  shape={after.shape}  dtype={after.dtype}")
    print(f"[RSS] After generating dummy batches:  {rss_mb():7.1f} MB")

    print("\nLoading model...")
    t0 = time.perf_counter()
    model = load_model(WEIGHTS_PATH)
    print(f"  Loaded in {time.perf_counter() - t0:.2f} s")
    print(f"[RSS] After model loaded:              {rss_mb():7.1f} MB")

    print("\nRunning inference...")
    rss_before_infer = rss_mb()
    t0 = time.perf_counter()
    labels = predict_before_after_chips(before, after, model)
    infer_s = time.perf_counter() - t0
    rss_after_infer = rss_mb()
    print(f"  Inference time: {infer_s:.2f} s "
          f"({infer_s / BATCH_SIZE * 1000:.1f} ms/chip)")
    print(f"[RSS] After inference:                 {rss_after_infer:7.1f} MB  "
          f"(delta {rss_after_infer - rss_before_infer:+6.1f} MB)")

    print(f"\nOutput: shape={labels.shape}  dtype={labels.dtype}")
    classes, counts = np.unique(labels, return_counts=True)
    total = labels.size
    print("Per-class pixel counts:")
    for cls, cnt in zip(classes, counts):
        print(f"  class {int(cls)}: {int(cnt):>10,} pixels  ({100 * cnt / total:5.2f}%)")

    print(f"\n[RSS] Final:                           {rss_mb():7.1f} MB")


if __name__ == "__main__":
    main()
