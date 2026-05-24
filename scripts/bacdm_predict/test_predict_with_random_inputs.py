"""
test_predict_with_random_inputs.py

Smoke-test for predict_before_after_chips using dummy uint16 chips.

Requires Test_weight_path in AAA_Configs.py to point to a valid .pth file.
All other inputs are randomly generated, so no real imagery is needed.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import numpy as np
import AAA_Configs
from predict import load_model, predict_before_after_chips

# ── Parameters ─────────────────────────────────────────────────────────────────
B = 2        # batch size (number of chip pairs)
H = W = 256  # chip spatial dimensions (must match the model's expected 256×256)
# C must cover every index in selected_nums; uint16 exercises the _to_uint8 path
C = max(AAA_Configs.selected_nums) + 1

# ── Random chips ───────────────────────────────────────────────────────────────
# Values in [100, 5000] — realistic Sentinel-2 surface reflectance range,
# well clear of the NODATA sentinel (65535).
rng = np.random.default_rng(seed=42)
before_batch = rng.integers(100, 5000, size=(B, H, W, C), dtype=np.uint16)
after_batch  = rng.integers(100, 5000, size=(B, H, W, C), dtype=np.uint16)

print(f"Input shape  : {before_batch.shape}  dtype={before_batch.dtype}")
print(f"Model weights: {AAA_Configs.Test_weight_path}")

# ── Load model and run inference ───────────────────────────────────────────────
model  = load_model(AAA_Configs.Test_weight_path)
labels = predict_before_after_chips(before_batch, after_batch, model)

# ── Report ─────────────────────────────────────────────────────────────────────
print(f"\nOutput shape : {labels.shape}")
print(f"Output dtype : {labels.dtype}")
print(f"Unique classes: {sorted(np.unique(labels).tolist())}")
for cls_id in sorted(np.unique(labels).tolist()):
    count = int((labels == cls_id).sum())
    pct   = count / labels.size * 100
    name  = AAA_Configs.CLASS_NAMES.get(int(cls_id), str(cls_id))
    print(f"  class {cls_id} ({name:12s}): {count:8d} px  ({pct:5.1f} %)")

print("\nSmoke-test passed.")
