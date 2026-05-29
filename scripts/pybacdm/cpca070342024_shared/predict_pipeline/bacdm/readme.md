# bacdm_predict

Self-contained inference package for **BACDM / Swin-YNet** vegetation-loss change detection.
Load the model once, then call it repeatedly on batches of before/after 256 × 256 chips.

## Contents

| File | Description |
|---|---|
| `AAA_Configs.py` | Inference-only configuration (see [Configuration](#configuration)) |
| `predict.py` | Public API: `load_model` and `predict_before_after_chips` |
| `swin_ynet.py` | Swin-YNet architecture (encoder + decoder) |
| `YTYAttention.py` | Attention modules used by the decoder |
| `data/dataset_swin_GZ.py` | `_to_uint8` helper (16-bit → 8-bit stretch) |

## Quick start

```python
from predict import load_model, predict_before_after_chips

model = load_model("path/to/weights.pth")   # load once

# before_batch, after_batch: np.ndarray (B, 256, 256, C), uint8 or uint16
labels = predict_before_after_chips(before_batch, after_batch, model)
# labels: np.ndarray (B, 256, 256), uint8
# class index per pixel — see AAA_Configs.CLASS_NAMES
```

## Configuration

Edit `AAA_Configs.py` before running inference.

| Parameter | Default | Description |
|---|---|---|
| `Test_weight_path` | `"<path_to_model_weights>.pth"` | **Must be set.** Path to the fine-tuned `.pth` checkpoint. |
| `USE_CUDA` | `True` | Use GPU if available. |
| `channel_nums` | `10` | Number of input spectral bands (`6` or `10`). |
| `CUTS_THRESHOLD` | `0.3` | P(Cuts) threshold for recall recovery; `None` to use plain argmax. |
| `MIN_PATCH_SIZE` | `25` | Minimum predicted patch size in pixels (post-processing). |
| `CLOSING_RADIUS` | `3` | Morphological closing radius in pixels (post-processing). |
| `CLASS_GROUPING` | `{0:[0,3], 1:[1,2], 2:[4]}` | Maps original 5 classes → 3 classes. Set to `None` for the original 5. |

## Testing with dummy inputs

`test_predict_with_random_inputs.py` runs a full forward pass with randomly
generated uint16 chips — no real imagery needed.

**Prerequisites**

1. Set `Test_weight_path` in `AAA_Configs.py` to a valid `.pth` checkpoint.
2. Install the required Python packages (PyTorch, torchvision, scipy, einops, timm).

**Run**

```bash
# from the bacdm_predict directory
python test_predict_with_random_inputs.py

# or from any other working directory
python scripts/bacdm_predict/test_predict_with_random_inputs.py
```

**Expected output**

```
Input shape  : (2, 256, 256, 10)  dtype=uint16
Model weights: <your/path/to/weights.pth>
SwinTransformerSys expand initial----...
Loading pretrained model ...

Output shape : (2, 256, 256)
Output dtype : uint8
Unique classes: [0, 1, 2]
  class 0 (Background  ):   xxxxxx px  ( xx.x %)
  class 1 (Cuts        ):   xxxxxx px  ( xx.x %)
  class 2 (Fires       ):   xxxxxx px  ( xx.x %)

Smoke-test passed.
```

The script generates two random chips (`B=2`, `256×256`, `C=10`) with values in
`[100, 5000]` (uint16, realistic Sentinel-2 reflectance range, clear of the
65535 NODATA sentinel).  The fixed seed `42` makes results reproducible.
If the smoke-test prints `Smoke-test passed.` without raising an exception, the
model loads and runs correctly.

## Changes from the original `scripts/bacdm/` sources

| File | What changed |
|---|---|
| `AAA_Configs.py` | Training parameters removed (loss weights, data paths, LR, EPOCH, Tversky). Placeholder `Test_weight_path` added. |
| `predict.py` | `sys.path` simplified — only the local directory is added (no longer needs the parent `scripts/` folder). |
| `swin_ynet.py` | `from bacdm.YTYAttention import *` → `from YTYAttention import *`. `import copy` removed. `encoder1.load_from()` and `new_load_from()` removed: they called `sys.exit(0)` when `Train_pretrained_path` was `None`, which crashes at inference time. `load_model()` in `predict.py` loads the full checkpoint after construction, making the encoder pre-init redundant. |
| `YTYAttention.py` | Unchanged — no local imports. |
| `data/dataset_swin_GZ.py` | Trimmed to `_to_uint8` only. `rasterio`, `PIL`, `os`, `Dataset`, `MyData`, `MyTestData` removed. |
