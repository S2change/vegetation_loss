"""
AAA_Configs.py — Inference-only configuration for BACDM / Swin-YNet.

Training parameters (batch_size, EPOCH, loss weights, data paths, etc.)
are intentionally omitted.  Update Test_weight_path before running inference.
"""
import sys
import numpy as np

USE_CUDA = True

# ── Input channels ─────────────────────────────────────────────────────────────
channel_nums = 10

if channel_nums == 6:
    normalization_mean = (0.485, 0.456, 0.406, 0.456, 0.406, 0.485)
    normalization_std  = (0.229, 0.224, 0.225, 0.224, 0.225, 0.229)
    selected_nums      = [0, 1, 2, 3, 4, 5] # Training data was reversed before input
    reversed_nums      = [5, 4, 3, 2, 1, 0] # predict_pipeline ingests S2 data with standard ordering, needs to be reversed
elif channel_nums == 10:
    normalization_mean = (0.485, 0.456, 0.406, 0.485, 0.456, 0.406, 0.485, 0.456, 0.406, 0.485)
    normalization_std  = (0.229, 0.224, 0.225, 0.229, 0.224, 0.225, 0.229, 0.224, 0.225, 0.229)
    selected_nums      = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] # Training data was reversed before input
    reversed_nums      = [9, 8, 7, 6, 5, 4, 3, 2, 1, 0] # predict_pipeline ingests S2 data with standard ordering, needs to be reversed
else:
    sys.exit('channel_nums is wrong')

# ── Post-processing ────────────────────────────────────────────────────────────
MIN_PATCH_SIZE = 25   # minimum viable patch in pixels
CLOSING_RADIUS = 3    # morphological closing radius (fallback for unlisted classes)

# Per-class morphological closing radius. Closing runs at two stages — per
# chip (predict.postprocess_prediction) and per block, post-vote
# (polygonize.close_labels) — and BOTH read this dict so the rule can't drift.
# Keys are grouped class IDs (see CLASS_GROUPING_NAMES): 1 = Cuts, 2 = Fires.
# Classes absent from this dict fall back to CLOSING_RADIUS.
CLOSING_RADII = {1: 3, 2: 1}   # Cuts → 3, Fires → 1

# ── Per-class inference threshold ──────────────────────────────────────────────
# Pixels where P(Cuts) exceeds this value are labelled Cuts even if not argmax.
# Set to None to fall back to plain argmax for all classes.
CUTS_THRESHOLD = 0.3

# ── Class definitions ──────────────────────────────────────────────────────────
ORIG_CLASS_NAMES  = {0: 'Background', 1: 'ClearCuts', 2: 'OtherCuts', 3: 'AgriLoss', 4: 'Fires'}
ORIG_CLASS_COLORS = {0: (0,0,0,0), 1: (255,0,0,255), 2: (0,255,0,255), 3: (0,0,255,255), 4: (255,255,0,255)}

CLASS_GROUPING        = {0: [0, 3], 1: [1, 2], 2: [4]}   # agriculture → background
CLASS_GROUPING_NAMES  = {0: 'Background', 1: 'Cuts',  2: 'Fires'}
CLASS_GROUPING_COLORS = {0: (0,0,0,0),    1: (255,255,0,255), 2: (255,0,0,255)}

if CLASS_GROUPING is not None:
    NUM_CLASSES = len(CLASS_GROUPING)
    _max_orig   = max(c for lst in CLASS_GROUPING.values() for c in lst)
    CLASS_REMAP = np.zeros(_max_orig + 1, dtype=np.int64)
    for _new_c, _old_list in CLASS_GROUPING.items():
        for _old_c in _old_list:
            CLASS_REMAP[_old_c] = _new_c
    CLASS_NAMES  = CLASS_GROUPING_NAMES
    CLASS_COLORS = CLASS_GROUPING_COLORS
else:
    NUM_CLASSES = 5
    CLASS_REMAP = None
    CLASS_NAMES  = ORIG_CLASS_NAMES
    CLASS_COLORS = ORIG_CLASS_COLORS

# ── Inference weights ──────────────────────────────────────────────────────────
# Set this to the path of your trained model checkpoint (.pth).
Test_weight_path = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\bacdm\bacdm_weights\teste20260429163505_best.pth"
