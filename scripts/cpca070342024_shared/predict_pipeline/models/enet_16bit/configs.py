"""
configs.py — Inference-only configuration for EfficientNet-B2 U-Net.

Training parameters are intentionally omitted.
Update Test_weight_path (or copy best_model.pth into weights/) before inference.
"""

import os
import numpy as np

USE_CUDA = True

# ── Input channels ─────────────────────────────────────────────────────────────
# B2-B12 ordering
NUM_BANDS = 10

# ── Radiometric scaling ────────────────────────────────────────────────────────
# uint16 reflectance divided by SCALE_FACTOR → [0, 1] float32.
# No per-chip or per-band normalisation; absolute spectral values are preserved.
SCALE_FACTOR  = 10000
NODATA_UINT16 = 65535   # sentinel in input chips
NODATA_UINT8  = 255     # sentinel in prediction output

# ── Post-processing ────────────────────────────────────────────────────────────
MIN_PATCH_SIZE = 25   # minimum viable patch in pixels
CLOSING_RADIUS = 3    # morphological closing radius (fallback for unlisted classes)

# Per-class closing radii — both chip-level and block-level closing read this
# dict so the rule stays consistent.  Keys are grouped class IDs (1=Cuts, 2=Fires).
CLOSING_RADII = {1: 3, 2: 1}

# ── Per-class inference threshold ──────────────────────────────────────────────
# Pixels where P(Cuts) exceeds this value are labelled Cuts even if not argmax.
# Set to None to fall back to plain argmax for all classes.
CUTS_THRESHOLD = 0.5

# Fires pixels where P(Fires) ≤ this value are reverted to Background.
# Set to None to disable.
FIRES_THRESHOLD = 0.8

# ── Class definitions ──────────────────────────────────────────────────────────
NUM_CLASSES  = 3
CLASS_NAMES  = {0: 'Background', 1: 'Cuts', 2: 'Fires'}
CLASS_COLORS = {0: (0, 0, 0, 0), 1: (255, 165, 0, 255), 2: (220, 20, 60, 255)}

# ── Inference weights ──────────────────────────────────────────────────────────
# Default: weights/best_model.pth alongside this file.
_here = os.path.dirname(os.path.abspath(__file__))
Test_weight_path = os.path.join(_here, "weights", "best_model.pth")
