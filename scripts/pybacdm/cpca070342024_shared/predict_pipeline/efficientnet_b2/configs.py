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

NORM_MEAN = (0.485, 0.456, 0.406, 0.485, 0.456, 0.406, 0.485, 0.456, 0.406, 0.485)
NORM_STD  = (0.229, 0.224, 0.225, 0.229, 0.224, 0.225, 0.229, 0.224, 0.225, 0.229)

# ── Post-processing ────────────────────────────────────────────────────────────
MIN_PATCH_SIZE = 25   # minimum viable patch in pixels
CLOSING_RADIUS = 3    # morphological closing radius (fallback for unlisted classes)

# Per-class closing radii — both chip-level and block-level closing read this
# dict so the rule stays consistent.  Keys are grouped class IDs (1=Cuts, 2=Fires).
CLOSING_RADII = {1: 3, 2: 1}

# ── Per-class inference threshold ──────────────────────────────────────────────
# Pixels where P(Cuts) exceeds this value are labelled Cuts even if not argmax.
# Set to None to fall back to plain argmax for all classes.
CUTS_THRESHOLD = 0.3

# ── Class definitions ──────────────────────────────────────────────────────────
NUM_CLASSES  = 3
CLASS_NAMES  = {0: 'Background', 1: 'Cuts', 2: 'Fires'}
CLASS_COLORS = {0: (0, 0, 0, 0), 1: (255, 165, 0, 255), 2: (220, 20, 60, 255)}

# ── Inference weights ──────────────────────────────────────────────────────────
# Default: weights/best_model.pth alongside this file.
_here = os.path.dirname(os.path.abspath(__file__))
Test_weight_path = os.path.join(_here, "weights", "best_model.pth")
