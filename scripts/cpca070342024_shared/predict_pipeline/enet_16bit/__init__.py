"""EfficientNet-B2 U-Net model package, 16-bit (select with MODEL=enet_16bit).

Every model package under predict_pipeline/ exposes the same interface so
the distribute/ pipeline can swap models via the MODEL env var:

  <pkg>.predict.load_model(weights_path, device=None)        -> nn.Module
  <pkg>.predict.predict_before_after_chips(before, after, m) -> (B, H, W) uint8
  <pkg>.DEFAULT_WEIGHTS   default .pth checkpoint (used when WEIGHTS_PATH
                          is unset)
  <pkg>.CLOSING_RADII     per-class morphological closing radii, shared by
                          the chip-level and block-level closes
  <pkg>.CLOSING_RADIUS    fallback radius for classes absent from the dict

Keep this module import-light (no torch/smp) — the pipeline imports it on
the login node just to resolve defaults.
"""
from pathlib import Path as _Path

MODEL_NAME = "enet_16bit"
MODEL_DIR = _Path(__file__).resolve().parent

# Default checkpoint, used when the WEIGHTS_PATH env var is unset.
# (= configs.Test_weight_path; place best_model.pth in weights/ first.)
DEFAULT_WEIGHTS = MODEL_DIR / "weights" / "best_model.pth"

from .configs import CLOSING_RADII, CLOSING_RADIUS  # noqa: E402
