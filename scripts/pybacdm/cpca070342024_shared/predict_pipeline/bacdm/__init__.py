"""BACDM / Swin-YNet model package (select with MODEL=bacdm).

Every model package under predict_pipeline/ exposes the same interface so
the distribute/ pipeline can swap models via the MODEL env var:

  <pkg>.predict.load_model(weights_path, device=None)        -> nn.Module
  <pkg>.predict.predict_before_after_chips(before, after, m) -> (B, H, W) uint8
  <pkg>.DEFAULT_WEIGHTS   default .pth checkpoint (used when WEIGHTS_PATH
                          is unset)
  <pkg>.CLOSING_RADII     per-class morphological closing radii, shared by
                          the chip-level and block-level closes
  <pkg>.CLOSING_RADIUS    fallback radius for classes absent from the dict

Keep this module import-light (no torch) — the pipeline imports it on the
login node just to resolve defaults.
"""
from pathlib import Path as _Path

MODEL_NAME = "bacdm"
MODEL_DIR = _Path(__file__).resolve().parent

# Default checkpoint, used when the WEIGHTS_PATH env var is unset.
DEFAULT_WEIGHTS = MODEL_DIR / "model_weights" / "teste20260429163505_best.pth"

from .AAA_Configs import CLOSING_RADII, CLOSING_RADIUS  # noqa: E402
