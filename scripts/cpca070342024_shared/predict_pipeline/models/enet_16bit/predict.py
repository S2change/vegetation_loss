"""
predict.py — Batch inference for EfficientNet-B2 U-Net change detection.

Designed for HPC use where the model is loaded once and called repeatedly
on batches of before/after 256×256 chips.

Public API
----------
load_model(weights_path, device=None)  → nn.Module (eval mode)

predict_before_after_chips(
    before_batch, after_batch, model_or_path, device=None
) → np.ndarray (B, H, W) uint8

Typical HPC usage
-----------------
    from predict import load_model, predict_before_after_chips

    model = load_model("weights/best_model.pth")   # load once
    for before_batch, after_batch in data_stream:
        labels = predict_before_after_chips(before_batch, after_batch, model)
"""

import sys
from pathlib import Path

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here))

import numpy as np
import torch
import segmentation_models_pytorch as smp

import configs as C

# NOTE: chip-level post-processing (per-class morphological closing + small-
# component removal) lives in postprocess.chip_records.postprocess_prediction,
# shared across all models and applied by predict_block.py after this module
# returns raw labels. configs.CLOSING_RADII / MIN_PATCH_SIZE feed it via the
# efficientnet_b2 package (see efficientnet_b2/__init__.py).


# ── Preprocessing ─────────────────────────────────────────────────────────────

def _chip_to_tensor(arr_hwc):
    """(H, W, C) uint16 chip → (C, H, W) float32 tensor in [0, 1].

    Divides by SCALE_FACTOR (10000). NoData pixels (NODATA_UINT16) are set
    to 0.0 so they do not produce out-of-range activations in the encoder.
    """
    arr    = arr_hwc.astype(np.float32)
    nodata = arr == C.NODATA_UINT16
    arr    = np.clip(arr / C.SCALE_FACTOR, 0.0, 1.0)
    arr[nodata] = 0.0
    return torch.from_numpy(arr.transpose(2, 0, 1))


# ── Model ─────────────────────────────────────────────────────────────────────

def load_model(weights_path, device=None):
    """Load EfficientNet-B2 U-Net from a .pth checkpoint; return in eval mode.

    Parameters
    ----------
    weights_path : str or Path
    device       : torch.device or None — auto-detected when None

    Returns
    -------
    model : nn.Module, on `device`, in eval mode
    """
    if device is None:
        device = torch.device(
            'cuda' if torch.cuda.is_available() and getattr(C, 'USE_CUDA', True)
            else 'cpu'
        )
    model = smp.Unet(
        encoder_name="efficientnet-b2",
        encoder_weights=None,
        in_channels=C.NUM_BANDS * 2,
        classes=C.NUM_CLASSES,
        activation=None,
    ).to(device)
    state = torch.load(weights_path, map_location=device, weights_only=False)
    model.load_state_dict(state)
    model.eval()
    return model


# ── Public inference API ──────────────────────────────────────────────────────

def predict_before_after_chips(before_batch, after_batch, model_or_path,
                               device=None):
    """Segment a batch of before/after 256×256 chip pairs.

    Returns RAW model output (argmax + any per-class threshold override).
    Chip-level post-processing — per-class morphological closing and small-
    component removal — is applied downstream by predict_block.py via the
    shared postprocess.chip_records.postprocess_prediction, so every model
    is cleaned up identically and the chip-level close matches the block-
    level one (polygonize.close_labels).

    Parameters
    ----------
    before_batch  : array-like of (H, W, C) chips, shape (B, H, W, C), uint8 or uint16
    after_batch   : array-like of (H, W, C) chips, shape (B, H, W, C), uint8 or uint16
    model_or_path : nn.Module already loaded by load_model(), OR a str/Path to a .pth file.
                    Pass a pre-loaded model when calling this function repeatedly to avoid
                    reloading weights on every batch.
    device        : torch.device or None (auto-detected)

    Returns
    -------
    labels : np.ndarray, shape (B, H, W), dtype uint8
             Predicted class index per pixel.
             0 = Background, 1 = Cuts, 2 = Fires  (see configs.CLASS_NAMES)
    """
    if device is None:
        device = torch.device(
            'cuda' if torch.cuda.is_available() and getattr(C, 'USE_CUDA', True)
            else 'cpu'
        )

    if isinstance(model_or_path, (str, Path)):
        model = load_model(model_or_path, device)
    else:
        model = model_or_path

    # Nodata mask: True where either image has nodata in any band
    nodata_mask = np.stack([
        np.any(before_batch[i] == C.NODATA_UINT16, axis=-1) |
        np.any(after_batch[i]  == C.NODATA_UINT16, axis=-1)
        for i in range(len(before_batch))
    ])  # (B, H, W) bool

    # Preprocess and stack: (B, 10, H, W) before + after → (B, 20, H, W)
    t_before = torch.stack([_chip_to_tensor(before_batch[i])
                            for i in range(len(before_batch))]).to(device)
    t_after  = torch.stack([_chip_to_tensor(after_batch[i])
                            for i in range(len(after_batch))]).to(device)
    x = torch.cat([t_before, t_after], dim=1)   # (B, 20, H, W) — before first

    with torch.no_grad():
        logits = model(x)                            # (B, NUM_CLASSES, H, W)
        probs  = torch.softmax(logits, dim=1)
        pred   = torch.argmax(probs, dim=1)          # (B, H, W)

        if C.CUTS_THRESHOLD is not None:
            cuts_id = next(
                (k for k, v in C.CLASS_NAMES.items() if v == 'Cuts'), None
            )
            if cuts_id is not None:
                pred[probs[:, cuts_id] > C.CUTS_THRESHOLD] = cuts_id

        if getattr(C, 'FIRES_THRESHOLD', None) is not None:
            fires_id = next(
                (k for k, v in C.CLASS_NAMES.items() if v == 'Fires'), None
            )
            if fires_id is not None:
                pred[(pred == fires_id) & (probs[:, fires_id] <= C.FIRES_THRESHOLD)] = 0

    labels = pred.cpu().numpy().astype(np.uint8)
    labels[nodata_mask] = C.NODATA_UINT8
    return labels
