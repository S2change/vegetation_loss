"""
predict.py — Batch inference for BACDM / Swin-YNet.

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

    model = load_model("weights_best.pth")          # load once
    for before_batch, after_batch in data_stream:
        labels = predict_before_after_chips(before_batch, after_batch, model)
"""

import sys
from pathlib import Path

# Allow imports whether this script is called from scripts/bacdm/ or anywhere else
_here    = Path(__file__).resolve().parent   # scripts/bacdm/
_scripts = _here.parent                      # scripts/
sys.path.insert(0, str(_scripts))
sys.path.insert(0, str(_here))

import numpy as np
import torch
from torchvision import transforms
from scipy.ndimage import binary_closing, label as nd_label

import AAA_Configs
from swin_ynet import Encoder

MIN_PATCH_SIZE = getattr(AAA_Configs, 'MIN_PATCH_SIZE', 25)
CLOSING_RADIUS = getattr(AAA_Configs, 'CLOSING_RADIUS',  3)

def postprocess_prediction(pred, min_size=MIN_PATCH_SIZE, closing_radius=CLOSING_RADIUS):
    """Morphological closing + small-component removal (mirrors test.py)."""
    out  = pred.copy()
    r    = closing_radius
    gy, gx = np.ogrid[-r:r+1, -r:r+1]
    disk = (gx**2 + gy**2) <= r**2
    valid = pred < 255

    for cls in [c for c in np.unique(pred[valid]) if c != 0]:
        closed = binary_closing(pred == cls, structure=disk)
        out[closed & (out == 0) & valid] = cls

    for cls in [c for c in np.unique(out[valid]) if c != 0]:
        labeled, n = nd_label(out == cls)
        for i in range(1, n + 1):
            if (labeled == i).sum() < min_size:
                out[labeled == i] = 0

    return out
from data.dataset_swin_GZ import _to_uint8   # reuse the 16→8-bit converter

# ---------------------------------------------------------------------------
# Module-level transform (ToTensor + Normalize) — identical to dataset_swin_GZ
# ---------------------------------------------------------------------------
_TRANSFORM = transforms.Compose([
    transforms.ToTensor(),   # (H, W, C) uint8 → (C, H, W) float32 in [0, 1]
    transforms.Normalize(AAA_Configs.normalization_mean, AAA_Configs.normalization_std),
])


def _chip_to_tensor(arr):
    """Convert one (H, W, C) chip (uint8 or uint16) to a normalised float tensor.

    Applies _to_uint8 (no-op if already uint8), selects/reorders bands via
    AAA_Configs.selected_nums, then applies ToTensor + Normalize.
    """
    arr = _to_uint8(arr)                        # (H, W, C) uint8
    arr = arr[:, :, AAA_Configs.selected_nums]  # band selection / reorder
    return _TRANSFORM(arr)                      # (C, H, W) float32


def load_model(weights_path, device=None):
    """Load Swin-YNet from a .pth checkpoint and return it in eval mode.

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
            'cuda' if torch.cuda.is_available() and getattr(AAA_Configs, 'USE_CUDA', False)
            else 'cpu'
        )
    model = Encoder(num_classes=AAA_Configs.NUM_CLASSES).to(device)
    state = torch.load(weights_path, map_location=device, weights_only=False)
    model.load_state_dict(state)
    model.eval()
    return model


def predict_before_after_chips(before_batch, after_batch, model_or_path, device=None):
    """Segment a batch of before/after 256×256 chip pairs.

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
             Class mapping is defined by AAA_Configs.CLASS_NAMES, e.g.:
               0 = Background, 1 = Cuts, 2 = Fires
    """
    if device is None:
        device = torch.device(
            'cuda' if torch.cuda.is_available() and getattr(AAA_Configs, 'USE_CUDA', False)
            else 'cpu'
        )

    if isinstance(model_or_path, (str, Path)):
        model = load_model(model_or_path, device)
    else:
        model = model_or_path

    # Stack individual chips into (B, C, H, W) tensors
    t_before = torch.stack([_chip_to_tensor(before_batch[i])
                            for i in range(len(before_batch))]).to(device)
    t_after  = torch.stack([_chip_to_tensor(after_batch[i])
                            for i in range(len(after_batch)) ]).to(device)

    with torch.no_grad():
        outputs = model(t_before, t_after)
        # outputs[0] is the full-resolution logits: (B, NUM_CLASSES, H, W)
        probs = torch.softmax(outputs[0], dim=1)   # (B, C, H, W)
        pred  = torch.argmax(probs, dim=1)          # (B, H, W)

        # Apply per-class threshold overrides (mirrors test.py and animate script)
        cuts_threshold = getattr(AAA_Configs, 'CUTS_THRESHOLD', None)
        if cuts_threshold is not None:
            cuts_id = next(
                (k for k, v in AAA_Configs.CLASS_NAMES.items() if v == 'Cuts'), None
            )
            if cuts_id is not None:
                pred[probs[:, cuts_id] > cuts_threshold] = cuts_id

    labels = pred.cpu().numpy().astype(np.uint8)
    return np.stack([postprocess_prediction(labels[i]) for i in range(len(labels))])
