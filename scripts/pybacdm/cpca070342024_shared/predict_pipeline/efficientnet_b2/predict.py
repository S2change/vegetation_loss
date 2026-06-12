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
from scipy.ndimage import binary_closing, label as nd_label
import segmentation_models_pytorch as smp

import configs

# ── Preprocessing constants ───────────────────────────────────────────────────
_NODATA_16 = 65535
_NODATA_8  = 255

_MEAN = torch.tensor(configs.NORM_MEAN).view(-1, 1, 1)   # (10, 1, 1)
_STD  = torch.tensor(configs.NORM_STD).view(-1, 1, 1)    # (10, 1, 1)


# ── Preprocessing ─────────────────────────────────────────────────────────────

def _to_uint8(arr_hwc):
    """(H, W, C) uint16 → uint8 via per-band q0.02–q0.98 stretch.

    NoData (65535) maps to 255. Returns arr unchanged if already uint8.
    """
    if arr_hwc.dtype == np.uint8:
        return arr_hwc
    arr   = arr_hwc.astype(np.float32)
    nodata = arr == _NODATA_16
    arr[nodata] = np.nan
    out = np.empty(arr.shape, dtype=np.uint8)
    for b in range(arr.shape[2]):
        band, nd = arr[:, :, b], nodata[:, :, b]
        q02, q98 = np.nanpercentile(band, [2, 98])
        denom = float(q98 - q02) if q98 > q02 else 1.0
        scaled = np.clip((band - q02) / denom * (_NODATA_8 - 1), 0, _NODATA_8 - 1)
        scaled[nd] = _NODATA_8
        out[:, :, b] = scaled.astype(np.uint8)
    return out


def _chip_to_tensor(arr_hwc):
    """(H, W, C) uint16 or uint8 chip → normalised (C, H, W) float32 tensor."""
    u8 = _to_uint8(arr_hwc)
    t  = torch.from_numpy(u8.transpose(2, 0, 1)).float() / 255.0
    return (t - _MEAN) / _STD


# ── Post-processing ───────────────────────────────────────────────────────────

def _disk(r):
    """Boolean disk structuring element of integer radius r."""
    r = int(r)
    gy, gx = np.ogrid[-r:r + 1, -r:r + 1]
    return (gx ** 2 + gy ** 2) <= r ** 2


def _class_radius(cls):
    """Per-class closing radius — CLOSING_RADII override, else CLOSING_RADIUS."""
    return int(configs.CLOSING_RADII.get(int(cls), configs.CLOSING_RADIUS))


def postprocess_prediction(pred, min_size=None, closing_radius=None, remove_small=True):
    """Per-class morphological closing + optional small-component removal.

    Closing uses a per-class radius from configs.CLOSING_RADII (Cuts→3, Fires→1),
    falling back to configs.CLOSING_RADIUS for unlisted classes.

    Pass an int closing_radius to force one radius for all classes.
    Set remove_small=False to close only — used at chip level so chips vote on
    closed-but-unfiltered output; size filtering is deferred to block/master stages.
    """
    if min_size is None:
        min_size = configs.MIN_PATCH_SIZE
    out   = pred.copy()
    valid = pred < _NODATA_8

    for cls in [c for c in np.unique(pred[valid]) if c != 0]:
        r = closing_radius if closing_radius is not None else _class_radius(cls)
        closed = binary_closing(pred == cls, structure=_disk(r))
        out[closed & (out == 0) & valid] = cls

    if remove_small:
        for cls in [c for c in np.unique(out[valid]) if c != 0]:
            labeled, n = nd_label(out == cls)
            for i in range(1, n + 1):
                if (labeled == i).sum() < min_size:
                    out[labeled == i] = 0

    return out


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
            'cuda' if torch.cuda.is_available() and getattr(configs, 'USE_CUDA', False)
            else 'cpu'
        )
    model = smp.Unet(
        encoder_name="efficientnet-b2",
        encoder_weights=None,          # no ImageNet init at inference time
        in_channels=configs.NUM_BANDS * 2,   # 10 before + 10 after = 20
        classes=configs.NUM_CLASSES,
        activation=None,
    ).to(device)
    state = torch.load(weights_path, map_location=device, weights_only=False)
    model.load_state_dict(state)
    model.eval()
    return model


# ── Public inference API ──────────────────────────────────────────────────────

def predict_before_after_chips(before_batch, after_batch, model_or_path,
                               device=None, postprocess=True):
    """Segment a batch of before/after 256×256 chip pairs.

    Parameters
    ----------
    before_batch  : array-like of (H, W, C) chips, shape (B, H, W, C), uint8 or uint16
    after_batch   : array-like of (H, W, C) chips, shape (B, H, W, C), uint8 or uint16
    model_or_path : nn.Module already loaded by load_model(), OR a str/Path to a .pth file.
                    Pass a pre-loaded model when calling this function repeatedly to avoid
                    reloading weights on every batch.
    device        : torch.device or None (auto-detected)
    postprocess   : bool (default True). When True, each chip's label map is
                    passed through postprocess_prediction with remove_small=False —
                    per-class morphological closing only (Cuts→3, Fires→1). Size
                    filtering is deferred to block/master stages where patches are whole.
                    Set False to return raw model output.

    Returns
    -------
    labels : np.ndarray, shape (B, H, W), dtype uint8
             Predicted class index per pixel.
             0 = Background, 1 = Cuts, 2 = Fires  (see configs.CLASS_NAMES)
    """
    if device is None:
        device = torch.device(
            'cuda' if torch.cuda.is_available() and getattr(configs, 'USE_CUDA', False)
            else 'cpu'
        )

    if isinstance(model_or_path, (str, Path)):
        model = load_model(model_or_path, device)
    else:
        model = model_or_path

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

        if configs.CUTS_THRESHOLD is not None:
            cuts_id = next(
                (k for k, v in configs.CLASS_NAMES.items() if v == 'Cuts'), None
            )
            if cuts_id is not None:
                pred[probs[:, cuts_id] > configs.CUTS_THRESHOLD] = cuts_id

    labels = pred.cpu().numpy().astype(np.uint8)
    if postprocess:
        return np.stack([postprocess_prediction(labels[i], remove_small=False)
                         for i in range(len(labels))])
    return labels
