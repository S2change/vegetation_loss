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

_here = Path(__file__).resolve().parent
sys.path.insert(0, str(_here))

import numpy as np
import torch
from torchvision import transforms

import AAA_Configs
from swin_ynet import Encoder
from data.dataset_swin_GZ import _to_uint8

# NOTE: chip-level post-processing (per-class morphological closing + small-
# component removal) lives in block_postprocess.chip_records.postprocess_prediction,
# shared across all models and applied by predict_block.py after this module
# returns raw labels. AAA_Configs.CLOSING_RADII / MIN_PATCH_SIZE feed it via
# the bacdm package (see bacdm/__init__.py).


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
    AAA_Configs, then applies ToTensor + Normalize.
    """
    arr = _to_uint8(arr)                        # (H, W, C) uint8
    arr = arr[:, :, AAA_Configs.reversed_nums]  # band selection / reorder
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


def predict_before_after_chips(before_batch, after_batch, model_or_path,
                               device=None):
    """Segment a batch of before/after 256×256 chip pairs.

    Returns RAW model output (argmax + any per-class threshold override).
    Chip-level post-processing — per-class morphological closing and small-
    component removal — is applied downstream by predict_block.py via the
    shared block_postprocess.chip_records.postprocess_prediction, so every model
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

    return pred.cpu().numpy().astype(np.uint8)
