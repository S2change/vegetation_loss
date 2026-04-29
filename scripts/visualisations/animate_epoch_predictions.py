r'''
Animate BACDM segmentation predictions across training epochs for a single chip.

Produces an animated GIF where each frame shows:
  - Background : percentile-stretched RGB composite of the "after" chip
  - Foreground : per-epoch model prediction overlay (class 0 = transparent)
  - Label      : epoch number stamped on the top-left corner

Inputs are configured in the "Configuration" block below.  The script mirrors
the data paths used in qgis_load_before_after_prediction_labels.py but adds:
  - CHIP_PREFIX   : stem of the chip to animate, e.g. "vchip_665975_4428695_20200527_01"
  - WEIGHTS_DIR   : folder holding all epoch checkpoints
  - WEIGHTS_PREFIX: shared filename prefix up to the "_NN.pth" epoch suffix,
                    e.g. "303b_10bands_softLR_0110101002_LR01_G00311224_A4510_20260424170531"

Run from the repo root, or from scripts/bacdm/, since the script adds the
relevant parent directories to sys.path automatically.
'''

import sys
import os
from pathlib import Path

# Allow imports of AAA_Configs (in scripts/) and swin_ynet (in scripts/bacdm/)
_scripts_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_scripts_dir))
sys.path.insert(0, str(_scripts_dir / 'bacdm'))

import re
import random
import numpy as np
import torch
import rasterio
import imageio.v2 as imageio_v2
from PIL import Image, ImageDraw
from torchvision import transforms

import AAA_Configs
from swin_ynet import Encoder

# ---------------------------------------------------------------------------
# Configuration  — edit these paths/values before running
# ---------------------------------------------------------------------------

# Filter: select chips that contain pixels of this vegetation-loss type.
# Must match a value in AAA_Configs.CLASS_NAMES, e.g. "Cuts" or "Fires".
TYPE = "Cuts"
MAX_NUMBER_EPOCHS=100

TESTING_ROOT   = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\testing_data"
BEFORE_DIR     = os.path.join(TESTING_ROOT, "before")
AFTER_DIR      = os.path.join(TESTING_ROOT, "after")
LABEL_DIR      = os.path.join(TESTING_ROOT, "label")        # ground-truth PNGs
PRED_DIR       = os.path.join(TESTING_ROOT, "predictions")  # pre-computed TIFs (optional)


# Set to a specific chip stem to skip auto-selection, e.g.:
#   CHIP_PREFIX = "vchip_665975_4428695_20200527_01"
# Leave as None to pick a random chip that contains TYPE pixels.
CHIP_PREFIX    = None

WEIGHTS_DIR    = AAA_Configs.WEIGHTS_DIR
WEIGHTS_PREFIX = AAA_Configs.WEIGHTS_PREFIX

# 1-indexed band numbers for R, G, B in the 10-band TIF
# Matches the QGIS QgsMultiBandColorRenderer(..., 3, 8, 9) call
RGB_BANDS      = (3, 8, 9)

MIN_NUMBER_CHANGE_PIXELS = 25  # minimum pixels of TYPE required to include a chip
PRED_ALPHA     = 255   # opacity (0–255) for non-background prediction overlay
FRAME_DURATION = 300   # ms per frame in the output GIF
# ---------------------------------------------------------------------------

USE_CUDA = torch.cuda.is_available() and getattr(AAA_Configs, 'USE_CUDA', False)
device   = torch.device('cuda' if USE_CUDA else 'cpu')

# ---------------------------------------------------------------------------
# Probability visualisation — viridis ramp over 7 bins from 0.20 to 1.0
# ---------------------------------------------------------------------------
# Bin edges: probabilities below the first edge are transparent (background).
PROB_BIN_EDGES = [0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 1.01]

# Viridis RGB values sampled evenly from t=0.20 to t=1.00 (one per bin).
_VIRIDIS_7 = [
    ( 62,  74, 138),  # t=0.20 — dark blue/purple
    ( 43, 104, 141),  # t=0.33 — blue
    ( 41, 129, 139),  # t=0.47 — teal
    ( 71, 160, 121),  # t=0.60 — blue-green
    (128, 187,  90),  # t=0.73 — green
    (193, 213,  42),  # t=0.87 — yellow-green
    (253, 231,  37),  # t=1.00 — yellow
]


def find_epoch_weights(weights_dir, prefix):
    """Return sorted list of (epoch_int, abs_path) for files matching prefix_NN.pth."""
    pattern = re.compile(rf"^{re.escape(prefix)}.*_(\d{{1,2}})\.pth$")
    results = []
    for fname in os.listdir(weights_dir):
        m = pattern.match(fname)
        if m:
            results.append((int(m.group(1)), os.path.join(weights_dir, fname)))
    results.sort(key=lambda x: x[0])
    return results


def select_chip(type_name, before_dir, pred_dir, label_dir):
    """Return a random chip stem whose TYPE pixels are non-zero.

    Checks pre-computed predictions in pred_dir first (fast); falls back to
    ground-truth labels in label_dir if no predictions are available.
    """
    type_class_id = next(
        (k for k, v in AAA_Configs.CLASS_NAMES.items() if v == type_name), None
    )
    if type_class_id is None:
        raise ValueError(f"TYPE='{type_name}' not found in CLASS_NAMES: {AAA_Configs.CLASS_NAMES}")

    candidates = []

    # --- try existing predictions first ---
    pred_path = Path(pred_dir)
    if pred_path.exists():
        for tif in sorted(pred_path.glob("*.tif")):
            stem = tif.stem
            if not (Path(before_dir) / f"{stem}.tif").exists():
                continue
            with rasterio.open(str(tif)) as src:
                pred_arr = src.read(1)
            if np.sum(pred_arr == type_class_id) > MIN_NUMBER_CHANGE_PIXELS:
                candidates.append(stem)

    # --- fall back to ground-truth labels ---
    if not candidates:
        label_path = Path(label_dir)
        for png in sorted(label_path.glob("*.png")):
            stem = png.stem
            if not (Path(before_dir) / f"{stem}.tif").exists():
                continue
            label_arr = np.array(Image.open(str(png)))
            if AAA_Configs.CLASS_REMAP is not None:
                label_arr = AAA_Configs.CLASS_REMAP[label_arr].astype(np.uint8)
            if np.sum(label_arr == type_class_id) > MIN_NUMBER_CHANGE_PIXELS:
                candidates.append(stem)

    if not candidates:
        raise ValueError(
            f"No chips found with type '{type_name}' (class {type_class_id}) "
            f"in {pred_dir!r} or {label_dir!r}"
        )

    chosen = random.choice(candidates)
    source = "predictions" if Path(pred_dir).exists() and any(
        Path(pred_dir).glob("*.tif")) else "labels"
    print(f"Auto-selected chip ({len(candidates)} candidates with '{type_name}' "
          f"from {source}): {chosen}")
    return chosen


def load_chip_tensor(tif_path):
    """Load a chip TIF and return a normalised (1, C, H, W) float32 tensor.

    Replicates the preprocessing in MyTestData.transform() so that model
    inputs are identical to those produced during test.py inference.
    """
    arr = imageio_v2.imread(tif_path)          # (H, W, C) uint8, same as dataset loader
    arr = arr[:, :, AAA_Configs.selected_nums]
    tfm = transforms.Compose([
        transforms.ToTensor(),                 # (H,W,C) → (C,H,W), scale to [0,1]
        transforms.Normalize(AAA_Configs.normalization_mean, AAA_Configs.normalization_std),
    ])
    return tfm(arr).unsqueeze(0)               # (1, C, H, W)


def run_inference(model, before_t, after_t, type_class_id):
    """Return a (H, W) float32 array of TYPE-class probabilities."""
    with torch.no_grad():
        outputs = model(before_t.to(device), after_t.to(device))
    probs = torch.softmax(outputs[0], dim=1)          # [1, C, H, W]
    return probs[0, type_class_id].cpu().numpy()      # [H, W] float32


def make_rgb_background(tif_path):
    """Build a (H, W, 3) uint8 RGB from the after-chip TIF.

    Uses RGB_BANDS with a per-band 2–98 percentile stretch.
    Nodata pixels (value 255) are rendered as mid-grey (128).
    """
    with rasterio.open(tif_path) as src:
        bands = [src.read(b).astype(np.float32) for b in RGB_BANDS]

    def stretch(band):
        valid = band[band < 255]
        if valid.size == 0:
            return np.full_like(band, 128, dtype=np.uint8)
        lo, hi = np.percentile(valid, [2, 98])
        if hi <= lo:
            hi = lo + 1.0
        out = np.clip((band - lo) / (hi - lo) * 255, 0, 255)
        out[band == 255] = 128
        return out.astype(np.uint8)

    return np.stack([stretch(b) for b in bands], axis=2)


def colorize_probability(prob_arr):
    """Convert (H, W) float32 probability array → (H, W, 4) RGBA.

    Probabilities below PROB_BIN_EDGES[0] are transparent (background).
    Each bin maps to the corresponding viridis colour in _VIRIDIS_7.
    """
    rgba = np.zeros((*prob_arr.shape, 4), dtype=np.uint8)
    edges = PROB_BIN_EDGES
    for i, (lo, hi) in enumerate(zip(edges[:-1], edges[1:])):
        mask = (prob_arr >= lo) & (prob_arr < hi)
        if mask.any():
            rgba[mask, :3] = _VIRIDIS_7[i]
            rgba[mask,  3] = PRED_ALPHA
    return rgba


def _stamp_label(img_rgba, text):
    """Stamp a semi-transparent text pill on the top-left of an RGBA image."""
    draw = ImageDraw.Draw(img_rgba)
    tw = 6 * len(text) + 4
    draw.rectangle([(3, 3), (3 + tw, 15)], fill=(0, 0, 0, 160))
    draw.text((5, 4), text, fill=(255, 255, 255, 255))


def build_static_frame(rgb_arr, label):
    """Return a plain RGB frame with a text label (no prediction overlay)."""
    frame = Image.fromarray(rgb_arr, mode='RGB').convert('RGBA')
    _stamp_label(frame, label)
    return frame.convert('RGB')


def build_label_frame(rgb_bg, label_arr, type_class_id):
    """Show the ground-truth TYPE-class mask as a solid yellow overlay."""
    rgba = np.zeros((*label_arr.shape, 4), dtype=np.uint8)
    mask = label_arr == type_class_id
    print(f"  Label: {mask.sum()} pixels of type {type_class_id}")
    rgba[mask] = (255, 255, 0, 255)   # solid yellow, fully opaque
    base  = Image.fromarray(rgb_bg, mode='RGB').convert('RGBA')
    ov    = Image.fromarray(rgba, mode='RGBA')
    frame = Image.alpha_composite(base, ov)
    _stamp_label(frame, "Label")
    return frame.convert('RGB')


def build_frame(rgb_bg, type_probs, epoch):
    """Composite TYPE-class probability overlay over the RGB background."""
    base  = Image.fromarray(rgb_bg, mode='RGB').convert('RGBA')
    ov    = Image.fromarray(colorize_probability(type_probs), mode='RGBA')
    frame = Image.alpha_composite(base, ov)
    _stamp_label(frame, f"Epoch {epoch:02d}")
    return frame.convert('RGB')


def main():
    chip_prefix = CHIP_PREFIX or select_chip(TYPE, BEFORE_DIR, PRED_DIR, LABEL_DIR)

    before_path = os.path.join(BEFORE_DIR, f"{chip_prefix}.tif")
    after_path  = os.path.join(AFTER_DIR,  f"{chip_prefix}.tif")

    for p, name in [(before_path, 'before'), (after_path, 'after')]:
        if not os.path.exists(p):
            raise FileNotFoundError(f"{name} chip not found: {p}")

    epoch_weights = find_epoch_weights(WEIGHTS_DIR, WEIGHTS_PREFIX)
    if not epoch_weights:
        raise FileNotFoundError(
            f"No checkpoints found in {WEIGHTS_DIR!r} matching prefix {WEIGHTS_PREFIX!r}"
        )
    print(f"Found {len(epoch_weights)} checkpoints — epochs "
          f"{epoch_weights[0][0]:02d} to {epoch_weights[-1][0]:02d}.")

    before_t    = load_chip_tensor(before_path)
    after_t     = load_chip_tensor(after_path)
    rgb_before  = make_rgb_background(before_path)
    rgb_after   = make_rgb_background(after_path)

    type_class_id = next(
        (k for k, v in AAA_Configs.CLASS_NAMES.items() if v == TYPE), None
    )
    if type_class_id is None:
        raise ValueError(f"TYPE='{TYPE}' not found in CLASS_NAMES: {AAA_Configs.CLASS_NAMES}")

    model = Encoder(num_classes=AAA_Configs.NUM_CLASSES).to(device)
    model.eval()

    label_path = os.path.join(LABEL_DIR, f"{chip_prefix}.png")
    if not os.path.exists(label_path):
        raise FileNotFoundError(f"Label PNG not found: {label_path}")
    label_arr = np.array(Image.open(label_path))
    label_arr[label_arr == 255] = 0   # treat NoData as background before remap
    if AAA_Configs.CLASS_REMAP is not None:
        label_arr = AAA_Configs.CLASS_REMAP[label_arr].astype(np.uint8)

    frames  = [build_static_frame(rgb_before, "Before")] * 10
    frames += [build_static_frame(rgb_after,  "After")]  * 10
    frames += [build_label_frame(rgb_after, label_arr, type_class_id)] * 10

    for epoch, wpath in epoch_weights:
        if epoch <= MAX_NUMBER_EPOCHS:
            print(f"  Epoch {epoch:02d}  {os.path.basename(wpath)}")
            state = torch.load(wpath, map_location=device, weights_only=False)
            model.load_state_dict(state)
            type_probs = run_inference(model, before_t, after_t, type_class_id)
            frames.append(build_frame(rgb_after, type_probs, epoch))

    OUTPUT_GIF     = rf"C:\Users\mlc\Downloads\temp\training_evolution_{TYPE}_{chip_prefix}.gif"

    print(f"Saving {len(frames)}-frame animation → {OUTPUT_GIF}")
    frames[0].save(
        OUTPUT_GIF,
        save_all=True,
        append_images=frames[1:],
        duration=FRAME_DURATION,
        loop=0,
    )
    print("Done.")


if __name__ == "__main__":
    main()
