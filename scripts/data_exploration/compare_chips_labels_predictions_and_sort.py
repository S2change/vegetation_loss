import os
import sys
import numpy as np
from PIL import Image
from skimage.measure import label as connected_components
from sklearn.metrics import classification_report
import pandas as pd

# Allow importing AAA_Configs from the parent scripts/ directory
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import AAA_Configs

# Define paths
label_dir = r'C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\testing_data\label'
pred_dir  = r'C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\testing_data\predictions'

NUM_CLASSES  = AAA_Configs.NUM_CLASSES
CLASS_NAMES  = AAA_Configs.CLASS_NAMES          # {new_id: name}
NON_BG_CLASSES = list(range(1, NUM_CLASSES))

PATCH_MIN_SIZE = 25  # patches smaller than this (in pixels) are ignored in patch-level metrics

# ── Accumulators ──────────────────────────────────────────────────────────────
results           = []
label_accumulated = np.array([])
pred_accumulated  = np.array([])

# patch_matrix[true_c, pred_c]:
#   number of label patches of class true_c whose dominant predicted class is pred_c
#   pred_c == 0  →  patch was entirely missed (no non-bg prediction inside it)
patch_matrix = np.zeros((NUM_CLASSES, NUM_CLASSES), dtype=int)

# false_alarms[pred_c]:  prediction patches of class pred_c with no label-patch overlap
# total_pred_patches[pred_c]: total prediction patches of class pred_c (for the FA rate)
false_alarms       = np.zeros(NUM_CLASSES, dtype=int)
total_pred_patches = np.zeros(NUM_CLASSES, dtype=int)


def get_patches(arr, cls, min_size=1):
    """Return boolean masks for connected components of `cls`, skipping those smaller than min_size."""
    binary = (arr == cls)
    if not binary.any():
        return []
    labeled, n = connected_components(binary, connectivity=2, return_num=True)
    return [m for i in range(1, n + 1) if (m := labeled == i).sum() >= min_size]


filenames = [os.path.splitext(f)[0] for f in os.listdir(label_dir) if f.endswith('.png')]

for name in filenames:
    label_path = os.path.join(label_dir, f"{name}.png")
    pred_path  = os.path.join(pred_dir,  f"{name}.tif")
    if not os.path.exists(pred_path):
        continue

    label_arr = np.array(Image.open(label_path))
    pred_arr  = np.array(Image.open(pred_path))

    # Remap labels to the grouped class space if a grouping is active
    if AAA_Configs.CLASS_REMAP is not None:
        label_arr = AAA_Configs.CLASS_REMAP[label_arr].astype(np.uint8)

    # ── Patch-level: label patches ────────────────────────────────────────────
    # Build valid_pixels in the same pass: background always included; non-bg
    # pixels only if they belong to a label patch >= PATCH_MIN_SIZE.
    valid_pixels = (label_arr == 0)
    for true_c in NON_BG_CLASSES:
        for patch_mask in get_patches(label_arr, true_c, min_size=PATCH_MIN_SIZE):
            valid_pixels |= patch_mask
            pred_pixels = pred_arr[patch_mask]
            pred_nonbg  = pred_pixels[pred_pixels != 0]
            if len(pred_nonbg) == 0:
                dominant_pred = 0  # entirely missed
            else:
                counts        = np.bincount(pred_nonbg, minlength=NUM_CLASSES)
                dominant_pred = int(np.argmax(counts))
            patch_matrix[true_c, dominant_pred] += 1

    # ── Patch-level: flag prediction patches with no label-patch overlap ──────
    for pred_c in NON_BG_CLASSES:
        for patch_mask in get_patches(pred_arr, pred_c, min_size=PATCH_MIN_SIZE):
            total_pred_patches[pred_c] += 1
            if not np.any(label_arr[patch_mask] != 0):
                false_alarms[pred_c] += 1

    # ── Pixel-level accumulation (small label patches excluded) ───────────────
    label_accumulated = np.concatenate((label_accumulated, label_arr[valid_pixels]))
    pred_accumulated  = np.concatenate((pred_accumulated,  pred_arr[valid_pixels]))

    mask = (label_arr != 0) & valid_pixels
    total_non_bg = int(np.sum(mask))
    accuracy = (float(np.sum((label_arr == pred_arr) & mask)) / total_non_bg * 100
                if total_non_bg > 0 else 0.0)
    results.append({'filename': name, 'accuracy': accuracy, 'non_zero_pixels': total_non_bg})


# ── Pixel-level classification report ────────────────────────────────────────
print("=" * 60)
print(f"PIXEL-LEVEL CLASSIFICATION REPORT  (label patches < {PATCH_MIN_SIZE} px excluded)")
print("=" * 60)
print(classification_report(
    label_accumulated, pred_accumulated,
    labels=list(range(NUM_CLASSES)),
    target_names=[CLASS_NAMES[i] for i in range(NUM_CLASSES)],
    zero_division=0))

# ── Patch-level confusion matrix ─────────────────────────────────────────────
print("=" * 60)
print("PATCH-LEVEL CONFUSION MATRIX")
print("Rows = true label class | Cols = dominant predicted class")
print("'missed' col = label patch has no non-bg prediction overlap")
print("=" * 60)

col_labels = {**{0: 'missed'}, **{c: CLASS_NAMES[c] for c in NON_BG_CLASSES}}
row_labels  = [CLASS_NAMES[c] for c in NON_BG_CLASSES]
col_display = [col_labels[c] for c in range(NUM_CLASSES)]

df = pd.DataFrame(
    patch_matrix[1:, :],   # rows 1..N (skip unused row 0)
    index=row_labels,
    columns=col_display,
)
df['total'] = df.sum(axis=1)
df['patch_recall'] = [
    patch_matrix[c, c] / patch_matrix[c, :].sum()
    if patch_matrix[c, :].sum() > 0 else 0.0
    for c in NON_BG_CLASSES
]
print(df.to_string(float_format=lambda x: f"{x:.2f}"))

# ── False-alarm patches ───────────────────────────────────────────────────────
print()
print("=" * 60)
print("FALSE-ALARM PATCHES  (prediction patches with no label overlap)")
print("=" * 60)
for c in NON_BG_CLASSES:
    fa    = false_alarms[c]
    total = total_pred_patches[c]
    pct   = 100.0 * fa / total if total > 0 else 0.0
    print(f"  {CLASS_NAMES[c]:12s}: {fa:5d} / {total:5d} pred patches are false alarms  ({pct:.1f}%)")

# ── Per-chip pixel accuracy sorted ───────────────────────────────────────────
sorted_results = sorted(results, key=lambda x: x['accuracy'], reverse=True)

if False:
    print(f"\n{'Filename':<25} | {'Accuracy (%)':<15} | {'Non-Zero Pixels':<15}")
    print("-" * 60)
    for item in sorted_results:
        print(f"{item['filename']:<25} | {item['accuracy']:>11.2f}% | {item['non_zero_pixels']:>15}")
