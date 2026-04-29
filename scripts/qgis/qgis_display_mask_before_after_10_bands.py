"""
QGIS Python console script.

Randomly picks N_CHIPS mask rasters from MASK_DIR and, for each one, builds a
group containing (top → bottom): mask | after | before.

RGB display uses bands 3/8/9 stretched to mean ± 2 × stddev per band.
Mask uses a simple palette: 0=background (transparent), 1–4 = vegetation-loss classes.
"""

import os
import random

from qgis.core import (
    QgsContrastEnhancement,
    QgsMultiBandColorRenderer,
    QgsPalettedRasterRenderer,
    QgsProject,
    QgsRasterBandStats,
    QgsRasterLayer,
)
from qgis.PyQt.QtGui import QColor

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MASK_DIR   = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\vchips\mask_rasters"
BEFORE_DIR = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\vchips\before_vchips"
AFTER_DIR  = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\vchips\after_vchips"

RED_BAND   = 3   # 1-based band indices used for RGB display
GREEN_BAND = 8
BLUE_BAND  = 9

N_CHIPS = 10     # how many chips to display

# Palette: value, RGBA colour, label
# Background is fully transparent so the RGB layer below shows through.
MASK_CLASSES = [
    QgsPalettedRasterRenderer.Class(0, QColor(128, 128, 128,   0), "background"),
    QgsPalettedRasterRenderer.Class(1, QColor(255, 140,   0, 220), "clear_cuts"),
    QgsPalettedRasterRenderer.Class(2, QColor(255, 255,   0, 220), "other_cuts"),
    QgsPalettedRasterRenderer.Class(3, QColor(  0, 180,   0, 220), "agri"),
    QgsPalettedRasterRenderer.Class(4, QColor(220,  30,  30, 220), "fire"),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _contrast_enhancement(provider, band):
    """Stretch a single band to mean ± 2 × stddev."""
    stats = provider.bandStatistics(band, QgsRasterBandStats.All)
    ce = QgsContrastEnhancement(provider.dataType(band))
    ce.setContrastEnhancementAlgorithm(QgsContrastEnhancement.StretchToMinimumMaximum)
    ce.setMinimumValue(stats.mean - 2.0 * stats.stdDev)
    ce.setMaximumValue(stats.mean + 2.0 * stats.stdDev)
    return ce


def load_rgb_layer(path, name):
    layer = QgsRasterLayer(path, name)
    if not layer.isValid():
        print(f"  [WARN] Could not load: {path}")
        return None
    dp = layer.dataProvider()
    renderer = QgsMultiBandColorRenderer(dp, RED_BAND, GREEN_BAND, BLUE_BAND)
    renderer.setRedContrastEnhancement(_contrast_enhancement(dp, RED_BAND))
    renderer.setGreenContrastEnhancement(_contrast_enhancement(dp, GREEN_BAND))
    renderer.setBlueContrastEnhancement(_contrast_enhancement(dp, BLUE_BAND))
    layer.setRenderer(renderer)
    QgsProject.instance().addMapLayer(layer, False)
    return layer


def load_mask_layer(path, name):
    layer = QgsRasterLayer(path, name)
    if not layer.isValid():
        print(f"  [WARN] Could not load: {path}")
        return None
    renderer = QgsPalettedRasterRenderer(layer.dataProvider(), 1, MASK_CLASSES)
    layer.setRenderer(renderer)
    QgsProject.instance().addMapLayer(layer, False)
    return layer

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

mask_files = sorted(f for f in os.listdir(MASK_DIR) if f.endswith("_mask.tif"))
if not mask_files:
    raise FileNotFoundError(f"No *_mask.tif files found in {MASK_DIR}")

selected = random.sample(mask_files, min(N_CHIPS, len(mask_files)))
print(f"Selected {len(selected)} chips from {len(mask_files)} available.")

root = QgsProject.instance().layerTreeRoot()

for mask_file in selected:
    # 'vchip_648575_4499185_20220715_mask.tif' → '648575_4499185_20220715'
    chip_id = mask_file[len("vchip_"):-len("_mask.tif")]

    mask_path   = os.path.join(MASK_DIR,   mask_file)
    before_path = os.path.join(BEFORE_DIR, f"vchip_{chip_id}_mask_before.tif")
    after_path  = os.path.join(AFTER_DIR,  f"vchip_{chip_id}_mask_after.tif")

    missing = [p for p in (before_path, after_path) if not os.path.exists(p)]
    if missing:
        print(f"Skipping {chip_id}: missing files {missing}")
        continue

    print(f"Loading {chip_id} ...")
    group = root.insertGroup(0, chip_id)

    # Insert order determines visual order (first added = top of group).
    for path, loader, label in [
        (mask_path,   load_mask_layer, "mask"),
        (after_path,  load_rgb_layer,  "after"),
        (before_path, load_rgb_layer,  "before"),
    ]:
        lyr = loader(path, label)
        if lyr:
            group.addLayer(lyr)

print("Done.")
