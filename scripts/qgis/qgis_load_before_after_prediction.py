import os
import random
import sys
from pathlib import Path
from PIL import Image
import numpy as np
import rasterio
from qgis.core import (QgsProject, QgsRasterLayer, QgsRasterMinMaxOrigin, 
                       QgsMultiBandColorRenderer, QgsPalettedRasterRenderer)
from qgis.utils import iface
from PyQt5.QtGui import QColor
from qgis.core import QgsRectangle

# --- PATHS AND CONFIGS ---
label_dir = r'C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\label'

# (Working directory and AAA_Configs logic remains same as your snippet)
import inspect
try:
    script_path = inspect.getfile(inspect.currentframe())
except:
    import console
    script_path = iface.mainWindow().findChild(console.console.PythonConsole).findChild(console.console_editor.EditorTabWidget).currentWidget().file_path()

working_dir = os.path.dirname(os.path.abspath(script_path))

# Load AAA_Configs
Test_im_pathA, Test_im_pathB, Test_det_path = None, None, None
with open(os.path.join(os.path.dirname(working_dir), 'AAA_Configs.py')) as f: 
    exec(f.read())

def resolve_path(base_dir, relative_path):
    if relative_path.startswith('.'):
        return os.path.normpath(os.path.join(base_dir, relative_path))
    return relative_path

Test_im_pathA = resolve_path(working_dir, Test_im_pathA)
Test_im_pathB = resolve_path(working_dir, Test_im_pathB)
Test_det_path = resolve_path(working_dir, Test_det_path)

# --- HELPER FUNCTIONS ---

def get_comparison_rankings(pred_folder, label_folder):
    results = []
    pred_files = [f for f in os.listdir(pred_folder) if f.endswith('.tif')]
    for f_name in pred_files:
        stem = os.path.splitext(f_name)[0]
        label_path = os.path.join(label_folder, f"{stem}.png")
        pred_path = os.path.join(pred_folder, f_name)
        if not os.path.exists(label_path): continue
        with rasterio.open(pred_path) as p_src:
            pred_arr = p_src.read(1)
        label_arr = np.array(Image.open(label_path))
        mask = (label_arr != 0)
        total_non_bg = np.sum(mask)
        acc = (np.sum((label_arr == pred_arr) & mask) / total_non_bg * 100) if total_non_bg > 0 else 0.0
        results.append({'filename': f_name, 'accuracy': acc, 'non_zero_pixels': int(total_non_bg)})
    return results

def select_stratified_samples(results):
    sorted_acc = sorted(results, key=lambda x: x['accuracy'])
    n = len(sorted_acc)
    tiers = [sorted_acc[:n//3], sorted_acc[n//3 : 2*n//3], sorted_acc[2*n//3:]]
    final_selection = []
    for tier in tiers:
        tier_sorted = sorted(tier, key=lambda x: x['non_zero_pixels'])
        m = len(tier_sorted)
        final_selection.extend(random.sample(tier_sorted[:m//2], min(2, m//2)))
        final_selection.extend(random.sample(tier_sorted[m//2:], min(2, m-m//2)))
    return final_selection

def load_layers_to_group(folder_path, filenames, group_name, RGB=True, position=None, extension_override=None):
    root = QgsProject.instance().layerTreeRoot()
    group = root.findGroup(group_name)
    if not group:
        group = root.insertGroup(0, group_name) if position == 0 else root.addGroup(group_name)
            
    absolute_folder = os.path.normpath(os.path.join(working_dir, folder_path))
    
    for item in filenames:
        stem = os.path.splitext(item['filename'])[0]
        ext = extension_override if extension_override else os.path.splitext(item['filename'])[1]
        f_full = f"{stem}{ext}"
        
        path = os.path.join(absolute_folder, f_full)
        layer = QgsRasterLayer(path, f_full)
        
        if layer.isValid():
            # 1. ADD TO PROJECT FIRST
            # This ensures subsequent changes are "registered" to the project instance
            QgsProject.instance().addMapLayer(layer, False)
            group.addLayer(layer)

            # 2. CRS AND GEOTRANSFORM FIX
            if ext.lower() == '.png':
                prediction_path = os.path.join(Test_det_path, f"{stem}.tif")
                if os.path.exists(prediction_path):
                    with rasterio.open(prediction_path) as src:
                        # Apply CRS
                        crs_wkt = src.crs.to_wkt()
                        qgis_crs = QgsCoordinateReferenceSystem()
                        qgis_crs.createFromWkt(crs_wkt)
                        layer.setCrs(qgis_crs)
                        
                        # Apply Extent
                        b = src.bounds
                        rect = QgsRectangle(b.left, b.bottom, b.right, b.top)
                        layer.setExtent(rect)

            # 3. SYMBOLOGY & VISIBILITY
            # 3. SYMBOLOGY & VISIBILITY
            if not RGB:
                # --- AGGRESSIVE VISIBILITY FIX ---
                # 1. Clear any existing NoData/Transparency masks
                layer.dataProvider().setNoDataValue(1, -9999) # Set to a value that doesn't exist
                
                # 2. Disable the "Transparency Band" (often band 2 or 4 in PNGs)
                layer.renderer().setAlphaBand(-1) 
                
                # 3. Ensure the layer is 100% opaque regardless of internal alpha
                layer.renderer().setOpacity(1.0)

                # 4. Define the class map
                class_map = {
                    0: {'color': QColor(0, 0, 0, 0), 'label': 'Unchanged'},
                    1: {'color': QColor(255, 0, 0, 255), 'label': 'Clear Cut'},
                    2: {'color': QColor(0, 255, 0, 255), 'label': 'Other 1'},
                    3: {'color': QColor(0, 0, 255, 255), 'label': 'Other 2'},
                    4: {'color': QColor(255, 255, 0, 255), 'label': 'Burned'}
                }
                
                classes = [QgsPalettedRasterRenderer.Class(v, c['color'], c['label']) for v, c in class_map.items()]
                renderer = QgsPalettedRasterRenderer(layer.dataProvider(), 1, classes)
                layer.setRenderer(renderer)
            
            # 4. FINAL REFRESH
            layer.triggerRepaint()

# --- EXECUTION ---
all_stats = get_comparison_rankings(Test_det_path, label_dir)
selected_filenames = select_stratified_samples(all_stats)

QgsProject.instance().clear()

# 1) Background Images
load_layers_to_group(Test_im_pathB, selected_filenames, 'after')
load_layers_to_group(Test_im_pathA, selected_filenames, 'before')

# 2) Ground Truth Labels (PNGs) - Positioned below predictions
load_layers_to_group(label_dir, selected_filenames, 'label', RGB=False, position=0, extension_override='.png')

# 3) Predictions (TIFs) - Top Position
load_layers_to_group(Test_det_path, selected_filenames, 'prediction', RGB=False, position=0)

iface.mapCanvas().refresh()
print(f"Loaded {len(selected_filenames)} sets of comparison layers.")