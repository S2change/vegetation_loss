import os
import random
import sys
from pathlib import Path
from PIL import Image
import numpy as np
import rasterio
from qgis.core import (QgsProject, QgsRasterLayer, QgsRasterMinMaxOrigin, 
                       QgsMultiBandColorRenderer, QgsPalettedRasterRenderer,
                       QgsCoordinateReferenceSystem, QgsRectangle)
from qgis.utils import iface
from PyQt5.QtGui import QColor


# --- NEW DIRECTORY FOR GEOTIFF LABELS ---
label_png_dir = r'C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\label'
label_tif_dir = r'C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\label_tif'
if not os.path.exists(label_tif_dir):
    os.makedirs(label_tif_dir)

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

def convert_png_to_geotiff(filenames, png_dir, ref_tif_dir, output_dir):
    """
    Reads PNG labels and Prediction TIFs to create new GeoTIFF labels 
    with embedded spatial metadata.
    """
    print("Converting labels to GeoTIFF...")
    for item in filenames:
        stem = os.path.splitext(item['filename'])[0]
        png_path = os.path.join(png_dir, f"{stem}.png")
        ref_path = os.path.join(ref_tif_dir, f"{stem}.tif")
        out_path = os.path.join(output_dir, f"{stem}.tif")
        
        if not os.path.exists(out_path): # Skip if already converted
            with rasterio.open(ref_path) as ref:
                meta = ref.meta.copy()
                # Ensure it's 1-band Byte data
                meta.update(driver='GTiff', dtype='uint8', count=1, compress='lzw')
                
                label_data = np.array(Image.open(png_path))
                
                with rasterio.open(out_path, 'w', **meta) as dest:
                    dest.write(label_data.astype('uint8'), 1)


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

def load_layers_to_group(folder_path, filenames, group_name, RGB=True, position=None):
    root = QgsProject.instance().layerTreeRoot()
    group = root.findGroup(group_name)
    if not group:
        group = root.insertGroup(0, group_name) if position == 0 else root.addGroup(group_name)
            
    for item in filenames:
        # Now everything is a .tif
        f_name = f"{os.path.splitext(item['filename'])[0]}.tif"
        path = os.path.join(folder_path, f_name)
        layer = QgsRasterLayer(path, f_name)
        
        if layer.isValid():
            QgsProject.instance().addMapLayer(layer, False)
            group.addLayer(layer)

            if RGB and layer.bandCount() >= 5:
                renderer = QgsMultiBandColorRenderer(layer.dataProvider(), 3, 4, 5)                
                layer.setRenderer(renderer)
                # (Add cumulative cut logic if needed)
            else:
                # Paletted Symbology for discrete values
                class_map = {
                    0: {'color': QColor(0, 0, 0, 0), 'label': 'Unchanged'},
                    1: {'color': QColor(255, 0, 0, 255), 'label': 'Clear Cut'},
                    2: {'color': QColor(0, 255, 0, 255), 'label': 'Other 1'},
                    3: {'color': QColor(0, 0, 255, 255), 'label': 'Other 2'},
                    4: {'color': QColor(255, 255, 0, 255), 'label': 'Burned'}
                }
                classes = [QgsPalettedRasterRenderer.Class(v, c['color'], c['label']) for v, c in class_map.items()]
                layer.setRenderer(QgsPalettedRasterRenderer(layer.dataProvider(), 1, classes))
            
            layer.triggerRepaint()

# --- EXECUTION ---
# 1. Get stats and select 12
all_stats = get_comparison_rankings(Test_det_path, label_png_dir) # Use previous logic
selected_filenames = select_stratified_samples(all_stats)

# 2. Convert ONLY the 12 selected PNGs to GeoTIFF
convert_png_to_geotiff(selected_filenames, label_png_dir, Test_det_path, label_tif_dir)

# 3. Load to QGIS
QgsProject.instance().clear()
load_layers_to_group(Test_im_pathB, selected_filenames, 'after')
load_layers_to_group(Test_im_pathA, selected_filenames, 'before')
load_layers_to_group(label_tif_dir, selected_filenames, 'label', RGB=False, position=0)
load_layers_to_group(Test_det_path, selected_filenames, 'prediction', RGB=False, position=0)

print("Layers loaded into QGIS:)")

iface.mapCanvas().refresh()