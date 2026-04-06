from ctypes.wintypes import RGB
import os
import sys
from pathlib import Path
import numpy as np
import rasterio
from qgis.core import QgsProject, QgsRasterLayer
from qgis.utils import iface
from qgis.core import (QgsProject, QgsRasterLayer, QgsRasterMinMaxOrigin, 
                       QgsMultiBandColorRenderer, QgsSingleBandGrayRenderer)
from qgis.core import QgsPalettedRasterRenderer, QgsColorRampShader, QgsRasterBlock
from PyQt5.QtGui import QColor

'''
Script to load before and after 6-channel geo-referenced TIF files and predicted change maps into QGIS, 
organized in groups for better visualization and comparison.

Inputs:
- The paths to the folders containing the before and after TIF files (configured in AAA_Configs.py as Test_im_pathA and Test_im_pathB); 
these folders will contain the 6-channel geo-referenced TIF files that represent the before and after conditions of the area of interest, 
respectively; the TIF files should have a valid CRS defined within them for accurate loading and visualization in QGIS
- The path to the folder where the predicted change maps are stored (configured in AAA_Configs.py as Test_det_path); 
this folder should contain the predicted change maps as TIF files that represent the predicted changes between the before and after conditions; 
the TIF files should have a valid CRS defined within them for accurate loading and visualization in QGIS

Outputs:
- The before and after TIF files will be loaded into QGIS and organized in separate groups named "before" and "after" for better visualization and comparison; 
the predicted change maps will be loaded into a group named "prediction" and placed above the before and after groups in the layer stack for easy comparison; 
the RGB symbology with 1.5% cumulative cut will be applied to the before and after TIF files for better visualization of the changes, 
while the predicted change maps will be displayed with a default singleband gray symbology without custom stretch to allow for clear visualization of the predicted changes; 
all layers will be added to the current QGIS project and can be further customized or analyzed as needed.   
'''

F=0 # index of first image (0 is the one with most non-0s in predicted)
N=10 # number of images after that (F+N) should be less than the total number of prediction images

######################################### determine working_dir from script location
import inspect
# 1. Get the path of the current script file
# This is more robust than __file__ inside QGIS Editor
try:
    # Frame 0 is the current execution frame
    script_path = inspect.getfile(inspect.currentframe())
except:
    # Absolute Fallback: Hardcode the path during debugging if all else fails
    # script_path = r"C:\Your\Path\To\Script.py"
    import console
    script_path = iface.mainWindow().findChild(console.console.PythonConsole).findChild(console.console_editor.EditorTabWidget).currentWidget().file_path()

working_dir = os.path.dirname(os.path.abspath(script_path))
print(f"Working directory identified as: {working_dir}")

if working_dir not in sys.path:
    sys.path.append(working_dir)

print(working_dir)

#########################################

def get_active_prediction_rankings(pred_folder):
    """
    Scans the specified folder for .tif files, counts the number of pixels with values > 0 in each file, 
    and ranks the files based on this count. Files with no pixels > 0 are counted but not included in the ranking.
    """
    pred_path = Path(pred_folder)
    tif_files = list(pred_path.glob("*.tif"))
    
    rankings = []
    empty_count = 0

    print(f"Analyzing {len(tif_files)} prediction files...")

    for fpath in tif_files:
        try:
            with rasterio.open(fpath) as src:
                data = src.read(1)
                
                # Count pixels where class is 1, 2, 3, or 4
                change_pixel_count = np.count_nonzero(data > 0)
                
                if change_pixel_count > 0:
                    rankings.append({
                        'filename': fpath.name,
                        'change_count': int(change_pixel_count),
                        'percent': (change_pixel_count / data.size) * 100
                    })
                else:
                    empty_count += 1
                    
        except Exception as e:
            print(f"Error reading {fpath.name}: {e}")

    # Sort descending by change_count
    sorted_rankings = sorted(rankings, key=lambda x: x['change_count'], reverse=True)

    r'''print("\n--- Predictions Ranked by Activity (Excluding Empty) ---")
    print("\n--- Predictions Ranked by Activity (Excluding Empty) ---")
    print(f"{'Filename':<45} | {'Pixels > 0':<12} | {'% Coverage'}")
    print("-" * 75)

    for item in sorted_rankings:
        print(f"{item['filename']:<45} | {item['change_count']:>12,} | {item['percent']:>8.2f}%")
    
    print("\n" + "="*30)
    print(f"Total Files Scanned: {len(tif_files)}")
    print(f"Active Files (Shown): {len(sorted_rankings)}")
    print(f"Empty Files (Hidden): {empty_count}")
    print("="*30)
    '''
    return sorted_rankings

######################################## read variable names from AAA_Configs
Test_im_pathA, Test_im_pathB, Test_det_path=None,None,None # to be updated by AAA_Configs.py
# open 'AAA_Configs.py' which is in the parent directory of the current script
with open(os.path.join(os.path.dirname(working_dir), 'AAA_Configs.py')) as f: 
    exec(f.read()) # reads Test_im_pathA, Test_im_pathB, Test_det_path

# Resolve relative to script if it starts with '.'
def resolve_path(base_dir, relative_path):
    """Joins and normalizes a path if it starts with a dot."""
    if relative_path.startswith('.'):
        return os.path.normpath(os.path.join(base_dir, relative_path))
    return relative_path

# Applying the function to your variables
Test_im_pathA = resolve_path(working_dir, Test_im_pathA)
Test_im_pathB = resolve_path(working_dir, Test_im_pathB)
Test_det_path = resolve_path(working_dir, Test_det_path)
######################################

print(Test_im_pathA, Test_im_pathB, Test_det_path)

# --- CLEANUP ---
# Closes the current project and removes all layers/groups from the legend
QgsProject.instance().clear()

# Optional: Refresh the map canvas to ensure it's visually empty
iface.mapCanvas().refresh()


def load_tifs_to_group(folder_path, filenames, group_name, RGB=True, position=None):
    """Loads .tif files. Applies RGB 3-4-5 with 1.5% cut only if RGB=True."""
    root = QgsProject.instance().layerTreeRoot()
    
    # 1. Handle Group Creation/Finding
    group = root.findGroup(group_name)
    if not group:
        if position == 0:
            group = root.insertGroup(0, group_name)
        else:
            group = root.addGroup(group_name)
            
    absolute_folder = os.path.normpath(os.path.join(working_dir, folder_path))
    
    # 4. Iterate through the top chips
    for item in filenames:
        f_name = item['filename']  # Extract the string filename from the dict
        path = os.path.join(absolute_folder, f_name)
        layer = QgsRasterLayer(path, f_name)
        
        if layer.isValid():
            # 3. Apply Symbology Logic
            if RGB and layer.bandCount() >= 5:
                renderer = QgsMultiBandColorRenderer(layer.dataProvider(), 3, 4, 5)                
                layer.setRenderer(renderer)
                
                # Apply Cumulative Cut only for RGB
                min_max_origin = QgsRasterMinMaxOrigin()
                min_max_origin.setLimits(QgsRasterMinMaxOrigin.CumulativeCut)
                min_max_origin.setCumulativeCutLower(0.015)
                min_max_origin.setCumulativeCutUpper(0.985)
                layer.renderer().setMinMaxOrigin(min_max_origin)
            else:
                # 1. Define your classes and their corresponding colors
                # Class 0: Transparent/Black, 1: Red, 2: Green, 3: Blue, 4: Yellow (Burned)
                class_map = {
                    0: {'color': QColor(0, 0, 0, 0), 'label': 'Unchanged'},     # Transparent
                    1: {'color': QColor(255, 0, 0, 255), 'label': 'Clear Cut'}, # Red
                    2: {'color': QColor(0, 255, 0, 255), 'label': 'Other 1'},  # Green
                    3: {'color': QColor(0, 0, 255, 255), 'label': 'Other 2'},     # Blue
                    4: {'color': QColor(255, 255, 0, 255), 'label': 'Burned'}   # Yellow
                }

                # 2. Create the color classes for the renderer
                classes = []
                for val, info in class_map.items():
                    classes.append(QgsPalettedRasterRenderer.Class(val, info['color'], info['label']))
                renderer = QgsPalettedRasterRenderer(layer.dataProvider(), 1, classes)
                layer.setRenderer(renderer)
            
            # 4. Add to Project and Group
            QgsProject.instance().addMapLayer(layer, False)
            group.addLayer(layer)
            layer.triggerRepaint()
        else:
            print(f"Failed to load: {f_name}")

# --- EXECUTION ---

# 0) determine names of chips we want to read based on the number of pixels > 0 in the predicted change maps, and only load the top N most active predictions to avoid overwhelming QGIS with too many layers; this also ensures we focus on the most significant predictions for visualization and analysis
active_list = get_active_prediction_rankings(Test_det_path)
# Take the top N most active chips
filenames = active_list[F:(F+N)] 
print(filenames)

# 1) Load 'after' group (Bottom)
load_tifs_to_group(Test_im_pathB, filenames,'after')

# 2) Load 'before' group (Middle)
load_tifs_to_group(Test_im_pathA, filenames, 'before')

# 3) Load 'prediction' group (Top - Position 0)
load_tifs_to_group(Test_det_path, filenames, 'prediction', position=0)

print("Layers loaded successfully.")