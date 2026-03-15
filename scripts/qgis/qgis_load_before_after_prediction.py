import os
import sys
from qgis.core import QgsProject, QgsRasterLayer
from qgis.utils import iface
from qgis.core import (QgsProject, QgsRasterLayer, QgsRasterMinMaxOrigin, 
                       QgsMultiBandColorRenderer, QgsSingleBandGrayRenderer)

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


def load_tifs_to_group(folder_path, group_name, RGB=True, position=None):
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
    
    # 2. Iterate and Load Files
    files = [f for f in os.listdir(absolute_folder) if f.lower().endswith('.tif')]
    for f in sorted(files):
        path = os.path.join(absolute_folder, f)
        layer = QgsRasterLayer(path, f)
        
        if layer.isValid():
            # 3. Apply Symbology Logic
            if RGB and layer.bandCount() >= 5:
                # Set Renderer to Bands 3, 4, 5
                renderer = QgsMultiBandColorRenderer(layer.dataProvider(), 3, 4, 5)
                layer.setRenderer(renderer)
                
                # Apply Cumulative Cut only for RGB
                min_max_origin = QgsRasterMinMaxOrigin()
                min_max_origin.setLimits(QgsRasterMinMaxOrigin.CumulativeCut)
                min_max_origin.setCumulativeCutLower(0.015)
                min_max_origin.setCumulativeCutUpper(0.985)
                layer.renderer().setMinMaxOrigin(min_max_origin)
            else:
                # Default to Singleband Gray (Band 1) without custom stretch
                renderer = QgsSingleBandGrayRenderer(layer.dataProvider(), 1)
                layer.setRenderer(renderer)
            
            # 4. Add to Project and Group
            QgsProject.instance().addMapLayer(layer, False)
            group.addLayer(layer)
            layer.triggerRepaint()
        else:
            print(f"Failed to load: {f}")

# --- EXECUTION ---

# 1) Load 'after' group (Bottom)
load_tifs_to_group(Test_im_pathB, 'after')

# 2) Load 'before' group (Middle)
load_tifs_to_group(Test_im_pathA, 'before')

# 3) Load 'prediction' group (Top - Position 0)
load_tifs_to_group(Test_det_path, 'prediction', position=0)

print("Layers loaded successfully.")