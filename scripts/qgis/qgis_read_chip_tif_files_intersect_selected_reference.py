import os
import shutil
import sys
from qgis.core import (QgsProject, QgsCoordinateTransform, QgsCoordinateReferenceSystem, 
                       QgsRasterLayer, QgsCoordinateTransformContext)
from qgis.utils import iface

'''
Script to read TIF files from a source folder, check if they intersect with the selected features in a specified QGIS layer, 
and copy the intersecting TIFs to a destination folder for further processing.

Inputs:
- A QGIS layer with selected features that define the area of interest (e.g., "ardida_2024"): these are the "target features"
- A source folder containing all available 16-band TIF chip files (configured in AAA_Configs.py as chip_source_folder)
- A destination folder where the selected TIF files will be copied (configured in AAA_Configs.py as Input_dir)
- The path to the shapefile to which the target features belong (configured in AAA_Configs.py as shp_path); this is only needed to determine the CRS of the target features for accurate intersection checks
Outputs:
- The TIF files that intersect with the selected features will be copied to the destination folder (Input_dir) for further processing by split_tifs.py
- The intersecting TIF files will also be loaded into the current QGIS project for visualization     
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

###########################################################################################  read variable names from AAA_Configs
chip_source_folder=None # where all available chips are; configured in  by AAA_Configs.py
Input_dir=None # where selected chips will be stored; configured in AAA_Configs.py
shp_path=None # path to shapefile to which target feature belongs; configured in AAA_Configs.py
with open(os.path.join(os.path.dirname(working_dir), 'AAA_Configs.py')) as f:    
    exec(f.read()) # 

# Resolve relative_paths wrt to working_dir if it starts with '.'; otherwise leave it as is (e.g., absolute paths or paths that are already correctly resolved)
def resolve_path(base_dir, relative_path):
    """Joins and normalizes a path if it starts with a dot."""
    if relative_path.startswith('.'):
        return os.path.normpath(os.path.join(base_dir, relative_path))
    return relative_path

# Resolve paths from AAA_Configs
destination_folder = resolve_path(working_dir, Input_dir) 
chip_source_folder = resolve_path(working_dir, chip_source_folder) # where all available chi
shp_path = resolve_path(working_dir, shp_path) # just to define the layer name from the shapefile name; the actual loading of the layer is not needed, as we will just read the geometries directly from the shapefile using OGR to avoid issues with missing or incorrect CRS in the shapefile layer in QGIS; but we still need the path to determine the target_layer_name for intersection checks and for cleanup of other layers in the project
target_layer_name=os.path.splitext(os.path.basename(shp_path))[0] # QGIS layer name to which the target feature belongs; derived from the shapefile name by default, but can be set to a custom name if needed

# Create destination folder if it doesn't exist
print(f"Destination folder resolved to: {destination_folder}")

if not os.path.exists(destination_folder):
    os.makedirs(destination_folder)
# delete all files in destination folder before copying new files
for f in os.listdir(destination_folder):
    file_path = os.path.join(destination_folder, f)
    if os.path.isfile(file_path):
        os.remove(file_path)            
######################################

# --- 1) Cleanup: Remove all layers except reference layer
layers_dict = QgsProject.instance().mapLayers()
for layer_id, layer in layers_dict.items():
    if layer.name() != target_layer_name:
        QgsProject.instance().removeMapLayer(layer_id)

# Get reference to the main layer
main_layers = QgsProject.instance().mapLayersByName(target_layer_name)
if not main_layers:
    print(f"Error: Layer '{target_layer_name}' not found!")
else:
    main_layer = main_layers[0]
    selected_features = main_layer.selectedFeatures()

    if not selected_features:
        print(f"Action Required: Please select at least one feature in '{target_layer_name}'!")
    else:
        # --- 2) Prepare Geometry & CRS Transformation ---
        # Combine all selected geometries
        combined_geom = selected_features[0].geometry()
        for i in range(1, len(selected_features)):
            combined_geom = combined_geom.combine(selected_features[i].geometry())

        # Determine CRS of the 1st available chip TIF to use as destination CRS for intersection checks; this is safer than relying on the CRS of the shapefile layer in QGIS, which may be missing or incorrect; if no valid TIFs are found, we will proceed with the original CRS of the combined geometry, but this may lead to inaccurate intersection checks if the TIFs have a different CRS
        tifs_in_folder = [f for f in os.listdir(chip_source_folder) if f.lower().endswith('.tif')]
        if tifs_in_folder:
            first_tif = os.path.join(chip_source_folder, tifs_in_folder[0])
            first_tif_layer = QgsRasterLayer(first_tif, os.path.basename(first_tif))
            if first_tif_layer.isValid():
                dest_crs = first_tif_layer.crs()
            else:
                print(f"Warning: First TIF '{first_tif}' is invalid. Proceeding with original CRS of the combined geometry for intersection checks, which may lead to inaccuracies if the TIFs have a different CRS.")
                dest_crs = QgsCoordinateReferenceSystem() # Default CRS if TIF is invalid
        else:
            print(f"Warning: No valid TIF files found in '{chip_source_folder}'. Proceeding with original CRS of the combined geometry for intersection checks, which may lead to inaccuracies if the TIFs have a different CRS.")
            dest_crs = QgsCoordinateReferenceSystem() # Default CRS if no TIFs found

        # Set up coordinate transformation to match the TIF chips CRS (assuming chips_crs is defined in AAA_Configs.py)
        src_crs = main_layer.crs()
        transform = QgsCoordinateTransform(src_crs, dest_crs, QgsProject.instance())
        
        # Transform the selection to the Tiff's coordinate system
        combined_geom.transform(transform)
        selection_bbox = combined_geom.boundingBox()

        # --- 3) Iterate, Load, and Copy ---
        tifs_in_folder = [f for f in os.listdir(chip_source_folder) if f.lower().endswith('.tif')]
        
        # clear the destination folder before copying new files
        for f in os.listdir(destination_folder):
            file_path = os.path.join(destination_folder, f)
            if os.path.isfile(file_path):
                os.remove(file_path)
        
        # Iterate through TIF files, check intersection, load into QGIS, and copy to destination folder
        for tif_name in tifs_in_folder:
            tif_path = os.path.join(chip_source_folder, tif_name)
            tmp_layer = QgsRasterLayer(tif_path, tif_name)
            
            if tmp_layer.isValid():
                # Check intersection in EPSG:32629
                if tmp_layer.extent().intersects(selection_bbox):
                    # Load into QGIS
                    QgsProject.instance().addMapLayer(tmp_layer)
                    
                    # Copy to destination folder
                    dest_path = os.path.join(destination_folder, tif_name)
                    shutil.copy2(tif_path, dest_path) # copy2 preserves metadata
                    print(f"Loaded and Copied: {tif_name}")

        # --- 4) Move reference layer to top and refresh canvas ---
        root = QgsProject.instance().layerTreeRoot()
        main_layer_node = root.findLayer(main_layer.id())
        if main_layer_node:
            clone = main_layer_node.clone()
            root.insertChildNode(0, clone)
            root.removeChildNode(main_layer_node)

        iface.mapCanvas().refresh()
        print(f"Finished. Files copied to: {destination_folder}")
