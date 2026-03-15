import os
import re
from qgis.core import (QgsProject, QgsRasterLayer, QgsContrastEnhancement, 
                       QgsMultiBandColorRenderer, QgsRasterMinMaxOrigin)


''' 
Script to load GeoTIFF files from a specified folder into QGIS, grouping them by a date extracted from their filenames. 
Each group corresponds to a unique date, and the layers within each group are styled with an RGB 11-10-9 band combination and a Gaussian stretch based on 2 standard deviations. The script also ensures that any existing groups with the same names are removed before loading new ones, and it moves the newly created groups to the bottom of the layer tree for better organization.
Inputs:
- folder_path: The directory containing the GeoTIFF files to be loaded.
- date_pattern: A regular expression pattern to extract the date from the filenames (default is r'_(\d{8})_').
Outputs:
- GeoTIFF layers added to QGIS, organized into groups based on the extracted dates, with specified symbology and contrast enhancement.
'''

# --- CONFIGURATION ---
folder_path = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\chips"
date_pattern = re.compile(r'_(\d{8})_')

# 1. Gather files and group by date
files = [f for f in os.listdir(folder_path) if f.endswith('.tif')]
date_groups = {}
for f in files:
    match = date_pattern.search(f)
    if match:
        date = match.group(1)
        date_groups.setdefault(date, []).append(f)

sorted_dates = sorted(date_groups.keys())
root = QgsProject.instance().layerTreeRoot()

# 2. CLEANUP: Remove existing groups with the same names
for date in sorted_dates:
    existing_group = root.findGroup(date)
    if existing_group:
        # This removes the group and all layers inside it from the project
        root.removeChildNode(existing_group)

# 3. Process each date group
for date in sorted_dates:
    # Create the group at the root level initially
    group = root.addGroup(date)
    # Set group to invisible by default to prevent UI lag during loading
    group.setItemVisibilityChecked(False)
    
    for filename in date_groups[date]:
        path = os.path.join(folder_path, filename)
        layer = QgsRasterLayer(path, filename)
        
        if not layer.isValid():
            continue

        # --- SYMBOLOGY: RGB 10-9-8 ---
        renderer = QgsMultiBandColorRenderer(layer.dataProvider(), 11, 10, 9)
        layer.setRenderer(renderer)
        
        # --- GAUSSIAN STRETCH (2 Std Dev) ---
        min_max_origin = QgsRasterMinMaxOrigin()
        min_max_origin.setLimits(QgsRasterMinMaxOrigin.StdDev)
        layer.renderer().setMinMaxOrigin(min_max_origin)
        
        # Apply the Stretch algorithm
        layer.setContrastEnhancement(
            QgsContrastEnhancement.StretchToMinimumMaximum, 
            QgsRasterMinMaxOrigin.StdDev
        )
        
        # Add to project (not to legend yet)
        QgsProject.instance().addMapLayer(layer, False)
        # Add to our specific date group
        group.addLayer(layer)

# 4. Move groups to the bottom of the tree
for date in sorted_dates:
    node_group = root.findGroup(date)
    if node_group:
        node_copy = node_group.clone()
        root.insertChildNode(-1, node_copy)
        root.removeChildNode(node_group)

print(f"Cleaned up and re-loaded {len(sorted_dates)} date groups.")