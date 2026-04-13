import os
import re
from qgis.core import QgsProject, QgsRasterLayer, QgsMultiBandColorRenderer
from qgis.utils import iface

'''
This script is designed to be run within the QGIS Python Console. It will:
1. Identify the most recent (or a specific) reconstructed 6-band GeoTIFF file based on a 13-digit timestamp in the filename.
2. Create a group structure in the QGIS Layers panel for 6-band, 4-band, and 2-band layers.
3. Load the identified 6-band reconstructed file and its corresponding 4-band and 2-band original files (if they exist) into their respective groups.
4. Set the symbology for the 6-band layer to use specific bands for RGB visualization.
5. Adjust the map canvas to focus on the extent of the loaded layers.   
Make sure to update the `base_path` variable to point to the directory where your reconstructed and original GeoTIFF files are stored. 
The script assumes that the filenames contain a 13-digit timestamp that can be used to match the corresponding files across the different directories.
'''

# --- CONFIGURATION ---
base_path = r'C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\temp\test_tif_to_hdf5'
path_6b_dir = os.path.join(base_path, 'reconstructed_tifs')
path_4b_dir = os.path.join(base_path, '4bands')
path_2b_dir = os.path.join(base_path, '2bands')
index_image_to_load=10 # Set to -1 to load the most recent file based on timestamp, or set to a specific index (0-based) to load a specific file from the sorted list of files in the 6bands directory.

QgsProject.instance().clear()

# Helper to find a file in a directory that contains a specific 13-digit string
def find_file_by_timestamp(directory, timestamp_str):
    if not os.path.exists(directory):
        return None
    for f in os.listdir(directory):
        if timestamp_str in f and f.endswith('.tif'):
            return os.path.join(directory, f)
    return None

# 1. Identify the reference reconstructed file
files_6b = sorted([f for f in os.listdir(path_6b_dir) if f.endswith('.tif')])

if not files_6b:
    print("No reconstructed files found.")
else:
    if index_image_to_load == -1:
        filename_6b = files_6b[-1] # Assuming the most recent file is the one we want to visualize
    else:
        filename_6b = files_6b[index_image_to_load]
    
    # Extract the 13-digit timestamp using regex
    match = re.search(r'(\d{13})', filename_6b)
    if not match:
        print(f"Could not find a 13-digit timestamp in {filename_6b}")
    else:
        ts_str = match.group(1)
        print(f"Matching timestamp: {ts_str}")

        # --- 2. Create Group Structure ---
        root = QgsProject.instance().layerTreeRoot()
        grp_6 = root.addGroup("6bands")
        grp_4 = root.addGroup("4bands")
        grp_2 = root.addGroup("2bands")

        # --- 3. Locate and Load Layers ---
        # Find matching originals based on the 13-digit ID
        path_6b = os.path.join(path_6b_dir, filename_6b)
        path_4b = find_file_by_timestamp(path_4b_dir, ts_str)
        path_2b = find_file_by_timestamp(path_2b_dir, ts_str)

        layers = []
        
        # Load 6-band (Reconstructed)
        l6 = QgsRasterLayer(path_6b, f"Recon_6b_{ts_str}")
        if l6.isValid():
            QgsProject.instance().addMapLayer(l6, False)
            grp_6.addLayer(l6)
            layers.append(l6)
            
            # Setup Symbology (RGB=8,4,3 -> Indices 4,3,2)
            renderer = QgsMultiBandColorRenderer(l6.dataProvider(), 4, 3, 2)
            l6.setRenderer(renderer)
            l6.setDefaultContrastEnhancement()

        # Load 4-band (Original)
        if path_4b:
            l4 = QgsRasterLayer(path_4b, f"Orig_4b_{ts_str}")
            if l4.isValid():
                QgsProject.instance().addMapLayer(l4, False)
                grp_4.addLayer(l4)
                l4.setDefaultContrastEnhancement()

        # Load 2-band (Original)
        if path_2b:
            l2 = QgsRasterLayer(path_2b, f"Orig_2b_{ts_str}")
            if l2.isValid():
                QgsProject.instance().addMapLayer(l2, False)
                grp_2.addLayer(l2)
                l2.setDefaultContrastEnhancement()

        # --- 4. Final View adjustment ---
        if layers:
            l6.triggerRepaint()
            iface.mapCanvas().setExtent(l6.extent())
            iface.mapCanvas().refresh()
            print("Visualization successfully loaded via timestamp matching.")