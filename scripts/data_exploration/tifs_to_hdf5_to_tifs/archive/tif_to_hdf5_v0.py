import os
import re
import numpy as np
import h5py
import rasterio
from rasterio.windows import from_bounds
from datetime import datetime, timezone

'''
This script reads the HDF5 file created by the previous scripts, extracts the data, and reconstructs georeferenced GeoTIFF files for each timestamp. The output GeoTIFFs will be compressed using LZW and will include the original spatial metadata for accurate georeferencing.   
Make sure to adjust the paths and configurations as needed before running the script.
Key Steps:
1. Load the HDF5 file and read the datasets (values, xs, ys, ts).
2. Determine the spatial grid dimensions and calculate the affine transform.
3. For each timestamp, create a GeoTIFF file with the corresponding bands, applying or not  LZW compression (reduces file size by 50%) and using the original spatial metadata for georeferencing.  
'''

folder_path = r'C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\temp\test_tif_to_hdf5'
folder_path_4bands = os.path.join(folder_path, '4bands')
folder_path_2bands = os.path.join(folder_path, '2bands')
h5_filename = os.path.join(folder_path, 'satellite_data_6bands.h5')

# Define the band order based on the stacking logic below
# Folder '4bands' (B3, B4, B8, B12) followed by Folder '2bands' (B2, B11)
band_names = ["B3", "B4", "B8", "B12", "B2", "B11"]
    
# Updated to 6 bands (4 from folder A + 2 from folder B)
nbands = len(band_names) 

# 1. Parse and Sort Files (Using 4bands as the reference)
files = [f for f in os.listdir(folder_path_4bands) if f.endswith('.tif')]
file_metadata = []
for f in files:
    match = re.search(r'_(\d{13})\.tif', f)
    if match:
        ts_ms = int(match.group(1)) # Keep the full 13-digit integer
        dt = datetime.fromtimestamp(ts_ms / 1000.0, timezone.utc).date()
        file_metadata.append({
            'filename': f, 
            'ordinal': dt.toordinal(), 
            'timestamp_ms': ts_ms  # Store the raw value
        })

file_metadata.sort(key=lambda x: x['ordinal'])
sorted_files = [m['filename'] for m in file_metadata]

# 2. Find the Common Intersection Bounding Box
# (Assuming all bands across both folders cover the same spatial extent)
print("Calculating common intersection...")
lefts, bottoms, rights, tops = [], [], [], []

for f in sorted_files:
    with rasterio.open(os.path.join(folder_path_4bands, f)) as src:
        b = src.bounds
        lefts.append(b.left); bottoms.append(b.bottom)
        rights.append(b.right); tops.append(b.top)

inter_left, inter_bottom = max(lefts), max(bottoms)
inter_right, inter_top = min(rights), min(tops)

# 3. Get Dimensions and Coordinates
with rasterio.open(os.path.join(folder_path_4bands, sorted_files[0])) as src:
    inter_window = from_bounds(inter_left, inter_bottom, inter_right, inter_top, src.transform)
    win_width = int(inter_window.width)
    win_height = int(inter_window.height)
    total_pixels = win_width * win_height
    
    win_transform = src.window_transform(inter_window)
    cols, rows = np.meshgrid(np.arange(win_width), np.arange(win_height))
    xs, ys = rasterio.transform.xy(win_transform, rows, cols, offset='ul')
    xs_flat = np.array(xs, dtype=np.int32).flatten()
    ys_flat = np.array(ys, dtype=np.int32).flatten()

print(f"Intersection Shape: {win_height}x{win_width} ({total_pixels} pixels)")

# 4. Write to HDF5
with h5py.File(h5_filename, 'w') as h5f:
    dset_values = h5f.create_dataset("values", (len(sorted_files), nbands, total_pixels), 
                                     dtype='uint16', chunks=(1, nbands, 1000000))
    
    # Store band names as a fixed-length string attribute
    h5f.attrs['band_names'] = [n.encode('ascii') for n in band_names]
    
    h5f.create_dataset("xs", data=xs_flat, dtype='int32')
    h5f.create_dataset("ys", data=ys_flat, dtype='int32')
    h5f.create_dataset("ts", data=[m['ordinal'] for m in file_metadata], dtype='int32')
    # Add this new dataset for the raw timestamps
    h5f.create_dataset("original_timestamps", 
                       data=[m['timestamp_ms'] for m in file_metadata], 
                       dtype='int64') # int64 handles large 13-digit numbers

    for i, filename in enumerate(sorted_files):
        print(f"Processing {i+1}/{len(sorted_files)}: {filename}")
        
        path_4b = os.path.join(folder_path_4bands, filename)
        path_2b = os.path.join(folder_path_2bands, filename)
        
        with rasterio.open(path_4b) as src4, rasterio.open(path_2b) as src2:
            # Efficient block reading
            data4 = src4.read(window=inter_window)[:, :win_height, :win_width].reshape(4, -1)
            data2 = src2.read(window=inter_window)[:, :win_height, :win_width].reshape(2, -1)
            
            # Fast concatenation
            dset_values[i, :, :] = np.concatenate([data4, data2], axis=0)

print(f"Done! Created {h5_filename} with band metadata.")