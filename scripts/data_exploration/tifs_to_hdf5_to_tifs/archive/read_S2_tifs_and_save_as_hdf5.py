
import os
import re
import numpy as np
import h5py
import rasterio
from rasterio.windows import from_bounds
from datetime import datetime, timezone

folder_path = r'C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\temp\test_tif_to_hdf5'
folder_path_4bands = r'C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\temp\test_tif_to_hdf5\4bands'
h5_filename = os.path.join(folder_path, 'satellite_data_intersected.h5')
nbands = 4  # Assuming we want to read the first 4 bands (e.g., B3, B4, B8, B12 for Sentinel-2)

# 1. Parse and Sort Files (as before)
files = [f for f in os.listdir(folder_path_4bands) if f.endswith('.tif')]
file_metadata = []
for f in files:
    match = re.search(r'_(\d{13})\.tif', f)
    if match:
        dt = datetime.fromtimestamp(int(match.group(1)) / 1000.0, timezone.utc).date()
        file_metadata.append({'filename': f, 'ordinal': dt.toordinal()})

file_metadata.sort(key=lambda x: x['ordinal'])
sorted_files = [m['filename'] for m in file_metadata]

# 2. Find the Common Intersection Bounding Box
print("Calculating common intersection...")
lefts, bottoms, rights, tops = [], [], [], []

for f in sorted_files:
    with rasterio.open(os.path.join(folder_path_4bands, f)) as src:
        b = src.bounds
        lefts.append(b.left); bottoms.append(b.bottom)
        rights.append(b.right); tops.append(b.top)

# The intersection is the innermost boundary
inter_left, inter_bottom = max(lefts), max(bottoms)
inter_right, inter_top = min(rights), min(tops)

# 3. Get Dimensions from first file using the intersection window
with rasterio.open(os.path.join(folder_path_4bands, sorted_files[0])) as src:
    res = src.res[0] # Pixel size (usually 10 or 20)
    # Define the window
    inter_window = from_bounds(inter_left, inter_bottom, inter_right, inter_top, src.transform)
    # Round to integers to get pixel dimensions
    win_width = int(inter_window.width)
    win_height = int(inter_window.height)
    total_pixels = win_width * win_height
    
    # Calculate XY coordinates for this specific window
    win_transform = src.window_transform(inter_window)
    cols, rows = np.meshgrid(np.arange(win_width), np.arange(win_height))
    xs, ys = rasterio.transform.xy(win_transform, rows, cols)
    xs_flat = np.array(xs, dtype=np.int32).flatten()
    ys_flat = np.array(ys, dtype=np.int32).flatten()

print(f"Intersection Shape: {win_height}x{win_width} ({total_pixels} pixels)")

# 4. Write to HDF5
with h5py.File(h5_filename, 'w') as h5f:
    dset_values = h5f.create_dataset("values", (len(sorted_files), nbands, total_pixels), 
                                     dtype='uint16', chunks=(1, nbands, 1000000))
    h5f.create_dataset("xs", data=xs_flat, dtype='int32')
    h5f.create_dataset("ys", data=ys_flat, dtype='int32')
    h5f.create_dataset("ts", data=[m['ordinal'] for m in file_metadata], dtype='int32')

    for i, filename in enumerate(sorted_files):
        print(f"Processing {i+1}/{len(sorted_files)}: {filename}")
        with rasterio.open(os.path.join(folder_path_4bands, filename)) as src:
            # Read only the intersection window
            data = src.read([1, 2, 3, 4], window=inter_window)
            # Ensure data matches the expected flat shape exactly
            # (Sometimes window math results in 1-pixel difference, so we crop/pad to be safe)
            data_flat = data[:, :win_height, :win_width].reshape(4, -1)
            dset_values[i, :, :] = data_flat

print("Done! Data aligned and saved.")