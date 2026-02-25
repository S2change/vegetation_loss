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
folder_path_4bands = r"D:\s2_images\T29TQG"
folder_path_2bands = r"C:\Users\Public\Documents\s2_images_B2_B11\T29TQG"
h5_filename = os.path.join(r"E:\T29TQG", 'T29TQG_6bands.h5')

# Tolerance (in CRS units, i.e. metres) used to filter files whose spatial
# extent deviates too far from the median extent across all timestamps.
tol = 1000

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

# Step 2a: Read all extents (using the 4-band folder as the spatial reference)
print("Reading extents from all files...")
all_bounds = {}  # filename -> rasterio BoundingBox
for f in sorted_files:
    with rasterio.open(os.path.join(folder_path_4bands, f)) as src:
        all_bounds[f] = src.bounds

first_b = all_bounds[sorted_files[0]]
print(f"First file: {sorted_files[0]}  bounds=(left: {first_b.left:.1f}, bottom: {first_b.bottom:.1f}, right: {first_b.right:.1f}, top: {first_b.top:.1f})")

# Step 2b: Compute median extent and filter outliers within tol
min_xs = np.array([b.left   for b in all_bounds.values()])
max_xs = np.array([b.right  for b in all_bounds.values()])
min_ys = np.array([b.bottom for b in all_bounds.values()])
max_ys = np.array([b.top    for b in all_bounds.values()])

median_min_x = np.median(min_xs)
median_max_x = np.median(max_xs)
median_min_y = np.median(min_ys)
median_max_y = np.median(max_ys)

print(f"Median extent: X=[{median_min_x:.1f}, {median_max_x:.1f}]  Y=[{median_min_y:.1f}, {median_max_y:.1f}]")
print(f"Filtering files whose extent deviates more than {tol} m from median...")

outlier_files = []
aligned_files = []
for f, b in all_bounds.items():
    if (abs(b.left   - median_min_x) <= tol and
        abs(b.right  - median_max_x) <= tol and
        abs(b.bottom - median_min_y) <= tol and
        abs(b.top    - median_max_y) <= tol):
        aligned_files.append(f)
    else:
        outlier_files.append(f)

if outlier_files:
    print(f"WARNING: Discarding {len(outlier_files)} file(s) whose extent is outside tolerance:")
    for fname in outlier_files[:5]:
        b = all_bounds[fname]
        print(f"  - {fname}  bounds=({b.left:.1f}, {b.bottom:.1f}, {b.right:.1f}, {b.top:.1f})")
    if len(outlier_files) > 5:
        print(f"  ... and {len(outlier_files) - 5} more")

sorted_files = aligned_files
file_metadata = [m for m in file_metadata if m['filename'] in set(aligned_files)]
print(f"Continuing with {len(sorted_files)} files in set S")

# Step 2c: Compute intersection I of all extents in S
inter_left   = max(all_bounds[f].left   for f in sorted_files)
inter_bottom = max(all_bounds[f].bottom for f in sorted_files)
inter_right  = min(all_bounds[f].right  for f in sorted_files)
inter_top    = min(all_bounds[f].top    for f in sorted_files)

if inter_left >= inter_right or inter_bottom >= inter_top:
    raise ValueError("Intersection of aligned extents is empty — check your input files or increase tol.")

print(f"Intersection extent I: X=[{inter_left:.1f}, {inter_right:.1f}]  Y=[{inter_bottom:.1f}, {inter_top:.1f}]")

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