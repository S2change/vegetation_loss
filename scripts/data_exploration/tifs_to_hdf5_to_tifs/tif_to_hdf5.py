import os
import re
import numpy as np
import h5py
import rasterio
from rasterio.features import rasterize
import rasterio.transform
from datetime import datetime, timezone
from shapely.geometry import box
import geopandas as gpd

'''
This script reads pairs of 4-band and 2-band GeoTIFF files from specified folders, filters out
files with no overlap with a vector mask, rasterizes the mask to identify valid pixels, and writes
the sparse pixel time series to an HDF5 file. Only pixels inside the vector mask are stored.

The output HDF5 file contains:
- values: (time, bands, pixels) - sparse pixel array for masked pixels only
- xs, ys: (pixels,) - coordinate arrays for masked pixels
- ts: (time,) - ordinal dates
- original_timestamps: (time,) - unix timestamps in milliseconds
- band_names attribute: band names in order

Inputs:
- 'folder_path_4bands': Directory containing the 4-band GeoTIFF files (e.g., B3, B4, B8, B12).
- 'folder_path_2bands': Directory containing the 2-band GeoTIFF files (e.g., B2, B11).
- 'vector_mask_path': Path to vector file (shapefile, GeoJSON, etc.) defining the region of interest.
- 'h5_filename': Path for the output HDF5 file to be created.

Note: TIFs with no overlap with the mask bounding box are discarded. TIFs that partially
overlap are kept — the boolean pixel mask determines which pixels are written to the HDF5.
'''

folder_path_4bands = r"D:\s2_images\T29TQG"
folder_path_2bands = r"C:\Users\Public\Documents\s2_images_B2_B11\T29TQG"
vector_mask_path   = r"C:\path\to\your\mask.shp"
h5_filename        = os.path.join(r"E:\T29TQG", 'T29TQG_6bands_masked.h5')

# Define the band order based on the stacking logic below
# Folder '4bands' (B3, B4, B8, B12) followed by Folder '2bands' (B2, B11)
band_names = ["B3", "B4", "B8", "B12", "B2", "B11"]

nbands = len(band_names)

# 1. Parse and Sort Files (Using 4bands as the reference)
files = [f for f in os.listdir(folder_path_4bands) if f.endswith('.tif')]
file_metadata = []
for f in files:
    match = re.search(r'_(\d{13})\.tif', f)
    if match:
        ts_ms = int(match.group(1))
        dt = datetime.fromtimestamp(ts_ms / 1000.0, timezone.utc).date()
        file_metadata.append({
            'filename': f,
            'ordinal': dt.toordinal(),
            'timestamp_ms': ts_ms
        })

file_metadata.sort(key=lambda x: x['ordinal'])
sorted_files = [m['filename'] for m in file_metadata]

# 2. Derive reference extent from vector mask

# Step 2a: Read all extents (using the 4-band folder as the spatial reference)
print("Reading extents from all files...")
all_bounds = {}
for f in sorted_files:
    with rasterio.open(os.path.join(folder_path_4bands, f)) as src:
        all_bounds[f] = src.bounds

# Step 2b: Find the largest TIF by bounding box area and use it as the spatial reference
print("Finding largest TIF as spatial reference...")
largest_file = max(sorted_files, key=lambda f: (
    (all_bounds[f].right - all_bounds[f].left) * (all_bounds[f].top - all_bounds[f].bottom)
))
with rasterio.open(os.path.join(folder_path_4bands, largest_file)) as ref_src:
    ref_crs  = ref_src.crs
    ref_transform = ref_src.transform
    ref_meta = ref_src.meta.copy()

print(f"  Reference TIF: {largest_file}")
print(f"  Bounds: {all_bounds[largest_file]}")

# Step 2c: Load vector mask, reproject to TIF CRS if needed, clip to reference TIF extent,
#          and compute the bounding box of the clipped mask
print(f"Loading vector mask: {vector_mask_path}")
vector_mask = gpd.read_file(vector_mask_path)
if vector_mask.crs != ref_crs:
    vector_mask = vector_mask.to_crs(ref_crs)

ref_bounds = all_bounds[largest_file]
tile_polygon = box(ref_bounds.left, ref_bounds.bottom, ref_bounds.right, ref_bounds.top)
tile_gdf = gpd.GeoDataFrame({"geometry": [tile_polygon]}, crs=ref_crs)
clipped_mask = gpd.clip(vector_mask, tile_gdf)

if clipped_mask.empty:
    raise ValueError("Vector mask does not overlap the reference TIF extent.")

mask_bbox = clipped_mask.total_bounds  # (minx, miny, maxx, maxy)
mask_left, mask_bottom, mask_right, mask_top = mask_bbox
print(f"  Mask bounding box: X=[{mask_left:.1f}, {mask_right:.1f}]  Y=[{mask_bottom:.1f}, {mask_top:.1f}]")

# Step 2d: Filter out TIFs with no overlap with the mask bounding box
print("Filtering files against mask bounding box...")
outlier_files = []
aligned_files = []
for f, b in all_bounds.items():
    no_overlap = (b.right < mask_left or b.left > mask_right or
                  b.top < mask_bottom or b.bottom > mask_top)
    if no_overlap:
        outlier_files.append(f)
    else:
        aligned_files.append(f)

if outlier_files:
    print(f"WARNING: Discarding {len(outlier_files)} file(s):")
    for fname in outlier_files[:5]:
        b = all_bounds[fname]
        print(f"  - {fname}  bounds=({b.left:.1f}, {b.bottom:.1f}, {b.right:.1f}, {b.top:.1f})")
    if len(outlier_files) > 5:
        print(f"  ... and {len(outlier_files) - 5} more")

sorted_files  = aligned_files
file_metadata = [m for m in file_metadata if m['filename'] in set(aligned_files)]
print(f"Continuing with {len(sorted_files)} files")

# 3. Rasterize the vector mask using the reference TIF's grid
print("Rasterizing vector mask...")
ref_meta.update({"count": 1})
clipped_mask["raster_value"] = 1
shapes = [(geom, val) for geom, val in zip(clipped_mask.geometry, clipped_mask["raster_value"])]
rasterized = rasterize(
    shapes=shapes,
    out_shape=(ref_meta['height'], ref_meta['width']),
    transform=ref_meta['transform'],
    fill=0,
    dtype="uint8"
).astype(bool)

total_masked_pixels = int(rasterized.sum())
print(f"  Total masked pixels: {total_masked_pixels}")

# Get row/col indices of masked pixels in the reference grid
mask_rows, mask_cols = np.where(rasterized)

# Compute x, y coordinates for masked pixels
xs_flat, ys_flat = rasterio.transform.xy(ref_transform, mask_rows, mask_cols, offset='ul')
xs_flat = np.array(xs_flat, dtype=np.int32)
ys_flat = np.array(ys_flat, dtype=np.int32)

# 4. Header-only check: verify 4-band and 2-band files match for each timestamp
print("Checking 4-band/2-band header compatibility...")
header_failed = []
header_passed = []
for filename in sorted_files:
    with rasterio.open(os.path.join(folder_path_4bands, filename)) as src4, \
         rasterio.open(os.path.join(folder_path_2bands, filename)) as src2:
        if (src4.bounds != src2.bounds or
                src4.transform != src2.transform or
                src4.shape != src2.shape):
            print(f"  WARNING: Header mismatch for {filename}")
            header_failed.append(filename)
        else:
            header_passed.append(filename)

if header_failed:
    print(f"Excluding {len(header_failed)} file(s) with mismatched headers:")
    for fname in header_failed:
        print(f"  - {fname}")

sorted_files  = header_passed
file_metadata = [m for m in file_metadata if m['filename'] in set(header_passed)]
print(f"Header check complete. {len(sorted_files)} file(s) ready for HDF5 write.")

# 5. Write to HDF5 (sparse: only masked pixels)
with h5py.File(h5_filename, 'w') as h5f:
    dset_values = h5f.create_dataset(
        "values",
        shape=(len(sorted_files), nbands, total_masked_pixels),
        dtype='uint16',
        chunks=(1, nbands, min(1000000, total_masked_pixels)),
        compression="lzf"
    )

    h5f.attrs['band_names'] = [n.encode('ascii') for n in band_names]

    h5f.create_dataset("xs", data=xs_flat, dtype='int32')
    h5f.create_dataset("ys", data=ys_flat, dtype='int32')
    h5f.create_dataset("ts", data=[m['ordinal'] for m in file_metadata], dtype='int32')
    h5f.create_dataset("original_timestamps",
                       data=[m['timestamp_ms'] for m in file_metadata],
                       dtype='int64')

    for i, filename in enumerate(sorted_files):
        print(f"Processing {i+1}/{len(sorted_files)}: {filename}")

        path_4b = os.path.join(folder_path_4bands, filename)
        path_2b = os.path.join(folder_path_2bands, filename)

        with rasterio.open(path_4b) as src4, rasterio.open(path_2b) as src2:
            # Read full arrays — use mask_rows/mask_cols to select pixels
            # band axis is 0, so read() returns (bands, height, width)
            data4 = src4.read()  # (4, H, W)
            data2 = src2.read()  # (2, H, W)

            # Stack bands and select masked pixels
            data_all = np.concatenate([data4, data2], axis=0)  # (6, H, W)
            dset_values[i, :, :] = data_all[:, mask_rows, mask_cols]

print(f"Done! Created {h5_filename} with {total_masked_pixels} masked pixels and {len(sorted_files)} timesteps.")
