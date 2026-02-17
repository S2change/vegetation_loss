import os
import h5py
import numpy as np
import rasterio
from rasterio.transform import from_origin
from datetime import date

'''
This script reads the HDF5 file created by the previous scripts, extracts the data, and reconstructs georeferenced GeoTIFF files for each timestamp. The output GeoTIFFs will be compressed using LZW and will include the original spatial metadata for accurate georeferencing.

Make sure to adjust the paths and configurations as needed before running the script.

Key Steps:
1. Load the HDF5 file and read the datasets (values, xs, ys, ts).
2. Determine the spatial grid dimensions and calculate the affine transform.
3. For each timestamp, create a GeoTIFF file with the corresponding bands, applying or not  LZW compression (reduces file size by 50%) and using the original spatial metadata for georeferencing.  
'''

# --- CONFIGURATION CONSTANTS ---
h5_filename = r'C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\temp\test_tif_to_hdf5\satellite_data_6bands.h5'
output_dir = r'C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\temp\test_tif_to_hdf5\reconstructed_tifs'
USE_COMPRESSION = False  # Set to False to disable LZW compression
CONVERT_TO_YYYYMMDD=False  # Set to True to use human-readable dates in filenames, False to use raw timestamps UNIX milliseconds from 1979/1/1
crs = "EPSG:32629"
target_band_order = ["B2", "B3", "B4", "B8", "B11", "B12"] # hdf5 file contains bands labels, so we can use them to determine the order of bands in the output GeoTIFFs
# -------------------------------

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

with h5py.File(h5_filename, 'r') as h5f:
    # 1. Load coordinates and metadata
    xs = h5f['xs'][:]
    ys = h5f['ys'][:]
    ts_ordinals = h5f['ts'][:]
    ts_milliseconds = h5f['original_timestamps'][:] # The new dataset with raw timestamps

    # Handle both byte-strings and regular strings
    stored_bands = [b.decode('ascii') if isinstance(b, bytes) else b for b in h5f.attrs['band_names']]
    print(f"Bands found in HDF5: {stored_bands}")
    
    band_indices = [stored_bands.index(b) for b in target_band_order]
    
    # 2. Determine grid dimensions
    unique_xs = np.sort(np.unique(xs))
    unique_ys = np.sort(np.unique(ys))[::-1] 
    
    width = len(unique_xs)
    height = len(unique_ys)
    res_x = unique_xs[1] - unique_xs[0]
    res_y = unique_ys[0] - unique_ys[1] 
    
    transform = from_origin(unique_xs[0], unique_ys[0], res_x, res_y)
    
    # 3. Process each timestamp
    for i in range(len(ts_ordinals)):
        if CONVERT_TO_YYYYMMDD:
            # Result: 2017-04-08.tif
            date_str = date.fromordinal(ts_ordinals[i]).strftime('%Y-%m-%d')
        else:
            # Result: 1491651247967.tif
            date_str = str(ts_milliseconds[i])
        tif_name = f"sentinel2_6bands_{date_str}.tif"
        output_path = os.path.join(output_dir, tif_name)
        
        if os.path.exists(output_path):
            print(f"Skipping: {tif_name} (already exists)")
            continue
            
        print(f"Exporting: {tif_name} (Compression: {USE_COMPRESSION})")
        
        # Base metadata
        meta = {
            'driver': 'GTiff',
            'height': height,
            'width': width,
            'count': len(target_band_order),
            'dtype': 'uint16',
            'crs': crs,
            'transform': transform,
            'nodata': 65535,  # CHANGED: Set to 65535 to match your data range and avoid confusion with valid 0 values
            'tiled': True,
            'blockxsize': 256,
            'blockysize': 256
        }
        
        # Conditionally add compression parameters
        if USE_COMPRESSION:
            meta.update({
                'compress': 'lzw',
                'predictor': 2
            })
        
        with rasterio.open(output_path, 'w', **meta) as dst:
            for dst_idx, src_idx in enumerate(band_indices, start=1):
                band_data_2d = h5f['values'][i, src_idx, :].reshape(height, width)
                dst.write(band_data_2d.astype('uint16'), dst_idx)
                dst.set_band_description(dst_idx, target_band_order[dst_idx-1])

print(f"\nDone! Files processed in {output_dir}")