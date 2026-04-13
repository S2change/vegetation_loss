import os
import h5py
import numpy as np
import rasterio
from rasterio.transform import from_origin


# --- CONFIGURATION CONSTANTS ---
#h5_filename = r'C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\temp\test_tif_to_hdf5\satellite_data_6bands.h5'
hdf5_path = r'C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\temp\test_tif_to_hdf5\test_1667647823345_6bands.h5' #T29TNE_6bands.h5'
output_dir = r'C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\temp\test_tif_to_hdf5\reconstructed_tifs'
USE_COMPRESSION = False  # Set to False to disable LZW compression
CONVERT_TO_YYYYMMDD=False  # Set to True to use human-readable dates in filenames, False to use raw timestamps UNIX milliseconds from 1979/1/1
CRS = "EPSG:32629"
target_band_order = ["B2", "B3", "B4", "B8", "B11", "B12"] # hdf5 file contains bands labels, so we can use them to determine the order of bands in the output GeoTIFFs
# -----------------------------

def export_sparse_hdf5_to_geotiff(hdf5_path, prefix, crs):
    with h5py.File(hdf5_path, 'r') as f:
        # Load datasets into memory
        xs = f['xs'][:]
        ys = f['ys'][:]
        ts = f['ts'][:]
        values = f['values'][:]

        # 1. Define the unique grid axes
        unique_xs = np.unique(xs)
        unique_ys = np.sort(np.unique(ys))[::-1]  # North-to-South
        
        # 2. Get grid dimensions
        cols, rows = len(unique_xs), len(unique_ys)
        
        # 3. Setup Transform (Assuming 10m resolution, or calculate from data)
        # We calculate resolution based on the first two unique steps
        res_x = unique_xs[1] - unique_xs[0]
        res_y = unique_ys[0] - unique_ys[1]
        transform = from_origin(unique_xs[0], unique_ys[0], res_x, res_y)
        
        # 4. Process each timestamp
        # Load the full timestamp array into memory once to speed things up
        ts_np = ts[:] 

        unique_timestamps = np.unique(ts_np)

        for t_val in unique_timestamps:
            # Get integer indices where the timestamp matches
            # This avoids the boolean mask shape mismatch
            
            # ... inside the loop ...
            indices = np.where(ts_np == t_val)[0]
            t_xs = xs[indices]
            t_ys = ys[indices]

            col_indices = np.searchsorted(unique_xs, t_xs)
            row_indices = np.searchsorted(-unique_ys, -t_ys)

            # Create a 3D grid: (Bands, Rows, Cols)
            # Note: your shape (1, 6, ...) suggests 6 is the band count
            num_bands = len(target_band_order)
            grid = np.full((num_bands, rows, cols), np.nan, dtype=np.float32)

            for b in range(num_bands):
                # Extract the b-th band for the current timestamp indices
                band_data = values[0, b, indices]
                grid[b, row_indices, col_indices] = band_data

            # Convert timestamp to string for filename (handles bytes or floats)
            ts_str = str(t_val).strip("b'").replace(":", "-")
            output_path = f"{prefix}_{ts_str}.tif"

            # Write to multi-band GeoTIFF
            with rasterio.open(
                output_path, 'w',
                driver='GTiff',
                height=rows, width=cols,
                count=num_bands, # Set count to 6
                dtype=grid.dtype,
                crs=crs,
                transform=transform,
                nodata=np.nan,
                compress='lzw'
            ) as dst:
                dst.write(grid) # Writes all bands at once
            
            print(f"Success: {output_path}")

#Run the function
export_sparse_hdf5_to_geotiff(hdf5_path, output_dir, CRS)