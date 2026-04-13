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
NODATA_VALUE=65535  # Use a value that is outside the valid range of the original data (0-10000) to represent NoData in the HDF5

# ... (Configuration constants remain the same) ...

def export_multiband_hdf5(hdf5_path, output_dir, prefix, crs, target_band_order):
    num_bands = len(target_band_order)
    os.makedirs(output_dir, exist_ok=True)

    with h5py.File(hdf5_path, 'r') as f:
        # Load arrays and ROUND them to avoid float precision errors (e.g., to 2 decimal places)
        # Before the loop
        xs = np.round(f['xs'][:], 2)
        ys = np.round(f['ys'][:], 2)
        unique_xs = np.round(np.unique(xs), 2)
        unique_ys = np.round(np.sort(np.unique(ys))[::-1], 2)
        

        # Define unique grid axes with rounded values
        unique_xs = np.unique(xs)
        unique_ys = np.sort(np.unique(ys))[::-1] # North-to-South
        
        cols, rows = len(unique_xs), len(unique_ys)
        
        # Calculate resolution
        res_x = unique_xs[1] - unique_xs[0]
        res_y = unique_ys[0] - unique_ys[1]
        transform = from_origin(unique_xs[0], unique_ys[0], res_x, res_y)
        
        unique_timestamps = np.unique(ts_np)
        
        for t_val in unique_timestamps:
            indices = np.where(ts_np == t_val)[0]
            if indices.size == 0: continue

            # Initialize grid
            grid = np.full((num_bands, rows, cols), NODATA_VALUE, dtype=np.uint16)
            
            t_xs = xs[indices]
            t_ys = ys[indices]

            # SAFER MAPPING: Find indices in the unique coordinate arrays
            col_indices = np.searchsorted(unique_xs, t_xs)
            # For descending unique_ys, we search in the negative and negate the input
            row_indices = np.searchsorted(-unique_ys, -t_ys)

            # --- DEBUG CHECK ---
            # If this prints 0, your mapping is failing
            valid_points = len(indices)
            print(f"Timestamp {t_val}: Mapping {valid_points} points...")

            for b_idx in range(num_bands):
                band_data = values_ds[0, b_idx, indices]
                
                # Check if band_data actually has non-NaN values
                if np.isnan(band_data).all():
                    print(f"Warning: Band {target_band_order[b_idx]} is all NaNs in HDF5!")
                
                grid[b_idx, row_indices, col_indices] = band_data

            # Final check before writing: total non-nan values in grid
            count_data = np.count_nonzero(~np.isnan(grid))
            if count_data == 0:
                print(f"CRITICAL: Grid for {t_val} is empty! Check coordinate matching.")
            else:
                print(f"Success: Populated {count_data} pixels.")

            # (Rest of the rasterio.open and saving logic...)


            # Clean filename: handle potential bytes type from HDF5
            ts_str = t_val.decode() if isinstance(t_val, bytes) else str(t_val)
            output_path = os.path.join(output_dir, f"{prefix}_{ts_str}.tif")
            
            with rasterio.open(
                output_path, 'w',
                driver='GTiff',
                height=rows, width=cols,
                count=num_bands,
                dtype='uint16', # Changed from grid.dtype to be explicit
                crs=crs,
                transform=transform,
                nodata=65535,   # Changed from np.nan
                compress='lzw'
            ) as dst:
                dst.write(grid)
                # Apply band names to metadata
                dst.descriptions = tuple(target_band_order)
            
            print(f"Exported multi-band image: {output_path}")

# Run
export_multiband_hdf5(hdf5_path,output_dir,'test', CRS, target_band_order) 