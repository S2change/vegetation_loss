import os
import h5py
import numpy as np
import rasterio
from rasterio.transform import from_origin

# --- CONFIGURATION ---
hdf5_path = r'C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\temp\test_tif_to_hdf5\T29TNE_6bands_20210101_20210630.h5'
# where tifs will be saved
output_dir = r'C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\temp\test_tif_to_hdf5\reconstructed_tifs'
CRS = "EPSG:32629"
# Order of bands in hdf5
target_band_order = ["B3", "B4", "B8", "B12", "B2", "B11"]
NODATA_VAL = 65535 
IDX=-3
# index of the timestamp to process

def export_multiband_hdf5(hdf5_path, output_dir, prefix, crs, target_band_order):
    num_bands = len(target_band_order)
    os.makedirs(output_dir, exist_ok=True)

    with h5py.File(hdf5_path, 'r') as f:
        print("Loading coordinates...")
        # Rounding to 1 decimal place to ensure grid alignment
        xs = np.round(f['xs'][:], 1)
        ys = np.round(f['ys'][:], 1)
        ts_val = f['ts'][IDX]  # Get the single global timestamp
        values_ds = f['values']

        print("Calculating unique grid...")
        unique_xs = np.unique(xs)
        unique_ys = np.sort(np.unique(ys))[::-1]
        
        cols, rows = len(unique_xs), len(unique_ys)
        print(f"Grid Dimensions: {rows} rows x {cols} cols")
        
        res_x = unique_xs[1] - unique_xs[0]
        res_y = unique_ys[0] - unique_ys[1]
        transform = from_origin(unique_xs[0], unique_ys[0], res_x, res_y)
        
        # Initialize the 3D grid
        grid = np.full((num_bands, rows, cols), NODATA_VAL, dtype=np.uint16)
        
        print("Mapping coordinates to pixels...")
        col_indices = np.searchsorted(unique_xs, xs)
        row_indices = np.searchsorted(-unique_ys, -ys)

        for b_idx in range(num_bands):
            print(f"Processing Band {target_band_order[b_idx]}...")
            # values shape is (1, 6, 66911408)
            # We take the 0th slice of the 1st dimension, and b_idx of the 2nd
            band_data = values_ds[IDX, b_idx, :]
            
            # Map the 66 million points into the 2D grid
            grid[b_idx, row_indices, col_indices] = band_data
            
            # Validation print
            valid_pixels = np.count_nonzero(band_data != NODATA_VAL)
            print(f"  - Band {b_idx} contains {valid_pixels:,} valid pixels.")

        # Cleanup timestamp for filename
        ts_str = ts_val.decode() if isinstance(ts_val, bytes) else str(ts_val)
        output_path = os.path.join(output_dir, f"{prefix}_{ts_str}.tif")
        
        print(f"Writing to GeoTIFF: {output_path}...")
        with rasterio.open(
            output_path, 'w',
            driver='GTiff',
            height=rows, width=cols,
            count=num_bands,
            dtype='uint16',
            crs=crs,
            transform=transform,
            nodata=NODATA_VAL,
            compress='lzw'
        ) as dst:
            dst.write(grid)
            dst.descriptions = tuple(target_band_order)
        
        print("Export Complete.")

# Run
export_multiband_hdf5(hdf5_path, output_dir, 'TNE_hdf5', CRS, target_band_order)