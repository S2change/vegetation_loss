import datetime
import os
import sys
import h5py
import numpy as np
import rasterio
from rasterio.transform import from_origin

'''
This script reconstructs georeferenced TIFF files from a multiband HDF5 file containing satellite data.
The HDF5 file is expected to have the following structure:
- 'xs': 1D array of x coordinates (longitude)
- 'ys': 1D array of y coordinates (latitude)
- 'ts': 1D array of timestamps; ts_values are ordinal dates (Proleptic Gregorian Ordinal: Day 1: 0001-01-01), like 737849 for 2021-02-28
- 'values': 3D array of satellite values
The script processes the HDF5 data to create a 3D grid (bands x rows x cols) and writes it to a GeoTIFF file with appropriate georeferencing and metadata.
'''

# --- CONFIGURATION ---
hdf5_path = r"E:\T29TQG\CNCA_tifs_to_hdf5_tests\T29TQG_CNCA_test_appended.h5"
# where tifs will be saved
output_dir = r"E:\T29TQG\CNCA_tifs_to_hdf5_tests\T29TQG_reconstructed_tifs\appended_hdf5"
CRS = "EPSG:32629"
# Order of bands in hdf5
target_band_order = ["B3", "B4", "B8", "B12"]
NODATA_VAL = 65535 
# select timestamp to determine which timestamp is going to be processed; if None, will process the first timestamp in the hdf5 file
DATE_OUTPUT=None


def main():
    export_multiband_hdf5(hdf5_path, output_dir, "T29TQG_CNCA_appended_2", CRS, target_band_order,DATE_OUTPUT)

def export_multiband_hdf5(hdf5_path, output_dir, prefix, crs, target_band_order, DATE_OUTPUT):
    num_bands = len(target_band_order)
    os.makedirs(output_dir, exist_ok=True)

    with h5py.File(hdf5_path, 'r') as f:
        print("Loading coordinates...")
        # Rounding to 1 decimal place to ensure grid alignment
        xs = np.round(f['xs'][:], 1)
        ys = np.round(f['ys'][:], 1)
        # compute IDX based on DATE_OUTPUT
        ts_values = f['ts'][:]  # ts_values are ordinal dates, like 737849
        ts_str_values = [datetime.datetime.fromordinal(o).strftime('%Y-%m-%d') for o in ts_values]  # Convert to YYYY-MM-DD format strings
        print(f"Timestamps in HDF5 (YYYY-MM-DD): {ts_str_values}")
        
        if DATE_OUTPUT is not None:
            # choose the date closest to the specified DATE_OUTPUT
            date_output_dt = datetime.datetime.strptime(DATE_OUTPUT, '%Y-%m-%d')
            date_diffs = [abs((datetime.datetime.fromordinal(o) - date_output_dt).days) for o in ts_values]
            IDX = date_diffs.index(min(date_diffs))
            DATE_OUTPUT = ts_str_values[IDX]  # Update DATE_OUTPUT to the actual closest date found in the HDF5
            print(f"Selected timestamp '{DATE_OUTPUT}' found at index {IDX}.")  
        else:
            IDX = 4
            DATE_OUTPUT = ts_str_values[IDX]
            print(f"No DATE_OUTPUT specified. Defaulting to first timestamp at date {DATE_OUTPUT}.")

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
        output_path = os.path.join(output_dir, f"{prefix}_{DATE_OUTPUT}.tif")
        
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

main()