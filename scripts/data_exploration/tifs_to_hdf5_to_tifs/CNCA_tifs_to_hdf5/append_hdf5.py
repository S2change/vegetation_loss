import os
import numpy as np
import h5py
import rasterio
import rasterio.transform
from datetime import datetime

from hdf5_utils import TILE_NAMES, parse_and_sort_files, INPUT_NODATA_VAL, OUTPUT_NODATA_VAL

'''
Appends new timesteps to an existing HDF5 file created by create_hdf5.py.

Timestamps already present in the HDF5 are skipped automatically. The spatial
grid (xs, ys) is read from the existing file and new TIFs must cover the same
pixel footprint.

Inputs:
- 'folder_tifs': Directory containing the 10-band GeoTIFF files.
- 'folder_hdf5': Path to the folder containing the existing HDF5 files (one per tile).
- 'MIN_DATE' and 'MAX_DATE': Optional date filters to only include TIFs within a certain date range, based on the timestamp in the filename.
'''

root_folder = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5"
folder_tifs = os.path.join(root_folder, "input_tifs")
folder_hdf5=os.path.join(root_folder, "hdf5")

MIN_DATE = None # or datetime(2017, 1, 1) # set a minimum date to filter out files with earlier timestamps; if None, all files are included regardless of date
MAX_DATE = None # or datetime(2030, 1, 1) # set a maximum date to filter out files with later timestamps; if None, all files are included regardless of date

def main():
    """
    Main Steps:
    - Parse and sorts input directory of GeoTIFFs
    - Checks existing timestamps in HDF5 file, filters any matches input directory
    - Appends additional data to the HDF5 file
    """
    for tile in TILE_NAMES:
        print(f"Processing tile {tile}...")
   
        h5_filename      = os.path.join(folder_hdf5, f'{tile}.h5')

        if not os.path.exists(h5_filename):
            print(f"Cannot append — HDF5 file not found: {h5_filename}")
            continue
        
        # Parse and sort input TIF files, filtering by date range if specified
        # Reads all TIFs in the input folder, but only keeps those matching the current tile and date range
        # Removal of already existing timestamps in HDF5 is done in append_hdf5, since we need to read the existing timestamps from the file first before we can filter the input files
        new_metadata = parse_and_sort_files(folder_tifs, tile, MIN_DATE, MAX_DATE)
        if not new_metadata:
            print(f"No new files found for tile {tile} in the specified date range. Skipping.")
            continue    

        with h5py.File(h5_filename, 'r') as h5f:
            xs_flat = h5f["xs"][:]
            ys_flat = h5f["ys"][:]

        append_hdf5(h5_filename, new_metadata, folder_tifs, xs_flat, ys_flat)


def append_hdf5(h5_filename, new_metadata, folder_tifs, xs_flat, ys_flat):
    """Append new timesteps to an existing HDF5 file, skipping duplicates."""
    with h5py.File(h5_filename, 'a') as h5f:
        existing_ts = set(h5f["original_timestamps"][:].tolist())
        new_metadata = [m for m in new_metadata if m['timestamp_ms'] not in existing_ts]
        new_files    = [m['path'] for m in new_metadata] # paths to the new files to append, after filtering out those already present in HDF5 based on timestamp

        if not new_files:
            print("No new timesteps to append — all files already present in HDF5.")
            return

        skipped = len(set(m['path'] for m in new_metadata) - set(new_files))
        if skipped:
            print(f"Skipping {skipped} file(s) already present in HDF5.")

        nbands = h5f["values"].shape[1]
        total_masked_pixels = h5f["values"].shape[2]
        current_t = h5f["values"].shape[0]
        new_t = current_t + len(new_files)

        h5f["values"].resize(new_t, axis=0)
        h5f["ts"].resize((new_t,))
        h5f["original_timestamps"].resize((new_t,))

        h5f["ts"][current_t:] = [m['ordinal'] for m in new_metadata]
        h5f["original_timestamps"][current_t:] = [m['timestamp_ms'] for m in new_metadata]

        for i, filename in enumerate(new_files):
            print(f"Appending {i+1}/{len(new_files)}: {filename}")

            with rasterio.open(os.path.join(folder_tifs, filename)) as src:
                tif_rows, tif_cols = rasterio.transform.rowcol(src.transform, xs_flat, ys_flat)
                tif_rows = np.array(tif_rows)
                tif_cols = np.array(tif_cols)

                valid = ((tif_rows >= 0) & (tif_rows < src.height) &
                         (tif_cols >= 0) & (tif_cols < src.width))

                data_all = src.read()  # (10, H, W)

                nodata_mask = np.any(data_all == INPUT_NODATA_VAL, axis=0)
                data_all[:, nodata_mask] = OUTPUT_NODATA_VAL

                out = np.full((nbands, total_masked_pixels), OUTPUT_NODATA_VAL, dtype=np.uint16)
                out[:, valid] = data_all[:, tif_rows[valid], tif_cols[valid]]
                h5f["values"][current_t + i, :, :] = out

    print(f"Done! Appended {len(new_files)} timestep(s) to {h5_filename}.")

if __name__ == "__main__":
    main()
