import os
import numpy as np
import h5py
import rasterio
import rasterio.transform
from datetime import datetime

from hdf5_utils import parse_and_sort_files, NODATA_VAL

'''
Appends new timesteps to an existing HDF5 file created by create_hdf5.py.

Timestamps already present in the HDF5 are skipped automatically. The spatial
grid (xs, ys) is read from the existing file and new TIFs must cover the same
pixel footprint.

Inputs:
- 'folder_path_tifs': Directory containing the 10-band GeoTIFF files.
- 'h5_filename': Path to the existing HDF5 file to append to.
'''

folder_path_tifs = r"E:\T29TQG\CNCA_tifs_to_hdf5_tests\T29TQG_tifs_for_testing\append_to_hdf5"
h5_filename      = os.path.join(r"E:\T29TQG\CNCA_tifs_to_hdf5_tests", 'T29TQG_CNCA_test_appended.h5')

MIN_DATE = None
MAX_DATE = None


def append_hdf5(h5_filename, new_files, new_metadata, folder_tifs, xs_flat, ys_flat):
    """Append new timesteps to an existing HDF5 file, skipping duplicates."""
    with h5py.File(h5_filename, 'a') as h5f:
        existing_ts = set(h5f["original_timestamps"][:].tolist())
        new_metadata = [m for m in new_metadata if m['timestamp_ms'] not in existing_ts]
        new_files    = [m['filename'] for m in new_metadata]

        if not new_files:
            print("No new timesteps to append — all files already present in HDF5.")
            return

        skipped = len(set(m['filename'] for m in new_metadata) - set(new_files))
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

                nodata_mask = np.any(data_all == NODATA_VAL, axis=0)
                data_all[:, nodata_mask] = NODATA_VAL

                out = np.full((nbands, total_masked_pixels), NODATA_VAL, dtype=np.uint16)
                out[:, valid] = data_all[:, tif_rows[valid], tif_cols[valid]]
                h5f["values"][current_t + i, :, :] = out

    print(f"Done! Appended {len(new_files)} timestep(s) to {h5_filename}.")


if __name__ == "__main__":
    if not os.path.exists(h5_filename):
        raise FileNotFoundError(f"Cannot append — HDF5 file not found: {h5_filename}")

    file_metadata = parse_and_sort_files(folder_path_tifs, MIN_DATE, MAX_DATE)
    sorted_files = [m['filename'] for m in file_metadata]

    with h5py.File(h5_filename, 'r') as h5f:
        xs_flat = h5f["xs"][:]
        ys_flat = h5f["ys"][:]

    append_hdf5(h5_filename, sorted_files, file_metadata, folder_path_tifs, xs_flat, ys_flat)
