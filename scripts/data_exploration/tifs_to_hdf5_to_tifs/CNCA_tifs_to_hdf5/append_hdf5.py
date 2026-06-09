"""
append_hdf5.py

Appends new Sentinel-2 timestamps to an existing chip-chunked HDF5 file.

For each tile in TILE_NAMES:
  - Reads FOLDER_HDF5/tile.h5
  - Scans FOLDER_S2 for all acquisitions (identical logic to create_hdf5.py)
  - Identifies new timestamps: those whose S2_filename is not already in the file
  - Writes new timestamps in chronological order, extending all time-axis datasets
  - Updates the n_ts, date_first, and date_last attributes

The chip spatial layout (sort_order, xs_new, ys_new, chip_*) is never modified;
the PT mask is assumed to be unchanged between runs.

Usage:
    python append_hdf5.py
"""

import os
from datetime import date
import numpy as np
import h5py
import rasterio.windows
from hdf5_utils import (
    parse_filter_sort_files,
    FOLDER_S2, FOLDER_PT_MASKS, FOLDER_HDF5,
    BAND_NAMES, TILE_NAMES, MIN_DATE, MAX_DATE, OUTPUT_NODATA_VAL,
    read_pt_mask_pixels, read_and_combine_tifs,
)


def append_tile(tile, h5_filename):
    if not os.path.exists(h5_filename):
        raise FileNotFoundError(
            f"{h5_filename} does not exist — run create_hdf5.py first."
        )

    # ── Read existing state ────────────────────────────────────────────────────
    with h5py.File(h5_filename, 'r') as h5f:
        existing_names = {
            s.decode('ascii') if isinstance(s, bytes) else s
            for s in h5f['S2_filename'][:]
        }
        n_ts_existing = int(h5f['values'].shape[0])
        n_bands       = int(h5f['values'].shape[1])
        n_pix_dst     = int(h5f['values'].shape[2])
        sort_order    = h5f['sort_order'][:]
        last_ordinal  = int(h5f['ts'][-1]) if n_ts_existing > 0 else 0

    valid = sort_order >= 0

    # ── Find new timestamps ────────────────────────────────────────────────────
    all_metadata = parse_filter_sort_files(
        FOLDER_S2, FOLDER_PT_MASKS, tile, MIN_DATE, MAX_DATE
    )
    if not all_metadata:
        print(f"  No files found for tile {tile}.")
        return

    new_metadata = [m for m in all_metadata if m['filename'] not in existing_names]
    if not new_metadata:
        print(f"  No new timestamps for tile {tile}.")
        return

    # new_metadata is already sorted by timestamp_ms (from parse_filter_sort_files)
    n_new = len(new_metadata)
    print(f"  {n_new} new timestamp(s) to append")

    first_new_ordinal = new_metadata[0]['ordinal']
    if first_new_ordinal < last_ordinal:
        print(
            f"  WARNING: earliest new timestamp ({date.fromordinal(first_new_ordinal)}) "
            f"predates last existing timestamp ({date.fromordinal(last_ordinal)}). "
            f"Appending anyway — the ts array will not be strictly sorted. "
            f"Consider recreating the file with create_hdf5.py."
        )

    # ── Prepare PT mask window (same as create_hdf5.py) ───────────────────────
    m0 = all_metadata[0]
    mask_rows, mask_cols, _, _ = read_pt_mask_pixels(m0)
    window = rasterio.windows.Window(
        m0['col_off_pt'], m0['row_off_pt'], m0['ncols_pt'], m0['nrows_pt']
    )

    # ── Extend datasets and write new data ────────────────────────────────────
    with h5py.File(h5_filename, 'a') as h5f:
        n_ts_new = n_ts_existing + n_new

        # Resize all extendable time-axis datasets
        for name in ('ts', 'original_timestamps', 'S2_filename', 'S2_original_filenames',
                     'cloud_cover_pt', 'pixel_count_pt', 'clear_pixel_count_pt',
                     'count_orbit_pixels_pt'):
            h5f[name].resize(n_ts_new, axis=0)
        h5f['values'].resize(n_ts_new, axis=0)

        i0 = n_ts_existing

        # Write scalar metadata for new timestamps
        h5f['ts'][i0:]                    = [m['ordinal']          for m in new_metadata]
        h5f['original_timestamps'][i0:]   = [m['timestamp_ms']     for m in new_metadata]
        h5f['S2_filename'][i0:]           = [m['filename'].encode('ascii')
                                             for m in new_metadata]
        h5f['S2_original_filenames'][i0:] = [m['s2_original_filenames'].encode('ascii')
                                             for m in new_metadata]
        h5f['cloud_cover_pt'][i0:]        = [round(m['cloud_cover_pt'] * 100)
                                             for m in new_metadata]
        h5f['pixel_count_pt'][i0:]        = [round(m['pixel_count_pt'])
                                             for m in new_metadata]
        h5f['clear_pixel_count_pt'][i0:]  = [round(m['clear_pixel_count_pt'])
                                             for m in new_metadata]
        h5f['count_orbit_pixels_pt'][i0:] = [round(m['count_orbit_pixels_pt'])
                                             for m in new_metadata]

        # Write values for new timestamps
        buf = np.full((n_bands, n_pix_dst), OUTPUT_NODATA_VAL, dtype=np.uint16)
        for j, m in enumerate(new_metadata):
            print(f"  [{j+1}/{n_new}] {m['filename']}")
            combined = read_and_combine_tifs(m['paths'], window)
            if combined.shape[0] != n_bands:
                raise ValueError(
                    f"Expected {n_bands} bands, got {combined.shape[0]} "
                    f"for {m['filename']}"
                )
            flat = combined[:, mask_rows, mask_cols]   # (n_bands, n_pixels)
            buf[:] = OUTPUT_NODATA_VAL
            buf[:, valid] = flat[:, sort_order[valid]]
            h5f['values'][i0 + j] = buf

        # Update file-level attributes
        all_ts = h5f['ts'][:]
        h5f.attrs['n_ts']       = n_ts_new
        h5f.attrs['date_first'] = str(date.fromordinal(int(all_ts.min())))
        h5f.attrs['date_last']  = str(date.fromordinal(int(all_ts.max())))

    print(f"  Done: {h5_filename}  ({n_ts_existing} + {n_new} = {n_ts_new} timestamps)")


def main():
    for tile in TILE_NAMES:
        print(f"\nProcessing tile {tile}...")
        h5_filename = os.path.join(FOLDER_HDF5, f'{tile}.h5')
        append_tile(tile, h5_filename)


if __name__ == "__main__":
    main()
