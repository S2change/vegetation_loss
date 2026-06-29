import os
from datetime import date
import numpy as np
import h5py
import rasterio.windows
from hdf5_utils import (
    parse_filter_sort_files, write_tile_log,
    FOLDER_S2, FOLDER_PT_MASKS, FOLDER_HDF5, FOLDER_LOGS,
    BAND_NAMES, TILE_NAMES, MIN_DATE, MAX_DATE, N_TS_CHUNK, OUTPUT_NODATA_VAL,
    CHIP_SIDE, PIXEL_RES,
    read_pt_mask_pixels, build_chip_layout, read_and_combine_tifs,
)


def write_hdf5(h5_filename, file_metadata, band_names, mask_rows, mask_cols, xs_flat, ys_flat):
    nbands  = len(band_names)
    n_times = len(file_metadata)
    m0      = file_metadata[0]
    transform = m0['transform_pt']
    window  = rasterio.windows.Window(m0['col_off_pt'], m0['row_off_pt'], m0['ncols_pt'], m0['nrows_pt'])

    n_slots = CHIP_SIDE ** 2

    print("  Building chip layout...")
    (sort_order_padded, xs_new, ys_new,
     chip_x_bin, chip_y_bin, chip_counts) = build_chip_layout(xs_flat, ys_flat)

    n_chips   = len(chip_x_bin)
    n_pix_dst = n_chips * n_slots
    valid     = sort_order_padded >= 0
    padding   = (n_pix_dst - len(xs_flat)) / n_pix_dst * 100
    print(f"  {n_chips} chips × {n_slots} slots = {n_pix_dst:,} padded pixels (padding {padding:.1f} %)")

    ts_ordinals = [m['ordinal'] for m in file_metadata]
    date_first  = date.fromordinal(ts_ordinals[0])
    date_last   = date.fromordinal(ts_ordinals[-1])

    with h5py.File(h5_filename, 'w') as h5f:
        # Attributes — mirror what preprocess_to_n_ts_chip_chunked.py stores
        h5f.attrs['band_names']    = [n.encode('ascii') for n in band_names]
        h5f.attrs['crs']           = m0['crs'].to_wkt()
        h5f.attrs['bounds_left']   = float(xs_flat.min())
        h5f.attrs['bounds_right']  = float(xs_flat.max()) + transform.a
        h5f.attrs['bounds_bottom'] = float(ys_flat.min()) + transform.e
        h5f.attrs['bounds_top']    = float(ys_flat.max())
        h5f.attrs['chip_size']     = CHIP_SIDE
        h5f.attrs['pixel_res']     = PIXEL_RES
        h5f.attrs['n_ts']          = n_times
        h5f.attrs['nodata_val']    = OUTPUT_NODATA_VAL
        h5f.attrs['date_first']    = str(date_first)
        h5f.attrs['date_last']     = str(date_last)

        # Chip layout datasets — same names as preprocess_to_n_ts_chip_chunked.py output
        h5f.create_dataset('sort_order',       data=sort_order_padded, dtype='int64')
        h5f.create_dataset('xs_new',           data=xs_new,            dtype='int32')
        h5f.create_dataset('ys_new',           data=ys_new,            dtype='int32')
        h5f.create_dataset('chip_x_bin',       data=chip_x_bin,        dtype='int32')
        h5f.create_dataset('chip_y_bin',       data=chip_y_bin,        dtype='int32')
        h5f.create_dataset('chip_pixel_count', data=chip_counts,       dtype='int32')

        # Per-timestamp metadata
        h5f.create_dataset('ts',
                           data=ts_ordinals,
                           dtype='int32', maxshape=(None,))
        h5f.create_dataset('original_timestamps',
                           data=[m['timestamp_ms'] for m in file_metadata],
                           dtype='int64', maxshape=(None,))
        h5f.create_dataset('S2_filename',
                           data=[m['filename'].encode('ascii') for m in file_metadata],
                           maxshape=(None,))
        h5f.create_dataset('S2_original_filenames',
                           data=[m['s2_original_filenames'].encode('ascii') for m in file_metadata],
                           maxshape=(None,))
        h5f.create_dataset('cloud_cover_pt',
                           data=[round(m['cloud_cover_pt'] * 100) for m in file_metadata],
                           dtype='uint8', maxshape=(None,))
        h5f.create_dataset('pixel_count_pt',
                           data=[round(m['pixel_count_pt']) for m in file_metadata],
                           dtype='uint64', maxshape=(None,))
        h5f.create_dataset('clear_pixel_count_pt',
                           data=[round(m['clear_pixel_count_pt']) for m in file_metadata],
                           dtype='uint64', maxshape=(None,))
        h5f.create_dataset('count_orbit_pixels_pt',
                           data=[round(m['count_orbit_pixels_pt']) for m in file_metadata],
                           dtype='uint64', maxshape=(None,))

        # values: (n_times, nbands, n_chips * n_slots) — one chunk per chip across N_TS_CHUNK timestamps
        dset = h5f.create_dataset(
            'values',
            shape=(n_times, nbands, n_pix_dst),
            dtype='uint16',
            chunks=(N_TS_CHUNK, nbands, n_slots),
            compression='lzf',
            maxshape=(None, nbands, n_pix_dst),
            fillvalue=OUTPUT_NODATA_VAL,
        )

        buf = np.full((nbands, n_pix_dst), OUTPUT_NODATA_VAL, dtype=np.uint16)
        for i, m in enumerate(file_metadata):
            print(f"  [{i+1}/{n_times}] {m['filename']}")
            combined = read_and_combine_tifs(m['paths'], window)
            if combined.shape[0] != nbands:
                raise ValueError(f"Expected {nbands} bands, got {combined.shape[0]} for {m['filename']}")
            flat = combined[:, mask_rows, mask_cols]   # (nbands, n_pixels) — valid PT pixels only
            buf[:] = OUTPUT_NODATA_VAL
            buf[:, valid] = flat[:, sort_order_padded[valid]]
            dset[i] = buf


def main():
    for tile in TILE_NAMES:
        print(f"\nProcessing tile {tile}...")
        file_metadata, folders_dict, all_metrics = parse_filter_sort_files(FOLDER_S2, FOLDER_PT_MASKS, tile, MIN_DATE, MAX_DATE)
        if not file_metadata:
            print(f"  No files found for tile {tile}. Skipping.")
            continue

        mask_rows, mask_cols, xs_flat, ys_flat = read_pt_mask_pixels(file_metadata[0])
        print(f"  PT mask pixels: {len(xs_flat)}, timesteps: {len(file_metadata)}")

        os.makedirs(FOLDER_HDF5, exist_ok=True)
        h5_filename = os.path.join(FOLDER_HDF5, f'{tile}.h5')
        write_hdf5(h5_filename, file_metadata, BAND_NAMES, mask_rows, mask_cols, xs_flat, ys_flat)
        print(f"  Done: {h5_filename}")

        os.makedirs(FOLDER_LOGS, exist_ok=True)
        write_tile_log(folders_dict, all_metrics,
                       os.path.join(FOLDER_LOGS, f'{tile}_log.csv'),
                       stored_prefixes={m['filename'] for m in file_metadata},
                       append=False)


if __name__ == "__main__":
    main()
