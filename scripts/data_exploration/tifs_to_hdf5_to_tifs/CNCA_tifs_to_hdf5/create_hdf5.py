import os
from datetime import date
import numpy as np
import h5py
import rasterio
import rasterio.windows
from rasterio.transform import xy
from hdf5_utils import (
    parse_filter_sort_files,
    FOLDER_S2, FOLDER_PT_MASKS, FOLDER_HDF5,
    BAND_NAMES, TILE_NAMES, MIN_DATE, MAX_DATE, N_TS_CHUNK, OUTPUT_NODATA_VAL, COORD_NODATA_VAL
)

CHIP_SIDE = 256   # pixels per chip side (256 × 256 = 65 536 px/chip)
PIXEL_RES = 10    # Sentinel-2 native resolution, metres


def read_pt_mask_pixels(m):
    """Return (mask_rows, mask_cols, xs_flat, ys_flat) for value-0 pixels in the tight bbox.

    Row/col indices are relative to the tight bbox origin. xs/ys are upper-left pixel corners
    in the CRS of the PT mask, stored as int32 (valid for UTM coordinates in Portugal).
    """
    window = rasterio.windows.Window(m['col_off_pt'], m['row_off_pt'], m['ncols_pt'], m['nrows_pt'])
    with rasterio.open(m['path_pt_mask']) as src:
        pt_tight = src.read(1, window=window)
    mask_rows, mask_cols = np.where(pt_tight == 0)
    xs, ys = xy(m['transform_pt'], mask_rows, mask_cols, offset='ul')
    return mask_rows, mask_cols, np.array(xs, dtype=np.int32), np.array(ys, dtype=np.int32)


def build_chip_layout(xs, ys):
    """Sort pixels into chip grid cells, pad each to CHIP_SIDE² slots.

    Identical logic to preprocess_to_n_ts_chip_chunked.py so that downstream
    consumers see the same layout without the preprocessing step.
    """
    n_slots = CHIP_SIDE ** 2
    chip_m  = CHIP_SIDE * PIXEL_RES
    x_bins  = ((xs.astype(np.int64) - int(xs.min())) // chip_m).astype(np.int32)
    y_bins  = ((int(ys.max()) - ys.astype(np.int64)) // chip_m).astype(np.int32)
    chip_id = x_bins.astype(np.int64) * 1_000_000 + y_bins.astype(np.int64)

    unique_ids, pixel_chip = np.unique(chip_id, return_inverse=True)
    n_chips     = len(unique_ids)
    chip_counts = np.bincount(pixel_chip, minlength=n_chips).astype(np.int32)
    order       = np.argsort(pixel_chip, kind='stable')

    # Padded sort order: slot i → original pixel index (-1 = padding)
    sort_order_padded = np.full(n_chips * n_slots, -1, dtype=np.int64)
    chip_starts = np.zeros(n_chips + 1, dtype=np.int64)
    np.cumsum(chip_counts, out=chip_starts[1:])
    for c in range(n_chips):
        s, e = chip_starts[c], chip_starts[c + 1]
        sort_order_padded[c * n_slots : c * n_slots + (e - s)] = order[s:e]

    xs_new = np.full(n_chips * n_slots, COORD_NODATA_VAL, dtype=np.int32)
    ys_new = np.full(n_chips * n_slots, COORD_NODATA_VAL, dtype=np.int32)
    real   = sort_order_padded >= 0
    xs_new[real] = xs[sort_order_padded[real]]
    ys_new[real] = ys[sort_order_padded[real]]

    chip_x_bin = (unique_ids // 1_000_000).astype(np.int32)
    chip_y_bin = (unique_ids %  1_000_000).astype(np.int32)

    return sort_order_padded, xs_new, ys_new, chip_x_bin, chip_y_bin, chip_counts


def read_and_combine_tifs(paths, window):
    """Read the tight-bbox window from each path and combine by per-band minimum.
    nodata=65535 is the uint16 maximum, so np.minimum naturally prefers valid data
    over nodata: min(65535, valid_value) = valid_value.
    """
    combined = None
    for path in paths:
        print(path, window)
        with rasterio.open(path) as src:
            data = src.read(window=window)  # (nbands, nrows_pt, ncols_pt), uint16
        combined = data if combined is None else np.minimum(combined, data)
    return combined  # (nbands, nrows_pt, ncols_pt)


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
        file_metadata = parse_filter_sort_files(FOLDER_S2, FOLDER_PT_MASKS, tile, MIN_DATE, MAX_DATE)
        if not file_metadata:
            print(f"  No files found for tile {tile}. Skipping.")
            continue

        mask_rows, mask_cols, xs_flat, ys_flat = read_pt_mask_pixels(file_metadata[0])
        print(f"  PT mask pixels: {len(xs_flat)}, timesteps: {len(file_metadata)}")

        os.makedirs(FOLDER_HDF5, exist_ok=True)
        h5_filename = os.path.join(FOLDER_HDF5, f'{tile}.h5')
        write_hdf5(h5_filename, file_metadata, BAND_NAMES, mask_rows, mask_cols, xs_flat, ys_flat)
        print(f"  Done: {h5_filename}")


if __name__ == "__main__":
    main()
