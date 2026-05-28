import os
import numpy as np
import h5py
import rasterio
import rasterio.windows
from rasterio.transform import xy
from hdf5_utils import (
    parse_filter_sort_files,
    FOLDER_S2, FOLDER_PT_MASKS, FOLDER_HDF5,
    BAND_NAMES, TILE_NAMES, MIN_DATE, MAX_DATE,
)


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
    nbands = len(band_names)
    n_pixels = len(xs_flat)
    n_times = len(file_metadata)
    m0 = file_metadata[0]  # for pt_mask since it's always the same for the tile
    transform = m0['transform_pt']
    window = rasterio.windows.Window(m0['col_off_pt'], m0['row_off_pt'], m0['ncols_pt'], m0['nrows_pt'])

    with h5py.File(h5_filename, 'w') as h5f:
        h5f.attrs['band_names']    = [n.encode('ascii') for n in band_names]
        h5f.attrs['crs']           = m0['crs'].to_wkt()
        h5f.attrs['bounds_left']   = float(xs_flat.min())
        h5f.attrs['bounds_right']  = float(xs_flat.max()) + transform.a   # right edge of rightmost pixel
        h5f.attrs['bounds_bottom'] = float(ys_flat.min()) + transform.e   # bottom edge (transform.e < 0)
        h5f.attrs['bounds_top']    = float(ys_flat.max())

        h5f.create_dataset("xs", data=xs_flat, dtype='int32')
        h5f.create_dataset("ys", data=ys_flat, dtype='int32')
        h5f.create_dataset("ts",
                           data=[m['ordinal'] for m in file_metadata],
                           dtype='int32')
        h5f.create_dataset("original_timestamps",
                           data=[m['timestamp_ms'] for m in file_metadata],
                           dtype='int64')
        h5f.create_dataset("S2_filename",
                           data=[m['filename'].encode('ascii') for m in file_metadata])
        h5f.create_dataset("cloud_cover_pt",
                           data=[round(m['cloud_cover_pt'] * 100) for m in file_metadata],
                           dtype='uint8')
        dset = h5f.create_dataset(
            "values",
            shape=(n_times, nbands, n_pixels),
            dtype='uint16',
            chunks=(1, nbands, min(n_pixels, 1 << 20)),
            compression="lzf",
        )

        for i, m in enumerate(file_metadata):
            print(f"  [{i+1}/{n_times}] {m['filename']}")
            combined = read_and_combine_tifs(m['paths'], window)
            if combined.shape[0] != nbands:
                raise ValueError(f"Expected {nbands} bands, got {combined.shape[0]} for {m['filename']}")
            dset[i] = combined[:, mask_rows, mask_cols]  # (nbands, n_pixels)


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
