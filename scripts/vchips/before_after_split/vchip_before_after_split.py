"""
For each vchip, identifies which Sentinel-2 tile HDF5 file covers it based on
the x/y coordinates in the vchip filename, loads pre/post break S2 composites,
and writes two output GeoTIFFs (before and after) per vchip.

Vchip filename format: vchip_{x}_{y}_{date}_mask.tif
HDF5 filename format:  {tile_id}.h5  (e.g. T29SMC.h5)
All coordinates are in EPSG:32629.

Output bands (same for before and after):
    B12, 11, 8a, 8, 7, 6, 5, 4, 3, 2
"""
import os
import sys
import re
import glob
import h5py
import numpy as np
import rasterio as rio
from collections import defaultdict
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

# VCHIP_DIR, HDF5_DIR, BEFORE_OUTPUT, and AFTER_OUTPUT are supplied on the
# command line — see USAGE below.
USAGE = (
    "Usage: python vchip_before_after_split.py "
    "<vchip_dir> <hdf5_dir> <before_output_dir> <after_output_dir>"
)

# Temporal compositing parameters
TEMPORAL_WINDOW_DAYS = 45
MAX_IMAGES_PER_PERIOD = 9

HDF5_NODATA = 65535
OUTPUT_NODATA = 65535

# Input HDF5 file has bands in ascending order, output order is reversed to match BACDM setup
# Reversal happens before cascading_selection, so use reverse order for picking
# band to check for pixel's having NoData in cascading_selection
# B12, 11, 8a, 8, 7, 6, 5, 4, 3, 2
SELECTION_BAND_INDEX = 3  # B8 (NIR)

# Output band descriptions, aligned with the reversed (descending) band order
BAND_NAMES = ('B12', 'B11', 'B8A', 'B8', 'B7', 'B6', 'B5', 'B4', 'B3', 'B2')

# ============================================================================
# MAIN
# ============================================================================

def main():
    if len(sys.argv) != 5:
        print(USAGE, file=sys.stderr)
        sys.exit(1)
    vchip_dir, hdf5_dir, before_output, after_output = sys.argv[1:5]

    print("Building tile index...")
    tile_index = build_tile_index(hdf5_dir)

    vchip_files = sorted(glob.glob(os.path.join(vchip_dir, "vchip_*_mask.tif")))
    print(f"\nFound {len(vchip_files)} vchip files\n")

    # Group vchips by tile so each HDF5 file is opened and read only once
    by_tile = defaultdict(list)
    unmatched = []
    for vchip_path in vchip_files:
        parsed = parse_vchip_filename(vchip_path)
        if parsed is None:
            print(f"Skipping (unexpected filename): {os.path.basename(vchip_path)}")
            continue

        x, y, date_str = parsed
        tile_id = find_tile_for_point(x, y, tile_index)

        if tile_id is None:
            print(f"No tile found for ({x}, {y}) — {os.path.basename(vchip_path)}")
            unmatched.append(vchip_path)
            continue

        by_tile[tile_id].append((vchip_path, date_str))

    # Process one tile at a time, loading its coordinate/time arrays just once
    for tile_id, vchips in by_tile.items():
        hdf5_path = tile_index[tile_id]['path']
        print(f"\nOpening tile {tile_id} ({len(vchips)} vchips)...")

        with h5py.File(hdf5_path, 'r') as h5f:
            xs: np.ndarray = h5f['xs'][:]  # type: ignore[index]
            ys: np.ndarray = h5f['ys'][:]  # type: ignore[index]
            ts: np.ndarray = h5f['ts'][:]  # type: ignore[index]
            values_ds = h5f['values']      # type: ignore[index]

            for vchip_path, date_str in vchips:
                break_ordinal = date_str_to_ordinal(date_str)
                print(f"  {os.path.basename(vchip_path)}  break date: {date_str}")
                process_vchip(
                    vchip_path, xs, ys, ts, values_ds,
                    break_ordinal, before_output, after_output,
                )

    if unmatched:
        print(f"\nWarning: {len(unmatched)} vchips had no matching tile")

# ============================================================================
# TILE INDEX
# ============================================================================

def build_tile_index(hdf5_dir):
    """
    Read the bounding box of every HDF5 tile file in hdf5_dir.

    Only xs and ys are read — the large values array is never touched.

    Returns
    -------
    dict mapping tile_id (str, e.g. 'T29SMC') to
        {'path': str, 'xmin': float, 'xmax': float, 'ymin': float, 'ymax': float}
    """
    index = {}
    h5_files = glob.glob(os.path.join(hdf5_dir, "*.h5"))
    if not h5_files:
        raise FileNotFoundError(f"No .h5 files found in {hdf5_dir}")

    for path in h5_files:
        tile_id = os.path.splitext(os.path.basename(path))[0]  # e.g. 'T29SMC'
        with h5py.File(path, 'r') as h5f:
            xs: np.ndarray = h5f['xs'][:]  # type: ignore[index]
            ys: np.ndarray = h5f['ys'][:]  # type: ignore[index]

        index[tile_id] = {
            'path': path,
            'xmin': float(xs.min()),
            'xmax': float(xs.max()),
            'ymin': float(ys.min()),
            'ymax': float(ys.max()),
        }
        print(f"  {tile_id}: x=[{index[tile_id]['xmin']:.0f}, {index[tile_id]['xmax']:.0f}]  "
              f"y=[{index[tile_id]['ymin']:.0f}, {index[tile_id]['ymax']:.0f}]")

    print(f"Tile index built: {len(index)} tiles")
    return index


def find_tile_for_point(x, y, tile_index):
    """
    Return the tile_id whose bounding box contains (x, y).

    If the point falls in more than one tile (edge overlap), the first match
    is returned. Returns None if no tile covers the point.

    Parameters
    ----------
    x, y : float
        Coordinates in EPSG:32629
    tile_index : dict
        Output of build_tile_index()

    Returns
    -------
    str or None
    """
    for tile_id, bbox in tile_index.items():
        if bbox['xmin'] <= x <= bbox['xmax'] and bbox['ymin'] <= y <= bbox['ymax']:
            return tile_id
    return None


# ============================================================================
# VCHIP HELPERS
# ============================================================================

VCHIP_PATTERN = re.compile(r"vchip_(-?\d+)_(-?\d+)_(\d{8})_mask\.tif$")

def parse_vchip_filename(filename):
    """
    Extract (x, y, date_str) from a vchip filename.

    Returns None if the filename does not match the expected pattern.
    """
    m = VCHIP_PATTERN.search(os.path.basename(filename))
    if m is None:
        return None
    x, y, date_str = int(m.group(1)), int(m.group(2)), m.group(3)
    return x, y, date_str


def date_str_to_ordinal(date_str):
    """Convert a YYYYMMDD string to a Python ordinal date integer."""
    return datetime.strptime(date_str, "%Y%m%d").toordinal()


def ordinal_array_to_yyyymmdd(ordinal_array, nodata):
    """
    Convert a 2D array of Python ordinal dates to YYYYMMDD integers.

    Pixels equal to `nodata` are preserved as `nodata` in the output.
    """
    result = np.full_like(ordinal_array, nodata, dtype=np.int64)
    unique_ordinals = np.unique(ordinal_array)
    for ordinal in unique_ordinals:
        if ordinal == nodata:
            continue
        d = datetime.fromordinal(int(ordinal))
        yyyymmdd = d.year * 10000 + d.month * 100 + d.day
        result[ordinal_array == ordinal] = yyyymmdd
    return result


# ============================================================================
# HDF5 LOADING
# ============================================================================

def load_hdf5_for_vchip(xs, ys, values_ds, vchip_transform, vchip_width, vchip_height, time_indices):
    """
    Load specific timesteps from an already-open HDF5 tile, placing pixels onto
    the vchip's grid.

    The output array matches the vchip's width/height/transform exactly, so any
    HDF5 pixel that falls outside the vchip extent is ignored, and any vchip
    cell with no matching HDF5 pixel is left as HDF5_NODATA.

    Parameters
    ----------
    xs, ys : ndarray
        Pre-loaded coordinate arrays for the tile.
    values_ds : h5py.Dataset
        Open reference to the tile's (time, band, pixel) values dataset.
    vchip_transform : affine.Affine
        Transform from the input vchip TIF (defines output pixel grid).
    vchip_width, vchip_height : int
        Output grid dimensions from the vchip.
    time_indices : ndarray
        Indices into the HDF5 ts/values arrays to load (pre- and post-break combined).

    Returns
    -------
    dict with keys:
        values       : (n_t, n_bands, vchip_height, vchip_width) uint16 array
        n_bands      : int
    or None if no HDF5 pixels fall within the vchip.
    """
    # Vchip geographic bounds
    xmin = vchip_transform.c
    ymax = vchip_transform.f
    pixel_size_x = vchip_transform.a
    pixel_size_y = -vchip_transform.e  # transform.e is negative for north-up
    xmax = xmin + vchip_width * pixel_size_x
    ymin = ymax - vchip_height * pixel_size_y

    _, n_bands, _ = values_ds.shape  # type: ignore[misc]

    # Keep HDF5 pixels whose centres fall inside the vchip
    pixel_mask = (xs >= xmin) & (xs < xmax) & (ys > ymin) & (ys <= ymax)
    pixel_indices = np.where(pixel_mask)[0]

    if len(pixel_indices) == 0:
        return None

    xs_chip = xs[pixel_mask]
    ys_chip = ys[pixel_mask]

    # Map each HDF5 pixel to its (row, col) in the vchip grid
    # Col 0 = smallest x (left), row 0 = largest y (top)
    cols = np.floor((xs_chip - xmin) / pixel_size_x).astype(int)
    rows = np.floor((ymax - ys_chip) / pixel_size_y).astype(int)
    cols = np.clip(cols, 0, vchip_width - 1)
    rows = np.clip(rows, 0, vchip_height - 1)

    # Load only the requested timesteps
    n_t = len(time_indices)
    result = np.full((n_t, n_bands, vchip_height, vchip_width), HDF5_NODATA, dtype=np.uint16)
    for i, t_idx in enumerate(time_indices):
        pixel_data: np.ndarray = values_ds[int(t_idx), :, pixel_indices]  # type: ignore[index]  # (n_bands, n_chip_pixels)
        # Mixing a slice with advanced indexing produces a (n_chip_pixels, n_bands)-shaped view, so pixel_data must be transposed to match.
        result[i, :, rows, cols] = pixel_data.T

    # HDF5 stores bands in ascending order; output expects descending order
    result = result[:, ::-1, :, :]

    return {
        'values': result,
        'n_bands': n_bands,
    }


# ============================================================================
# COMPOSITE AND SAVE
# ============================================================================

def process_vchip(vchip_path, xs, ys, ts, values_ds,
                  break_ordinal, before_output_dir, after_output_dir):
    """
    Compute pre/post composites for a single vchip and write output TIFs.

    Parameters
    ----------
    vchip_path : str
        Path to the input vchip mask TIF (used for spatial bounds and metadata).
    xs, ys : ndarray
        Pre-loaded coordinate arrays for the tile this vchip belongs to.
    ts : ndarray
        Pre-loaded ordinal timestamps for the tile.
    values_ds : h5py.Dataset
        Open reference to the tile's (time, band, pixel) values dataset.
    break_ordinal : int
        Break date as a Python ordinal.
    before_output_dir : str
    after_output_dir : str
    """
    stem = os.path.splitext(os.path.basename(vchip_path))[0]

    # Skip if both outputs already exist
    before_path = os.path.join(before_output_dir, f"{stem}_before.tif")
    after_path = os.path.join(after_output_dir, f"{stem}_after.tif")
    if os.path.exists(before_path) and os.path.exists(after_path):
        print(f"    Outputs already exist — skipping")
        return

    # Read vchip grid (transform, width, height) — this is the canonical output grid
    with rio.open(vchip_path) as src:
        vchip_meta = src.meta.copy()
        vchip_transform = src.transform
        vchip_width = src.width
        vchip_height = src.height

    # Select temporal indices before loading pixel data
    pre_indices, post_indices, pre_ordinals, post_ordinals = select_temporal_indices(
        ts, break_ordinal, TEMPORAL_WINDOW_DAYS, MAX_IMAGES_PER_PERIOD
    )
    if pre_indices is None or post_indices is None:
        print(f"    No images found in temporal window — skipping")
        return

    print(f"    {len(pre_indices)} pre-break and {len(post_indices)} post-break timesteps selected")

    # Load only the required timesteps, placed onto the vchip grid
    all_indices = np.concatenate([pre_indices, post_indices])
    chip = load_hdf5_for_vchip(
        xs, ys, values_ds, vchip_transform, vchip_width, vchip_height, all_indices
    )
    if chip is None:
        print(f"  No HDF5 pixels found within vchip bounds — skipping")
        return

    values = chip['values']   # (n_pre+n_post, n_bands, vchip_height, vchip_width)
    n_bands = chip['n_bands']

    # Split values back into pre and post
    n_pre = len(pre_indices)
    pre_data = values[:n_pre]
    post_data = values[n_pre:]

    # Cascading composite: pick first valid observation per pixel
    pre_selected, post_selected, pre_ts, post_ts = cascading_selection_optimized(
        pre_data, post_data, pre_ordinals, post_ordinals,
        SELECTION_BAND_INDEX, HDF5_NODATA, OUTPUT_NODATA
    )
    # pre_selected / post_selected: (n_bands, vchip_height, vchip_width), dtype int64
    # pre_ts / post_ts: (vchip_height, vchip_width) ordinal dates

    # Convert ordinal timestamps to YYYYMMDD integers (NODATA pixels stay as OUTPUT_NODATA)
    pre_dates = ordinal_array_to_yyyymmdd(pre_ts, OUTPUT_NODATA)
    post_dates = ordinal_array_to_yyyymmdd(post_ts, OUTPUT_NODATA)

    # Stack spectral bands + date band
    pre_output = np.vstack([pre_selected, pre_dates[np.newaxis, :, :]])
    post_output = np.vstack([post_selected, post_dates[np.newaxis, :, :]])

    # Build output metadata — grid inherits directly from the vchip
    # uint32 needed because YYYYMMDD values (~20250101) exceed uint16 range
    out_meta = vchip_meta.copy()
    out_meta.update({
        'count': n_bands + 1,
        'dtype': 'uint32',
        'nodata': OUTPUT_NODATA,
    })

    os.makedirs(before_output_dir, exist_ok=True)
    os.makedirs(after_output_dir, exist_ok=True)

    output_descriptions = BAND_NAMES[:n_bands] + ('date_yyyymmdd',)

    with rio.open(before_path, 'w', **out_meta) as dst:
        dst.write(pre_output.astype(np.uint32))
        dst.descriptions = output_descriptions
    print(f"  Wrote before: {before_path}")

    with rio.open(after_path, 'w', **out_meta) as dst:
        dst.write(post_output.astype(np.uint32))
        dst.descriptions = output_descriptions
    print(f"  Wrote after:  {after_path}")


# ============================================================================
# chip_creation.py functions
# ============================================================================

# These functions are in /scripts/utils/bacdm_utils/chip_creation.py
# Copied to here so that utils script does not need to be copied to CACN machine

def select_temporal_indices(all_ordinals, break_ordinal, window_days, max_images):
    """
    Select temporal indices for pre and post break periods.

    Returns:
    --------
    tuple of (pre_indices, post_indices, pre_ordinals, post_ordinals)
    """
    # Pre-break selection
    pre_mask = (all_ordinals <= break_ordinal) & (all_ordinals >= break_ordinal - window_days)
    pre_indices = np.where(pre_mask)[0]
    pre_ordinals = all_ordinals[pre_indices]

    # Sort by date descending and take max_images
    sorted_idx = np.argsort(pre_ordinals)[::-1][:max_images]
    pre_indices = pre_indices[sorted_idx]
    pre_ordinals = pre_ordinals[sorted_idx]

    # Post-break selection
    post_mask = (all_ordinals > break_ordinal) & (all_ordinals <= break_ordinal + window_days)
    post_indices = np.where(post_mask)[0]
    post_ordinals = all_ordinals[post_indices]

    # Sort by date ascending and take max_images
    sorted_idx = np.argsort(post_ordinals)[:max_images]
    post_indices = post_indices[sorted_idx]
    post_ordinals = post_ordinals[sorted_idx]

    if len(pre_indices) == 0 or len(post_indices) == 0:
        return None, None, None, None

    return pre_indices, post_indices, pre_ordinals, post_ordinals


def cascading_selection_optimized(pre_data, post_data, pre_ordinals, post_ordinals,
                                  selection_band_idx, s2_nodata, output_nodata):
    """
    Optimized cascading selection working directly with numpy arrays.

    Parameters:
    -----------
    pre_data : ndarray
        Shape (n_pre_timesteps, n_bands, height, width)
    post_data : ndarray
        Shape (n_post_timesteps, n_bands, height, width)
    pre_ordinals : ndarray
        Ordinal dates for pre-break timesteps
    post_ordinals : ndarray
        Ordinal dates for post-break timesteps
    selection_band_idx : int
        Band index to use for selection

    Returns:
    --------
    tuple of (pre_selected, post_selected, pre_timestamps, post_timestamps)
        Each 'selected' is shape (n_bands, height, width)
        Each 'timestamps' is shape (height, width) with ordinal dates
    """
    n_bands, height, width = pre_data.shape[1], pre_data.shape[2], pre_data.shape[3]

    # Extract selection band
    pre_selection_band = pre_data[:, selection_band_idx, :, :]  # (n_pre, h, w)
    post_selection_band = post_data[:, selection_band_idx, :, :]  # (n_post, h, w)

    # Find first valid timestep for each pixel (cascading)
    pre_valid_mask = pre_selection_band < s2_nodata  # (n_pre, h, w)
    pre_first_valid_idx = pre_valid_mask.argmax(axis=0)  # (h, w)
    pre_any_valid = pre_valid_mask.any(axis=0)  # (h, w)

    post_valid_mask = post_selection_band < s2_nodata
    post_first_valid_idx = post_valid_mask.argmax(axis=0)
    post_any_valid = post_valid_mask.any(axis=0)

    # Create output arrays
    pre_selected = np.full((n_bands, height, width), output_nodata, dtype=np.int64)
    post_selected = np.full((n_bands, height, width), output_nodata, dtype=np.int64)
    pre_timestamps = np.full((height, width), output_nodata, dtype=np.int64)
    post_timestamps = np.full((height, width), output_nodata, dtype=np.int64)

    # Gather data using advanced indexing
    # Create meshgrid for row and column indices
    row_indices, col_indices = np.meshgrid(np.arange(height), np.arange(width), indexing='ij')

    for band_idx in range(n_bands):
        # For each pixel, select the value from its first valid timestep
        pre_selected[band_idx] = pre_data[pre_first_valid_idx, band_idx, row_indices, col_indices]
        post_selected[band_idx] = post_data[post_first_valid_idx, band_idx, row_indices, col_indices]

    # Get timestamps
    pre_timestamps[:] = pre_ordinals[pre_first_valid_idx]
    post_timestamps[:] = post_ordinals[post_first_valid_idx]

    # Apply validity mask
    pre_selected[:, ~pre_any_valid] = output_nodata
    post_selected[:, ~post_any_valid] = output_nodata
    pre_timestamps[~pre_any_valid] = output_nodata
    post_timestamps[~post_any_valid] = output_nodata

    return pre_selected, post_selected, pre_timestamps, post_timestamps


if __name__ == "__main__":
    main()
