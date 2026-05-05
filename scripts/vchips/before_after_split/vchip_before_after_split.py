"""
For each vchip, identifies which Sentinel-2 tile HDF5 file covers it based on
the x/y coordinates in the vchip filename, loads pre/post break S2 composites,
and writes two output GeoTIFFs (before and after) per vchip.

Vchip filename format: vchip_{x}_{y}_{date}_mask.tif
HDF5 filename format:  {tile_id}.h5  (e.g. T29SMC.h5)
All coordinates are in EPSG:32629.

Output bands (same for before and after):
    B12, 11, 8a, 8, 7, 6, 5, 4, 3, 2, date_yyyymmdd
"""
import os
import sys
import re
import glob
import h5py
import numpy as np
import rasterio as rio
import geopandas as gpd
from collections import defaultdict
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

# VCHIP_DIR, HDF5_DIR, BEFORE_OUTPUT, AFTER_OUTPUT, and TILES_GPKG are supplied
# on the command line — see USAGE below.
USAGE = (
    "Usage: python vchip_before_after_split.py "
    "<vchip_dir> <hdf5_dir> <before_output_dir> <after_output_dir> <tiles_gpkg>\n"
    "  <tiles_gpkg>  Path to a GeoPackage of S2 tile polygons. Must have a\n"
    "                column 'Name' with tile IDs (e.g. T29SMC) matching the\n"
    "                HDF5 filenames, and CRS EPSG:32629."
)

# If True, vchips with both _before.tif and _after.tif already present will be
# skipped. Lets you safely resume a run after partial failure. Set to False if
# you want to overwrite existing outputs (e.g. after changing parameters).
SKIP_IF_EXISTS = True

# Temporal compositing parameters
TEMPORAL_WINDOW_DAYS = 45
MAX_IMAGES_PER_PERIOD = 9

#B2_VALID_MAX used for filtering out clouds when SELECTION_BAND_INDEX = 0
B2_VALID_MAX = 5000
HDF5_NODATA = 65535
OUTPUT_NODATA = 65535

# Input HDF5 file has bands in ascending order
# B2, 3, 4, 5, 6, 7, 8, 8a, 11, 12
SELECTION_BAND_INDEX = 0  # B2 (Blue)

# Output band descriptions, aligned with the reversed (descending) band order
BAND_NAMES = ('B12', 'B11', 'B8A', 'B8', 'B7', 'B6', 'B5', 'B4', 'B3', 'B2')

# ============================================================================
# MAIN
# ============================================================================

def main():
    if len(sys.argv) != 6:
        print(USAGE, file=sys.stderr)
        sys.exit(1)
    vchip_dir, hdf5_dir, before_output, after_output, tiles_gpkg = sys.argv[1:6]

    print("Building tile index...")
    tile_index = build_tile_index(tiles_gpkg, hdf5_dir)

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

def build_tile_index(tiles_gpkg, hdf5_dir):
    """
    Read tile bounding boxes from a GeoPackage of S2 tile polygons.

    Avoids opening any HDF5 files at startup — those are only opened later
    when a tile actually has vchips that need processing.

    Parameters
    ----------
    tiles_gpkg : str
        Path to the GeoPackage. Must have a 'Name' column with tile IDs
        matching the HDF5 filenames, and geometry in EPSG:32629.
    hdf5_dir : str
        Directory containing the per-tile .h5 files. Used only to build
        each tile's expected HDF5 path.

    Returns
    -------
    dict mapping tile_id (str, e.g. 'T29SMC') to
        {'path': str, 'xmin': float, 'xmax': float, 'ymin': float, 'ymax': float,
         'cx': float, 'cy': float}
    """
    gdf = gpd.read_file(tiles_gpkg)
    if 'Name' not in gdf.columns:
        raise ValueError(f"Expected 'Name' column in {tiles_gpkg}; got {list(gdf.columns)}")

    index = {}
    for _, row in gdf.iterrows():
        tile_id = row['Name']
        minx, miny, maxx, maxy = row.geometry.bounds
        index[tile_id] = {
            'path': os.path.join(hdf5_dir, f"{tile_id}.h5"),
            'xmin': float(minx),
            'xmax': float(maxx),
            'ymin': float(miny),
            'ymax': float(maxy),
            'cx': float((minx + maxx) / 2),
            'cy': float((miny + maxy) / 2),
        }
        print(f"  {tile_id}: x=[{minx:.0f}, {maxx:.0f}]  y=[{miny:.0f}, {maxy:.0f}]")

    print(f"Tile index built: {len(index)} tiles")
    return index


def find_tile_for_point(x, y, tile_index):
    """
    Return the tile_id whose bounding box contains (x, y).

    Tile polygons in the source gpkg overlap, so a point may fall inside
    multiple bboxes. Tiebreaker: pick the tile whose center is closest to
    the point (squared distance, no sqrt). This favors tiles where the
    vchip sits squarely inside, avoiding edge tiles with more missing data.

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
    best_tile = None
    best_dist_sq = float('inf')
    for tile_id, bbox in tile_index.items():
        if bbox['xmin'] <= x <= bbox['xmax'] and bbox['ymin'] <= y <= bbox['ymax']:
            dx = x - bbox['cx']
            dy = y - bbox['cy']
            dist_sq = dx * dx + dy * dy
            if dist_sq < best_dist_sq:
                best_dist_sq = dist_sq
                best_tile = tile_id
    return best_tile


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

def compute_vchip_pixel_mapping(xs, ys, vchip_transform, vchip_width, vchip_height):
    """
    Determine which HDF5 pixels fall in the vchip and where they map to.

    Returns (pixel_indices, rows, cols) or None if no HDF5 pixels fall in
    the vchip's bounds.
    """
    xmin = vchip_transform.c
    ymax = vchip_transform.f
    pixel_size_x = vchip_transform.a
    pixel_size_y = -vchip_transform.e
    xmax = xmin + vchip_width * pixel_size_x
    ymin = ymax - vchip_height * pixel_size_y

    pixel_mask = (xs >= xmin) & (xs < xmax) & (ys > ymin) & (ys <= ymax)
    pixel_indices = np.where(pixel_mask)[0]
    if len(pixel_indices) == 0:
        return None

    xs_chip = xs[pixel_mask]
    ys_chip = ys[pixel_mask]

    cols = np.floor((xs_chip - xmin) / pixel_size_x).astype(int)
    rows = np.floor((ymax - ys_chip) / pixel_size_y).astype(int)
    cols = np.clip(cols, 0, vchip_width - 1)
    rows = np.clip(rows, 0, vchip_height - 1)
    return pixel_indices, rows, cols


def cascade_one_side(values_ds, pixel_indices, rows, cols,
                     vchip_height, vchip_width, n_bands,
                     time_indices, ordinals):
    """
    Load timesteps in cascade order and stop early once every eligible pixel
    has a valid value.

    `time_indices` and `ordinals` must already be in cascade-priority order:
    descending date for pre-break (most recent first), ascending for post-break.
    `select_temporal_indices` already returns them in that order.

    For each timestep:
      1. Read the (n_bands, n_chip_pixels) slice from HDF5.
      2. Determine which pixels are still unfilled AND have valid SELECTION_BAND
         data this timestep.
      3. Fill those pixels in `selected` and `timestamps`.
      4. If no pixels remain unfilled, break — skip remaining timesteps.

    Output band order is reversed at fill time (descending B12 -> B2) so the
    consumer doesn't need to re-reverse later.

    Returns
    -------
    (selected, timestamps)
        selected   : (n_bands, vchip_height, vchip_width) int64, descending band order
        timestamps : (vchip_height, vchip_width) int64 ordinal dates
        Both use OUTPUT_NODATA where no valid observation was found.
    """
    selected = np.full((n_bands, vchip_height, vchip_width), OUTPUT_NODATA, dtype=np.int64)
    timestamps = np.full((vchip_height, vchip_width), OUTPUT_NODATA, dtype=np.int64)

    # Track which vchip-grid pixels still need a value. Only pixels with an
    # HDF5 source are eligible; everything else stays NODATA.
    unfilled = np.zeros((vchip_height, vchip_width), dtype=bool)
    unfilled[rows, cols] = True

    for t_idx, t_ord in zip(time_indices, ordinals):
        if not unfilled.any():
            break  # cascade complete

        # (n_bands, n_chip_pixels), ascending band order
        pixel_data: np.ndarray = values_ds[int(t_idx), :, pixel_indices]  # type: ignore[index]

        if SELECTION_BAND_INDEX == 0:
            valid_per_pixel = pixel_data[SELECTION_BAND_INDEX] < B2_VALID_MAX
        else:
            valid_per_pixel = pixel_data[SELECTION_BAND_INDEX] < HDF5_NODATA  # (n_chip_pixels,)
        still_unfilled_per_pixel = unfilled[rows, cols]
        fill_now = valid_per_pixel & still_unfilled_per_pixel

        if fill_now.any():
            fill_rows = rows[fill_now]
            fill_cols = cols[fill_now]
            # pixel_data[::-1] is a stride view — flip ascending -> descending
            # without copying. Then index pixels we want to fill.
            selected[:, fill_rows, fill_cols] = pixel_data[::-1][:, fill_now]
            timestamps[fill_rows, fill_cols] = int(t_ord)
            unfilled[fill_rows, fill_cols] = False

    return selected, timestamps


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
    before_path = os.path.join(before_output_dir, f"{stem}_before.tif")
    after_path = os.path.join(after_output_dir, f"{stem}_after.tif")

    if SKIP_IF_EXISTS and os.path.exists(before_path) and os.path.exists(after_path):
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

    # Compute vchip pixel mapping once; both sides reuse it.
    mapping = compute_vchip_pixel_mapping(
        xs, ys, vchip_transform, vchip_width, vchip_height
    )
    if mapping is None:
        print(f"  No HDF5 pixels found within vchip bounds — skipping")
        return
    pixel_indices, rows, cols = mapping
    n_bands = values_ds.shape[1]  # type: ignore[union-attr]

    # Cascading composite with early termination — one side at a time.
    # Stops loading timesteps once every eligible pixel is filled.
    pre_selected, pre_ts = cascade_one_side(
        values_ds, pixel_indices, rows, cols,
        vchip_height, vchip_width, n_bands,
        pre_indices, pre_ordinals,
    )
    post_selected, post_ts = cascade_one_side(
        values_ds, pixel_indices, rows, cols,
        vchip_height, vchip_width, n_bands,
        post_indices, post_ordinals,
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


if __name__ == "__main__":
    main()
