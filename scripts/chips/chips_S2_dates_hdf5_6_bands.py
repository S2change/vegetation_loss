"""
HDF5-optimized version of chips_S2_dates.py

This script is specifically designed for efficient processing of S2 data stored in HDF5 format.
It avoids the overhead of reshaping flat pixel arrays to 2D grids by working directly with
pixel indices.

HDF5 Structure Expected:
- values: (time, band, pixels) - flattened pixel array
- xs, ys: (pixels,) - coordinate arrays
- ts: (time,) - ordinal dates
- original_timestamps: (time,) - unix timestamps in milliseconds
- band_names attribute: ['B3', 'B4', 'B8', 'B12', 'B2', 'B11']

Outputs:
- Individual 16 band tif files for each chip.
    - Bands 1-6: Pre-break spectral values (B2, B3, B4, B8, B11, B12)
    - Band 7: break date for the pixel (same break date used for all pixels in each chip)
    - Band 8-13: Post-break spectral values (B2, B3, B4, B8, B11, B12)
    - Band 14: is_break value from ccd_to_raster.py output
    - Band 15: Pre-break timestamp of S2 reading used for this pixel
    - Band 16: Post-break timestamp of S2 reading used for this pixel
"""

import os
import numpy as np
import rasterio as rio
from rasterio import windows
from datetime import datetime
import sys
import re
import time
from functools import wraps
import h5py
from affine import Affine
from rasterio.windows import bounds as window_bounds

# Add parent directory to path to import pyccd modules
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
from ccd_results_utils.segment_identification import yyyymmdd_to_ordinal

# ============================================================================
# CONFIGURATION
# ============================================================================

# HDF5 file path
S2_HDF5_FILE = r"E:\T29TQG\T29TQG_6bands_lzf_compression.h5"

# Input TIF file path and relevant bands
INPUT_TIF = r"C:\Users\Public\Documents\new_parquets_2017_2025\tabular\T29TQG\processed_outputs\rasters\output_raster_ccd_20180101_to_20211231.tif"
BREAK_DATE_BAND = 1
IS_BREAK_BAND = 3

# Output directory for chips
OUTPUT_DIR = r"E:\T29TQG\05_hdf5_filters_test"

# Output filename pattern
OUTPUT_FILENAME = 'spatial_filter_test_1_T29TQG_20180101_20211231_{}-{}.tif'

# Chip dimensions in pixels
CHIP_WIDTH = 256
CHIP_HEIGHT = 256

# Overlap between adjacent chips in pixels
OVERLAP = 128

# Percentage of pixels that need to have a break date
PROCESSING_THRESHOLD = 0.0

# Temporal window for image selection
TEMPORAL_WINDOW_DAYS = 45

# Maximum images to consider per period (pre/post)
MAX_IMAGES_PER_PERIOD = 9

# NODATA value for S2 imagery
S2_NODATA = 65535

# NODATA value for output raster
OUTPUT_NODATA = 0

# Optional date range filter. Set to None to use all available timesteps.
# Use format: datetime(2024, 12, 31)
MIN_DATE = None
MAX_DATE = None

# Optional spatial bounding box filter. Set to None to use full INPUT_TIF extent.
# Use format: (left, right, bottom, top) in the same CRS as INPUT_TIF.
SPATIAL_BOUNDS = (699960, 702520, 4646360, 4648920)

# Band index to use for initial cascading selection (0-indexed)
# After reordering to [B2, B11, B3, B4, B8, B12], index 0 is B2
SELECTION_BAND_INDEX = 0

# Automatically extract tile from INPUT_TIF path
tile_match = re.search(r'T\d{2}[A-Z]{3}', INPUT_TIF)
if tile_match:
    TILE = tile_match.group()
    print(f"Automatically detected tile from path: {TILE}")
else:
    TILE = "T29TQG"
    print(f"Warning: Could not auto-detect tile from path. Using fallback: {TILE}")

# ============================================================================
# TIMING DECORATOR
# ============================================================================

def timing_decorator(func):
    """Decorator to measure and print function execution time"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed = end_time - start_time

        if elapsed < 1:
            time_str = f"{elapsed*1000:.2f} ms"
        elif elapsed < 60:
            time_str = f"{elapsed:.2f} seconds"
        else:
            time_str = f"{elapsed/60:.2f} minutes ({elapsed:.2f} seconds)"

        print(f"  ⏱️  {func.__name__}: {time_str}")
        return result
    return wrapper

# ============================================================================
# HDF5 DATA LOADER
# ============================================================================

class HDF5DataLoader:
    """
    Efficient HDF5 data loader that works with flat pixel arrays.
    Pre-computes spatial mappings to avoid repeated coordinate lookups.
    """

    def __init__(self, hdf5_path, input_tif_bounds, min_date=None, max_date=None):
        """
        Initialize HDF5 loader with spatial filtering.

        Parameters:
        -----------
        hdf5_path : str
            Path to HDF5 file
        input_tif_bounds : tuple
            Bounds (left, right, bottom, top) to filter spatially
        min_date : datetime, optional
            Only use timesteps on or after this date
        max_date : datetime, optional
            Only use timesteps on or before this date
        """
        print(f"\nInitializing HDF5 data loader: {hdf5_path}")
        self.hdf5_path = hdf5_path

        with h5py.File(hdf5_path, 'r') as h5f:
            # Load coordinate arrays
            xs = h5f['xs'][:]
            ys = h5f['ys'][:]
            ts_all = h5f['ts'][:]
            original_timestamps_all = h5f['original_timestamps'][:]

            # Get band names and count
            if 'band_names' in h5f.attrs:
                band_names = [b.decode('ascii') if isinstance(b, bytes) else b
                             for b in h5f.attrs['band_names']]
                print(f"  HDF5 band order: {band_names}")

            self.n_bands = h5f['values'].shape[1]
            self.n_timesteps = h5f['values'].shape[0]

            print(f"  Total timesteps: {self.n_timesteps}")
            print(f"  Total pixels: {len(xs)}")

        # Filter timesteps by date range
        time_mask = np.ones(len(ts_all), dtype=bool)
        if min_date is not None:
            time_mask &= ts_all >= min_date.toordinal()
        if max_date is not None:
            time_mask &= ts_all <= max_date.toordinal()

        self.time_indices = np.where(time_mask)[0]  # global HDF5 indices for filtered timesteps
        self.ts = ts_all[time_mask]
        self.original_timestamps = original_timestamps_all[time_mask]

        if min_date is not None or max_date is not None:
            print(f"  Date filter: {min_date} to {max_date} → {len(self.ts)} of {len(ts_all)} timesteps kept")

        # Apply spatial filtering
        minx, maxx, miny, maxy = input_tif_bounds
        pixel_mask = (xs >= minx) & (xs <= maxx) & (ys >= miny) & (ys <= maxy)

        self.filtered_pixel_indices = np.where(pixel_mask)[0]
        self.xs_filtered = xs[pixel_mask]
        self.ys_filtered = ys[pixel_mask]

        print(f"  Filtered to {len(self.filtered_pixel_indices)} pixels within INPUT_TIF bounds")

        # Create ordinal to unix timestamp mapping
        self.ordinal_to_unix_ms = {int(ordinal): int(unix_ms)
                                   for ordinal, unix_ms in zip(self.ts, self.original_timestamps)}

        # Compute unique coordinates and create grid dimensions
        self.unique_xs = np.unique(self.xs_filtered)
        self.unique_ys = np.unique(self.ys_filtered)
        self.grid_width = len(self.unique_xs)
        self.grid_height = len(self.unique_ys)

        print(f"  Filtered grid dimensions: {self.grid_height} x {self.grid_width}")

        # Calculate affine transform for the filtered data
        x_min = self.unique_xs.min()
        y_max = self.unique_ys.max()
        pixel_width = np.diff(self.unique_xs).min() if len(self.unique_xs) > 1 else 10
        pixel_height = -np.abs(np.diff(self.unique_ys).min() if len(self.unique_ys) > 1 else 10)

        self.transform = Affine(pixel_width, 0.0, x_min, 0.0, pixel_height, y_max)

        # Create coordinate-to-index mappings for reshaping
        self.x_to_col = {x: i for i, x in enumerate(self.unique_xs)}
        self.y_to_row = {y: i for i, y in enumerate(self.unique_ys)}

        # Band reordering: HDF5 has [B3, B4, B8, B12, B2, B11], we want [B2, B11, B3, B4, B8, B12]
        self.band_reorder = [4, 5, 0, 1, 2, 3]

    @timing_decorator
    def get_chip_pixel_indices(self, chip_window, input_tif_transform):
        """
        Get HDF5 pixel indices that fall within a chip window.

        Parameters:
        -----------
        chip_window : rasterio.windows.Window
            Window specification for chip extent
        input_tif_transform : affine.Affine
            Transform from INPUT_TIF

        Returns:
        --------
        tuple of (pixel_indices, chip_grid_shape, coord_to_chip_mapping)
            pixel_indices: indices into filtered_pixel_indices
            chip_grid_shape: (height, width) of chip
            coord_to_chip_mapping: dict mapping (x, y) to (row, col) in chip grid
        """
        # Convert window to geographic bounds
        minx, miny, maxx, maxy = window_bounds(chip_window, input_tif_transform)

        # Find pixels within chip bounds
        chip_mask = ((self.xs_filtered >= minx) & (self.xs_filtered <= maxx) &
                     (self.ys_filtered >= miny) & (self.ys_filtered <= maxy))

        chip_pixel_indices = np.where(chip_mask)[0]
        chip_xs = self.xs_filtered[chip_mask]
        chip_ys = self.ys_filtered[chip_mask]

        # Map pixels to row/col positions within the CHIP_WIDTH x CHIP_HEIGHT output grid
        # X: ascending (left to right), col 0 = smallest x
        # Y: descending (top to bottom), row 0 = largest y  <-- critical for correct orientation
        # Align to the INPUT_TIF pixel grid using the chip window origin
        # This ensures pixels are placed at their correct position within the 256x256 output
        pixel_size_x = float(self.unique_xs[1] - self.unique_xs[0]) if len(self.unique_xs) > 1 else 10.0
        pixel_size_y = float(self.unique_ys[1] - self.unique_ys[0]) if len(self.unique_ys) > 1 else 10.0  # positive, unique_ys is ascending

        # col/row offset of each pixel relative to the chip window top-left corner
        chip_cols = np.array([int(round((x - minx) / pixel_size_x)) for x in chip_xs])
        chip_rows = np.array([int(round((maxy - y) / pixel_size_y)) for y in chip_ys])

        # Clip to valid range in case of floating point edge cases
        chip_cols = np.clip(chip_cols, 0, CHIP_WIDTH - 1)
        chip_rows = np.clip(chip_rows, 0, CHIP_HEIGHT - 1)

        return chip_pixel_indices, (CHIP_HEIGHT, CHIP_WIDTH), (chip_rows, chip_cols)

    @timing_decorator
    def load_timesteps_for_chip(self, chip_pixel_indices, time_indices, chip_grid_shape, chip_row_col):
        """
        Load specific timesteps for specific pixels and reshape to chip grid.

        Parameters:
        -----------
        chip_pixel_indices : ndarray
            Indices into filtered_pixel_indices for this chip
        time_indices : list
            List of time indices to load
        chip_grid_shape : tuple
            (height, width) of chip
        chip_row_col : tuple
            (rows, cols) arrays for mapping flat pixels to grid

        Returns:
        --------
        ndarray
            Array of shape (n_timesteps, n_bands, height, width)
        """
        if len(time_indices) == 0:
            return None

        chip_height, chip_width = chip_grid_shape
        chip_rows, chip_cols = chip_row_col
        n_timesteps = len(time_indices)

        # Convert chip pixel indices to global HDF5 indices
        global_pixel_indices = self.filtered_pixel_indices[chip_pixel_indices]

        result = np.full((n_timesteps, self.n_bands, chip_height, chip_width),
                        S2_NODATA, dtype=np.uint16)

        with h5py.File(self.hdf5_path, 'r') as h5f:
            for i, time_idx in enumerate(time_indices):
                # Map filtered time index to global HDF5 index
                global_time_idx = int(self.time_indices[time_idx])
                timestep_data = h5f['values'][global_time_idx, :, global_pixel_indices]

                # Fill into grid using vectorized indexing
                # Need to loop over bands or use proper broadcasting
                for band_idx in range(self.n_bands):
                    result[i, band_idx, chip_rows, chip_cols] = timestep_data[band_idx, :]

        # Reorder bands from [B3, B4, B8, B12, B2, B11] to [B2, B11, B3, B4, B8, B12]
        result = result[:, self.band_reorder, :, :]

        return result

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

@timing_decorator
def get_chips(ds, chip_width, chip_height, overlap):
    """Generate chips from a rasterio dataset with overlap."""
    nols, nrows = ds.meta['width'], ds.meta['height']
    xstep = chip_width - overlap
    ystep = chip_height - overlap

    for x in range(0, nols, xstep):
        if x + chip_width > nols:
            x = nols - chip_width
        for y in range(0, nrows, ystep):
            if y + chip_height > nrows:
                y = nrows - chip_height
            window = windows.Window(x, y, chip_width, chip_height)
            transform = windows.transform(window, ds.transform)
            yield window, transform

@timing_decorator
def pixel_proportion_check(chip_data, is_break_band_index):
    """Calculate the proportion of processed pixels that have a break."""
    is_break_band = chip_data[is_break_band_index]
    break_count = ((is_break_band == 1) | (is_break_band == -1)).sum()
    no_break_count = (is_break_band == 0).sum()
    total_processed = break_count + no_break_count

    if total_processed == 0:
        return 0.0

    return break_count / total_processed

@timing_decorator
def determine_break_date(chip_data, break_date_band_index):
    """Determine most frequent break date in chip."""
    break_date_band = chip_data[break_date_band_index]
    valid_dates = break_date_band[break_date_band > 0]

    if len(valid_dates) == 0:
        return 0

    unique_dates, counts = np.unique(valid_dates, return_counts=True)
    max_count_index = np.argmax(counts)

    return int(unique_dates[max_count_index])

@timing_decorator
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

@timing_decorator
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

@timing_decorator
def reorder_bands(bands_combined):
    """Reorder from [B2, B11, B3, B4, B8, B12] to [B2, B3, B4, B8, B11, B12]."""
    return np.stack([
        bands_combined[0],  # B2
        bands_combined[2],  # B3
        bands_combined[3],  # B4
        bands_combined[4],  # B8
        bands_combined[1],  # B11
        bands_combined[5]   # B12
    ])

@timing_decorator
def ordinal_to_unix_timestamp(ordinal_array, ordinal_to_unix_map, output_nodata=0):
    """Convert array of ordinal dates to Unix timestamps in milliseconds."""
    result = np.full_like(ordinal_array, output_nodata, dtype=np.int64)
    unique_ordinals = np.unique(ordinal_array)
    unique_ordinals = unique_ordinals[unique_ordinals != output_nodata]

    for ordinal in unique_ordinals:
        if ordinal in ordinal_to_unix_map:
            mask = ordinal_array == ordinal
            result[mask] = ordinal_to_unix_map[ordinal]

    return result

# ============================================================================
# MAIN PROCESSING
# ============================================================================

if __name__ == "__main__":
    start = time.time()

    # Open INPUT_TIF to get bounds and for chip generation
    print("\nReading INPUT_TIF bounds...")
    with rio.open(INPUT_TIF) as src:
        input_bounds = src.bounds
        filter_bounds = (input_bounds.left, input_bounds.right, input_bounds.bottom, input_bounds.top)
        print(f"INPUT_TIF bounds: x=[{filter_bounds[0]}, {filter_bounds[1]}], y=[{filter_bounds[2]}, {filter_bounds[3]}]")

        if SPATIAL_BOUNDS is not None:
            sb_left, sb_right, sb_bottom, sb_top = SPATIAL_BOUNDS
            filter_bounds = (
                max(filter_bounds[0], sb_left),
                min(filter_bounds[1], sb_right),
                max(filter_bounds[2], sb_bottom),
                min(filter_bounds[3], sb_top),
            )
            print(f"Spatial filter applied. Intersected bounds: x=[{filter_bounds[0]}, {filter_bounds[1]}], y=[{filter_bounds[2]}, {filter_bounds[3]}]")

        # Initialize HDF5 data loader
        hdf5_loader = HDF5DataLoader(S2_HDF5_FILE, filter_bounds, min_date=MIN_DATE, max_date=MAX_DATE)

        metadata = src.meta.copy()
        band_names = src.descriptions

        print(f"\nInput image size: {src.meta['width']} x {src.meta['height']}")
        print(f"Number of bands: {src.count}")
        print(f"Band names: {band_names}")
        print(f"Chip size: {CHIP_WIDTH} x {CHIP_HEIGHT}")
        print(f"Overlap: {OVERLAP} pixels")
        print(f"Output directory: {OUTPUT_DIR}")
        print(f"Processing mode: HDF5 OPTIMIZED\n")

        # Create output directory if it doesn't exist
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        total_attempts_count = 0
        chip_count = 0

        for window, transform in get_chips(src, CHIP_WIDTH, CHIP_HEIGHT, OVERLAP):
            chip_start_time = time.time()
            total_attempts_count += 1

            # Read chip data from INPUT_TIF
            chip_data = src.read(window=window)

            # Check if chip meets processing threshold
            processed_proportion = pixel_proportion_check(chip_data, IS_BREAK_BAND - 1)
            if processed_proportion == 0.0:
                continue
            if processed_proportion < PROCESSING_THRESHOLD:
                continue

            chip_break_date = determine_break_date(chip_data, BREAK_DATE_BAND - 1)
            if chip_break_date == 0:
                continue

            break_ordinal = yyyymmdd_to_ordinal(chip_break_date)
            if break_ordinal is None:
                continue

            print(f"\nProcessing chip at ({window.col_off}, {window.row_off}), break date: {chip_break_date}")

            # Get pixel indices for this chip
            chip_pixel_indices, chip_grid_shape, chip_row_col = hdf5_loader.get_chip_pixel_indices(
                window, src.transform
            )

            if len(chip_pixel_indices) == 0:
                print("  Warning: No HDF5 pixels found for this chip")
                continue

            print(f"  Chip has {len(chip_pixel_indices)} pixels from HDF5")

            # Select temporal indices
            pre_time_indices, post_time_indices, pre_ordinals, post_ordinals = select_temporal_indices(
                hdf5_loader.ts, break_ordinal, TEMPORAL_WINDOW_DAYS, MAX_IMAGES_PER_PERIOD
            )

            if pre_time_indices is None or post_time_indices is None:
                print("  Warning: Could not find images in temporal window")
                continue

            print(f"  Selected {len(pre_time_indices)} pre-break and {len(post_time_indices)} post-break timesteps")

            # Load data for selected timesteps
            pre_data = hdf5_loader.load_timesteps_for_chip(
                chip_pixel_indices, pre_time_indices.tolist(), chip_grid_shape, chip_row_col
            )
            post_data = hdf5_loader.load_timesteps_for_chip(
                chip_pixel_indices, post_time_indices.tolist(), chip_grid_shape, chip_row_col
            )

            if pre_data is None or post_data is None:
                print("  Warning: Failed to load data")
                continue

            # Cascading selection
            pre_selected, post_selected, pre_timestamps, post_timestamps = cascading_selection_optimized(
                pre_data, post_data, pre_ordinals, post_ordinals,
                SELECTION_BAND_INDEX, S2_NODATA, OUTPUT_NODATA
            )

            # Reorder bands to final output order [B2, B3, B4, B8, B11, B12]
            pre_bands_reordered = reorder_bands(pre_selected)
            post_bands_reordered = reorder_bands(post_selected)

            # Convert timestamps to unix format
            pre_timestamps_unix = ordinal_to_unix_timestamp(
                pre_timestamps, hdf5_loader.ordinal_to_unix_ms, OUTPUT_NODATA
            )
            post_timestamps_unix = ordinal_to_unix_timestamp(
                post_timestamps, hdf5_loader.ordinal_to_unix_ms, OUTPUT_NODATA
            )

            # Resize to match chip dimensions if needed
            actual_height, actual_width = pre_bands_reordered.shape[1], pre_bands_reordered.shape[2]
            if actual_height != CHIP_HEIGHT or actual_width != CHIP_WIDTH:
                print(f"  Warning: Chip size mismatch. Expected {CHIP_HEIGHT}x{CHIP_WIDTH}, got {actual_height}x{actual_width}")
                # Pad or crop as needed
                padded_pre = np.full((6, CHIP_HEIGHT, CHIP_WIDTH), OUTPUT_NODATA, dtype=np.int64)
                padded_post = np.full((6, CHIP_HEIGHT, CHIP_WIDTH), OUTPUT_NODATA, dtype=np.int64)
                padded_pre_ts = np.full((CHIP_HEIGHT, CHIP_WIDTH), OUTPUT_NODATA, dtype=np.int64)
                padded_post_ts = np.full((CHIP_HEIGHT, CHIP_WIDTH), OUTPUT_NODATA, dtype=np.int64)

                h = min(actual_height, CHIP_HEIGHT)
                w = min(actual_width, CHIP_WIDTH)
                padded_pre[:, :h, :w] = pre_bands_reordered[:, :h, :w]
                padded_post[:, :h, :w] = post_bands_reordered[:, :h, :w]
                padded_pre_ts[:h, :w] = pre_timestamps_unix[:h, :w]
                padded_post_ts[:h, :w] = post_timestamps_unix[:h, :w]

                pre_bands_reordered = padded_pre
                post_bands_reordered = padded_post
                pre_timestamps_unix = padded_pre_ts
                post_timestamps_unix = padded_post_ts

            # Stack into 16-band output
            output_bands = np.vstack([
                pre_bands_reordered,                                                        # Bands 0-5
                np.full((1, CHIP_HEIGHT, CHIP_WIDTH), chip_break_date, dtype=np.int64),   # Band 6
                post_bands_reordered,                                                       # Bands 7-12
                chip_data[IS_BREAK_BAND - 1][np.newaxis],                                  # Band 13
                pre_timestamps_unix[np.newaxis],                                            # Band 14
                post_timestamps_unix[np.newaxis]                                            # Band 15
            ])

            # Update metadata for output
            metadata['transform'] = transform
            metadata['width'], metadata['height'] = window.width, window.height
            metadata['count'] = 16
            metadata['dtype'] = 'int64'
            metadata['nodata'] = OUTPUT_NODATA

            # Write output chip
            out_filepath = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME.format(window.col_off, window.row_off))

            with rio.open(out_filepath, 'w', **metadata) as dst:
                dst.write(output_bands)
                dst.descriptions = (
                    'B2_pre', 'B3_pre', 'B4_pre', 'B8_pre', 'B11_pre', 'B12_pre', 'break_date',
                    'B2_post', 'B3_post', 'B4_post', 'B8_post', 'B11_post', 'B12_post', 'is_break',
                    'pre_timestamp', 'post_timestamp'
                )

            chip_count += 1
            chip_end_time = time.time()
            chip_processing_time = (chip_end_time - chip_start_time) / 60
            print(f"  Wrote chip: {out_filepath}")
            print(f"  Chip processing time: {chip_processing_time:.2f} minutes")

            if chip_count >= 5:
                break

        end = time.time()
        total_time_minutes = (end - start) / 60
        creation_percent = (chip_count / total_attempts_count) * 100 if total_attempts_count > 0 else 0
        print(f"\nSuccessfully created {chip_count} chips out of {total_attempts_count} ({creation_percent:.2f}%) in {OUTPUT_DIR}")
        print(f"Total processing time: {total_time_minutes:.2f} minutes")