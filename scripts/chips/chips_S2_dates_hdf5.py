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
import glob
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
from scripts.utils.ccd_results_utils.segment_identification import yyyymmdd_to_ordinal
from scripts.utils.bacdm_utils.chip_creation import (
    select_temporal_indices,
    cascading_selection_optimized,
    ordinal_to_unix_timestamp,
)

# ============================================================================
# CONFIGURATION
# ============================================================================

# HDF5 file path
S2_HDF5_FILE =  r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\T29TNE_6bands_20180630_20211231.h5" #r"E:\T29TQG\T29TQG_6bands_lzf_compression.h5"

# ReferenceTIF file path and relevant bands
# INPUT_TIF should have the date of break, i.e. the most recent date of the 'before' window for time compositiing of the spectral values
RASTER_FOLDER_BEST_DATE=r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\tifs_at_best_break_date"
# INPUT_TIF = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\harmonized_to_tifs\output_20210305_570077_573077_4417818_4420818.tif" #  r"C:\Users\Public\Documents\new_parquets_2017_2025\tabular\T29TQG\processed_outputs\rasters\output_raster_ccd_20180101_to_20211231.tif"
BREAK_DATE_BAND = 1
IS_BREAK_BAND = 3

# Output directory for chips
OUTPUT_DIR = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\chips\all" #r"E:\T29TQG\05_hdf5_filters_test"

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

# NODATA value for S2 imagery (in h5 files, this is 65535; in output tif we will use 0 to save space since we are using uint16 and 65535 is reserved for NODATA)
S2_NODATA = 65535

# NODATA value for output raster
OUTPUT_NODATA = 0

# Optional date range filter. Set to None to use all available timesteps.
# Use format: datetime(2024, 12, 31)
MIN_DATE = None
MAX_DATE = None

# Optional spatial bounding box filter. Set to None to use full INPUT_TIF extent.
# Use format: (left, right, bottom, top) in the same CRS as INPUT_TIF.
SPATIAL_BOUNDS = None #(699960, 702520, 4646360, 4648920)

# Band index to use for initial cascading selection (0-indexed)
# After reordering to [B2, B11, B3, B4, B8, B12], index 0 is B2
SELECTION_BAND_INDEX = 3

# Automatically extract tile from INPUT_TIF path
'''
tile_match = re.search(r'T\d{2}[A-Z]{3}', INPUT_TIF)
if tile_match:
    TILE = tile_match.group()
    print(f"Automatically detected tile from path: {TILE}")
else:
    TILE = "T29TQG"
    print(f"Warning: Could not auto-detect tile from path. Using fallback: {TILE}")
'''
tile_match = '_'

# ============================================================================
# MAIN PROCESSING
# ============================================================================
def main():
    tif_files = glob.glob(os.path.join(RASTER_FOLDER_BEST_DATE, "*.tif"))
    if tif_files:
        for INPUT_TIF in tif_files:
            input_tif_stem = os.path.splitext(os.path.basename(INPUT_TIF))[0]
            OUTPUT_FILENAME = f'{input_tif_stem}_{{}}_{{}}.tif'
            print(OUTPUT_FILENAME)
            main_access_hdf5(INPUT_TIF,OUTPUT_FILENAME)

def main_access_hdf5(INPUT_TIF,OUTPUT_FILENAME):
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
        filepaths=[]

        for window, transform in get_chips(src, CHIP_WIDTH, CHIP_HEIGHT, OVERLAP):
            chip_start_time = time.time()
            total_attempts_count += 1

            # Generate output filename based on chip position
            out_filepath = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME.format(window.col_off, window.row_off))
            if out_filepath in filepaths:
                print(f"  Warning: Duplicate chip filename detected: {out_filepath}. This should not happen with unique window positions.")
                continue
            filepaths.append(out_filepath)

            # Read chip data from INPUT_TIF
            chip_data = src.read(window=window)

            # Check if chip meets processing threshold
            processed_proportion = pixel_proportion_check(chip_data, IS_BREAK_BAND - 1)
            if processed_proportion == 0.0:
                continue
            if processed_proportion < PROCESSING_THRESHOLD:
                continue

            # Determines most frequent break date in chip using the break date band; this is used as the reference break date for selecting pre and post break images from the HDF5 data; by using the most frequent break date among the pixels in the chip, we can help ensure that we are selecting images that are relevant for the majority of pixels in the chip, which can improve the quality of our change detection results; if we were to use a single pixel's break date or an arbitrary date, we might end up selecting images that are not representative of the actual break occurring in that area, which could lead to poorer performance of our model
            chip_break_date = determine_break_date(chip_data, BREAK_DATE_BAND - 1)
            if chip_break_date == 0:
                continue

            break_ordinal = yyyymmdd_to_ordinal(chip_break_date)
            if break_ordinal is None:
                continue

            print(f"\nProcessing chip at ({window.col_off}, {window.row_off}), break date: {chip_break_date},  adjusted break ordinal: {break_ordinal}, processed pixel proportion: {processed_proportion:.2%}")

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

            '''
            # convert UNIX ordinal dates to human-readable format for debugging
            pre_dates = [datetime.fromordinal(int(ordinal)).strftime('%Y-%m-%d') for ordinal in hdf5_loader.ts[pre_time_indices]]
            post_dates = [datetime.fromordinal(int(ordinal)).strftime('%Y-%m-%d') for ordinal in hdf5_loader.ts[post_time_indices]] 
            print(f"  Pre-break timestamps: {pre_dates}")
            print(f"  Post-break timestamps: {post_dates}")

            # print chip_pixel_indices, chip_grid_shape, chip_row_col
            print(f"  Chip pixel indices: {chip_pixel_indices}")
            print(f"  Distinct pixel indices: {len(np.unique(chip_pixel_indices))}")
            print(f"  Chip grid shape: {chip_grid_shape}")
            print(f"  Chip row/column: {chip_row_col}")
            '''

            # Load data for selected timesteps
            pre_data = hdf5_loader.load_timesteps_for_chip(
                chip_pixel_indices, pre_time_indices.tolist(), chip_grid_shape, chip_row_col
            )
            post_data = hdf5_loader.load_timesteps_for_chip(
                chip_pixel_indices, post_time_indices.tolist(), chip_grid_shape, chip_row_col
            )

            '''
            print(f"  Loaded pre-break data shape: {pre_data.shape if pre_data is not None else 'None'}")
            print(f"  Loaded post-break data shape: {post_data.shape if post_data is not None else 'None'}")

            # print values for pixel (128, 128) for debugging
            if pre_data is not None:
                print(f"  Sample pre-break pixel values at (128, 128): {pre_data[:, :, 128, 128]}")
            if post_data is not None:
                print(f"  Sample post-break pixel values at (128, 128): {post_data[:, :, 128, 128]}")
            '''

            if pre_data is None or post_data is None:
                print("  Warning: Failed to load data")
                continue

            '''
            # debug: some values of the selected data for the 1st band and a 16 by 16 extent of pixels in the middle of the chip
            if pre_data is not None:
                print(f"  Sample pre-break pixel values for the 1st band (16x16 extent): \n {pre_data[3, :12, :12]}")
            if post_data is not None:
                print(f"  Sample post-break pixel values for the 1st band (16x16 extent): {post_data[3,  :12, :12]}")
            '''

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

        end = time.time()
        total_time_minutes = (end - start) / 60
        creation_percent = (chip_count / total_attempts_count) * 100 if total_attempts_count > 0 else 0
        print(f"\nSuccessfully created {chip_count} chips out of {total_attempts_count} ({creation_percent:.2f}%) in {OUTPUT_DIR}")
        print(f"Total processing time: {total_time_minutes:.2f} minutes")


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

        # debugging prints for transform parameters:
        print(f"  Affine transform: {self.transform}")
        print(pixel_width, 0.0, x_min, 0.0, pixel_height, y_max)

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
                # Reshape the entire 2D timestep_data (6, 65536) into 3D (6, 256, 256)
                # Do this OUTSIDE any band loop
                reshaped_3d = timestep_data.reshape((self.n_bands, chip_height, chip_width))
                # Assign the entire 3D block to the result at index 'i'
                # This assumes chip_rows and chip_cols represent the full range
                result[i, :, :, :] = reshaped_3d
                if False:
                    # count how many NODATA are in the 1st band of timestep_data for the pixels in this chip
                    for k in range(self.n_bands):
                        nodata_count = np.sum(timestep_data[k-1, :] == S2_NODATA)
                        print(f"  NODATA count in {k}st band for chip pixels: {nodata_count} out of {len(chip_pixel_indices)} pixels")
                        print(f"  Sample pre-break pixel values for the {k}st band (16x16 extent): \n {result[i,k, :12, :12]}")

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



#------------------------------------

if __name__ == "__main__":
    main()