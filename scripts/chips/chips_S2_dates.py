"""
Takes a raster file which has bands for break date and is break (ie files produced from ccd_to_raster.py) and creates chips with S2 spectral readings from before after after the break date. Readings are Bands 2, 3, 4, 8, 11, and 12

Inputs:
- TIF with band for break date and is break. Usually the tif files produced from ccd_to_raster.py
- Path to directory with S2 images that contain B2 and B11 readings
- Path to directory with S2 images that contain B3, B4, B8, and B12 readings

Outputs:
- Individual 16 band tif files for each chip. 
    - Bands 1-6: Pre-break spectral values
    - Band 7: break date for the pixel (same break date used for all pixels in each chip)
    - Band 8-13: Post-break spectral values
    - Band 14: is_break value from ccd_to_raster.py output (-99 = No no data, -1 = uncertain break, 0 = had data but no break, 1 = valid break)
    - Band 15: Pre-break timestamp of S2 reading used for this pixel
    - Band 16: Post-break timestamp of S2 reading used for this pixel
"""


import os
import numpy as np
import rasterio as rio
from rasterio import windows
from datetime import datetime
import sys
import xarray as xr
import rioxarray
from dask.diagnostics import ProgressBar
import re
from rasterio.windows import bounds as window_bounds
import time
from functools import wraps

# Add parent directory to path to import pyccd modules
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
from pyccd.shared.read_files import read_tif_files_gee
from ccd_results_utils.segment_identification import yyyymmdd_to_ordinal


# ============================================================================
# CONFIGURATION
# ============================================================================

# Input TIF file path and relevant bands
INPUT_TIF = r"C:\Users\Public\Documents\new_parquets_2017_2025\tabular\T29TQG\processed_outputs\rasters\output_raster_ccd_20180101_to_20211231.tif"
BREAK_DATE_BAND = 1
IS_BREAK_BAND = 3
# Min/Max dates for S2 files. Use format datetime(2024, 12, 31)
MIN_DATE = datetime(2017, 10, 1)
MAX_DATE = datetime(2022, 3, 1)

# Output directory for chips
OUTPUT_DIR = r"E:\T29TQG\improved_tif_chips"

# Output filename pattern, {} will be filled with the x, y coordinates of the first pixel in the chip
# '(tile)_(break's start date)_(break's end date)_{}-{}.tif
OUTPUT_FILENAME = 'T29TQG_20180101_20211231_{}-{}.tif'

# Chip dimensions in pixels
CHIP_WIDTH = 256
CHIP_HEIGHT = 256

# Overlap between adjacent chips in pixels
OVERLAP = 128

# Percentage in float of DGT pixels that need to have a break date
PROCESSING_THRESHOLD = 0.0

# S2 image folder paths (two separate collections)
S2_IMAGES_FOLDER_B2_B11 = r"C:/Users/Public/Documents/s2_images_B2_B11/"
S2_IMAGES_FOLDER_4_BANDS = r"D:/s2_images/"

# Automatically extract tile from INPUT_TIF path
# Looks for pattern like T29TQF, T29TQG, etc. in the path
# Update fallback to set default if no tile is in path
tile_match = re.search(r'T\d{2}[A-Z]{3}', INPUT_TIF)
if tile_match:
    TILE = tile_match.group()
    print(f"Automatically detected tile from path: {TILE}")
else:
    # Fallback to manual specification if pattern not found
    TILE = "T29TQG"
    print(f"Warning: Could not auto-detect tile from path. Using fallback: {TILE}")

# Temporal window for image selection
TEMPORAL_WINDOW_DAYS = 45

# Maximum images to consider per period (pre/post)
MAX_IMAGES_PER_PERIOD = 9

# NODATA value for S2 imagery
S2_NODATA = 65535

# NODATA value for output raster
OUTPUT_NODATA = 0

# Use combined processing for pre and post (faster, but newer implementation)
# Band index to use for initial cascading selection (0-indexed in the 6-band combined array)
# 0=B2, 1=B11, 2=B3, 3=B4, 4=B8, 5=B12
# Recommend using B2 (index 0) or B8 (index 4) as they're typically most reliable
SELECTION_BAND_INDEX = 0

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

        # Format time appropriately
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


@timing_decorator
def load_s2_timeseries_xarray(s2_folder, tile, tif_names, tif_dates, filter_bounds=None):
    """
    Load full S2 time series into xarray DataArray with chunking for memory efficiency.

    Parameters:
    -----------
    s2_folder : str
        Base folder path for S2 images
    tile : str
        Tile name (e.g., 'T29TQF')
    tif_names : list of str
        List of S2 image filenames
    tif_dates : list of datetime
        Corresponding dates for each image
    filter_bounds : tuple, optional
        Bounds (minx, miny, maxx, maxy) to filter images. Only images overlapping these bounds will be loaded.

    Returns:
    --------
    tuple of (xr.DataArray, dict)
        First element: DataArray with dimensions (time, band, y, x) where time is in ordinal format
        Second element: Dictionary mapping ordinal dates to Unix timestamps in milliseconds
    """
    print(f"  Loading {len(tif_names)} images into xarray...")

    def bounds_overlap(bounds1, bounds2):
        """Check if two bounding boxes overlap. Bounds format: (minx, maxx, miny, maxy)"""
        # bounds1/2 format from xarray: (x_min, x_max, y_min, y_max)
        return not (bounds1[1] < bounds2[0] or  # bounds1 right < bounds2 left
                    bounds1[0] > bounds2[1] or  # bounds1 left > bounds2 right
                    bounds1[3] < bounds2[2] or  # bounds1 top < bounds2 bottom
                    bounds1[2] > bounds2[3])    # bounds1 bottom > bounds2 top

    # Load with spatial chunking aligned to chip size for optimal performance
    tifs_xr = []
    tif_dates_filtered = []
    tif_names_filtered = []
    filtered_out_count = 0

    # DEBUG: Track spatial properties of each file
    spatial_info = []

    for i, fname in enumerate(tif_names):
        da = rioxarray.open_rasterio(
            os.path.join(s2_folder, tile, fname),
            chunks={'x': -1, 'y': -1, 'band': -1}
        )

        # Current bounds format: (x_min, x_max, y_min, y_max)
        # Use min/max to ensure correct ordering since y-coords may be top-to-bottom (decreasing)
        current_bounds = (da.x.values[0], da.x.values[-1],
                          min(da.y.values[0], da.y.values[-1]),
                          max(da.y.values[0], da.y.values[-1]))

        # Filter check: skip images that don't overlap with filter_bounds
        if filter_bounds is not None:
            if not bounds_overlap(current_bounds, filter_bounds):
                filtered_out_count += 1
                continue  # Skip this image

        # DEBUG: Store spatial info for file
        spatial_info.append({
            'filename': fname,
            'bounds': da.rio.bounds(),
            'transform': da.rio.transform(),
            'shape': da.shape
        })

        tifs_xr.append(da)
        tif_dates_filtered.append(tif_dates[i])
        tif_names_filtered.append(fname)

    # Convert filtered dates to ordinals for time dimension
    tif_dates_ord_filtered = [d.toordinal() for d in tif_dates_filtered]
    time_var_filtered = xr.Variable('time', tif_dates_ord_filtered)

    # Extract Unix timestamps from filenames and create ordinal -> unix_timestamp mapping
    ordinal_to_unix_ms = {}
    timestamp_pattern = re.compile(r'S2SR_image_(\d{13})')
    for fname, ordinal in zip(tif_names_filtered, tif_dates_ord_filtered):
        match = timestamp_pattern.search(fname)
        if match:
            unix_timestamp_ms = int(match.group(1))
            ordinal_to_unix_ms[ordinal] = unix_timestamp_ms

    # DEBUG: Check which files have different spatial extents
    if len(spatial_info) > 0:
        reference = spatial_info[0]
        misaligned = []
        misaligned_indices = []
        for i, info in enumerate(spatial_info[1:], start=1):
            if (info['bounds'] != reference['bounds'] or
                info['transform'] != reference['transform'] or
                info['shape'] != reference['shape']):
                misaligned.append(info['filename'])
                misaligned_indices.append(i)

        if misaligned:
            print(f"  WARNING: Found {len(misaligned)} files with different spatial extents!")

    # Concatenate along time dimension
    geotiffs_da = xr.concat(tifs_xr, dim=time_var_filtered, join='outer')
    geotiffs_da = geotiffs_da.chunk({'time': 1}) # One chunk per time step

    print(f"  Loaded xarray with shape: {geotiffs_da.shape}")
    print(f"  Filtered out {filtered_out_count} images due to not overlapping input tif boundary")

    # Return misaligned filenames for comparison between collections
    return geotiffs_da, ordinal_to_unix_ms, [info['filename'] for info in spatial_info if info['filename'] in misaligned]


@timing_decorator
def spatial_subset_by_window(xarray_da, window, input_transform):
    """
    Extract chip extent from xarray DataArray using rasterio window.

    Parameters:
    -----------
    xarray_da : xr.DataArray
        DataArray with dimensions (time, band, y, x)
    window : rasterio.windows.Window
        Window specification for chip extent

    Returns:
    --------
    xr.DataArray
        DataArray subset to window extent
    """
    # Convert window pixel coords to xarray slice indices
    # x_slice = slice(window.col_off, window.col_off + window.width)
    # y_slice = slice(window.row_off, window.row_off + window.height)

    # result = xarray_da.rio.isel_window(window)
    # return result

    # Convert window to geographic bounds using INPUT_TIF's transform
    minx, miny, maxx, maxy = window_bounds(window, input_transform)
    
    # Select by geographic coordinates instead of pixel indices
    result = xarray_da.rio.clip_box(minx=minx, miny=miny, maxx=maxx, maxy=maxy)
    return result


@timing_decorator
@timing_decorator
def select_temporal_window_pre_and_post(xarray_da, break_ordinal, window_days=45, max_images=9):
    """
    Select temporal windows for both pre and post break in a single operation.

    Parameters:
    -----------
    xarray_da : xr.DataArray
        DataArray with time dimension in ordinals
    break_ordinal : int
        Break date as ordinal
    window_days : int
        Temporal window in days
    max_images : int
        Maximum number of images to return per period

    Returns:
    --------
    tuple of (xr.DataArray, xr.DataArray) or (None, None)
        First element: Pre-break DataArray (shape: n_images, band, y, x)
        Second element: Post-break DataArray (shape: n_images, band, y, x)
        Returns (None, None) if either period has no images
    """
    if break_ordinal is None:
        return None, None

    times = xarray_da.time.values

    # Pre-break selection
    pre_mask = (times <= break_ordinal) & (times >= break_ordinal - window_days)
    pre_valid_times = times[pre_mask]
    pre_valid_times = np.sort(pre_valid_times)[::-1][:max_images]

    # Post-break selection
    post_mask = (times > break_ordinal) & (times <= break_ordinal + window_days)
    post_valid_times = times[post_mask]
    post_valid_times = np.sort(post_valid_times)[:max_images]

    # Check if both have data
    if len(pre_valid_times) == 0 or len(post_valid_times) == 0:
        return None, None

    # Select time steps for both
    pre_indices = [i for i, t in enumerate(times) if t in pre_valid_times]
    post_indices = [i for i, t in enumerate(times) if t in post_valid_times]

    pre_result = xarray_da.isel(time=pre_indices).sortby('time', ascending=False)
    post_result = xarray_da.isel(time=post_indices).sortby('time')

    return pre_result, post_result


@timing_decorator
def cascading_selection_single_band_pre_and_post(pre_stack_xr, post_stack_xr, band_index=0, s2_nodata=65535, output_nodata=0):
    """
    Apply cascading selection using only a single band to determine required dates.

    This is a lightweight version that identifies which date is needed for each pixel
    without loading all bands. Use this first, then load remaining bands only for
    the required dates.

    Parameters:
    -----------
    pre_stack_xr : xr.DataArray
        Pre-break DataArray with dimensions (time, band, y, x)
    post_stack_xr : xr.DataArray
        Post-break DataArray with dimensions (time, band, y, x)
    band_index : int
        Index of band to use for selection (0-indexed)
    s2_nodata : int
        NODATA sentinel value
    output_nodata : int
        NODATA value for output

    Returns:
    --------
    tuple of (pre_timestamps, post_timestamps, pre_unique_dates, post_unique_dates)
        pre_timestamps: Ordinal timestamp for each pixel (y, x)
        post_timestamps: Ordinal timestamp for each pixel (y, x)
        pre_unique_dates: List of unique ordinal dates needed for pre-break
        post_unique_dates: List of unique ordinal dates needed for post-break
    """
    if pre_stack_xr is None or post_stack_xr is None:
        return None, None, None, None

    # Select only the specified band for both pre and post
    pre_single_band = pre_stack_xr.isel(band=band_index)
    post_single_band = post_stack_xr.isel(band=band_index)

    # PRE-BREAK: Find first valid image for this band
    pre_valid_mask = pre_single_band < s2_nodata
    pre_first_valid_idx = pre_valid_mask.argmax(dim='time')

    # POST-BREAK: Find first valid image for this band
    post_valid_mask = post_single_band < s2_nodata
    post_first_valid_idx = post_valid_mask.argmax(dim='time')

    # Compute both index arrays together
    computed_indices = xr.Dataset({
        'pre_idx': pre_first_valid_idx,
        'post_idx': post_first_valid_idx
    }).compute()

    pre_first_valid_idx = computed_indices['pre_idx']
    post_first_valid_idx = computed_indices['post_idx']

    # Handle edge case: pixels where NO valid images exist
    pre_any_valid = pre_valid_mask.any(dim='time')
    post_any_valid = post_valid_mask.any(dim='time')

    # Get timestamps for each pixel
    pre_timestamp_map = xr.DataArray(pre_stack_xr.time.values, dims=['time'])
    pre_pixel_timestamps = pre_timestamp_map.isel(time=pre_first_valid_idx)
    pre_pixel_timestamps = pre_pixel_timestamps.where(pre_any_valid, output_nodata)

    post_timestamp_map = xr.DataArray(post_stack_xr.time.values, dims=['time'])
    post_pixel_timestamps = post_timestamp_map.isel(time=post_first_valid_idx)
    post_pixel_timestamps = post_pixel_timestamps.where(post_any_valid, output_nodata)

    # Compute timestamps
    computed_timestamps = xr.Dataset({
        'pre_ts': pre_pixel_timestamps,
        'post_ts': post_pixel_timestamps
    }).compute()

    pre_timestamps = computed_timestamps['pre_ts'].values
    post_timestamps = computed_timestamps['post_ts'].values

    # Get unique dates needed (excluding nodata)
    pre_unique_dates = np.unique(pre_timestamps)
    pre_unique_dates = pre_unique_dates[pre_unique_dates != output_nodata].tolist()

    post_unique_dates = np.unique(post_timestamps)
    post_unique_dates = post_unique_dates[post_unique_dates != output_nodata].tolist()

    return pre_timestamps, post_timestamps, pre_unique_dates, post_unique_dates


@timing_decorator
def load_remaining_bands_for_dates(xarray_da, required_dates, band_indices_to_load):
    """
    Load specific bands for only the required dates.

    Parameters:
    -----------
    xarray_da : xr.DataArray
        Full DataArray with dimensions (time, band, y, x)
    required_dates : list
        List of ordinal dates to load
    band_indices_to_load : list
        List of band indices to load (0-indexed)

    Returns:
    --------
    xr.DataArray
        DataArray subset to specified dates and bands
    """
    if not required_dates:
        return None

    # Select only the required time steps
    time_mask = xarray_da.time.isin(required_dates)
    subset_times = xarray_da.isel(time=time_mask)

    # Select only the required bands
    subset_bands = subset_times.isel(band=band_indices_to_load)

    return subset_bands


@timing_decorator
def gather_bands_by_timestamp(timestamp_array, band_data_dict, all_band_indices, output_nodata=0):
    """
    Gather all bands for each pixel based on its timestamp using pre-loaded date-specific data.

    Parameters:
    -----------
    timestamp_array : numpy.ndarray
        Array of shape (y, x) containing ordinal dates for each pixel
    band_data_dict : dict
        Dictionary mapping ordinal dates to xr.DataArray with shape (band, y, x)
    all_band_indices : list
        List of all band indices in output order
    output_nodata : int
        NODATA value for output

    Returns:
    --------
    xr.DataArray
        Array of shape (band, y, x) with gathered band values
    """
    height, width = timestamp_array.shape
    n_bands = len(all_band_indices)

    # Initialize output array
    output = np.full((n_bands, height, width), output_nodata, dtype=np.int64)

    # For each unique date, gather pixels that use that date
    unique_dates = np.unique(timestamp_array)
    unique_dates = unique_dates[unique_dates != output_nodata]

    for date in unique_dates:
        if date not in band_data_dict:
            continue

        # Get mask of pixels that need this date
        pixel_mask = (timestamp_array == date)

        # Get band data for this date
        band_data = band_data_dict[date].values  # Shape: (band, y, x)

        # Assign to output where mask is True
        for band_idx in range(n_bands):
            output[band_idx][pixel_mask] = band_data[band_idx][pixel_mask]

    return output


@timing_decorator
def get_chips(ds, chip_width, chip_height, overlap):
    """
    Generate chips from a rasterio dataset with overlap.

    Parameters:
    -----------
    ds : rasterio dataset
        The source dataset
    chip_width : int
        Width of each chip in pixels
    chip_height : int
        Height of each chip in pixels
    overlap : int
        Number of pixels to overlap between adjacent chips

    Yields:
    -------
    window : rasterio.windows.Window
        Window object for reading the chip
    transform : affine.Affine
        Geospatial transform for the chip
    """
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
    """
    Calculate the proportion of processed pixels that have a break

    Parameters:
    -----------
    chip_data : numpy array
        The chip data with all bands
    is_break_band_index : int
        The index of the IS_BREAK_BAND (0-indexed)

    Returns:
    --------
    float
        Proportion of processed pixels that have a break
    """
    is_break_band = chip_data[is_break_band_index]

    break_count = ((is_break_band == 1) | (is_break_band == -1)).sum()
    no_break_count = (is_break_band == 0).sum()
    total_processed = break_count + no_break_count

    # Avoid division by zero
    if total_processed == 0:
        return 0.0

    return break_count / total_processed

@timing_decorator
def determine_break_date(chip_data, break_date_band_index):
    """
    Determine most frequent break date in chip

    chip_data : numpy array
        The chip data with all bands
    break_date_band_index : int
        The index of the BREAK_DATE_BAND (0-indexed)

    Returns:
    --------
    int
        Most frequent break date in YYYYMMDD format (returns 0 if no valid dates found)
    """
    import numpy as np

    break_date_band = chip_data[break_date_band_index]
    valid_dates = break_date_band[break_date_band > 0]

    if len(valid_dates) == 0:
        return 0

    unique_dates, counts = np.unique(valid_dates, return_counts=True)
    max_count_index = np.argmax(counts)

    return int(unique_dates[max_count_index])

@timing_decorator
def reorder_bands(bands_combined):
    """
    Reorder combined bands from [B2, B11, B3, B4, B8, B12] to [B2, B3, B4, B8, B11, B12]

    Parameters:
    -----------
    bands_combined : numpy.ndarray
        Array of shape (6, height, width) with bands in order [B2, B11, B3, B4, B8, B12]

    Returns:
    --------
    numpy.ndarray
        Array of shape (6, height, width) with bands reordered to [B2, B3, B4, B8, B11, B12]
    """
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
    """
    Convert array of ordinal dates to Unix timestamps in milliseconds using a mapping.

    Parameters:
    -----------
    ordinal_array : numpy.ndarray
        Array containing ordinal date values
    ordinal_to_unix_map : dict
        Dictionary mapping ordinal dates to Unix timestamps in milliseconds
    nodata : int
        NODATA sentinel value that should be preserved

    Returns:
    --------
    numpy.ndarray
        Array with ordinal values converted to Unix timestamps (nodata preserved)
    """
    result = np.full_like(ordinal_array, output_nodata, dtype=np.int64)

    # Get unique ordinal values (excluding nodata)
    unique_ordinals = np.unique(ordinal_array)
    unique_ordinals = unique_ordinals[unique_ordinals != output_nodata]

    # Convert each ordinal to Unix timestamp
    for ordinal in unique_ordinals:
        if ordinal in ordinal_to_unix_map:
            mask = ordinal_array == ordinal
            result[mask] = ordinal_to_unix_map[ordinal]

    return result

@timing_decorator
def s2_band_files_identical_check(first_files_names, first_files_dates, second_files_names, second_files_dates):
    """
    Safety check that S2 tif files have identical dates for combining the 2 bands with the 4 bands files
    
    Parameters:
    -----------
    first_files_names : list
        Names of the first list of tif files
    irst_files_dates : list
        Dates of the first list of tif files
    second_files_names : list
        Names of the second list of tif files
    second_files_dates : list
        Dates of the second list of tif files

    Returns:
    --------
    tuple
        tuple of the 4 different lists that have now been filtered to only include identical dates
    """
    available_dates = set(first_files_dates) & set(second_files_dates)
    print("Filtering images to common dates...")
    first_names_filtered = [name for name, date in zip(first_files_names, first_files_dates) if date in available_dates]
    first_dates_filtered = [date for date in first_files_dates if date in available_dates]
    second_names_filtered = [name for name, date in zip(second_files_names, second_files_dates) if date in available_dates]
    second_dates_filtered = [date for date in second_files_dates if date in available_dates]
    print(f"Filtered to {len(first_names_filtered)} common dates")
    return first_names_filtered, first_dates_filtered, second_names_filtered, second_dates_filtered

@timing_decorator
def load_combined_xarray(S2_IMAGES_FOLDER_B2_B11,
                         TILE,
                         b2b11_names,
                         b2b11_dates,
                         S2_IMAGES_FOLDER_4_BANDS,
                         bands4_names,
                         bands4_dates,
                         filter_bounds=None):
    """
    Loads the 2 different S2 band files into xarrays and then combines them

    Parameters:
    -----------
    filter_bounds : tuple, optional
        Bounds (minx, maxx, miny, maxy) to filter images. Only images overlapping these bounds will be loaded.

    Returns:
    --------
    tuple of (xr.DataArray, dict)
        First element: Combined DataArray with all bands
        Second element: Dictionary mapping ordinal dates to Unix timestamps in milliseconds
    """
    print("\nLoading S2 time series into xarray...")
    print("Loading B2B11 collection...")
    geotiffs_b2b11, timestamp_map_b2b11, misaligned_b2b11 = load_s2_timeseries_xarray(S2_IMAGES_FOLDER_B2_B11, 
                                                                                      TILE, 
                                                                                      b2b11_names, 
                                                                                      b2b11_dates, 
                                                                                      filter_bounds
                                                                                      )

    print("Loading 4-band collection...")
    geotiffs_4bands, timestamp_map_4bands, misaligned_4bands = load_s2_timeseries_xarray(S2_IMAGES_FOLDER_4_BANDS, 
                                                                                         TILE, 
                                                                                         bands4_names, 
                                                                                         bands4_dates, 
                                                                                         filter_bounds
                                                                                         )

    print("Combining into one array...")
    # Use 'exact' join to ensure no silent misalignment
    geotiffs_combined = xr.concat([geotiffs_b2b11, geotiffs_4bands], dim='band', join='exact')

    # Ensure CRS is not corrupted from concat
    geotiffs_combined = geotiffs_combined.rio.write_crs(geotiffs_b2b11.rio.crs)

    # Merge timestamp mappings (they should be identical since files are filtered to common dates)
    timestamp_map_combined = {**timestamp_map_b2b11, **timestamp_map_4bands}

    print("Xarray loading complete!\n")
    return geotiffs_combined, timestamp_map_combined

if __name__ == "__main__":
    start = time.time()
    ProgressBar().register()

    # Load S2 image file lists for both band collections
    print("Loading S2 image file lists...")
    tif_names_b2b11, tif_dates_b2b11 = read_tif_files_gee(
        TILE, os.path.join(S2_IMAGES_FOLDER_B2_B11, TILE), MAX_DATE, MIN_DATE
    )
    tif_names_bands4, tif_dates_bands4 = read_tif_files_gee(
        TILE, os.path.join(S2_IMAGES_FOLDER_4_BANDS, TILE), MAX_DATE, MIN_DATE
    )
    print(f"Found {len(tif_names_b2b11)} B2B11 images and {len(tif_names_bands4)} 4-band images")

    b2b11_names, b2b11_dates, bands4_names, bands4_dates = s2_band_files_identical_check(tif_names_b2b11, tif_dates_b2b11, tif_names_bands4, tif_dates_bands4)

    # Open INPUT_TIF to get bounds for filtering S2 images and for processing
    print("\nReading INPUT_TIF bounds for filtering S2 images...")
    with rio.open(INPUT_TIF) as src:
        input_bounds = src.bounds
        # Convert rasterio BoundingBox to (minx, maxx, miny, maxy) format for xarray
        filter_bounds = (input_bounds.left, input_bounds.right, input_bounds.bottom, input_bounds.top)
        print(f"INPUT_TIF bounds: x=[{filter_bounds[0]}, {filter_bounds[1]}], y=[{filter_bounds[2]}, {filter_bounds[3]}]")

        geotiffs_combined, timestamp_mapping = load_combined_xarray(S2_IMAGES_FOLDER_B2_B11, 
                                                                    TILE, 
                                                                    b2b11_names, 
                                                                    b2b11_dates, 
                                                                    S2_IMAGES_FOLDER_4_BANDS, 
                                                                    bands4_names, 
                                                                    bands4_dates, 
                                                                    filter_bounds
                                                                    )

        # xarray automatically sorts in ascending order, need to reverse Y coordinates to return to descending order
        geotiffs_combined = geotiffs_combined.reindex(y=geotiffs_combined.y[::-1])

        metadata = src.meta.copy()
        band_names = src.descriptions

        print(f"Input image size: {src.meta['width']} x {src.meta['height']}")
        print(f"Number of bands: {src.count}")
        print(f"Band names: {band_names}")
        print(f"Chip size: {CHIP_WIDTH} x {CHIP_HEIGHT}")
        print(f"Overlap: {OVERLAP} pixels")
        print(f"Output directory: {OUTPUT_DIR}")
        print(f"Processing mode: TWO-STAGE OPTIMIZED")

        # Create output directory if it doesn't exist
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        total_attempts_count = 0
        chip_count = 0
        for window, transform in get_chips(src, CHIP_WIDTH, CHIP_HEIGHT, OVERLAP):
            chip_start_time = time.time()
            total_attempts_count += 1

            # Timer: Read chip data
            read_start = time.time()
            chip_data = src.read(window=window)
            print(f"  ⏱️  read_chip_data: {time.time() - read_start:.2f} seconds")

            # Check if chip meets processing threshold
            processed_proportion = pixel_proportion_check(chip_data, IS_BREAK_BAND - 1)
            if processed_proportion == 0.0:
                print("No pixels have breaks")
                continue
            if processed_proportion < PROCESSING_THRESHOLD:
                print("Chip failed processing proportion check")
                continue

            chip_break_date = determine_break_date(chip_data, BREAK_DATE_BAND - 1)
            if chip_break_date == 0:
                print("Chip has no break date")
                continue

            # Convert break date to ordinal for xarray temporal indexing
            break_ordinal = yyyymmdd_to_ordinal(chip_break_date)
            if break_ordinal is None:
                print("Chip has no ordinal break date")
                continue

            print(f"\nProcessing chip at ({window.col_off}, {window.row_off}), break date: {chip_break_date}")

            # Spatially subset xarray for this chip
            spatial_subset_chip_xr = spatial_subset_by_window(geotiffs_combined, window, src.transform)

            # TWO-STAGE OPTIMIZED LOADING
            # Stage 1: Use single band to determine required dates
            print("  Stage 1: Single-band cascading selection...")

            # Temporal selection for pre and post
            pre_selected_chip_xr, post_selected_chip_xr = select_temporal_window_pre_and_post(
                spatial_subset_chip_xr, break_ordinal, TEMPORAL_WINDOW_DAYS, MAX_IMAGES_PER_PERIOD
            )

            if pre_selected_chip_xr is None or post_selected_chip_xr is None:
                print("  Warning: Could not find any images in temporal window, skipping chip")
                continue

            # Single-band cascading selection to determine required dates
            pre_timestamps, post_timestamps, pre_dates_needed, post_dates_needed = cascading_selection_single_band_pre_and_post(
                pre_selected_chip_xr, post_selected_chip_xr, SELECTION_BAND_INDEX, S2_NODATA, OUTPUT_NODATA
            )

            if pre_timestamps is None or post_timestamps is None or pre_dates_needed is None or post_dates_needed is None:
                print("  Warning: Single-band cascading selection failed, skipping chip")
                continue

            print(f"  Stage 1 complete: Pre needs {len(pre_dates_needed)} dates, Post needs {len(post_dates_needed)} dates")

            # Stage 2: Load all bands only for required dates
            print("  Stage 2: Loading all bands for required dates...")

            # Get all band indices
            all_band_indices = list(range(6))

            # Load all 6 bands for pre-break dates
            pre_all_bands = load_remaining_bands_for_dates(
                pre_selected_chip_xr, pre_dates_needed, all_band_indices
            )

            # Load all 6 bands for post-break dates
            post_all_bands = load_remaining_bands_for_dates(
                post_selected_chip_xr, post_dates_needed, all_band_indices
            )

            if pre_all_bands is None or post_all_bands is None:
                print("  Warning: Failed to load bands, skipping chip")
                continue

            # Compute all bands
            pre_all_bands = pre_all_bands.compute()
            post_all_bands = post_all_bands.compute()

            # Create dictionaries mapping dates to band data
            # For pre-break
            pre_band_dict = {}
            for date in pre_dates_needed:
                # Select by date VALUE, not by index position
                # All 6 bands are already loaded, so just select this date
                pre_band_dict[date] = pre_all_bands.sel(time=date)

            # For post-break
            post_band_dict = {}
            for date in post_dates_needed:
                # Select by date VALUE, not by index position
                post_band_dict[date] = post_all_bands.sel(time=date)

            # Gather all bands for each pixel based on timestamp
            pre_selected = gather_bands_by_timestamp(pre_timestamps, pre_band_dict, all_band_indices, OUTPUT_NODATA)
            post_selected = gather_bands_by_timestamp(post_timestamps, post_band_dict, all_band_indices, OUTPUT_NODATA)

            print("  Stage 2 complete: All bands gathered")

            # Reorder to [B2, B3, B4, B8, B11, B12] for output
            pre_bands_reordered = reorder_bands(pre_selected)
            post_bands_reordered = reorder_bands(post_selected)

            pre_timestamps_unix = ordinal_to_unix_timestamp(pre_timestamps, timestamp_mapping, OUTPUT_NODATA)
            post_timestamps_unix = ordinal_to_unix_timestamp(post_timestamps, timestamp_mapping, OUTPUT_NODATA)

            # Timer: Stack arrays
            stack_start = time.time()
            # Stack pre/post bands and metadata into 16-band output
            output_bands = np.vstack([
                pre_bands_reordered,                                                        # Bands 0-5
                np.full((1, CHIP_HEIGHT, CHIP_WIDTH), chip_break_date, dtype=np.int64),   # Band 6
                post_bands_reordered,                                                       # Bands 7-12
                chip_data[IS_BREAK_BAND - 1][np.newaxis],                                   # Band 13
                pre_timestamps_unix[np.newaxis],                                            # Band 14
                post_timestamps_unix[np.newaxis]                                            # Band 15
            ])
            print(f"  ⏱️  array_stacking: {time.time() - stack_start:.2f} seconds")

            # Update metadata for output
            metadata['transform'] = transform
            metadata['width'], metadata['height'] = window.width, window.height
            metadata['count'] = 16  # 16 bands
            metadata['dtype'] = 'int64'
            metadata['nodata'] = S2_NODATA

            # Timer: Write output chip
            write_start = time.time()
            out_filepath = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME.format(window.col_off, window.row_off))

            with rio.open(out_filepath, 'w', **metadata) as dst:
                dst.write(output_bands)
                dst.descriptions = (
                    'B2_pre', 'B3_pre', 'B4_pre', 'B8_pre', 'B11_pre', 'B12_pre', 'break_date',
                    'B2_post', 'B3_post', 'B4_post', 'B8_post', 'B11_post', 'B12_post', 'is_break',
                    'pre_timestamp', 'post_timestamp'
                )
            print(f"  ⏱️  write_output_file: {time.time() - write_start:.2f} seconds")

            chip_count += 1
            chip_end_time = time.time()
            chip_processing_time = (chip_end_time - chip_start_time) / 60
            print(f"  Wrote chip: {out_filepath}")
            print(f"  Processing time = {chip_processing_time:.2f} minutes")
            # if chip_count >= 5:
            #     break
        
        end = time.time()
        total_time_minutes = (end - start) / 60
        creation_percent = (chip_count / total_attempts_count) * 100
        print(f"Successfully created {chip_count} chips out of {total_attempts_count} ({creation_percent:.2f}) in {OUTPUT_DIR}")
        print(f"Process took {total_time_minutes:.2f} minutes")