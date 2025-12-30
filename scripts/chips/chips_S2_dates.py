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
INPUT_TIF = r"C:\Users\Public\Documents\outputs_ROI\tabular\T29TQG\processed_outputs\output_raster_ccd_20180101_to_20211231.tif"
BREAK_DATE_BAND = 1
IS_BREAK_BAND = 3
# Min/Max dates for S2 files. Use format datetime(2024, 12, 31)
MIN_DATE = None
MAX_DATE = datetime(2022, 3, 1)

# Output directory for chips
OUTPUT_DIR = r"C:\Users\Public\Documents\outputs_ROI\tabular\T29TQG\processed_outputs\chips_s2_dates"

# Output filename pattern, {} will be filled with the x, y coordinates of the first pixel in the chip
# '(tile)_(break's start date)_(break's end date)_{}-{}.tif
OUTPUT_FILENAME = '03_T29TQG_20180101_20211231_{}-{}.tif'

# Chip dimensions in pixels
CHIP_WIDTH = 256
CHIP_HEIGHT = 256

# Overlap between adjacent chips in pixels
OVERLAP = 64

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
NODATA = 65535

# ============================================================================


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

        # DEBUG: Store spatial info for this file BEFORE appending to list
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
            print(f"  ⚠️  WARNING: Found {len(misaligned)} files with different spatial extents!")
            print(f"  Reference file: {reference['filename']}")
            print(f"    Bounds: {reference['bounds']}")
            print(f"    Transform: {reference['transform']}")
            print(f"    Shape: {reference['shape']}")
            print(f"  Misaligned files (first 5):")
            for fname in misaligned[:5]:
                idx = [i for i, info in enumerate(spatial_info) if info['filename'] == fname][0]
                info = spatial_info[idx]
                print(f"    - {fname}")
                print(f"      Bounds: {info['bounds']}")
                print(f"      Transform: {info['transform']}")
                print(f"      Shape: {info['shape']}")

            # FILTER OUT MISALIGNED FILES BEFORE CONCATENATION
            print(f"  🔧 Filtering out {len(misaligned)} misaligned files BEFORE concat...")
            # Create new lists excluding misaligned files
            tifs_xr_aligned = [tifs_xr[i] for i in range(len(tifs_xr)) if i not in misaligned_indices]
            tif_names_aligned = [tif_names_filtered[i] for i in range(len(tif_names_filtered)) if i not in misaligned_indices]
            tif_dates_aligned = [tif_dates_filtered[i] for i in range(len(tif_dates_filtered)) if i not in misaligned_indices]
            tif_dates_ord_aligned = [tif_dates_ord_filtered[i] for i in range(len(tif_dates_ord_filtered)) if i not in misaligned_indices]

            # Update the filtered lists
            tifs_xr = tifs_xr_aligned
            tif_names_filtered = tif_names_aligned
            tif_dates_filtered = tif_dates_aligned
            tif_dates_ord_filtered = tif_dates_ord_aligned
            time_var_filtered = xr.Variable('time', tif_dates_ord_aligned)

            # Update ordinal to unix mapping
            ordinal_to_unix_ms = {}
            for fname, ordinal in zip(tif_names_filtered, tif_dates_ord_filtered):
                match = timestamp_pattern.search(fname)
                if match:
                    unix_timestamp_ms = int(match.group(1))
                    ordinal_to_unix_ms[ordinal] = unix_timestamp_ms

            print(f"  ✅ Filtered! New count: {len(tifs_xr)} files (was {len(tifs_xr) + len(misaligned)})")


    # DEBUG: Check spatial properties before concat
    print(f"  DEBUG: Spatial properties of first 3 files before concat:")
    for i, da in enumerate(tifs_xr[:3]):
        print(f"    File {i} ({tif_names_filtered[i]}): bounds={da.rio.bounds()}, shape={da.shape}")

    # Concatenate along time dimension
    geotiffs_da = xr.concat(tifs_xr, dim=time_var_filtered, join='outer')
    geotiffs_da = geotiffs_da.chunk({'time': 1}) # One chunk per time step

    # DEBUG: Check spatial properties after concat
    print(f"  DEBUG: After concat: bounds={geotiffs_da.rio.bounds()}, shape={geotiffs_da.shape}")

    print(f"  Loaded xarray with shape: {geotiffs_da.shape}")
    print(f"  Filtered out {filtered_out_count} images due to not overlapping input tif boundary")

    # Return misaligned filenames for comparison between collections
    return geotiffs_da, ordinal_to_unix_ms, [info['filename'] for info in spatial_info if info['filename'] in misaligned]


def spatial_subset_by_window(xarray_da, window):
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

    result = xarray_da.rio.isel_window(window)
    return result


def select_temporal_window_xarray(xarray_da, break_ordinal, window_days=45,
                                   max_images=9, pre_break=True):
    """
    Select up to max_images within window_days of break, ordered by proximity.

    Parameters:
    -----------
    xarray_da : xr.DataArray
        DataArray with time dimension in ordinals
    break_ordinal : int
        Break date as ordinal
    window_days : int
        Temporal window in days
    max_images : int
        Maximum number of images to return
    pre_break : bool
        If True, select dates before break; if False, after break

    Returns:
    --------
    xr.DataArray or None
        DataArray with selected time steps (shape: n_images, band, y, x)
        Returns None if no images found in window
    """
    if break_ordinal is None:
        return None

    times = xarray_da.time.values

    if pre_break:
        # Find times before break within window
        mask = (times < break_ordinal) & (times >= break_ordinal - window_days)
        valid_times = times[mask]
        # Sort by proximity (closest to break first)
        valid_times = np.sort(valid_times)[::-1][:max_images]
    else:
        # Find times after break within window
        mask = (times >= break_ordinal) & (times <= break_ordinal + window_days)
        valid_times = times[mask]
        # Sort by proximity (closest to break first)
        valid_times = np.sort(valid_times)[:max_images]

    if len(valid_times) == 0:
        return None

    # Select these time steps from xarray
    indices = [i for i, t in enumerate(xarray_da.time.values) if t in valid_times]
    return xarray_da.isel(time=indices)
    # return xarray_da.sel(time=valid_times)


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

def cascading_selection(image_stack_xr, nodata=65535):
    """
    Apply cascading selection using index-based gathering.

    Finds first valid image considering ALL bands together,
    then extracts all bands from that same image to maintain
    spectral consistency.

    Parameters:
    -----------
    image_stack_xr : xr.DataArray
        DataArray with dimensions (time, band, y, x)
    nodata : int
        NODATA sentinel value

    Returns:
    --------
    tuple of (xr.DataArray, xr.DataArray)
        First element: Selected values of shape (band, y, x)
        Second element: Timestamp (ordinal) of the selected image for each pixel, shape (y, x)
    """
    if image_stack_xr is None:
        return None, None

    # get index of first image where all bands have data
    valid_mask = image_stack_xr < nodata
    all_bands_valid = valid_mask.all(dim='band')
    first_valid_idx = all_bands_valid.argmax(dim='time')

    # Compute the index array (convert from dask to numpy)
    first_valid_idx = first_valid_idx.compute()

    result = image_stack_xr.isel(time=first_valid_idx)

    # Handle edge case: pixels where NO images have all bands valid
    any_image_all_valid = all_bands_valid.any(dim='time')
    result = result.where(any_image_all_valid, nodata)

    # Get the actual timestamp (ordinal) for each pixel using the indices
    # The time coordinate values are the ordinals
    timestamp_map = xr.DataArray(image_stack_xr.time.values, dims=['time'])
    pixel_timestamps = timestamp_map.isel(time=first_valid_idx)

    # Set timestamp to nodata where no valid images exist
    pixel_timestamps = pixel_timestamps.where(any_image_all_valid, nodata)

    return result, pixel_timestamps

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

def ordinal_to_unix_timestamp(ordinal_array, ordinal_to_unix_map, nodata=65535):
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
    result = np.full_like(ordinal_array, nodata, dtype=np.int64)

    # Get unique ordinal values (excluding nodata)
    unique_ordinals = np.unique(ordinal_array)
    unique_ordinals = unique_ordinals[unique_ordinals != nodata]

    # Convert each ordinal to Unix timestamp
    for ordinal in unique_ordinals:
        if ordinal in ordinal_to_unix_map:
            mask = ordinal_array == ordinal
            result[mask] = ordinal_to_unix_map[ordinal]

    return result

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

def load_combined_xarray(S2_IMAGES_FOLDER_B2_B11, TILE, b2b11_names, b2b11_dates, S2_IMAGES_FOLDER_4_BANDS, bands4_names, bands4_dates, filter_bounds=None):
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
    geotiffs_b2b11, timestamp_map_b2b11, misaligned_b2b11 = load_s2_timeseries_xarray(S2_IMAGES_FOLDER_B2_B11, TILE, b2b11_names, b2b11_dates, filter_bounds)

    print("Loading 4-band collection...")
    geotiffs_4bands, timestamp_map_4bands, misaligned_4bands = load_s2_timeseries_xarray(S2_IMAGES_FOLDER_4_BANDS, TILE, bands4_names, bands4_dates, filter_bounds)

    # DEBUG: Compare misaligned files between collections
    b2b11_set = set(misaligned_b2b11)
    bands4_set = set(misaligned_4bands)
    only_in_b2b11 = b2b11_set - bands4_set
    only_in_bands4 = bands4_set - b2b11_set
    in_both = b2b11_set & bands4_set

    print("\n" + "="*60)
    print("MISALIGNMENT COMPARISON BETWEEN COLLECTIONS:")
    print("="*60)
    print(f"Misaligned files in BOTH collections: {len(in_both)}")
    if in_both:
        print(f"  Files: {list(in_both)[:3]}..." if len(in_both) > 3 else f"  Files: {list(in_both)}")
    print(f"Misaligned ONLY in B2B11: {len(only_in_b2b11)}")
    if only_in_b2b11:
        print(f"  ⚠️  CRITICAL: These files will cause spatial misalignment!")
        print(f"  Files: {list(only_in_b2b11)[:3]}..." if len(only_in_b2b11) > 3 else f"  Files: {list(only_in_b2b11)}")
    print(f"Misaligned ONLY in 4-bands: {len(only_in_bands4)}")
    if only_in_bands4:
        print(f"  ⚠️  CRITICAL: These files will cause spatial misalignment!")
        print(f"  Files: {list(only_in_bands4)[:3]}..." if len(only_in_bands4) > 3 else f"  Files: {list(only_in_bands4)}")
    print("="*60 + "\n")

    # FILTER OUT FILES THAT ARE MISALIGNED IN ONE COLLECTION BUT NOT THE OTHER
    files_to_exclude = only_in_b2b11 | only_in_bands4
    if files_to_exclude:
        print(f"🔧 FIXING SPATIAL ALIGNMENT: Filtering out {len(files_to_exclude)} problematic files...")
        print(f"   Files to exclude: {list(files_to_exclude)}")

        # Get ordinals from filenames to filter time dimension
        timestamp_pattern = re.compile(r'S2SR_image_(\d{13})')
        ordinals_to_exclude = set()

        # Find ordinals for files to exclude
        for fname in files_to_exclude:
            match = timestamp_pattern.search(fname)
            if match:
                unix_ts = int(match.group(1))
                # Find corresponding ordinal in timestamp mappings
                for ordinal, ts in timestamp_map_b2b11.items():
                    if ts == unix_ts:
                        ordinals_to_exclude.add(ordinal)
                        break

        print(f"   Excluding {len(ordinals_to_exclude)} time steps (ordinals): {ordinals_to_exclude}")

        # Filter both collections by excluding these time steps using boolean indexing
        # Create boolean mask for time steps to keep (True = keep, False = exclude)
        keep_mask_b2b11 = np.array([t not in ordinals_to_exclude for t in geotiffs_b2b11.time.values])
        keep_mask_4bands = np.array([t not in ordinals_to_exclude for t in geotiffs_4bands.time.values])

        # Use isel with boolean indexing to filter
        geotiffs_b2b11_filtered = geotiffs_b2b11.isel(time=keep_mask_b2b11)
        geotiffs_4bands_filtered = geotiffs_4bands.isel(time=keep_mask_4bands)

        print(f"   B2B11 shape before filter: {geotiffs_b2b11.shape}, after: {geotiffs_b2b11_filtered.shape}")
        print(f"   4-bands shape before filter: {geotiffs_4bands.shape}, after: {geotiffs_4bands_filtered.shape}")
        print(f"✅ Spatial alignment fixed!\n")

        geotiffs_b2b11 = geotiffs_b2b11_filtered
        geotiffs_4bands = geotiffs_4bands_filtered

    print("Combining into one array...")
    geotiffs_combined = xr.concat([geotiffs_b2b11, geotiffs_4bands], dim='band', join='outer')

    # Ensure CRS is not corrupted from concat
    geotiffs_combined = geotiffs_combined.rio.write_crs(geotiffs_b2b11.rio.crs)

    # Merge timestamp mappings (they should be identical since files are filtered to common dates)
    timestamp_map_combined = {**timestamp_map_b2b11, **timestamp_map_4bands}

    print("Xarray loading complete!\n")
    return geotiffs_combined, timestamp_map_combined

if __name__ == "__main__":
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

        geotiffs_combined, timestamp_mapping = load_combined_xarray(S2_IMAGES_FOLDER_B2_B11, TILE, b2b11_names, b2b11_dates, S2_IMAGES_FOLDER_4_BANDS, bands4_names, bands4_dates, filter_bounds)

        metadata = src.meta.copy()
        band_names = src.descriptions

        print(f"Input image size: {src.meta['width']} x {src.meta['height']}")
        print(f"Number of bands: {src.count}")
        print(f"Band names: {band_names}")
        print(f"Chip size: {CHIP_WIDTH} x {CHIP_HEIGHT}")
        print(f"Overlap: {OVERLAP} pixels")
        print(f"Output directory: {OUTPUT_DIR}")

        # Create output directory if it doesn't exist
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        chip_count = 0
        for window, transform in get_chips(src, CHIP_WIDTH, CHIP_HEIGHT, OVERLAP):
            chip_data = src.read(window=window)

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
            print(f"  Window: col_off={window.col_off}, row_off={window.row_off}, width={window.width}, height={window.height}")

            # Spatially subset xarray for this chip
            spatial_subset_chip_xr = spatial_subset_by_window(geotiffs_combined, window)
            print(f"  Subset shape: {spatial_subset_chip_xr.shape}")
            print(f"  Subset x range: [{spatial_subset_chip_xr.x.values[0]}, {spatial_subset_chip_xr.x.values[-1]}]")
            print(f"  Subset y range: [{spatial_subset_chip_xr.y.values[0]}, {spatial_subset_chip_xr.y.values[-1]}]")

            # Temporal selection using xarray
            pre_selected_chip_xr = select_temporal_window_xarray(
                spatial_subset_chip_xr, break_ordinal, TEMPORAL_WINDOW_DAYS, MAX_IMAGES_PER_PERIOD, pre_break=True
            )
            post_selected_chip_xr = select_temporal_window_xarray(
                spatial_subset_chip_xr, break_ordinal, TEMPORAL_WINDOW_DAYS, MAX_IMAGES_PER_PERIOD, pre_break=False
            )

            if pre_selected_chip_xr is None or post_selected_chip_xr is None:
                print("  Warning: Could not find any images in temporal window, skipping chip")
                continue

            pre_selected_xr, pre_timestamps_xr = cascading_selection(pre_selected_chip_xr, NODATA)
            post_selected_xr, post_timestamps_xr = cascading_selection(post_selected_chip_xr, NODATA)

            if pre_selected_xr is None or post_selected_xr is None or pre_timestamps_xr is None or post_timestamps_xr is None:
                print("  Warning: Cascading selection failed, skipping chip")
                continue

            # Convert xarray to numpy for further processing
            # Shape: (band, y, x) -> (6, height, width)
            pre_selected = pre_selected_xr.values.transpose(2, 0, 1)
            post_selected = post_selected_xr.values.transpose(2, 0, 1)

            # Extract timestamp arrays (shape: y, x)
            pre_timestamps = pre_timestamps_xr.values
            post_timestamps = post_timestamps_xr.values

            # DEBUG: Verify per-pixel band consistency
            # Check that for a given pixel, all 6 bands come from the same source date
            valid_mask = pre_timestamps != NODATA
            if valid_mask.any():
                valid_indices = np.argwhere(valid_mask)
                sample_idx = valid_indices[0]  # First valid pixel
                y, x = sample_idx[0], sample_idx[1]

                print(f"  DEBUG: Per-pixel verification at pixel ({y},{x}):")
                print(f"    Pre timestamp ordinal: {pre_timestamps[y,x]}")
                print(f"    Pre band values (before reorder): {pre_selected[:, y, x]}")
                print(f"    Expected: All 6 bands should come from same S2 file")

            # Reorder to [B2, B3, B4, B8, B11, B12] for output
            pre_bands_reordered = reorder_bands(pre_selected)
            post_bands_reordered = reorder_bands(post_selected)

            # Convert ordinal timestamps to Unix timestamps in milliseconds
            print(f"  DEBUG: Pre-timestamps unique ordinals: {np.unique(pre_timestamps)}")
            print(f"  DEBUG: Post-timestamps unique ordinals: {np.unique(post_timestamps)}")
            print(f"  DEBUG: Timestamp mapping has {len(timestamp_mapping)} entries")
            print(f"  DEBUG: Sample mapping entries: {list(timestamp_mapping.items())[:3]}")

            pre_timestamps_unix = ordinal_to_unix_timestamp(pre_timestamps, timestamp_mapping, NODATA)
            post_timestamps_unix = ordinal_to_unix_timestamp(post_timestamps, timestamp_mapping, NODATA)

            print(f"  DEBUG: Pre-timestamps Unix unique values: {np.unique(pre_timestamps_unix)}")
            print(f"  DEBUG: Post-timestamps Unix unique values: {np.unique(post_timestamps_unix)}")

            # Stack pre/post bands and metadata into 16-band output
            output_bands = np.vstack([
                pre_bands_reordered,                                                        # Bands 0-5
                np.full((1, CHIP_HEIGHT, CHIP_WIDTH), chip_break_date, dtype=np.int64),   # Band 6
                post_bands_reordered,                                                       # Bands 7-12
                chip_data[IS_BREAK_BAND - 1][np.newaxis],                                   # Band 13
                pre_timestamps_unix[np.newaxis],                                            # Band 14
                post_timestamps_unix[np.newaxis]                                            # Band 15
            ])

            # Update metadata for output
            metadata['transform'] = transform
            metadata['width'], metadata['height'] = window.width, window.height
            metadata['count'] = 16  # 16 bands
            metadata['dtype'] = 'int64'
            metadata['nodata'] = NODATA

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
            print(f"  Wrote chip: {out_filepath}")
            if chip_count >= 1:
                break

        print(f"Successfully created {chip_count} chips in {OUTPUT_DIR}")