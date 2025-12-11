import os
import numpy as np
import rasterio as rio
from rasterio import windows
from datetime import datetime, timedelta
import sys

# Add parent directory to path to import pyccd modules
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
from pyccd.shared.read_files import read_tif_files_gee


# ============================================================================
# CONFIGURATION
# ============================================================================

# Input TIF file path and relevant bands
INPUT_TIF = r"/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/personal_tests/09_optimized_test_20180101_to_20211231.tif"
BREAK_DATE_BAND = 1
IS_BREAK_BAND = 4

# Output directory for chips
OUTPUT_DIR = r"/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/personal_tests/chips"

# Output filename pattern, {} will be filled with the x, y coordinates of the first pixel in the chip
# '(tile)_(break's start date)_(break's end date)_{}-{}.tif
OUTPUT_FILENAME = 'BDR300_20180101_20211231_{}-{}.tif'

# Chip dimensions in pixels
CHIP_WIDTH = 256
CHIP_HEIGHT = 256

# Overlap between adjacent chips in pixels
OVERLAP = 64

# Percentage in float of DGT pixels that need to have a break date
PROCESSING_THRESHOLD = 0.8

# S2 image folder paths (two separate collections)
S2_IMAGES_FOLDER_B2_B11 = r"/Users/domwelsh/green_ds/Thesis/s2_images_B2_B11/"
S2_IMAGES_FOLDER_4_BANDS = r"/Users/domwelsh/green_ds/Thesis/s2_images/"

# Tile name (e.g., T29TQF)
TILE = "T29TQF"  # Update based on your data

# Temporal window for image selection
TEMPORAL_WINDOW_DAYS = 45

# Maximum images to consider per period (pre/post)
MAX_IMAGES_PER_PERIOD = 9

# NODATA value for S2 imagery
NODATA = 65535

# ============================================================================


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

def chip_processing_check(chip_data, is_break_band_index):
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

def yyyymmdd_to_datetime(yyyymmdd):
    """
    Convert YYYYMMDD integer to datetime object.

    Parameters:
    -----------
    yyyymmdd : int
        Date in YYYYMMDD format

    Returns:
    --------
    datetime or None
        Datetime object or None if invalid
    """
    if yyyymmdd == 0 or yyyymmdd is None:
        return None

    try:
        date_str = str(int(yyyymmdd))
        year = int(date_str[:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])
        return datetime(year, month, day)
    except (ValueError, IndexError):
        return None

def select_images_in_window(break_datetime, all_dates, all_filenames,
                            window_days=45, max_images=9, pre_break=True):
    """
    Select up to max_images S2 images within window_days of break date.

    Parameters:
    -----------
    break_datetime : datetime
        The break date
    all_dates : list of datetime
        All available image dates
    all_filenames : list of str
        Corresponding filenames
    window_days : int
        Maximum days from break date
    max_images : int
        Maximum number of images to return
    pre_break : bool
        If True, select images before break; if False, after break

    Returns:
    --------
    list of str
        Selected filenames, sorted by temporal proximity (closest first)
    """
    if break_datetime is None:
        return []

    window = timedelta(days=window_days)
    candidates = []

    for date, filename in zip(all_dates, all_filenames):
        if pre_break:
            # Pre-break: date must be before break and within window
            if date < break_datetime and (break_datetime - date) <= window:
                candidates.append((date, filename))
        else:
            # Post-break: date must be after break and within window
            if date >= break_datetime and (date - break_datetime) <= window:
                candidates.append((date, filename))

    # Sort by temporal proximity (closest to break date first)
    if pre_break:
        candidates.sort(key=lambda x: break_datetime - x[0])  # Closest = smallest difference
    else:
        candidates.sort(key=lambda x: x[0] - break_datetime)

    # Return up to max_images filenames
    return [filename for date, filename in candidates[:max_images]]

def load_s2_window(image_paths, s2_folder, tile, window):
    """
    Load windowed S2 data from multiple images.

    Parameters:
    -----------
    image_paths : list of str
        List of S2 image filenames
    s2_folder : str
        Base folder path for S2 images
    tile : str
        Tile name (e.g., 'T29TQF')
    window : rasterio.windows.Window
        Window specification for chip extent

    Returns:
    --------
    numpy.ndarray or None
        Array of shape (n_images, n_bands, height, width)
        Returns None if no valid images loaded
    """
    if not image_paths:
        return None

    loaded_images = []

    for img_path in image_paths:
        full_path = os.path.join(s2_folder, tile, img_path)
        try:
            with rio.open(full_path) as src:
                # Read windowed data (all bands)
                chip_data = src.read(window=window)
                loaded_images.append(chip_data)
        except Exception as e:
            print(f"Warning: Could not load {img_path}: {e}")
            continue

    if not loaded_images:
        return None

    # Stack into single array: (n_images, n_bands, height, width)
    return np.stack(loaded_images, axis=0)

def apply_cascading_selection(image_stack, nodata=65535):
    """
    Apply cascading selection to choose best pixel from multiple images.

    For each pixel location, selects the first image (temporally closest)
    that has valid data (not NODATA).

    Parameters:
    -----------
    image_stack : numpy.ndarray
        Array of shape (n_images, n_bands, height, width)
    nodata : int
        NODATA sentinel value

    Returns:
    --------
    numpy.ndarray
        Selected values of shape (n_bands, height, width)
    """
    if image_stack is None or len(image_stack) == 0:
        # Return NODATA array if no images available
        # Default assumes 2 bands (B2, B11) or 4 bands (B3, B4, B8, B12)
        # Will be overridden by actual data shape when available
        n_bands, height, width = 2, CHIP_HEIGHT, CHIP_WIDTH
        return np.full((n_bands, height, width), nodata, dtype=np.uint16)

    n_images, n_bands, height, width = image_stack.shape
    result = np.full((n_bands, height, width), nodata, dtype=np.uint16)

    # Process each band separately
    for band_idx in range(n_bands):
        # Extract all images for this band: (n_images, height, width)
        band_stack = image_stack[:, band_idx, :, :]

        # Create condition list: [img0 < nodata, img1 < nodata, ...]
        conditions = [band_stack[i] < nodata for i in range(n_images)]

        # Create choice list: [img0, img1, img2, ...]
        choices = [band_stack[i] for i in range(n_images)]

        # Apply np.select - picks first valid (non-NODATA) value
        result[band_idx] = np.select(conditions, choices, default=nodata)

    return result


if __name__ == "__main__":
    tile = TILE
    in_path = os.path.dirname(INPUT_TIF)
    input_filename = os.path.basename(INPUT_TIF)

    # Load S2 image file lists for both band collections
    print("Loading S2 image file lists...")
    tif_names_b2b11, tif_dates_b2b11 = read_tif_files_gee(
        tile, os.path.join(S2_IMAGES_FOLDER_B2_B11, tile), max_date=datetime(2024, 12, 31)
    )
    tif_names_4bands, tif_dates_4bands = read_tif_files_gee(
        tile, os.path.join(S2_IMAGES_FOLDER_4_BANDS, tile), max_date=datetime(2024, 12, 31)
    )
    print(f"Found {len(tif_names_b2b11)} B2B11 images and {len(tif_names_4bands)} 4-band images")

    with rio.open(INPUT_TIF) as src:
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
            processed_proportion = chip_processing_check(chip_data, IS_BREAK_BAND - 1)
            if processed_proportion < PROCESSING_THRESHOLD:
                continue

            chip_break_date = determine_break_date(chip_data, BREAK_DATE_BAND - 1)
            if chip_break_date == 0:
                continue

            # Convert break date to datetime
            break_dt = yyyymmdd_to_datetime(chip_break_date)
            if break_dt is None:
                continue

            print(f"\nProcessing chip at ({window.col_off}, {window.row_off}), break date: {chip_break_date}")

            # Select images within temporal window for B2B11
            pre_images_b2b11 = select_images_in_window(
                break_dt, tif_dates_b2b11, tif_names_b2b11,
                TEMPORAL_WINDOW_DAYS, MAX_IMAGES_PER_PERIOD, pre_break=True
            )
            post_images_b2b11 = select_images_in_window(
                break_dt, tif_dates_b2b11, tif_names_b2b11,
                TEMPORAL_WINDOW_DAYS, MAX_IMAGES_PER_PERIOD, pre_break=False
            )

            # Select images within temporal window for 4-bands
            pre_images_4bands = select_images_in_window(
                break_dt, tif_dates_4bands, tif_names_4bands,
                TEMPORAL_WINDOW_DAYS, MAX_IMAGES_PER_PERIOD, pre_break=True
            )
            post_images_4bands = select_images_in_window(
                break_dt, tif_dates_4bands, tif_names_4bands,
                TEMPORAL_WINDOW_DAYS, MAX_IMAGES_PER_PERIOD, pre_break=False
            )

            print(f"  Pre-break: {len(pre_images_b2b11)} B2B11 images, {len(pre_images_4bands)} 4-band images")
            print(f"  Post-break: {len(post_images_b2b11)} B2B11 images, {len(post_images_4bands)} 4-band images")

            # Load windowed data for B2B11 images
            pre_stack_b2b11 = load_s2_window(pre_images_b2b11, S2_IMAGES_FOLDER_B2_B11, tile, window)
            post_stack_b2b11 = load_s2_window(post_images_b2b11, S2_IMAGES_FOLDER_B2_B11, tile, window)

            # Load windowed data for 4-band images
            pre_stack_4bands = load_s2_window(pre_images_4bands, S2_IMAGES_FOLDER_4_BANDS, tile, window)
            post_stack_4bands = load_s2_window(post_images_4bands, S2_IMAGES_FOLDER_4_BANDS, tile, window)

            # Apply cascading selection for each collection
            pre_selected_b2b11 = apply_cascading_selection(pre_stack_b2b11, NODATA)  # (2, 256, 256) - B2, B11
            post_selected_b2b11 = apply_cascading_selection(post_stack_b2b11, NODATA)

            pre_selected_4bands = apply_cascading_selection(pre_stack_4bands, NODATA)  # (4, 256, 256) - B3, B4, B8, B12
            post_selected_4bands = apply_cascading_selection(post_stack_4bands, NODATA)

            # Construct 14-band output
            # Band order: [B2, B3, B4, B8, B11, B12, metadata] × 2 (pre/post)
            # B2B11 collection has: [B2, B11]
            # 4-band collection has: [B3, B4, B8, B12]

            # Initialize output array: (14, height, width)
            output_bands = np.zeros((14, CHIP_HEIGHT, CHIP_WIDTH), dtype=np.uint16)

            # Pre-break bands (0-6): [B2, B3, B4, B8, B11, B12, break_date]
            output_bands[0] = pre_selected_b2b11[0]   # B2
            output_bands[1] = pre_selected_4bands[0]  # B3
            output_bands[2] = pre_selected_4bands[1]  # B4
            output_bands[3] = pre_selected_4bands[2]  # B8
            output_bands[4] = pre_selected_b2b11[1]   # B11
            output_bands[5] = pre_selected_4bands[3]  # B12
            output_bands[6] = chip_break_date         # Break date (broadcast to all pixels)

            # Post-break bands (7-13): [B2, B3, B4, B8, B11, B12, is_break]
            output_bands[7] = post_selected_b2b11[0]   # B2
            output_bands[8] = post_selected_4bands[0]  # B3
            output_bands[9] = post_selected_4bands[1]  # B4
            output_bands[10] = post_selected_4bands[2] # B8
            output_bands[11] = post_selected_b2b11[1]  # B11
            output_bands[12] = post_selected_4bands[3] # B12
            output_bands[13] = chip_data[IS_BREAK_BAND - 1]  # is_break values from input TIF

            # Update metadata for output
            metadata['transform'] = transform
            metadata['width'], metadata['height'] = window.width, window.height
            metadata['count'] = 14  # 14 bands
            metadata['dtype'] = 'uint16'
            metadata['nodata'] = NODATA

            # Write output chip
            out_filepath = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME.format(window.col_off, window.row_off))

            with rio.open(out_filepath, 'w', **metadata) as dst:
                dst.write(output_bands)
                dst.descriptions = (
                    'B2_pre', 'B3_pre', 'B4_pre', 'B8_pre', 'B11_pre', 'B12_pre', 'break_date',
                    'B2_post', 'B3_post', 'B4_post', 'B8_post', 'B11_post', 'B12_post', 'is_break'
                )

            chip_count += 1
            print(f"  Wrote chip: {out_filepath}")
            # if chip_count >= 5:
            #     break

        print(f"Successfully created {chip_count} chips in {OUTPUT_DIR}")