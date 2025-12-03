import os
import rasterio as rio
from rasterio import windows


# ============================================================================
# CONFIGURATION
# ============================================================================

# Input TIF file path
INPUT_TIF = r"/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/personal_tests/09_optimized_test_20180101_to_20211231.tif"

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


if __name__ == "__main__":
    in_path = os.path.dirname(INPUT_TIF)
    input_filename = os.path.basename(INPUT_TIF)

    with rio.open(INPUT_TIF) as src:
        metadata = src.meta.copy()

        print(f"Input image size: {src.meta['width']} x {src.meta['height']}")
        print(f"Chip size: {CHIP_WIDTH} x {CHIP_HEIGHT}")
        print(f"Overlap: {OVERLAP} pixels")
        print(f"Output directory: {OUTPUT_DIR}")

        # Create output directory if it doesn't exist
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        chip_count = 0
        for window, transform in get_chips(src, CHIP_WIDTH, CHIP_HEIGHT, OVERLAP):
            metadata['transform'] = transform
            metadata['width'], metadata['height'] = window.width, window.height
            out_filepath = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME.format(window.col_off, window.row_off))

            with rio.open(out_filepath, 'w', **metadata) as dst:
                dst.write(src.read(window=window))

            chip_count += 1
            # if chip_count >= 5:
            #     break

        print(f"Successfully created {chip_count} chips in {OUTPUT_DIR}")