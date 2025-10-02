"""
PURPOSE:
This script validates that the output raster from ccd_break_filter_to_raster.py contains
a value for every pixel that exists in the input parquet files.

VALIDATION CHECKS:
1. Counts all unique pixels (x_coord, y_coord) in the input parquet files
2. Counts all pixels with data in the output raster (excluding NoData values)
3. Verifies that every input pixel has a corresponding raster value
4. Reports any discrepancies including missing pixels

INPUTS:
- input_directory: Directory containing the source parquet files
- output_raster_file: Path to the GeoTIFF file to validate

OUTPUTS:
- Console report showing:
  * Total unique pixels in parquet files
  * Total pixels with data in raster (includes 0 for filtered pixels)
  * Validation status (PASS/FAIL)
  * Details of any missing pixels
"""

import pandas as pd
import numpy as np
import glob
import os
import rasterio
from collections import defaultdict

## VALIDATION CONFIGS ##
##################################

# Set paths to match your ccd_break_filter_to_raster.py run
input_directory = "/Users/domwelsh/green_ds/Thesis/BDR_300_artigo"  # UPDATE - same as script input
output_raster_file = "/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/personal_tests/01_10_25_optimize_test_02.tif"  # UPDATE - output raster to validate

##################################

def extract_unique_pixels_from_parquet(input_dir):
    """
    Extract all unique pixel coordinates from parquet files in the input directory

    Returns:
    --------
    set: Set of (x_coord, y_coord) tuples representing all unique pixels
    """
    parquet_files = glob.glob(os.path.join(input_dir, "*.parquet"))

    if not parquet_files:
        raise ValueError(f"No parquet files found in {input_dir}")

    print(f"Found {len(parquet_files)} parquet files to process")

    unique_pixels = set()
    total_rows_processed = 0

    for i, file_path in enumerate(parquet_files, 1):
        print(f"Processing file {i}/{len(parquet_files)}: {os.path.basename(file_path)}")

        # Read parquet file
        df = pd.read_parquet(file_path)
        total_rows_processed += len(df)

        # Extract unique pixels from this file
        file_pixels = set(zip(df['x_coord'], df['y_coord']))
        unique_pixels.update(file_pixels)

        print(f"  - Rows in file: {len(df)}, Unique pixels in file: {len(file_pixels)}")

    print(f"\nTotal rows processed across all files: {total_rows_processed}")
    print(f"Total unique pixels found: {len(unique_pixels)}")

    return unique_pixels

def extract_pixels_from_raster(raster_file):
    """
    Extract all pixel coordinates that have data (not NoData) from the raster

    Returns:
    --------
    tuple: (set of (x_coord, y_coord) tuples, dict with statistics)
    """
    with rasterio.open(raster_file) as src:
        # Read the raster data
        raster_array = src.read(1)
        transform = src.transform
        nodata = src.nodata if src.nodata is not None else -9999

        print(f"\nRaster information:")
        print(f"  - Dimensions: {src.width} x {src.height}")
        print(f"  - Resolution: {transform.a} x {-transform.e} meters")
        print(f"  - NoData value: {nodata}")
        print(f"  - CRS: {src.crs}")

        # Create mask for non-NoData pixels
        data_mask = raster_array != nodata

        # Get row, col indices of non-NoData pixels
        rows, cols = np.where(data_mask)

        # Convert pixel indices to coordinates (pixel centers)
        raster_pixels = set()
        value_counts = defaultdict(int)

        for row, col in zip(rows, cols):
            # Get the coordinate of the pixel center
            x_coord, y_coord = rasterio.transform.xy(transform, row, col, offset='center')

            # Round to avoid floating point precision issues (assuming 10m resolution)
            x_coord = round(x_coord, 1)
            y_coord = round(y_coord, 1)

            raster_pixels.add((x_coord, y_coord))

            # Track value distribution
            pixel_value = raster_array[row, col]
            if pixel_value == 0:
                value_counts['filtered_out'] += 1
            else:
                value_counts['valid_breaks'] += 1

        stats = {
            'total_pixels_with_data': len(raster_pixels),
            'pixels_with_breaks': value_counts['valid_breaks'],
            'pixels_filtered_out': value_counts['filtered_out'],
            'nodata_pixels': np.sum(raster_array == nodata)
        }

        print(f"\nRaster pixel statistics:")
        print(f"  - Total pixels with data (non-NoData): {stats['total_pixels_with_data']}")
        print(f"  - Pixels with valid break dates: {stats['pixels_with_breaks']}")
        print(f"  - Pixels filtered out (value = 0): {stats['pixels_filtered_out']}")
        print(f"  - Pixels with NoData: {stats['nodata_pixels']}")

        return raster_pixels, stats

def validate_completeness(parquet_pixels, raster_pixels, max_missing_to_show=20):
    """
    Validate that all parquet pixels have corresponding raster values

    Parameters:
    -----------
    parquet_pixels : set
        Set of (x_coord, y_coord) from parquet files
    raster_pixels : set
        Set of (x_coord, y_coord) from raster with data
    max_missing_to_show : int
        Maximum number of missing pixels to display in detail

    Returns:
    --------
    bool: True if validation passes, False otherwise
    """
    print("\n" + "="*70)
    print("VALIDATION RESULTS")
    print("="*70)

    # Find missing pixels (in parquet but not in raster)
    missing_pixels = parquet_pixels - raster_pixels

    # Find extra pixels (in raster but not in parquet)
    extra_pixels = raster_pixels - parquet_pixels

    print(f"\nTotal unique pixels in parquet files: {len(parquet_pixels)}")
    print(f"Total pixels with data in raster: {len(raster_pixels)}")
    print(f"Missing pixels (in parquet but not in raster): {len(missing_pixels)}")
    print(f"Extra pixels (in raster but not in parquet): {len(extra_pixels)}")

    # Determine validation status
    validation_passed = len(missing_pixels) == 0

    if validation_passed:
        print("\n" + "✓"*70)
        print("VALIDATION PASSED!")
        print("✓"*70)
        print("\nAll pixels from the parquet files have values in the raster.")

        if len(extra_pixels) > 0:
            print(f"\nNote: The raster contains {len(extra_pixels)} extra pixels not in the parquet files.")
            print("This is unexpected and may indicate an issue with coordinate precision or processing.")
    else:
        print("\n" + "✗"*70)
        print("VALIDATION FAILED!")
        print("✗"*70)
        print(f"\n{len(missing_pixels)} pixels from the parquet files are missing from the raster.")

        # Show sample of missing pixels
        if len(missing_pixels) > 0:
            print(f"\nShowing first {min(len(missing_pixels), max_missing_to_show)} missing pixels:")
            for i, (x, y) in enumerate(sorted(missing_pixels)[:max_missing_to_show]):
                print(f"  {i+1}. x={x}, y={y}")

            if len(missing_pixels) > max_missing_to_show:
                print(f"  ... and {len(missing_pixels) - max_missing_to_show} more")

    if len(extra_pixels) > 0 and not validation_passed:
        print(f"\nShowing first {min(len(extra_pixels), max_missing_to_show)} extra pixels:")
        for i, (x, y) in enumerate(sorted(extra_pixels)[:max_missing_to_show]):
            print(f"  {i+1}. x={x}, y={y}")

        if len(extra_pixels) > max_missing_to_show:
            print(f"  ... and {len(extra_pixels) - max_missing_to_show} more")

    return validation_passed

def run_validation(input_dir, raster_file):
    """
    Main validation function
    """
    print("Starting validation...")
    print(f"Input directory: {input_dir}")
    print(f"Raster file: {raster_file}")
    print("="*70)

    # Check that files exist
    if not os.path.exists(input_dir):
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    if not os.path.exists(raster_file):
        raise FileNotFoundError(f"Raster file not found: {raster_file}")

    # Step 1: Extract unique pixels from parquet files
    print("\nStep 1: Extracting unique pixels from parquet files...")
    parquet_pixels = extract_unique_pixels_from_parquet(input_dir)

    # Step 2: Extract pixels with data from raster
    print("\nStep 2: Extracting pixels with data from raster...")
    raster_pixels, raster_stats = extract_pixels_from_raster(raster_file)

    # Step 3: Validate completeness
    print("\nStep 3: Validating completeness...")
    validation_passed = validate_completeness(parquet_pixels, raster_pixels)

    return validation_passed

if __name__ == "__main__":
    try:
        validation_passed = run_validation(input_directory, output_raster_file)

        # Exit with appropriate code
        exit(0 if validation_passed else 1)

    except Exception as e:
        print(f"\nError during validation: {str(e)}")
        import traceback
        traceback.print_exc()
        exit(2)
