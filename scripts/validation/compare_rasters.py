# Dominic Welsh

"""
===============================================================================
Description:
    This script compares two GeoTIFF raster files to determine if they are
    identical or different. It checks both metadata (dimensions, CRS,
    transform, nodata values) and pixel values.

Usage:
    python compare_rasters.py

Outputs:
    - Console output showing comparison results
    - Optional difference raster if rasters have matching dimensions
===============================================================================
"""

import numpy as np
import rasterio
from pathlib import Path
import sys


def compare_raster_metadata(raster1_path, raster2_path):
    """
    Compare metadata of two rasters.

    Parameters
    ----------
    raster1_path : str
        Path to the first raster file.
    raster2_path : str
        Path to the second raster file.

    Returns
    -------
    dict
        Dictionary containing metadata comparison results.
    """
    with rasterio.open(raster1_path) as src1, rasterio.open(raster2_path) as src2:
        # Handle NaN comparison for nodata values
        nodata1 = src1.nodata
        nodata2 = src2.nodata

        # Check if both are NaN or if they're equal
        if nodata1 is None and nodata2 is None:
            nodata_match = True
        elif nodata1 is not None and nodata2 is not None:
            # Both are NaN
            if np.isnan(nodata1) and np.isnan(nodata2):
                nodata_match = True
            else:
                nodata_match = nodata1 == nodata2
        else:
            nodata_match = False

        metadata_comparison = {
            "dimensions_match": (src1.height == src2.height) and (src1.width == src2.width),
            "dimensions_1": (src1.height, src1.width),
            "dimensions_2": (src2.height, src2.width),
            "crs_match": src1.crs == src2.crs,
            "crs_1": str(src1.crs),
            "crs_2": str(src2.crs),
            "transform_match": src1.transform == src2.transform,
            "transform_1": src1.transform,
            "transform_2": src2.transform,
            "nodata_match": nodata_match,
            "nodata_1": nodata1,
            "nodata_2": nodata2,
            "band_count_match": src1.count == src2.count,
            "band_count_1": src1.count,
            "band_count_2": src2.count,
            "dtype_match": src1.dtypes[0] == src2.dtypes[0],
            "dtype_1": src1.dtypes[0],
            "dtype_2": src2.dtypes[0],
        }

    return metadata_comparison


def compare_raster_values(raster1_path, raster2_path, tolerance=1e-6):
    """
    Compare pixel values of two rasters.

    Parameters
    ----------
    raster1_path : str
        Path to the first raster file.
    raster2_path : str
        Path to the second raster file.
    tolerance : float, optional
        Tolerance for floating-point comparison (default: 1e-6).

    Returns
    -------
    dict
        Dictionary containing value comparison results.
    """
    with rasterio.open(raster1_path) as src1, rasterio.open(raster2_path) as src2:
        # Check if dimensions match
        if src1.height != src2.height or src1.width != src2.width:
            return {
                "can_compare": False,
                "reason": "Dimensions don't match"
            }

        # Read all bands
        data1 = src1.read()
        data2 = src2.read()

        # Handle nodata values
        nodata1 = src1.nodata
        nodata2 = src2.nodata

        # Create masks for valid data (True = valid data, False = nodata)
        if nodata1 is not None:
            if np.isnan(nodata1):
                mask1 = ~np.isnan(data1)
            else:
                mask1 = data1 != nodata1
        else:
            mask1 = ~np.isnan(data1)

        if nodata2 is not None:
            if np.isnan(nodata2):
                mask2 = ~np.isnan(data2)
            else:
                mask2 = data2 != nodata2
        else:
            mask2 = ~np.isnan(data2)

        # Check if masks match
        masks_match = np.array_equal(mask1, mask2)

        # Compare values where both have valid data
        valid_both = mask1 & mask2

        if np.any(valid_both):
            values1_valid = data1[valid_both]
            values2_valid = data2[valid_both]

            # Check if values are close (accounting for floating point precision)
            if np.issubdtype(data1.dtype, np.floating) or np.issubdtype(data2.dtype, np.floating):
                values_match = np.allclose(values1_valid, values2_valid, rtol=tolerance, atol=tolerance)
                max_diff = np.max(np.abs(values1_valid - values2_valid))
                mean_diff = np.mean(np.abs(values1_valid - values2_valid))
            else:
                values_match = np.array_equal(values1_valid, values2_valid)
                max_diff = np.max(np.abs(values1_valid.astype(float) - values2_valid.astype(float)))
                mean_diff = np.mean(np.abs(values1_valid.astype(float) - values2_valid.astype(float)))

            diff_count = np.sum(~np.isclose(values1_valid, values2_valid, rtol=tolerance, atol=tolerance))
            diff_percentage = (diff_count / len(values1_valid)) * 100
        else:
            values_match = True
            max_diff = 0
            mean_diff = 0
            diff_count = 0
            diff_percentage = 0

        return {
            "can_compare": True,
            "masks_match": masks_match,
            "values_match": values_match,
            "total_pixels": data1.size,
            "valid_pixels_1": np.sum(mask1),
            "valid_pixels_2": np.sum(mask2),
            "valid_both": np.sum(valid_both),
            "different_pixels": diff_count,
            "difference_percentage": diff_percentage,
            "max_difference": max_diff,
            "mean_difference": mean_diff,
        }


def generate_difference_raster(raster1_path, raster2_path, output_path):
    """
    Generate a raster showing the differences between two rasters.

    Parameters
    ----------
    raster1_path : str
        Path to the first raster file.
    raster2_path : str
        Path to the second raster file.
    output_path : str
        Path for the output difference raster.
    """
    with rasterio.open(raster1_path) as src1, rasterio.open(raster2_path) as src2:
        # Check if dimensions match
        if src1.height != src2.height or src1.width != src2.width:
            print("[ERROR] Cannot generate difference raster: dimensions don't match")
            return

        # Read data
        data1 = src1.read(1)
        data2 = src2.read(1)

        # Calculate difference
        diff = np.abs(data1.astype(float) - data2.astype(float))

        # Create output raster
        profile = src1.profile.copy()
        profile.update(dtype=rasterio.float32, nodata=np.nan)

        with rasterio.open(output_path, 'w', **profile) as dst:
            dst.write(diff.astype(rasterio.float32), 1)

        print(f"[INFO] Difference raster saved to: {output_path}")


def print_comparison_report(raster1_path, raster2_path, metadata_results, value_results):
    """
    Print a formatted comparison report.

    Parameters
    ----------
    raster1_path : str
        Path to the first raster file.
    raster2_path : str
        Path to the second raster file.
    metadata_results : dict
        Metadata comparison results.
    value_results : dict
        Value comparison results.
    """
    print("\n" + "=" * 80)
    print("RASTER COMPARISON REPORT")
    print("=" * 80)
    print(f"\nRaster 1: {raster1_path}")
    print(f"Raster 2: {raster2_path}")

    print("\n" + "-" * 80)
    print("METADATA COMPARISON")
    print("-" * 80)

    # Dimensions
    status = "✓" if metadata_results["dimensions_match"] else "✗"
    print(f"{status} Dimensions: {metadata_results['dimensions_1']} vs {metadata_results['dimensions_2']}")

    # CRS
    status = "✓" if metadata_results["crs_match"] else "✗"
    print(f"{status} CRS: {metadata_results['crs_1']} vs {metadata_results['crs_2']}")

    # Transform
    status = "✓" if metadata_results["transform_match"] else "✗"
    print(f"{status} Transform: {metadata_results['transform_match']}")
    if not metadata_results["transform_match"]:
        print(f"    Raster 1: {metadata_results['transform_1']}")
        print(f"    Raster 2: {metadata_results['transform_2']}")

    # NoData
    status = "✓" if metadata_results["nodata_match"] else "✗"
    print(f"{status} NoData: {metadata_results['nodata_1']} vs {metadata_results['nodata_2']}")

    # Band count
    status = "✓" if metadata_results["band_count_match"] else "✗"
    print(f"{status} Band count: {metadata_results['band_count_1']} vs {metadata_results['band_count_2']}")

    # Data type
    status = "✓" if metadata_results["dtype_match"] else "✗"
    print(f"{status} Data type: {metadata_results['dtype_1']} vs {metadata_results['dtype_2']}")

    print("\n" + "-" * 80)
    print("VALUE COMPARISON")
    print("-" * 80)

    if not value_results["can_compare"]:
        print(f"✗ Cannot compare values: {value_results['reason']}")
    else:
        # Masks
        status = "✓" if value_results["masks_match"] else "✗"
        print(f"{status} Valid data masks match: {value_results['masks_match']}")
        print(f"    Valid pixels in Raster 1: {value_results['valid_pixels_1']:,}")
        print(f"    Valid pixels in Raster 2: {value_results['valid_pixels_2']:,}")
        print(f"    Valid in both: {value_results['valid_both']:,}")

        # Values
        status = "✓" if value_results["values_match"] else "✗"
        print(f"\n{status} Pixel values match: {value_results['values_match']}")

        if not value_results["values_match"]:
            print(f"    Different pixels: {value_results['different_pixels']:,} ({value_results['difference_percentage']:.2f}%)")
            print(f"    Max difference: {value_results['max_difference']:.6f}")
            print(f"    Mean difference: {value_results['mean_difference']:.6f}")

    print("\n" + "=" * 80)
    print("FINAL RESULT")
    print("=" * 80)

    # Determine if rasters are identical
    metadata_identical = all([
        metadata_results["dimensions_match"],
        metadata_results["crs_match"],
        metadata_results["transform_match"],
        metadata_results["nodata_match"],
        metadata_results["band_count_match"],
        metadata_results["dtype_match"]
    ])

    values_identical = (value_results["can_compare"] and
                       value_results["masks_match"] and
                       value_results["values_match"])

    if metadata_identical and values_identical:
        print("✓ The rasters are IDENTICAL")
    else:
        print("✗ The rasters are DIFFERENT")
        if not metadata_identical:
            print("    - Metadata differs")
        if not values_identical:
            print("    - Values differ")

    print("=" * 80 + "\n")


def compare_rasters(raster1_path, raster2_path, generate_diff=False, diff_output_path=None):
    """
    Main function to compare two rasters.

    Parameters
    ----------
    raster1_path : str
        Path to the first raster file.
    raster2_path : str
        Path to the second raster file.
    generate_diff : bool, optional
        Whether to generate a difference raster (default: False).
    diff_output_path : str, optional
        Path for the difference raster output.

    Returns
    -------
    bool
        True if rasters are identical, False otherwise.
    """
    # Check if files exist
    if not Path(raster1_path).exists():
        print(f"[ERROR] File not found: {raster1_path}")
        return False

    if not Path(raster2_path).exists():
        print(f"[ERROR] File not found: {raster2_path}")
        return False

    # Compare metadata
    print("[INFO] Comparing metadata...")
    metadata_results = compare_raster_metadata(raster1_path, raster2_path)

    # Compare values
    print("[INFO] Comparing pixel values...")
    value_results = compare_raster_values(raster1_path, raster2_path)

    # Print report
    print_comparison_report(raster1_path, raster2_path, metadata_results, value_results)

    # Generate difference raster if requested
    if generate_diff and value_results["can_compare"]:
        if diff_output_path is None:
            diff_output_path = str(Path(raster1_path).parent / "difference.tif")
        print(f"[INFO] Generating difference raster...")
        generate_difference_raster(raster1_path, raster2_path, diff_output_path)

    # Return True if identical
    metadata_identical = all([
        metadata_results["dimensions_match"],
        metadata_results["crs_match"],
        metadata_results["transform_match"],
        metadata_results["nodata_match"],
        metadata_results["band_count_match"],
        metadata_results["dtype_match"]
    ])

    values_identical = (value_results["can_compare"] and
                       value_results["masks_match"] and
                       value_results["values_match"])

    return metadata_identical and values_identical


if __name__ == "__main__":
    # Example usage - modify these paths to your rasters
    raster1 = r"/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/personal_tests/08_loop_check_with_optimized_20180101_to_20180228.tif"
    raster2 = r"/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/personal_tests/09_optimized_test_20180101_to_20180228.tif"

    # To use from command line, uncomment the following:
    # if len(sys.argv) < 3:
    #     print("Usage: python compare_rasters.py <raster1> <raster2> [--generate-diff] [--output <diff_path>]")
    #     sys.exit(1)
    #
    # raster1 = sys.argv[1]
    # raster2 = sys.argv[2]
    # generate_diff = "--generate-diff" in sys.argv
    # diff_output = None
    # if "--output" in sys.argv:
    #     idx = sys.argv.index("--output")
    #     if idx + 1 < len(sys.argv):
    #         diff_output = sys.argv[idx + 1]

    # Compare the rasters
    are_identical = compare_rasters(
        raster1,
        raster2,
        generate_diff=False,  # Set to True to generate difference raster
        diff_output_path=None
    )

    # Exit with appropriate code
    sys.exit(0 if are_identical else 1)

