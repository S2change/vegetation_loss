"""
Script to split 16-band GeoTIFFs into separate before/after 6-band GeoTIFFs.
Preserves all geospatial metadata (CRS, transform, bounds, etc.)

Input: 16-band TIF files where:
  - Bands 1-6 (indices 0-5): Pre-change image (B2, B3, B4, B8, B11, B12)
  - Band 7 (index 6): Unused
  - Bands 8-13 (indices 7-12): Post-change image (B2, B3, B4, B8, B11, B12)
  - Bands 14-16 (indices 13-15): Unused

Output: Two 6-band GeoTIFFs per input file (before and after)
"""

import os
import glob
import numpy as np
import rasterio  #Always import torch before rasterio in your scripts.
import sys
import AAA_Configs

# OUTPUT BANDS 0-255
MAX_VALUE=254 # maximum value to scale to, to avoid large uniform areas of 255 which can cause issues for some models
NODATA_VALUE=255 # value to use for nodata pixels in the output uint8 images, which will be set to 255 to be consistent with the original BACDM data and to avoid issues with some models which can arise when there are large uniform areas of 255 (if we used 0 instead, we would have large uniform areas of 0 which can cause similar issues for some models)
# INPUT BANDS
NODATA_INPUT=65535

# ============================================================================
# CONFIGURATION - CHANGE THESE PATHS
# ============================================================================

# Input directory containing 16-band TIF files
INPUT_DIR = AAA_Configs.Input_dir #"./chips_test/TQG_burn_area"

# Output directories for before and after images
# where before and after 6-channel geo-referenced tifs are stored
#Test_im_pathA = r".\test_data\before_TQG_burn_area_minmax_resample"
# Test_im_pathB = r".\test_data\after_TQG_burn_area_minmax_resample"
OUTPUT_BEFORE_DIR = AAA_Configs.Test_im_pathA
OUTPUT_AFTER_DIR = AAA_Configs.Test_im_pathB

# Band indices to extract (reversed order for output to match BACDM data)
BEFORE_BANDS = [6, 5, 4, 3, 2, 1]
AFTER_BANDS = [13, 12, 11, 10, 9, 8]
# not used: BANDS_TO_RESAMPLE 
BANDS_TO_RESAMPLE = [0, 1] # indices of bands to apply smoothing to , i.e. B12 and B11

# Scaling method: "fixed", "minmax", "minmax_perband", or "percentile_perband"
# - "fixed": Scale from 0-10000 range to 0-255
# - "minmax": Scale from actual min-max values across all bands to 0-255
# - "minmax_perband": Scale each band independently using its own min-max to 0-255
# - "percentile_perband": Clip each band at 1.5% and 98.5% percentiles, then scale to 0-255
SCALING_METHOD = "percentile_perband" # "minmax_perband"  # Options: "fixed", "minmax", "minmax_perband", or "percentile_perband"

# Percentile clipping thresholds (only used when SCALING_METHOD is "percentile_perband")
PERCENTILE_LOW = 1.5   # Lower percentile for clipping
PERCENTILE_HIGH = 98.5 # Upper percentile for clipping

# noise scale, to be added after scaling to uint8, to avoid large uniform areas of 255 which can cause issues for some models
SCALE = 5 # Adjust this value to increase/decrease noise intensity (e.g., 1 for very slight noise, 5 for more noticeable noise)

# ============================================================================
# PROCESSING
# ============================================================================

def scale_to_uint8(data, nodata_mask, scaling_method):
    """
    Scale data to uint8 range (0-255).

    Args:
        data: Input array to scale (shape: bands, height, width)
        nodata_mask: Boolean mask indicating nodata pixels
        scaling_method: "fixed", "minmax", "minmax_perband", or "percentile_perband"

    Returns:
        Scaled uint8 array
    """
    if scaling_method == "fixed":
        # Fixed scaling: 0-10000 range to 0-255
        scaled = (data / 10000.0 * MAX_VALUE).clip(0, MAX_VALUE).astype('uint8')

    elif scaling_method == "minmax":
        # Min-max scaling: normalize actual data range to 0-255 (all bands together)
        # Only consider non-nodata pixels for min/max calculation
        valid_data = data[~nodata_mask]
        if valid_data.size > 0:
            data_min = valid_data.min()
            data_max = valid_data.max()
            if data_max > data_min:
                # Scale to 0-255 range
                scaled = ((data - data_min) / (data_max - data_min) * MAX_VALUE).clip(0, MAX_VALUE).astype('uint8')
            else:
                # All values are the same, set to middle of range
                scaled = (data * 0 + 127).astype('uint8')
        else:
            # All pixels are nodata
            scaled = data.astype('uint8')

    elif scaling_method == "minmax_perband":
        # Per-band min-max scaling: normalize each band independently
        scaled = data.copy()
        num_bands = data.shape[0]

        for band_idx in range(num_bands):
            band_data = data[band_idx]
            band_nodata_mask = nodata_mask[band_idx]

            # Get valid (non-nodata) pixels for this band
            valid_pixels = band_data[~band_nodata_mask]

            if valid_pixels.size > 0:
                band_min = valid_pixels.min()
                band_max = valid_pixels.max()

                if band_max > band_min:
                    # Scale this band to 0-255 range
                    scaled[band_idx] = ((band_data - band_min) / (band_max - band_min) * MAX_VALUE).clip(0, 255).astype('uint8')
                else:
                    # All values in this band are the same
                    scaled[band_idx] = (band_data * 0 + 127).astype('uint8')
            else:
                # All pixels in this band are nodata
                scaled[band_idx] = band_data.astype('uint8')

        scaled = scaled.astype('uint8')

    elif scaling_method == "percentile_perband":
        # Per-band percentile clipping and scaling
        scaled = data.copy()
        num_bands = data.shape[0]

        for band_idx in range(num_bands):
            band_data = data[band_idx]
            band_nodata_mask = nodata_mask[band_idx]

            # Get valid (non-nodata) pixels for this band
            valid_pixels = band_data[~band_nodata_mask]

            if valid_pixels.size > 0:
                # Calculate percentiles from valid pixels only
                p_low = np.percentile(valid_pixels, PERCENTILE_LOW)
                p_high = np.percentile(valid_pixels, PERCENTILE_HIGH)

                if p_high > p_low:
                    # Clip values to percentile range
                    clipped = band_data.clip(p_low, p_high)
                    # Scale to 0-MAX_VALUE range
                    scaled[band_idx] = ((clipped - p_low) / (p_high - p_low) * MAX_VALUE).clip(0, MAX_VALUE).astype('uint8')
                else:
                    # All values in percentile range are the same
                    scaled[band_idx] = (band_data * 0 + 127).astype('uint8')
            else:
                # All pixels in this band are nodata
                scaled[band_idx] = band_data.astype('uint8')

        scaled = scaled.astype('uint8')

    else:
        raise ValueError(f"Unknown scaling method: {scaling_method}. Use 'fixed', 'minmax', 'minmax_perband', or 'percentile_perband'.")

    # Set nodata pixels to 255
    scaled[nodata_mask] = NODATA_VALUE
    return scaled
    #return resample_smooth_bands(scaled, bands_to_resample=BANDS_TO_RESAMPLE) # apply smoothing to the 10m bands (B2 and B3) using the 20m neighborhood, to reduce noise and make them more similar to the original BACDM data which has some inherent smoothing due to resampling from 20m to 10m resolution 

# NOT USED IN FINAL VERSION, but can be used to add noise to the output images after scaling to uint8, to avoid large uniform areas of 255 which can cause issues for some models
def add_noise_to_array(image_np,loc=0, scale=SCALE):
    # Assume image_np is your (6, 256, 256) uint8 array
    # 1. Convert to a signed type to allow for negative noise and avoid overflow
    noisy_image = image_np.astype(np.int16)
    # 2. Generate random noise
    # 'loc' is the mean, 'scale' is the standard deviation (intensity of noise)
    noise = np.random.normal(loc=loc, scale=scale, size=image_np.shape).astype(np.int16)
    # 3. Add noise and clip to valid uint8 range [0, MAX_VALUE]
    noisy_image = np.clip(noisy_image + noise, 0, MAX_VALUE)
    # 4. Convert back to uint8
    return(noisy_image.astype(np.uint8))

# NOT USED IN FINAL VERSION, but can be used to apply smoothing to the 10m bands (B2 and B3) using the 20m neighborhood, to reduce noise and make them more similar to the original BACDM data which has some inherent smoothing due to resampling from 20m to 10m resolution
def resample_smooth_bands(image_stack, bands_to_resample):
    """
    Args:
        image_stack (np.ndarray): Array of shape (6, 256, 256)
        bands_to_resample (list): List of band indices (e.g., [0,1])
    Returns:
        np.ndarray: Modified (6, 256, 256) array
    """
    # Create a copy to avoid modifying the original input array in-place
    output_stack = image_stack.copy()
    
    for b in bands_to_resample:
        # 1. Downsample (Extract original 20m pixels)
        # Slicing the specific band 'b'
        band_20m = output_stack[b, ::2, ::2]
        # 2. Smooth Upsample (Interpolate back to 10m)
        # zoom factor is 2 for height and width axes
        band_10m_smooth = zoom(band_20m, 2, order=3)
        # 3. Post-processing (Clip and cast)
        # Ensure values stay within uint8 bounds
        band_10m_smooth = np.clip(band_10m_smooth, 0, MAX_VALUE).astype(np.uint8)
        # 4. Replace the band in the output stack
        output_stack[b] = band_10m_smooth
    return output_stack
# Example usage:
# new_image = resample_smooth_bands(original_stack, [4, 5])


def split_tif(input_path, output_before_path, output_after_path):
    """
    Split a 16-band TIF into two 6-band TIFs (before and after).

    Args:
        input_path: Path to input 16-band TIF
        output_before_path: Path for output before TIF (bands 1-6)
        output_after_path: Path for output after TIF (bands 8-13)
    """
    with rasterio.open(input_path) as src:
        # Read metadata
        meta = src.meta.copy()

        # Verify we have enough bands
        if src.count < 13:
            raise ValueError(f"Input file has only {src.count} bands, expected at least 13")

        # Update metadata for 6-band output with uint8 dtype and nodata value
        meta.update(count=6, dtype='uint8', nodata=NODATA_VALUE)

        # Read before bands and convert to uint8
        before_data = src.read(BEFORE_BANDS)
        # Create mask for nodata pixels (value 65535)
        before_nodata_mask = before_data == NODATA_INPUT
        # Scale data to 0-255 uint8 range
        before_data = scale_to_uint8(before_data, before_nodata_mask, SCALING_METHOD)
        
        # Read after bands and convert to uint8
        after_data = src.read(AFTER_BANDS)
        # Create mask for nodata pixels (value 65535)
        after_nodata_mask = after_data == NODATA_INPUT
        # Scale data to 0-255 uint8 range
        after_data = scale_to_uint8(after_data, after_nodata_mask, SCALING_METHOD)

        # Write before image
        with rasterio.open(output_before_path, 'w', **meta) as dst:
            dst.write(before_data)
            # Copy band descriptions if they exist
            for i, band_idx in enumerate(BEFORE_BANDS, start=1):
                desc = src.descriptions[band_idx - 1]
                if desc:
                    dst.set_band_description(i, desc)

        # Write after image
        with rasterio.open(output_after_path, 'w', **meta) as dst:
            dst.write(after_data)
            # Copy band descriptions if they exist
            for i, band_idx in enumerate(AFTER_BANDS, start=1):
                desc = src.descriptions[band_idx - 1]
                if desc:
                    dst.set_band_description(i, desc)

        print(f"  ✓ Created before: {os.path.basename(output_before_path)}")
        print(f"  ✓ Created after:  {os.path.basename(output_after_path)}")


def main():
    """Process all TIF files in the input directory."""

    # Validate scaling method
    if SCALING_METHOD not in ["fixed", "minmax", "minmax_perband", "percentile_perband"]:
        print(f"Error: Invalid SCALING_METHOD '{SCALING_METHOD}'. Must be 'fixed', 'minmax', 'minmax_perband', or 'percentile_perband'.")
        return

    print(f"Scaling method: {SCALING_METHOD}")
    if SCALING_METHOD == "fixed":
        print("  - Using fixed scaling: 0-10000 → 0-255")
    elif SCALING_METHOD == "minmax":
        print("  - Using min-max normalization across all bands: [min, max] → 0-255")
    elif SCALING_METHOD == "minmax_perband":
        print("  - Using per-band min-max normalization: each band scaled independently to 0-255")
    else:
        print(f"  - Using per-band percentile clipping: clip at {PERCENTILE_LOW}% and {PERCENTILE_HIGH}%, then scale to 0-255")
    print()

    # Create output directories if they don't exist
    os.makedirs(OUTPUT_BEFORE_DIR, exist_ok=True)
    os.makedirs(OUTPUT_AFTER_DIR, exist_ok=True)

    # Find all TIF files in input directory
    tif_pattern = os.path.join(INPUT_DIR, "*.tif")
    tif_files = glob.glob(tif_pattern)

    if not tif_files:
        print(f"No TIF files found in {INPUT_DIR}")
        return

    print(f"Found {len(tif_files)} TIF file(s) to process\n")

    # Process each file
    success_count = 0
    error_count = 0

    for tif_path in tif_files:
        filename = os.path.basename(tif_path)
        print(f"Processing: {filename}")

        # Create output paths
        output_before = os.path.join(OUTPUT_BEFORE_DIR, filename)
        output_after = os.path.join(OUTPUT_AFTER_DIR, filename)

        try:
            split_tif(tif_path, output_before, output_after)
            success_count += 1
        except Exception as e:
            print(f"  ✗ Error: {e}")
            error_count += 1

        print()

    # Summary
    print("="*70)
    print(f"Processing complete!")
    print(f"  Success: {success_count} files")
    print(f"  Errors:  {error_count} files")
    print(f"\nOutput directories:")
    print(f"  Before: {OUTPUT_BEFORE_DIR}")
    print(f"  After:  {OUTPUT_AFTER_DIR}")


if __name__ == "__main__":
    main()
