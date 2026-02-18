import os
import shutil
import rasterio

"""
This script identifies TIF files with different spatial bounds/transforms from a reference file
and copies them to a separate directory. This helps clean up datasets before processing with
scripts like tif_to_hdf5.py that require all files to have identical spatial properties.

The script checks two folders (4bands and 2bands) and identifies files that:
1. Don't match the reference file in their own folder
2. Don't match between the two folders (4bands vs 2bands for same timestamp)

Misaligned files are copied (not moved) to preserve originals.
"""

# ============================================================================
# CONFIGURATION
# ============================================================================

# Base folder containing the TIF files
folder_path = r'C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\temp\test_tif_to_hdf5'

# Subfolders with the two band collections
folder_path_4bands = os.path.join(folder_path, '4bands')
folder_path_2bands = os.path.join(folder_path, '2bands')

# Output directories for misaligned files
misaligned_dir_4bands = os.path.join(folder_path, 'misaligned_4bands')
misaligned_dir_2bands = os.path.join(folder_path, 'misaligned_2bands')
file_mismatch_dir_4bands = os.path.join(folder_path, 'file_mismatch', '4bands')
file_mismatch_dir_2bands = os.path.join(folder_path, 'file_mismatch', '2bands')

# ============================================================================
# MAIN SCRIPT
# ============================================================================

def get_spatial_info(filepath):
    """Extract bounds, transform, and shape from a TIF file."""
    with rasterio.open(filepath) as src:
        return {
            'bounds': src.bounds,
            'transform': src.transform,
            'shape': src.shape
        }


def check_alignment(files_list, folder_path):
    """
    Check which files in a folder are misaligned from the reference (first file).

    Returns:
        tuple: (reference_info, list of misaligned filenames)
    """
    if not files_list:
        return None, []

    # Use first file as reference
    reference_path = os.path.join(folder_path, files_list[0])
    reference_info = get_spatial_info(reference_path)

    misaligned = []

    for filename in files_list[1:]:  # Skip first file since it's the reference
        filepath = os.path.join(folder_path, filename)
        info = get_spatial_info(filepath)

        if (info['bounds'] != reference_info['bounds'] or
            info['transform'] != reference_info['transform'] or
            info['shape'] != reference_info['shape']):
            misaligned.append(filename)

    return reference_info, misaligned


def main():
    # Get list of TIF files from 4bands folder (use as master list)
    print("Scanning for TIF files...")
    files_4bands = sorted([f for f in os.listdir(folder_path_4bands) if f.endswith('.tif')])
    print(f"Found {len(files_4bands)} TIF files in 4bands folder")

    # Check if corresponding files exist in 2bands folder
    files_2bands = sorted([f for f in os.listdir(folder_path_2bands) if f.endswith('.tif')])
    print(f"Found {len(files_2bands)} TIF files in 2bands folder")

    # Get common files between both folders
    common_files = sorted(list(set(files_4bands) & set(files_2bands)))
    print(f"Found {len(common_files)} common files between both folders")

    if not common_files:
        print("ERROR: No common files found between folders!")
        return

    # Check alignment in 4bands folder
    print("\nChecking 4-band files for alignment...")
    reference_4bands, misaligned_4bands_internal = check_alignment(common_files, folder_path_4bands)
    print(f"  Found {len(misaligned_4bands_internal)} misaligned files within 4bands folder")

    # Check alignment in 2bands folder
    print("\nChecking 2-band files for alignment...")
    reference_2bands, misaligned_2bands_internal = check_alignment(common_files, folder_path_2bands)
    print(f"  Found {len(misaligned_2bands_internal)} misaligned files within 2bands folder")

    # Check for mismatches between 4bands and 2bands
    print("\nChecking for mismatches between 4bands and 2bands folders...")
    mismatched_between_folders = []

    for filename in common_files:
        info_4bands = get_spatial_info(os.path.join(folder_path_4bands, filename))
        info_2bands = get_spatial_info(os.path.join(folder_path_2bands, filename))

        if (info_4bands['bounds'] != info_2bands['bounds'] or
            info_4bands['transform'] != info_2bands['transform'] or
            info_4bands['shape'] != info_2bands['shape']):
            mismatched_between_folders.append(filename)

    print(f"  Found {len(mismatched_between_folders)} files that don't match between folders")

    # Separate misaligned files by category
    misaligned_4bands_only = set(misaligned_4bands_internal) - set(mismatched_between_folders)
    misaligned_2bands_only = set(misaligned_2bands_internal) - set(mismatched_between_folders)
    mismatched_set = set(mismatched_between_folders)

    all_misaligned = misaligned_4bands_only | misaligned_2bands_only | mismatched_set

    print(f"\n{'='*60}")
    print(f"SUMMARY:")
    print(f"  4bands internally misaligned: {len(misaligned_4bands_only)}")
    print(f"  2bands internally misaligned: {len(misaligned_2bands_only)}")
    print(f"  Mismatched between folders: {len(mismatched_set)}")
    print(f"  TOTAL MISALIGNED FILES: {len(all_misaligned)}")
    print(f"{'='*60}")

    if not all_misaligned:
        print("No misaligned files found! All files have matching spatial properties.")
        return

    # Show first 10 misaligned files by category
    if misaligned_4bands_only:
        print("\n4bands internally misaligned (showing first 5):")
        for filename in sorted(misaligned_4bands_only)[:5]:
            print(f"  - {filename}")
        if len(misaligned_4bands_only) > 5:
            print(f"  ... and {len(misaligned_4bands_only) - 5} more")

    if misaligned_2bands_only:
        print("\n2bands internally misaligned (showing first 5):")
        for filename in sorted(misaligned_2bands_only)[:5]:
            print(f"  - {filename}")
        if len(misaligned_2bands_only) > 5:
            print(f"  ... and {len(misaligned_2bands_only) - 5} more")

    if mismatched_set:
        print("\nMismatched between folders (showing first 5):")
        for filename in sorted(mismatched_set)[:5]:
            print(f"  - {filename}")
        if len(mismatched_set) > 5:
            print(f"  ... and {len(mismatched_set) - 5} more")

    # Ask for confirmation before copying
    print(f"\nFiles will be COPIED (not moved) to:")
    print(f"  4bands internally misaligned: {misaligned_dir_4bands}")
    print(f"  2bands internally misaligned: {misaligned_dir_2bands}")
    print(f"  Mismatched between folders: {os.path.join(folder_path, 'file_mismatch')}")
    response = input("\nProceed with copying? (yes/no): ").strip().lower()

    if response != 'yes':
        print("Operation cancelled.")
        return

    # Create output directories
    os.makedirs(misaligned_dir_4bands, exist_ok=True)
    os.makedirs(misaligned_dir_2bands, exist_ok=True)
    os.makedirs(file_mismatch_dir_4bands, exist_ok=True)
    os.makedirs(file_mismatch_dir_2bands, exist_ok=True)

    # Copy misaligned files
    print("\nCopying misaligned files...")
    copied_count = 0

    # Copy 4bands internally misaligned files
    for filename in misaligned_4bands_only:
        src = os.path.join(folder_path_4bands, filename)
        dst = os.path.join(misaligned_dir_4bands, filename)
        if os.path.exists(src):
            shutil.copy2(src, dst)
            copied_count += 1

    # Copy 2bands internally misaligned files
    for filename in misaligned_2bands_only:
        src = os.path.join(folder_path_2bands, filename)
        dst = os.path.join(misaligned_dir_2bands, filename)
        if os.path.exists(src):
            shutil.copy2(src, dst)
            copied_count += 1

    # Copy mismatched files (both 4bands and 2bands versions)
    for filename in mismatched_set:
        # Copy from 4bands folder to file_mismatch/4bands
        src_4bands = os.path.join(folder_path_4bands, filename)
        dst_4bands = os.path.join(file_mismatch_dir_4bands, filename)
        if os.path.exists(src_4bands):
            shutil.copy2(src_4bands, dst_4bands)
            copied_count += 1

        # Copy from 2bands folder to file_mismatch/2bands
        src_2bands = os.path.join(folder_path_2bands, filename)
        dst_2bands = os.path.join(file_mismatch_dir_2bands, filename)
        if os.path.exists(src_2bands):
            shutil.copy2(src_2bands, dst_2bands)
            copied_count += 1

    print(f"\nDone! Copied {copied_count} files")


if __name__ == "__main__":
    main()
