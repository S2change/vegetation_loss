"""
Script to merge bimonthly CCD vector outputs (.gpkg) into consolidated files for specific intervals.

The script:
- Searches for all CCD output GeoPackages in a given base folder.
- Groups them by the bimonthly date interval indicated in the filename (YYYYMMDD_to_YYYYMMDD).
- Filters only the intervals of interest (2023-2024, bimonthly).
- Merges the GeoPackages for each interval into a single GeoPackage.
- Saves the merged files into a dedicated output directory.
"""

import os
import pandas as pd
import geopandas as gpd
from glob import glob
from collections import defaultdict
import re

# ==========================
# ====== CONFIGURATION =====
# ==========================

BASE_FOLDER = r"C:\Users\Public\Documents\outputs_ROI\tabular"
MERGED_OUTPUT_DIR = os.path.join(BASE_FOLDER, "merged_polygons")
os.makedirs(MERGED_OUTPUT_DIR, exist_ok=True)

START_YEAR = 2023
END_YEAR = 2024

# ==========================
# ====== FUNCTIONS ========
# ==========================
def find_all_geopackages(base_folder: str) -> list:
    """
    Finds all GeoPackage (.gpkg) files recursively in the base folder under the processed_outputs/vectors subfolders.

    Args:
        base_folder (str): Root folder containing CCD outputs.

    Returns:
        List[str]: List of full paths to the GeoPackage files.
    """
    all_files = glob(os.path.join(base_folder, "*", "processed_outputs", "vectors", "*.gpkg"))
    if not all_files:
        print("• No GeoPackages found!")
        exit()
    print(f"• {len(all_files)} GeoPackages found")
    return all_files


def group_shapefiles_by_interval(shapefile_paths: list) -> dict:
    """
    Groups shapefiles by their bimonthly date interval extracted from the filename (pattern YYYYMMDD_to_YYYYMMDD).

    Args:
        shapefile_paths (list): List of GeoPackage file paths.

    Returns:
        dict: Dictionary with date interval as key and list of shapefile paths as value.
    """
    groups = defaultdict(list)
    for shp_path in shapefile_paths:
        filename = os.path.basename(shp_path)
        date_key = None

        # Try to find the interval in the filename
        parts = filename.split("_")
        for part in parts:
            if "_to_" in part:
                date_key = part
                break

        # If not found, try regex
        if not date_key:
            joined = "_".join(parts)
            match = re.search(r"(\d{8}_to_\d{8})", joined)
            if match:
                date_key = match.group(1)

        if not date_key:
            print(f"Date interval not found in filename: {filename}")
            continue

        groups[date_key].append(shp_path)

    print(f"\n• {len(groups)} GeoPackage groups found: {list(groups.keys())}")
    return groups


def generate_target_intervals(start_year: int, end_year: int) -> list:
    """
    Generates a list of bimonthly date intervals in the format YYYYMMDD_to_YYYYMMDD for given years.

    Args:
        start_year (int): Start year.
        end_year (int): End year.

    Returns:
        list[str]: List of bimonthly interval strings.
    """
    intervals = []
    for year in range(start_year, end_year + 1):
        for start_month in range(1, 12, 2):  # Jan, Mar, May, etc.
            end_month = start_month + 1
            start_date = pd.Timestamp(year=year, month=start_month, day=1)
            end_date = pd.Timestamp(year=year, month=end_month, day=1) + pd.offsets.MonthEnd(0)
            interval_str = f"{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}"
            intervals.append(interval_str)
            

    return intervals


def filter_groups_by_intervals(groups: dict, target_intervals: list) -> dict:
    """
    Filters the shapefile groups to keep only those matching the target intervals.

    Args:
        groups (dict): Dictionary of all shapefile groups.
        target_intervals (list): List of desired interval strings.

    Returns:
        dict: Filtered dictionary of shapefile groups.
    """
    filtered = {k: v for k, v in groups.items() if k in target_intervals}
    if not filtered:
        print("• No shapefile groups found for the specified intervals.")
        exit()
    print(f"\n• {len(filtered)} groups filtered: {list(filtered.keys())}")
    return filtered


def merge_shapefiles_for_intervals(filtered_groups: dict, output_dir: str):
    """
    Merges the GeoPackages for each interval and saves them to the output directory.

    Args:
        filtered_groups (dict): Dictionary of filtered shapefile groups by interval.
        output_dir (str): Path to the folder where merged files will be saved.

    Outputs:
        - GeoPackage (.gpkg) file for each bimonthly interval containing all polygons from the national tiles.
    """
    for date_key, gpkg_list in filtered_groups.items():
        print(f"\n• Merging {len(gpkg_list)} GeoPackages for interval {date_key}")
        
        # Print all files that will be merged
        print("  - Files to merge:")
        for gpkg_file in gpkg_list:
            print(f"    • {gpkg_file}")
        
        try:
            gdf_merged = gpd.GeoDataFrame(pd.concat([gpd.read_file(gpkg) for gpkg in gpkg_list], ignore_index=True))
            output_path = os.path.join(output_dir, f"merged_{date_key}_tol10_05ha_polygons.gpkg")
            gdf_merged.to_file(output_path, driver="GPKG")
            print(f"• Created: {output_path}")
        except Exception as e:
            print(f"• Error merging {date_key}: {e}")

# ==========================
# ==========================
def main():
    all_shapefiles = find_all_geopackages(BASE_FOLDER)
    grouped_shapefiles = group_shapefiles_by_interval(all_shapefiles)
    target_intervals = generate_target_intervals(START_YEAR, END_YEAR)
    filtered_groups = filter_groups_by_intervals(grouped_shapefiles, target_intervals)
    merge_shapefiles_for_intervals(filtered_groups, MERGED_OUTPUT_DIR)


if __name__ == "__main__":
    main()
