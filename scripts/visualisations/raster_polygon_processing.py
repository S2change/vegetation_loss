"""
Combined Raster and Polygon Processing Script

This script processes parquet files containing change detection results:
1. Creates multiple raster files (one per 2-month period) using ccd_break_filter_to_raster_multiple_outputs.py
2. Converts each raster file to vector polygons using raster_to_polygons.py
3. Organizes outputs into separate folders for rasters and polygons
"""

import os
import glob
from pathlib import Path

# Import functions from the other scripts
from ccd_break_filter_to_raster_multiple_outputs import process_directory_to_multiple_geotiffs
from raster_to_polygons import raster_to_polygons

# ============================================================================
# CONFIGURATION VARIABLES - UPDATE THESE FOR YOUR PROJECT
# ============================================================================

# Input directory containing parquet files
INPUT_DIRECTORY = "/Users/domwelsh/green_ds/Thesis/T29SMD_0999"

# Base output directory (raster and polygon subdirectories will be created here)
OUTPUT_BASE_DIR = "/Users/domwelsh/green_ds/Thesis/T29SMD_0999/processed_outputs"

# Date range for processing (BOTH must be provided)
SEARCH_START = "2023-01-01"  # Start date for filtering break dates ("YYYY-MM-DD" format)
SEARCH_END = "2024-12-31"    # End date for filtering break dates ("YYYY-MM-DD" format)

# Coordinate Reference System
TARGET_CRS = "EPSG:32629"  # Use "EPSG:4326" for lat/lon, "EPSG:32629" for UTM Zone 29N

# Boundary shapefile filtering (set to None to disable)
BOUNDARY_SHAPEFILE = None  # Path to shapefile for spatial boundary filtering

# Raster processing options
CREATE_QGIS_STYLE_FILES = False   # Set to True if .qml style files should be created
SAVE_VECTOR_POINT_FILES = False  # Set to True if you want vector point files for verification

# Polygon processing options
DATE_RANGE_DAYS = 30        # Number of days to group adjacent pixels in polygons (0 = no grouping)
MIN_AREA_HECTARES = 0.5     # Minimum polygon area in hectares
NODATA_VALUE = -9999        # Nodata value to exclude from polygons
POLYGON_FORMAT = "shp"     # Output format: "shp", "gpkg", or "geojson"

# ============================================================================
# MAIN PROCESSING FUNCTIONS
# ============================================================================

def setup_output_directories(base_dir):
    """
    Create output directory structure.
    
    Returns:
        tuple: (raster_dir, polygon_dir)
    """
    raster_dir = os.path.join(base_dir, "rasters")
    polygon_dir = os.path.join(base_dir, "polygons")
    
    # Create directories if they don't exist
    Path(raster_dir).mkdir(parents=True, exist_ok=True)
    Path(polygon_dir).mkdir(parents=True, exist_ok=True)
    
    print(f"Output directories created:")
    print(f"  Rasters: {raster_dir}")
    print(f"  Polygons: {polygon_dir}")
    
    return raster_dir, polygon_dir

def process_rasters_to_polygons(raster_dir, polygon_dir, date_range_days, 
                               min_area_ha, nodata_value, output_format):
    """
    Convert all raster files in raster_dir to polygon files in polygon_dir.
    
    Args:
        raster_dir: Directory containing raster TIFF files
        polygon_dir: Directory to save polygon files
        date_range_days: Days to group pixels by date
        min_area_ha: Minimum polygon area in hectares
        nodata_value: Nodata value to exclude
        output_format: Output format ("shp", "gpkg", or "geojson")
    """
    # Find all TIFF files in raster directory
    tiff_pattern = os.path.join(raster_dir, "*.tif")
    tiff_files = glob.glob(tiff_pattern)
    
    if not tiff_files:
        print(f"No TIFF files found in {raster_dir}")
        return
    
    print(f"\nFound {len(tiff_files)} raster files to convert to polygons")
    
    # Determine file extension based on format
    ext_map = {
        "shp": ".shp",
        "gpkg": ".gpkg", 
        "geojson": ".geojson"
    }
    file_extension = ext_map.get(output_format.lower(), ".shp")
    
    successful_conversions = 0
    
    # Process each raster file
    for i, tiff_file in enumerate(tiff_files, 1):
        try:
            # Get base filename without extension
            base_name = os.path.splitext(os.path.basename(tiff_file))[0]
            
            # Create output polygon file path
            polygon_file = os.path.join(polygon_dir, f"{base_name}_polygons{file_extension}")
            
            print(f"\n{'='*60}")
            print(f"CONVERTING RASTER {i}/{len(tiff_files)}: {base_name}")
            print(f"{'='*60}")
            print(f"Input: {tiff_file}")
            print(f"Output: {polygon_file}")
            
            # Convert raster to polygons
            raster_to_polygons(
                input_raster=tiff_file,
                output_vector=polygon_file,
                date_range_days=date_range_days,
                min_area_ha=min_area_ha,
                nodata_value=nodata_value
            )
            
            successful_conversions += 1
            print(f"Successfully converted: {base_name}")
            
        except Exception as e:
            print(f"Error converting {tiff_file}: {str(e)}")
            continue
    
    print(f"\n{'='*60}")
    print(f"POLYGON CONVERSION COMPLETE")
    print(f"Successfully converted {successful_conversions}/{len(tiff_files)} raster files")
    print(f"{'='*60}")

def main():
    """Main processing function."""
    
    print("="*80)
    print("COMBINED RASTER AND POLYGON PROCESSING")
    print("="*80)
    
    # Validate inputs
    if not os.path.exists(INPUT_DIRECTORY):
        print(f"Error: Input directory does not exist: {INPUT_DIRECTORY}")
        return
    
    if SEARCH_START is None or SEARCH_END is None:
        print("Error: Both SEARCH_START and SEARCH_END must be provided")
        return
    
    # Setup output directories
    raster_dir, polygon_dir = setup_output_directories(OUTPUT_BASE_DIR)
    
    # Step 1: Process parquet files to create rasters
    print(f"\n{'='*80}")
    print("STEP 1: PROCESSING PARQUET FILES TO RASTERS")
    print(f"{'='*80}")
    
    try:
        process_directory_to_multiple_geotiffs(
            input_dir=INPUT_DIRECTORY,
            output_base_path=raster_dir,
            target_crs=TARGET_CRS,
            search_start=SEARCH_START,
            search_end=SEARCH_END,
            boundary_shapefile=BOUNDARY_SHAPEFILE,
            qgis_style_file=CREATE_QGIS_STYLE_FILES,
            save_vector_files=SAVE_VECTOR_POINT_FILES
        )
        
        print(f"\nRaster processing completed successfully!")
        
    except Exception as e:
        print(f"Error in raster processing: {str(e)}")
        return
    
    # Step 2: Convert rasters to polygons
    print(f"\n{'='*80}")
    print("STEP 2: CONVERTING RASTERS TO POLYGONS")
    print(f"{'='*80}")
    
    try:
        process_rasters_to_polygons(
            raster_dir=raster_dir,
            polygon_dir=polygon_dir,
            date_range_days=DATE_RANGE_DAYS,
            min_area_ha=MIN_AREA_HECTARES,
            nodata_value=NODATA_VALUE,
            output_format=POLYGON_FORMAT
        )
        
        print(f"\nPolygon processing completed successfully!")
        
    except Exception as e:
        print(f"Error in polygon processing: {str(e)}")
        return
    
    # Final summary
    print(f"\n{'='*80}")
    print("PROCESSING COMPLETE")
    print(f"{'='*80}")
    print(f"Input directory: {INPUT_DIRECTORY}")
    print(f"Date range: {SEARCH_START} to {SEARCH_END}")
    print(f"Raster files saved to: {raster_dir}")
    print(f"Polygon files saved to: {polygon_dir}")
    print(f"Coordinate system: {TARGET_CRS}")
    print(f"Polygon grouping: {DATE_RANGE_DAYS} days")
    print(f"Minimum polygon area: {MIN_AREA_HECTARES} hectares")
    print(f"Output format: {POLYGON_FORMAT}")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()