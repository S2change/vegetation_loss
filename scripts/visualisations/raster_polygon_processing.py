"""
Combined Raster and Polygon Processing Script

This script processes parquet files containing change detection results:
1. Creates multiple raster files (one per 2-month period) using ccd_break_filter_to_raster_multiple_outputs.py
2. Converts each raster file to vector polygons using raster_to_polygons.py
3. Organizes outputs into separate folders for rasters and polygons
4. Can loop through multiple tile directories

"""

import os
import glob
from pathlib import Path
from ccd_break_filter_to_raster_multiple_outputs import process_directory_to_multiple_geotiffs
from raster_to_polygons import raster_to_polygons

# ============================================================================
# CONFIGURATION VARIABLES - UPDATE THESE FOR YOUR PROJECT
# ============================================================================

# OPTION 1: Process a single directory
# This variable is not used if PARENT_DIRECTORY is not None
INPUT_DIRECTORY = "/Users/domwelsh/green_ds/Thesis/T29TNE_0999"

# OPTION 2: Process all subdirectories within a parent directory
# Set this to a parent directory path to process all subdirectories within it
# Set to None to use INPUT_DIRECTORY instead
PARENT_DIRECTORY = None

# Base output directory (raster and polygon subdirectories will be created here)
# If OUTPUT_BASE_DIR = PARENT_DIRECTORY, results will be saved in each tile's subdirectory
OUTPUT_BASE_DIR = "/Users/domwelsh/green_ds/Thesis/T29TNE_0999/2019_2020_processed_outputs"

# Date range for processing (BOTH must be provided)
SEARCH_START = "2019-01-01"  # Start date for filtering break dates ("YYYY-MM-DD" format)
SEARCH_END = "2020-12-31"    # End date for filtering break dates ("YYYY-MM-DD" format)

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

# Formatted variables for output names
MIN_AREA_HECTARES_STR = str(MIN_AREA_HECTARES).replace(".", "")

# ============================================================================
# DIRECTORY DISCOVERY FUNCTIONS
# ============================================================================

def get_directories_to_process():
    """
    Determine which directories to process based on configuration.
    
    Returns:
        list: List of directory paths to process
    """
    if PARENT_DIRECTORY is not None:
        # Process all subdirectories in parent directory
        if not os.path.exists(PARENT_DIRECTORY):
            print(f"Error: Parent directory does not exist: {PARENT_DIRECTORY}")
            return []
        
        # Find all subdirectories
        subdirs = []
        for item in os.listdir(PARENT_DIRECTORY):
            item_path = os.path.join(PARENT_DIRECTORY, item)
            if os.path.isdir(item_path):
                # Check if directory contains parquet files
                parquet_files = glob.glob(os.path.join(item_path, "*.parquet"))
                if parquet_files:
                    subdirs.append(item_path)
                else:
                    print(f"Skipping {item_path} - no parquet files found")
        
        if not subdirs:
            print(f"No directories with parquet files found in: {PARENT_DIRECTORY}")
            return []
        
        print(f"Found {len(subdirs)} directories to process in parent directory:")
        for subdir in subdirs:
            print(f"  - {subdir}")
        
        return subdirs
    
    else:
        # Process single directory
        if not os.path.exists(INPUT_DIRECTORY):
            print(f"Error: Input directory does not exist: {INPUT_DIRECTORY}")
            return []
        
        # Check if directory contains parquet files
        parquet_files = glob.glob(os.path.join(INPUT_DIRECTORY, "*.parquet"))
        if not parquet_files:
            print(f"No parquet files found in: {INPUT_DIRECTORY}")
            return []
        
        return [INPUT_DIRECTORY]

# ============================================================================
# PROCESSING FUNCTIONS
# ============================================================================

def setup_output_directories(base_dir, tile_name):
    """
    Create output directory structure for a specific tile.
    
    Args:
        base_dir: Base output directory
        tile_name: Name of the tile/directory being processed
        
    Returns:
        tuple: (raster_dir, polygon_dir)
    """
    # Create tile-specific directory with parameter-based subdirectories
    tile_dir = os.path.join(base_dir, tile_name)
    raster_dir = os.path.join(tile_dir, f"{tile_name}_tol{DATE_RANGE_DAYS}_{MIN_AREA_HECTARES_STR}ha_rasters")
    polygon_dir = os.path.join(tile_dir, f"{tile_name}_tol{DATE_RANGE_DAYS}_{MIN_AREA_HECTARES_STR}ha_polygons")
    
    # Create directories if they don't exist
    Path(raster_dir).mkdir(parents=True, exist_ok=True)
    Path(polygon_dir).mkdir(parents=True, exist_ok=True)
    
    print(f"Output directories created:")
    print(f"  Tile directory: {tile_dir}")
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
        
    Returns:
        tuple: (successful_conversions, total_files)
    """
    # Find all TIFF files in raster directory
    tiff_pattern = os.path.join(raster_dir, "*.tif")
    tiff_files = glob.glob(tiff_pattern)
    
    if not tiff_files:
        print(f"No TIFF files found in {raster_dir}")
        return 0, 0
    
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
            polygon_file = os.path.join(polygon_dir, f"{base_name}_tol{DATE_RANGE_DAYS}_{MIN_AREA_HECTARES_STR}ha_polygons{file_extension}")
            
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
    
    return successful_conversions, len(tiff_files)

def process_single_directory(input_dir, output_base_dir):
    """
    Process a single directory containing parquet files.
    
    Args:
        input_dir: Directory containing parquet files
        output_base_dir: Base directory for outputs
        
    Returns:
        dict: Processing results
    """
    tile_name = os.path.basename(input_dir)
    
    print(f"\n{'='*80}")
    print(f"PROCESSING DIRECTORY: {tile_name}")
    print(f"{'='*80}")
    print(f"Input path: {input_dir}")
    
    # Setup output directories for this tile
    raster_dir, polygon_dir = setup_output_directories(output_base_dir, tile_name)
    
    results = {
        'tile_name': tile_name,
        'input_dir': input_dir,
        'raster_dir': raster_dir,
        'polygon_dir': polygon_dir,
        'raster_success': False,
        'polygon_success': False,
        'polygons_converted': 0,
        'total_rasters': 0,
        'error': None
    }
    
    # Step 1: Process parquet files to create rasters
    print(f"\n{'='*60}")
    print("STEP 1: PROCESSING PARQUET FILES TO RASTERS")
    print(f"{'='*60}")
    
    try:
        process_directory_to_multiple_geotiffs(
            input_dir=input_dir,
            output_base_path=raster_dir,
            target_crs=TARGET_CRS,
            search_start=SEARCH_START,
            search_end=SEARCH_END,
            boundary_shapefile=BOUNDARY_SHAPEFILE,
            qgis_style_file=CREATE_QGIS_STYLE_FILES,
            save_vector_files=SAVE_VECTOR_POINT_FILES
        )
        
        results['raster_success'] = True
        print(f"\nRaster processing completed successfully for {tile_name}!")
        
    except Exception as e:
        error_msg = f"Error in raster processing for {tile_name}: {str(e)}"
        print(error_msg)
        results['error'] = error_msg
        return results
    
    # Step 2: Convert rasters to polygons
    print(f"\n{'='*60}")
    print("STEP 2: CONVERTING RASTERS TO POLYGONS")
    print(f"{'='*60}")
    
    try:
        polygons_converted, total_rasters = process_rasters_to_polygons(
            raster_dir=raster_dir,
            polygon_dir=polygon_dir,
            date_range_days=DATE_RANGE_DAYS,
            min_area_ha=MIN_AREA_HECTARES,
            nodata_value=NODATA_VALUE,
            output_format=POLYGON_FORMAT
        )
        
        results['polygon_success'] = True
        results['polygons_converted'] = polygons_converted
        results['total_rasters'] = total_rasters
        print(f"\nPolygon processing completed successfully for {tile_name}!")
        
    except Exception as e:
        error_msg = f"Error in polygon processing for {tile_name}: {str(e)}"
        print(error_msg)
        results['error'] = error_msg
        return results
    
    print(f"\n{'='*60}")
    print(f"COMPLETED PROCESSING: {tile_name}")
    print(f"{'='*60}")
    
    return results

def main():
    """Main processing function."""
    
    print("="*80)
    print("COMBINED RASTER AND POLYGON PROCESSING")
    print("="*80)
    
    # Validate date inputs
    if SEARCH_START is None or SEARCH_END is None:
        print("Error: Both SEARCH_START and SEARCH_END must be provided")
        return
    
    # Get directories to process
    directories_to_process = get_directories_to_process()
    
    if not directories_to_process:
        print("No directories to process. Exiting.")
        return
    
    # Process each directory
    all_results = []
    successful_dirs = 0
    failed_dirs = 0
    
    for i, input_dir in enumerate(directories_to_process, 1):
        print(f"\n{'='*80}")
        print(f"PROCESSING DIRECTORY {i}/{len(directories_to_process)}")
        print(f"{'='*80}")
        
        try:
            results = process_single_directory(input_dir, OUTPUT_BASE_DIR)
            all_results.append(results)
            
            if results.get('raster_success') and results.get('polygon_success'):
                successful_dirs += 1
            else:
                failed_dirs += 1
                
        except Exception as e:
            print(f"Critical error processing {input_dir}: {str(e)}")
            failed_dirs += 1
            continue
    
    # Final summary
    print(f"\n{'='*80}")
    print("PROCESSING SUMMARY")
    print(f"{'='*80}")
    print(f"Total directories processed: {len(directories_to_process)}")
    print(f"Successful: {successful_dirs}")
    print(f"Failed: {failed_dirs}")
    print(f"Date range: {SEARCH_START} to {SEARCH_END}")
    print(f"Coordinate system: {TARGET_CRS}")
    print(f"Polygon grouping: {DATE_RANGE_DAYS} days")
    print(f"Minimum polygon area: {MIN_AREA_HECTARES} hectares")
    print(f"Output format: {POLYGON_FORMAT}")
    
    # Detailed results for each directory
    if len(all_results) > 1:
        print(f"\n{'='*60}")
        print("DETAILED RESULTS BY DIRECTORY")
        print(f"{'='*60}")
        
        for result in all_results:
            tile_name = result['tile_name']
            status = "SUCCESS" if (result.get('raster_success') and result.get('polygon_success')) else "FAILED"
            
            print(f"\n{tile_name}: {status}")
            if result.get('error'):
                print(f"  Error: {result['error']}")
            else:
                print(f"  Rasters: {'✓' if result.get('raster_success') else '✗'}")
                print(f"  Polygons: {'✓' if result.get('polygon_success') else '✗'} ({result.get('polygons_converted', 0)}/{result.get('total_rasters', 0)} converted)")
                print(f"  Output: {result['raster_dir']}")
                print(f"          {result['polygon_dir']}")
    
    print(f"{'='*80}")

if __name__ == "__main__":
    main()