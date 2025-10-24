"""
PURPOSE:
This script processes parquet files containing change detection results from satellite imagery analysis.
It filters and aggregates pixel-level change detection data, validates breaks using NDVI loss calculations,
converts the data to a multi-band raster format, and creates visualization files for use in GIS software like QGIS.

MAIN FUNCTIONALITY:
- Reads multiple parquet files containing change detection segments with break points
- Filters data by date range (optional) - only breaks within the date range are considered
- Processes each pixel to identify the most recent valid vegetation loss break:
    * Iterates through segments in reverse chronological order (newest to oldest)
    * Validates breaks using NDVI loss calculation between consecutive segments
    * Classifies each pixel as: valid break (is_break=1), no break (is_break=0), or uncertain break (is_break=-1)
- Converts filtered point data to a 3-band georeferenced raster (GeoTIFF)
- Creates QGIS style files for visualization of Band 1 (break dates)
- Optionally saves filtered points as a vector file

INPUTS:
- input_directory: Directory containing parquet files with columns:
  * x_coord, y_coord: UTM coordinates (EPSG:32629 assumed)
  * tBreak: Break date as milliseconds since Unix epoch (UTC)
  * tEnd: Segment end date as milliseconds since Unix epoch (UTC)
  * nirEnd, redEnd: NIR and Red band values at segment end (for NDVI calculation)
  * Other columns used by ndvi_loss_calculation function
- date_ranges: List of tuples with (start_date, end_date) for filtering (format: 'YYYY-MM-DD')
  * A separate raster is created for each date range
- boundary_shapefile: Optional shapefile path for spatial filtering (not currently implemented)

OUTPUTS:
- Multi-band GeoTIFF raster file (.tif):
  * Band 1: last_tBreak (break dates in YYYYMMDD format)
    * Valid/uncertain breaks: YYYYMMDD integer value
    * Pixels with no breaks: 0
    * Pixels with no data: -9999 (NoData)
  * Band 2: is_break (break classification)
    * 1: valid break (confirmed vegetation loss via NDVI)
    * 0: no break detected
    * -1: uncertain break (tBreak != tEnd, needs validation)
    * -99: NoData
  * Band 3: ndvi_last_segment (NDVI value of last segment)
    * Float value for pixels with breaks
    * NaN for pixels without breaks or NoData
  * Resolution: 10m x 10m pixels
  * Coordinate system: UTM (EPSG:32629) or optionally reprojected
- QGIS style file (.qml): Color-coded visualization of Band 1 by year with gradient by day-of-year
- Optional vector file (.gpkg): Point locations with break dates and attributes for verification
"""

import pandas as pd
import geopandas as gpd
import os
import glob
from pathlib import Path
import rasterio
from rasterio.transform import from_origin
from rasterio.warp import calculate_default_transform, reproject, Resampling
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import colorsys
import time
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
   sys.path.append(module_path)
from ccd_results_utils.segment_identification import ndvi_loss_calculation

## SCRIPT CONFIGS ##
##################################

# Set input directory and output files
input_directory = "/Users/domwelsh/green_ds/Thesis/BDR_300_artigo" # UPDATE
output_raster_file = "/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/personal_tests/05_loop_debugging_style_file.tif" # UPDATE

# Vector file is not set up
output_vector_file = None # Add path if vector file is wanted, to check which points were processed to make the raster

# List of date ranges to filter for, in format (start_date, end_date)
# Use "YYYY-MM-DD" for date values
# Raster will be created for each date range pair
date_ranges = [("2018-01-01", "2021-12-31"),
            #    ("2018-01-01", "2018-02-28"),
              ]

# Boundary shapefile filtering (set to None to disable)
# boundary filtering is not set up yet
boundary_shapefile = None  # Path to shapefile for spatial boundary filtering

qgis_style_file = True  # Set to True if a .qml style file should be created

# Timer for testing
set_timer = True

##################################

def calculate_ndvi(input_row):
    """
    Calculate NDVI (Normalized Difference Vegetation Index) from NIR and Red band values.

    Parameters:
    -----------
    input_row : pandas.Series or dict-like
        Row containing 'nirEnd' and 'redEnd' values

    Returns:
    --------
    float : NDVI value calculated as (NIR - Red) / (NIR + Red)
    """
    ndvi = (input_row["nirEnd"] - input_row["redEnd"]) / (input_row["nirEnd"] + input_row["redEnd"])
    return ndvi

def date_conversion_ms(start_date, end_date):
    """
    Convert start and end dates to milliseconds since Unix epoch.

    Parameters:
    -----------
    start_date : str, datetime, or None
        Start date for filtering
    end_date : str, datetime, or None
        End date for filtering

    Returns:
    --------
    tuple : (start_date_ms, end_date_ms) as milliseconds or None
    """
    start_date_ms = None
    end_date_ms = None

    if start_date is not None:
        start_date_dt = pd.to_datetime(start_date)
        start_date_ms = int(start_date_dt.timestamp() * 1000)

    if end_date is not None:
        end_date_dt = pd.to_datetime(end_date)
        end_date_ms = int(end_date_dt.timestamp() * 1000)

    return start_date_ms, end_date_ms

def load_boundary_shapefile(shapefile_path, source_crs="EPSG:32629"):
    """
    Load boundary shapefile and ensure it's in the same CRS as the data
    
    Parameters:
    -----------
    shapefile_path : str
        Path to the boundary shapefile
    source_crs : str
        CRS of the input data (default: EPSG:32629)
        
    Returns:
    --------
    geopandas.GeoDataFrame
        Boundary geometry in the same CRS as the input data
    """
    try:
        boundary_gdf = gpd.read_file(shapefile_path)
        
        # Reproject to match source CRS if necessary
        if boundary_gdf.crs.to_string() != source_crs:
            print(f"Reprojecting boundary from {boundary_gdf.crs} to {source_crs}")
            boundary_gdf = boundary_gdf.to_crs(source_crs)
        
        # Dissolve all geometries into a single boundary if multiple features exist
        boundary_dissolved = boundary_gdf.dissolve().reset_index(drop=True)
        
        print(f"Loaded boundary shapefile: {shapefile_path}")
        print(f"Boundary CRS: {boundary_dissolved.crs}")
        print(f"Number of boundary features: {len(boundary_gdf)} (dissolved to 1)")
        
        return boundary_dissolved
        
    except Exception as e:
        raise Exception(f"Error loading boundary shapefile {shapefile_path}: {str(e)}")
    
def date_filtering(date_value_ms, search_start_ms=None, search_end_ms=None):
    """
    Check if a date value (in milliseconds) falls within the specified date range.

    Parameters:
    -----------
    date_value_ms : int
        The date to check (as milliseconds since Unix epoch)
    search_start_ms : int or None
        Start date for filtering (as milliseconds since Unix epoch)
    search_end_ms : int or None
        End date for filtering (as milliseconds since Unix epoch)

    Returns:
    --------
    bool : True if date passes the filter, False otherwise
    """
    if search_start_ms is None and search_end_ms is None:
        return True

    if date_value_ms is None or pd.isna(date_value_ms):
        return False

    if search_start_ms is not None:
        if date_value_ms < search_start_ms:
            return False

    if search_end_ms is not None:
        if date_value_ms > search_end_ms:
            return False

    return True
    
def process_parquet_file_optimized(file_path, search_start_ms=None, search_end_ms=None, boundary_gdf=None, source_crs="EPSG:32629"):
    """
    Process a single parquet file and return pixel results by iterating in reverse chronological order.
    For each pixel, identifies the most recent break that passes date filtering and NDVI loss validation.

    Algorithm:
    - Iterates through segments in reverse order (newest to oldest)
    - Groups segments by pixel (x_coord, y_coord)
    - For each pixel:
        * If the last segment has tBreak != tEnd, returns is_break=-1 (uncertain break)
        * Otherwise, compares consecutive segments using NDVI loss calculation
        * Returns is_break=1 (valid break) if NDVI loss is confirmed
        * Returns is_break=0 (no break) if no valid break is found
    - Only processes segments that pass date filtering

    Parameters:
    -----------
    file_path : str
        Path to the parquet file
    search_start_ms : int or None
        Start date for filtering (as milliseconds since Unix epoch)
    search_end_ms : int or None
        End date for filtering (as milliseconds since Unix epoch)
    boundary_gdf : geopandas.GeoDataFrame, optional
        Boundary geometry for spatial filtering (currently not implemented)
    source_crs : str
        CRS of the coordinates

    Returns:
    --------
    list : List of tuples (x_coord, y_coord, is_break, tBreak_used, ndvi_last_segment)
        - is_break: 1 (valid break), 0 (no break), or -1 (uncertain break)
        - tBreak_used: tEnd value for breaks (ms since Unix epoch), None for no breaks
        - ndvi_last_segment: NDVI value of the last segment (NaN for no breaks)
    """
    df = pd.read_parquet(file_path)

    # Track which pixels we've fully processed
    processed_pixels = set()
    # Store segments we're currently collecting for a pixel
    current_pixel = None
    current_segments = []
    results = []

    # Iterate in reverse (newest to oldest)
    for i in range(len(df) - 1, -1, -1):
        row = df.iloc[i]
        x, y = row["x_coord"], row["y_coord"]
        pixel_key = (x, y)

        # Skip if we've already fully processed this pixel
        if pixel_key in processed_pixels:
            continue

        # If we've moved to a different pixel, process the previous one
        if current_pixel is not None and pixel_key != current_pixel:
            # appended results are the same no matter number of segments
            results.append((current_pixel[0], current_pixel[1], 0, None, np.nan))
            processed_pixels.add(current_pixel)

            # Start collecting for new pixel
            current_pixel = pixel_key
            current_segments = [row]
        elif current_pixel is None:
            # First pixel encountered
            current_pixel = pixel_key
            current_segments = [row]
        else:
            # Same pixel, add segment
            current_segments.append(row)

        # Date filtering
        date_check = date_filtering(row["tBreak"], search_start_ms, search_end_ms)
        if date_check == False:
            continue

        # Check if we can determine the result early
        if len(current_segments) == 1:
            last_seg = current_segments[0]
            last_tBreak = last_seg["tBreak"]
            last_tEnd = last_seg["tEnd"]

            # If last segment has tBreak != tEnd, don't need to process more segments
            if pd.notna(last_tBreak) and pd.notna(last_tEnd) and last_tBreak != last_tEnd:
                ndvi = calculate_ndvi(last_seg)
                results.append((pixel_key[0], pixel_key[1], -1, last_tEnd, ndvi))
                processed_pixels.add(pixel_key)
                current_pixel = None
                current_segments = []
                continue

        # We need at least 2 segments to check NDVI change
        if len(current_segments) >= 2:
            active_segment = current_segments[-1]
            newer_segment = current_segments[-2]

            ndvi_check = ndvi_loss_calculation(active_segment, newer_segment)
            if ndvi_check == 1:
                # Valid break
                ndvi = calculate_ndvi(active_segment)
                results.append((pixel_key[0], pixel_key[1], 1, active_segment["tEnd"], ndvi))
                processed_pixels.add(pixel_key)
                current_pixel = None
                current_segments = []

    # Process last pixel if no break was found
    if current_pixel is not None:
        results.append((current_pixel[0], current_pixel[1], 0, None, np.nan))

    return results

def process_files_chunked(input_dir, search_start=None, search_end=None, boundary_shapefile=None, source_crs="EPSG:32629"):
    """
    Generator that yields processed data from parquet files one at a time to avoid memory issues.
    Converts date filters to milliseconds once before processing begins for efficiency.

    Parameters:
    -----------
    input_dir : str
        Directory containing parquet files
    search_start : str or datetime, optional
        Start date for filtering (format: 'YYYY-MM-DD' or datetime object)
    search_end : str or datetime, optional
        End date for filtering (format: 'YYYY-MM-DD' or datetime object)
    boundary_shapefile : str, optional
        Path to shapefile for spatial boundary filtering
    source_crs : str
        CRS of the coordinates
    """
    parquet_files = glob.glob(os.path.join(input_dir, "*.parquet"))

    if not parquet_files:
        print(f"No parquet files found in {input_dir}")
        return

    print(f"Found {len(parquet_files)} parquet files to process")

    # Convert date filters to milliseconds ONCE before processing
    search_start_ms, search_end_ms = date_conversion_ms(search_start, search_end)

    # Load boundary shapefile if specified
    boundary_gdf = None
    if boundary_shapefile is not None:
        boundary_gdf = load_boundary_shapefile(boundary_shapefile, source_crs)

    # Print filtering information
    filter_info = []
    if search_start is not None or search_end is not None:
        date_info = "Date filtering: "
        if search_start is not None:
            date_info += f"from {search_start} "
        if search_end is not None:
            date_info += f"to {search_end}"
        filter_info.append(date_info)

    if boundary_shapefile is not None:
        filter_info.append(f"Spatial filtering: using boundary from {boundary_shapefile}")

    if filter_info:
        print("Filters applied:")
        for info in filter_info:
            print(f"  - {info}")
    else:
        print("No filters applied")

    for i, file_path in enumerate(parquet_files, 1):
        print(f"Processing file {i}/{len(parquet_files)}: {os.path.basename(file_path)}")
        results_list = process_parquet_file_optimized(file_path, search_start_ms, search_end_ms, boundary_gdf, source_crs)
        yield results_list

def collect_pixel_data_chunked(input_dir, search_start=None, search_end=None, boundary_shapefile=None, source_crs="EPSG:32629"):
    """
    Collect and aggregate pixel data from all parquet files into a DataFrame.

    Parameters:
    -----------
    input_dir : str
        Directory containing parquet files
    search_start : str or datetime, optional
        Start date for filtering
    search_end : str or datetime, optional
        End date for filtering
    boundary_shapefile : str, optional
        Path to shapefile for spatial boundary filtering
    source_crs : str
        CRS of the coordinates

    Returns:
    --------
    pandas.DataFrame
        DataFrame with columns: x_coord, y_coord, is_break, tBreak_used, ndvi_last_segment, tBreak_used_yyyymmdd
    """
    all_results = []

    for results_list in process_files_chunked(input_dir, search_start, search_end, boundary_shapefile, source_crs):
        all_results.extend(results_list)

    # data_dict needed to specify data type for each column, so that tBreak_used does not get converted to float64 and lose precision
    data_dict = {col: [row[i] for row in all_results] for i, col in enumerate(["x_coord", "y_coord", "is_break", 
                                                                               "tBreak_used", "ndvi_last_segment"])}

    # Create DataFrame from flattened list of tuples
    # results_df = pd.DataFrame(all_results, columns=["x_coord", "y_coord", "is_break",
    #                                                 "tBreak_used", "ndvi_last_segment"])

    results_df = pd.DataFrame(data_dict).astype({
        "x_coord": 'int64',
        "y_coord": 'int64',
        "is_break": 'int64',
        "tBreak_used": 'Int64',
        "ndvi_last_segment": 'float64'
    })

    # DEBUG: Check values before datetime conversion
    print("\n" + "="*70)
    print("DEBUG [Line 407-416]: Date conversion process")
    print("="*70)
    print(f"Total rows in results_df: {len(results_df)}")
    print(f"Rows with breaks (is_break != 0): {len(results_df[results_df['is_break'] != 0])}")
    print("\nSample tBreak_used values (ms) before conversion:")
    print(results_df[results_df['is_break'] != 0]['tBreak_used'].head(10))
    print(f"\ntBreak_used dtype before conversion: {results_df['tBreak_used'].dtype}")
    print(results_df.dtypes)

    # Convert tBreak_used from milliseconds to pandas Timestamp
    results_df["tBreak_used"] = pd.to_datetime(results_df["tBreak_used"], unit='ms', utc=True, errors='coerce').dt.tz_localize(None)

    # DEBUG: Check values after datetime conversion
    print("\nSample tBreak_used values after datetime conversion:")
    print(results_df[results_df['is_break'] != 0]['tBreak_used'].head(10))

    # Convert to YYYYMMDD integer format
    results_df["tBreak_used_yyyymmdd"] = (
        results_df["tBreak_used"].dt.strftime("%Y%m%d").fillna("0").astype(int)
    )

    # DEBUG: Check final YYYYMMDD values
    print("\nSample tBreak_used_yyyymmdd values after YYYYMMDD conversion:")
    print(results_df[results_df['is_break'] != 0]['tBreak_used_yyyymmdd'].head(10))
    print("="*70 + "\n")

    return results_df

def calculate_raster_parameters_from_pixels(results_df):
    """
    Calculate raster dimensions and resolution from results DataFrame
    with fixed 10x10 meter resolution. Assumes coordinates are pixel centers.
    Considers all pixels regardless of break status to determine the full extent.

    Parameters:
    -----------
    results_df : pandas.DataFrame
        DataFrame with columns: x_coord, y_coord, is_break, tBreak_used, ndvi_last_segment
    """
    if results_df.empty:
        raise ValueError("No coordinate data found in results DataFrame")

    # Extract all coordinates from the DataFrame
    all_x_coords = results_df['x_coord'].tolist()
    all_y_coords = results_df['y_coord'].tolist()

    min_x, max_x = min(all_x_coords), max(all_x_coords)
    min_y, max_y = min(all_y_coords), max(all_y_coords)

    # Fixed 10 meter resolution
    res_x = 10.0
    res_y = 10.0

    # Adjust bounds to account for pixel centers (extend by half pixel in each direction)
    min_x_corner = min_x - res_x / 2
    min_y_corner = min_y - res_y / 2
    max_x_corner = max_x + res_x / 2
    max_y_corner = max_y + res_y / 2

    # Calculate dimensions
    width = int(np.ceil((max_x_corner - min_x_corner) / res_x))
    height = int(np.ceil((max_y_corner - min_y_corner) / res_y))

    # Create transform (origin at top-left corner)
    transform = from_origin(min_x_corner, max_y_corner, res_x, res_y)

    return {
        'width': width,
        'height': height,
        'transform': transform,
        'resolution': (res_x, res_y),
        'bounds': (min_x_corner, min_y_corner, max_x_corner, max_y_corner)
    }

def create_raster_array_from_pixels(results_df, raster_params):
    """
    Create a 3-band raster array from results DataFrame with fixed 10m resolution in UTM.
    Assumes coordinates are pixel centers.

    Parameters:
    -----------
    results_df : pandas.DataFrame
        DataFrame with columns: x_coord, y_coord, is_break, tBreak_used, ndvi_last_segment, tBreak_used_yyyymmdd
        - is_break = 1: valid_break (confirmed vegetation loss)
        - is_break = 0: no_break (no breaks detected)
        - is_break = -1: uncertain_break (potential break but uncertain)
    raster_params : dict
        Raster parameters from calculate_raster_parameters_from_pixels

    Returns:
    --------
    numpy.ndarray
        3D array with shape (3, height, width) containing:
        - Band 1: last_tBreak (YYYYMMDD format, 0 for no break)
        - Band 2: is_break (1, 0, or -1)
        - Band 3: ndvi_last_segment (float, NaN for no data)
    """
    width = raster_params['width']
    height = raster_params['height']
    min_x, min_y, max_x, max_y = raster_params['bounds']
    res_x, res_y = raster_params['resolution']

    # Initialize 3 bands with NoData values
    # Band 1: tBreak dates (int32)
    tbreak_array = np.full((height, width), -9999, dtype=np.int32)
    # Band 2: is_break status (int8 to save memory: 1, 0, -1, or -99 for NoData)
    is_break_array = np.full((height, width), -99, dtype=np.int8)
    # Band 3: NDVI values (float32)
    ndvi_array = np.full((height, width), np.nan, dtype=np.float32)

    # Process all pixels from the DataFrame
    for _, row in results_df.iterrows():
        x_coord = row['x_coord']
        y_coord = row['y_coord']
        is_break = row['is_break']
        tBreak_yyyymmdd = row['tBreak_used_yyyymmdd']
        ndvi = row['ndvi_last_segment']

        # Calculate pixel indices
        x_idx = int(np.round((x_coord - min_x) / res_x - 0.5))
        y_idx = int(np.round((max_y - y_coord) / res_y - 0.5))

        if 0 <= x_idx < width and 0 <= y_idx < height:
            # Band 1: tBreak date (0 for no_break, YYYYMMDD for valid/uncertain breaks)
            if is_break == 0:
                tbreak_array[y_idx, x_idx] = 0
            else:
                tbreak_array[y_idx, x_idx] = tBreak_yyyymmdd

            # Band 2: is_break status
            is_break_array[y_idx, x_idx] = is_break

            # Band 3: NDVI (keep as NaN if not available)
            if not pd.isna(ndvi):
                ndvi_array[y_idx, x_idx] = ndvi

    # Stack into 3-band array
    raster_3band = np.stack([tbreak_array, is_break_array, ndvi_array])

    return raster_3band

def save_geotiff(array, output_file, raster_params, source_crs='EPSG:32629', target_crs='EPSG:32629'):
    """
    Save a 3-band numpy array as a GeoTIFF file, reprojecting to target CRS if needed.

    Parameters:
    -----------
    array : numpy.ndarray
        3D array with shape (3, height, width) containing:
        - Band 1: last_tBreak (int32, NoData=-9999)
        - Band 2: is_break (int8, NoData=-99)
        - Band 3: ndvi_last_segment (float32, NoData=NaN)
    output_file : str
        Path to output GeoTIFF file
    raster_params : dict
        Raster parameters from calculate_raster_parameters_from_pixels
    source_crs : str
        Source coordinate reference system
    target_crs : str
        Target coordinate reference system
    """

    # Define band-specific properties
    band_dtypes = [np.int32, np.int8, np.float32]
    band_nodata = [-9999, -99, np.nan]

    # If target CRS is different from source, reproject directly
    if source_crs != target_crs:
        # Create a temporary in-memory dataset first
        from rasterio.io import MemoryFile

        with MemoryFile() as memfile:
            with memfile.open(
                driver='GTiff',
                height=raster_params['height'],
                width=raster_params['width'],
                count=3,
                dtype=rasterio.float32,  # Use float32 to accommodate all bands
                crs=source_crs,
                transform=raster_params['transform'],
                nodata=np.nan
            ) as src:
                # Write all 3 bands
                for i in range(3):
                    src.write(array[i], i + 1)
                    src.set_band_description(i + 1, ['last_tBreak', 'is_break', 'ndvi_last_segment'][i])

                # Calculate reprojection parameters
                transform, width, height = calculate_default_transform(
                    src.crs, target_crs, src.width, src.height, *src.bounds)

                kwargs = src.meta.copy()
                kwargs.update({
                    'crs': target_crs,
                    'transform': transform,
                    'width': width,
                    'height': height
                })

                # Write directly to output file with reprojection
                with rasterio.open(output_file, 'w', **kwargs) as dst:
                    for i in range(1, src.count + 1):
                        dst.set_band_description(i, src.descriptions[i-1])
                        reproject(
                            source=rasterio.band(src, i),
                            destination=rasterio.band(dst, i),
                            src_transform=src.transform,
                            src_crs=src.crs,
                            dst_transform=transform,
                            dst_crs=target_crs,
                            resampling=Resampling.nearest)

    else:
        # If no reprojection needed, save each band with its appropriate dtype
        with rasterio.open(
            output_file,
            'w',
            driver='GTiff',
            height=raster_params['height'],
            width=raster_params['width'],
            count=3,
            dtype=rasterio.float32,  # Use float32 to accommodate all bands
            crs=source_crs,
            transform=raster_params['transform'],
            nodata=np.nan
        ) as dst:
            # Write all 3 bands
            for i in range(3):
                dst.write(array[i].astype(np.float32), i + 1)
                dst.set_band_description(i + 1, ['last_tBreak', 'is_break', 'ndvi_last_segment'][i])
                # Set band-specific nodata values in tags
                dst.update_tags(i + 1, nodata_value=str(band_nodata[i]))

def save_vector_points(results_df, output_file, target_crs="EPSG:32629", source_crs="EPSG:32629"):
    """
    Save all points from the results DataFrame as a vector file.

    Parameters:
    -----------
    results_df : pandas.DataFrame
        DataFrame with columns: x_coord, y_coord, is_break, tBreak_used, ndvi_last_segment, tBreak_used_yyyymmdd
    output_file : str
        Path to output vector file
    target_crs : str
        Target coordinate reference system
    source_crs : str
        Source coordinate reference system
    """
    if results_df.empty:
        print("No data to save as vector points")
        return 0

    # Create a copy for the vector output
    vector_df = results_df.copy()

    # Convert tBreak_used (Timestamp) to date string format
    vector_df['tBreak_date'] = vector_df['tBreak_used'].dt.strftime('%Y-%m-%d')

    # Create GeoDataFrame from the results
    gdf = gpd.GeoDataFrame(
        vector_df,
        geometry=gpd.points_from_xy(vector_df.x_coord, vector_df.y_coord),
        crs=source_crs
    )

    # Reproject if necessary
    if gdf.crs.to_string() != target_crs:
        gdf = gdf.to_crs(target_crs)

    # Save to file
    gdf.to_file(output_file, driver='GPKG')

    return len(gdf)


def create_qgis_style_file_from_pixels(results_df, output_style_file):
    """
    Create a QGIS .qml style file that colors pixels by year with gradient shading by day of year.
    This styles Band 1 (last_tBreak) of the multi-band raster.

    Parameters:
    -----------
    results_df : pandas.DataFrame
        DataFrame with columns: x_coord, y_coord, is_break, tBreak_used, ndvi_last_segment, tBreak_used_yyyymmdd
    output_style_file : str
        Path to output .qml style file
    """

    print("\n" + "="*70)
    print("DEBUG: Starting QGIS style file creation")
    print("="*70)

    # Debug: Check input DataFrame
    print(f"DEBUG: Total rows in results_df: {len(results_df)}")
    print(f"DEBUG: Columns in results_df: {results_df.columns.tolist()}")
    print(f"DEBUG: Sample of results_df:")
    print(results_df.head())
    print(results_df.dtypes)

    # Get all unique dates from pixels with breaks (is_break == 1 or -1)
    valid_breaks = results_df[results_df['is_break'] != 0]

    if valid_breaks.empty:
        print("No valid breaks found for styling")
        return

    # Extract unique dates (already as Timestamps)
    unique_dates = valid_breaks['tBreak_used_yyyymmdd'].dropna().unique()
    print(f"\nDEBUG: Number of unique dates found: {len(unique_dates)}")
    print(f"DEBUG: Sample unique dates (first 10): {sorted(unique_dates)[:10]}")
    print(f"DEBUG: Data type of unique_dates: {type(unique_dates[0]) if len(unique_dates) > 0 else 'N/A'}")

    valid_dates = pd.to_datetime(unique_dates, format='%Y%m%d')
    print(f"DEBUG: Successfully converted {len(valid_dates)} dates to datetime objects")
    print(f"DEBUG: Date range: {valid_dates.min()} to {valid_dates.max()}")

    # Group dates by year
    dates_by_year = {}
    for date in valid_dates:
        year = date.year
        date_int = int(date.strftime('%Y%m%d'))
        if year not in dates_by_year:
            dates_by_year[year] = []
        dates_by_year[year].append(date_int)

    print(f"\nDEBUG: Dates grouped by year:")
    for year in sorted(dates_by_year.keys()):
        print(f"  {year}: {len(dates_by_year[year])} dates (range: {min(dates_by_year[year])} to {max(dates_by_year[year])})")

    # Sort years and create color map
    years = sorted(dates_by_year.keys())
    cmap = plt.get_cmap('tab20', len(years))

    # Create QML content
    qml_content = '''<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="3.22.0" minScale="0" maxScale="1e+08" styleCategories="AllStyleCategories">
  <pipe>
    <rasterrenderer opacity="1" type="paletted" band="1">
      <rasterTransparency/>
      <colorPalette>
'''

    # Add color entries for each date, grouped by year with gradient
    for i, year in enumerate(years):
        # Get base color for this year
        base_rgb = cmap(i)[:3]  # RGB values in 0-1 range

        # Convert to HSV for easier manipulation
        h, s, v = colorsys.rgb_to_hsv(*base_rgb)

        # Get unique dates for this year and sort them
        year_dates = sorted(set(dates_by_year[year]))

        for j, date_value in enumerate(year_dates):
            # Ensure date_value is an integer
            date_value = int(date_value)

            # Extract day of year (1-365/366)
            date_obj = datetime.strptime(str(date_value), '%Y%m%d')
            day_of_year = date_obj.timetuple().tm_yday

            # Calculate position in year (0 to 1)
            # Account for leap years
            days_in_year = 366 if date_obj.year % 4 == 0 and (date_obj.year % 100 != 0 or date_obj.year % 400 == 0) else 365
            position = (day_of_year - 1) / (days_in_year - 1)

            # Adjust value (brightness) and saturation based on position
            # Early in year: lighter (higher value, lower saturation)
            # Late in year: darker (lower value, higher saturation)
            new_v = 0.9 - (position * 0.4)  # Goes from 0.9 to 0.5
            new_s = s * (0.5 + position * 0.5)  # Goes from 50% to 100% of original saturation

            # Convert back to RGB
            new_rgb = colorsys.hsv_to_rgb(h, new_s, new_v)
            rgb = [int(c * 255) for c in new_rgb]
            color_hex = '#{:02x}{:02x}{:02x}'.format(rgb[0], rgb[1], rgb[2])

            # Format label to show month-day
            label = date_obj.strftime('%Y-%m-%d')
            qml_content += f'        <paletteEntry value="{date_value}" color="{color_hex}" label="{label}"/>\n'

    # Add entries for no break pixels (value = 0) and nodata
    qml_content += '''        <paletteEntry value="0" color="#808080" label="No Break (Pixels with no detected breaks)"/>
        <paletteEntry value="-9999" color="#000000" label="No Data" alpha="0"/>
      </colorPalette>
    </rasterrenderer>
  </pipe>
</qgis>'''
    # Save style file
    with open(output_style_file, 'w') as f:
        f.write(qml_content)

    print(f"\nQGIS style file saved to: {output_style_file}")
    print(f"Years in data: {years}")

def process_directory_to_geotiff(input_dir, output_raster_file, output_vector_file, search_start=None, search_end=None,
                                target_crs="EPSG:32629", boundary_shapefile=None, qgis_style_file=False):
    """
    Main function to process all parquet files in a directory and save as a 3-band GeoTIFF
    and optionally a vector file of points.
    Uses UTM coordinates throughout and only reprojects at the end if needed.

    The output GeoTIFF contains 3 bands:
    - Band 1: last_tBreak (YYYYMMDD format, 0 for no break, -9999 for NoData)
    - Band 2: is_break (1=valid_break, 0=no_break, -1=uncertain_break, -99=NoData)
    - Band 3: ndvi_last_segment (float, NaN for NoData)

    Parameters:
    -----------
    input_dir : str
        Directory containing parquet files
    output_raster_file : str
        Path for output GeoTIFF file
    output_vector_file : str or None
        Path for output vector file (None to skip)
    search_start : str or datetime, optional
        Start date for filtering (format: 'YYYY-MM-DD' or datetime object)
    search_end : str or datetime, optional
        End date for filtering (format: 'YYYY-MM-DD' or datetime object)
    target_crs : str
        Target coordinate reference system
    boundary_shapefile : str, optional
        Path to shapefile for spatial boundary filtering
    qgis_style_file : bool
        Whether to create a QGIS style file
    """
    # Create output directories if they don't exist
    for output_file in [output_raster_file, output_vector_file]:
        if output_file is None:
            continue
        output_dir = os.path.dirname(output_file)
        if output_dir:
            Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Collect pixel data using chunked processing
    results_df = collect_pixel_data_chunked(input_dir, search_start, search_end, boundary_shapefile)
    if results_df.empty:
        print("No data found")
        return

    # Create QGIS style file based on break data
    if qgis_style_file == True and not results_df.empty:
        style_file = output_raster_file.replace('.tif', '_year_colors.qml')
        create_qgis_style_file_from_pixels(results_df, style_file)

    # Calculate raster parameters from all pixels
    raster_params = calculate_raster_parameters_from_pixels(results_df)

    print(f"Creating raster with dimensions: {raster_params['width']} x {raster_params['height']}")
    print(f"Resolution: {raster_params['resolution'][0]} x {raster_params['resolution'][1]} meters")

    # Create 3-band raster array
    tbreak_array = create_raster_array_from_pixels(results_df, raster_params)

    # Save to GeoTIFF (with optional reprojection)
    save_geotiff(tbreak_array, output_raster_file, raster_params, source_crs='EPSG:32629', target_crs=target_crs)

    print(f"3-band GeoTIFF saved to: {output_raster_file}")
    print(f"  - Band 1: last_tBreak (YYYYMMDD format)")
    print(f"  - Band 2: is_break (1=valid, 0=none, -1=uncertain)")
    print(f"  - Band 3: ndvi_last_segment")

    # Save vector points if requested
    if output_vector_file is not None:
        num_points_saved = save_vector_points(results_df, output_vector_file, target_crs, source_crs='EPSG:32629')
        print(f"Vector points saved to: {output_vector_file}")
        print(f"Points saved to vector file: {num_points_saved}")

    # Summary statistics
    total_pixels = len(results_df)
    valid_breaks = len(results_df[results_df['is_break'] == 1])
    no_breaks = len(results_df[results_df['is_break'] == 0])
    uncertain_breaks = len(results_df[results_df['is_break'] == -1])

    print(f"\nTotal pixels processed: {total_pixels}")
    print(f"  - Pixels with valid breaks (is_break=1): {valid_breaks}")
    print(f"  - Pixels with no breaks (is_break=0): {no_breaks}")
    print(f"  - Pixels with uncertain breaks (is_break=-1): {uncertain_breaks}")
    print(f"  - Pixels not in parquet files will show as NoData")

if __name__ == "__main__":
    if set_timer == True:
        start_time = time.time()
        print(f"Script started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)

    for start_date, end_date in date_ranges:
        # Create unique filenames for each date range
        # Convert dates to string format for filename (YYYYMMDD)
        start_str = start_date.replace("-", "") if start_date else "NoStart"
        end_str = end_date.replace("-", "") if end_date else "NoEnd"
        date_suffix = f"_{start_str}_to_{end_str}"

        # Insert date suffix before file extension
        base_raster_file = output_raster_file.replace('.tif', f'{date_suffix}.tif')

        # Handle vector file if specified
        if output_vector_file is not None:
            base_vector_file = output_vector_file.replace('.gpkg', f'{date_suffix}.gpkg')
        else:
            base_vector_file = None

        print(f"\n{'='*70}")
        print(f"Processing date range: {start_date} to {end_date}")
        print(f"Output file: {base_raster_file}")
        print(f"{'='*70}\n")

        process_directory_to_geotiff(
            input_directory,
            base_raster_file,
            base_vector_file,
            search_start=start_date,
            search_end=end_date,
            boundary_shapefile=boundary_shapefile,
            qgis_style_file=qgis_style_file
        ) # target_crs='EPSG:4326'

    if set_timer == True:
        end_time = time.time()
        elapsed_time = end_time - start_time
        hours, remainder = divmod(elapsed_time, 3600)
        minutes, seconds = divmod(remainder, 60)

        print("="*70)
        print(f"Script completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total execution time: {int(hours):02d}:{int(minutes):02d}:{seconds:05.2f}")
        print(f"Total execution time: {elapsed_time:.2f} seconds")
