"""
PURPOSE:
This script processes parquet files containing change detection results from satellite imagery analysis.
It filters and aggregates pixel-level change detection data, converts it to raster format, and creates
visualization files for use in GIS software like QGIS.

MAIN FUNCTIONALITY:
- Reads multiple parquet files containing change detection break points (tBreak values)
- Filters data by date range (optional)
- Filters data by shapefile boundary (optional)
- For each pixel location, only the most recent break point is returned
    - If there are no breaks for the pixel, 0 is returned (to seperate between pixels with no breaks and pixels with no data)
- Converts filtered point data to a georeferenced raster (GeoTIFF)
- Creates QGIS style files for visualization
- Optionally saves filtered points as a vector file

INPUTS:
- input_directory: Directory containing parquet files with columns:
  * x_coord, y_coord: UTM coordinates (EPSG:32629 assumed)
  * tBreak: Break date as milliseconds since Unix epoch (UTC)
  * Other columns are preserved but not used for filtering
- search_start: Optional start date for filtering (format: 'YYYY-MM-DD' or datetime object)
- search_end: Optional end date for filtering (format: 'YYYY-MM-DD' or datetime object)
- boundary_shapefile: Optional shapefile path for spatial filtering

OUTPUTS:
- GeoTIFF raster file (.tif): 
  * Pixel values represent break dates in YYYYMMDD format (integer)
    * Pixels without a break date have the value 0
    * Pixels with no data have the NoData value: -9999
  * Resolution: 10m x 10m pixels
  * Coordinate system: UTM (EPSG:32629) or optionally reprojected
- QGIS style file (.qml): Color-coded visualization by year and day-of-year
- Optional vector file (.gpkg): Point locations with break dates for verification
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

## SCRIPT CONFIGS ##
##################################

# Set input directory and output files
input_directory = "/Users/domwelsh/green_ds/Thesis/BDR_300_artigo" # UPDATE
output_raster_file = "/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/personal_tests/01_10_25_memory_test_01.tif" # UPDATE
output_vector_file = None # Add path if vector file is wanted, to check which points were processed to make the raster

# String date range filtering (set both to None to disable filtering)
search_start = "2018-01-01"  # Start date for filtering break dates ("YYYY-MM-DD" format)
search_end = "2021-12-31"    # End date for filtering break dates ("YYYY-MM-DD" format)

# Boundary shapefile filtering (set to None to disable)
boundary_shapefile = None  # Path to shapefile for spatial boundary filtering

qgis_style_file = True  # Set to True if a .qml style file should be created

##################################


## CONVERTING SEARCH DATES TO TIMESTAMP ##
#################################################

# Start and End Dates only used in filter_pixel_group()
if search_start is not None:
    if isinstance(search_start, str):
        search_start_dt = pd.to_datetime(search_start)
    else:
        search_start_dt = search_start
    search_start_ms = int(search_start_dt.timestamp() * 1000)

if search_end is not None:
    if isinstance(search_end, str):
        search_end_dt = pd.to_datetime(search_end)
    else:
        search_end_dt = search_end
    search_end_ms = int(search_end_dt.timestamp() * 1000)

#################################################

def filter_pixel_group(group):
    """
    Filter a group of rows for a single pixel according to the rules:
    - Only return the row with the highest tBreak value
    - Only consider rows within the date range if specified
    - Returns a tuple: (result_row, was_filtered_out)
      where was_filtered_out is True if pixel had data but was filtered out by date
    
    Uses the global search_start_ms and search_end_ms variables
    """
    # Get the most recent break regardless of filtering
    most_recent_row = group.loc[group['tBreak'].idxmax()]

    # Filter by date range if specified
    if search_start is not None or search_end is not None:
        filtered_group = group.copy()

        if search_start is not None:
            filtered_group = filtered_group[filtered_group['tBreak'] >= search_start_ms]

        if search_end is not None:
            filtered_group = filtered_group[filtered_group['tBreak'] <= search_end_ms]

        # If no rows remain after filtering, return the most recent row but marked as filtered out
        if len(filtered_group) == 0:
            return (most_recent_row, True)

        group = filtered_group

    return (group.loc[group['tBreak'].idxmax()], False)

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

def filter_points_by_boundary(df, boundary_gdf, source_crs="EPSG:32629"):
    """
    Separate points into those within and outside the boundary

    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame with x_coord and y_coord columns
    boundary_gdf : geopandas.GeoDataFrame
        Boundary geometry
    source_crs : str
        CRS of the coordinates

    Returns:
    --------
    tuple: (points_within_df, points_outside_df)
        Two DataFrames - points inside and outside the boundary
    """
    # Create GeoDataFrame from points
    points_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.x_coord, df.y_coord),
        crs=source_crs
    )

    # Perform spatial join to find points within boundary
    points_within = gpd.sjoin(points_gdf, boundary_gdf, predicate='within')

    # Get points that are within boundary
    within_df = points_within.drop(columns=['geometry', 'index_right']).reset_index(drop=True)

    # Get points that are outside boundary (those not in the spatial join result)
    within_indices = set(points_within.index)
    all_indices = set(points_gdf.index)
    outside_indices = all_indices - within_indices

    outside_df = df.loc[list(outside_indices)].reset_index(drop=True) if outside_indices else pd.DataFrame()

    print(f"Total points: {len(df)}")
    print(f"Points within boundary: {len(within_df)}")
    print(f"Points outside boundary: {len(outside_df)}")

    return within_df, outside_df

def read_parquet_identify_breaks(filepath):
    """
    Separate segments that corresponds to breaks from terminal segments (not breaks)
    Assumption: all segments in the parquet file for the same pixel are in sequence, from the earlier segment to the last one (the terminal one)

    Inputs: 
    * path to parquet file (output of a pyccd task); each row is a ccd segment; the file has columns 'x_coord' and 'y_coord'

    Output dataframe with additional column ['is_break'] ; each row is still a segment
    
    Note: 'is_break' is 0 for a terminal segment (not a break) and 'is_break' is 1 otherwise 
    """
    df = pd.read_parquet(filepath)

    # create mask and new binary column 'is_break'
    # Using .values avoids pandas index alignment overhead
    mask = (df['x_coord'].values == df['x_coord'].shift(-1).values) & (df['y_coord'].values == df['y_coord'].shift(-1).values)
    df['is_break'] = mask.astype(int)

    return df 

def process_parquet_file(file_path, boundary_gdf=None, source_crs="EPSG:32629"):
    """
    Process a single parquet file and return filtered rows
    Returns a tuple: (valid_rows, filtered_out_rows)
    """
    try:
        df = read_parquet_identify_breaks(file_path)

        df_breaks = df[df['is_break'] == 1]

        valid_rows = []
        filtered_out_rows = []

        # Apply boundary filtering if specified
        if boundary_gdf is not None:
            df_within_boundary, df_outside_boundary = filter_points_by_boundary(df_breaks, boundary_gdf, source_crs)

            # Process points within boundary (apply date filtering)
            if not df_within_boundary.empty:
                grouped = df_within_boundary.groupby(['x_coord', 'y_coord'], sort=False)
                for (x_coord, y_coord), group in grouped:
                    filtered_row, was_filtered_out = filter_pixel_group(group)

                    if was_filtered_out:
                        # Create a copy of the row with tBreak set to 0 to indicate filtered out by date
                        filtered_out_row = filtered_row.copy()
                        filtered_out_row['tBreak'] = 0
                        filtered_out_rows.append(filtered_out_row)
                    else:
                        valid_rows.append(filtered_row)

            # Process points outside boundary (all become filtered out with value 0)
            if not df_outside_boundary.empty:
                grouped_outside = df_outside_boundary.groupby(['x_coord', 'y_coord'], sort=False)
                for (x_coord, y_coord), group in grouped_outside:
                    # Get most recent break for this pixel but mark as filtered out
                    most_recent_row = group.loc[group['tBreak'].idxmax()]
                    filtered_out_row = most_recent_row.copy()
                    filtered_out_row['tBreak'] = 0
                    filtered_out_rows.append(filtered_out_row)

        else:
            # No boundary filtering - only apply date filtering
            grouped = df_breaks.groupby(['x_coord', 'y_coord'], sort=False)
            for (x_coord, y_coord), group in grouped:
                filtered_row, was_filtered_out = filter_pixel_group(group)

                if was_filtered_out:
                    # Create a copy of the row with tBreak set to 0 to indicate filtered out by date
                    filtered_out_row = filtered_row.copy()
                    filtered_out_row['tBreak'] = 0
                    filtered_out_rows.append(filtered_out_row)
                else:
                    valid_rows.append(filtered_row)

        # Process terminal segments (pixels with no breaks) - set tBreak = 0
        df_terminal = df[df['is_break'] == 0]
        if not df_terminal.empty:
            df_terminal_copy = df_terminal.copy()
            df_terminal_copy['tBreak'] = 0
            for _, row in df_terminal_copy.iterrows():
                filtered_out_rows.append(row)

        return valid_rows, filtered_out_rows
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return [], []

def process_files_chunked(input_dir, boundary_shapefile=None, source_crs="EPSG:32629"):
    """
    Generator that yields processed data from parquet files one at a time to avoid memory issues
    Yields tuples: (valid_rows, filtered_out_rows) for each file
    """
    parquet_files = glob.glob(os.path.join(input_dir, "*.parquet"))

    if not parquet_files:
        print(f"No parquet files found in {input_dir}")
        return

    print(f"Found {len(parquet_files)} parquet files to process")

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
        valid_rows, filtered_out_rows = process_parquet_file(file_path, boundary_gdf, source_crs)
        yield valid_rows, filtered_out_rows

def collect_pixel_data_chunked(input_dir, boundary_shapefile=None, source_crs="EPSG:32629"):
    """
    Process all parquet files using pixel-level tracking to handle cross-file duplicates
    Returns pixel trackers for valid and filtered pixels

    Memory optimized: stores only tBreak values instead of full row objects
    """
    # Dictionaries to track the most recent date for each pixel
    valid_pixels = {}  # {(x_coord, y_coord): tBreak_value}
    filtered_pixels = {}  # {(x_coord, y_coord): tBreak_value}

    total_valid_count = 0
    total_filtered_count = 0

    for valid_rows, filtered_out_rows in process_files_chunked(input_dir, boundary_shapefile, source_crs):
        # Process valid rows - keep only most recent per pixel
        for row in valid_rows:
            pixel_key = (row['x_coord'], row['y_coord'])
            tBreak = row['tBreak']

            # If we haven't seen this pixel or found more recent data
            if pixel_key not in valid_pixels or tBreak > valid_pixels[pixel_key]:
                valid_pixels[pixel_key] = tBreak

        # Process filtered out rows - keep only most recent per pixel
        for row in filtered_out_rows:
            pixel_key = (row['x_coord'], row['y_coord'])
            tBreak = row['tBreak']

            # Only add to filtered if not in valid pixels (valid takes priority)
            if pixel_key not in valid_pixels:
                if pixel_key not in filtered_pixels or tBreak > filtered_pixels[pixel_key]:
                    filtered_pixels[pixel_key] = tBreak

        total_valid_count += len(valid_rows)
        total_filtered_count += len(filtered_out_rows)

    print(f"Total valid points processed: {total_valid_count} (unique pixels: {len(valid_pixels)})")
    print(f"Total filtered out points processed: {total_filtered_count} (unique pixels: {len(filtered_pixels)})")

    return valid_pixels, filtered_pixels

def create_geodataframe_from_pixels(pixel_dict, source_crs="EPSG:32629"):
    """
    Create a GeoDataFrame from pixel dictionary keeping it in UTM

    pixel_dict format: {(x_coord, y_coord): tBreak_value}
    """
    if not pixel_dict:
        return None

    # Convert pixel dictionary to DataFrame
    # Single iteration through dict ensures alignment of coordinates and tBreak values
    coords_and_breaks = [(key[0], key[1], value) for key, value in pixel_dict.items()]
    x_coords, y_coords, tBreaks = zip(*coords_and_breaks)

    df = pd.DataFrame({
        'x_coord': x_coords,
        'y_coord': y_coords,
        'tBreak': tBreaks
    })

    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.x_coord, df.y_coord),
        crs=source_crs
    )
    return gdf

def calculate_raster_parameters_from_pixels(valid_pixels, filtered_pixels):
    """
    Calculate raster dimensions and resolution from pixel dictionaries
    with fixed 10x10 meter resolution. Assumes coordinates are pixel centers.
    Considers both valid and filtered out pixels to determine the full extent.
    """
    all_x_coords = []
    all_y_coords = []

    # Extract coordinates from pixel dictionaries
    for (x_coord, y_coord) in valid_pixels.keys():
        all_x_coords.append(x_coord)
        all_y_coords.append(y_coord)

    for (x_coord, y_coord) in filtered_pixels.keys():
        all_x_coords.append(x_coord)
        all_y_coords.append(y_coord)

    if not all_x_coords or not all_y_coords:
        raise ValueError("No coordinate data found in either valid or filtered out datasets")

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

def create_raster_array_from_pixels(valid_pixels, filtered_pixels, raster_params):
    """
    Create a raster array from pixel dictionaries with fixed 10m resolution in UTM.
    Assumes coordinates are pixel centers.

    Parameters:
    -----------
    valid_pixels : dict
        {(x_coord, y_coord): tBreak_value} for points that passed all filters
    filtered_pixels : dict
        {(x_coord, y_coord): tBreak_value} for points filtered out but present in data
    raster_params : dict
        Raster parameters from calculate_raster_parameters_from_pixels
    """
    width = raster_params['width']
    height = raster_params['height']
    min_x, min_y, max_x, max_y = raster_params['bounds']
    res_x, res_y = raster_params['resolution']

    # Initialize with NoData values
    tbreak_array = np.full((height, width), -9999, dtype=np.int32)

    # Process filtered out pixels first (set to 0)
    for (x_coord, y_coord), tBreak in filtered_pixels.items():
        x_idx = int(np.round((x_coord - min_x) / res_x - 0.5))
        y_idx = int(np.round((max_y - y_coord) / res_y - 0.5))

        if 0 <= x_idx < width and 0 <= y_idx < height:
            tbreak_array[y_idx, x_idx] = 0

    # Process valid pixels (set to actual break dates)
    for (x_coord, y_coord), tBreak in valid_pixels.items():
        x_idx = int(np.round((x_coord - min_x) / res_x - 0.5))
        y_idx = int(np.round((max_y - y_coord) / res_y - 0.5))

        if 0 <= x_idx < width and 0 <= y_idx < height:
            if not pd.isna(tBreak) and tBreak != 0:
                date_obj = pd.to_datetime(tBreak, unit='ms', utc=True)
                date_obj = date_obj.tz_localize(None)
                yyyymmdd = int(date_obj.strftime('%Y%m%d'))
                tbreak_array[y_idx, x_idx] = yyyymmdd

    return tbreak_array

def save_geotiff(array, output_file, raster_params, source_crs='EPSG:32629', target_crs='EPSG:32629'):
    """
    Save a numpy array as a GeoTIFF file with a year-based color table, reprojecting to target CRS
    """
    
    nodata_value = -9999
    # array = array.astype(np.int32)

    # If target CRS is different from source, reproject directly
    if source_crs != target_crs:
        # Create a temporary in-memory dataset first
        from rasterio.io import MemoryFile
        
        with MemoryFile() as memfile:
            with memfile.open(
                driver='GTiff',
                height=raster_params['height'],
                width=raster_params['width'],
                count=1,
                dtype=np.int32,
                crs=source_crs,
                transform=raster_params['transform'],
                nodata=nodata_value
            ) as src:
                src.write(array, 1)
                
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
                        reproject(
                            source=rasterio.band(src, i),
                            destination=rasterio.band(dst, i),
                            src_transform=src.transform,
                            src_crs=src.crs,
                            dst_transform=transform,
                            dst_crs=target_crs,
                            resampling=Resampling.nearest)

    else:
        # If no reprojection needed, just save directly
        with rasterio.open(
            output_file,
            'w',
            driver='GTiff',
            height=raster_params['height'],
            width=raster_params['width'],
            count=1,
            dtype=np.int32,
            crs=source_crs,
            transform=raster_params['transform'],
            nodata=nodata_value
        ) as dst:
            dst.write(array, 1)

def save_vector_points(gdf, output_file, target_crs="EPSG:32629"):
    """
    Save all points from the GeoDataFrame that have valid break dates as a vector file.
    """
    valid_points_gdf = gdf.copy()

    # Convert break_date from milliseconds to date format - use UTC consistently
    if not valid_points_gdf.empty:
        # Assuming break_date is in milliseconds since epoch
        valid_points_gdf['tBreak_date'] = pd.to_datetime(
            valid_points_gdf['tBreak'], unit='ms', utc=True
        ).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
    
    # Reproject if necessary
    if valid_points_gdf.crs.to_string() != target_crs:
        valid_points_gdf = valid_points_gdf.to_crs(target_crs)
        
    valid_points_gdf.to_file(output_file, driver='GPKG')
    
    return len(valid_points_gdf)


def create_qgis_style_file_from_pixels(valid_pixels, output_style_file):
    """
    Create a QGIS .qml style file that colors pixels by year with gradient shading by day of year

    valid_pixels format: {(x_coord, y_coord): tBreak_value}
    """

    # Get all unique dates and extract years - use UTC consistently
    valid_dates = [pd.to_datetime(tBreak, unit='ms', utc=True).tz_localize(None)
                   for tBreak in valid_pixels.values() if not pd.isna(tBreak)]
    
    # Group dates by year
    dates_by_year = {}
    for date in valid_dates:
        year = date.year
        date_int = int(date.strftime('%Y%m%d'))
        if year not in dates_by_year:
            dates_by_year[year] = []
        dates_by_year[year].append(date_int)
    
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
        
        for date_value in year_dates:
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
    
    # Add entries for filtered out pixels (value = 0) and nodata
    qml_content += '''        <paletteEntry value="0" color="#808080" label="Filtered Out (Data present but outside filter criteria)"/>
        <paletteEntry value="-9999" color="#000000" label="No Data" alpha="0"/>
      </colorPalette>
    </rasterrenderer>
  </pipe>
</qgis>'''
    
    # Save style file
    with open(output_style_file, 'w') as f:
        f.write(qml_content)
    
    print(f"QGIS style file saved to: {output_style_file}")
    print(f"Years in data: {years}")

def process_directory_to_geotiff(input_dir, output_raster_file, output_vector_file, target_crs="EPSG:32629",
                                boundary_shapefile=None, qgis_style_file=False):
    """
    Main function to process all parquet files in a directory and save as a single GeoTIFF
    and a vector file of used points.
    Uses UTM coordinates throughout and only reprojects at the end if needed.

    Parameters:
    -----------
    input_dir : str
        Directory containing parquet files
    output_raster_file : str
        Path for output GeoTIFF file
    output_vector_file : str or None
        Path for output vector file (None to skip)
    target_crs : str
        Target coordinate reference system
    boundary_shapefile : str, optional
        Path to shapefile for spatial boundary filtering
    """
    # Create output directories if they don't exist
    for output_file in [output_raster_file, output_vector_file]:
        if output_file is None:
            continue
        output_dir = os.path.dirname(output_file)
        if output_dir:
            Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Collect pixel data using chunked processing
    valid_pixels, filtered_pixels = collect_pixel_data_chunked(input_dir, boundary_shapefile)
    if not valid_pixels and not filtered_pixels:
        print("No data found")
        return

    # Create QGIS style file based on valid data only
    if qgis_style_file == True and valid_pixels:
        style_file = output_raster_file.replace('.tif', '_year_colors.qml')
        create_qgis_style_file_from_pixels(valid_pixels, style_file)

    # Calculate raster parameters considering both datasets
    raster_params = calculate_raster_parameters_from_pixels(valid_pixels, filtered_pixels)

    print(f"Creating raster with dimensions: {raster_params['width']} x {raster_params['height']}")
    print(f"Resolution: {raster_params['resolution'][0]} x {raster_params['resolution'][1]} meters")

    # Create raster array
    tbreak_array = create_raster_array_from_pixels(valid_pixels, filtered_pixels, raster_params)

    # Save to GeoTIFF (with optional reprojection)
    save_geotiff(tbreak_array, output_raster_file, raster_params, source_crs='EPSG:32629', target_crs=target_crs)

    # Save vector points (only valid points for vector output)
    if output_vector_file is not None and valid_pixels:
        valid_gdf = create_geodataframe_from_pixels(valid_pixels, source_crs='EPSG:32629')
        num_points_saved = save_vector_points(valid_gdf, output_vector_file, target_crs)
        print(f"Vector points saved to: {output_vector_file}")
        print(f"Points saved to vector file: {num_points_saved}")

    print(f"Combined GeoTIFF saved to: {output_raster_file}")

    # Summary statistics
    total_valid = len(valid_pixels)
    total_filtered_out = len(filtered_pixels)
    total_processed = total_valid + total_filtered_out

    print(f"Total pixels processed: {total_processed}")
    print(f"  - Pixels with valid break dates (passing filters): {total_valid}")
    print(f"  - Pixels filtered out but present in data (set to 0): {total_filtered_out}")
    print(f"  - Pixels not in parquet files will show as NoData")

if __name__ == "__main__":
    # start_time = time.time()
    # print(f"Script started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    # print("="*70)

    process_directory_to_geotiff(
        input_directory,
        output_raster_file,
        output_vector_file,
        boundary_shapefile=boundary_shapefile,
        qgis_style_file=qgis_style_file
    ) # target_crs='EPSG:4326'

    # end_time = time.time()
    # elapsed_time = end_time - start_time
    # hours, remainder = divmod(elapsed_time, 3600)
    # minutes, seconds = divmod(remainder, 60)

    # print("="*70)
    # print(f"Script completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    # print(f"Total execution time: {int(hours):02d}:{int(minutes):02d}:{seconds:05.2f}")
    # print(f"Total execution time: {elapsed_time:.2f} seconds")
