"""
PURPOSE:
This script processes parquet files containing change detection results from satellite imagery analysis.
It filters and aggregates pixel-level change detection data, converts it to raster format, and creates
visualization files for use in GIS software like QGIS.

MAIN FUNCTIONALITY:
- Reads multiple parquet files containing change detection break points (tBreak values)
- Filters data by date range (optional)
- Filters data by shapefile boundary (optional)
- For each pixel location, selects the most relevant break point using these rules:
  * If only one break exists: keep it
  * If multiple breaks exist: keep the one with the second-highest tBreak value
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
  * NoData value: -9999
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

def filter_pixel_group(group, search_start=None, search_end=None):
    """
    Filter a group of rows for a single pixel according to the rules:
    - If only one row exists, keep it
    - If multiple rows exist, keep the row with the second highest tBreak
    - Only consider rows within the date range if specified
    """
    # Filter by date range if specified
    if search_start is not None or search_end is not None:
        filtered_group = group.copy()
        
        if search_start is not None:
            # Convert search_start to milliseconds timestamp
            if isinstance(search_start, str):
                search_start_dt = pd.to_datetime(search_start)
            else:
                search_start_dt = search_start
            search_start_ms = int(search_start_dt.timestamp() * 1000)
            filtered_group = filtered_group[filtered_group['tBreak'] >= search_start_ms]
        
        if search_end is not None:
            # Convert search_end to milliseconds timestamp
            if isinstance(search_end, str):
                search_end_dt = pd.to_datetime(search_end)
            else:
                search_end_dt = search_end
            search_end_ms = int(search_end_dt.timestamp() * 1000)
            filtered_group = filtered_group[filtered_group['tBreak'] <= search_end_ms]
        
        # If no rows remain after filtering, return None
        if len(filtered_group) == 0:
            return None
            
        group = filtered_group
    
    if len(group) == 1:
        return group.iloc[0]
    else:
        second_highest_idx = group['tBreak'].nlargest(2).index[-1]
        return group.loc[second_highest_idx]

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
    Filter points to only include those within the boundary
    
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
    pandas.DataFrame
        Filtered DataFrame with only points inside boundary
    """
    # Create GeoDataFrame from points
    points_gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.x_coord, df.y_coord),
        crs=source_crs
    )
    
    # Perform spatial join to find points within boundary
    points_within = gpd.sjoin(points_gdf, boundary_gdf, predicate='within')
    
    # Remove the extra columns from the spatial join and return as DataFrame
    result_df = points_within.drop(columns=['geometry', 'index_right']).reset_index(drop=True)
    
    print(f"Points before boundary filtering: {len(df)}")
    print(f"Points after boundary filtering: {len(result_df)}")
    
    return result_df

def process_parquet_file(file_path, search_start=None, search_end=None, boundary_gdf=None, source_crs="EPSG:32629"):
    """
    Process a single parquet file and return filtered rows
    """
    try:
        df = pd.read_parquet(file_path)
        
        # Apply boundary filtering first if specified
        if boundary_gdf is not None:
            df = filter_points_by_boundary(df, boundary_gdf, source_crs)
            if len(df) == 0:
                return []
        
        grouped = df.groupby(['x_coord', 'y_coord'])
        filtered_rows = []
        
        for (x, y), group in grouped:
            filtered_row = filter_pixel_group(group, search_start, search_end)
            if filtered_row is not None:
                filtered_rows.append(filtered_row)
        
        return filtered_rows
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return []

def collect_data_from_directory(input_dir, search_start=None, search_end=None, boundary_shapefile=None, source_crs="EPSG:32629"):
    """
    Collect and process data from all parquet files in a directory
    """
    parquet_files = glob.glob(os.path.join(input_dir, "*.parquet"))
    
    if not parquet_files:
        print(f"No parquet files found in {input_dir}")
        return None
    
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
    
    all_filtered_rows = []
    
    for i, file_path in enumerate(parquet_files, 1):
        print(f"Processing file {i}/{len(parquet_files)}: {os.path.basename(file_path)}")
        filtered_rows = process_parquet_file(file_path, search_start, search_end, boundary_gdf, source_crs)
        all_filtered_rows.extend(filtered_rows)
    
    if not all_filtered_rows:
        print("No valid data found in any files (possibly due to filtering)")
        return None
    
    print(f"Total points after all filtering: {len(all_filtered_rows)}")
    return pd.DataFrame(all_filtered_rows)

def create_geodataframe(df, source_crs="EPSG:32629"):
    """
    Create a GeoDataFrame from the DataFrame keeping it in UTM
    """
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.x_coord, df.y_coord),
        crs=source_crs
    )
    return gdf

def calculate_raster_parameters_utm(gdf):
    """
    Calculate raster dimensions and resolution from GeoDataFrame in UTM
    with fixed 10x10 meter resolution. Assumes coordinates are pixel centers.
    """
    # Assuming gdf is in UTM (EPSG:32629)
    min_x, min_y = gdf['x_coord'].min(), gdf['y_coord'].min()
    max_x, max_y = gdf['x_coord'].max(), gdf['y_coord'].max()
    
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

def create_raster_array_utm(gdf, raster_params):
    """
    Create a raster array from GeoDataFrame with fixed 10m resolution in UTM.
    Assumes coordinates are pixel centers.
    """
    width = raster_params['width']
    height = raster_params['height']
    min_x, min_y, max_x, max_y = raster_params['bounds']
    res_x, res_y = raster_params['resolution']
    
    tbreak_array = np.full((height, width), -9999, dtype=np.int32)
    for idx, row in gdf.iterrows():
        # Calculate indices from pixel center coordinates
        x_idx = int(np.round((row['x_coord'] - min_x) / res_x - 0.5))
        y_idx = int(np.round((max_y - row['y_coord']) / res_y - 0.5))
        
        if 0 <= x_idx < width and 0 <= y_idx < height:
            if not pd.isna(row['tBreak']):
                date_obj = pd.to_datetime(row['tBreak'], unit='ms', utc=True)
                date_obj = date_obj.tz_localize(None)
                yyyymmdd = int(date_obj.strftime('%Y%m%d'))
                tbreak_array[y_idx, x_idx] = yyyymmdd
            # tbreak_array[y_idx, x_idx] = row['tBreak']
    
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


def create_qgis_style_file(gdf, output_style_file):
    """
    Create a QGIS .qml style file that colors pixels by year with gradient shading by day of year
    """
    
    # Get all unique dates and extract years - use UTC consistently
    valid_dates = gdf[~pd.isna(gdf['tBreak'])]['tBreak'].apply(
        lambda x: pd.to_datetime(x, unit='ms', utc=True).tz_localize(None)
    )
    
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
    
    # Add nodata value
    qml_content += '''        <paletteEntry value="-9999" color="#000000" label="No Data" alpha="0"/>
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
                                search_start=None, search_end=None, boundary_shapefile=None):
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
    search_start : str or datetime, optional
        Start date for filtering (e.g., '2020-01-01')
    search_end : str or datetime, optional
        End date for filtering (e.g., '2023-12-31')
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
    
    # Collect data from all parquet files
    df = collect_data_from_directory(input_dir, search_start, search_end, boundary_shapefile)
    if df is None:
        print("No data")
        return
    
    # Create GeoDataFrame
    gdf = create_geodataframe(df)

    style_file = output_raster_file.replace('.tif', '_year_colors.qml')
    create_qgis_style_file(gdf, style_file)
    
    # Calculate raster parameters
    raster_params = calculate_raster_parameters_utm(gdf)
    
    print(f"Creating raster with dimensions: {raster_params['width']} x {raster_params['height']}")
    print(f"Resolution: {raster_params['resolution'][0]} x {raster_params['resolution'][1]} meters")
    
    # Create raster array
    tbreak_array = create_raster_array_utm(gdf, raster_params)
    
    # Save to GeoTIFF (with optional reprojection)
    save_geotiff(tbreak_array, output_raster_file, raster_params, source_crs='EPSG:32629', target_crs=target_crs)
    
    # Save vector points
    if output_vector_file is not None:
        num_points_saved = save_vector_points(gdf, output_vector_file, target_crs)
        print(f"Vector points saved to: {output_vector_file}")
        print(f"Points saved to vector file: {num_points_saved}")
    
    print(f"Combined GeoTIFF saved to: {output_raster_file}")
    print(f"Total pixels processed: {len(df)}")

if __name__ == "__main__":
    # Set input directory and output files
    input_directory = "/Users/domwelsh/green_ds/Thesis/BDR_300_artigo" # UPDATE
    output_raster_file = "/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/accuracy_assessment/last_break_dates_date_filter_test.tif" # UPDATE
    output_vector_file = None # Add path if vector file is wanted, to check which points were processed to make the raster
    
    # String date range filtering (set both to None to disable filtering)
    search_start = None  # Start date for filtering break dates ("YYYY-MM-DD" format)
    search_end = None    # End date for filtering break dates ("YYYY-MM-DD" format)
    
    # Boundary shapefile filtering (set to None to disable)
    boundary_shapefile = None  # Path to shapefile for spatial boundary filtering
    
    process_directory_to_geotiff(
        input_directory, 
        output_raster_file, 
        output_vector_file, 
        search_start=search_start, 
        search_end=search_end,
        boundary_shapefile=boundary_shapefile
    ) # target_crs='EPSG:4326'
