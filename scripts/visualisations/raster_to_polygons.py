"""
Raster to Vector Polygon Converter

This script converts a raster TIFF file to vector polygons, with options for:
- Merging adjacent pixels with the same value
- Merging pixels within a specified date range
- Filtering polygons by minimum area
- Setting polygon values based on most common pixel value
"""

import os
import sys
from datetime import datetime
import numpy as np
import rasterio
from rasterio.features import shapes
from shapely.geometry import shape
import geopandas as gpd


def parse_date_value(date_str):
    """Parse YYYYMMDD string to datetime object."""
    try:
        return datetime.strptime(str(int(date_str)), '%Y%m%d')
    except (ValueError, TypeError):
        return None


# TODO: Figure out how to group dates of only adjacent pixels, instead of current implementation which does them globally 
def create_date_groups(raster_array, date_range_days=0):
    """
    Group pixel values based on date range (optimized version).
    
    Args:
        raster_array: numpy array with date values in YYYYMMDD format
        date_range_days: number of days to group together
    
    Returns:
        numpy array with grouped values
    """
    if date_range_days == 0:
        return raster_array
    
    # Get unique values and their counts in one pass
    unique_values, counts = np.unique(raster_array[raster_array != 0], return_counts=True)
    
    if len(unique_values) == 0:
        return raster_array
    
    # Convert to datetime objects for processing
    date_data = []
    for val, count in zip(unique_values, counts):
        date_obj = parse_date_value(val)
        if date_obj:
            date_data.append((val, date_obj, count))
    
    if not date_data:
        return raster_array
    
    # Sort by date
    date_data.sort(key=lambda x: x[1])
    
    print(f"Processing {len(date_data)} unique date values...")
    
    # Create mapping for value replacements
    value_mapping = {}
    processed = set()
    
    for i, (val1, date1, count1) in enumerate(date_data):
        if val1 in processed:
            continue
            
        # Find all values within date range using vectorized operations
        group_data = [(val1, count1)]
        
        # Only check subsequent dates (since sorted)
        for j in range(i + 1, len(date_data)):
            val2, date2, count2 = date_data[j]
            if val2 in processed:
                continue
                
            # Check if within date range
            days_diff = abs((date2 - date1).days)
            if days_diff <= date_range_days:
                group_data.append((val2, count2))
                processed.add(val2)
            elif days_diff > date_range_days:
                # Since sorted by date, no more matches possible
                break
        
        # Find most common value in group (already have counts)
        if len(group_data) > 1:
            most_common_value = max(group_data, key=lambda x: x[1])[0]
            
            # Store mapping for all values in group
            for val, _ in group_data:
                value_mapping[val] = most_common_value
        
        processed.add(val1)
    
    # Apply all mappings in one pass if any exist
    if value_mapping:
        print(f"Applying {len(value_mapping)} value mappings...")
        grouped_array = raster_array.copy()
        
        # Use vectorized operations for replacement
        for old_val, new_val in value_mapping.items():
            if old_val != new_val:  # Only replace if different
                mask = grouped_array == old_val
                grouped_array[mask] = new_val
        
        return grouped_array
    else:
        return raster_array


def raster_to_polygons(input_raster, output_vector, date_range_days=0, 
                      min_area_ha=0.5, nodata_value=-9999):
    """
    Convert raster to vector polygons.
    
    Args:
        input_raster: path to input TIFF file
        output_vector: path to output vector file
        date_range_days: number of days to group adjacent pixels
        min_area_ha: minimum polygon area in hectares
        nodata_value: value to treat as nodata
    """
    
    print(f"Processing raster: {input_raster}")
    
    # Read raster data
    with rasterio.open(input_raster) as src:
        raster_data = src.read(1)
        transform = src.transform
        crs = src.crs
        
        print(f"Raster shape: {raster_data.shape}")
        print(f"Raster CRS: {crs}")
    
    # Handle nodata
    if nodata_value is not None:
        mask = raster_data != nodata_value
        raster_data = np.where(mask, raster_data, 0)
    
    # Group pixels by date range if specified
    if date_range_days > 0:
        print(f"Grouping pixels within {date_range_days} days...")
        raster_data = create_date_groups(raster_data, date_range_days)
    
    # Convert raster to polygons
    print("Converting raster to polygons...")
    
    # Create polygons from raster
    polygons = []
    values = []
    
    for geom, value in shapes(raster_data, mask=(raster_data != 0), 
                             transform=transform):
        if value != 0:  # Skip nodata values
            poly = shape(geom)
            
            # Calculate area in hectares. Assuming input in meters
            area_m2 = poly.area
            area_ha = area_m2 / 10000  # Convert to hectares
            
            if area_ha >= min_area_ha:
                polygons.append(poly)
                values.append(int(value))
    
    print(f"Created {len(polygons)} polygons after filtering")
    
    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame({
        'date_value': values,
        'area_ha': [poly.area / 10000 for poly in polygons]
    }, geometry=polygons, crs=crs)
    
    # Add formatted date column
    gdf['date_formatted'] = gdf['date_value'].apply(
        lambda x: parse_date_value(x).strftime('%Y-%m-%d') if parse_date_value(x) else 'Invalid'
    )
    
    # Save to file
    print(f"Saving {len(gdf)} polygons to: {output_vector}")
    
    # Determine output format based on extension
    if output_vector.endswith('.shp'):
        gdf.to_file(output_vector, driver='ESRI Shapefile')
    elif output_vector.endswith('.gpkg'):
        gdf.to_file(output_vector, driver='GPKG')
    elif output_vector.endswith('.geojson'):
        gdf.to_file(output_vector, driver='GeoJSON')
    else:
        # Default to shapefile
        gdf.to_file(output_vector, driver='ESRI Shapefile')
    
    print("\nSummary Statistics:")
    print(f"Total polygons: {len(gdf)}")
    print(f"Total area: {gdf['area_ha'].sum():.2f} ha")
    print(f"Average polygon area: {gdf['area_ha'].mean():.2f} ha")
    print(f"Minimum polygon area: {gdf['area_ha'].min():.2f} ha")
    print(f"Maximum polygon area: {gdf['area_ha'].max():.2f} ha")
    
    if len(gdf) > 0:
        print(f"Date range: {gdf['date_formatted'].min()} to {gdf['date_formatted'].max()}")


def main(): 
    input_raster = "/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/personal_tests/testing_new_parquet_processing.tif"  # Path to input raster TIFF file
    output_vector = "/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/personal_tests/07_30day_tol.gpkg"  # Path to output vector file
    date_range_days = 30  # Number of days to group adjacent pixels (default: 0)
    min_area_ha = 0.5  # Minimum polygon area in hectares (default: 0.5)
    nodata_value = -9999  # Nodata value to exclude (default: -9999)
    
    # Validate inputs
    if not os.path.exists(input_raster):
        print(f"Error: Input raster file does not exist: {input_raster}")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(output_vector)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    try:
        # Process the raster
        raster_to_polygons(
            input_raster=input_raster,
            output_vector=output_vector,
            date_range_days=date_range_days,
            min_area_ha=min_area_ha,
            nodata_value=nodata_value
        )
        
        print(f"\nProcessing completed successfully!")
        print(f"Output saved to: {output_vector}")
        
    except Exception as e:
        print(f"Error processing raster: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()