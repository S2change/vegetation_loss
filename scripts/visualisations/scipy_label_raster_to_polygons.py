"""
Raster to Vector Polygon Converter using Scipy Labels

This script converts a raster TIFF file to vector polygons with spatial-temporal clustering.

- Uses connected component labeling (scipy.ndimage.label) to find spatially-connected groups
- Applies date grouping ONLY within each spatial cluster
- Configurable connectivity (4-connectivity or 8-connectivity)

Features:
- Spatial-temporal clustering of pixels
- Merging pixels within a specified date range (per cluster)
- Filtering polygons by minimum area
- Setting polygon values based on most common pixel value
"""

import os
import sys
from datetime import datetime
import time
import numpy as np
import rasterio
from rasterio.features import shapes
from shapely.geometry import shape
import geopandas as gpd
from scipy.ndimage import label, generate_binary_structure

## SCRIPT CONFIGS ##
##################################

input_raster = "/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/personal_tests/09_optimized_test_20180101_to_20211231.tif"  # Path to input raster TIFF file
output_vector = "/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/personal_tests/13_label_timer.gpkg"  # Path to output vector file

band_number = 1  # Which band contains the date values (default: 1 for first band)
date_range_days = 30  # Number of days to group adjacent pixels within each spatial cluster (default: 0)
min_area_ha = 0.5  # Minimum polygon area in hectares (default: 0.5)
nodata_value = -9999  # Nodata value to exclude (default: -9999)
connectivity = 8  # 4 for edge-only, 8 for edge+diagonal (default: 8)

##################################

def parse_date_value(date_str):
    """Parse YYYYMMDD string to datetime object."""
    try:
        return datetime.strptime(str(int(date_str)), '%Y%m%d')
    except (ValueError, TypeError):
        return None

def create_date_groups_within_cluster(cluster_dates, date_range_days):
    """
    Group date values within a single spatial cluster based on temporal proximity.

    Args:
        cluster_dates: numpy array of date values in YYYYMMDD format (from one cluster)
        date_range_days: number of days to group together

    Returns:
        dict mapping old date values to new grouped date values
    """
    unique_values, counts = np.unique(cluster_dates, return_counts=True)

    if len(unique_values) == 0:
        return {}

    # Convert to datetime objects for processing
    date_data = []
    for val, count in zip(unique_values, counts):
        date_obj = parse_date_value(val)
        if date_obj:
            date_data.append((val, date_obj, count))

    if not date_data:
        return {}

    date_data.sort(key=lambda x: x[1])

    value_mapping = {}
    processed = set()

    for i, (val1, date1, count1) in enumerate(date_data):
        if val1 in processed:
            continue

        # Find all values within date range
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

        # Find most common value in group
        if len(group_data) > 1:
            most_common_value = max(group_data, key=lambda x: x[1])[0]

            # Store mapping for all values in group
            for val, _ in group_data:
                value_mapping[val] = most_common_value

        processed.add(val1)

    return value_mapping

def create_spatial_temporal_groups(raster_array, date_range_days=0, connectivity=8):
    """
    Group pixel values based on spatial proximity and temporal similarity.

    Args:
        raster_array: numpy array with date values in YYYYMMDD format
        date_range_days: number of days to group together within each spatial cluster
        connectivity: 4 for edge-only connectivity, 8 for edge+diagonal connectivity

    Returns:
        numpy array with grouped values
    """
    if date_range_days == 0:
        return raster_array

    print(f"Starting spatial-temporal clustering (connectivity={connectivity}, date_range={date_range_days} days)...")

    mask = raster_array != 0

    # Find and label spatially-connected components
    start_time = time.time()
    if connectivity == 8:
        structure = generate_binary_structure(2, 2)  # 8-connectivity (edges + diagonals)
    else:
        structure = generate_binary_structure(2, 1)  # 4-connectivity (edges only)

    labeled_array, num_features = label(mask, structure=structure)  # type: ignore
    elapsed = time.time() - start_time
    print(f"  Scipy labeling took {elapsed:.2f} seconds")
    print(f"  Found {num_features} spatially-connected clusters")

    if num_features == 0:
        return raster_array

    # Process each spatial cluster independently
    start_time = time.time()
    grouped_array = raster_array.copy()
    total_mappings = 0

    for cluster_id in range(1, num_features + 1):
        if cluster_id % 1000 == 0:
            print(f"  Processed {cluster_id}/{num_features} clusters...")

        cluster_mask = (labeled_array == cluster_id)

        if not cluster_mask.any():
            continue

        cluster_dates = raster_array[cluster_mask]
        value_mapping = create_date_groups_within_cluster(cluster_dates, date_range_days)

        if value_mapping:
            total_mappings += len(value_mapping)

            # Build vectorized mapping for this cluster
            # Extract unique old values that need changing
            old_values = np.array([k for k, v in value_mapping.items() if k != v])
            new_values = np.array([value_mapping[k] for k in old_values])

            if len(old_values) > 0:
                # Get cluster pixels only once
                cluster_pixels = grouped_array[cluster_mask]

                # Vectorized replacement using np.isin
                for old_val, new_val in zip(old_values, new_values):
                    cluster_pixels[cluster_pixels == old_val] = new_val

                # Write back to the array
                grouped_array[cluster_mask] = cluster_pixels

    elapsed = time.time() - start_time
    print(f"  Date grouping within clusters took {elapsed:.2f} seconds")
    print(f"  Applied {total_mappings} date mappings")

    return grouped_array

def raster_to_polygons(input_raster, output_vector, band_number=1, date_range_days=0,
                      min_area_ha=0.5, nodata_value=-9999, connectivity=8):
    """
    Convert raster to vector polygons with spatial-temporal clustering.

    Args:
        input_raster: path to input TIFF file
        output_vector: path to output vector file
        band_number: which band contains the date values (default: 1)
        date_range_days: number of days to group adjacent pixels within each spatial cluster
        min_area_ha: minimum polygon area in hectares
        nodata_value: value to treat as nodata
        connectivity: 4 for edge-only, 8 for edge+diagonal (default: 8)
    """

    print(f"Processing raster: {input_raster}")

    start_time = time.time()
    with rasterio.open(input_raster) as src:
        print(f"Raster has {src.count} band(s)")
        print(f"Reading band {band_number}...")
        raster_data = src.read(band_number)
        transform = src.transform
        crs = src.crs

        print(f"Raster shape: {raster_data.shape}")
        print(f"Raster CRS: {crs}")
    elapsed = time.time() - start_time
    print(f"Raster reading took {elapsed:.2f} seconds\n")

    if nodata_value is not None:
        mask = raster_data != nodata_value
        raster_data = np.where(mask, raster_data, 0)

    # Group pixels by date range if specified (using spatial-temporal clustering)
    if date_range_days > 0:
        raster_data = create_spatial_temporal_groups(raster_data, date_range_days, connectivity)

    print("\nConverting raster to polygons...")
    start_time = time.time()

    polygons = []
    values = []
    areas_ha = []

    for geom, value in shapes(raster_data, mask=(raster_data != 0),
                              connectivity=connectivity,
                              transform=transform):
        poly = shape(geom)

        area_ha = poly.area / 10000

        if area_ha >= min_area_ha:
            polygons.append(poly)
            values.append(int(value))
            areas_ha.append(area_ha)

    elapsed = time.time() - start_time
    print(f"Polygon conversion took {elapsed:.2f} seconds")
    print(f"Created {len(polygons)} polygons after filtering\n")

    start_time = time.time()
    gdf = gpd.GeoDataFrame({
        'date_value': values,
        'area_ha': areas_ha
    }, geometry=polygons, crs=crs)

    def format_date(x):
        date_obj = parse_date_value(x)
        return date_obj.strftime('%Y-%m-%d') if date_obj else 'Invalid'

    gdf['date_formatted'] = gdf['date_value'].apply(format_date)

    print(f"Saving {len(gdf)} polygons to: {output_vector}")

    if output_vector.endswith('.shp'):
        gdf.to_file(output_vector, driver='ESRI Shapefile')
    elif output_vector.endswith('.gpkg'):
        gdf.to_file(output_vector, driver='GPKG')
    elif output_vector.endswith('.geojson'):
        gdf.to_file(output_vector, driver='GeoJSON')
    else:
        gdf.to_file(output_vector, driver='ESRI Shapefile')

    elapsed = time.time() - start_time
    print(f"File writing took {elapsed:.2f} seconds\n")

    print("Summary Statistics:")
    print(f"Total polygons: {len(gdf)}")
    print(f"Total area: {gdf['area_ha'].sum():.2f} ha")
    print(f"Average polygon area: {gdf['area_ha'].mean():.2f} ha")
    print(f"Minimum polygon area: {gdf['area_ha'].min():.2f} ha")
    print(f"Maximum polygon area: {gdf['area_ha'].max():.2f} ha")

    if len(gdf) > 0:
        print(f"Date range: {gdf['date_formatted'].min()} to {gdf['date_formatted'].max()}")

def main():
    overall_start = time.time()
    print("="*60)
    print("RASTER TO POLYGON CONVERTER - SCIPY LABEL ALGORITHM")
    print("="*60 + "\n")

    if not os.path.exists(input_raster):
        print(f"Error: Input raster file does not exist: {input_raster}")
        sys.exit(1)

    output_dir = os.path.dirname(output_vector)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    try:
        raster_to_polygons(
            input_raster=input_raster,
            output_vector=output_vector,
            band_number=band_number,
            date_range_days=date_range_days,
            min_area_ha=min_area_ha,
            nodata_value=nodata_value,
            connectivity=connectivity
        )

        overall_elapsed = time.time() - overall_start
        print("\n" + "="*60)
        print(f"TOTAL EXECUTION TIME: {overall_elapsed:.2f} seconds ({overall_elapsed/60:.2f} minutes)")
        print("="*60)
        print(f"\nProcessing completed successfully!")
        print(f"Output saved to: {output_vector}")

    except Exception as e:
        print(f"Error processing raster: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()