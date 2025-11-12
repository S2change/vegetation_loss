"""
Raster to Vector Polygon Converter using Graph Algorithm

This script converts a raster TIFF file to vector polygons with spatial-temporal clustering.

- Uses graph-based algorithm (networkx + scipy KDTree) to cluster pixels
- Combines spatial connectivity and temporal tolerance using graph edges
- Configurable connectivity (4-connectivity or 8-connectivity)

Features:
- Spatial-temporal clustering of pixels with tolerance-based date grouping
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
import networkx as nx
from scipy.spatial import cKDTree
import glob
from datetime import datetime, timedelta

## SCRIPT CONFIGS ##
##################################

input_raster = r"C:\Users\Public\Documents\outputs_ROI\tabular\T29TQG\processed_outputs\rasters"
output_vector = r"C:\Users\Public\Documents\outputs_ROI\tabular\T29TQG\processed_outputs\vectors"

band_1 = 1  # First band containing date values (the script will compute the average with band_2)
band_2 = 2  # Second band containing date values (used together with band_1 to compute average date)
date_range_days = 10  # Number of days to group adjacent pixels within each spatial cluster (default: 0)
min_area_ha = 0.5  # Minimum polygon area in hectares (default: 0.5)
nodata_value = -9999  # Nodata value to exclude (default: -9999)
connectivity = 8  # 4 for edge-only, 8 for edge+diagonal (default: 8)

##################################

def yyyymmdd_to_datetime(arr, nodata_value=-9999):
    """Converts a yyyymmdd array to datetime, ignoring nodata values and zeros."""
    dt_array = np.full(arr.shape, None, dtype=object)
    valid_mask = (arr != nodata_value) & (arr != 0)
    for idx in zip(*np.where(valid_mask)):
        val = arr[idx]
        try:
            dt_array[idx] = datetime.strptime(str(val), "%Y%m%d")
        except ValueError:
            dt_array[idx] = None
    return dt_array, valid_mask

def datetime_to_yyyymmdd(dt_array, nodata_value=-9999):
    """Converts a datetime array back to yyyymmdd integers, assigning nodata where values are None."""
    out = np.full(dt_array.shape, nodata_value, dtype=int)
    for idx in zip(*np.where(dt_array != None)):
        out[idx] = int(dt_array[idx].strftime("%Y%m%d"))
    return out

def parse_date_value(date_str):
    """Parse YYYYMMDD string to datetime object."""
    try:
        return datetime.strptime(str(int(date_str)), '%Y%m%d')
    except (ValueError, TypeError):
        return None

def date_to_days_vectorized(date_array):
    """
    Convert YYYYMMDD array to days since reference date (2000-01-01) using vectorized operations.

    Args:
        date_array: numpy array with date values in YYYYMMDD format

    Returns:
        numpy array with days since 2000-01-01
    """
    # Create output array
    result = np.zeros_like(date_array, dtype=np.int32)

    # Mask for valid dates (not 0 or -9999)
    valid_mask = (date_array != 0) & (date_array != -9999)

    if not np.any(valid_mask):
        return result

    # Extract valid dates
    valid_dates = date_array[valid_mask].astype(np.int32)

    # Vectorized date parsing: extract year, month, day
    year = valid_dates // 10000
    month = (valid_dates % 10000) // 100
    day = valid_dates % 100

    # Calculate days since 2000-01-01 using vectorized operations
    # Days since epoch for 2000-01-01
    ref_days = 10957  # days from 1970-01-01 to 2000-01-01

    # Calculate days from 1970-01-01 for each date
    # Using approximate day calculation (365.25 days per year average)
    days_from_1970 = (year - 1970) * 365 + (year - 1969) // 4 - (year - 1901) // 100 + (year - 1601) // 400

    # Add month days (cumulative days at start of each month for non-leap year)
    month_days = np.array([0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334])
    days_from_1970 += month_days[month - 1]

    # Add leap day if after February in leap year
    is_leap = ((year % 4 == 0) & (year % 100 != 0)) | (year % 400 == 0)
    days_from_1970 += ((month > 2) & is_leap).astype(np.int32)

    # Add day of month
    days_from_1970 += day - 1

    # Convert to days since 2000-01-01
    result[valid_mask] = days_from_1970 - ref_days

    return result

def graph_based_clustering(image, tolerance, connectivity=8):
    """
    Cluster pixels using graph algorithm with spatial and temporal constraints.

    Args:
        image: numpy array with numeric values (e.g., days since reference)
        tolerance: maximum difference in values to be considered same cluster
        connectivity: 4 for edge-only, 8 for edge+diagonal (default: 8)

    Returns:
        labels: numpy array with cluster IDs
        label_count: number of clusters found
    """
    labels = np.full(image.shape, -1, dtype=int)

    # Get valid pixel coordinates and values (non-zero pixels)
    rows, cols = np.where(image != 0)
    if len(rows) == 0:
        return labels, 0

    values = image[rows, cols]

    # Create coordinate array for KDTree (row, col positions)
    coords = np.column_stack([rows, cols])

    # Build KDTree for spatial neighbor queries
    tree = cKDTree(coords)

    # Determine maximum spatial distance based on connectivity
    # 4-connectivity: only orthogonal neighbors (distance = 1)
    # 8-connectivity: orthogonal + diagonal neighbors (distance = sqrt(2))
    maxdist = np.sqrt(2) if connectivity == 8 else 1.0

    # Build graph
    G = nx.Graph()

    # Add nodes with their temporal values
    for idx in range(len(rows)):
        G.add_node(idx, value=values[idx])

    # Find spatial neighbors within maxdist
    pairs = tree.query_pairs(r=maxdist)

    # Add edges if both spatial and temporal conditions are satisfied
    for i, j in pairs:
        # Add edge if temporal difference is within tolerance
        if abs(values[i] - values[j]) <= tolerance:
            G.add_edge(i, j)

    # Find connected components (clusters)
    connected_components = list(nx.connected_components(G))

    # Assign cluster labels
    for cluster_id, component in enumerate(connected_components):
        for node_idx in component:
            labels[rows[node_idx], cols[node_idx]] = cluster_id

    return labels, len(connected_components)

def create_spatial_temporal_groups(raster_array, date_range_days=0, connectivity=8):
    if date_range_days == 0:
        return raster_array, raster_array

    print(f"Starting spatial-temporal clustering (connectivity={connectivity}, date_range={date_range_days} days)...")

    start_time = time.time()
    days_array = date_to_days_vectorized(raster_array)
    elapsed = time.time() - start_time
    print(f"  Date conversion took {elapsed:.2f} seconds")

    start_time = time.time()
    labels, num_clusters = graph_based_clustering(days_array, tolerance=date_range_days, connectivity=connectivity)
    elapsed = time.time() - start_time
    print(f"  Graph-based clustering took {elapsed:.2f} seconds")
    print(f"  Found {num_clusters} clusters")

    return raster_array, labels

def raster_to_polygons(input_raster, output_vector, band_1=1, band_2=2, date_range_days=0,
                       min_area_ha=0.5, nodata_value=-9999, connectivity=8):

    print(f"Processing raster: {input_raster}")

    start_time = time.time()
    with rasterio.open(input_raster) as src:
        print(f"Raster has {src.count} band(s)")
        
        print("Reading band 1...")
        band1 = src.read(band_1)
        print("Reading band 2...")
        band2 = src.read(band_2)
        
        transform = src.transform
        crs = src.crs
        print(f"Raster shape: {band1.shape}")
        print(f"Raster CRS: {crs}")
    elapsed = time.time() - start_time
    print(f"Raster reading took {elapsed:.2f} seconds\n")

    # Convert bands to datetime
    dt1, mask1 = yyyymmdd_to_datetime(band1, nodata_value)
    dt2, mask2 = yyyymmdd_to_datetime(band2, nodata_value)

    combined_mask = mask1 & mask2

    # Calculate the average of valid dates
    mean_dt = np.full(band1.shape, None, dtype=object)
    for idx in zip(*np.where(combined_mask)):
        delta1 = dt1[idx] - datetime(1970,1,1)
        delta2 = dt2[idx] - datetime(1970,1,1)
        mean_days = (delta1.days + delta2.days) / 2
        mean_dt[idx] = datetime(1970,1,1) + timedelta(days=int(round(mean_days)))

    # Convert back to yyyymmdd
    raster_data = datetime_to_yyyymmdd(mean_dt, nodata_value)

    if date_range_days > 0:
        raster_data, cluster_labels = create_spatial_temporal_groups(raster_data, date_range_days, connectivity)
    else:
        cluster_labels = raster_data

    print("\nFiltering clusters by minimum area...")
    start_time = time.time()

    # Calculate pixel area from transform
    pixel_area_m2 = abs(transform[0] * transform[4])  # pixel width * pixel height
    pixel_area_ha = pixel_area_m2 / 10000
    min_pixels = int(np.ceil(min_area_ha / pixel_area_ha))

    # Count pixels per cluster
    unique_labels, label_counts = np.unique(cluster_labels[cluster_labels >= 0], return_counts=True)

    # Filter clusters by minimum pixel count
    valid_clusters = unique_labels[label_counts >= min_pixels]

    # Create filtered cluster labels using vectorized operations (set small clusters to -1)
    filtered_labels = np.full_like(cluster_labels, -1, dtype=np.int32)
    valid_mask = np.isin(cluster_labels, valid_clusters)
    filtered_labels[valid_mask] = cluster_labels[valid_mask]

    num_filtered_out = len(unique_labels) - len(valid_clusters)
    print(f"  Filtered out {num_filtered_out} small clusters (< {min_area_ha} ha)")
    print(f"  Remaining clusters: {len(valid_clusters)}")
    elapsed = time.time() - start_time
    print(f"  Filtering took {elapsed:.2f} seconds")

    print("\nConverting raster to polygons...")
    start_time = time.time()

    polygons = []
    values = []
    areas_ha = []
    min_dates = []
    max_dates = []
    date_diffs = []

    for geom, cluster_id in shapes(filtered_labels, mask=(filtered_labels >= 0),
                                   connectivity=connectivity,
                                   transform=transform):
        poly = shape(geom)
        area_ha = poly.area / 10000

        cluster_mask = (cluster_labels == cluster_id)
        cluster_dates = raster_data[cluster_mask]

        if cluster_dates.size > 0:
            unique_dates, counts = np.unique(cluster_dates, return_counts=True)
            most_common_date = unique_dates[np.argmax(counts)]

            # Calculate min, max dates and difference
            min_date = int(np.min(unique_dates))
            max_date = int(np.max(unique_dates))

            # Parse dates and calculate difference in days
            min_date_obj = parse_date_value(min_date)
            max_date_obj = parse_date_value(max_date)

            if min_date_obj and max_date_obj:
                date_diff_days = (max_date_obj - min_date_obj).days
            else:
                date_diff_days = -9999  # Use nodata value for invalid dates

            polygons.append(poly)
            values.append(int(most_common_date))
            areas_ha.append(area_ha)
            min_dates.append(min_date)
            max_dates.append(max_date)
            date_diffs.append(date_diff_days)

    elapsed = time.time() - start_time
    print(f"Polygon conversion took {elapsed:.2f} seconds")
    print(f"Created {len(polygons)} polygons after filtering\n")

    gdf = gpd.GeoDataFrame({
        'date_value': values,
        'area_ha': areas_ha,
        'min_date': min_dates,
        'max_date': max_dates,
        'date_diff_days': date_diffs
    }, geometry=polygons, crs=crs)

    def format_date(x):
        date_obj = parse_date_value(x)
        return date_obj.strftime('%Y-%m-%d') if date_obj else 'Invalid'

    gdf['date_formatted'] = gdf['date_value'].apply(format_date)
    gdf['min_date_formatted'] = gdf['min_date'].apply(format_date)
    gdf['max_date_formatted'] = gdf['max_date'].apply(format_date)

    print(f"Saving {len(gdf)} polygons to: {output_vector}")

    start_time = time.time()
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
        print(f"\nDate Difference Statistics:")
        valid_diffs = gdf[gdf['date_diff_days'] != -9999]['date_diff_days']
        if len(valid_diffs) > 0:
            print(f"  Average date difference: {valid_diffs.mean():.2f} days")
            print(f"  Minimum date difference: {valid_diffs.min()} days")
            print(f"  Maximum date difference: {valid_diffs.max()} days")
            print(f"  Polygons with date span > 0: {(valid_diffs > 0).sum()}")

def main():
    overall_start = time.time()
    print("="*60)
    print("RASTER TO POLYGON CONVERTER - GRAPH ALGORITHM")
    print("="*60 + "\n")

    if not os.path.exists(input_raster):
        print(f"Error: Input raster file does not exist: {input_raster}")
        sys.exit(1)

    output_dir = os.path.dirname(output_vector)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Procurar todos os rasters na pasta de entrada
    raster_list = sorted(glob.glob(os.path.join(input_raster, "*.tif")))

    if not raster_list:
        print(f"No .tif files found in {input_raster}")
        sys.exit(0)

    print(f"Found {len(raster_list)} rasters to process.\n")

    for i, raster_path in enumerate(raster_list, 1):
        raster_name = os.path.basename(raster_path)
        base_name = os.path.splitext(raster_name)[0]
        output_path = os.path.join(output_vector, f"{base_name}.gpkg")

        # Ensure the output folder exists
        output_folder = os.path.dirname(output_path)
        if not os.path.exists(output_folder):
            os.makedirs(output_folder, exist_ok=True)

        print(f"[{i}/{len(raster_list)}] Processing {raster_name}...")

        try:
            raster_to_polygons(
                input_raster=raster_path,
                output_vector=output_path,
                band_1=band_1,
                band_2=band_2,
                date_range_days=date_range_days,
                min_area_ha=min_area_ha,
                nodata_value=nodata_value,
                connectivity=connectivity
            )
            print(f"Completed: {output_path}\n")

        except Exception as e:
            print(f"Error processing raster: {raster_name}: {e}\n")

        overall_elapsed = time.time() - overall_start
        print("\n" + "="*60)
        print(f"TOTAL EXECUTION TIME: {overall_elapsed:.2f} seconds ({overall_elapsed/60:.2f} minutes)")
        print("="*60)
        print(f"\nProcessing completed successfully!")
        print(f"Output saved to: {output_vector}")

if __name__ == "__main__":
    main()
