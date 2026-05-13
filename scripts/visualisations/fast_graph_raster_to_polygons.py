"""
Raster to Vector Polygon Converter using Graph Algorithm

This script converts a raster TIFF file to vector polygons with spatial-temporal clustering.

- Uses graph-based algorithm (networkx + scipy KDTree) to cluster pixels
- Combines spatial connectivity and temporal tolerance using graph edges
- Configurable connectivity (4-connectivity or 8-connectivity)

Features:
- Spatial-temporal clustering of pixels with tolerance-based date grouping
- Filtering polygons by minimum area
- Setting polygon values based on most common pixel value: perhaps replace mode by median (?) MC
"""

import os
import sys
from datetime import datetime
import time
import numpy as np
import rasterio
from rasterio.features import shapes # fast polygonize
from shapely.geometry import shape
import geopandas as gpd
import networkx as nx
from scipy.spatial import cKDTree
import glob
from datetime import datetime, timedelta
import pandas as pd

## SCRIPT CONFIGS ##
##################################

input_raster = r"H:\new_parquets_2017_2025\tabular\T29TNE\processed_outputs\rasters"
output_vector = r"H:\new_parquets_2017_2025\tabular\T29TNE\processed_outputs\vectors"
input_raster = r"C:\Users\Public\Documents\outputs_ROI\tabular\T29TQG\processed_outputs\rasters"
output_vector = r"C:\Users\Public\Documents\outputs_ROI\tabular\T29TQG\processed_outputs\vectors"
N_input_rasters=1 # None or integer # Number of rasters to process (set to None to process all), starting with the most recent (first in sorted list)

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
    '''
    Creates spatial-temporal groups in the raster array using a graph-based clustering algorithm.
    Pixels are clustered based on spatial connectivity and temporal similarity within a specified date range.
    
    Parameters:
    - raster_array: 2D numpy array with date values (e.g., in YYYYMMDD format)
    - date_range_days: Maximum number of days difference to consider pixels as part of the same cluster (default: 0, which means no temporal grouping)
    - connectivity: 4 for edge-only connectivity, 8 for edge+diagonal connectivity (default: 8)

    '''
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
    '''
    Converts a raster to polygons using a graph-based clustering algorithm for spatial-temporal grouping.
    The function reads a raster file, clusters pixels based on spatial connectivity and temporal similarity,    
    and then polygonizes the clusters while calculating statistics for each polygon.    
    Parameters:
    - input_raster: Path to the input raster file (TIFF)    
    - output_vector: Path to the output vector file (GPKG)    
    - band_1: First band index containing date values (default: 1)  
    - band_2: Second band index containing date values (default: 2)
    - date_range_days: Maximum number of days difference to consider pixels as part of the same cluster (default: 0, which means no temporal grouping)
    - min_area_ha: Minimum area in hectares for polygons to be retained (default: 0.5 ha)
    - nodata_value: Value in the raster that represents no data (default: -9999)
    - connectivity: 4 for edge-only connectivity, 8 for edge+diagonal connectivity (default: 8)
    Returns:
    - gdf: GeoDataFrame containing the resulting polygons and their attributes
    '''

    print(f"Processing raster: {input_raster}")

    start_time = time.time()
    with rasterio.open(input_raster) as src:
        band1 = src.read(band_1)
        band2 = src.read(band_2)
        transform = src.transform
        crs = src.crs
    
    # --- Vectorized Date Processing ---
    # We use a simplified vectorized approach for the mean date
    # Instead of looping, we use NumPy masks
    mask1 = (band1 != nodata_value) & (band1 != 0)
    mask2 = (band2 != nodata_value) & (band2 != 0)
    combined_mask = mask1 & mask2

    # Calculate mean dates in "days since 1970"
    days1 = date_to_days_vectorized(band1)
    days2 = date_to_days_vectorized(band2)
    mean_days = np.full(band1.shape, -9999, dtype=np.int32)
    mean_days[combined_mask] = (days1[combined_mask] + days2[combined_mask]) // 2

    # Cluster the labels
    if date_range_days > 0:
        _, cluster_labels = create_spatial_temporal_groups(band1, date_range_days, connectivity)
    else:
        cluster_labels = band1.copy()

    # --- Fast Area Filtering ---
    pixel_area_ha = abs(transform[0] * transform[4]) / 10000
    min_pixels = int(np.ceil(min_area_ha / pixel_area_ha))
    
    unique_labels, counts = np.unique(cluster_labels, return_counts=True)
    valid_clusters = unique_labels[(counts >= min_pixels) & (unique_labels >= 0)]
    
    # Mask out invalid clusters
    filtered_labels = np.where(np.isin(cluster_labels, valid_clusters), cluster_labels, -1)

    # --- THE FAST POLYGONIZATION STEP ---
    print("\nVectorizing all clusters in one pass...")
    start_poly = time.time()
    
    # 1. Generate all shapes in one pass
    shape_gen = shapes(filtered_labels.astype(np.int32), 
                       mask=(filtered_labels >= 0), 
                       transform=transform, 
                       connectivity=connectivity)
    
    # 2. Extract into a list of geometries and IDs
    results = [{"geometry": shape(s), "cluster_id": int(v)} for s, v in shape_gen]
    gdf = gpd.GeoDataFrame.from_features(results, crs=crs)

    # --- FAST STATISTICS CALCULATION (Avoids the loop) ---
    # We create a mapping of ClusterID -> Stats using Pandas/NumPy
    print("Calculating statistics for all clusters...")
    
    # Flatten arrays for tabular processing
    flat_labels = filtered_labels.flatten()
    flat_days = mean_days.flatten()
    
    # Only look at valid pixels
    valid_idx = flat_labels >= 0
    df_pixels = pd.DataFrame({
        'cluster_id': flat_labels[valid_idx],
        'days': flat_days[valid_idx]
    })

    # Group by cluster to get min, max, and mode (most common date)
    # Using 'days' because YYYYMMDD doesn't average/sort as naturally
    stats = df_pixels.groupby('cluster_id')['days'].agg(
        min_days='min',
        max_days='max',
        mode_days=lambda x: x.value_counts().index[0]
    ).reset_index()

    # Merge stats back to the GeoDataFrame
    gdf = gdf.merge(stats, on='cluster_id', how='left')

    # --- Convert "Days" back to YYYYMMDD for final output ---
    def days_to_yyyymmdd(days_series):
        ref_date = datetime(2000, 1, 1) # Align with your date_to_days logic
        return days_series.apply(lambda d: int((ref_date + timedelta(days=int(d))).strftime('%Y%m%d')))

    gdf['min_date'] = days_to_yyyymmdd(gdf['min_days'])
    gdf['max_date'] = days_to_yyyymmdd(gdf['max_days'])
    gdf['date_value'] = days_to_yyyymmdd(gdf['mode_days'])
    gdf['date_diff_days'] = gdf['max_days'] - gdf['min_days']
    gdf['area_ha'] = gdf.geometry.area / 10000

    # Clean up columns and save
    final_cols = ['date_value', 'area_ha', 'min_date', 'max_date', 'date_diff_days', 'geometry']
    gdf = gdf[final_cols]
    
    # Add formatted string dates
    gdf['date_formatted'] = pd.to_datetime(gdf['date_value'], format='%Y%m%d').dt.strftime('%Y-%m-%d')

    print(f"Polygon conversion took {time.time() - start_poly:.2f} seconds")
    gdf.to_file(output_vector, driver='GPKG')
    return gdf


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

    if N_input_rasters is not None: # MC added to allow processing only a subset of rasters, starting with the most recent (first in sorted list)
        raster_list = raster_list[:N_input_rasters]

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
