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

## SCRIPT CONFIGS ##
##################################

input_raster = "/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/personal_tests/09_optimized_test_20180101_to_20211231.tif"  # Path to input raster TIFF file
output_vector = "/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/personal_tests/14_graph_first_test.gpkg"  # Path to output vector file

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

def date_to_days(date_val):
    """Convert YYYYMMDD to days since reference date (2000-01-01)."""
    if date_val == 0 or date_val == -9999:
        return 0
    try:
        dt = datetime.strptime(str(int(date_val)), '%Y%m%d')
        ref = datetime(2000, 1, 1)
        return (dt - ref).days
    except (ValueError, TypeError):
        return 0

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
    """
    Group pixel values based on spatial proximity and temporal similarity using graph algorithm.

    Args:
        raster_array: numpy array with date values in YYYYMMDD format
        date_range_days: number of days tolerance for grouping pixels
        connectivity: 4 for edge-only connectivity, 8 for edge+diagonal connectivity

    Returns:
        tuple: (labeled_array with cluster IDs, original_dates array)
    """
    if date_range_days == 0:
        # No clustering, return original array
        return raster_array, raster_array

    print(f"Starting spatial-temporal clustering (connectivity={connectivity}, date_range={date_range_days} days)...")

    # Convert YYYYMMDD dates to days since reference
    start_time = time.time()
    vectorized_date_to_days = np.vectorize(date_to_days)
    days_array = vectorized_date_to_days(raster_array)
    elapsed = time.time() - start_time
    print(f"  Date conversion took {elapsed:.2f} seconds")

    # Apply graph-based clustering
    start_time = time.time()
    labels, num_clusters = graph_based_clustering(days_array, tolerance=date_range_days, connectivity=connectivity)
    elapsed = time.time() - start_time
    print(f"  Graph-based clustering took {elapsed:.2f} seconds")
    print(f"  Found {num_clusters} clusters")

    # For each cluster, find the most common original date value and assign it
    start_time = time.time()
    result_array = np.zeros_like(raster_array)

    for cluster_id in range(num_clusters):
        cluster_mask = (labels == cluster_id)
        cluster_dates = raster_array[cluster_mask]

        if len(cluster_dates) > 0:
            # Use most common date in the cluster
            unique_dates, counts = np.unique(cluster_dates, return_counts=True)
            most_common_date = unique_dates[np.argmax(counts)]
            result_array[cluster_mask] = most_common_date

    elapsed = time.time() - start_time
    print(f"  Cluster post-processing took {elapsed:.2f} seconds")

    return result_array, labels

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

    # Group pixels by date range if specified (using graph-based clustering)
    if date_range_days > 0:
        raster_data, cluster_labels = create_spatial_temporal_groups(raster_data, date_range_days, connectivity)
    else:
        cluster_labels = raster_data

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
    print("RASTER TO POLYGON CONVERTER - GRAPH ALGORITHM")
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