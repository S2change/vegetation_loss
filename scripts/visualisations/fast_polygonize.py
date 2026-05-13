'''
Gemini prompt: I have a large numpy array, where each value is the index of a cluster or -9999. I also have two other numpy arrays with the same shape with coordinates x and y of each value (imagine each numpy value is a pixel in a image) for a certain CRS. I want a fast algorithm to create a geopackage where each cluster becomes a polygon feature (fast polygonize algorithm)
'''

import numpy as np
import rasterio.features
from rasterio.transform import from_origin
import geopandas as gpd
from shapely.geometry import shape

def fast_polygonize(cluster_array, x_coords, y_coords, crs_string, output_file):
    # 1. Calculate the resolution (pixel size)
    # Assumes uniform spacing in your x/y arrays
    res_x = x_coords[1] - x_coords[0]
    res_y = y_coords[1] - y_coords[0] # Usually negative for North-up rasters
    
    # 2. Create the Affine Transform
    # from_origin(west, north, x_size, y_size)
    transform = from_origin(x_coords[0], y_coords[0], res_x, -res_y)

    # 3. Create a mask to ignore -9999
    mask = (cluster_array != -9999)

    # 4. Polygonize (The fast part)
    # This returns a generator of (dict_geometry, value)
    results = (
        {'properties': {'cluster_id': int(v)}, 'geometry': shape(s)}
        for s, v in rasterio.features.shapes(
            cluster_array.astype(np.int32), 
            mask=mask, 
            transform=transform
        )
    )

    # 5. Convert to GeoDataFrame and Save
    gdf = gpd.GeoDataFrame.from_features(list(results), crs=crs_string)
    
    # Dissolve to merge polygons with same cluster_id if they are non-contiguous
    # (Optional: remove if you want each patch as a separate feature)
    # gdf = gdf.dissolve(by='cluster_id').reset_index()

    gdf.to_file(output_file, driver="GPKG")
    print(f"Exported {len(gdf)} features to {output_file}")

# Example Usage:
# fast_polygonize(cluster_data, x_data, y_data, "EPSG:4326", "clusters.gpkg")