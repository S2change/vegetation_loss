import geopandas as gpd
from shapely.geometry import box
from pathlib import Path
import pandas as pd


def points_to_s2_pixels(points_shp: str,
                        output_pixels_shp: str,
                        pixel_size: float = 10.0,
                        target_crs: str = "EPSG:32629") -> gpd.GeoDataFrame:
    """
    Convert Navigator / CCDC point centroids to Sentinel-2-like pixel polygons,
    snapping them to a regular grid, and save both:
    1) individual pixel polygons (one per original point), and
    2) a dissolved version by (id, data1), keeping features with data1 = NULL
       as individual polygons.

    Steps
    -----
    1) Read point layer.
    2) Reproject to a metric CRS (default: EPSG:32629).
    3) Snap point coordinates to a regular grid of size `pixel_size`.
    4) Replace each snapped point by a square polygon of size
       `pixel_size` × `pixel_size` centred on the grid node.
    5) Save all pixel polygons as `output_pixels_shp`.
    6) Split pixels into:
       - with data1 (data1 not null) → dissolve by (id, data1)
       - without data1 (data1 null) → kept as single pixels (no dissolve)
       Then merge both sets and save as `output_dissolved_shp`.

    Parameters
    ----------
    points_shp : str
        Path to the input point shapefile (Navigator / CCDC centroids).
        Must contain at least 'id' and 'data1'.
    output_pixels_shp : str
        Path to the output polygon shapefile with individual 10 m pixels
        (one polygon per original point).
    output_dissolved_shp : str var
        Path to the output polygon shapefile where:
        - features with data1 not null are dissolved by (id, data1)
        - features with data1 null are kept as individual polygons.
    pixel_size : float, optional
        Pixel size in CRS units (metres). For Sentinel-2, 10.0 is used.
    target_crs : str, optional
        Target CRS for processing. Default is 'EPSG:32629',
        the original CRS of the shapefile.

    Returns
    -------
    merged : geopandas.GeoDataFrame
        GeoDataFrame combining dissolved (id, data1) polygons and
        individual pixels with data1 = NULL, in the target CRS.
    """
    # 1. Read point layer
    gdf = gpd.read_file(points_shp)

    if gdf.crs is None:
        raise ValueError("Input layer has no CRS defined.")

    if "id" not in gdf.columns:
        raise ValueError("Field 'id' not found in the input layer.")
    if "data1" not in gdf.columns:
        raise ValueError("Field 'data1' not found in the input layer.")

    if gdf.geom_type.unique().tolist() != ["Point"]:
        print("Warning: geometry type is not Point only; continuing anyway.")

    # 2. Reproject to target CRS if needed
    if gdf.crs.to_string() != target_crs:
        gdf = gdf.to_crs(target_crs)

    # 3. Snap point centres to a regular grid
    pixel = float(pixel_size)
    half = pixel / 2.0

    # Use first point as grid origin for snapping
    x0 = gdf.geometry.iloc[0].x
    y0 = gdf.geometry.iloc[0].y

    xs = gdf.geometry.x
    ys = gdf.geometry.y

    # grid node for each point
    gdf["cx"] = x0 + ((xs - x0) / pixel).round() * pixel
    gdf["cy"] = y0 + ((ys - y0) / pixel).round() * pixel

    # 4. Build square pixels around snapped centres
    def center_to_square(row):
        x = row["cx"]
        y = row["cy"]
        return box(x - half, y - half, x + half, y + half)

    gdf_pixels = gdf.copy()
    gdf_pixels["geometry"] = gdf_pixels.apply(center_to_square, axis=1)

    # 5. Save individual pixel polygons (todos los píxeles, nada se pierde)
    gdf_pixels.to_file(output_pixels_shp)

    return gdf_pixels
