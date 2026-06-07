import geopandas as gpd
from pathlib import Path


def add_overlap_flag_to_layer(input_shp: str,
                              output_shp: str,
                              min_overlap_area: float = 0.0) -> gpd.GeoDataFrame:
    """
    Add an overlap flag field to a polygon layer.

    This function emulates a "must not overlap" topology rule for a single
    polygon layer. It detects overlaps between different features in the
    same layer and adds a boolean field ``Tplgy_error`` indicating whether
    each polygon overlaps any other polygon by more than a given area
    threshold.

    Please note that the name of the field 'Tplgy_error' is because the
    results will be evaluated against the topology rules. Some of these
    overlaps are not errors, because they represent two fire events that
    happened in the same place at different times. But other overlaps are
    errors, because they represent digitization errors.

    Parameters
    ----------
    input_shp : str
        Path to the input polygon shapefile.
    output_shp : str
        Path to the output shapefile with the new ``Tplgy_error`` field.
    min_overlap_area : float, optional
        Minimum overlap area (in layer CRS units, usually m²) to be
        considered as a real overlap. Overlaps with smaller area are
        ignored. Default is 0.0.

    Returns
    -------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame of the input layer with the new field ``Tplgy_error``.
    """
    # 1. Read input polygons
    gdf = gpd.read_file(input_shp)

    # 2. Fix invalid geometries (if any) with a zero-width buffer
    gdf["geometry"] = gdf.geometry.buffer(0)

    # 3. Create a unique ID for each feature
    gdf = gdf.reset_index(drop=True)
    gdf["fid"] = gdf.index

    # 4. Overlay the layer with itself to find intersections
    #    We only keep intersections between different features.
    inter = gpd.overlay(
        gdf[["fid", "geometry"]],
        gdf[["fid", "geometry"]],
        how="intersection"
    )

    # Keep only intersections between different polygons
    inter = inter[inter["fid_1"] != inter["fid_2"]].copy()

    # 5. Compute area of the overlapping geometries
    inter["overlap_area"] = inter.geometry.area

    # 6. Apply minimum overlap area filter (if requested)
    if min_overlap_area > 0:
        inter = inter[inter["overlap_area"] > min_overlap_area].copy()

    # 7. Collect IDs of polygons that have any overlap
    overlapped_ids = set(inter["fid_1"]).union(set(inter["fid_2"]))

    # 8. Create the flag field in the original layer
    #    Tplgy_error = True if polygon overlaps any other polygon
    gdf["Tplgy_error"] = gdf["fid"].isin(overlapped_ids)

    # 9. Save to a new shapefile (including the new field)
    gdf.to_file(output_shp)

    return gdf

