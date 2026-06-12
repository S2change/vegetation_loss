import geopandas as gpd
from pathlib import Path


def add_overlap_flag_to_layer(input_shp: str,
                              output_shp: str,
                              min_overlap_area: float = 0.0) -> gpd.GeoDataFrame:
    """
    Add an overlap flag field to a polygon layer.

    This function emulates a "must not overlap" topology rule for a single
    polygon layer. It detects overlaps between different features in the
    same layer and adds a boolean field ``tplgy_error`` indicating whether
    each polygon overlaps any other polygon by more than a given area
    threshold. Please note that the name of the field 'tplgy_error' is because the results will be evaluated
    against the topology rules. Some of these overlaps are not errors, because they represent 
    two events of fire that happened in the same place at the different times. But other overlaps are errors, because they represent
    digitalization errors.

    Parameters
    ----------
    input_shp : str
        Path to the input polygon shapefile (ICNF burned area layer).
    output_shp : str
        Path to the output shapefile with the new ``tplgy_error`` field.
    min_overlap_area : float, optional
        Minimum overlap area (in layer CRS units, usually m²) to be
        considered as a real overlap. Overlaps with smaller area are
        ignored. Default is 0.0.

    Returns
    -------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame of the input layer with the new field ``has_ovrlp``.
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
    #    tplgy_error = True if polygon overlaps any other polygon
    gdf["Tplgy_error"] = gdf["fid"].isin(overlapped_ids)

    # 9. Save to a new shapefile (including the new field)
    gdf.to_file(output_shp)

    return gdf


def process_icnf_folder(icnf_folder: str,
                        out_folder: str,
                        pattern: str = "ardida_*.shp",
                        min_overlap_area: float = 0.0) -> None:
    """
    Process all ICNF burned area shapefiles in a folder and add an overlap flag.

    For each shapefile whose name matches the given pattern (e.g.
    'ardida_2022.shp'), this function creates a new shapefile in the
    output folder with an additional field ``tplgy_error`` indicating
    whether each polygon overlaps any other polygon in the same layer
    by more than the specified area threshold.

    Parameters
    ----------
    icnf_folder : str
        Path to the folder containing the ICNF shapefiles (inputs).
    out_folder : str
        Path to the folder where the result shapefiles will be saved.
    pattern : str, optional
        Glob pattern to select the shapefiles to process.
        Default is 'ardida_*.shp'.
    min_overlap_area : float, optional
        Minimum overlap area (in layer CRS units, usually m²) to be
        considered as a real overlap. Overlaps with smaller area are
        ignored. Default is 0.0.

    Returns
    -------
    None
    """
    in_dir = Path(icnf_folder)
    out_dir = Path(out_folder)

    # Ensure output folder exists
    out_dir.mkdir(parents=True, exist_ok=True)

    for shp_path in in_dir.glob(pattern):
        print(f"Processing: {shp_path.name}")

        # Output filename: e.g. ardida_2022_overlap.shp in Results
        out_name = shp_path.stem + "_overlap.shp"
        out_path = out_dir / out_name

        add_overlap_flag_to_layer(
            input_shp=str(shp_path),
            output_shp=str(out_path),
            min_overlap_area=min_overlap_area
        )

        print(f"  → saved with overlap flag as: {out_path.name}")
