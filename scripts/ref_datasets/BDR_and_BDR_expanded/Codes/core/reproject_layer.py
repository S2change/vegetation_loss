import geopandas as gpd
from pathlib import Path
from typing import Optional, Union


def reproject_layer(
    input_path: Union[str, Path, gpd.GeoDataFrame],
    output_path: Union[str, Path],
    target_crs: str = "EPSG:3763",
    layer_name: Optional[str] = None,
) -> gpd.GeoDataFrame:
    """
    Reproject a vector spatial layer to a target coordinate reference system
    and export the result as a GeoPackage.

    This function supports two input modes:

    1) **File path input** (e.g. ``.shp`` or ``.gpkg``):
       the layer is read from disk using GeoPandas.

    2) **In-memory input** (``geopandas.GeoDataFrame``):
       the GeoDataFrame is used directly, without reading from disk.

    In both cases, the output is written as a GeoPackage (``.gpkg``). If the
    output path does not end with ``.gpkg``, the extension is enforced.

    Parameters
    ----------
    input_data : str or pathlib.Path or geopandas.GeoDataFrame
        Input vector layer, either as a file path (Shapefile/GeoPackage) or
        as an in-memory GeoDataFrame.
    output_path : str or pathlib.Path
        Destination path for the output GeoPackage. The ``.gpkg`` extension
        is enforced if missing.
    target_crs : str, optional
        Target CRS expressed as an EPSG code or PROJ string.
        Default is ``"EPSG:3763"``.
    layer_name : str, optional
        Name of the layer to be written inside the GeoPackage. If not
        provided, the stem of ``output_path`` is used.

    Returns
    -------
    gdf_out : geopandas.GeoDataFrame
        Reprojected GeoDataFrame. If the input is already in ``target_crs``,
        a copy is returned.

    Raises
    ------
    FileNotFoundError
        If ``input_data`` is a path and the file does not exist.
    ValueError
        If the input layer has no CRS defined.

    Notes
    -----
    - Reprojection is performed only when required.
    - Output is always written as GeoPackage for consistency and robustness.
    """
    output_path = Path(output_path)

    # Enforce .gpkg extension
    if output_path.suffix.lower() != ".gpkg":
        output_path = output_path.with_suffix(".gpkg")

    # Default layer name
    if layer_name is None:
        layer_name = output_path.stem

    # --- Read input depending on type ---
    if isinstance(input_path, gpd.GeoDataFrame):
        gdf = input_path
    else:
        input_path = Path(input_path)
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")
        gdf = gpd.read_file(input_path)

    # Validate CRS
    if gdf.crs is None:
        raise ValueError("Input layer has no CRS defined.")

    # Reproject only if needed
    if gdf.crs.to_string() != target_crs:
        gdf_out = gdf.to_crs(target_crs)
    else:
        gdf_out = gdf.copy()

    # Write output
    gdf_out.to_file(output_path, layer=layer_name, driver="GPKG")

    return gdf_out
