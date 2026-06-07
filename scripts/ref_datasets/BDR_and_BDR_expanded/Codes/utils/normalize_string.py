import geopandas as gpd
from pathlib import Path
from typing import List, Union
import unicodedata


def _normalize_single_string(text: str) -> str:
    """
    Normalize a string to lowercase and remove accents/diacritics.
    """
    if text is None:
        return None

    if not isinstance(text, str):
        return text

    s = str(text).strip()
    if s == "":
        return s

    # lowercase
    s = s.lower()

    # remove accents/diacritics
    normalized = unicodedata.normalize("NFD", s)
    s_ascii = "".join(ch for ch in normalized if not unicodedata.combining(ch))

    return s_ascii


def normalize_string(obj: Union[str, gpd.GeoDataFrame]):
    """
    Backwards-compatible normalization entry point.

    - If obj is a string: normalize and return a string.
    - If obj is a GeoDataFrame: normalize all text/object columns in-place and return the GeoDataFrame.
    - Otherwise: return obj unchanged.
    """
    # Case 1: string
    if isinstance(obj, str) or obj is None:
        return _normalize_single_string(obj)

    # Case 2: GeoDataFrame
    if isinstance(obj, gpd.GeoDataFrame):
        gdf = obj
        text_cols: List[str] = gdf.select_dtypes(include=["object"]).columns.tolist()

        for col in text_cols:
            gdf[col] = gdf[col].apply(_normalize_single_string)

        return gdf

    # Case 3: any other type
    return obj


def normalize_text_fields(input_shp: str, output_shp: str) -> gpd.GeoDataFrame:
    """
    Normalize all text fields in a shapefile:
    - convert to lowercase
    - remove accents/diacritics
    """
    gdf = gpd.read_file(input_shp)

    text_cols: List[str] = gdf.select_dtypes(include=["object"]).columns.tolist()
    print("Text columns found:", text_cols)

    for col in text_cols:
        print(f"Normalizing column: {col}")
        gdf[col] = gdf[col].apply(_normalize_single_string)

    gdf.to_file(output_shp)
    return gdf



