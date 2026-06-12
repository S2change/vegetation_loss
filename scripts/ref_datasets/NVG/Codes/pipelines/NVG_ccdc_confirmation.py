import geopandas as gpd
import numpy as np
from pathlib import Path


def flag_ccdc_results(input_shp: str, output_shp: str) -> gpd.GeoDataFrame:
    """
    Flag polygons where CCDC results are considered correct.

    Two cases are considered as "correct" CCDC results:

    1) Case 1:
       ``drop_date`` is not NULL and ``ECCD1``, ``ECCD2`` and ``NC`` are NULL.
    2) Case 2:
       ``drop_date`` is NULL and ``NC`` equals 1.

    Parameters
    ----------
    input_shp : str
        Path to the input Navigator shapefile.
    output_shp : str
        Path for the output shapefile with the new fields.

    Returns
    -------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame with the new ``ccdc_flag`` and ``ccdc_ok`` fields added.
    """
    # 1. Read the Navigator shapefile
    gdf = gpd.read_file(input_shp)

    # 2. Define the two conditions where CCDC results are considered correct

    # Case 1: drop_date is NOT NULL and ECCD1, ECCD2 and NC are NULL
    cond1 = (
        gdf["drop_date"].notna()
        & gdf["ECCD1"].isna()
        & gdf["ECCD2"].isna()
        & gdf["NC"].isna()
    )

    # Case 2: drop_date is NULL and NC = 1
    cond2 = (
        gdf["drop_date"].isna()
        & (gdf["NC"] == 1)
    )

    # # 3. Create a new field (short name because of shapefile 10-char limit)
    # gdf["ccdc_flag"] = np.select(
    #     [cond1, cond2],
    #     [1, 2],
    #     default=0
    # ) Please note: This section can be activated if is needed to obtain the two conditions separately

    # Optional: simple boolean "correct / not correct"
    gdf["ccdc_ok"] = cond1 | cond2

    # 4. Save result as a new shapefile
    gdf.to_file(output_shp)

    return gdf
