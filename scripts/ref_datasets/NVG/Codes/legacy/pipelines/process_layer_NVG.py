import geopandas as gpd
import pandas as pd
from pathlib import Path

def _nvg_prepare_pixels(
    input_pixels_shp: str,
    admin_areas_shp: str,
    target_crs: str = "EPSG:3763",
) -> tuple[gpd.GeoDataFrame, str, str, list[str]]:
    """
    Read and harmonize NVG pixel layer at feature level.

    The function standardizes core fields and assigns the administrative code
    ('Pi_dicofre') from the administrative polygons layer. The administrative
    assignment is performed using a centroid-based spatial join to avoid
    duplicate matches that may occur with polygon intersection joins.

    Parameters
    ----------
    input_pixels_shp : str
        Path to the input NVG pixel polygons.
    admin_areas_shp : str
        Path to the administrative polygons layer (must contain 'dtmnfr').
    target_crs : str, default "EPSG:3763"
        CRS to reproject the pixel layer to.

    Returns
    -------
    gdf : geopandas.GeoDataFrame
        Harmonized pixel GeoDataFrame.
    geometry_name : str
        Name of the geometry column.
    id_col : str
        Name of the standardized Id column (always "Id").
    harmonized_pixel_fields : list[str]
        List of standardized/harmonized attribute fields (no geometry).
    """
    pixels_path = Path(input_pixels_shp)
    admin_path = Path(admin_areas_shp)

    if not pixels_path.exists():
        raise FileNotFoundError(f"Input NVG pixel file not found: {pixels_path}")

    # 1) Read NVG pixels
    gdf = gpd.read_file(pixels_path)

    if gdf.crs is None:
        raise ValueError("NVG pixel layer has no CRS defined.")

    if gdf.crs.to_string() != target_crs:
        gdf = gdf.to_crs(target_crs)

    gdf = gdf.reset_index(drop=True)
    geometry_name = gdf.geometry.name

    # 2) Basic renaming: Id / id, Id_gleba, Data_0, Data_1
    rename_map: dict[str, str] = {}

    # Id
    if "Id" in gdf.columns:
        id_col = "Id"
    elif "id" in gdf.columns:
        rename_map["id"] = "Id"
        id_col = "Id"
    else:
        raise ValueError("Neither 'id' nor 'Id' was found in the NVG layer.")

    # Id_gleba (optional)
    if "id_gleba" in gdf.columns and "Id_gleba" not in gdf.columns:
        rename_map["id_gleba"] = "Id_gleba"

    # dates: data0 / data1 → Data_0 / Data_1
    if "data0" in gdf.columns and "Data_0" not in gdf.columns:
        rename_map["data0"] = "Data_0"
    if "data1" in gdf.columns and "Data_1" not in gdf.columns:
        rename_map["data1"] = "Data_1"

    if rename_map:
        gdf = gdf.rename(columns=rename_map)

    # 3) Normalize Data_0 / Data_1 to 'YYYY-MM-DD' strings or None
    for date_field in ["Data_0", "Data_1"]:
        if date_field in gdf.columns:
            dt = pd.to_datetime(gdf[date_field], errors="coerce")
            s = dt.dt.strftime("%Y-%m-%d")
            s[dt.isna()] = None
            gdf[date_field] = s

    # 4) Pixel-level harmonized fields
    gdf["Pix_area_ha"] = gdf.geometry.area / 10_000.0
    gdf["Src"] = "Nvg"

    if "Uid" not in gdf.columns:
        gdf["Uid"] = "Nvg_" + (gdf.index + 1).astype(str).str.zfill(7)

    if "Chg_type" not in gdf.columns:
        gdf["Chg_type"] = None

    if "ccdc_ok" not in gdf.columns:
        raise ValueError(
            "Field 'ccdc_ok' not found in NVG layer. "
            "Run the CCDC flag script first to create 'ccdc_ok'."
        )

    gdf["Q_flag"] = gdf["ccdc_ok"].map({True: "Ok", False: "Ccdc invalid"})
    gdf["Q_flag"] = gdf["Q_flag"].fillna("Ccdc invalid")

    # 5) Pi_dicofre from administrative layer (centroid-based join)
    if not admin_path.exists():
        raise FileNotFoundError(f"Administrative layer not found: {admin_path}")

    admin = gpd.read_file(admin_path)

    if admin.crs is None:
        raise ValueError("Administrative layer has no CRS defined.")
    if admin.crs.to_string() != gdf.crs.to_string():
        admin = admin.to_crs(gdf.crs)

    if "dtmnfr" not in admin.columns:
        raise ValueError("No 'dtmnfr' field found in administrative layer.")

    admin_min = admin[["dtmnfr", "geometry"]].copy()

    # Build centroid points (1-to-1 expected)
    cent = gdf[["Uid", geometry_name]].copy()
    cent = cent.rename(columns={geometry_name: "geometry"})
    cent = gpd.GeoDataFrame(cent, geometry="geometry", crs=gdf.crs)
    cent["geometry"] = cent.geometry.centroid

    # Primary join: centroid within admin polygon
    j = gpd.sjoin(
        cent,
        admin_min,
        how="left",
        predicate="within",
    )

    # Validate uniqueness: a centroid should match at most one admin polygon
    dup = j.duplicated(subset=["Uid"], keep=False)
    if bool(dup.any()):
        n_dup = int(dup.sum())
        raise ValueError(
            "Administrative layer has overlapping polygons: "
            f"{n_dup} centroid matches are not unique."
        )

    # Fallback: assign nearest admin polygon for unmatched centroids
    missing = j["dtmnfr"].isna()
    if bool(missing.any()):
        j_near = gpd.sjoin_nearest(
            cent.loc[missing].copy(),
            admin_min,
            how="left",
            distance_col=None,
        )
        j.loc[missing, "dtmnfr"] = j_near["dtmnfr"].to_numpy()

    # Ensure assignment is complete
    if bool(j["dtmnfr"].isna().any()):
        raise ValueError(
            "Some centroids could not be assigned an administrative code (dtmnfr)."
        )

    # Map back to polygons without changing row count
    uid_to_admin = j[["Uid", "dtmnfr"]].set_index("Uid")["dtmnfr"]
    gdf["Pi_dicofre"] = gdf["Uid"].map(uid_to_admin)

    # 6) List of harmonized pixel fields
    harmonized_pixel_fields = [
        "Src",
        "Id",
        "Uid",
        "Id_gleba" if "Id_gleba" in gdf.columns else None,
        "Data_0" if "Data_0" in gdf.columns else None,
        "Data_1" if "Data_1" in gdf.columns else None,
        "Chg_type",
        "Pix_area_ha",
        "Q_flag",
        "Pi_dicofre" if "Pi_dicofre" in gdf.columns else None,
    ]
    harmonized_pixel_fields = [f for f in harmonized_pixel_fields if f is not None]

    return gdf, geometry_name, id_col, harmonized_pixel_fields


def _compute_null_stats_from_pixels(
    gdf_pixels: gpd.GeoDataFrame,
    id_col: str,
) -> pd.DataFrame:
    """
    Compute null statistics from original pixels, before dissolve.

    For each Id:
      - Pix_total      : total number of pixels
      - Pix_null       : number of pixels with Data_1 = NULL
      - Pix_null_ratio : Pix_null / Pix_total
    """
    if "Data_1" not in gdf_pixels.columns:
        raise ValueError("Field 'Data_1' not found in NVG pixels.")

    null_mask = gdf_pixels["Data_1"].isna()

    grouped = (
        gdf_pixels.assign(_is_null=null_mask)
        .groupby(id_col)
        .agg(
            Pix_total=("_is_null", "size"),
            Pix_null=("_is_null", "sum"),
        )
        .reset_index()
    )

    grouped["Pix_null_ratio"] = grouped["Pix_null"] / grouped["Pix_total"]

    return grouped


def _build_stats_from_dissolved(
    merged_dissolved: gpd.GeoDataFrame,
    id_col: str,
    null_stats: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Build per-Id statistics from dissolved layer (LOCAL).

    For each Id:
      - Area_ha        : total area (sum of Pix_area_ha)
      - N_dates_valid  : number of distinct Data_1 with area
      - Area_iqr_ha    : Q3 - Q1 of area per date (hectares), within the Id
      - Conf_lvl       : based on local relative variability:
                           iqr_rel = Area_iqr_ha / median(area_per_date)
                           iqr_rel <= 0.10 -> 'High'
                           0.10 < iqr_rel <= 0.30 -> 'Medium'
                           iqr_rel >  0.30 -> 'Low'
                         If N_dates_valid == 0 -> None

    If null_stats is provided, add:
      - Pix_total, Pix_null, Pix_null_ratio

    Override:
      If all valid-date polygons in an Id have Q_flag != 'Ok', Conf_lvl = None.
    """

    total_area = (
        merged_dissolved
        .groupby(id_col, as_index=False)["Pix_area_ha"]
        .sum()
        .rename(columns={"Pix_area_ha": "Area_ha"})
    )

    gdf_with_date = merged_dissolved[merged_dissolved["Data_1"].notna()].copy()

    if gdf_with_date.empty:
        stats = total_area.copy()
        stats["N_dates_valid"] = 0
        stats["Area_iqr_ha"] = 0.0
        stats["Conf_lvl"] = None

        if null_stats is not None:
            stats = stats.merge(null_stats, on=id_col, how="left")
            stats["Pix_total"] = stats["Pix_total"].fillna(0).astype(int)
            stats["Pix_null"] = stats["Pix_null"].fillna(0).astype(int)
            stats["Pix_null_ratio"] = stats["Pix_null_ratio"].fillna(0.0)

        return stats

    area_by_date = (
        gdf_with_date
        .groupby([id_col, "Data_1"], as_index=False)["Pix_area_ha"]
        .sum()
    )

    n_dates = (
        area_by_date
        .groupby(id_col)["Data_1"]
        .nunique()
        .reset_index()
        .rename(columns={"Data_1": "N_dates_valid"})
    )

    dist = (
        area_by_date
        .groupby(id_col)["Pix_area_ha"]
        .agg(
            q1=lambda x: x.quantile(0.25),
            q3=lambda x: x.quantile(0.75),
            med="median",
        )
        .reset_index()
    )
    dist["Area_iqr_ha"] = dist["q3"] - dist["q1"]

    stats = (
        total_area
        .merge(n_dates, on=id_col, how="left")
        .merge(dist[[id_col, "Area_iqr_ha", "med"]], on=id_col, how="left")
    )

    stats["N_dates_valid"] = stats["N_dates_valid"].fillna(0).astype(int)
    stats["Area_iqr_ha"] = stats["Area_iqr_ha"].fillna(0.0)
    stats["med"] = stats["med"].fillna(0.0)

    def _conf_from_row(row) -> str | None:
        if row["N_dates_valid"] == 0:
            return None

        iqr = float(row["Area_iqr_ha"]) if pd.notna(row["Area_iqr_ha"]) else 0.0
        med = float(row["med"]) if pd.notna(row["med"]) else 0.0

        if med <= 0:
            return "High" if iqr == 0 else "Low"

        iqr_rel = iqr / med
        if iqr_rel <= 0.10:
            return "High"
        elif iqr_rel <= 0.30:
            return "Medium"
        else:
            return "Low"

    stats["Conf_lvl"] = stats.apply(_conf_from_row, axis=1)
    stats.loc[stats["N_dates_valid"] == 0, "Conf_lvl"] = None

    if "Q_flag" in merged_dissolved.columns:
        valid_q = merged_dissolved[merged_dissolved["Data_1"].notna()].copy()
        if not valid_q.empty:
            valid_q["is_ok"] = valid_q["Q_flag"] == "Ok"
            quality = valid_q.groupby(id_col)["is_ok"].sum().reset_index(name="n_ok")
            stats = stats.merge(quality, on=id_col, how="left")
            stats["n_ok"] = stats["n_ok"].fillna(0)
            stats.loc[(stats["N_dates_valid"] > 0) & (stats["n_ok"] == 0), "Conf_lvl"] = None
            stats = stats.drop(columns=["n_ok"])

    if null_stats is not None:
        stats = stats.merge(null_stats, on=id_col, how="left")
        stats["Pix_total"] = stats["Pix_total"].fillna(0).astype(int)
        stats["Pix_null"] = stats["Pix_null"].fillna(0).astype(int)
        stats["Pix_null_ratio"] = stats["Pix_null_ratio"].fillna(0.0)

    return stats.drop(columns=["med"])


def nvg_harmonize_and_dissolve_by_data1(
    input_pixels_shp: str,
    admin_areas_shp: str,
    out_pixels_harmonized_shp: str,
    out_dissolved_with_stats_shp: str,
    out_stats_csv: str,
    target_crs: str = "EPSG:3763",
    keep_only_harmonized_pixels: bool = False,
    keep_only_harmonized_dissolved: bool = False,
) -> None:
    """
    Harmonize NVG pixels and dissolve by (Id, Data_1).

    Pixel level:
      - Src, Id, Uid, Data_0, Data_1, Chg_type, Pix_area_ha,
        Q_flag, Pi_dicofre, Id_gleba (optional).

    Dissolve:
      - Data_1 not null → dissolve by (Id, Data_1)
      - Data_1 null     → kept as single polygons
      - Pix_area_ha is recomputed after dissolve
      - Q_flag in dissolved polygons is derived from ccdc_ok in the group

    Stats per Id (using dissolved + original pixels):
      - Area_ha        : total area
      - N_dates_valid  : number of distinct dates (Data_1)
      - Area_iqr_ha    : Q3 - Q1 of area by date (ha)
      - Conf_lvl       : confidence from Area_iqr_ha (High/Medium/Low/None)
      - Pix_total      : pixel count (before dissolve)
      - Pix_null       : pixel count with Data_1 = NULL (before dissolve)
      - Pix_null_ratio : Pix_null / Pix_total

    In the dissolved output, Area_ha / N_dates_valid / Area_iqr_ha / Conf_lvl
    are only filled for polygons with Data_1 not null.
    """
    out_pixels_path = Path(out_pixels_harmonized_shp)
    out_dissolved_path = Path(out_dissolved_with_stats_shp)
    out_stats_path = Path(out_stats_csv)

    # 1) Prepare pixels (harmonized)
    gdf, geometry_name, id_col, harmonized_pixel_fields = _nvg_prepare_pixels(
        input_pixels_shp=input_pixels_shp,
        admin_areas_shp=admin_areas_shp,
        target_crs=target_crs,
    )

    # 1bis) Null statistics from original pixels (before dissolve)
    null_stats = _compute_null_stats_from_pixels(gdf, id_col=id_col)

    # 2) Save pixel-level output
    out_pixels_path.parent.mkdir(parents=True, exist_ok=True)
    if keep_only_harmonized_pixels:
        gdf_pixels_out = gdf[harmonized_pixel_fields + [geometry_name]].copy()
    else:
        gdf_pixels_out = gdf.copy()
    gdf_pixels_out.to_file(out_pixels_path)

    # 3) Dissolve by (Id, Data_1)
    if "Data_1" not in gdf.columns:
        raise ValueError("Field 'Data_1' not found in NVG layer (after harmonization).")

    has_date = gdf["Data_1"].notna()
    gdf_with_date = gdf[has_date].copy()
    gdf_no_date = gdf[~has_date].copy()

    print(f"Pixels with Data_1 (for dissolve): {len(gdf_with_date)}")
    print(f"Pixels without Data_1 (kept as-is): {len(gdf_no_date)}")

    group_fields = [id_col, "Data_1"]

    # group-level ccdc_ok aggregation
    if len(gdf_with_date) > 0:
        ok_series = gdf_with_date["ccdc_ok"].fillna(False).astype(bool)
        ccdc_agg = (
            gdf_with_date.assign(_ok=ok_series)
            .groupby(group_fields, as_index=False)["_ok"]
            .all()
            .rename(columns={"_ok": "ccdc_ok_all"})
        )
    else:
        ccdc_agg = None

    if len(gdf_with_date) > 0:
        gdf_diss = gdf_with_date.dissolve(
            by=group_fields,
            as_index=False,
            aggfunc="first",
        )
        # area of dissolved polygons
        gdf_diss["Pix_area_ha"] = gdf_diss.geometry.area / 10_000.0

        # recompute Q_flag using aggregated ccdc_ok_all
        if ccdc_agg is not None:
            if "Q_flag" in gdf_diss.columns:
                gdf_diss = gdf_diss.drop(columns=["Q_flag"])

            gdf_diss = gdf_diss.merge(ccdc_agg, on=group_fields, how="left")
            gdf_diss["Q_flag"] = gdf_diss["ccdc_ok_all"].map(
                {True: "Ok", False: "Ccdc invalid"}
            )
            gdf_diss["Q_flag"] = gdf_diss["Q_flag"].fillna("Ccdc invalid")
            gdf_diss = gdf_diss.drop(columns=["ccdc_ok_all"])
    else:
        gdf_diss = gdf_with_date.copy()

    # combine dissolved (with date) and undissolved (no date)
    merged_dissolved = gpd.GeoDataFrame(
        pd.concat([gdf_diss, gdf_no_date], ignore_index=True),
        crs=gdf.crs,
    )

    # 4) Stats per Id
    stats = _build_stats_from_dissolved(
        merged_dissolved,
        id_col=id_col,
        null_stats=null_stats,
    )

    # save stats CSV
    out_stats_path.parent.mkdir(parents=True, exist_ok=True)
    stats.to_csv(out_stats_path, index=False)

    # join stats back to dissolved layer
    merged_with_stats = merged_dissolved.merge(stats, on=id_col, how="left")

    # 5) For polygons with Data_1 = NULL, remove date-based stats
    mask_has_valid_date = merged_with_stats["Data_1"].notna()
    mask_no_valid_date = ~mask_has_valid_date

    for col in ["Area_ha", "N_dates_valid", "Area_iqr_ha", "Conf_lvl"]:
        if col in merged_with_stats.columns:
            merged_with_stats.loc[mask_no_valid_date, col] = None

    # 6) Optional: keep only harmonized + stats + geometry
    harmonized_dissolved_fields = harmonized_pixel_fields + [
        "Area_ha",
        "N_dates_valid",
        "Area_iqr_ha",
        "Conf_lvl",
        "Pix_total",
        "Pix_null",
        "Pix_null_ratio",
    ]

    out_dissolved_path.parent.mkdir(parents=True, exist_ok=True)

    if keep_only_harmonized_dissolved:
        cols = [c for c in harmonized_dissolved_fields if c in merged_with_stats.columns]
        merged_out = merged_with_stats[cols + [geometry_name]]
    else:
        merged_out = merged_with_stats

    merged_out.to_file(out_dissolved_path)

    print("NVG harmonize + dissolve by (Id, Data_1) finished.")
    print("  Harmonized pixels:   ", out_pixels_path)
    print("  Dissolved + stats:   ", out_dissolved_path)
    print("  Stats CSV:           ", out_stats_path)
