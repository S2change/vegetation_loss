from __future__ import annotations

import geopandas as gpd
import pandas as pd
from pathlib import Path

from Codes.utils.normalize_string import *
from Codes.core.reproject_layer import *


def _normalize_s2_tile_value(v) -> str | None:
    if v is None or pd.isna(v):
        return None
    s = str(v).strip()
    if s == "" or s.lower() in {"null", "nan", "none", "nat"}:
        return None
    return s.lower()


def _first_existing_col(cols: list[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in cols:
            return c
    return None


def harmonize_icnf_layer(
    input_shp: str,
    output_shp: str,
    report_xlsx: str,
    keep_only_harmonized: bool = False,
    *,
    layer_name: str | None = None,
) -> gpd.GeoDataFrame:
    """
    Harmonize an ICNF burned area layer (un año) by:

    1) Adding harmonized fields:
       - Src             : 'icnf'
       - Id              : numeric id, 1..N, no empty values
       - Uid             : unique string ID per feature ('icnf_XXXXXXX')
       - Data0           : start date (from DH_Inicio), in 'YYYY-MM-DD'
       - Data1           : end date   (from DH_Fim),    in 'YYYY-MM-DD'
       - Temp_eval_start : inicio del tiempo evaluado (por feature) = Data0
       - Temp_eval_end   : fin del tiempo evaluado (por feature)    = Data1
       - Chg_type        : 'fogo'
       - Area_ha         : polygon area in hectares
       - Validation_flag : quality flag derived from Tplgy_erro
       - Pi_dicofre      : viene del campo existente PI_DICOFRE
                           (si falta, se rellena con centroid join a dtmnfr)
       - S2_tile         : tile Sentinel-2 SOLO si el polígono cae
                           completamente dentro de un único tile

    2) Optionally keeping only the harmonized fields + geometry
       (if keep_only_harmonized=True).

    3) Writing:
       - output_shp puede ser .shp o .gpkg
         * si es .gpkg, se escribe en la capa `layer_name`
       - report_xlsx: change report
    """
    input_path = Path(input_shp)
    output_path = Path(output_shp)
    report_path = Path(report_xlsx)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # 1. Read ICNF layer
    gdf = gpd.read_file(input_path)

    if gdf.crs is None:
        raise ValueError("ICNF layer has no CRS defined. A projected CRS is required.")

    gdf = gdf.reset_index(drop=True)

    geometry_name = gdf.geometry.name
    original_cols = [c for c in gdf.columns if c != geometry_name]

    # Lists for the report
    added_fields_raw: list[str] = []

    # ----------------------------------------------------------
    # 1bis. Crear Data0 y Data1 a partir de DH_Inicio / DH_Fim
    #       y normalizar a 'YYYY-MM-DD'
    # ----------------------------------------------------------
    start_field = "DH_Inicio"
    end_field = "DH_Fim"

    if start_field not in gdf.columns:
        raise ValueError(f"Field '{start_field}' not found in ICNF layer.")
    if end_field not in gdf.columns:
        raise ValueError(f"Field '{end_field}' not found in ICNF layer.")

    start_dt = pd.to_datetime(gdf[start_field], errors="coerce")
    end_dt = pd.to_datetime(gdf[end_field], errors="coerce")

    gdf["Data0"] = start_dt.dt.strftime("%Y-%m-%d").where(start_dt.notna(), None)
    gdf["Data1"] = end_dt.dt.strftime("%Y-%m-%d").where(end_dt.notna(), None)

    added_fields_raw.extend(["Data0", "Data1"])

    # ----------------------------------------------------------
    # 1ter. Tiempo evaluado separado (como NVG)
    # ----------------------------------------------------------
    year_series = start_dt.dt.year
    year_series = year_series.where(year_series.notna(), end_dt.dt.year)

    years = pd.Series(year_series.dropna().unique())
    if years.empty:
        raise ValueError("Cannot infer evaluation year: both DH_Inicio and DH_Fim are NULL/invalid.")
    if len(years) != 1:
        raise ValueError(f"ICNF layer must contain a single evaluation year, found: {sorted(years.tolist())}")

    eval_year = int(years.iloc[0])

    gdf["Temp_eval_start"] = f"{eval_year:04d}-01-01"
    gdf["Temp_eval_end"] = f"{eval_year:04d}-12-31"
    added_fields_raw.extend(["Temp_eval_start", "Temp_eval_end"])

    # ----------------------------------------------------------
    # 2. Add harmonized fields
    # ----------------------------------------------------------

    # 2.0 Id
    gdf["Id"] = pd.Series(range(1, len(gdf) + 1), index=gdf.index)
    added_fields_raw.append("Id")

    # 2.1 SRC
    if "SRC" not in gdf.columns:
        gdf["SRC"] = "icnf"
        added_fields_raw.append("SRC")

    # 2.2 UID
    if "UID" not in gdf.columns:
        gdf["UID"] = "icnf_" + gdf.index.astype(str).str.zfill(7)
        added_fields_raw.append("UID")

    # 2.3 CHG_TYPE
    gdf["CHG_TYPE"] = "fogo"
    added_fields_raw.append("CHG_TYPE")

    # 2.4 AREA_HA
    gdf["AREA_HA"] = gdf.geometry.area / 10_000.0
    added_fields_raw.append("AREA_HA")

    # 2.5 Validation_flag from Tplgy_erro
    topo_field = "Tplgy_erro"
    if topo_field in gdf.columns:
        v_map = {
            0: "no topology error",
            1: "topology error",
            False: "no topology error",
            True: "topology error",
        }
        gdf["Validation_flag"] = gdf[topo_field].map(v_map).fillna("unknown")
    else:
        gdf["Validation_flag"] = "unknown"
    added_fields_raw.append("Validation_flag")

    # ----------------------------------------------------------
    # 4) Ensure PI_DICOFRE exists and has no NULLs (centroid join)
    # ----------------------------------------------------------
    admin_path = input_path.parent.parent.parent / "Data" / "NUTS" / "areas_administrativas.shp"
    if not admin_path.exists():
        raise FileNotFoundError(f"Administrative layer not found: {admin_path}")

    admin = gpd.read_file(admin_path)

    if admin.crs is None:
        raise ValueError("areas_administrativas.shp has no CRS defined.")
    if admin.crs != gdf.crs:
        admin = admin.to_crs(gdf.crs)

    cand_cols = [c for c in admin.columns if c.upper() == "DTMNFR" or c.lower() == "dtmnfr"]
    if not cand_cols:
        raise ValueError("Field 'dtmnfr' not found in areas_administrativas.shp")
    admin_code_field = cand_cols[0]

    admin_min = admin[[admin_code_field, "geometry"]].copy()

    if "PI_DICOFRE" not in gdf.columns:
        gdf["PI_DICOFRE"] = None

    to_fill = gdf["PI_DICOFRE"].isna() | (gdf["PI_DICOFRE"].astype(str).str.strip() == "")
    if to_fill.any():
        centroids = gdf.loc[to_fill, [geometry_name]].copy()
        centroids = centroids.set_geometry(geometry_name)
        centroids["geometry"] = centroids.geometry.centroid
        centroids["_idx"] = centroids.index

        join_cent = gpd.sjoin(
            centroids,
            admin_min,
            how="left",
            predicate="within",
        ).drop(columns=["index_right"], errors="ignore")

        join_cent = join_cent.drop_duplicates(subset="_idx")
        mapping = join_cent.set_index("_idx")[admin_code_field].to_dict()

        gdf.loc[to_fill, "PI_DICOFRE"] = gdf.loc[to_fill].index.map(mapping)

    # ----------------------------------------------------------
    # 4b) Assign S2_tile ONLY if polygon is fully within one tile
    # ----------------------------------------------------------
    tiles_path = input_path.parent.parent.parent / "Data" / "S2_tiles" / "sentinel2_tiles_PT_terra_tm06.shp"
    if not tiles_path.exists():
        raise FileNotFoundError(f"Sentinel-2 tiles layer not found: {tiles_path}")

    tiles = gpd.read_file(tiles_path)

    if tiles.crs is None:
        raise ValueError("sentinel2_tiles_PT_terra_tm06.shp has no CRS defined.")
    if tiles.crs != gdf.crs:
        tiles = tiles.to_crs(gdf.crs)

    tile_field = _first_existing_col(
        list(tiles.columns),
        [
            "Name", "NAME", "name",
            "Tile", "TILE", "tile",
            "Tile_id", "TILE_ID", "tile_id",
            "Tile_name", "TILE_NAME", "tile_name",
            "MGRS_TILE", "mgrs_tile",
            "S2_TILE", "s2_tile",
            "Id", "ID", "id",
        ],
    )
    if tile_field is None:
        raise ValueError(
            "Could not identify a tile field in sentinel2_tiles_PT_terra_tm06.shp."
        )

    tiles_min = tiles[[tile_field, "geometry"]].copy()

    polys_tiles = gdf[[geometry_name]].copy()
    polys_tiles = polys_tiles.set_geometry(geometry_name)
    polys_tiles["_idx"] = polys_tiles.index

    join_tiles = gpd.sjoin(
        polys_tiles,
        tiles_min,
        how="left",
        predicate="within",
    ).drop(columns=["index_right"], errors="ignore")

    if join_tiles.empty:
        gdf["S2_TILE"] = None
    else:
        counts = join_tiles.groupby("_idx").size().rename("__n_matches")
        valid_idx = counts[counts == 1].index

        join_tiles = join_tiles[join_tiles["_idx"].isin(valid_idx)].copy()
        join_tiles = join_tiles.drop_duplicates(subset="_idx", keep="first")

        tile_mapping = join_tiles.set_index("_idx")[tile_field].map(_normalize_s2_tile_value).to_dict()
        gdf["S2_TILE"] = gdf.index.map(tile_mapping)

    added_fields_raw.append("S2_TILE")

    # ----------------------------------------------------------
    # lista de campos armonizados "crudos"
    # ----------------------------------------------------------
    harmonized_raw = [
        "SRC", "Id", "UID",
        "Data0", "Data1",
        "Temp_eval_start", "Temp_eval_end",
        "CHG_TYPE", "AREA_HA", "Validation_flag",
        "S2_TILE",
    ]

    if "PI_DICOFRE" in gdf.columns:
        harmonized_raw.append("PI_DICOFRE")

    # 3. Optionally keep only harmonized fields + geometry
    if keep_only_harmonized:
        cols_to_keep = harmonized_raw + [geometry_name]
        gdf = gdf[cols_to_keep]

    # Save columns before renaming
    cols_pre_rename = [c for c in gdf.columns]

    # ----------------------------------------------------------
    # Renombrado FINAL explícito
    # ----------------------------------------------------------
    final_rename_map = {
        "SRC": "Src",
        "UID": "Uid",
        "CHG_TYPE": "Chg_type",
        "AREA_HA": "Area_ha",
        "PI_DICOFRE": "Pi_dicofre",
        "S2_TILE": "S2_tile",
        "Data0": "Data0",
        "Data1": "Data1",
        "Temp_eval_start": "Temp_eval_start",
        "Temp_eval_end": "Temp_eval_end",
        "Validation_flag": "Validation_flag",
        "Id": "Id",
    }
    gdf = gdf.rename(columns={k: v for k, v in final_rename_map.items() if k in gdf.columns})

    # 5. Normalize text VALUES only (not column names)
    for col in gdf.columns:
        if col == geometry_name:
            continue
        if gdf[col].dtype == object:
            gdf[col] = gdf[col].where(
                gdf[col].isna(),
                gdf[col].astype(str).str.lower()
            )

    # 6. Build field change report
    rows = []

    for orig in original_cols:
        if orig in cols_pre_rename:
            final_name = final_rename_map.get(orig, orig)
            status = "kept_and_renamed"
        else:
            final_name = None
            status = "dropped"
        rows.append({
            "original_name": orig,
            "final_name": final_name,
            "status": status
        })

    for new_raw in added_fields_raw:
        if new_raw in cols_pre_rename and new_raw not in original_cols:
            final_name = final_rename_map.get(new_raw, new_raw)
            rows.append({
                "original_name": new_raw,
                "final_name": final_name,
                "status": "added"
            })

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_df = pd.DataFrame(rows)
    report_df.to_excel(report_path, index=False)

    # 7. Save final layer
    gdf = normalize_string(gdf)
    gdf = reproject_layer(gdf, output_path, target_crs="EPSG:3763", layer_name=layer_name)

    return gdf


def harmonize_icnf_years_one_gpkg(
    year_to_shp: dict[int, str],
    out_gpkg: str,
    reports_dir: str,
    *,
    keep_only_harmonized: bool = True,
) -> None:
    """
    Corre ICNF año por año y escribe todas las capas dentro de un único GPKG.

    year_to_shp = {2020: ".../icnf_2020.shp", 2021: ".../icnf_2021.shp", ...}
    out_gpkg = ".../ICNF_harmonized.gpkg"
    reports_dir = carpeta donde guardar los xlsx por año
    """
    out_gpkg = str(Path(out_gpkg))
    reports_dir = Path(reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    for year in sorted(year_to_shp.keys()):
        shp = year_to_shp[year]
        report = str(reports_dir / f"icnf_{year}_harmonization_report.xlsx")

        harmonize_icnf_layer(
            input_shp=shp,
            output_shp=out_gpkg,
            report_xlsx=report,
            keep_only_harmonized=keep_only_harmonized,
            layer_name=f"ICNF_{year}",
        )