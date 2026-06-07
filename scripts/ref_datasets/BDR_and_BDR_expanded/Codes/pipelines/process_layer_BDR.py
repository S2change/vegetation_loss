from __future__ import annotations

import geopandas as gpd
import pandas as pd
from pathlib import Path
import numpy as np
import datetime as dt
from pandas.api.types import is_object_dtype, is_string_dtype

from shapely.ops import unary_union, polygonize

from Codes.utils.normalize_string import normalize_string
from Codes.core.reproject_layer import reproject_layer


def harmonize_bdr_layer(
    input_shp: str,
    output_shp: str,
    report_xlsx: str,
    keep_only_harmonized: bool = False,
    *,
    layer_name: str | None = None,
    admin_areas_shp: str | None = None,
    sentinel2_tiles_shp: str | None = None,
) -> gpd.GeoDataFrame:
    """
    Harmonize BDR-like layers (sin ambigüedad en clases).

    Output harmonized fields:
      Src, Id, Uid, Data0, Data1, Temp_eval_start, Temp_eval_end,
      Chg_type, Area_ha, Validation_flag, Pi_dicofre, S2_tile,
      + Classe_0, Classe_1, Buffer_id

    Ajuste importante:
      - Las fechas de entrada se preservan y normalizan de forma robusta.
      - Los campos de fecha NO pasan por normalización de texto.
      - S2_tile SOLO se asigna si la geometría cae completamente dentro
        de un único tile Sentinel-2.
    """
    input_path = Path(input_shp)
    output_path = Path(output_shp)
    report_path = Path(report_xlsx)

    def _support_path(explicit_path: str | None, *relative_parts: str) -> Path:
        if explicit_path is not None:
            return Path(explicit_path)

        project_root = Path(__file__).resolve().parents[3]
        project_candidate = project_root.joinpath(*relative_parts)
        if project_candidate.exists():
            return project_candidate

        return input_path.parents[2].joinpath(*relative_parts)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    gdf = gpd.read_file(input_path)

    if gdf.crs is None:
        raise ValueError("BDR layer has no CRS defined. A projected CRS is required.")

    gdf = gdf.reset_index(drop=True)

    geometry_name = gdf.geometry.name
    original_cols = [c for c in gdf.columns if c != geometry_name]
    added_fields_raw: list[str] = []

    # ----------------------------------------------------------
    # Helpers
    # ----------------------------------------------------------
    def _first_existing(cols: list[str], source_cols=None) -> str | None:
        cols_available = source_cols if source_cols is not None else gdf.columns
        for c in cols:
            if c in cols_available:
                return c
        return None

    def _format_single_date(value):
        """
        Convierte un valor individual a 'YYYY-MM-DD' o None.
        Preserva correctamente strings ya válidos, date, datetime, Timestamp,
        enteros YYYYMMDD y variantes con hora.
        """
        if value is None:
            return None

        # NaN / NaT
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass

        # datetime/date/Timestamp
        if isinstance(value, pd.Timestamp):
            if pd.isna(value):
                return None
            return value.strftime("%Y-%m-%d")

        if isinstance(value, dt.datetime):
            return value.strftime("%Y-%m-%d")

        if isinstance(value, dt.date):
            return value.strftime("%Y-%m-%d")

        s = str(value).strip()

        if s == "":
            return None

        if s.upper() in {"NULL", "NONE", "NAN", "NAT"}:
            return None

        # YYYYMMDD exacto
        if len(s) == 8 and s.isdigit():
            try:
                return pd.to_datetime(s, format="%Y%m%d", errors="raise").strftime("%Y-%m-%d")
            except Exception:
                return None

        # YYYY-MM-DD exacto: devolver tal cual si es válido
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            try:
                return pd.to_datetime(s, format="%Y-%m-%d", errors="raise").strftime("%Y-%m-%d")
            except Exception:
                pass

        # datetime tipo '2020-09-11 00:00:00'
        try:
            dt_parsed = pd.to_datetime(s, errors="raise")
            return dt_parsed.strftime("%Y-%m-%d")
        except Exception:
            return None

    def _norm_date_series_to_str(series: pd.Series) -> pd.Series:
        return series.apply(_format_single_date).astype("object")

    def _normalize_buffer_id(series: pd.Series) -> pd.Series:
        s = series.astype("object")
        out = pd.Series([None] * len(s), index=s.index, dtype="object")

        s_str = s.astype(str).str.strip()
        miss = s.isna() | s_str.eq("") | s_str.str.upper().isin({"NULL", "NAN", "NONE", "NAT"})
        out.loc[~miss] = s_str.loc[~miss]

        out = out.where(out.isna(), out.astype(str).str.replace(r"\.0$", "", regex=True).str.strip())
        out = out.where(out.astype("object").notna(), None)
        return out

    def _compute_topology_flags_qgis_like(
        gdf_in: gpd.GeoDataFrame,
        geom_col: str,
        *,
        area_tol: float = 1e-6,
    ) -> tuple[pd.Series, pd.Series]:
        overlap_per_feature = pd.Series(False, index=gdf_in.index)
        gap_per_feature = pd.Series(False, index=gdf_in.index)

        g = gdf_in[[geom_col]].copy()
        g = g[g[geom_col].notna()].copy()

        if g.empty:
            return overlap_per_feature, gap_per_feature

        try:
            g[geom_col] = g[geom_col].make_valid()
        except Exception:
            g[geom_col] = g[geom_col].buffer(0)

        boundaries = unary_union(g[geom_col].boundary)
        faces = list(polygonize(boundaries))
        if not faces:
            return overlap_per_feature, gap_per_feature

        faces_gdf = gpd.GeoDataFrame({"geometry": faces}, crs=gdf_in.crs)
        faces_gdf["__area"] = faces_gdf.geometry.area

        outside_idx = faces_gdf["__area"].idxmax()
        faces_gdf = faces_gdf.drop(index=outside_idx).copy()

        faces_gdf = faces_gdf.loc[faces_gdf["__area"] > area_tol].copy()
        if faces_gdf.empty:
            return overlap_per_feature, gap_per_feature

        poly = gpd.GeoDataFrame({"__idx": g.index}, geometry=g[geom_col], crs=gdf_in.crs)

        faces_for_join = faces_gdf[["geometry"]].reset_index(drop=False).rename(columns={"index": "__face_id"})
        j = gpd.sjoin(
            faces_for_join,
            poly[["__idx", "geometry"]],
            how="left",
            predicate="within",
        )

        cov = (
            j.groupby("__face_id")["__idx"]
            .nunique(dropna=True)
            .rename("cover_n")
            .reset_index()
        )

        faces_gdf = faces_gdf.reset_index(drop=True).rename_axis("__face_id").reset_index()
        faces_gdf = faces_gdf.merge(cov, on="__face_id", how="left")
        faces_gdf["cover_n"] = faces_gdf["cover_n"].fillna(0).astype(int)

        gap_faces = faces_gdf.loc[faces_gdf["cover_n"] == 0, ["__face_id", "geometry"]].copy()
        overlap_faces = faces_gdf.loc[faces_gdf["cover_n"] > 1, ["__face_id", "geometry"]].copy()

        if not overlap_faces.empty:
            j_ov = gpd.sjoin(
                poly[["__idx", "geometry"]],
                overlap_faces,
                how="inner",
                predicate="intersects",
            )
            overlap_per_feature.loc[j_ov["__idx"].unique()] = True

        if not gap_faces.empty:
            j_gap = gpd.sjoin(
                poly[["__idx", "geometry"]],
                gap_faces,
                how="inner",
                predicate="touches",
            )
            gap_per_feature.loc[j_gap["__idx"].unique()] = True

        return overlap_per_feature, gap_per_feature

    def _join_by_centroid_single_match(
        base_gdf: gpd.GeoDataFrame,
        target_gdf: gpd.GeoDataFrame,
        value_field: str,
        out_field: str,
        id_field: str = "Id",
    ) -> gpd.GeoDataFrame:
        cent = base_gdf[[id_field, geometry_name]].copy()
        cent = cent.set_geometry(geometry_name)
        cent["geometry"] = cent.geometry.centroid

        target_min = target_gdf[[value_field, "geometry"]].copy()

        j_within = gpd.sjoin(
            cent,
            target_min,
            how="left",
            predicate="within",
        ).drop(columns=["index_right"], errors="ignore")

        j_within = j_within.drop_duplicates(subset=id_field, keep="first")
        matched_ids = set(j_within.loc[j_within[value_field].notna(), id_field].tolist())

        missing = cent.loc[~cent[id_field].isin(matched_ids)].copy()

        if not missing.empty:
            j_inter = gpd.sjoin(
                missing,
                target_min,
                how="left",
                predicate="intersects",
            ).drop(columns=["index_right"], errors="ignore")
            j_inter = j_inter.drop_duplicates(subset=id_field, keep="first")

            joined = pd.concat(
                [
                    j_within[[id_field, value_field]],
                    j_inter[[id_field, value_field]],
                ],
                ignore_index=True,
            )
            joined = joined.drop_duplicates(subset=id_field, keep="first")
        else:
            joined = j_within[[id_field, value_field]].copy()

        joined = joined.rename(columns={value_field: out_field})
        return base_gdf.merge(joined, on=id_field, how="left")

    def _join_if_fully_within_single_match(
        base_gdf: gpd.GeoDataFrame,
        target_gdf: gpd.GeoDataFrame,
        value_field: str,
        out_field: str,
        id_field: str = "Id",
    ) -> gpd.GeoDataFrame:
        """
        Asigna out_field SOLO si la geometría del feature está completamente
        contenida dentro de una única geometría target.

        Regla:
          - within en la geometría completa
          - si no hay match -> null
          - si hay más de un match para el mismo feature -> null
        """
        base = base_gdf.copy()

        geom_col = base.geometry.name
        target_min = target_gdf[[value_field, "geometry"]].copy()

        work = base[[id_field, geom_col]].copy()

        j = gpd.sjoin(
            work,
            target_min,
            how="left",
            predicate="within",
        ).drop(columns=["index_right"], errors="ignore")

        if j.empty:
            base[out_field] = None
            return base

        counts = j.groupby(id_field).size().rename("__n_matches")
        valid_ids = counts[counts == 1].index

        j_valid = j[j[id_field].isin(valid_ids)].copy()
        j_valid = j_valid.drop_duplicates(subset=id_field, keep="first")

        joined = j_valid[[id_field, value_field]].rename(columns={value_field: out_field})

        base = base.merge(joined, on=id_field, how="left")
        return base

    def _normalize_text_columns_only(
        gdf_in: gpd.GeoDataFrame,
        skip_cols: set[str],
    ) -> gpd.GeoDataFrame:
        """
        Normaliza únicamente las columnas de texto seguras.

        Reglas:
          - reconoce columnas object, string[python] y string[pyarrow];
          - aplica normalize_string valor por valor;
          - conserva valores nulos;
          - no modifica geometrías ni campos de fecha.
        """
        gdf_out = gdf_in.copy()

        for col in gdf_out.columns:
            if col == geometry_name or col in skip_cols:
                continue

            series = gdf_out[col]

            if is_object_dtype(series.dtype) or is_string_dtype(series.dtype):
                gdf_out[col] = series.map(
                    lambda value: (
                        normalize_string(str(value))
                        if pd.notna(value)
                        else None
                    )
                )

        return gdf_out

    # ----------------------------------------------------------
    # 0) REQUIRED FIELDS
    # ----------------------------------------------------------
    classe_0_src = _first_existing(["classe_0", "clase_0", "Classe_0", "Clase_0"])
    classe_1_src = _first_existing(["classe_1", "clase_1", "Classe_1", "Clase_1"])
    buffer_id_src = _first_existing(["buffer_ID", "buffer_id", "Buffer_ID", "Buffer_id"])

    if classe_0_src is None:
        raise ValueError("Missing required field 'classe_0' or 'clase_0' in input layer.")
    if classe_1_src is None:
        raise ValueError("Missing required field 'classe_1' or 'clase_1' in input layer.")
    if buffer_id_src is None:
        raise ValueError("Missing required field 'buffer_ID' in input layer.")

    if classe_0_src != "classe_0":
        gdf["classe_0"] = gdf[classe_0_src]
    if classe_1_src != "classe_1":
        gdf["classe_1"] = gdf[classe_1_src]
    if buffer_id_src != "buffer_ID":
        gdf["buffer_ID"] = gdf[buffer_id_src]

    gdf["buffer_ID"] = _normalize_buffer_id(gdf["buffer_ID"])

    # ----------------------------------------------------------
    # 1) Build Data0 / Data1
    # ----------------------------------------------------------
    data0_src = _first_existing(["data_0", "Data_0", "DATA_0", "Data0", "DATA0"])
    data1_src = _first_existing(["data_1", "Data_1", "DATA_1", "Data1", "DATA1"])

    if data0_src is not None:
        gdf["Data0"] = _norm_date_series_to_str(gdf[data0_src])
    else:
        gdf["Data0"] = None
    added_fields_raw.append("Data0")

    if data1_src is not None:
        gdf["Data1"] = _norm_date_series_to_str(gdf[data1_src])
    else:
        gdf["Data1"] = None
    added_fields_raw.append("Data1")

    if data0_src is not None:
        bad0 = gdf[gdf[data0_src].notna() & gdf["Data0"].isna()]
        print(f"[DATE CHECK] {data0_src} -> Data0 | input_non_null={gdf[data0_src].notna().sum()} | output_non_null={gdf['Data0'].notna().sum()} | lost={len(bad0)}")
        if not bad0.empty:
            print("[DATE CHECK] Examples lost in Data0:")
            print(bad0[[data0_src]].head(10).to_string())

    if data1_src is not None:
        bad1 = gdf[gdf[data1_src].notna() & gdf["Data1"].isna()]
        print(f"[DATE CHECK] {data1_src} -> Data1 | input_non_null={gdf[data1_src].notna().sum()} | output_non_null={gdf['Data1'].notna().sum()} | lost={len(bad1)}")
        if not bad1.empty:
            print("[DATE CHECK] Examples lost in Data1:")
            print(bad1[[data1_src]].head(10).to_string())

    TEMP_EVAL_START = "2018-09-01"
    TEMP_EVAL_END = "2021-09-30"
    gdf["Temp_eval_start"] = TEMP_EVAL_START
    gdf["Temp_eval_end"] = TEMP_EVAL_END
    added_fields_raw.extend(["Temp_eval_start", "Temp_eval_end"])

    # ----------------------------------------------------------
    # 2) Harmonized IDs and constants
    # ----------------------------------------------------------
    gdf["Id"] = pd.Series(range(1, len(gdf) + 1), index=gdf.index)
    added_fields_raw.append("Id")

    layer_key = (layer_name or "").strip().lower()
    source_name = "bdr_expanded" if "expanded" in layer_key else "bdr"

    if "SRC" not in gdf.columns:
        gdf["SRC"] = source_name
        added_fields_raw.append("SRC")

    if "UID" not in gdf.columns:
        gdf["UID"] = source_name + "_" + gdf.index.astype(str).str.zfill(7)
        added_fields_raw.append("UID")

    # ----------------------------------------------------------
    # 3) CHG_TYPE
    # ----------------------------------------------------------
    tipo_field = _first_existing(["tipo_1", "Tipo_1", "TIPO_1"])
    gdf["CHG_TYPE"] = None

    altera_field = _first_existing(["altera", "Altera", "ALTERA"])
    if altera_field is not None and tipo_field is not None:
        altera_lower = gdf[altera_field].astype(str).str.lower()
        no_change_mask = altera_lower.str.contains("sem alteracao", na=False)
        change_mask = ~no_change_mask
        gdf.loc[change_mask, "CHG_TYPE"] = gdf.loc[change_mask, tipo_field]
    else:
        change_field = _first_existing(["Change", "change", "CHANGE"])
        if change_field is not None and tipo_field is not None:
            ch = gdf[change_field].astype(str).str.lower()
            change_mask = (
                ch.str.contains("change", na=False)
                & ~ch.str.contains("no change|not aplicable|not applicable", na=False)
            )
            gdf.loc[change_mask, "CHG_TYPE"] = gdf.loc[change_mask, tipo_field]

    added_fields_raw.append("CHG_TYPE")

    # ----------------------------------------------------------
    # 4) Area
    # ----------------------------------------------------------
    gdf["AREA_HA"] = gdf.geometry.area / 10_000.0
    added_fields_raw.append("AREA_HA")

    # ----------------------------------------------------------
    # 5) Validation_flag
    # ----------------------------------------------------------
    overlap_per_feat, gap_per_feat = _compute_topology_flags_qgis_like(gdf, geometry_name, area_tol=1e-6)
    topo_error_per_feat = overlap_per_feat | gap_per_feat

    gdf["Validation_flag"] = np.where(
        topo_error_per_feat.to_numpy(),
        "Topology error",
        "No topology error",
    )
    added_fields_raw.append("Validation_flag")

    # ----------------------------------------------------------
    # 6) Pi_dicofre
    # ----------------------------------------------------------
    admin_path = _support_path(admin_areas_shp, "NUTS", "areas_administrativas.shp")
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

    gdf = _join_by_centroid_single_match(
        base_gdf=gdf,
        target_gdf=admin,
        value_field=admin_code_field,
        out_field="PI_DICOFRE",
        id_field="Id",
    )
    added_fields_raw.append("PI_DICOFRE")

    # ----------------------------------------------------------
    # 6b) S2 tile
    # ----------------------------------------------------------
    tiles_path = _support_path(sentinel2_tiles_shp, "S2_tiles", "sentinel2_tiles_PT_terra_tm06.shp")
    if not tiles_path.exists():
        raise FileNotFoundError(f"Sentinel-2 tiles layer not found: {tiles_path}")

    tiles = gpd.read_file(tiles_path)

    if tiles.crs is None:
        raise ValueError("sentinel2_tiles_PT_terra_tm06.shp has no CRS defined.")
    if tiles.crs != gdf.crs:
        tiles = tiles.to_crs(gdf.crs)

    tile_field = _first_existing(
        [
            "Name", "NAME", "name",
            "Tile", "TILE", "tile",
            "Tile_id", "TILE_ID", "tile_id",
            "Tile_name", "TILE_NAME", "tile_name",
            "MGRS_TILE", "mgrs_tile",
            "S2_TILE", "s2_tile",
            "Id", "ID", "id",
        ],
        source_cols=tiles.columns,
    )

    if tile_field is None:
        raise ValueError(
            "Could not identify a tile field in sentinel2_tiles_PT_terra_tm06.shp. "
            "Please specify the tile code/name column."
        )

    gdf = _join_if_fully_within_single_match(
        base_gdf=gdf,
        target_gdf=tiles,
        value_field=tile_field,
        out_field="S2_TILE",
        id_field="Id",
    )
    added_fields_raw.append("S2_TILE")

    # ----------------------------------------------------------
    # 7) Harmonized raw fields list
    # ----------------------------------------------------------
    harmonized_raw = [
        "SRC", "Id", "UID",
        "Data0", "Data1",
        "Temp_eval_start", "Temp_eval_end",
        "CHG_TYPE", "AREA_HA",
        "Validation_flag",
        "PI_DICOFRE",
        "S2_TILE",
        "classe_0", "classe_1",
        "buffer_ID",
    ]

    if keep_only_harmonized:
        cols_to_keep = harmonized_raw + [geometry_name]
        gdf = gdf[cols_to_keep]

    cols_pre_rename = [c for c in gdf.columns]

    final_rename_map = {
        "SRC": "Src",
        "UID": "Uid",
        "CHG_TYPE": "Chg_type",
        "AREA_HA": "Area_ha",
        "PI_DICOFRE": "Pi_dicofre",
        "S2_TILE": "S2_tile",
        "classe_0": "Classe_0",
        "classe_1": "Classe_1",
        "buffer_ID": "Buffer_id",
        "Data0": "Data0",
        "Data1": "Data1",
        "Temp_eval_start": "Temp_eval_start",
        "Temp_eval_end": "Temp_eval_end",
        "Id": "Id",
    }
    gdf = gdf.rename(columns={k: v for k, v in final_rename_map.items() if k in gdf.columns})


    # ----------------------------------------------------------
    # Report
    # ----------------------------------------------------------
    rows = []

    for orig in original_cols:
        if orig in cols_pre_rename:
            final_name = final_rename_map.get(orig, orig)
            status = "kept_and_renamed"
        else:
            final_name = None
            status = "dropped"
        rows.append({"original_name": orig, "final_name": final_name, "status": status})

    for new_raw in added_fields_raw:
        if new_raw in cols_pre_rename and new_raw not in original_cols:
            final_name = final_rename_map.get(new_raw, new_raw)
            rows.append({"original_name": new_raw, "final_name": final_name, "status": "added"})

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_df = pd.DataFrame(rows)
    report_df.to_excel(report_path, index=False)

    # Normalización final de valores de texto; las fechas se excluyen.
    gdf = _normalize_text_columns_only(
        gdf,
        skip_cols={"Data0", "Data1", "Temp_eval_start", "Temp_eval_end"},
    )

    gdf = reproject_layer(gdf, output_path, target_crs="EPSG:3763", layer_name=layer_name)

    return gdf
