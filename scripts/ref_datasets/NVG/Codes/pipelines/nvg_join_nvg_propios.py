from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import pandas as pd
import geopandas as gpd


_WS_RE = re.compile(r"\s+")


# ------------------------
# Value normalization (propios)
# ------------------------
def _norm_text_value(x) -> str | None:
    if x is None or pd.isna(x):
        return None
    s = str(x).strip()
    if s == "":
        return ""
    s = s.lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = _WS_RE.sub(" ", s)
    return s


def _normalize_propios_text_fields(
    propios: gpd.GeoDataFrame,
    *,
    exclude_cols: set[str],
) -> gpd.GeoDataFrame:
    propios = propios.copy()

    for column in propios.columns:
        if column in exclude_cols or column == propios.geometry.name:
            continue

        if pd.api.types.is_string_dtype(propios[column].dtype):
            propios[column] = propios[column].map(_norm_text_value)

    return propios


def _is_date_colname(name: str) -> bool:
    n = name.lower()
    return (
        n.startswith("dt_")
        or n.startswith("data")
        or "date" in n
        or "inicio" in n
        or "fim" in n
        or "start" in n
        or "end" in n
    )


def _format_dates_yyyy_mm_dd(df: pd.DataFrame, *, exclude_cols: set[str]) -> pd.DataFrame:
    df = df.copy()
    for c in df.columns:
        if c in exclude_cols:
            continue

        if pd.api.types.is_datetime64_any_dtype(df[c]):
            dt = pd.to_datetime(df[c], errors="coerce")
            df[c] = dt.dt.strftime("%Y-%m-%d")
            continue

        if df[c].dtype == "object" and _is_date_colname(c):
            dt = pd.to_datetime(df[c], errors="coerce", dayfirst=False)
            mask = dt.notna()
            if mask.any():
                out = df[c].copy()
                out.loc[mask] = dt.loc[mask].dt.strftime("%Y-%m-%d")
                df[c] = out
    return df


# ------------------------
# Geometry cleaning (robust intersections)
# ------------------------
def _clean_geoms(gdf: gpd.GeoDataFrame, *, label: str) -> gpd.GeoDataFrame:
    """
    - drop None/empty
    - attempt make_valid() (shapely>=2)
    - fallback buffer(0)
    """
    gdf = gdf.copy()
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()

    try:
        gdf["geometry"] = gdf.geometry.make_valid()
    except Exception:
        gdf["geometry"] = gdf.geometry.buffer(0)

    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    return gdf


# ------------------------
# Area handling (ACORDADO: drop + recreate Area_ha)
# ------------------------
def _rebuild_area_ha(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Acordado:
      - eliminar TODAS las columnas equivalentes a area_ha (case-insensitive)
      - crear una sola 'Area_ha' calculada sobre la geometría de salida
    """
    gdf = gdf.copy()
    geom_col = gdf.geometry.name

    to_drop = [c for c in gdf.columns if c != geom_col and c.lower() == "area_ha"]
    if to_drop:
        gdf = gdf.drop(columns=to_drop, errors="ignore")

    gdf["Area_ha"] = gdf.geometry.area / 10000.0
    return gdf


# ------------------------
# Column name harmonization (Firstcap + dedupe keep-first)
# ------------------------
def _firstcap(col: str) -> str:
    if not col:
        return col
    return col[0].upper() + col[1:].lower()


def _harmonize_column_names_firstcap_dedupe_keep_first(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Normaliza nombres a Firstcap.
    Si genera duplicados por la normalización, mantiene la primera ocurrencia y elimina las demás.
    """
    gdf = gdf.copy()
    geom_col = gdf.geometry.name

    rename = {c: _firstcap(c) for c in gdf.columns if c != geom_col}
    gdf = gdf.rename(columns=rename)

    cols = [c for c in gdf.columns if c != geom_col]
    seen = set()
    drop_cols = []
    for c in cols:
        if c in seen:
            drop_cols.append(c)
        else:
            seen.add(c)

    if drop_cols:
        gdf = gdf.drop(columns=drop_cols, errors="ignore")

    return gdf


def _drop_internal_pid_cols(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Requisito: NO debe salir _pid en ningún caso.
    También eliminamos cualquier columna interna tipo __pid, pid, etc.
    """
    gdf = gdf.copy()
    geom_col = gdf.geometry.name
    bad = []
    for c in gdf.columns:
        if c == geom_col:
            continue
        cl = c.lower()
        if cl in {"_pid", "__pid", "pid"}:
            bad.append(c)
        elif cl.startswith("_pid") or cl.startswith("__pid"):
            bad.append(c)
    if bad:
        gdf = gdf.drop(columns=bad, errors="ignore")
    return gdf


# ------------------------
# Sentinel-2 tile by centroid
# ------------------------
def _normalize_s2_tile_value(v) -> str | None:
    if v is None or pd.isna(v):
        return None
    s = str(v).strip()
    if s == "" or s.upper() in {"NULL", "NAN", "NONE", "NAT"}:
        return None
    return s.lower()


def _first_existing_col(cols: list[str], candidates: list[str]) -> str | None:
    for c in candidates:
        if c in cols:
            return c
    return None


def _assign_s2_tile_if_fully_within(
    gdf: gpd.GeoDataFrame,
    *,
    sentinel2_tiles_path: str | None,
    out_col: str = "S2_tile",
) -> gpd.GeoDataFrame:
    """
    Asigna S2_tile SOLO si la geometría está completamente contenida
    dentro de un único tile Sentinel-2.

    Regla:
      - si feature within tile -> asigna ese tile
      - si no está completamente dentro de ningún tile -> NULL
      - no usa centroid, intersects ni nearest
    """
    out = gdf.copy()

    if out_col not in out.columns:
        out[out_col] = pd.NA

    if sentinel2_tiles_path is None:
        return out

    if out.empty:
        return out

    tiles_path = Path(sentinel2_tiles_path)
    if not tiles_path.exists():
        raise FileNotFoundError(f"Sentinel-2 tiles layer not found: {tiles_path}")

    tiles = gpd.read_file(tiles_path)
    if tiles.crs is None:
        raise ValueError("Sentinel-2 tiles layer has no CRS defined.")

    if out.crs is None:
        raise ValueError("Output layer has no CRS defined, cannot assign S2_tile.")

    if tiles.crs != out.crs:
        tiles = tiles.to_crs(out.crs)

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
            "Sentinel-2 tiles layer does not contain a recognized tile field."
        )

    geom_col = out.geometry.name
    tiles_min = tiles[[tile_field, "geometry"]].copy()

    work = out.reset_index(drop=False).rename(columns={"index": "__rowid"}).copy()

    # Solo matches donde el polígono esté completamente dentro del tile
    j = gpd.sjoin(
        work[["__rowid", geom_col]].copy(),
        tiles_min,
        how="left",
        predicate="within",
    ).copy()

    if j.empty:
        work[out_col] = pd.NA
    else:
        # Si por alguna razón hay más de un match para un mismo feature,
        # se considera ambiguo y NO se asigna tile.
        counts = j.groupby("__rowid").size().rename("__n_matches")
        valid_rowids = counts[counts == 1].index

        j_valid = j[j["__rowid"].isin(valid_rowids)].copy()
        j_valid = j_valid.drop_duplicates(subset="__rowid", keep="first")

        tile_map = j_valid.set_index("__rowid")[tile_field].map(_normalize_s2_tile_value)
        work[out_col] = work["__rowid"].map(tile_map)

    work = work.drop(columns=["__rowid"], errors="ignore")
    work = gpd.GeoDataFrame(work, geometry=geom_col, crs=out.crs)
    return work


# ------------------------
# Src/Uid para propios sin match
# ------------------------
def _fill_propios_src_uid_for_unmatched(
    gdf: gpd.GeoDataFrame,
    *,
    id_col: str,
    src_col: str = "Src",
    uid_col: str = "Uid",
    propios_src_value: str = "nvg",
    propios_uid_prefix: str = "nvg_propios_",
    propios_uid_width: int = 6,
) -> gpd.GeoDataFrame:
    """
    Para features UNMATCHED (id_col es NA):
      - asigna Src = propios_src_value
      - asigna Uid = propios_uid_prefix + consecutivo (zero-pad)

    No sobrescribe Src/Uid de matched.
    """
    gdf = gdf.copy()

    if src_col not in gdf.columns:
        gdf[src_col] = pd.NA
    if uid_col not in gdf.columns:
        gdf[uid_col] = pd.NA

    if id_col not in gdf.columns:
        raise KeyError(f"id_col={id_col!r} no existe en gdf. Columnas: {list(gdf.columns)}")

    mask_unmatched = gdf[id_col].isna()
    if not mask_unmatched.any():
        return gdf

    gdf.loc[mask_unmatched & gdf[src_col].isna(), src_col] = propios_src_value

    need_uid = mask_unmatched & gdf[uid_col].isna()
    if need_uid.any():
        idx = gdf.index[need_uid]
        seq = pd.Series(range(1, len(idx) + 1), index=idx)
        gdf.loc[idx, uid_col] = seq.map(lambda k: f"{propios_uid_prefix}{k:0{propios_uid_width}d}")

    return gdf


def _set_src_for_matched(
    gdf: gpd.GeoDataFrame,
    *,
    id_col: str,
    src_col: str = "Src",
    matched_src_value: str = "nvg_s2",
) -> gpd.GeoDataFrame:
    """
    Asigna Src solo a las features matched (id_col NOT NA).
    No toca las unmatched.
    """
    gdf = gdf.copy()

    if id_col not in gdf.columns:
        raise KeyError(f"id_col={id_col!r} no existe en gdf. Columnas: {list(gdf.columns)}")

    if src_col not in gdf.columns:
        gdf[src_col] = pd.NA

    mask_matched = gdf[id_col].notna()
    gdf.loc[mask_matched, src_col] = matched_src_value
    return gdf


# ------------------------
# Exclusion by count of dropped points INSIDE polygon
# ------------------------
def _mark_propios_excluded_by_dropped(
    propios_sp: gpd.GeoDataFrame,
    dropped_pts: gpd.GeoDataFrame,
    *,
    min_points_to_allow_join: int = 10,
) -> gpd.GeoDataFrame:
    """
    Excluye del join los polígonos que tengan menos de min_points_to_allow_join
    puntos dentro.

    Regla:
      - __drop_pt_count < min_points_to_allow_join  -> excluido del join
      - __drop_pt_count >= min_points_to_allow_join -> sí entra al join

    No elimina geometrías del output final; solo controla si participan en el join.
    """
    propios_sp = propios_sp.copy()

    if "__excluded_by_dropped" not in propios_sp.columns:
        propios_sp["__excluded_by_dropped"] = False
    if "__drop_pt_count" not in propios_sp.columns:
        propios_sp["__drop_pt_count"] = 0

    if dropped_pts is None or dropped_pts.empty:
        propios_sp["__drop_pt_count"] = 0
        propios_sp["__excluded_by_dropped"] = True
        return propios_sp

    dropped_pts = dropped_pts.copy()
    dropped_pts = dropped_pts[dropped_pts.geometry.notna() & ~dropped_pts.geometry.is_empty].copy()
    if dropped_pts.empty:
        propios_sp["__drop_pt_count"] = 0
        propios_sp["__excluded_by_dropped"] = True
        return propios_sp

    dropped_pts = _clean_geoms(dropped_pts, label="dropped_points")

    hit = gpd.sjoin(
        dropped_pts[["geometry"]],
        propios_sp[["__pid", "geometry"]],
        how="inner",
        predicate="within",
    ).copy()

    if hit.empty:
        propios_sp["__drop_pt_count"] = 0
        propios_sp["__excluded_by_dropped"] = True
        return propios_sp

    counts = hit.groupby("__pid").size().rename("__drop_pt_count_new").reset_index()

    propios_sp = propios_sp.merge(counts, on="__pid", how="left")
    propios_sp["__drop_pt_count"] = propios_sp["__drop_pt_count_new"].fillna(0).astype(int)
    propios_sp = propios_sp.drop(columns=["__drop_pt_count_new"], errors="ignore")

    propios_sp["__excluded_by_dropped"] = (
        propios_sp["__drop_pt_count"] < int(min_points_to_allow_join)
    )

    return propios_sp


def _finalize_output(
    out: gpd.GeoDataFrame,
    *,
    normalize_output_columns: bool,
    rebuild_area_ha: bool,
) -> gpd.GeoDataFrame:
    out = out.copy()

    out = _format_dates_yyyy_mm_dd(out, exclude_cols={out.geometry.name})
    out = _drop_internal_pid_cols(out)

    if rebuild_area_ha:
        out = _rebuild_area_ha(out)

    if normalize_output_columns:
        out = _harmonize_column_names_firstcap_dedupe_keep_first(out)

    return out


# ------------------------
# MAIN PIPELINE
# ------------------------
def nvg_join_harmon_to_propios_maxarea_1to1_one_gpkg(
    *,
    input_propios: str,
    propios_layer: str | None,
    input_harmonized: str,
    harmonized_layer: str | None,
    out_gpkg: str,
    sentinel2_tiles_path: str | None = None,

    # puntos dropped para exclusión del join
    input_dropped_points: str | None = None,
    dropped_points_layer: str | None = None,
    min_dropped_points_to_allow_join: int = 10,

    # layers de auditoría
    out_layer_before_dissolve: str = "NVG_propios_join_harmon_before_dissolve",
    out_layer_after_dissolve_id: str = "NVG_propios_after_dissolve_by_Id",
    out_layer_after_dissolve_gleba: str = "NVG_propios_after_dissolve_by_Id_gleba",
    out_layer_after_dissolve_final: str = "NVG_propios_after_dissolve_FINAL",
    out_qa_layer: str = "QA_join_stats",

    # layer de exclusión por dropped
    export_excluded_by_dropped_layer: bool = True,
    out_layer_excluded_by_dropped: str = "QA_excluded_by_dropped",

    target_crs: str | None = None,

    # columnas clave
    harmon_id_col: str = "Id",
    harmon_uid_col: str = "Uid",
    propios_gleba_col: str = "Id_gleba",

    # thresholds
    min_intersection_area: float = 1.0,
    min_r_prop: float = 0.0001,

    # compatibilidad
    keep_only_best_1to1: bool = True,
    dissolve_unmatched_by_gleba: bool = True,

    # Src/Uid para propios sin match
    fill_propios_src_uid: bool = True,
    propios_src_value: str = "nvg",
    propios_uid_prefix: str = "nvg_propios_",
    propios_uid_width: int = 6,

    # Src para matched
    matched_src_value: str = "nvg_s2",

    # validaciones
    strict_join_validation: bool = True,
    join_validation_tol: float = 1e-9,
    export_worst_matches_layer: bool = True,
    worst_matches_top_n: int = 50,
    out_layer_worst_matches: str = "QA_worst_matches",
    export_worst_intersections: bool = False,
    out_layer_worst_intersections: str = "QA_worst_intersections",

    run_validation: bool = True,
    validation_report_csv: str | None = None,

    normalize_output_columns: bool = True,
    rebuild_area_ha: bool = True,
) -> gpd.GeoDataFrame:
    """
    Flujo robusto con:
      - CRS común obligatorio
      - limpieza de geometrías
      - matching determinista con tie-breakers
      - dissolve real de matched por Id
      - dissolve de unmatched por Id_gleba
      - Src/Uid para propios sin match
      - exclusión del join basada en cantidad de puntos dentro del polígono,
        conservando esas geometrías en la salida final
      - asignación de S2_tile por centroide en las capas de salida

    REGLAS:
      - excluir del join si __drop_pt_count < min_dropped_points_to_allow_join
      - matched   -> Src = nvg_s2
      - unmatched -> Src = nvg
    """

    # --- read
    propios = gpd.read_file(input_propios, layer=propios_layer) if propios_layer else gpd.read_file(input_propios)
    harmon = gpd.read_file(input_harmonized, layer=harmonized_layer) if harmonized_layer else gpd.read_file(input_harmonized)

    dropped_pts = None
    if input_dropped_points:
        dropped_pts = (
            gpd.read_file(input_dropped_points, layer=dropped_points_layer)
            if dropped_points_layer
            else gpd.read_file(input_dropped_points)
        )

    # --- CRS robusto
    if propios.crs is None:
        raise ValueError("La capa 'propios' no tiene CRS definido (crs=None). Asigna CRS o pasa target_crs.")
    if harmon.crs is None:
        raise ValueError("La capa 'harmonized' no tiene CRS definido (crs=None). Asigna CRS o pasa target_crs.")

    if dropped_pts is not None and dropped_pts.crs is None:
        raise ValueError("La capa 'dropped_points' no tiene CRS definido (crs=None). Asigna CRS o pasa target_crs.")

    if target_crs:
        if str(propios.crs) != str(target_crs):
            propios = propios.to_crs(target_crs)
        if str(harmon.crs) != str(target_crs):
            harmon = harmon.to_crs(target_crs)
        if dropped_pts is not None and str(dropped_pts.crs) != str(target_crs):
            dropped_pts = dropped_pts.to_crs(target_crs)
    else:
        if propios.crs != harmon.crs:
            harmon = harmon.to_crs(propios.crs)
        if dropped_pts is not None and dropped_pts.crs != propios.crs:
            dropped_pts = dropped_pts.to_crs(propios.crs)

    if propios.crs != harmon.crs:
        raise ValueError(f"CRS mismatch después de reproyección: propios={propios.crs}, harmon={harmon.crs}")

    if dropped_pts is not None and propios.crs != dropped_pts.crs:
        raise ValueError(f"CRS mismatch después de reproyección: propios={propios.crs}, dropped_points={dropped_pts.crs}")

    if hasattr(propios.crs, "is_projected") and not propios.crs.is_projected:
        raise ValueError(f"CRS no proyectado (grados). Usa CRS métrico. CRS actual: {propios.crs}")

    # --- checks de columnas
    if harmon_id_col not in harmon.columns:
        raise KeyError(f"No existe {harmon_id_col!r} en harmonized")
    if harmon_uid_col not in harmon.columns:
        raise KeyError(f"No existe {harmon_uid_col!r} en harmonized")
    if dissolve_unmatched_by_gleba and (propios_gleba_col not in propios.columns):
        raise KeyError(
            f"dissolve_unmatched_by_gleba=True pero {propios_gleba_col!r} no existe en propios."
        )

    # --- normalización valores
    propios = _normalize_propios_text_fields(propios, exclude_cols={propios.geometry.name})
    propios = _format_dates_yyyy_mm_dd(propios, exclude_cols={propios.geometry.name})
    harmon = _format_dates_yyyy_mm_dd(harmon, exclude_cols={harmon.geometry.name})

    # --- limpieza geométrica
    propios = _clean_geoms(propios, label="propios")
    harmon = _clean_geoms(harmon, label="harmon")
    if dropped_pts is not None:
        dropped_pts = _clean_geoms(dropped_pts, label="dropped_points")

    # =========================
    # 1) Multipart -> Singleparts
    # =========================
    propios_sp = propios.explode(index_parts=False).reset_index(drop=True).copy()
    propios_sp["__pid"] = range(1, len(propios_sp) + 1)
    propios_sp["__a_prop"] = propios_sp.geometry.area
    propios_sp["__excluded_by_dropped"] = False
    propios_sp["__drop_pt_count"] = 0

    # =========================
    # 1b) Excluir del join por cantidad de puntos dentro
    # =========================
    if dropped_pts is not None:
        propios_sp = _mark_propios_excluded_by_dropped(
            propios_sp,
            dropped_pts,
            min_points_to_allow_join=min_dropped_points_to_allow_join,
        )

    if export_excluded_by_dropped_layer:
        excluded_by_dropped = propios_sp.loc[propios_sp["__excluded_by_dropped"]].copy()
        if not excluded_by_dropped.empty:
            qa_excluded = excluded_by_dropped.drop(columns=["__a_prop"], errors="ignore").copy()
            qa_excluded["drop_reason"] = "excluded_by_point_count_rule"
            qa_excluded["min_dropped_points_to_allow_join"] = int(min_dropped_points_to_allow_join)
            Path(out_gpkg).parent.mkdir(parents=True, exist_ok=True)
            qa_excluded.to_file(out_gpkg, layer=out_layer_excluded_by_dropped, driver="GPKG")

    # =========================
    # 2) Candidatos (sjoin) + área exacta intersección
    # =========================
    harmon_min = harmon[[harmon_id_col, harmon_uid_col, harmon.geometry.name]].copy()

    propios_joinable = propios_sp.loc[~propios_sp["__excluded_by_dropped"]].copy()

    cand = gpd.sjoin(
        propios_joinable[["__pid", "__a_prop", propios_joinable.geometry.name]],
        harmon_min,
        how="inner",
        predicate="intersects",
    ).rename(columns={"index_right": "__hidx"}).copy()

    def _write_layers_when_no_match() -> gpd.GeoDataFrame:
        out_before = propios_sp.drop(columns=["__a_prop"], errors="ignore").copy()
        for c in harmon.columns:
            if c == harmon.geometry.name:
                continue
            if c not in out_before.columns:
                out_before[c] = pd.NA

        out_before = _finalize_output(
            out_before,
            normalize_output_columns=normalize_output_columns,
            rebuild_area_ha=rebuild_area_ha,
        )
        out_before = _assign_s2_tile_if_fully_within(
            out_before,
            sentinel2_tiles_path=sentinel2_tiles_path,
            out_col="S2_tile",
        )

        id_col_present_local = harmon_id_col
        if normalize_output_columns:
            id_col_present_local = _firstcap(harmon_id_col)

        if fill_propios_src_uid:
            out_before = _fill_propios_src_uid_for_unmatched(
                out_before,
                id_col=id_col_present_local,
                propios_src_value=propios_src_value,
                propios_uid_prefix=propios_uid_prefix,
                propios_uid_width=propios_uid_width,
            )

        out_before.to_file(out_gpkg, layer=out_layer_before_dissolve, driver="GPKG")
        out_before.to_file(out_gpkg, layer=out_layer_after_dissolve_id, driver="GPKG")
        out_before.to_file(out_gpkg, layer=out_layer_after_dissolve_gleba, driver="GPKG")
        out_before.to_file(out_gpkg, layer=out_layer_after_dissolve_final, driver="GPKG")
        return out_before

    if cand.empty:
        return _write_layers_when_no_match()

    harmon_geom_by_idx = harmon.geometry

    def _int_area(row) -> float:
        g1 = row.geometry
        g2 = harmon_geom_by_idx.iloc[int(row["__hidx"])]
        inter = g1.intersection(g2)
        if inter.is_empty:
            return 0.0
        return float(inter.area)

    cand["__a_int"] = cand.apply(_int_area, axis=1)
    cand["__r_prop"] = cand["__a_int"] / cand["__a_prop"]

    cand_f = cand[
        (cand["__a_int"] >= float(min_intersection_area)) &
        (cand["__r_prop"] >= float(min_r_prop))
    ].copy()

    if cand_f.empty:
        return _write_layers_when_no_match()

    # =========================
    # Selección determinista
    # =========================
    cand_f = cand_f.sort_values(
        ["__pid", "__a_int", "__r_prop", harmon_id_col],
        ascending=[True, False, False, True],
    ).copy()

    best = cand_f.drop_duplicates(subset=["__pid"], keep="first").copy()

    # =========================
    # VALIDACIONES fuertes del join
    # =========================
    n_zero_int = int((best["__a_int"] <= 0).sum())

    mx = cand_f.groupby("__pid")["__a_int"].max()
    picked = best.set_index("__pid")["__a_int"]
    bad = picked < (mx.loc[picked.index] - float(join_validation_tol))
    n_bad_max = int(bad.sum())

    r = best["__r_prop"].astype(float)
    a = best["__a_int"].astype(float)

    def _pct(s: pd.Series, q: float) -> float:
        if s.empty:
            return float("nan")
        return float(s.quantile(q))

    best_id_counts = best[harmon_id_col].value_counts(dropna=True)
    n_ids_with_multiple_parts = int((best_id_counts > 1).sum())
    max_parts_per_id = int(best_id_counts.max()) if not best_id_counts.empty else 0

    qa_join = {
        "join_n_zero_intersections_in_best": n_zero_int,
        "join_n_best_not_max_for_pid": n_bad_max,
        "join_rprop_p10": _pct(r, 0.10),
        "join_rprop_p50": _pct(r, 0.50),
        "join_rprop_p90": _pct(r, 0.90),
        "join_aint_p10": _pct(a, 0.10),
        "join_aint_p50": _pct(a, 0.50),
        "join_aint_p90": _pct(a, 0.90),
        "join_n_unique_ids_in_best": int(best[harmon_id_col].nunique()),
        "join_n_ids_with_multiple_parts": n_ids_with_multiple_parts,
        "join_max_parts_per_id": max_parts_per_id,
    }

    if strict_join_validation:
        if n_zero_int > 0:
            raise ValueError(f"[STRICT JOIN] Hay {n_zero_int} matches con área de intersección 0.")
        if n_bad_max > 0:
            raise ValueError(
                f"[STRICT JOIN] Hay {n_bad_max} casos donde el match elegido NO es el máximo por __pid."
            )

    # =========================
    # QA EXTRA: exportar peores matches
    # =========================
    if export_worst_matches_layer and worst_matches_top_n > 0:
        n = int(min(worst_matches_top_n, len(best)))

        worst = best.sort_values(["__r_prop", "__a_int"], ascending=[True, True]).head(n).copy()

        pid_geom = propios_sp[["__pid", "geometry"]].copy()
        worst_gdf = pid_geom.merge(
            worst[["__pid", harmon_id_col, harmon_uid_col, "__a_int", "__r_prop"]],
            on="__pid",
            how="inner",
        )
        worst_gdf = gpd.GeoDataFrame(worst_gdf, geometry="geometry", crs=propios_sp.crs)

        Path(out_gpkg).parent.mkdir(parents=True, exist_ok=True)
        worst_gdf.to_file(out_gpkg, layer=out_layer_worst_matches, driver="GPKG")

        if export_worst_intersections:
            harmon_by_id = harmon.set_index(harmon_id_col)

            inter_geoms = []
            for _, row in worst.iterrows():
                pid = int(row["__pid"])
                hid = row[harmon_id_col]
                g1 = pid_geom.loc[pid_geom["__pid"] == pid, "geometry"].iloc[0]
                g2 = harmon_by_id.loc[hid].geometry
                inter = g1.intersection(g2)
                inter_geoms.append(inter)

            worst_int = worst[["__pid", harmon_id_col, harmon_uid_col, "__a_int", "__r_prop"]].copy()
            worst_int = gpd.GeoDataFrame(
                worst_int,
                geometry=gpd.GeoSeries(inter_geoms, crs=propios_sp.crs),
            )

            worst_int = worst_int[worst_int.geometry.notna() & ~worst_int.geometry.is_empty].copy()
            worst_int.to_file(out_gpkg, layer=out_layer_worst_intersections, driver="GPKG")

    # =========================
    # 3) Merge attrs harmon + BEFORE
    # =========================
    harmon_attrs = harmon.drop(columns=[harmon.geometry.name], errors="ignore").copy()

    map_pid = best[["__pid", harmon_id_col]].merge(harmon_attrs, on=harmon_id_col, how="left")

    out_before_raw = propios_sp.merge(map_pid, on="__pid", how="left").drop(columns=["__a_prop"], errors="ignore")

    out_before = _finalize_output(
        out_before_raw,
        normalize_output_columns=normalize_output_columns,
        rebuild_area_ha=rebuild_area_ha,
    )
    out_before = _assign_s2_tile_if_fully_within(
        out_before,
        sentinel2_tiles_path=sentinel2_tiles_path,
        out_col="S2_tile",
    )

    id_col_present = harmon_id_col
    gleba_col_present = propios_gleba_col
    if normalize_output_columns:
        id_col_present = _firstcap(harmon_id_col)
        gleba_col_present = _firstcap(propios_gleba_col)

    if fill_propios_src_uid:
        out_before = _fill_propios_src_uid_for_unmatched(
            out_before,
            id_col=id_col_present,
            propios_src_value=propios_src_value,
            propios_uid_prefix=propios_uid_prefix,
            propios_uid_width=propios_uid_width,
        )

    out_before = _set_src_for_matched(
        out_before,
        id_col=id_col_present,
        matched_src_value=matched_src_value,
    )

    Path(out_gpkg).parent.mkdir(parents=True, exist_ok=True)
    out_before.to_file(out_gpkg, layer=out_layer_before_dissolve, driver="GPKG")

    # =========================
    # 4) Dissolves por separado
    # =========================
    matched = out_before[out_before[id_col_present].notna()].copy()
    unmatched = out_before[out_before[id_col_present].isna()].copy()

    # A) matched por Id
    if matched.empty:
        dissolved_matched = matched
    else:
        geom_col = matched.geometry.name
        matched = matched.loc[:, ~matched.columns.duplicated()].copy()

        agg = {}
        for c in matched.columns:
            if c == geom_col:
                continue
            if c == id_col_present:
                continue
            if c == "Area_ha":
                agg[c] = "sum"
            else:
                agg[c] = "first"

        dissolved_matched = matched.dissolve(by=id_col_present, aggfunc=agg, as_index=False)
        dissolved_matched = gpd.GeoDataFrame(dissolved_matched, geometry=geom_col, crs=matched.crs)

    # B) unmatched por gleba
    if not dissolve_unmatched_by_gleba:
        dissolved_unmatched = unmatched
    elif unmatched.empty:
        dissolved_unmatched = unmatched
    else:
        geom_col = unmatched.geometry.name
        unmatched = unmatched.loc[:, ~unmatched.columns.duplicated()].copy()

        if unmatched[gleba_col_present].isna().any():
            na_mask = unmatched[gleba_col_present].isna()
            unmatched.loc[na_mask, gleba_col_present] = "no_gleba_" + unmatched.loc[na_mask].index.astype(str)

        agg_u = {}
        for c in unmatched.columns:
            if c == geom_col:
                continue
            if c == gleba_col_present:
                continue
            if c == "Area_ha":
                agg_u[c] = "sum"
            else:
                agg_u[c] = "first"

        dissolved_unmatched = unmatched.dissolve(by=gleba_col_present, aggfunc=agg_u, as_index=False)
        dissolved_unmatched = gpd.GeoDataFrame(dissolved_unmatched, geometry=geom_col, crs=unmatched.crs)

    dissolved_matched = _finalize_output(
        dissolved_matched,
        normalize_output_columns=normalize_output_columns,
        rebuild_area_ha=rebuild_area_ha,
    )
    dissolved_matched = _assign_s2_tile_if_fully_within(
        dissolved_matched,
        sentinel2_tiles_path=sentinel2_tiles_path,
        out_col="S2_tile",
    )

    dissolved_unmatched = _finalize_output(
        dissolved_unmatched,
        normalize_output_columns=normalize_output_columns,
        rebuild_area_ha=rebuild_area_ha,
    )
    dissolved_unmatched = _assign_s2_tile_if_fully_within(
        dissolved_unmatched,
        sentinel2_tiles_path=sentinel2_tiles_path,
        out_col="S2_tile",
    )

    if fill_propios_src_uid:
        dissolved_unmatched = _fill_propios_src_uid_for_unmatched(
            dissolved_unmatched,
            id_col=id_col_present,
            propios_src_value=propios_src_value,
            propios_uid_prefix=propios_uid_prefix,
            propios_uid_width=propios_uid_width,
        )

    dissolved_matched = _set_src_for_matched(
        dissolved_matched,
        id_col=id_col_present,
        matched_src_value=matched_src_value,
    )

    dissolved_matched.to_file(out_gpkg, layer=out_layer_after_dissolve_id, driver="GPKG")
    dissolved_unmatched.to_file(out_gpkg, layer=out_layer_after_dissolve_gleba, driver="GPKG")

    # =========================
    # 5) FINAL
    # =========================
    out_after = pd.concat([dissolved_matched, dissolved_unmatched], ignore_index=True)
    out_after = gpd.GeoDataFrame(out_after, geometry=out_before.geometry.name, crs=out_before.crs)

    out_after = _finalize_output(
        out_after,
        normalize_output_columns=normalize_output_columns,
        rebuild_area_ha=rebuild_area_ha,
    )
    out_after = _assign_s2_tile_if_fully_within(
        out_after,
        sentinel2_tiles_path=sentinel2_tiles_path,
        out_col="S2_tile",
    )

    if fill_propios_src_uid:
        out_after = _fill_propios_src_uid_for_unmatched(
            out_after,
            id_col=id_col_present,
            propios_src_value=propios_src_value,
            propios_uid_prefix=propios_uid_prefix,
            propios_uid_width=propios_uid_width,
        )

    out_after = _set_src_for_matched(
        out_after,
        id_col=id_col_present,
        matched_src_value=matched_src_value,
    )

    out_after.to_file(out_gpkg, layer=out_layer_after_dissolve_final, driver="GPKG")

    # =========================
    # QA
    # =========================
    if run_validation:
        qa = {
            "n_propios_singleparts": int(len(propios_sp)),
            "n_propios_excluded_by_dropped": int(propios_sp["__excluded_by_dropped"].sum()),
            "n_propios_joinable": int((~propios_sp["__excluded_by_dropped"]).sum()),
            "n_harmon": int(len(harmon)),
            "n_candidates_sjoin": int(len(cand)),
            "n_candidates_after_filter": int(len(cand_f)),
            "n_best_matches": int(len(best)),
            "n_assigned_harmon_ids": int(best[harmon_id_col].nunique()),
            "n_assigned_propios": int(best["__pid"].nunique()),
            "n_out_before": int(len(out_before)),
            "n_out_dissolve_id": int(len(dissolved_matched)),
            "n_out_dissolve_gleba": int(len(dissolved_unmatched)),
            "n_out_final": int(len(out_after)),
            "min_intersection_area": float(min_intersection_area),
            "min_r_prop": float(min_r_prop),
            "min_dropped_points_to_allow_join": int(min_dropped_points_to_allow_join),
            "keep_only_best_1to1": bool(keep_only_best_1to1),
            "dissolve_unmatched_by_gleba": bool(dissolve_unmatched_by_gleba),
            "fill_propios_src_uid": bool(fill_propios_src_uid),
            "propios_src_value": str(propios_src_value),
            "propios_uid_prefix": str(propios_uid_prefix),
            "propios_uid_width": int(propios_uid_width),
            "matched_src_value": str(matched_src_value),
            "strict_join_validation": bool(strict_join_validation),
            "join_validation_tol": float(join_validation_tol),
            "normalize_output_columns": bool(normalize_output_columns),
            "rebuild_area_ha": bool(rebuild_area_ha),
            "sentinel2_tiles_path": str(sentinel2_tiles_path) if sentinel2_tiles_path is not None else "",
            "out_layer_before_dissolve": out_layer_before_dissolve,
            "out_layer_after_dissolve_id": out_layer_after_dissolve_id,
            "out_layer_after_dissolve_gleba": out_layer_after_dissolve_gleba,
            "out_layer_after_dissolve_final": out_layer_after_dissolve_final,
            "out_layer_excluded_by_dropped": out_layer_excluded_by_dropped if export_excluded_by_dropped_layer else "",
        }
        qa.update(qa_join)

        qa_df = pd.DataFrame([qa])
        qa_gdf = gpd.GeoDataFrame(qa_df, geometry=gpd.GeoSeries([None], crs=out_after.crs))
        qa_gdf.to_file(out_gpkg, layer=out_qa_layer, driver="GPKG")
        if validation_report_csv:
            qa_df.to_csv(validation_report_csv, index=False)

    return out_after