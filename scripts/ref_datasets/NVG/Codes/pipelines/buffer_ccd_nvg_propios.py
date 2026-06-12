from __future__ import annotations

from pathlib import Path
import numpy as np
import geopandas as gpd


# -----------------------------
# IO + geometry helpers
# -----------------------------
def _read_vector(path: str, layer: str | None = None) -> gpd.GeoDataFrame:
    """
    Lee SHP (sin layer) o GPKG/otros (con layer opcional).
    """
    p = Path(path)
    if p.suffix.lower() == ".shp":
        return gpd.read_file(path)
    if layer is None:
        return gpd.read_file(path)
    return gpd.read_file(path, layer=layer)


def _fix_geoms(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Limpieza robusta (evita warning de notna con empties).
    """
    gdf = gdf.copy()
    gdf = gdf[gdf.geometry.notna()].copy()
    gdf = gdf[~gdf.geometry.is_empty].copy()
    gdf["geometry"] = gdf.geometry.buffer(0)
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    return gdf


def _assert_metric_crs(crs) -> None:
    """
    Buffer interno asume unidades métricas.
    """
    try:
        if crs is None:
            print("WARN: CRS is None. Usa target_crs métrico para buffers en metros.")
            return
        if hasattr(crs, "is_geographic") and crs.is_geographic:
            print(f"WARN: CRS {crs} es geográfico (grados). Debes usar target_crs métrico (p.ej. EPSG:32629).")
    except Exception:
        return


# -----------------------------
# NVG mask build
# -----------------------------
def nvg_multipart_to_single(nvg: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    multipart -> singlepart
    """
    nvg = _fix_geoms(nvg)
    nvg_s = nvg.explode(index_parts=True).reset_index(drop=True)
    return gpd.GeoDataFrame(nvg_s, geometry="geometry", crs=nvg.crs)


def build_internal_mask(
    nvg_single: gpd.GeoDataFrame,
    *,
    buffer_m: float = -5.0,
    min_abs_buffer_m: float = 0.5,
) -> gpd.GeoDataFrame:
    """
    Máscara = buffer interno (negativo) por single-part.
    Fallback: si colapsa, usa geom original (no filtra ese polígono).
    """
    if buffer_m >= 0:
        raise ValueError("buffer_m debe ser NEGATIVO (buffer interno).")

    nvg_single = _fix_geoms(nvg_single)

    buf = float(buffer_m)
    if abs(buf) < min_abs_buffer_m:
        buf = -min_abs_buffer_m

    masks = nvg_single[["geometry"]].copy()
    masks["geometry"] = nvg_single.geometry.buffer(buf)
    masks = gpd.GeoDataFrame(masks, geometry="geometry", crs=nvg_single.crs)

    ok = masks.geometry.notna() & ~masks.geometry.is_empty
    masks["ok_internal"] = ok

    # fallback: si colapsó, ponemos geom original (no descarta)
    masks.loc[~ok, "geometry"] = nvg_single.loc[~ok, "geometry"].values

    masks = _fix_geoms(masks)
    return masks


# -----------------------------
# Points filtering ONLY (no attrs)
# -----------------------------
def filter_points_by_mask(
    points: gpd.GeoDataFrame,
    masks: gpd.GeoDataFrame,
    *,
    predicate: str = "within",
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Devuelve (kept, dropped).
    kept: puntos dentro de la máscara (buffer interno)
    dropped: el resto

    NO modifica columnas/atributos de points (solo filtra).
    """
    points = points.copy()
    points = points[points.geometry.notna()].copy()
    points = points[~points.geometry.is_empty].copy()

    # Join solo para obtener índices de puntos que pasan el filtro
    joined = gpd.sjoin(points, masks[["geometry"]], how="inner", predicate=predicate)

    kept_idx = joined.index.unique()
    kept = points.loc[points.index.isin(kept_idx)].copy()
    dropped = points.loc[~points.index.isin(kept_idx)].copy()

    return kept, dropped


# -----------------------------
# Public pipeline
# -----------------------------
def run_nvg_point_cleaning(
    nvg_path: str,
    nvg_layer: str | None,
    points_path: str,
    points_layer: str | None,
    out_dir: str,
    *,
    buffer_m: float = -5.0,
    target_crs: str | None = None,
    export_masks: bool = True,
) -> None:
    """
    Pipeline SIN MEZCLAR ATRIBUTOS:

    - Lee NVG (GPKG layer) y points (SHP o GPKG)
    - Reproyecta a target_crs (recomendado) o iguala CRS
    - NVG multipart -> singlepart
    - Máscara = buffer interno
    - Filtra points por máscara
    - Guarda points_clean y points_dropped (mismos campos originales)
    - (opcional) guarda nvg_singleparts y nvg_internal_masks para auditoría
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    nvg = _read_vector(nvg_path, layer=nvg_layer)
    pts = _read_vector(points_path, layer=points_layer)

    if target_crs is not None:
        nvg = nvg.to_crs(target_crs)
        pts = pts.to_crs(target_crs)
    else:
        if nvg.crs is not None and pts.crs != nvg.crs:
            pts = pts.to_crs(nvg.crs)

    _assert_metric_crs(nvg.crs)

    geom_types = pts.geom_type.value_counts().to_dict()
    if not pts.geom_type.isin(["Point", "MultiPoint"]).all():
        print("WARN: points NO es Point/MultiPoint. geom types:", geom_types)

    nvg_single = nvg_multipart_to_single(nvg)
    masks = build_internal_mask(nvg_single, buffer_m=buffer_m)

    pts_kept, pts_dropped = filter_points_by_mask(pts, masks, predicate="within")

    # Guardar SOLO puntos (sin columnas extra)
    pts_kept.to_file(out_dir / "points_clean.gpkg", layer="points_clean", driver="GPKG")
    pts_dropped.to_file(out_dir / "points_dropped.gpkg", layer="points_dropped", driver="GPKG")

    # Auditoría opcional
    if export_masks:
        nvg_single.to_file(out_dir / "nvg_singleparts.gpkg", layer="nvg_singleparts", driver="GPKG")
        masks.to_file(out_dir / "nvg_internal_masks.gpkg", layer="nvg_internal_masks", driver="GPKG")

    print(
        "DONE\n"
        f"- NVG singleparts: {len(nvg_single)}\n"
        f"- Masks:          {len(masks)} (buffer_m={buffer_m})\n"
        f"- Points total:   {len(pts)}\n"
        f"- Kept:           {len(pts_kept)}\n"
        f"- Dropped:        {len(pts_dropped)}\n"
        f"- CRS:            {nvg_single.crs}\n"
        f"- Points geom:    {geom_types}\n"
        f"- Out dir:        {out_dir}"
    )