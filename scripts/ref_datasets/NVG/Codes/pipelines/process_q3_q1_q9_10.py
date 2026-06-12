
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import geopandas as gpd


# ---------------------------------------------------------------------
# helpers NUEVOS / AJUSTADOS
# ---------------------------------------------------------------------
def _median_date_by_id_for_one_col(
    gdf: gpd.GeoDataFrame,
    col: str,
    *,
    id_col: str = "Id",
) -> pd.DataFrame:
    """
    Mediana por Id de UNA columna de fechas (string/datetime).
    Calcula mediana por ordinal (robusto).
    Retorna: Id, {col}_med_id
    """
    if col not in gdf.columns:
        return pd.DataFrame({id_col: pd.unique(gdf[id_col]), f"{col}_med_id": None})

    dt = pd.to_datetime(gdf[col], errors="coerce")
    ords = (
        dt.dt.floor("D")
        .dt.date
        .map(lambda d: d.toordinal() if pd.notna(d) else np.nan)
        .astype("float64")
    )

    tmp = pd.DataFrame({id_col: gdf[id_col].values, "_ord": ords.values})
    med_ord = tmp.groupby(id_col)["_ord"].median()

    def _ord_to_datestr(x):
        if pd.isna(x):
            return None
        xi = int(np.round(x))
        try:
            return pd.Timestamp.fromordinal(xi).strftime("%Y-%m-%d")
        except Exception:
            return None

    med_date = med_ord.map(_ord_to_datestr).rename(f"{col}_med_id").reset_index()
    return med_date


def _ccdc_summary_by_id(
    gdf: gpd.GeoDataFrame,
    *,
    id_col: str = "Id",
    ccdc_col: str = "Ccdc_ok",
    ok_threshold: float = 0.80,
) -> pd.DataFrame:
    """
    Resume CCDC por Id y define Validation_flag a NIVEL ID:
      - Pix_total_ccdc
      - Pix_ok_ccdc
      - Ok_ratio_ccdc
      - Validation_flag (ccdc ok / ccdc no ok) según ok_ratio>=threshold
    """
    s = pd.to_numeric(gdf[ccdc_col], errors="coerce").fillna(0).astype(int)
    tmp = gdf[[id_col]].copy()
    tmp["_ok"] = (s == 1).astype(int)

    summ = (
        tmp.groupby(id_col, as_index=False)
        .agg(
            Pix_total_ccdc=("_ok", "size"),
            Pix_ok_ccdc=("_ok", "sum"),
        )
    )
    summ["Ok_ratio_ccdc"] = (
        summ["Pix_ok_ccdc"] / summ["Pix_total_ccdc"].replace(0, np.nan)
    ).fillna(0.0)

    summ["Validation_flag"] = np.where(
        summ["Ok_ratio_ccdc"] >= ok_threshold, "ccdc ok", "ccdc no ok"
    )
    return summ


def _dissolve_by_id_keep_first(
    gdf: gpd.GeoDataFrame,
    *,
    id_col: str = "Id",
) -> gpd.GeoDataFrame:
    """
    Dissolve por Id:
    - geometría: union
    - atributos: 'first' (para columnas ya constantes por Id).
    IMPORTANTE: cualquier cosa que NO sea constante por Id debe pre-agregarse antes.
    """
    geom_col = gdf.geometry.name
    non_geom = [c for c in gdf.columns if c != geom_col]
    agg = {c: "first" for c in non_geom if c != id_col}
    d = gdf.dissolve(by=id_col, aggfunc=agg, as_index=False)
    return d


def _add_dissolved_ids(
    gdf_id: gpd.GeoDataFrame,
    *,
    src_value: str = "nvg",
    uid_prefix: str = "nvg_",
) -> gpd.GeoDataFrame:
    """
    IDs de salida para capa disuelta:
    - Src: fijo
    - fid: secuencial
    - Uid: secuencial (NO depende de Id)
    """
    out = gdf_id.reset_index(drop=True).copy()
    out["Src"] = src_value
    out["fid"] = (out.index + 1).astype(int)
    out["Uid"] = uid_prefix + (out.index).astype(str).str.zfill(7)
    return out


def _add_area_ha(gdf: gpd.GeoDataFrame, *, out_col: str = "Area_ha") -> gpd.GeoDataFrame:
    """
    Calcula área en hectáreas usando la geometría (requiere CRS proyectado en metros).
    """
    out = gdf.copy()
    out[out_col] = (out.geometry.area / 10000.0).astype(float)
    return out



def _write_vector(gdf: gpd.GeoDataFrame, out_path: str, *, layer: str | None = None) -> None:
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)

    if p.suffix.lower() == ".gpkg":
        lyr = layer or p.stem
        gdf.to_file(p, driver="GPKG", layer=lyr, index=False)
    else:
        gdf.to_file(p, index=False)


def _normalize_date_series_to_str(s: pd.Series) -> pd.Series:
    """Return YYYY-MM-DD strings or None."""
    dt = pd.to_datetime(s, errors="coerce")
    out = dt.dt.strftime("%Y-%m-%d")
    return out.where(dt.notna(), None)


def _is_missing_date(arr: np.ndarray) -> np.ndarray:
    """Treat None/NaN/empty/'NULL' variants as missing."""
    s = pd.Series(arr, dtype="object")
    miss = s.isna()
    txt = s.astype(str).str.strip()
    miss = miss | txt.eq("")
    miss = miss | txt.str.upper().isin({"NULL", "NAN", "NONE", "NAT"})
    return miss.to_numpy()


def _normalize_dicofre_code(v) -> str | None:
    """Normaliza Pi_dicofre a string (manteniendo ceros a la izquierda si aplica)."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    s = str(v).strip()
    if s == "" or s.upper() in {"NULL", "NAN", "NONE", "NAT"}:
        return None
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit():
        s = s.zfill(6)
    return s


def _normalize_and_filter_pixels(
    input_pixels_path: str,
    admin_areas_path: str,
    *,
    target_crs: str = "EPSG:3763",
) -> tuple[gpd.GeoDataFrame, str]:
    """
    Clean/normalize pixels, DROP Ids with any valid NC,
    keep Data0 & Data1, keep ECCD1/ECCD2, assign Pi_dicofre, generate Src/Uid/fid.
    """
    pix_path = Path(input_pixels_path)
    adm_path = Path(admin_areas_path)
    if not pix_path.exists():
        raise FileNotFoundError(f"Input pixel file not found: {pix_path}")
    if not adm_path.exists():
        raise FileNotFoundError(f"Administrative layer not found: {adm_path}")

    gdf = gpd.read_file(pix_path)
    if gdf.crs is None:
        raise ValueError("Pixel layer has no CRS defined.")
    if gdf.crs.to_string() != target_crs:
        gdf = gdf.to_crs(target_crs)
    gdf = gdf.reset_index(drop=True)
    geom_col = gdf.geometry.name

    # Id
    if "Id" not in gdf.columns:
        for alt in ["id", "ID"]:
            if alt in gdf.columns:
                gdf = gdf.rename(columns={alt: "Id"})
                break
        else:
            raise ValueError("Missing required field: 'Id'.")

    # Data0
    if "Data0" not in gdf.columns:
        for alt in ["Data_0", "data0", "data_0", "DATA0"]:
            if alt in gdf.columns:
                gdf = gdf.rename(columns={alt: "Data0"})
                break
        else:
            raise ValueError("Missing required field: 'Data0' (o equivalente).")

    # Data1
    if "Data1" not in gdf.columns:
        for alt in ["Data_1", "data1", "data_1", "DATA1"]:
            if alt in gdf.columns:
                gdf = gdf.rename(columns={alt: "Data1"})
                break
        else:
            raise ValueError("Missing required field: 'Data1' (o equivalente).")

    # ECCD1
    if "ECCD1" not in gdf.columns:
        for alt in ["eccd1", "Eccd1", "ECCD_1", "eccd_1"]:
            if alt in gdf.columns:
                gdf = gdf.rename(columns={alt: "ECCD1"})
                break
    if "ECCD1" not in gdf.columns:
        gdf["ECCD1"] = None

    # ECCD2
    if "ECCD2" not in gdf.columns:
        for alt in ["eccd2", "Eccd2", "ECCD_2", "eccd_2"]:
            if alt in gdf.columns:
                gdf = gdf.rename(columns={alt: "ECCD2"})
                break
    if "ECCD2" not in gdf.columns:
        gdf["ECCD2"] = None

    # Ccdc_ok
    ccdc_src = None
    for alt in ["Ccdc_ok", "ccdc_ok", "CCDC_OK"]:
        if alt in gdf.columns:
            ccdc_src = alt
            break
    if ccdc_src is None:
        raise ValueError("Missing required field: 'ccdc_ok' (o 'Ccdc_ok').")

    gdf["Ccdc_ok"] = pd.to_numeric(gdf[ccdc_src], errors="coerce").fillna(0).astype(int)
    if ccdc_src != "Ccdc_ok":
        gdf = gdf.drop(columns=[ccdc_src], errors="ignore")

    # NC (filtra Ids con cualquier NC válido)
    nc_candidates = [c for c in ["NC", "Nc", "nc"] if c in gdf.columns]
    if not nc_candidates:
        raise ValueError("Missing required field: 'NC'.")
    if nc_candidates[0] != "NC":
        gdf = gdf.rename(columns={nc_candidates[0]: "NC"})

    gdf["NC"] = pd.to_numeric(gdf["NC"], errors="coerce")
    bad_ids = set(gdf.loc[gdf["NC"].notna(), "Id"].unique())
    gdf = gdf.loc[~gdf["Id"].isin(bad_ids)].copy()
    gdf = gdf.reset_index(drop=True)
    gdf = gdf.drop(columns=["NC"])

    # normalizar fechas (string)
    gdf["Data0"] = _normalize_date_series_to_str(gdf["Data0"])
    gdf["Data1"] = _normalize_date_series_to_str(gdf["Data1"])
    gdf["ECCD1"] = _normalize_date_series_to_str(gdf["ECCD1"])
    gdf["ECCD2"] = _normalize_date_series_to_str(gdf["ECCD2"])

    # Chg_type = 'corte' SOLO si Data0 o Data1 no son NULL
    miss0 = _is_missing_date(gdf["Data0"].astype("object").to_numpy())
    miss1 = _is_missing_date(gdf["Data1"].astype("object").to_numpy())
    has_any_date = (~miss0) | (~miss1)
    gdf["Chg_type"] = np.where(has_any_date, "corte", None)

    # campos estándar
    gdf["fid"] = (gdf.index + 1).astype(int)
    gdf["Src"] = "nvg"
    gdf["Uid"] = "nvg_" + (gdf.index).astype(str).str.zfill(7)

    # Pi_dicofre
    admin = gpd.read_file(adm_path)
    if admin.crs is None:
        raise ValueError("Administrative layer has no CRS defined.")
    if admin.crs.to_string() != gdf.crs.to_string():
        admin = admin.to_crs(gdf.crs)
    if "dtmnfr" not in admin.columns:
        raise ValueError("Administrative layer missing 'dtmnfr'.")

    admin_min = admin[["dtmnfr", "geometry"]].copy()

    cent = gdf[["Uid", geom_col]].copy().rename(columns={geom_col: "geometry"})
    cent = gpd.GeoDataFrame(cent, geometry="geometry", crs=gdf.crs)
    cent["geometry"] = cent.geometry.centroid

    j = gpd.sjoin(cent, admin_min, how="left", predicate="within")

    dup = j.duplicated(subset=["Uid"], keep=False)
    if bool(dup.any()):
        raise ValueError("Administrative layer has overlapping polygons.")

    missing = j["dtmnfr"].isna()
    if bool(missing.any()):
        j_near = gpd.sjoin_nearest(cent.loc[missing].copy(), admin_min, how="left")
        j.loc[missing, "dtmnfr"] = j_near["dtmnfr"].to_numpy()

    if bool(j["dtmnfr"].isna().any()):
        raise ValueError("Some pixels could not be assigned 'dtmnfr'.")

    pi_map = j.set_index("Uid")["dtmnfr"].map(_normalize_dicofre_code)
    gdf["Pi_dicofre"] = gdf["Uid"].map(pi_map)

    gdf = gdf[
        [
            "fid", "Src", "Id", "Uid",
            "Data0", "Data1",
            "ECCD1", "ECCD2",
            "Chg_type",
            "Pi_dicofre", "Ccdc_ok",
            geom_col,
        ]
    ].copy()

    return gdf, geom_col


# ---------------------------------------------------------------------
# _compute_stats_by_id, _compute_eval_window_by_id: sin cambios
# ---------------------------------------------------------------------
def _compute_stats_by_id(
    gdf: gpd.GeoDataFrame,
    *,
    id_col: str = "Id",
    data0_col: str = "Data0",
    data1_col: str = "Data1",
) -> pd.DataFrame:
    """
    Stats por Id:
    - Pix_total
    - Pix_null(Data0), Null_prop_data0
    - Pix_null(Data1), Null_prop_data1
    - Cuantiles/percentiles EMPÍRICOS (por distribución con repeticiones), SIN interpolación:
        Data0: Data0_p10, Data0_q1, Data0_q3, Data0_p90
        Data1: Data1_p10, Data1_q1, Data1_q3, Data1_p90
    - IQR (días): (q3 - q1)
    - Extremos + delta:
        Data0_min, Data1_max, Data1_Data0_difference
    """

    miss0 = _is_missing_date(gdf[data0_col].astype("object").to_numpy())
    miss1 = _is_missing_date(gdf[data1_col].astype("object").to_numpy())

    counts = (
        gdf.assign(_is_null0=miss0, _is_null1=miss1)
        .groupby(id_col, as_index=False)
        .agg(
            Pix_total=("_is_null0", "size"),
            Pix_null_data0=("_is_null0", "sum"),
            Pix_null_data1=("_is_null1", "sum"),
        )
    )
    counts["Pix_total"] = counts["Pix_total"].astype(int)
    counts["Pix_null_data0"] = counts["Pix_null_data0"].astype(int)
    counts["Pix_null_data1"] = counts["Pix_null_data1"].astype(int)

    counts["Null_prop_data0"] = (
        counts["Pix_null_data0"] / counts["Pix_total"].replace(0, np.nan)
    ).fillna(0.0)
    counts["Null_prop_data1"] = (
        counts["Pix_null_data1"] / counts["Pix_total"].replace(0, np.nan)
    ).fillna(0.0)

    def _quantiles_one_field(colname: str, probs: list[float], prefix: str) -> pd.DataFrame:
        miss = _is_missing_date(gdf[colname].astype("object").to_numpy())
        sub = gdf.loc[~miss, [id_col, colname]].copy()
        sub["_dt"] = pd.to_datetime(sub[colname], errors="coerce")
        sub = sub[sub["_dt"].notna()].copy()

        if sub.empty:
            out = pd.DataFrame({id_col: []})
            for p in probs:
                out[f"{prefix}_{p}"] = []
            return out

        # Convertir fechas a días desde 1970 sin asumir que el dtype interno
        # está expresado en nanosegundos. GeoPackage/pyogrio puede devolver
        # datetime64[ms] o datetime64[us]; dividir siempre por 10**9 provoca
        # fechas falsas cercanas a 1970 (por ejemplo, 1970-01-20).
        epoch = pd.Timestamp("1970-01-01")
        sub["_day"] = (
            sub["_dt"].dt.floor("D").sub(epoch).dt.days.astype(np.int64)
        )

        def _nearest_rank(a_sorted: np.ndarray, p: float) -> int:
            n = a_sorted.size
            k = int(np.ceil(p * n))
            idx = max(0, min(n - 1, k - 1))
            return int(a_sorted[idx])

        rows = []
        for gid, grp in sub.groupby(id_col):
            a = np.sort(grp["_day"].to_numpy(dtype=np.int64))
            row = {id_col: gid}
            for p in probs:
                row[p] = _nearest_rank(a, p)
            rows.append(row)

        q = pd.DataFrame(rows)

        rename_map = {}
        for p in probs:
            if p == 0.25:
                rename_map[p] = f"{prefix}_q1"
            elif p == 0.75:
                rename_map[p] = f"{prefix}_q3"
            elif p == 0.10:
                rename_map[p] = f"{prefix}_p10"
            elif p == 0.90:
                rename_map[p] = f"{prefix}_p90"
            else:
                rename_map[p] = f"{prefix}_{p}"

        q = q.rename(columns=rename_map)

        for c in list(rename_map.values()):
            q[c] = pd.to_datetime(q[c], unit="D", origin="unix", utc=True).dt.strftime("%Y-%m-%d")

        return q[[id_col] + list(rename_map.values())]

    def _iqr_days_from_quantiles(out_df: pd.DataFrame, q1_col: str, q3_col: str, out_col: str) -> pd.DataFrame:
        sub = out_df[[id_col, q1_col, q3_col]].copy()
        sub["_q1"] = pd.to_datetime(sub[q1_col], errors="coerce")
        sub["_q3"] = pd.to_datetime(sub[q3_col], errors="coerce")
        sub[out_col] = (sub["_q3"] - sub["_q1"]).dt.days
        return sub[[id_col, out_col]]

    def _extremos_y_delta_days() -> pd.DataFrame:
        miss0_ = _is_missing_date(gdf[data0_col].astype("object").to_numpy())
        miss1_ = _is_missing_date(gdf[data1_col].astype("object").to_numpy())

        sub0 = gdf.loc[~miss0_, [id_col, data0_col]].copy()
        sub1 = gdf.loc[~miss1_, [id_col, data1_col]].copy()

        sub0["_dt0"] = pd.to_datetime(sub0[data0_col], errors="coerce")
        sub1["_dt1"] = pd.to_datetime(sub1[data1_col], errors="coerce")
        sub0 = sub0[sub0["_dt0"].notna()].copy()
        sub1 = sub1[sub1["_dt1"].notna()].copy()

        d0min = (
            sub0.groupby(id_col, as_index=False)["_dt0"]
            .min()
            .rename(columns={"_dt0": "Data0_min"})
        )
        d1max = (
            sub1.groupby(id_col, as_index=False)["_dt1"]
            .max()
            .rename(columns={"_dt1": "Data1_max"})
        )

        ext = d0min.merge(d1max, on=id_col, how="outer")
        ext["Data1_Data0_difference"] = (ext["Data1_max"] - ext["Data0_min"]).dt.days.astype(float)

        ext["Data0_min"] = ext["Data0_min"].dt.strftime("%Y-%m-%d")
        ext["Data1_max"] = ext["Data1_max"].dt.strftime("%Y-%m-%d")

        return ext[[id_col, "Data0_min", "Data1_max", "Data1_Data0_difference"]]

    probs = [0.10, 0.25, 0.75, 0.90]
    q0 = _quantiles_one_field(data0_col, probs=probs, prefix="Data0")
    q1 = _quantiles_one_field(data1_col, probs=probs, prefix="Data1")

    out = counts.merge(q0, on=id_col, how="left").merge(q1, on=id_col, how="left")

    iqr0 = _iqr_days_from_quantiles(out, "Data0_q1", "Data0_q3", "Data_iqr_days_data0")
    iqr1 = _iqr_days_from_quantiles(out, "Data1_q1", "Data1_q3", "Data_iqr_days_data1")
    out = out.merge(iqr0, on=id_col, how="left").merge(iqr1, on=id_col, how="left")
    out["Data_iqr_days_data0"] = out["Data_iqr_days_data0"].fillna(0.0)
    out["Data_iqr_days_data1"] = out["Data_iqr_days_data1"].fillna(0.0)

    ext = _extremos_y_delta_days()
    out = out.merge(ext, on=id_col, how="left")

    for c in [
        "Data0_p10", "Data0_q1", "Data0_q3", "Data0_p90",
        "Data1_p10", "Data1_q1", "Data1_q3", "Data1_p90",
        "Data0_min", "Data1_max", "Data1_Data0_difference",
    ]:
        if c in out.columns:
            out.loc[out["Pix_total"] == 0, c] = None

    return out


def _compute_eval_window_by_id(
    gdf: gpd.GeoDataFrame,
    stats: pd.DataFrame,
    *,
    id_col: str = "Id",
    ccdc_col: str = "Ccdc_ok",
    eccd1_col: str = "ECCD1",
    eccd2_col: str = "ECCD2",
    ok_threshold: float = 0.80,
    out_ini: str = "Temp_eval_start",
    out_fin: str = "Temp_eval_end",
) -> pd.DataFrame:
    """
    Tiempo evaluado por Id (por conversado):
    - Si ok_ratio >= 0.80:
        start = Data0_p10
        end   = Data1_p90
    - Si ok_ratio < 0.80:
        start = min(ECCD1) si existe, si no Data0_p10
        end   = max(ECCD2) si existe, si no Data1_p90
    """

    ok_ratio = (
        gdf.assign(_ok=pd.to_numeric(gdf[ccdc_col], errors="coerce").fillna(0).astype(int).eq(1))
        .groupby(id_col)["_ok"].mean()
        .reset_index()
        .rename(columns={"_ok": "_ok_ratio"})
    )

    def _min_date_str(series: pd.Series) -> str | None:
        miss = _is_missing_date(series.astype("object").to_numpy())
        dt = pd.to_datetime(series.where(~miss), errors="coerce")
        return dt.min().strftime("%Y-%m-%d") if dt.notna().any() else None

    def _max_date_str(series: pd.Series) -> str | None:
        miss = _is_missing_date(series.astype("object").to_numpy())
        dt = pd.to_datetime(series.where(~miss), errors="coerce")
        return dt.max().strftime("%Y-%m-%d") if dt.notna().any() else None

    eccd_ext = (
        gdf.groupby(id_col, as_index=False)
        .agg(_e1=(eccd1_col, _min_date_str), _e2=(eccd2_col, _max_date_str))
    )

    base = stats.merge(ok_ratio, on=id_col, how="left").merge(eccd_ext, on=id_col, how="left")

    ok = base["_ok_ratio"].fillna(0.0) >= ok_threshold
    q_ini = base.get("Data0_p10")
    q_fin = base.get("Data1_p90")

    base[out_ini] = np.where(ok, q_ini, np.where(base["_e1"].notna(), base["_e1"], q_ini))
    base[out_fin] = np.where(ok, q_fin, np.where(base["_e2"].notna(), base["_e2"], q_fin))

    return base[[id_col, out_ini, out_fin]]


def _validate_stats_by_id_against_pixels(
    gdf_pix: gpd.GeoDataFrame,
    stats_by_id: pd.DataFrame,
    *,
    id_col: str = "Id",
    data0_col: str = "Data0",
    data1_col: str = "Data1",
    date_tol_days: int = 0,
) -> pd.DataFrame:
    """
    Recalcula stats desde gdf_pix usando EXACTAMENTE el mismo método de _compute_stats_by_id
    y compara contra 'stats_by_id'. Retorna DataFrame con discrepancias (Id, field, given, recalculated).
    Si retorna vacío => OK.
    """
    re = _compute_stats_by_id(gdf_pix, id_col=id_col, data0_col=data0_col, data1_col=data1_col)

    common = [c for c in stats_by_id.columns if c in re.columns and c != id_col]
    if not common:
        return pd.DataFrame(columns=[id_col, "field", "given", "recalc"])

    cmp = (
        stats_by_id[[id_col] + common]
        .merge(re[[id_col] + common], on=id_col, how="outer", suffixes=("_given", "_recalc"))
    )

    diffs = []

    date_stat_fields = {
        "Data0_p10", "Data0_q1", "Data0_q3", "Data0_p90",
        "Data1_p10", "Data1_q1", "Data1_q3", "Data1_p90",
        "Data0_min", "Data1_max",
    }
    date_fields = [c for c in common if c in date_stat_fields]
    num_fields = [c for c in common if c not in date_fields]

    for f in date_fields:
        a = pd.to_datetime(cmp[f"{f}_given"], errors="coerce")
        b = pd.to_datetime(cmp[f"{f}_recalc"], errors="coerce")

        both_nan = a.isna() & b.isna()
        ok = both_nan.copy()

        both = a.notna() & b.notna()
        if date_tol_days == 0:
            ok.loc[both] = (a.loc[both].dt.floor("D") == b.loc[both].dt.floor("D")).to_numpy()
        else:
            ok.loc[both] = (
                (a.loc[both].dt.floor("D") - b.loc[both].dt.floor("D")).dt.days.abs() <= date_tol_days
            ).to_numpy()

        bad = ~ok
        if bool(bad.any()):
            sub = cmp.loc[bad, [id_col, f"{f}_given", f"{f}_recalc"]].copy()
            sub = sub.rename(columns={f"{f}_given": "given", f"{f}_recalc": "recalc"})
            sub["field"] = f
            diffs.append(sub)

    for f in num_fields:
        a = pd.to_numeric(cmp[f"{f}_given"], errors="coerce")
        b = pd.to_numeric(cmp[f"{f}_recalc"], errors="coerce")
        both_nan = a.isna() & b.isna()
        close = np.isclose(a.fillna(0.0), b.fillna(0.0), atol=1e-12, rtol=0.0) | both_nan
        bad = ~close
        if bool(bad.any()):
            sub = cmp.loc[bad, [id_col, f"{f}_given", f"{f}_recalc"]].copy()
            sub = sub.rename(columns={f"{f}_given": "given", f"{f}_recalc": "recalc"})
            sub["field"] = f
            diffs.append(sub)

    if not diffs:
        return pd.DataFrame(columns=[id_col, "field", "given", "recalc"])

    out = pd.concat(diffs, ignore_index=True)
    out = out[[id_col, "field", "given", "recalc"]]
    return out


def _validate_columns_constant_within_id(
    gdf: gpd.GeoDataFrame,
    *,
    id_col: str = "Id",
    include_cols: list[str] | None = None,
) -> pd.DataFrame:
    """
    Revisa constancia SOLO para include_cols (si se pasa).
    Retorna filas (Id, field, nunique) donde nunique>1.
    """
    geom_col = gdf.geometry.name

    if include_cols is None:
        cols = [c for c in gdf.columns if c not in {id_col, geom_col}]
    else:
        cols = [c for c in include_cols if c in gdf.columns and c not in {id_col, geom_col}]

    if not cols:
        return pd.DataFrame(columns=[id_col, "field", "nunique"])

    nun = gdf.groupby(id_col)[cols].nunique(dropna=False)
    bad = (nun > 1)
    if not bool(bad.any().any()):
        return pd.DataFrame(columns=[id_col, "field", "nunique"])

    rows = []
    bad_ids = bad.any(axis=1)
    for gid, row in nun.loc[bad_ids].iterrows():
        for c in cols:
            v = row[c]
            if v > 1:
                rows.append({id_col: gid, "field": c, "nunique": int(v)})

    return pd.DataFrame(rows)


def nvg_pipeline_pixels_normal_one_gpkg(
    input_pixels_shp: str,
    admin_areas_shp: str,
    out_gpkg: str,
    *,
    out_stats_csv: str | None = None,
    target_crs: str = "EPSG:3763",
    # NUEVO: validación enfocada
    run_validation: bool = True,
    validation_raise: bool = False,
    validation_report_csv: str | None = None,
    date_tol_days: int = 0,
) -> None:
    """
    Produce:
      - layer Pixels_con_chk_p10
      - layer PorId_dissolve_sin_Data0_Data1

    VALIDACIÓN (enfocada a métricas):
      - stats_vs_recalc: compara stats_by_id vs recalculo desde pixels
      - non_constant_before_dissolve: chequea constancia SOLO en columnas que deben ser constantes (stats/eval/ccdc)
    """

    # 1) Normaliza + filtra pixeles
    gdf, _ = _normalize_and_filter_pixels(
        input_pixels_path=input_pixels_shp,
        admin_areas_path=admin_areas_shp,
        target_crs=target_crs,
    )

    # 2) Stats por Id
    stats = _compute_stats_by_id(gdf, id_col="Id", data0_col="Data0", data1_col="Data1")

    # 3) Ventana de evaluación por Id
    eval_df = _compute_eval_window_by_id(gdf, stats, id_col="Id", ok_threshold=0.80)

    # 4) Resumen CCDC por Id (Validation_flag a nivel Id)
    ccdc_id = _ccdc_summary_by_id(gdf, id_col="Id", ccdc_col="Ccdc_ok", ok_threshold=0.80)

    # 5) Enriquecer pixeles con stats + eval + Validation_flag
    gdf_out = (
        gdf.merge(stats, on="Id", how="left")
        .merge(eval_df, on="Id", how="left")
        .merge(ccdc_id, on="Id", how="left")
    )

    # -------------------------------------------------------------
    # CAPA A: Pixel-level con chequeo SOLO para Data1_p10 (mediana por Id)
    # -------------------------------------------------------------
    p10_col = "Data1_p10" if "Data1_p10" in gdf_out.columns else ("Data1_p10" if "Data1_p10" in gdf_out.columns else None)
    if p10_col is None:
        raise ValueError("No se encontró la columna Data1_p10 ni Data1_p10 para calcular la mediana por Id.")

    med_df = _median_date_by_id_for_one_col(gdf_out, p10_col, id_col="Id")
    gdf_chk = gdf_out.merge(med_df, on="Id", how="left")

    a = pd.to_datetime(gdf_chk[p10_col], errors="coerce")
    b = pd.to_datetime(gdf_chk[f"{p10_col}_med_id"], errors="coerce")
    both = a.notna() & b.notna()
    gdf_chk["Chk_p10_median_eq_value_by_id"] = None
    gdf_chk.loc[both, "Chk_p10_median_eq_value_by_id"] = (
        a.loc[both].dt.floor("D").to_numpy() == b.loc[both].dt.floor("D").to_numpy()
    )

    _write_vector(gdf_chk.drop(columns=["Ccdc_ok"], errors="ignore"), out_gpkg, layer="Pixels_con_chk_p10")

    # -------------------------------------------------------------
    # CAPA B: Por Id (dissolve), SIN Data0/Data1, con IDs nuevos + Area_ha
    # -------------------------------------------------------------
    gdf_nodates = gdf_out.drop(columns=["Data0", "Data1"], errors="ignore")
    gdf_nodates = gdf_nodates.drop(columns=["Ccdc_ok"], errors="ignore")

    # =========================
    # VALIDACIÓN (ENFOCADA)
    # =========================
    if run_validation:
        reports = []

        # (A) stats_by_id vs recalculo desde pixeles (esto es lo importante)
        diff_stats = _validate_stats_by_id_against_pixels(
            gdf_pix=gdf,
            stats_by_id=stats,
            id_col="Id",
            data0_col="Data0",
            data1_col="Data1",
            date_tol_days=date_tol_days,
        )
        diff_stats["check"] = "stats_vs_recalc"
        reports.append(diff_stats)

        # (B) constancia SOLO en columnas que deben ser constantes por Id
        must_be_constant = (
            [c for c in stats.columns if c != "Id"]
            + [c for c in eval_df.columns if c != "Id"]
            + [c for c in ccdc_id.columns if c != "Id"]
        )
        # si esperas que Pi_dicofre sea constante por Id, añádela:
        if "Pi_dicofre" in gdf_nodates.columns:
            must_be_constant.append("Pi_dicofre")
            
            print("\n[VALIDATION] Columns checked for constancy (must_be_constant):")
            for c in must_be_constant:
                print(" -", c)

        diff_const = _validate_columns_constant_within_id(
            gdf=gdf_nodates,
            id_col="Id",
            include_cols=must_be_constant,
        )

        if not diff_const.empty:
            diff_const = diff_const.copy()
            diff_const["given"] = None
            diff_const["recalc"] = None
            diff_const["check"] = "non_constant_before_dissolve"
            diff_const = diff_const.rename(columns={"field": "field"})
            diff_const = diff_const[[ "Id", "field", "given", "recalc", "check" ]]
        else:
            diff_const = pd.DataFrame(columns=["Id", "field", "given", "recalc", "check"])

        reports.append(diff_const)

        report = pd.concat(reports, ignore_index=True) if reports else pd.DataFrame(
            columns=["Id", "field", "given", "recalc", "check"]
        )

        # evitar repetidos
        if not report.empty:
            report = report.drop_duplicates(subset=["Id", "field", "check"], keep="first")

        if validation_report_csv is not None:
            p = Path(validation_report_csv)
            p.parent.mkdir(parents=True, exist_ok=True)
            report.to_csv(p, index=False)

        if validation_raise and not report.empty:
            top = report.head(20).to_string(index=False)
            raise ValueError(
                "Validation failed: discrepancies detected in aggregated stats and/or non-constant Id-level fields.\n"
                f"Total issues: {len(report)}\n"
                f"First rows:\n{top}\n"
                f"{'Report saved to: ' + str(validation_report_csv) if validation_report_csv else ''}"
            )

    # Dissolve (ahora ya auditado para las columnas relevantes)
    gdf_id = _dissolve_by_id_keep_first(gdf_nodates, id_col="Id")

    # Area en ha
    gdf_id = _add_area_ha(gdf_id, out_col="Area_ha")

    # IDs nuevos
    gdf_id = _add_dissolved_ids(gdf_id, src_value="nvg", uid_prefix="nvg_")

    _write_vector(gdf_id, out_gpkg, layer="PorId_dissolve_sin_Data0_Data1")

    # opcional: CSV de stats
    if out_stats_csv is not None:
        Path(out_stats_csv).parent.mkdir(parents=True, exist_ok=True)
        stats.to_csv(out_stats_csv, index=False)