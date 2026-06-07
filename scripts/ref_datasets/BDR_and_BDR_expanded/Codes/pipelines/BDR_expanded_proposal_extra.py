# -*- coding: utf-8 -*-
# Full flow: normal + centered grids, each producing:
#   1) candidates squares layer
#   2) selected ONE layer containing squares (role='tile') + intersection pieces (role='piece')
#   3) selected TILE-ONLY layer (only the square, no intersection pieces)
# Fix: category is recomputed FROM ACTUAL PIECES (guarantees that BDR+ICNF really has BDR pieces)
# Added: adjustable "how centered" via CENTER_STRENGTH (0..1)
# Added now: flexible category filling so NVG_ONLY / ICNF_ONLY are not starved

from typing import Dict, Any, List, Tuple, Callable

from qgis.PyQt.QtCore import QVariant
from qgis.PyQt.QtGui import QColor
from qgis.core import (
    QgsProject, QgsVectorLayer, QgsFeature, QgsFields, QgsField,
    QgsCoordinateReferenceSystem, QgsRectangle, QgsSymbol,
    QgsRendererCategory, QgsCategorizedSymbolRenderer
)
import processing

# ============================================================
# CONFIG
# ============================================================
YEAR = 2020
GRID_KM = 1
TARGET_CRS = QgsCoordinateReferenceSystem("EPSG:3763")

EXCLUDE_BDREXP_TILES = True

# presence thresholds (ha)
MIN_HA_PRESENT = 0.5
MIN_HA_PRESENT_BDR = 0.05  # more permissive when recomputing category from pieces

# --- SINGLE-LAYER "fotointerpretable" filters ---
ICNF_ONLY_MAX_HA = 40.0
ICNF_ONLY_MAXPART_HA = 25.0
ICNF_ONLY_MIN_COUNT = 2

NVG_ONLY_MAX_HA = 40.0
NVG_ONLY_MAXPART_HA = 25.0
NVG_ONLY_MIN_COUNT = 2

# soft penalty for ranking
ICNF_MAXPART_HA_SOFT = 60.0

# spacing
MIN_SEP_KM = 3.0
TARGET_N_TOTAL = 80

# quotas
QUOTAS = {
    "NOEVENT": 15,
    "NVG_ONLY": 10,
    "ICNF_ONLY": 10,
    "NVG+ICNF": 15,
    "BDR+ICNF": 10,
    "BDR+NVG": 10,
    "BDR+ICNF+NVG": 10,
}
POOL_MULT = 20

# adjustable "how centered" (0=no shift, 1=full center shift)
CENTER_STRENGTH = 1.0   # try 0.5 if you want "slightly centered"

# flexible filling behaviour
ENABLE_FLEX_CATEGORY_FILL = True

# date fields
DATE_FIELD = {
    "BDR": "Data0",
    "BDRexp": "Data0",
    "ICNF": "Data0",
    "NVG": "Data0_min",
}

# layer names (exact)
LAYER_NAMES = {
    "BDR": "BDR_CCDC_TNE_v3_harmonized",
    "BDRexp": "BDR_expanded",
    "ICNF": "ICNF_2020_2024_harmonized — ICNF_2020",
    "NVG": "NVG_propios_split_by_harmonized_keep_propios — NVG_propios_after_dissolve_FINAL",
}

# outputs: 3 per version
OUT_CAND_BASE = f"tiles_candidates_{YEAR}_strata"
OUT_SEL_BASE  = f"tiles_selected_{YEAR}_strata"
OUT_SEL_TILEONLY_BASE = f"tiles_selected_{YEAR}_strata_tileonly"

OUT_CAND_CENTER = f"tiles_candidates_{YEAR}_strata_centered"
OUT_SEL_CENTER  = f"tiles_selected_{YEAR}_strata_centered"
OUT_SEL_TILEONLY_CENTER = f"tiles_selected_{YEAR}_strata_centered_tileonly"

# optional extra fields (keep if exist in pieces)
EXTRA_FIELDS_TRY = ["Classe_0", "Classe_1", "Chg_type", "Pidi_cofre"]


# ============================================================
# HELPERS
# ============================================================
def get_layer_by_name(name: str) -> QgsVectorLayer:
    ls = QgsProject.instance().mapLayersByName(name)
    if not ls:
        raise RuntimeError(f"No encuentro la capa: {name}")
    lyr = ls[0]
    if not lyr.isValid():
        raise RuntimeError(f"Capa inválida: {name}")
    return lyr

def fix_geoms(layer: QgsVectorLayer) -> QgsVectorLayer:
    return processing.run("native:fixgeometries", {"INPUT": layer, "OUTPUT": "memory:"})["OUTPUT"]

def to_crs(layer: QgsVectorLayer, crs: QgsCoordinateReferenceSystem) -> QgsVectorLayer:
    if layer.crs() == crs:
        return layer
    return processing.run("native:reprojectlayer", {"INPUT": layer, "TARGET_CRS": crs, "OUTPUT": "memory:"})["OUTPUT"]

def filter_year(layer: QgsVectorLayer, date_field: str, year: int) -> QgsVectorLayer:
    y0, y1 = f"{year}-01-01", f"{year}-12-31"
    expr = f"""to_date("{date_field}") >= to_date('{y0}') AND to_date("{date_field}") <= to_date('{y1}')"""
    return processing.run("native:extractbyexpression", {"INPUT": layer, "EXPRESSION": expr, "OUTPUT": "memory:"})["OUTPUT"]

def dissolve_all(layer: QgsVectorLayer) -> QgsVectorLayer:
    return processing.run("native:dissolve", {"INPUT": layer, "FIELD": [], "OUTPUT": "memory:"})["OUTPUT"]

def difference(a: QgsVectorLayer, b: QgsVectorLayer) -> QgsVectorLayer:
    return processing.run("native:difference", {"INPUT": a, "OVERLAY": b, "OUTPUT": "memory:"})["OUTPUT"]

def ensure_nvg_data0_data1(nvg_layer: QgsVectorLayer) -> QgsVectorLayer:
    tmp = processing.run(
        "native:fieldcalculator",
        {"INPUT": nvg_layer, "FIELD_NAME": "Data0", "FIELD_TYPE": 5, "FIELD_LENGTH": 10,
         "FIELD_PRECISION": 0, "FORMULA": 'to_date("Data0_min")', "OUTPUT": "memory:"}
    )["OUTPUT"]
    out = processing.run(
        "native:fieldcalculator",
        {"INPUT": tmp, "FIELD_NAME": "Data1", "FIELD_TYPE": 5, "FIELD_LENGTH": 10,
         "FIELD_PRECISION": 0, "FORMULA": 'to_date("Data1_max")', "OUTPUT": "memory:"}
    )["OUTPUT"]
    return out

def make_grid(extent, crs: QgsCoordinateReferenceSystem, grid_km: int) -> QgsVectorLayer:
    step = grid_km * 1000.0
    grid = processing.run(
        "native:creategrid",
        {"TYPE": 2, "EXTENT": extent, "HSPACING": step, "VSPACING": step,
         "HOVERLAY": 0, "VOVERLAY": 0, "CRS": crs, "OUTPUT": "memory:"}
    )["OUTPUT"]
    grid = processing.run(
        "native:fieldcalculator",
        {"INPUT": grid, "FIELD_NAME": "tile_id", "FIELD_TYPE": 1, "FIELD_LENGTH": 10,
         "FIELD_PRECISION": 0, "FORMULA": "@row_number", "OUTPUT": "memory:"}
    )["OUTPUT"]
    return fix_geoms(grid)

def make_grid_centered(extent, crs: QgsCoordinateReferenceSystem, grid_km: int, strength: float) -> QgsVectorLayer:
    """
    strength: 0..1, how much of the centering shift to apply.
    """
    strength = max(0.0, min(1.0, float(strength)))
    step = grid_km * 1000.0

    xmin, xmax = extent.xMinimum(), extent.xMaximum()
    ymin, ymax = extent.yMinimum(), extent.yMaximum()

    width = xmax - xmin
    height = ymax - ymin

    rx = width % step
    ry = height % step

    # shift to center
    sx = (rx / 2.0) * strength
    sy = (ry / 2.0) * strength

    # expand to next multiple (independent of strength)
    xmax2 = xmax + (step - (rx / 2.0) if rx != 0 else 0)
    ymax2 = ymax + (step - (ry / 2.0) if ry != 0 else 0)

    # apply partial shift
    ext2 = QgsRectangle(xmin - sx, ymin - sy, xmax2, ymax2)
    return make_grid(ext2, crs, grid_km)

def tile_ids_intersecting(grid: QgsVectorLayer, mask: QgsVectorLayer) -> set:
    inter = processing.run("native:intersection", {"INPUT": grid, "OVERLAY": mask, "OUTPUT": "memory:"})["OUTPUT"]
    return {int(f["tile_id"]) for f in inter.getFeatures()}

def intersect_stats_by_tile(grid: QgsVectorLayer, layer: QgsVectorLayer) -> Dict[int, Dict[str, float]]:
    inter = processing.run("native:intersection", {"INPUT": grid, "OVERLAY": layer, "OUTPUT": "memory:"})["OUTPUT"]
    inter = processing.run(
        "native:fieldcalculator",
        {"INPUT": inter, "FIELD_NAME": "a_ha", "FIELD_TYPE": 0, "FIELD_LENGTH": 20,
         "FIELD_PRECISION": 6, "FORMULA": "$area/10000", "OUTPUT": "memory:"}
    )["OUTPUT"]

    out: Dict[int, Dict[str, float]] = {}
    for f in inter.getFeatures():
        tid = int(f["tile_id"])
        a = float(f["a_ha"] or 0.0)
        if tid not in out:
            out[tid] = {"sum_ha": 0.0, "maxpart_ha": 0.0, "count": 0.0}
        out[tid]["sum_ha"] += a
        out[tid]["maxpart_ha"] = max(out[tid]["maxpart_ha"], a)
        out[tid]["count"] += 1.0
    return out

def apply_category_symbology(vlayer: QgsVectorLayer) -> None:
    colors = {
        "NOEVENT": QColor("#9e9e9e"),
        "NVG_ONLY": QColor("#4daf4a"),
        "ICNF_ONLY": QColor("#ffcc33"),
        "NVG+ICNF": QColor("#1b9e77"),
        "BDR+ICNF": QColor("#d95f02"),
        "BDR+NVG": QColor("#7570b3"),
        "BDR+ICNF+NVG": QColor("#e7298a"),
    }
    cats = []
    for k, col in colors.items():
        sym = QgsSymbol.defaultSymbol(vlayer.geometryType())
        sym.setColor(col)
        sym.symbolLayer(0).setStrokeColor(QColor("#000000"))
        sym.symbolLayer(0).setStrokeWidth(0.4)
        cats.append(QgsRendererCategory(k, sym, k))
    vlayer.setRenderer(QgsCategorizedSymbolRenderer("category", cats))
    vlayer.triggerRepaint()

def make_selected_output_layer(name: str, crs_authid: str) -> QgsVectorLayer:
    flds = QgsFields()
    flds.append(QgsField("tile_id", QVariant.Int))
    flds.append(QgsField("category", QVariant.String))
    flds.append(QgsField("role", QVariant.String))   # tile / piece
    flds.append(QgsField("src", QVariant.String))    # TILE / BDR / ICNF / NVG / NOEVENT
    flds.append(QgsField("Data0", QVariant.Date))
    flds.append(QgsField("Data1", QVariant.Date))
    for nm in EXTRA_FIELDS_TRY:
        flds.append(QgsField(nm, QVariant.String))

    out = QgsVectorLayer(f"Polygon?crs={crs_authid}", name, "memory")
    out.dataProvider().addAttributes(list(flds))
    out.updateFields()
    return out

def make_tileonly_output_layer(name: str, crs_authid: str) -> QgsVectorLayer:
    flds = QgsFields()
    flds.append(QgsField("tile_id", QVariant.Int))
    flds.append(QgsField("category", QVariant.String))

    out = QgsVectorLayer(f"Polygon?crs={crs_authid}", name, "memory")
    out.dataProvider().addAttributes(list(flds))
    out.updateFields()
    return out

def add_tile_feature(out_layer: QgsVectorLayer, tile_id: int, category: str, geom) -> None:
    f = QgsFeature(out_layer.fields())
    f.setGeometry(geom)
    attrs = [tile_id, category, "tile", "TILE", None, None] + [None for _ in EXTRA_FIELDS_TRY]
    f.setAttributes(attrs)
    out_layer.dataProvider().addFeatures([f])

def append_piece_features(out_layer: QgsVectorLayer, tiles_sel: QgsVectorLayer, src_layer: QgsVectorLayer, *, src_tag: str) -> None:
    # 1) Fix geometries (both sides)
    tiles_sel2 = fix_geoms(tiles_sel)
    src2 = fix_geoms(src_layer)

    # 2) Explode multiparts (important)
    tiles_sel2 = processing.run("native:multiparttosingleparts", {"INPUT": tiles_sel2, "OUTPUT": "memory:"})["OUTPUT"]
    src2 = processing.run("native:multiparttosingleparts", {"INPUT": src2, "OUTPUT": "memory:"})["OUTPUT"]

    # 3) Intersection
    inter = processing.run(
        "native:intersection",
        {"INPUT": tiles_sel2, "OVERLAY": src2, "OUTPUT": "memory:"}
    )["OUTPUT"]

    names = inter.fields().names()

    # Normalize dates if exist
    if "Data0" in names:
        inter = processing.run(
            "native:fieldcalculator",
            {"INPUT": inter, "FIELD_NAME": "Data0", "FIELD_TYPE": 5, "FIELD_LENGTH": 10,
             "FIELD_PRECISION": 0, "FORMULA": 'to_date("Data0")', "OUTPUT": "memory:"}
        )["OUTPUT"]

    if "Data1" in names:
        inter = processing.run(
            "native:fieldcalculator",
            {"INPUT": inter, "FIELD_NAME": "Data1", "FIELD_TYPE": 5, "FIELD_LENGTH": 10,
             "FIELD_PRECISION": 0, "FORMULA": 'to_date("Data1")', "OUTPUT": "memory:"}
        )["OUTPUT"]

    names = inter.fields().names()

    for r in inter.getFeatures():
        f = QgsFeature(out_layer.fields())
        f.setGeometry(r.geometry())

        tid = r["tile_id"] if "tile_id" in names else None
        cat = r["category"] if "category" in names else None
        d0 = r["Data0"] if "Data0" in names else None
        d1 = r["Data1"] if "Data1" in names else None

        extra_vals = {nm: (r[nm] if nm in names else None) for nm in EXTRA_FIELDS_TRY}
        attrs = [tid, cat, "piece", src_tag, d0, d1] + [extra_vals[nm] for nm in EXTRA_FIELDS_TRY]
        f.setAttributes(attrs)

        out_layer.dataProvider().addFeatures([f])

def recompute_category_from_pieces(out_layer: QgsVectorLayer) -> None:
    """
    Guarantees category matches actual pieces:
      - if category is BDR+ICNF, there WILL be BDR pieces in that tile (>= threshold)
    Applies to BOTH role=tile and role=piece features.
    Keeps rule: never BDR-only -> becomes NOEVENT.
    """
    # Sum area per tile_id per src (only role='piece')
    sum_by_tile_src: Dict[Tuple[int, str], float] = {}
    for f in out_layer.getFeatures():
        if f["role"] != "piece":
            continue
        tid = f["tile_id"]
        src = f["src"]
        if tid is None or src is None:
            continue
        a_ha = f.geometry().area() / 10000.0
        sum_by_tile_src[(int(tid), str(src))] = sum_by_tile_src.get((int(tid), str(src)), 0.0) + a_ha

    def has_src(tid: int, src: str) -> bool:
        thr = MIN_HA_PRESENT_BDR if src == "BDR" else MIN_HA_PRESENT
        return sum_by_tile_src.get((tid, src), 0.0) >= thr

    def compute_cat(tid: int) -> str:
        pb = has_src(tid, "BDR")
        pi = has_src(tid, "ICNF")
        pn = has_src(tid, "NVG")

        if (not pb) and (not pi) and (not pn):
            return "NOEVENT"
        if pb and (not pi) and (not pn):
            return "NOEVENT"  # never BDR-only
        if (not pb) and pi and (not pn):
            return "ICNF_ONLY"
        if (not pb) and (not pi) and pn:
            return "NVG_ONLY"
        if (not pb) and pi and pn:
            return "NVG+ICNF"
        if pb and pi and (not pn):
            return "BDR+ICNF"
        if pb and (not pi) and pn:
            return "BDR+NVG"
        if pb and pi and pn:
            return "BDR+ICNF+NVG"
        return "NOEVENT"

    out_layer.startEditing()
    for f in out_layer.getFeatures():
        tid = f["tile_id"]
        if tid is None:
            continue
        new_cat = compute_cat(int(tid))
        f["category"] = new_cat
        out_layer.updateFeature(f)
    out_layer.commitChanges()


# ============================================================
# PIPELINE PER VERSION
# ============================================================
def run_version(
    *,
    version_name: str,
    grid_builder: Callable,
    out_cand_name: str,
    out_sel_name: str,
    out_sel_tileonly_name: str,
    ext,
    bdrexp_mask: QgsVectorLayer,
    bdr_nonexp: QgsVectorLayer,
    icnf_2020: QgsVectorLayer,
    nvg_2020_std: QgsVectorLayer,
    centered_strength: float = 1.0
) -> None:
    # grid
    if grid_builder.__name__ == "make_grid_centered":
        grid = grid_builder(ext, TARGET_CRS, GRID_KM, centered_strength)
    else:
        grid = grid_builder(ext, TARGET_CRS, GRID_KM)

    exclude_ids = set()
    if EXCLUDE_BDREXP_TILES:
        exclude_ids = tile_ids_intersecting(grid, bdrexp_mask)

    # stats
    S_bdr = intersect_stats_by_tile(grid, bdr_nonexp)
    S_icnf = intersect_stats_by_tile(grid, icnf_2020)
    S_nvg = intersect_stats_by_tile(grid, nvg_2020_std)

    # OUTPUT 1: candidates squares
    cand_fields = QgsFields()
    cand_fields.append(QgsField("tile_id", QVariant.Int))
    cand_fields.append(QgsField("category", QVariant.String))
    cand_fields.append(QgsField("ha_BDR", QVariant.Double))
    cand_fields.append(QgsField("ha_ICNF", QVariant.Double))
    cand_fields.append(QgsField("ha_NVG", QVariant.Double))
    cand_fields.append(QgsField("cnt_BDR", QVariant.Int))
    cand_fields.append(QgsField("cnt_ICNF", QVariant.Int))
    cand_fields.append(QgsField("cnt_NVG", QVariant.Int))
    cand_fields.append(QgsField("maxp_ICNF", QVariant.Double))
    cand_fields.append(QgsField("maxp_NVG", QVariant.Double))
    cand_fields.append(QgsField("strict_ok", QVariant.Int))

    cand = QgsVectorLayer(f"Polygon?crs={TARGET_CRS.authid()}", out_cand_name, "memory")
    cand.dataProvider().addAttributes(list(cand_fields))
    cand.updateFields()

    rows: List[Dict[str, Any]] = []

    for gf in grid.getFeatures():
        tid = int(gf["tile_id"])
        if EXCLUDE_BDREXP_TILES and tid in exclude_ids:
            continue

        ha_b = float(S_bdr.get(tid, {}).get("sum_ha", 0.0))
        ha_i = float(S_icnf.get(tid, {}).get("sum_ha", 0.0))
        ha_n = float(S_nvg.get(tid, {}).get("sum_ha", 0.0))

        cnt_b = int(S_bdr.get(tid, {}).get("count", 0.0))
        cnt_i = int(S_icnf.get(tid, {}).get("count", 0.0))
        cnt_n = int(S_nvg.get(tid, {}).get("count", 0.0))

        maxp_i = float(S_icnf.get(tid, {}).get("maxpart_ha", 0.0))
        maxp_n = float(S_nvg.get(tid, {}).get("maxpart_ha", 0.0))

        p_b = ha_b >= MIN_HA_PRESENT
        p_i = ha_i >= MIN_HA_PRESENT
        p_n = ha_n >= MIN_HA_PRESENT

        # Category (never allow BDR-only)
        if (not p_b) and (not p_i) and (not p_n):
            category = "NOEVENT"
        elif p_b and (not p_i) and (not p_n):
            continue
        elif (not p_b) and p_i and (not p_n):
            category = "ICNF_ONLY"
        elif (not p_b) and (not p_i) and p_n:
            category = "NVG_ONLY"
        elif (not p_b) and p_i and p_n:
            category = "NVG+ICNF"
        elif p_b and p_i and (not p_n):
            category = "BDR+ICNF"
        elif p_b and (not p_i) and p_n:
            category = "BDR+NVG"
        elif p_b and p_i and p_n:
            category = "BDR+ICNF+NVG"
        else:
            category = "NOEVENT"

        ha_total = ha_b + ha_i + ha_n
        if category != "NOEVENT" and ha_total < 1.0:
            continue

        strict_ok = 1

        # Single-layer constraints are now preferences, not immediate hard kills
        if category == "ICNF_ONLY":
            if ha_i > ICNF_ONLY_MAX_HA or maxp_i > ICNF_ONLY_MAXPART_HA or cnt_i < ICNF_ONLY_MIN_COUNT:
                strict_ok = 0

        if category == "NVG_ONLY":
            if ha_n > NVG_ONLY_MAX_HA or maxp_n > NVG_ONLY_MAXPART_HA or cnt_n < NVG_ONLY_MIN_COUNT:
                strict_ok = 0

        v = {
            "tile_id": tid,
            "geom": gf.geometry(),
            "category": category,
            "ha_BDR": ha_b, "ha_ICNF": ha_i, "ha_NVG": ha_n,
            "cnt_BDR": cnt_b, "cnt_ICNF": cnt_i, "cnt_NVG": cnt_n,
            "maxp_ICNF": maxp_i, "maxp_NVG": maxp_n,
            "strict_ok": strict_ok,
        }
        rows.append(v)

        ff = QgsFeature(cand.fields())
        ff.setGeometry(v["geom"])
        ff.setAttributes([
            v["tile_id"], v["category"], v["ha_BDR"], v["ha_ICNF"], v["ha_NVG"],
            v["cnt_BDR"], v["cnt_ICNF"], v["cnt_NVG"], v["maxp_ICNF"], v["maxp_NVG"],
            v["strict_ok"]
        ])
        cand.dataProvider().addFeatures([ff])

    cand.updateExtents()
    QgsProject.instance().addMapLayer(cand)
    apply_category_symbology(cand)
    print(f"[{version_name}] candidates: {len(rows)} -> {out_cand_name}")

    # selection
    by_cat: Dict[str, List[Dict[str, Any]]] = {}
    for v in rows:
        by_cat.setdefault(v["category"], []).append(v)

    def quality_key(v: Dict[str, Any]) -> Tuple:
        strict_pen = 0 if v.get("strict_ok", 1) == 1 else 1
        frag = v["cnt_ICNF"] + v["cnt_NVG"] + v["cnt_BDR"]
        ic_pen = 1 if v["maxp_ICNF"] > ICNF_MAXPART_HA_SOFT else 0
        nonbdr = v["ha_ICNF"] + v["ha_NVG"]
        return (strict_pen, ic_pen, -frag, -nonbdr)

    for cat in by_cat:
        by_cat[cat] = sorted(by_cat[cat], key=quality_key)

    min_sep_m = MIN_SEP_KM * 1000.0
    selected: List[Dict[str, Any]] = []
    selected_pts = []
    selected_ids = set()

    def can_add(v: Dict[str, Any], sep_m: float) -> bool:
        c = v["geom"].centroid().asPoint()
        for p in selected_pts:
            if c.distance(p) < sep_m:
                return False
        return True

    def add_selected(v: Dict[str, Any]) -> None:
        tid = int(v["tile_id"])
        selected.append(v)
        selected_pts.append(v["geom"].centroid().asPoint())
        selected_ids.add(tid)

    # pass 1: strict + normal spacing
    for cat, quota in QUOTAS.items():
        pool = by_cat.get(cat, [])
        want = quota
        take_pool = pool[: min(len(pool), quota * POOL_MULT)]

        strict_pool = [v for v in take_pool if v.get("strict_ok", 1) == 1]
        relaxed_pool = [v for v in take_pool if v.get("strict_ok", 1) != 1]

        for v in strict_pool:
            if want <= 0:
                break
            if int(v["tile_id"]) in selected_ids:
                continue
            if can_add(v, min_sep_m):
                add_selected(v)
                want -= 1

        # pass 2: relaxed candidates, still respecting spacing
        if ENABLE_FLEX_CATEGORY_FILL and want > 0:
            for v in relaxed_pool:
                if want <= 0:
                    break
                if int(v["tile_id"]) in selected_ids:
                    continue
                if can_add(v, min_sep_m):
                    add_selected(v)
                    want -= 1

        # pass 3: fill category ignoring spacing if still empty/short
        if ENABLE_FLEX_CATEGORY_FILL and want > 0:
            for v in take_pool:
                if want <= 0:
                    break
                if int(v["tile_id"]) in selected_ids:
                    continue
                add_selected(v)
                want -= 1

        got = quota - want
        print(f"[{version_name}] category {cat}: selected {got} / {quota}")

    if len(selected) < TARGET_N_TOTAL:
        remaining = TARGET_N_TOTAL - len(selected)
        all_pool = sorted(rows, key=quality_key)

        # first remaining pass with spacing
        for v in all_pool:
            if remaining <= 0:
                break
            if int(v["tile_id"]) in selected_ids:
                continue
            if can_add(v, min_sep_m):
                add_selected(v)
                remaining -= 1

        # second remaining pass ignoring spacing
        if ENABLE_FLEX_CATEGORY_FILL and remaining > 0:
            for v in all_pool:
                if remaining <= 0:
                    break
                if int(v["tile_id"]) in selected_ids:
                    continue
                add_selected(v)
                remaining -= 1

    # tiles_sel temp
    tiles_sel = QgsVectorLayer(f"Polygon?crs={TARGET_CRS.authid()}", "tiles_sel_tmp", "memory")
    tiles_sel.dataProvider().addAttributes([QgsField("tile_id", QVariant.Int), QgsField("category", QVariant.String)])
    tiles_sel.updateFields()
    for v in selected:
        f = QgsFeature(tiles_sel.fields())
        f.setGeometry(v["geom"])
        f.setAttributes([v["tile_id"], v["category"]])
        tiles_sel.dataProvider().addFeatures([f])
    tiles_sel.updateExtents()

    # OUTPUT 2: one layer squares + pieces
    out = make_selected_output_layer(out_sel_name, TARGET_CRS.authid())

    # tiles
    for f in tiles_sel.getFeatures():
        add_tile_feature(out, int(f["tile_id"]), str(f["category"]), f.geometry())

    # pieces
    append_piece_features(out, tiles_sel, bdr_nonexp, src_tag="BDR")
    append_piece_features(out, tiles_sel, icnf_2020, src_tag="ICNF")
    append_piece_features(out, tiles_sel, nvg_2020_std, src_tag="NVG")

    print(
        "OUT counts:",
        "tile =", sum(1 for f in out.getFeatures() if f["role"] == "tile"),
        "piece=", sum(1 for f in out.getFeatures() if f["role"] == "piece")
    )

    # optional NOEVENT full squares as piece
    for f in tiles_sel.getFeatures():
        if f["category"] != "NOEVENT":
            continue
        nf = QgsFeature(out.fields())
        nf.setGeometry(f.geometry())
        attrs = [int(f["tile_id"]), str(f["category"]), "piece", "NOEVENT", None, None] + [None for _ in EXTRA_FIELDS_TRY]
        nf.setAttributes(attrs)
        out.dataProvider().addFeatures([nf])

    out.updateExtents()

    # IMPORTANT FIX: recompute category from actual pieces (guarantees consistency)
    recompute_category_from_pieces(out)

    QgsProject.instance().addMapLayer(out)
    print(f"[{version_name}] selected output -> {out_sel_name} (squares + pieces, category fixed)")

    # OUTPUT 3: only selected squares, no intersections
    out_tileonly = make_tileonly_output_layer(out_sel_tileonly_name, TARGET_CRS.authid())

    for f in tiles_sel.getFeatures():
        nf = QgsFeature(out_tileonly.fields())
        nf.setGeometry(f.geometry())
        nf.setAttributes([int(f["tile_id"]), str(f["category"])])
        out_tileonly.dataProvider().addFeatures([nf])

    out_tileonly.updateExtents()
    QgsProject.instance().addMapLayer(out_tileonly)
    apply_category_symbology(out_tileonly)
    print(f"[{version_name}] selected tile-only output -> {out_sel_tileonly_name} (only squares)")


# ============================================================
# PREPARE INPUTS ONCE
# ============================================================
bdr = to_crs(get_layer_by_name(LAYER_NAMES["BDR"]), TARGET_CRS)
bdrexp = to_crs(get_layer_by_name(LAYER_NAMES["BDRexp"]), TARGET_CRS)
icnf = to_crs(get_layer_by_name(LAYER_NAMES["ICNF"]), TARGET_CRS)
nvg = to_crs(get_layer_by_name(LAYER_NAMES["NVG"]), TARGET_CRS)

bdr_2020 = fix_geoms(filter_year(bdr, DATE_FIELD["BDR"], YEAR))
bdrexp_2020 = fix_geoms(filter_year(bdrexp, DATE_FIELD["BDRexp"], YEAR))
icnf_2020 = fix_geoms(filter_year(icnf, DATE_FIELD["ICNF"], YEAR))
nvg_2020 = fix_geoms(filter_year(nvg, DATE_FIELD["NVG"], YEAR))

# extent restricted to NVG 2020, as already requested before
ext = nvg_2020.extent()

nvg_2020_std = ensure_nvg_data0_data1(nvg_2020)

bdrexp_mask = fix_geoms(dissolve_all(bdrexp_2020))
bdr_nonexp = fix_geoms(difference(bdr_2020, bdrexp_mask))

# ============================================================
# RUN BOTH VERSIONS
# ============================================================
run_version(
    version_name="grid_normal",
    grid_builder=make_grid,
    out_cand_name=OUT_CAND_BASE,
    out_sel_name=OUT_SEL_BASE,
    out_sel_tileonly_name=OUT_SEL_TILEONLY_BASE,
    ext=ext,
    bdrexp_mask=bdrexp_mask,
    bdr_nonexp=bdr_nonexp,
    icnf_2020=icnf_2020,
    nvg_2020_std=nvg_2020_std
)

run_version(
    version_name=f"grid_centered_s{CENTER_STRENGTH}",
    grid_builder=make_grid_centered,
    out_cand_name=OUT_CAND_CENTER,
    out_sel_name=OUT_SEL_CENTER,
    out_sel_tileonly_name=OUT_SEL_TILEONLY_CENTER,
    ext=ext,
    bdrexp_mask=bdrexp_mask,
    bdr_nonexp=bdr_nonexp,
    icnf_2020=icnf_2020,
    nvg_2020_std=nvg_2020_std,
    centered_strength=CENTER_STRENGTH
)

print("DONE: 6 outputs created (3 per version). Adjust CENTER_STRENGTH to control centering.")