import os
import re
import shutil

from osgeo import gdal, ogr, osr

from qgis.PyQt.QtGui import QColor
from qgis.core import (
    QgsProject,
    QgsRasterLayer,
    QgsVectorLayer,
    QgsGeometry,
    QgsCoordinateReferenceSystem,
    QgsCoordinateTransform,
    QgsPalettedRasterRenderer,
)
  
# ============================================================
# CONFIG
# ============================================================

MASK_VECTOR_LAYER_NAME = "Edicion"
CHG_TYPE_FIELD = "Chg_type"

OUTPUT_DIR = r"C:\Users\jesus\OneDrive\Ejercicio 7\Documents\Work\PT\Harmonizacion_datos\chips_data\mask_outputs"
OUT_POLY_DIR = os.path.join(OUTPUT_DIR, "mask_polygons")
OUT_MASK_DIR = os.path.join(OUTPUT_DIR, "mask_rasters")
OUT_SOURCE_DIR = os.path.join(OUTPUT_DIR, "source_rasters")

ADD_OUTPUTS_TO_QGIS = True

EXPORT_SOURCE_STYLE_QML = True
EXPORT_SOURCE_STYLE_SLD = True
EXPORT_MASK_STYLE_QML = True

MASK_NODATA_VALUE = 255

CHG_TYPE_TO_VALUE = {
    "nao_alteracao": 0,
    "corte": 1,
    "outro": 2,
    "agricultura": 3,
    "fogo": 4,
}

SKIP_UNKNOWN_CHG_TYPE = True

MASK_CLASS_STYLES = {
    0: {"label": "nao_alteracao", "color": "#e41a1c"},
    1: {"label": "corte", "color": "#ff7f00"},
    2: {"label": "outro", "color": "#984ea3"},
    3: {"label": "agricultura", "color": "#4daf4a"},
    4: {"label": "fogo", "color": "#377eb8"},
    255: {"label": "nodata", "color": "#000000"},
}


# ============================================================
# HELPERS
# ============================================================

def safe_name(text):
    text = re.sub(r"[^A-Za-z0-9_]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "layer"


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def delete_gpkg_family(path):
    base, _ = os.path.splitext(path)
    for p in [path, base + ".gpkg-wal", base + ".gpkg-shm"]:
        if os.path.exists(p):
            try:
                os.remove(p)
            except Exception:
                pass


def get_vector_layer_by_name(name):
    layers = QgsProject.instance().mapLayersByName(name)
    if not layers:
        raise Exception(f"No se encontró la capa vectorial: {name}")
    lyr = layers[0]
    if not isinstance(lyr, QgsVectorLayer) or not lyr.isValid():
        raise Exception(f"La capa no es una vectorial válida: {name}")
    return lyr


def get_visible_rasters():
    project = QgsProject.instance()
    root = project.layerTreeRoot()
    out = []

    for lyr in project.mapLayers().values():
        if not isinstance(lyr, QgsRasterLayer):
            continue
        if not lyr.isValid():
            continue

        node = root.findLayer(lyr.id())
        if node is None:
            continue

        if node.isVisible():
            out.append(lyr)

    return out


def parse_phase_and_date_from_raster_name(name):
    """
    Expected examples:
        BDRexp_v1_832_05_05_30_before_20200522
        BDRexp_v1_832_05_05_30_after_20200522
    """
    m = re.search(r"_(before|after)_(\d{8})$", name, flags=re.IGNORECASE)
    if not m:
        raise Exception(f"No se pudo extraer before/after y fecha desde: {name}")
    phase = m.group(1).lower()
    date_str = m.group(2)
    return phase, date_str


def chip_key_from_raster_name(name):
    """
    Groups before/after rasters that belong to the same chip.
    Keeps the date in the key so chips from different dates do not mix.
    """
    return re.sub(r"_(before|after)_(\d{8})$", r"_\2", name, flags=re.IGNORECASE)


def group_rasters_by_chip(raster_layers):
    groups = {}
    for lyr in raster_layers:
        key = chip_key_from_raster_name(lyr.name())
        groups.setdefault(key, []).append(lyr)

    grouped_items = []
    for key, layers in groups.items():
        after_layers = [l for l in layers if re.search(r"_after_\d{8}$", l.name(), flags=re.IGNORECASE)]
        before_layers = [l for l in layers if re.search(r"_before_\d{8}$", l.name(), flags=re.IGNORECASE)]

        if after_layers:
            rep = after_layers[0]
        elif before_layers:
            rep = before_layers[0]
        else:
            rep = layers[0]

        grouped_items.append({
            "chip_key": key,
            "representative": rep,
            "all_layers": layers,
        })

    return grouped_items


def build_extent_geom_from_raster_ds(ds):
    gt = ds.GetGeoTransform()
    xsize = ds.RasterXSize
    ysize = ds.RasterYSize

    xmin = gt[0]
    ymax = gt[3]
    xmax = xmin + xsize * gt[1]
    ymin = ymax + ysize * gt[5]

    wkt = (
        f"POLYGON (("
        f"{xmin} {ymin}, "
        f"{xmin} {ymax}, "
        f"{xmax} {ymax}, "
        f"{xmax} {ymin}, "
        f"{xmin} {ymin}"
        f"))"
    )
    return QgsGeometry.fromWkt(wkt)


def get_raster_centroid_xy(ds):
    gt = ds.GetGeoTransform()
    xsize = ds.RasterXSize
    ysize = ds.RasterYSize

    xmin = gt[0]
    ymax = gt[3]
    xmax = xmin + xsize * gt[1]
    ymin = ymax + ysize * gt[5]

    cx = (xmin + xmax) / 2.0
    cy = (ymin + ymax) / 2.0

    return int(round(cx)), int(round(cy))


def build_output_base_name_from_raster_ds_and_name(ds, raster_name):
    phase, date_str = parse_phase_and_date_from_raster_name(raster_name)
    cx, cy = get_raster_centroid_xy(ds)
    base_name = f"vchip_{cx}_{cy}_{date_str}"
    return base_name, phase


def normalize_chg_type(value):
    if value is None:
        return None
    txt = str(value).strip().lower()
    if txt == "" or txt == "null":
        return None
    return txt


def collect_mask_features_from_qgis_layer(mask_layer, target_crs, chg_type_field):
    source_crs = mask_layer.crs()
    need_transform = source_crs != target_crs

    transformer = None
    if need_transform:
        transformer = QgsCoordinateTransform(
            source_crs,
            target_crs,
            QgsProject.instance()
        )

    field_names = [f.name() for f in mask_layer.fields()]
    if chg_type_field not in field_names:
        raise Exception(f"No existe el campo '{chg_type_field}' en la capa máscara.")

    out = []

    for feat in mask_layer.getFeatures():
        geom = feat.geometry()
        if geom is None or geom.isEmpty():
            continue

        chg_raw = feat[chg_type_field]
        chg_norm = normalize_chg_type(chg_raw)

        if chg_norm not in CHG_TYPE_TO_VALUE:
            if SKIP_UNKNOWN_CHG_TYPE:
                print(f"    - Se omite feature fid={feat.id()} con {chg_type_field}={chg_raw}")
                continue
            raise Exception(f"Valor no reconocido en {chg_type_field}: {chg_raw}")

        g = QgsGeometry(geom)

        if need_transform:
            result = g.transform(transformer)
            if result != 0:
                raise Exception(f"Error transformando la feature fid={feat.id()}.")

        if not g.isGeosValid():
            g = g.makeValid()

        if g.isEmpty():
            continue

        out.append({
            "fid": int(feat.id()),
            "chg_type": chg_norm,
            "mask_val": int(CHG_TYPE_TO_VALUE[chg_norm]),
            "geom": g,
        })

    return out


def qgs_geom_to_ogr_geom(qgs_geom):
    ogr_geom = ogr.CreateGeometryFromWkb(bytes(qgs_geom.asWkb()))
    if ogr_geom is None:
        raise Exception("No se pudo convertir la geometría de QGIS a OGR.")
    if not ogr_geom.IsValid():
        ogr_geom = ogr_geom.MakeValid()
    return ogr_geom


def ogr_force_multipolygon(ogr_geom):
    if ogr_geom is None or ogr_geom.IsEmpty():
        return ogr_geom

    gtype = ogr_geom.GetGeometryType()
    if gtype in (ogr.wkbPolygon, ogr.wkbPolygon25D):
        multi = ogr.Geometry(ogr.wkbMultiPolygon)
        multi.AddGeometry(ogr_geom.Clone())
        return multi

    return ogr_geom


def qgs_crs_to_osr_srs(qgs_crs):
    srs = osr.SpatialReference()
    wkt = qgs_crs.toWkt()
    if not wkt:
        return None
    srs.ImportFromWkt(wkt)
    return srs


def save_features_to_gpkg_ogr(feature_items, crs, out_gpkg, out_layer_name):
    delete_gpkg_family(out_gpkg)

    drv = ogr.GetDriverByName("GPKG")
    if drv is None:
        raise Exception("No se encontró el driver OGR para GPKG.")

    ds = drv.CreateDataSource(out_gpkg)
    if ds is None:
        raise Exception(f"No se pudo crear el GPKG: {out_gpkg}")

    srs = qgs_crs_to_osr_srs(crs)
    lyr = ds.CreateLayer(out_layer_name, srs=srs, geom_type=ogr.wkbMultiPolygon)
    if lyr is None:
        ds = None
        raise Exception(f"No se pudo crear la capa {out_layer_name} en {out_gpkg}")

    lyr.CreateField(ogr.FieldDefn("src_fid", ogr.OFTInteger))
    lyr.CreateField(ogr.FieldDefn("chg_type", ogr.OFTString))
    lyr.CreateField(ogr.FieldDefn("mask_val", ogr.OFTInteger))

    for item in feature_items:
        feat_defn = lyr.GetLayerDefn()
        feat = ogr.Feature(feat_defn)

        feat.SetField("src_fid", int(item["fid"]))
        feat.SetField("chg_type", str(item["chg_type"]))
        feat.SetField("mask_val", int(item["mask_val"]))

        ogr_geom = qgs_geom_to_ogr_geom(item["geom"])
        ogr_geom = ogr_force_multipolygon(ogr_geom)
        feat.SetGeometry(ogr_geom)

        err = lyr.CreateFeature(feat)
        if err != 0:
            feat = None
            lyr = None
            ds = None
            raise Exception(f"No se pudo escribir una feature en {out_gpkg}")

        feat = None

    lyr = None
    ds = None


def rasterize_vector_layer_to_match_raster(
    vector_gpkg,
    vector_layer_name,
    raster_ds,
    out_tif,
    attr_field="mask_val",
    nodata=255
):
    if os.path.exists(out_tif):
        try:
            os.remove(out_tif)
        except Exception:
            pass

    xsize = raster_ds.RasterXSize
    ysize = raster_ds.RasterYSize
    gt = raster_ds.GetGeoTransform()
    proj = raster_ds.GetProjection()

    drv = gdal.GetDriverByName("GTiff")
    out_ds = drv.Create(
        out_tif,
        xsize,
        ysize,
        1,
        gdal.GDT_Byte,
        options=["COMPRESS=LZW"]
    )
    if out_ds is None:
        raise Exception(f"No se pudo crear raster de salida: {out_tif}")

    out_ds.SetGeoTransform(gt)
    out_ds.SetProjection(proj)

    band = out_ds.GetRasterBand(1)
    band.Fill(nodata)
    band.SetNoDataValue(nodata)

    src_ds = ogr.Open(vector_gpkg, 0)
    if src_ds is None:
        raise Exception(f"No se pudo abrir el vector para rasterizar: {vector_gpkg}")

    src_lyr = src_ds.GetLayerByName(vector_layer_name)
    if src_lyr is None:
        src_ds = None
        raise Exception(f"No se encontró la layer {vector_layer_name} en {vector_gpkg}")

    err = gdal.RasterizeLayer(
        out_ds,
        [1],
        src_lyr,
        options=[f"ATTRIBUTE={attr_field}"]
    )
    if err != 0:
        src_lyr = None
        src_ds = None
        band = None
        out_ds = None
        raise Exception(f"Error rasterizando la máscara hacia {out_tif}")

    band.FlushCache()

    src_lyr = None
    src_ds = None
    band = None
    out_ds = None


def copy_source_raster(src_path, dst_path):
    if not os.path.exists(src_path):
        raise Exception(f"No existe el raster fuente: {src_path}")

    if os.path.exists(dst_path):
        try:
            os.remove(dst_path)
        except Exception:
            pass

    shutil.copy2(src_path, dst_path)

    aux_xml = src_path + ".aux.xml"
    if os.path.exists(aux_xml):
        dst_aux = dst_path + ".aux.xml"
        try:
            if os.path.exists(dst_aux):
                os.remove(dst_aux)
        except Exception:
            pass
        try:
            shutil.copy2(aux_xml, dst_aux)
        except Exception:
            pass


def export_qgis_style_for_raster(qgs_raster_layer, exported_raster_path, export_qml=True, export_sld=True):
    base_path, _ = os.path.splitext(exported_raster_path)

    if export_qml:
        qml_path = base_path + ".qml"
        if os.path.exists(qml_path):
            try:
                os.remove(qml_path)
            except Exception:
                pass

        ok_qml, msg_qml = qgs_raster_layer.saveNamedStyle(qml_path)
        if not ok_qml:
            raise Exception(f"No se pudo guardar QML: {msg_qml}")

    if export_sld:
        sld_path = base_path + ".sld"
        if os.path.exists(sld_path):
            try:
                os.remove(sld_path)
            except Exception:
                pass

        ok_sld, msg_sld = qgs_raster_layer.saveSldStyle(sld_path)
        if not ok_sld:
            raise Exception(f"No se pudo guardar SLD: {msg_sld}")


def apply_mask_raster_style(mask_raster_layer):
    provider = mask_raster_layer.dataProvider()

    classes = []
    for value in sorted(MASK_CLASS_STYLES.keys()):
        item = MASK_CLASS_STYLES[value]
        classes.append(
            QgsPalettedRasterRenderer.Class(
                int(value),
                QColor(item["color"]),
                item["label"]
            )
        )

    renderer = QgsPalettedRasterRenderer(provider, 1, classes)
    mask_raster_layer.setRenderer(renderer)
    mask_raster_layer.triggerRepaint()


def export_mask_raster_style(mask_raster_path):
    tmp_layer_name = os.path.basename(mask_raster_path)
    lyr = QgsRasterLayer(mask_raster_path, tmp_layer_name)
    if not lyr.isValid():
        raise Exception(f"No se pudo abrir el mask raster para estilizar: {mask_raster_path}")

    apply_mask_raster_style(lyr)

    qml_path = os.path.splitext(mask_raster_path)[0] + ".qml"
    if os.path.exists(qml_path):
        try:
            os.remove(qml_path)
        except Exception:
            pass

    ok_qml, msg_qml = lyr.saveNamedStyle(qml_path)
    if not ok_qml:
        raise Exception(f"No se pudo guardar el estilo QML del mask raster: {msg_qml}")


def add_vector_if_valid(gpkg_path, layer_name):
    uri = f"{gpkg_path}|layername={layer_name}"
    lyr = QgsVectorLayer(uri, layer_name, "ogr")
    if lyr.isValid():
        QgsProject.instance().addMapLayer(lyr)


def add_mask_raster_if_valid_with_style(raster_path, layer_name):
    lyr = QgsRasterLayer(raster_path, layer_name)
    if not lyr.isValid():
        return

    apply_mask_raster_style(lyr)
    QgsProject.instance().addMapLayer(lyr)


# ============================================================
# MAIN
# ============================================================

ensure_dir(OUTPUT_DIR)
ensure_dir(OUT_POLY_DIR)
ensure_dir(OUT_MASK_DIR)
ensure_dir(OUT_SOURCE_DIR)

mask_layer = get_vector_layer_by_name(MASK_VECTOR_LAYER_NAME)
visible_rasters = get_visible_rasters()

if not visible_rasters:
    raise Exception("No hay raster visibles en QGIS.")

chip_groups = group_rasters_by_chip(visible_rasters)

print(f"Máscara vectorial: {mask_layer.name()}")
print(f"Campo de cambio: {CHG_TYPE_FIELD}")
print(f"Raster visibles: {len(visible_rasters)}")
print(f"Chips únicos: {len(chip_groups)}")

print("Diccionario Chg_type -> mask_val:")
for k, v in CHG_TYPE_TO_VALUE.items():
    print(f"  {k} -> {v}")

# 1) Copiar todos los raster fuente y estilos con nuevo nombre
print("\nCopiando raster fuente y estilos...")
for raster in visible_rasters:
    raster_name = raster.name()
    raster_path = raster.source()

    ds = gdal.Open(raster_path, gdal.GA_ReadOnly)
    if ds is None:
        print(f"  -> ERROR source {raster_name}: no se pudo abrir con GDAL")
        continue

    try:
        base_name, phase = build_output_base_name_from_raster_ds_and_name(ds, raster_name)
        out_source_tif = os.path.join(OUT_SOURCE_DIR, f"{base_name}_{phase}.tif")

        copy_source_raster(raster_path, out_source_tif)
        export_qgis_style_for_raster(
            qgs_raster_layer=raster,
            exported_raster_path=out_source_tif,
            export_qml=EXPORT_SOURCE_STYLE_QML,
            export_sld=EXPORT_SOURCE_STYLE_SLD
        )
        print(f"  -> OK source: {raster_name} -> {os.path.basename(out_source_tif)}")

    except Exception as e:
        print(f"  -> ERROR source {raster_name}: {e}")

    ds = None

# 2) Crear una sola máscara por chip
print("\nCreando mask por chip único...")
for i, group in enumerate(chip_groups, start=1):
    chip_key = group["chip_key"]
    rep_raster = group["representative"]
    raster_path = rep_raster.source()

    print(f"\n[{i}/{len(chip_groups)}] Chip: {chip_key}")
    print(f"Raster de referencia: {rep_raster.name()}")

    ds = gdal.Open(raster_path, gdal.GA_ReadOnly)
    if ds is None:
        print("  -> ERROR: no se pudo abrir el raster de referencia con GDAL")
        continue

    gt = ds.GetGeoTransform()
    proj_wkt = ds.GetProjection()

    if gt is None:
        print("  -> ERROR: raster sin geotransform")
        ds = None
        continue

    raster_crs = QgsCoordinateReferenceSystem()
    if not raster_crs.createFromWkt(proj_wkt):
        print("  -> ERROR: no se pudo construir el CRS del raster")
        ds = None
        continue

    try:
        base_name, _ = build_output_base_name_from_raster_ds_and_name(ds, rep_raster.name())

        source_items = collect_mask_features_from_qgis_layer(
            mask_layer=mask_layer,
            target_crs=raster_crs,
            chg_type_field=CHG_TYPE_FIELD
        )

        if not source_items:
            print("  -> ERROR: no quedaron features válidas en la máscara")
            ds = None
            continue

        raster_extent_geom = build_extent_geom_from_raster_ds(ds)
        output_items = []

        for item in source_items:
            geom = QgsGeometry(item["geom"])
            clipped = geom.intersection(raster_extent_geom)

            if clipped is None or clipped.isEmpty():
                continue

            if not clipped.isGeosValid():
                clipped = clipped.makeValid()

            if clipped.isEmpty():
                continue

            output_items.append({
                "fid": item["fid"],
                "chg_type": item["chg_type"],
                "mask_val": item["mask_val"],
                "geom": clipped,
            })

        if not output_items:
            print("  -> Sin intersección útil con este chip")
            ds = None
            continue

        out_poly_layer = f"{base_name}_mask"
        out_poly = os.path.join(OUT_POLY_DIR, f"{out_poly_layer}.gpkg")
        out_mask_tif = os.path.join(OUT_MASK_DIR, f"{base_name}_mask.tif")

        save_features_to_gpkg_ogr(
            feature_items=output_items,
            crs=raster_crs,
            out_gpkg=out_poly,
            out_layer_name=out_poly_layer
        )

        rasterize_vector_layer_to_match_raster(
            vector_gpkg=out_poly,
            vector_layer_name=out_poly_layer,
            raster_ds=ds,
            out_tif=out_mask_tif,
            attr_field="mask_val",
            nodata=MASK_NODATA_VALUE
        )

        if EXPORT_MASK_STYLE_QML:
            export_mask_raster_style(out_mask_tif)

        print(f"  -> Polígono único: {out_poly}")
        print(f"  -> Mask raster único: {out_mask_tif}")
        print(f"  -> Features escritas: {len(output_items)}")

        if ADD_OUTPUTS_TO_QGIS:
            add_vector_if_valid(out_poly, out_poly_layer)
            add_mask_raster_if_valid_with_style(out_mask_tif, f"{base_name}_mask")

    except Exception as e:
        print(f"  -> ERROR: {e}")

    ds = None

print("\nProceso terminado.")