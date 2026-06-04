import os
import re
import math
import tempfile
import shutil

import numpy as np 
from osgeo import gdal, ogr, osr

from qgis.core import QgsProject, QgsRasterLayer, QgsVectorLayer

# ============================================================
# CONFIG
# ============================================================

OUTPUT_GPKG = r"C:\Users\jesus\OneDrive\Ejercicio 7\Documents\Work\PT\temp_qgis_raster_footprints\visible_rasters_union.gpkg"
OUTPUT_LAYER_NAME = "visible_rasters_union"

# Si True, añade también las huellas individuales al proyecto
ADD_INDIVIDUAL_LAYERS = False


# ============================================================
# HELPERS
# ============================================================

def safe_name(text):
    text = re.sub(r"[^A-Za-z0-9_]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text or "layer"

def get_visible_raster_layers():
    project = QgsProject.instance()
    root = project.layerTreeRoot()
    out = []

    for layer in project.mapLayers().values():
        if not isinstance(layer, QgsRasterLayer):
            continue
        if not layer.isValid():
            continue

        node = root.findLayer(layer.id())
        if node is None:
            continue

        if node.isVisible():
            out.append(layer)

    return out

def delete_gpkg_family(path):
    base, ext = os.path.splitext(path)
    for p in [path, base + ".gpkg-wal", base + ".gpkg-shm"]:
        if os.path.exists(p):
            try:
                os.remove(p)
            except Exception:
                pass

def qgis_layer_from_gpkg(gpkg_path, layer_name):
    uri = f"{gpkg_path}|layername={layer_name}"
    return QgsVectorLayer(uri, layer_name, "ogr")

def create_mask_from_raster(src_path, mask_tif_path):
    ds = gdal.Open(src_path, gdal.GA_ReadOnly)
    if ds is None:
        raise RuntimeError(f"No se pudo abrir el raster: {src_path}")

    band = ds.GetRasterBand(1)
    arr = band.ReadAsArray()
    if arr is None:
        raise RuntimeError(f"No se pudo leer la banda 1: {src_path}")

    nodata = band.GetNoDataValue()

    if np.issubdtype(arr.dtype, np.floating):
        valid = np.isfinite(arr)
        if nodata is not None and not (isinstance(nodata, float) and math.isnan(nodata)):
            valid &= arr != nodata
    else:
        valid = np.ones(arr.shape, dtype=bool)
        if nodata is not None:
            valid &= arr != nodata

    mask = np.where(valid, 1, 0).astype(np.uint8)

    driver = gdal.GetDriverByName("GTiff")
    out_ds = driver.Create(
        mask_tif_path,
        ds.RasterXSize,
        ds.RasterYSize,
        1,
        gdal.GDT_Byte,
        options=["COMPRESS=LZW"]
    )
    if out_ds is None:
        raise RuntimeError(f"No se pudo crear el raster máscara: {mask_tif_path}")

    out_ds.SetGeoTransform(ds.GetGeoTransform())
    out_ds.SetProjection(ds.GetProjection())

    out_band = out_ds.GetRasterBand(1)
    out_band.WriteArray(mask)
    out_band.SetNoDataValue(0)
    out_band.FlushCache()

    out_band = None
    out_ds = None
    band = None
    ds = None

def polygonize_mask(mask_tif_path, out_gpkg, out_layer_name):
    delete_gpkg_family(out_gpkg)

    src_ds = gdal.Open(mask_tif_path, gdal.GA_ReadOnly)
    if src_ds is None:
        raise RuntimeError(f"No se pudo abrir la máscara: {mask_tif_path}")

    src_band = src_ds.GetRasterBand(1)

    drv = ogr.GetDriverByName("GPKG")
    dst_ds = drv.CreateDataSource(out_gpkg)
    if dst_ds is None:
        raise RuntimeError(f"No se pudo crear GPKG: {out_gpkg}")

    srs = None
    proj_wkt = src_ds.GetProjection()
    if proj_wkt:
        srs = osr.SpatialReference()
        srs.ImportFromWkt(proj_wkt)

    dst_layer = dst_ds.CreateLayer(out_layer_name, srs=srs, geom_type=ogr.wkbPolygon)
    dst_layer.CreateField(ogr.FieldDefn("value", ogr.OFTInteger))
    field_index = dst_layer.GetLayerDefn().GetFieldIndex("value")

    err = gdal.Polygonize(src_band, None, dst_layer, field_index, [], callback=None)
    if err != 0:
        raise RuntimeError(f"GDAL Polygonize devolvió error: {err}")

    dst_layer = None
    dst_ds = None
    src_band = None
    src_ds = None

def get_union_geom_value1(in_gpkg, in_layer_name):
    in_ds = ogr.Open(in_gpkg, 0)
    if in_ds is None:
        raise RuntimeError(f"No se pudo abrir: {in_gpkg}")

    in_lyr = in_ds.GetLayerByName(in_layer_name)
    if in_lyr is None:
        raise RuntimeError(f"No se encontró la capa: {in_layer_name}")

    union_geom = None
    srs = in_lyr.GetSpatialRef()

    in_lyr.ResetReading()
    for feat in in_lyr:
        val = feat.GetField("value")
        if val != 1:
            continue

        geom = feat.GetGeometryRef()
        if geom is None:
            continue

        geom = geom.Clone()
        if not geom.IsValid():
            geom = geom.MakeValid()

        if union_geom is None:
            union_geom = geom
        else:
            union_geom = union_geom.Union(geom)

    in_lyr = None
    in_ds = None

    if union_geom is None or union_geom.IsEmpty():
        return None, srs

    if not union_geom.IsValid():
        union_geom = union_geom.MakeValid()

    return union_geom, srs

def save_single_union_geom(out_gpkg, out_layer_name, geom, srs):
    delete_gpkg_family(out_gpkg)

    drv = ogr.GetDriverByName("GPKG")
    out_ds = drv.CreateDataSource(out_gpkg)
    if out_ds is None:
        raise RuntimeError(f"No se pudo crear: {out_gpkg}")

    out_lyr = out_ds.CreateLayer(out_layer_name, srs=srs, geom_type=ogr.wkbMultiPolygon)

    feat_defn = out_lyr.GetLayerDefn()
    feat = ogr.Feature(feat_defn)
    feat.SetGeometry(geom)
    out_lyr.CreateFeature(feat)

    feat = None
    out_lyr = None
    out_ds = None


# ============================================================
# MAIN
# ============================================================

os.makedirs(os.path.dirname(OUTPUT_GPKG), exist_ok=True)
delete_gpkg_family(OUTPUT_GPKG)

visible_rasters = get_visible_raster_layers()

if not visible_rasters:
    raise Exception("No hay rasters visibles cargados en QGIS.")

print(f"Raster visibles encontrados: {len(visible_rasters)}")

tmp_dir = tempfile.mkdtemp(prefix="qgis_visible_rasters_union_")

global_union = None
global_srs = None

try:
    for i, rlayer in enumerate(visible_rasters, start=1):
        raster_name = rlayer.name()
        raster_source = rlayer.source()
        raster_safe = safe_name(raster_name)

        print(f"\n[{i}/{len(visible_rasters)}] Procesando: {raster_name}")
        print(f"Fuente: {raster_source}")

        try:
            mask_tif = os.path.join(tmp_dir, f"{raster_safe}_mask.tif")
            poly_gpkg = os.path.join(tmp_dir, f"{raster_safe}_poly.gpkg")
            poly_layer = f"{raster_safe}_poly"

            create_mask_from_raster(raster_source, mask_tif)
            polygonize_mask(mask_tif, poly_gpkg, poly_layer)

            geom, srs = get_union_geom_value1(poly_gpkg, poly_layer)
            if geom is None or geom.IsEmpty():
                print("  -> Sin píxeles válidos")
                continue

            if global_union is None:
                global_union = geom
                global_srs = srs
            else:
                global_union = global_union.Union(geom)

            if ADD_INDIVIDUAL_LAYERS:
                indiv_gpkg = os.path.join(tmp_dir, f"{raster_safe}_union.gpkg")
                indiv_layer = f"{raster_safe}_union"
                save_single_union_geom(indiv_gpkg, indiv_layer, geom, srs)
                lyr = qgis_layer_from_gpkg(indiv_gpkg, indiv_layer)
                if lyr.isValid():
                    QgsProject.instance().addMapLayer(lyr)

            print("  -> OK")

        except Exception as e:
            print(f"  -> ERROR en {raster_name}: {e}")

    if global_union is None or global_union.IsEmpty():
        raise Exception("No se pudo generar la unión final.")

    if not global_union.IsValid():
        global_union = global_union.MakeValid()

    save_single_union_geom(OUTPUT_GPKG, OUTPUT_LAYER_NAME, global_union, global_srs)

    final_layer = qgis_layer_from_gpkg(OUTPUT_GPKG, OUTPUT_LAYER_NAME)
    if not final_layer.isValid():
        raise Exception("Se creó el GPKG pero no se pudo cargar la capa final.")

    QgsProject.instance().addMapLayer(final_layer)

    print("\nProceso terminado correctamente.")
    print(f"Salida final: {OUTPUT_GPKG}")
    print(f"Capa final: {OUTPUT_LAYER_NAME}")

finally:
    try:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    except Exception:
        pass