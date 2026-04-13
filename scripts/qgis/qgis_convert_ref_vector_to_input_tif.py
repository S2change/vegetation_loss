import os
import sys
from datetime import datetime
from osgeo import gdal

'''
This script converts either:
1) a selected vector feature in QGIS into a georeferenced TIFF file with specific attributes based on the feature's geometry 
and date fields. 
2) or, when run in a standalone environment like VSCode, it processes all features from a specified input vector file (e.g., GeoPackage) 
and generates corresponding TIFF files for each feature.

It can be run both inside the QGIS Python console and as a standalone script in an IDE like VSCode, with automatic detection of 
the environment and appropriate initialization of the QGIS application when needed. 

The output TIFF file is created using GDAL and includes three bands: 
two for the break date (encoded as integers) and one for a binary break/no-break mask. 
The script also handles coordinate transformations to ensure the output raster is in the desired CRS.
'''
# feature ID to process in VSCode (ignored in QGIS, where it processes the selected features)
FID=309

# 1. DETECT ENVIRONMENT
try:
    from qgis.utils import iface
    IN_QGIS = iface is not None
except (ImportError, AttributeError):
    IN_QGIS = False

from qgis.core import (QgsApplication, QgsProject, QgsVectorLayer, 
                       QgsVectorFileWriter, QgsCoordinateTransform, 
                       QgsCoordinateReferenceSystem, QgsFeatureRequest)

# 2. INITIALIZE (Only if in VSCode/Standalone)
if not IN_QGIS:
    # Adjust this path to your specific QGIS installation
    QgsApplication.setPrefixPath(r"C:\Program Files\QGIS 3.44.7\apps\qgis", True)
    qgs = QgsApplication([], False)
    qgs.initQgis()
    print("Running in Standalone Mode (VSCode)")
else:
    print("Running inside QGIS Console")

# --- CONFIGURATION ---
D = 3000 
OUTPUT_DIR = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\harmonized_to_tifs"
TARGET_CRS_ID = "EPSG:32629"  
INPUT_VECTOR_PATH = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\harmonized\BDR_expanded_v0.gpkg" # Only used in VSCode
NODATA_VALUE = 65535

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# 3. SETUP LAYER AND FEATURES
if IN_QGIS:
    layer = iface.activeLayer()
    features = layer.selectedFeatures()
    if not features:
        print("Error: No features selected in QGIS.")
        sys.exit()
else:
    layer = QgsVectorLayer(INPUT_VECTOR_PATH, "layer", "ogr")
    print(layer)
    # In VSCode, we process all features, or you could filter here
    features = list(layer.getFeatures(QgsFeatureRequest().setFilterExpression(f"fid = {FID}")))

target_crs = QgsCoordinateReferenceSystem(TARGET_CRS_ID)

# 4. PROCESSING LOOP
for feat in features:
    # --- Logic starts here (Same for both environments) ---
    
    # Date Calculation
    try:
        d0 = datetime.strptime(str(feat['Data0']), '%Y-%m-%d')
        d1 = datetime.strptime(str(feat['Data1']), '%Y-%m-%d')
        mid_date = d0 + (d1 - d0) / 2
        break_date_int = int(mid_date.strftime('%Y%m%d'))
    except Exception as e:
        print(f"Skipping feature {feat.id()}: Date error {e}")
        continue

    # Determine Extent
    geom = feat.geometry()
    transform = QgsCoordinateTransform(layer.crs(), target_crs, QgsProject.instance())
    geom.transform(transform)
    bbox = geom.boundingBox()
    center = bbox.center()
    
    half_width, half_height = max(bbox.width()/2, D/2), max(bbox.height()/2, D/2)
    left, right = int(center.x() - half_width), int(center.x() + half_width)
    bottom, top = int(center.y() - half_height), int(center.y() + half_height)

    # Output Path
    filename = f"output_{break_date_int}_{left}_{right}_{bottom}_{top}.tif"
    output_path = os.path.join(OUTPUT_DIR, filename)

    # Create Raster via GDAL
    res = 10
    width, height = int((right - left) / res), int((top - bottom) / res)
    driver = gdal.GetDriverByName('GTiff')
    ds = driver.Create(output_path, width, height, 3, gdal.GDT_UInt32)
    ds.SetProjection(target_crs.toWkt())
    ds.SetGeoTransform([left, res, 0, top, 0, -res])

    # Initialize Bands
    for i in [1, 2]:
        b = ds.GetRasterBand(i)
        b.SetNoDataValue(NODATA_VALUE)
        b.Fill(NODATA_VALUE)
    ds.GetRasterBand(3).Fill(0)

    # Export Feature to Temp GeoJSON (Needed for RasterizeLayer)
    temp_geojson = os.path.join(OUTPUT_DIR, f"temp_{feat.id()}.json")
    save_options = QgsVectorFileWriter.SaveVectorOptions()
    save_options.driverName = "GeoJSON"
    save_options.ct = transform
    # Filter for ONLY this current feature in the loop
    save_options.filterExpression = f"fid = {feat.id()}" 
    
    QgsVectorFileWriter.writeAsVectorFormatV3(layer, temp_geojson, QgsProject.instance().transformContext(), save_options)

    # Rasterize
    gdal_feat_ds = gdal.OpenEx(temp_geojson)
    if gdal_feat_ds:
        gdal.RasterizeLayer(ds, [1, 2], gdal_feat_ds.GetLayer(), burn_values=[break_date_int, break_date_int])
        gdal.RasterizeLayer(ds, [3], gdal_feat_ds.GetLayer(), burn_values=[1])
    
    # Cleanup loop
    ds.FlushCache()
    ds = None
    gdal_feat_ds = None
    if os.path.exists(temp_geojson):
        os.remove(temp_geojson)

    # 5. SIDE EFFECT: Only load into QGIS if we are actually in QGIS
    if IN_QGIS:
        iface.addRasterLayer(output_path, filename)
    
    print(f"Processed feature {feat.id()} -> {filename}")

# 6. SHUTDOWN (Only for Standalone)
if not IN_QGIS:
    qgs.exitQgis()