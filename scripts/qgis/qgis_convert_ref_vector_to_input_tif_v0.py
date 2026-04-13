'''
I want a script for QGIS. I have a vector layer with field Data0 and Data1 (in format YYYY-MM-DD) and CRS in meters. I want to manualy select one feature from the layer.  The script should compute the average date between DATA0 and DATA1 in YYYYMMDD format (call this break_date). The script should determine an extend of at least 3000 by 3000 centered at the selected features (left,right,bottom,top). The script should create a raster layer with resolution 10 m, and 3 bands (to be defined next) and same CRS as the vector layer.  The raster layer should be saved with  filename=output_{break_date}_{left}_{right}_bottom}_{top} where left,right,bottom,top are the extent converted to integers. Band 1 and band 2 of the output are the same: pixel values should be break_date at the selected feature and 65535 (NOData) everywhere else. Band 3 should be 1  at the selected feature and 0 everywhere else.
'''
import os
from datetime import datetime
from qgis.core import (QgsProject, QgsVectorLayer, QgsVectorFileWriter, 
                       QgsFeatureRequest, QgsCoordinateTransform, 
                       QgsCoordinateReferenceSystem)
from osgeo import gdal

# --- CONFIGURATION ---
D = 3000 
OUTPUT_DIR = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\harmonized_to_tifs"
TARGET_CRS_ID = "EPSG:32629"  
NODATA_VALUE = 0
IS_BREAK_NODATA_VALUE = 0

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# 1. Setup Layer and Selection
layer = iface.activeLayer()
if not isinstance(layer, QgsVectorLayer):
    raise ValueError("Active layer must be a Vector layer.")

selected_features = layer.selectedFeatures()
if len(selected_features) != 1:
    raise ValueError("Please select exactly one feature.")

feat = selected_features[0]
target_crs = QgsCoordinateReferenceSystem(TARGET_CRS_ID)

# 2. Date Calculation
d0 = datetime.strptime(str(feat['Data0']), '%Y-%m-%d')
d1 = datetime.strptime(str(feat['Data1']), '%Y-%m-%d')
mid_date = d0 + (d1 - d0) / 2
break_date_int = int(mid_date.strftime('%Y%m%d'))

# 3. Determine Extent in Target CRS
geom = feat.geometry()
# Transform geometry to target CRS immediately for accurate bounding box
transform = QgsCoordinateTransform(layer.crs(), target_crs, QgsProject.instance())
geom.transform(transform)

bbox = geom.boundingBox()
center = bbox.center()

half_width = max(bbox.width() / 2, D/2)
half_height = max(bbox.height() / 2, D/2)

left = int(center.x() - half_width)
right = int(center.x() + half_width)
bottom = int(center.y() - half_height)
top = int(center.y() + half_height)

# 4. Prepare Output Path
filename = f"output_{break_date_int}_{left}_{right}_{bottom}_{top}.tif"
output_path = os.path.join(OUTPUT_DIR, filename)

if os.path.exists(output_path):
    try: os.remove(output_path)
    except: pass

# 5. Create the 3-Band Raster
res = 10
width = int((right - left) / res)
height = int((top - bottom) / res)

driver = gdal.GetDriverByName('GTiff')
ds = driver.Create(output_path, width, height, 3, gdal.GDT_UInt32)
# CRITICAL: Use Target CRS WKT
ds.SetProjection(target_crs.toWkt())
ds.SetGeoTransform([left, res, 0, top, 0, -res])

# Initialize Bands
for i in [1, 2]:
    b = ds.GetRasterBand(i)
    b.SetDescription("BreakDate")
    b.SetNoDataValue(NODATA_VALUE)
    b.Fill(NODATA_VALUE)
ds.GetRasterBand(3).SetDescription("IsBreak")
ds.GetRasterBand(3).Fill(IS_BREAK_NODATA_VALUE)

# 6. Export Selected Feature to Temp GeoJSON (Re-projected)
temp_geojson_base = os.path.join(OUTPUT_DIR, "temp_selected_feat.json")
save_options = QgsVectorFileWriter.SaveVectorOptions()
save_options.driverName = "GeoJSON"
save_options.onlySelectedFeatures = True 
# CRITICAL: Export the JSON in the target CRS so it aligns with the raster pixels
save_options.ct = transform 

result = QgsVectorFileWriter.writeAsVectorFormatV3(
    layer, temp_geojson_base, QgsProject.instance().transformContext(), save_options
)

actual_temp_path = result[2] if isinstance(result, tuple) else temp_geojson_base

# 7. Rasterize
gdal_feat_ds = gdal.OpenEx(actual_temp_path)
if gdal_feat_ds:
    # Burn dates into 1 & 2, Burn 1 into 3
    gdal.RasterizeLayer(ds, [1, 2], gdal_feat_ds.GetLayer(), burn_values=[break_date_int, break_date_int])
    gdal.RasterizeLayer(ds, [3], gdal_feat_ds.GetLayer(), burn_values=[1])

# 8. Cleanup
ds.FlushCache()
ds = None
gdal_feat_ds = None
if os.path.exists(actual_temp_path):
    os.remove(actual_temp_path)

iface.addRasterLayer(output_path, filename)
print(f"Success! Created: {filename}")