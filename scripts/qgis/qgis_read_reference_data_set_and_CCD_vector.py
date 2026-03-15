import os
import sys
from AAA_Configs import Input_dir
from PyQt5.QtGui import QColor
from qgis.core import (QgsProject, QgsVectorLayer, QgsCategorizedSymbolRenderer, 
                       QgsRendererCategory, QgsSymbol, QgsPalLayerSettings, 
                       QgsTextFormat, QgsVectorLayerSimpleLabeling, 
                       QgsLabelingEngineSettings, QgsCoordinateReferenceSystem)
from qgis.utils import iface
from qgis import processing

'''
Script to load the ICNF burned area shapefile (or another shapefile reference file for tests) into QGIS, 
apply categorized symbology by month and labeling by day of month, and zoom to the layer extent.

Inputs:
- The path to the shapefile to which the target features belong (configured in AAA_Configs.py as shp_path); 
this is only needed to determine the CRS of the target features for accurate loading and visualization in QGIS; 
the layer name in QGIS will be derived from the shapefile name by default, but can be set to a custom name if needed
Outputs:   
- The specified shapefile layer will be loaded into the current QGIS project with categorized symbology by month and labeling by day of month, 
and the map canvas will zoom to the layer extent for visualization.
'''

CLEAR=False # whether to clear the current project before loading the layer; set to False if you want to load the layer into an existing project with other layers, but make sure to adjust the symbology and labeling settings accordingly to avoid conflicts with existing layers; set to True if you want a clean project with only the loaded layer and its symbology and labeling settings as defined in this script
ADD_CCD_VECTOR_LAYER=True # whether to load the CCD vectorized results for visual comparison with our predictions; this is optional and can be commented out if not needed, but it can be useful to visually compare the CCD vectorized results with our predictions in QGIS to understand the differences in spatial patterns and extents of detected changes; make sure to adjust the symbology settings for the CCD layer as needed to differentiate it from the reference layer

######################################### determine working_dir from script location
import inspect
# 1. Get the path of the current script file
# This is more robust than __file__ inside QGIS Editor
try:
    # Frame 0 is the current execution frame
    script_path = inspect.getfile(inspect.currentframe())
except:
    # Absolute Fallback: Hardcode the path during debugging if all else fails
    # script_path = r"C:\Your\Path\To\Script.py"
    import console
    script_path = iface.mainWindow().findChild(console.console.PythonConsole).findChild(console.console_editor.EditorTabWidget).currentWidget().file_path()

working_dir = os.path.dirname(os.path.abspath(script_path))
print(f"Working directory identified as: {working_dir}")

if working_dir not in sys.path:
    sys.path.append(working_dir)

print(working_dir)

######################################## read variable names from AAA_Configs
# read variable names from AAA_Configs
shp_path=None # reference data set; to be updated by AAA_Configs.py
DATA0=None
CCD_raster_results_path=None # CCD raw output; to be updated by AAA_Configs.py
CCD_vector_results_path=None # CCD vectorized output; to be updated by AAA_Configs.py
with open(os.path.join(working_dir, 'AAA_Configs.py')) as f:
    exec(f.read()) # 

# Resolve shp_path relative to script if it starts with '.'
def resolve_path(base_dir, relative_path):
    """Joins and normalizes a path if it starts with a dot."""
    if relative_path.startswith('.'):
        return os.path.normpath(os.path.join(base_dir, relative_path))
    return relative_path

if CCD_raster_results_path: CCD_raster_results_path = resolve_path(working_dir, CCD_raster_results_path)
if CCD_vector_results_path: CCD_vector_results_path = resolve_path(working_dir, CCD_vector_results_path)
if shp_path: shp_path = resolve_path(working_dir, shp_path)
layer_name=os.path.splitext(os.path.basename(shp_path))[0]
######################################

# --- 1) Clear Current Project ---
if CLEAR: 
    QgsProject.instance().clear()

def main():
    # --- 2) Load the Layer shp_path ---
    #load areproject shp_path file to OUTPUT_CRS
    if shp_path is not None and os.path.exists(shp_path):
        layer = QgsVectorLayer(shp_path, layer_name, "ogr")
    
    if not layer.isValid():
        print(f"Error: Layer at {shp_path} failed to load!")
    else:
        # Set Project CRS to Layer CRS immediately for consistent zoom
        QgsProject.instance().setCrs(layer.crs())
        # Add layer to project
        QgsProject.instance().addMapLayer(layer)
        apply_categorized_symbology_by_month(layer, expression_month=f'month("{DATA0}")', expression_day=f'day("{DATA0}")', stroke_width=1.0)
        
        if CLEAR:
            # --- 6) UI Refreshes & Zoom ---
            layer.triggerRepaint()
            iface.layerTreeView().refreshLayerSymbology(layer.id())
            # Zoom to extent
            iface.mapCanvas().setExtent(layer.extent())
            iface.mapCanvas().refresh()

        # CCD vector layer loading for visual comparison with our predictions; this is optional and can be commented out if not needed, but it can be useful to visually compare the CCD vectorized results with our predictions in QGIS to understand the differences in spatial patterns and extents of detected changes; make sure to adjust the symbology settings for the CCD layer as needed to differentiate it from the reference layer
        if CCD_vector_results_path is not None:
            if os.path.exists(CCD_vector_results_path) and ADD_CCD_VECTOR_LAYER:
                ccd_vector_layer = read_and_merge_CCD_gpkg_vector_results(CCD_vector_results_path)
                if ccd_vector_layer.isValid():
                    QgsProject.instance().addMapLayer(ccd_vector_layer)
                    apply_categorized_symbology_by_month(ccd_vector_layer, expression_month='to_int(substr(  "date_value" ,5,2))', expression_day='to_int(substr(  "date_value" ,7,2))', stroke_width=0.2)
                else:
                    print(f"Warning: CCD vector layer at {CCD_vector_results_path} failed to load!")

            print("Success: Layer loaded, Symbology and Labels applied.")


def read_and_merge_CCD_gpkg_vector_results(CCD_vector_results_path):
    """Reads and merges CCD vector results from a directory containing multiple GPKG files, returning a single merged QgsVectorLayer."""
    gpkg_files = [
    os.path.join(CCD_vector_results_path, f) 
    for f in os.listdir(CCD_vector_results_path) 
    if f.endswith('.gpkg')]
    # 3. Check if we actually found files
    if not gpkg_files:
        print("No GeoPackage files found in the specified directory.")
    else:
        print(f"Merging {len(gpkg_files)} layers...")

        # 4. Run the Merge algorithm
        # 'layers' takes a list of file paths or layer objects
        parameters = {
            'LAYERS': gpkg_files,
            'CRS': gpkg_files[0], # Uses the CRS of the first file
            'OUTPUT': 'memory:Merged_CCD_Results' # Creates a temporary layer
        }
        result = processing.run("native:mergevectorlayers", parameters)
        merged_layer = result['OUTPUT']
    return merged_layer

def apply_categorized_symbology_by_month(layer, expression_month, expression_day, stroke_width):
    # --- 3) Categorized Symbology by Month ---
    expression = expression_month
    months = [
        (1, 'January', '#313695'), (2, 'February', '#4575b4'),
        (3, 'March', '#74add1'),   (4, 'April', '#abd9e9'),
        (5, 'May', '#e0f3f8'),     (6, 'June', '#fee090'),
        (7, 'July', '#fdae61'),    (8, 'August', '#f46d43'),
        (9, 'September', '#d73027'), (10, 'October', '#a50026'),
        (11, 'November', '#660000'), (12, 'December', '#000000')
    ]
    
    categories = []
    for val, label, color_hex in months:
        symbol = QgsSymbol.defaultSymbol(layer.geometryType())
        symbol.setColor(QColor(color_hex))
        symbol.setOpacity(0.7)
        
        # --- NEW: Set Stroke Width ---
        # Polygons use QgsSimpleFillSymbolLayer for their outlines
        for i in range(symbol.symbolLayerCount()):
            symbol_layer = symbol.symbolLayer(i)
            if hasattr(symbol_layer, 'setStrokeWidth'):
                symbol_layer.setStrokeWidth(stroke_width)
        
        category = QgsRendererCategory(val, symbol, label)
        categories.append(category)
    
    renderer = QgsCategorizedSymbolRenderer(expression, categories)
    layer.setRenderer(renderer)
    
    # --- 4) Labeling by Day of Month ---
    label_settings = QgsPalLayerSettings()
    label_settings.isExpression = True
    label_settings.fieldName = expression_day    
    
    # Text Format
    text_format = QgsTextFormat()
    text_format.setSize(9)
    text_format.setColor(QColor("black"))
    buffer = text_format.buffer()
    buffer.setEnabled(True)
    buffer.setSize(0.8)
    text_format.setBuffer(buffer)
    label_settings.setFormat(text_format)

    # Correct Placement for Polygons
    label_settings.placement = QgsPalLayerSettings.Horizontal
    
    # Scale Visibility (Visible closer than 1:100,000)
    label_settings.scaleVisibility = True
    label_settings.minimumScale = 100000 

    # Apply to Layer
    layer.setLabeling(QgsVectorLayerSimpleLabeling(label_settings))
    layer.setLabelsEnabled(True)

    # --- 5) Fix Global Placement Engine ---
    engine_settings = QgsLabelingEngineSettings()
    engine_settings.setFlag(QgsLabelingEngineSettings.UsePartialCandidates, True)
    QgsProject.instance().setLabelingEngineSettings(engine_settings)

main()