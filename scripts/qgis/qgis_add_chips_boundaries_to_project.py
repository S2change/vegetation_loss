import os
import sys
from qgis.core import (
    QgsProject, QgsVectorLayer, QgsFeature, 
    QgsGeometry, QgsFields, QgsField, 
    QgsCoordinateTransform, QgsCoordinateReferenceSystem
)
from PyQt5.QtCore import QVariant

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

###########################################################################################  read variable names from AAA_Configs
Input_dir=None # where all available chips are; configured in  by AAA_Configs.py
with open(os.path.join(working_dir, 'AAA_Configs.py')) as f:    
    exec(f.read()) # 

# Resolve shp_path relative to script if it starts with '.'
def resolve_path(base_dir, relative_path):
    """Joins and normalizes a path if it starts with a dot."""
    if relative_path.startswith('.'):
        return os.path.normpath(os.path.join(base_dir, relative_path))
    return relative_path

chip_source_folder = resolve_path(working_dir, Input_dir)

def add_raster_extents_to_project(folder_path):
    # 1. Setup the Memory Layer
    project_crs = QgsProject.instance().crs().authid()
    uri = f"Polygon?crs={project_crs}"
    extent_layer = QgsVectorLayer(uri, "Chip boundaries", "memory")
    
    # Define attributes to store the filename
    provider = extent_layer.dataProvider()
    provider.addAttributes([QgsField("filename", QVariant.String)])
    extent_layer.updateFields()
    
    features = []
    
    # 2. Iterate through files
    for file in os.listdir(folder_path):
        if file.lower().endswith(('.tif', '.tiff')):
            file_path = os.path.join(folder_path, file)
            
            # Load raster temporarily to get info
            temp_raster = QgsRasterLayer(file_path, file)
            
            if not temp_raster.isValid():
                print(f"Skipping invalid raster: {file}")
                continue
            
            # 3. Handle CRS Transformation
            raster_extent = temp_raster.extent()
            raster_crs = temp_raster.crs()
            dest_crs = QgsProject.instance().crs()
            
            # Create geometry from extent
            geom = QgsGeometry.fromRect(raster_extent)
            
            # If CRS is different, transform the geometry
            if raster_crs != dest_crs:
                transform = QgsCoordinateTransform(raster_crs, dest_crs, QgsProject.instance())
                geom.transform(transform)
            
            # 4. Create Feature
            feat = QgsFeature(extent_layer.fields())
            feat.setGeometry(geom)
            feat.setAttribute("filename", file)
            features.append(feat)

    # 5. Add features and load to map
    provider.addFeatures(features)
    extent_layer.updateExtents()
    QgsProject.instance().addMapLayer(extent_layer)
    
    print(f"Added {len(features)} extents to the map.")

    # 6. set symbology to transparent fill and red border
    symbol = QgsSymbol.defaultSymbol(extent_layer.geometryType())   
    symbol.setColor(QColor(255, 0, 0, 20))  # Semi-transparent red fill (optional)   
    extent_layer.renderer().setSymbol(symbol)

# Usage:
add_raster_extents_to_project(chip_source_folder)