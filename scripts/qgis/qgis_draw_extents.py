from qgis.core import (QgsProject, QgsVectorLayer, QgsFeature, 
                       QgsGeometry, QgsRectangle, QgsSymbol, 
                       QgsRendererCategory, QgsSingleSymbolRenderer)
from qgis.gui import QgsMapCanvas


'''
Script to create a new memory layer in QGIS with a single polygon feature representing the spatial extent defined by the given coordinates (xmin, xmax, ymin, ymax). The layer is styled with a transparent fill and a colored border, and added to the current QGIS project. This can be used to visualize the spatial bounds of the data or for intersection checks with other layers.
Inputs:
- Coordinates defining the extent: xmin, xmax, ymin, ymax   
- Label for the layer (e.g., "SPATIAL_BOUNDS")
- Color for the border of the polygon (e.g., 'red')
- CRS for the layer (e.g., "EPSG:32629")
Outputs:
- A new memory layer added to the QGIS project with the specified polygon feature and styling
'''

# 1. Define the extent and CRS
xmin, xmax,ymin, ymax , label , col= 521518, 534321, 4427535, 4438495, 'SPATIAL_BOUNDS','red'
#xmin, xmax,ymin, ymax , label ,col= 521520.0, 534320.0, 4427540.0, 4438490.0, 'xs, ys filtered', 'blue'
#xmin, xmax,ymin, ymax , label ,col = 499980.0,  502540.0, 4410400.0, 4412960.0, '1st chip window bounds', 'black'
crs_string = "EPSG:32629"

# 2. Create the memory layer
# Format: "Type?crs=EPSG:xxxx"
layer = QgsVectorLayer(f"Polygon?crs={crs_string}", label, "memory")
provider = layer.dataProvider()

# 3. Create the rectangle geometry
rect = QgsRectangle(xmin, ymin, xmax, ymax)
feat = QgsFeature()
feat.setGeometry(QgsGeometry.fromRect(rect))

# 4. Add the feature to the layer
provider.addFeature(feat)

# 5. Set Sympathy to Transparent
# We create a simple fill symbol with a border but no fill color
symbol = QgsSymbol.defaultSymbol(layer.geometryType())
symbol.setOpacity(0.5) # Optional: adjust overall opacity
# To make it truly "hollow", set the brush style to NoBrush
symbol.symbolLayer(0).setBrushStyle(Qt.NoBrush)
symbol.symbolLayer(0).setStrokeColor(QColor(col)) # Red outline
symbol.symbolLayer(0).setStrokeWidth(0.6)

layer.setRenderer(QgsSingleSymbolRenderer(symbol))

# 6. Add to the project
QgsProject.instance().addMapLayer(layer)

print("Layer created successfully with the specified extent.")