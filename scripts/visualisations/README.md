### `ccd_to_raster.py`
For each inputted date range, it creates a geotiff with the last break dates in that range for pixels that have been processed through the CCD model in order to visualize results. Also creates a .qml file to color code break dates by year. A vector file can also be inputted to do spatial filtering.

### `ccd_plot_one_point.py`
Reads CCD segments from Parquet files and plots NDVI time series for a specified point, marking tStart, tBreak, and tEnd events.

Inputs:
- Coordinates of the point of interest (X, Y).
- NDVI dates as a NumPy array (.npy).
- NDVI values and coordinates stored in an HDF5 file (.h5).
- Parquet files containing CCD segments for the specified point.

Outputs:
- Time series plot of NDVI with vertical lines for tStart, tBreak, and tEnd.
- Shapefile (.shp) containing the point geometry and associated segment dates.

### `ccd_polygons_to_national_maps.py`
Merges bimonthly CCD vector outputs into national-scale GeoPackages for each date interval, enabling consolidated spatial analysis across all tiles. *Tile overlaps are not handled in this version.*

Inputs:
- `BASE_FOLDER` containing CCD vector outputs (.gpkg) organized by tile and date interval.
- User-defined `START_YEAR` and `END_YEAR` to select target bimonthly intervals.

Outputs:
- One merged GeoPackage (.gpkg) per bimonthly interval, containing all polygons from all tiles for that period, saved in the merged_polygons directory.


# Raster to Polygon Conversions
The following 3 scripts are different algorithms for clustering pixels with similar break dates together and then converting them to polygons.

### `flood_raster_to_polygons.py`
Raster to Vector Polygon Converter using Flood Fill Algorithm

Inputs:
- `input_raster` is a raster where one of the bands contains a break date value in YYYYMMDD format

Outputs:
- One GeoPackage (.gpkg) which contains polygons that are areas which had pixels whose break dates were within the date_range_days and the polygon area is greater than min_area_ha value

### `graph_raster_to_polygons.py`
Raster to Vector Polygon Converter using Graph Algorithm

Inputs:
- `input_raster` is a raster where one of the bands contains a break date value in YYYYMMDD format

Outputs:
- One GeoPackage (.gpkg) which contains polygons that are areas which had pixels whose break dates were within the date_range_days and the polygon area is greater than min_area_ha value

### `scipy_label_raster_to_polygons.py`
Raster to Vector Polygon Converter using connected component labeling

Inputs:
- `input_raster` is a raster where one of the bands contains a break date value in YYYYMMDD format

Outputs:
- One GeoPackage (.gpkg) which contains polygons that are areas which had pixels whose break dates were within the date_range_days and the polygon area is greater than min_area_ha value
