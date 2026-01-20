### `ccd_to_raster.py`
For each inputted date range, it creates a geotiff with the last break dates in that range for pixels that have been processed through the CCD model in order to visualize results. Also creates a .qml file to color code break dates by year. A vector file can also be inputted to do spatial filtering.

### `ccd_plot_one_point.py`
Reads segments in parquet and creates plot

### `ccd_polygons_to_national_maps.py`
Merges bimonthly CCD vector outputs into national-scale GeoPackages for each date interval, enabling consolidated spatial analysis across all tiles. *Tile overlaps are not handled in this version.*

Inputs:
- BASE_FOLDER containing CCD vector outputs (.gpkg) organized by tile and date interval.
- User-defined START_YEAR and END_YEAR to select target bimonthly intervals.

Outputs:
- One merged GeoPackage (.gpkg) per bimonthly interval, containing all polygons from all tiles for that period, saved in the merged_polygons directory.
