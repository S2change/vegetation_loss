Products:
- MBPV_v1: (*mapa bimestral de perdas de vegetação*). This is an experimental product for 2023-2024 that is solely based on the PyCCD estimated breaks. Pixels with similar dates are clustered and clusters with area smaller than 0.5 ha are discarded. A password is needed to unzip the file `MBPV_v2.zip`.

  The steps to produce this data set are the following:
  1. Process Sentinel-2 time series up to 2024-12-31 with PyCCD.
     - Input. hdf5 file with 4 bands B3, B4, B8, B12 (NA=65355), CRS=32629 + hdf5 files for x,y, DGT vegetation loss mask (see `/data_info/readme.md`) 
     - Ouputs: Parquets PyCCD outputs (see /data_info/readme.md)
  2. Convert PyCCD output to raster files with the most recent detection date (script at `/scripts/visualisations/ccd_to_raster.py`)
     - Input: Parquets PyCCD outputs; date_range, e.g. ['2023-01-01','2023-02-28'], so only breaks within the date range are considered. For this product, `date_range` is bimonthly, and one segment from CCD is always assigned to a single bimonthly period.
     - Output: Multi-band GeoTIFF raster file (.tif), with bands last_tEnd, last_tBreak, is_break, ndvi_last_segment
  3. Convert each bimonthly raster file to vector format with script `/scripts/visualisations/graph_raster_to_polygons.py`:
     - Input: For each period (2 months) and for each tile: 1 multi-band GeoTIFF raster file (.tif); date is mean between last_tEnd, last_tBreak, date_range_days = 10 (number of days to group adjacent pixels within each spatial cluster); min_area_ha = 0.5 (minimum polygon area in hectares).
     - Output: For each period (2 months) and for each tile: 1 polygon geopackage file; attributes: mean, min, max dates for each polygon; polygon area, with polygons at least 0.5 ha.
