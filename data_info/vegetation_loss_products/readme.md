## Data available to download
- DL_MBPV_v3: `DL_MBPV_v3_20230201_20230501_T5_A10.zip`. Fields: "date_iso" in format "yyyy-mm-dd", and "class_id" which takes values 1 ("Cuts") and 2 ("Fires"). This is a Geopackage file with predictions for Portugal for the time period 2023-02-01 to 2023-05-01 (February, March and April 2023). The full map for February 2023 to Jan 2024 (Deliverable E.4.2) will be uploaded here.
- CCD_MBPV_v1: `MBPV_v1.7z` zipped file (open with `7-zip` and password). It includes 12 bimensal maps for 2023-2024 (see below), each one with predicted vegetation loss polygons for the whole Portugal with the following fields: 
fid,
date_value,
area_ha,
min_date,
max_date,
date_diff_days,
date_formatted,
min_date_formatted,
max_date_formatted.

## Data sets description

All products have vector format, unless stated otherwise.

- DL_MBPV_v3:  (*mapa bimestral de perdas de vegetação v3-T5A10*). Product based on deep learning that is created on the HPC cluster using the pipeline described in <https://github.com/S2change/vegetation_loss/tree/main/scripts/cpca070342024_shared/predict_pipeline>. The main parameters used for the product are `MAX_THETA=5` days and `MAX_CLUSTER_AMPLITUDE=10` days which determine the potential change dates for each tile.
- CCD_RPV_v2: Raster product. Similarly to `MBPV_v1`, the Sentinel-2 series is processed up to a recent date (e.g. present), but the vector vegetation loss map is computed for some time interval I. For instance, S2 is processed by PyCCD up to 2025-11-17, but only breaks within ['2023-01-01','2023-02-28'] are extracted to create the vector product. Processing steps are similar to `MBVP_v1` with a few improvements.
- CCD_MIPV_v2: (*mapa de perdas de vegetação v2*). Similarly to `MBPV_v1`, the Sentinel-2 series is processed up to a recent date (e.g. present), but the vector vegetation loss map is computed for some time interval I. For instance, S2 is processed by PyCCD up to 2025-11-17, but only breaks within ['2023-01-01','2023-02-28'] are extracted to create the vector product. Processing steps are similar to `MBVP_v1` with a few improvements.
- CCD_MBPV_v2: (near-real time *mapa bimestral de perdas de vegetação v2*). Instead of being based on a Sentinel-2 time series up to 2024-12-31 (as `MBVP_v1`), this product is a *near-real time* bymonthly product, where for each date range,   e.g. ['2023-01-01','2023-02-28'], the Sentinel-2 time series is only processed up to the last date of the data range. Otherwise, the processing steps are similar to `MBVP_v1` with a few improvements. 
- CCD_MBPV_v1, also called just MBPV_v1: (*mapa bimestral de perdas de vegetação v1*). This is an experimental product for 2023-2024 that is solely based on the PyCCD estimated breaks. Furthermore, the PyCCD processing uses a parameter (minimum number of observations required  to identify break) which is probably too high and makes it harder to determine breaks, in particular at the end of the temporal series.  Neighbor pixels with similar dates (less than 10 days) are clustered and clusters with area smaller than 0.5 ha are discarded.

  The steps to produce `MBPV_v1` are the following:
  1. Process Sentinel-2 time series up to 2024-12-31 with PyCCD.
     - Input. hdf5 file with 4 bands B3, B4, B8, B12 (NA=65355), CRS=32629 + hdf5 files for x,y, DGT vegetation loss mask (see `/data_info/readme.md`) 
     - Ouputs: Parquets PyCCD outputs (see `/data_info/readme.md`)
  2. Convert PyCCD output to raster files with the most recent detection date (script at `/scripts/visualisations/ccd_to_raster.py`)
     - Input: Parquets PyCCD outputs; date_range, e.g. ['2023-01-01','2023-02-28'], so only breaks within the date range are considered. For this product, `date_range` is bimonthly, and one segment from CCD is always assigned to a single bimonthly period.
     - Output: Multi-band GeoTIFF raster file (.tif), with bands `last_tEnd`, `last_tBreak`, `is_break`, `ndvi_last_segment`: see `/scripts/visualisations/ccd_to_raster.py` for details.
  3. Convert each bimonthly raster file to vector format with script `/scripts/visualisations/graph_raster_to_polygons.py`:
     - Input: For each period (2 months) and for each tile: 1 multi-band GeoTIFF raster file (.tif); date is the mean between `last_tEnd` and `last_tBreak`, date_range_days = 10 (number of days to group adjacent pixels within each spatial cluster); min_area_ha = 0.5 (minimum polygon area in hectares).
     - Output: For each period (2 months) and for each tile: 1 polygon geopackage file; attributes: mean, min, max dates for each polygon; polygon area, with polygons at least 0.5 ha.



