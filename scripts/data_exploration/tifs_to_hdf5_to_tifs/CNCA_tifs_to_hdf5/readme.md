# version June 9, 2026

- Geotiff files for the same day (yyyy-mm-dd) are aggregated into a single timestamp. Multiple files can correspont to distinct `geotiff` subfiles (same acquisition date and distinct processing date) and/or to `geotiff` files for the same day (e.g. S2A and S2B separated by ~10 minutes);
- Output hdf5 files have fields: xs_new, ys_new, ts, original_timestamps, S2_filename, S2_original_filenames (all aggregated files), cloud_cover_pt, pixel_count_pt, clear_pixel_count_pt, count_orbit_pixels_pt,  with `cloud_cover_pt = (1-clear_pixel_count_pt/count_orbit_pixels_pt)`, where `clear_pixel_count_pt` is the number of pixels that satisfy 3 conditions: within the orbit, within the territory (pt), and not masked as clouds:
- Cloud cover is estimated just for PT and for the whole aggregate; aggregates with `cloud_cover_pt` less than `MAX_CLOUD_COVER_PT` (60%) are not stored in the output hdf5 files
- Aggregates (e.g. tile T29TPG and orbit 080) with `clear_pixel_count_pt==0` are not stored in the output hdf5 files.
- Inputs are:
  - band files, e.g. `S2C_MSIL2A_20250625-113341_N0511_R080_T29TPG_20250625T165206.tif`
  - PT cloud files (e.g. `S2C_MSIL1C_20250625-113341_N0511_R080_T29TPG_mask_omni.tif`)
  - PT mask files, e.g. `mask_T29TPG.tif`
- HDF5 chunks: (12,10,n_slots=256*256), open for appending new timestamps; COORDS_NODATA=-9999
- From the original tile, omne only keep the "tight bounding box": see bounds below and [this ilustration](https://github.com/S2change/vegetation_loss/blob/main/scripts/data_exploration/tifs_to_hdf5_to_tifs/CNCA_tifs_to_hdf5/s2_tiles_and_tight_bboxes_portugal.png)

# Instructions

- File to edit if constants need to be changed: `hdf5_utils.py`. It includes configuration constants (e.g. `TILE_NAMES`, `MIN_DATE`, `MAX_DATE`, `MAX_CLOUD_COVER_PT`), folders, etc. It also contains function `parse_filter_sort_files` that estimates `pt_cloud_cover` for each timestamp and applies the (by default 60%) maximum cloud cover filter (over the portuguese territory) and the date filter.
- File to be executed but **not to be edited**: `create_hdf5.py`. This script imports `hdf5_utils.py` and  uses `parse_filter_sort_files` to filter images to include in hdf5 file. Then, it aggregates geotiff files with identical timestamps and creates one hdf5 file for each tile.
- File to be executed but **not to be edited**: `append_hdf5.py`. The goal is to add new (later) timestamps to an existing `hdf5` file. This script imports `hdf5_utils.py` and  uses `parse_filter_sort_files` to filter images to include in hdf5 file. Then, it aggregates geotiff files with identical timestamps and appends those timestamps to the existing hdf5 file for each tile.


# File Structure

```
portugal_S2_data
    |-- hdf5
        |-- T29SPB.h5
        |-- T29TQG.h5
        |-- ...
    |-- input_tifs
        |-- 2025
            |-- S2C_MSIL2A_20251007-110951_N0511_R137_T29TPE_20251007T145121
                |-- S2C_MSIL2A_20251007-110951_N0511_R137_T29TPE_20251007T145121.tif: dados espetrais 10 bandas. NOData=65535 codifica pixels fora de tile/órbita (NoData ESA) e também codifica pixels na máscara de nuvens
                |-- S2C_MSIL1C_20251007-110951_N0511_R137_T29TPE_mask_omni.tif: 1 corresponde a nuvem; 0 corresponde a não nuvem ou a pixel exterior ao território. 
            |--- S2C_MSIL2A_20251109-112321_N0511_R037_T29SMC_20251109T130709
                |--- S2C_MSIL2A_20251109-112321_N0511_R037_T29SMC_20251109T130709.tif
                |--- S2C_MSIL1C_20251109-112321_N0511_R037_T29SMC_mask_omni.tif
            |--- S2C_MSIL2A_20251109-112321_N0511_R037_T29SMC_20251109T141914
                |--- S2C_MSIL2A_20251109-112321_N0511_R037_T29SMC_20251109T141914.tif
                |----S2C_MSIL1C_20251109-112321_N0511_R037_T29SMC_mask_omni.tif
                |-- ...
        |-- 2024
            |-- S2C_MSIL2A_2024...
                |-- S2C_MSIL2A_2024...tif
                |-- S2C_MSIL2A_2024..._mask_omni.tif
            |-- ...
        |-- ...
    |-- Mascara_PT_S2
        |--- mask_T29SMC.tif: em que 0 representa território e 1 representa fora do território 
        |--- mask_T29SMD.tif
        |--- mask_T29SNB.tif
        |--- ...
```

# Assumptions

1. Goetiff files have already been modified after OMNI cloud screening. Cloud pixels are encoded as 65535 in the geotiff spectral band files.
2. GeoTIFF filenames to be processed for a given tile (e.g. 'T29TNE') need to satisfy the following condition: `f.endswith('.tif') and 'S2' AND '_MSIL2A' in f and tile in f and 'mask_omni' not in f`
3. Bands in GeoTiff files are in the following order: `BAND_NAMES=["B2", "B3", "B4", "B5", "B6", "B7", "B8", "B8a", "B11", "B12"]`. Otherwise, variable `BAND_NAMES` in `hdf5_utils.py` has to be re-ordered. 
4. There will be a single output `hdf5` file per Sentinel-2 tile that contains all years, i.e. there will be 17 `hdf5` files for the whole Continental Portugal and Sentinel-2 time span 
5. The list of tiles is `TILE_NAMES=['T29SMC', 'T29TQF', 'T29SMD', 'T29TQG', 'T29SNB', 'T29TME', 'T29SNC', 'T29SND', 'T29SPB', 'T29SPC', 'T29TNE', 'T29SPD', 'T29TNF', 'T29TNG', 'T29TPE', 'T29TPF', 'T29TPG']` in `hdf5_utils.py`. Otherwise, it needs to be redefined.
6. The output HDF5 file should store coordinates in CRS EPSG:32629 (WGS 84 / UTM zone 29N). It also stores the S2 file name and the estimated cloud cover (over PT).
7. Raster PT masks: uses raster masks in folder `Mascara_PT_S2`, where pixels in PT have value 0, to estimate `pt_cloud_cover` and to define the bounding box for each hdf5 file. 

# PT_masks original bounding boxes and tight PT bounding boxes used to create hdf5 files

mask_T29SMC.tif
  CRS        : EPSG:32629
  Bounds     : left=399960.0, bottom=4190220.0, right=509760.0, top=4300020.0
  Shape      : 10980 rows x 10980 cols
  Value domain: min=0, max=1, unique=[0 1]
  Tight bbox (value==0): left=454470.0, bottom=4197220.0, right=509760.0, top=4300020.0
  Tight bbox shape     : 10280 rows x 5529 cols

mask_T29SMD.tif
  CRS        : EPSG:32629
  Bounds     : left=399960.0, bottom=4290240.0, right=509760.0, top=4400040.0
  Shape      : 10980 rows x 10980 cols
  Value domain: min=0, max=1, unique=[0 1]
  Tight bbox (value==0): left=453450.0, bottom=4290240.0, right=509760.0, top=4400040.0
  Tight bbox shape     : 10980 rows x 5631 cols

mask_T29SNB.tif
  CRS        : EPSG:32629
  Bounds     : left=499980.0, bottom=4090200.0, right=609780.0, top=4200000.0
  Shape      : 10980 rows x 10980 cols
  Value domain: min=0, max=1, unique=[0 1]
  Tight bbox (value==0): left=499980.0, bottom=4090200.0, right=609780.0, top=4200000.0
  Tight bbox shape     : 10980 rows x 10980 cols

mask_T29SNC.tif
  CRS        : EPSG:32629
  Bounds     : left=499980.0, bottom=4190220.0, right=609780.0, top=4300020.0
  Shape      : 10980 rows x 10980 cols
  Value domain: min=0, max=1, unique=[0 1]
  Tight bbox (value==0): left=499980.0, bottom=4190220.0, right=609780.0, top=4300020.0
  Tight bbox shape     : 10980 rows x 10980 cols

mask_T29SND.tif
  CRS        : EPSG:32629
  Bounds     : left=499980.0, bottom=4290240.0, right=609780.0, top=4400040.0
  Shape      : 10980 rows x 10980 cols
  Value domain: min=0, max=0, unique=[0]
  Tight bbox (value==0): left=499980.0, bottom=4290240.0, right=609780.0, top=4400040.0
  Tight bbox shape     : 10980 rows x 10980 cols

mask_T29SPB.tif
  CRS        : EPSG:32629
  Bounds     : left=600000.0, bottom=4090200.0, right=709800.0, top=4200000.0
  Shape      : 10980 rows x 10980 cols
  Value domain: min=0, max=1, unique=[0 1]
  Tight bbox (value==0): left=600000.0, bottom=4090200.0, right=655530.0, top=4200000.0
  Tight bbox shape     : 10980 rows x 5553 cols

mask_T29SPC.tif
  CRS        : EPSG:32629
  Bounds     : left=600000.0, bottom=4190220.0, right=709800.0, top=4300020.0
  Shape      : 10980 rows x 10980 cols
  Value domain: min=0, max=1, unique=[0 1]
  Tight bbox (value==0): left=600000.0, bottom=4190220.0, right=683130.0, top=4300020.0
  Tight bbox shape     : 10980 rows x 8313 cols

mask_T29SPD.tif
  CRS        : EPSG:32629
  Bounds     : left=600000.0, bottom=4290240.0, right=709800.0, top=4400040.0
  Shape      : 10980 rows x 10980 cols
  Value domain: min=0, max=1, unique=[0 1]
  Tight bbox (value==0): left=600000.0, bottom=4290240.0, right=679410.0, top=4400040.0
  Tight bbox shape     : 10980 rows x 7941 cols

mask_T29TME.tif
  CRS        : EPSG:32629
  Bounds     : left=399960.0, bottom=4390200.0, right=509760.0, top=4500000.0
  Shape      : 10980 rows x 10980 cols
  Value domain: min=0, max=1, unique=[0 1]
  Tight bbox (value==0): left=492090.0, bottom=4390200.0, right=509760.0, top=4460380.0
  Tight bbox shape     : 7018 rows x 1767 cols

mask_T29TNE.tif
  CRS        : EPSG:32629
  Bounds     : left=499980.0, bottom=4390200.0, right=609780.0, top=4500000.0
  Shape      : 10980 rows x 10980 cols
  Value domain: min=0, max=1, unique=[0 1]
  Tight bbox (value==0): left=499980.0, bottom=4390200.0, right=609780.0, top=4500000.0
  Tight bbox shape     : 10980 rows x 10980 cols

mask_T29TNF.tif
  CRS        : EPSG:32629
  Bounds     : left=499980.0, bottom=4490220.0, right=609780.0, top=4600020.0
  Shape      : 10980 rows x 10980 cols
  Value domain: min=0, max=1, unique=[0 1]
  Tight bbox (value==0): left=515130.0, bottom=4490220.0, right=609780.0, top=4600020.0
  Tight bbox shape     : 10980 rows x 9465 cols

mask_T29TNG.tif
  CRS        : EPSG:32629
  Bounds     : left=499980.0, bottom=4590240.0, right=609780.0, top=4700040.0
  Shape      : 10980 rows x 10980 cols
  Value domain: min=0, max=1, unique=[0 1]
  Tight bbox (value==0): left=507810.0, bottom=4590240.0, right=609780.0, top=4669240.0
  Tight bbox shape     : 7900 rows x 10197 cols

mask_T29TPE.tif
  CRS        : EPSG:32629
  Bounds     : left=600000.0, bottom=4390200.0, right=709800.0, top=4500000.0
  Shape      : 10980 rows x 10980 cols
  Value domain: min=0, max=1, unique=[0 1]
  Tight bbox (value==0): left=600000.0, bottom=4390200.0, right=690450.0, top=4500000.0
  Tight bbox shape     : 10980 rows x 9045 cols

mask_T29TPF.tif
  CRS        : EPSG:32629
  Bounds     : left=600000.0, bottom=4490220.0, right=709800.0, top=4600020.0
  Shape      : 10980 rows x 10980 cols
  Value domain: min=0, max=1, unique=[0 1]
  Tight bbox (value==0): left=600000.0, bottom=4490220.0, right=709800.0, top=4600020.0
  Tight bbox shape     : 10980 rows x 10980 cols

mask_T29TPG.tif
  CRS        : EPSG:32629
  Bounds     : left=600000.0, bottom=4590240.0, right=709800.0, top=4700040.0
  Shape      : 10980 rows x 10980 cols
  Value domain: min=0, max=1, unique=[0 1]
  Tight bbox (value==0): left=600000.0, bottom=4590240.0, right=709800.0, top=4653280.0
  Tight bbox shape     : 6304 rows x 10980 cols

mask_T29TQF.tif
  CRS        : EPSG:32629
  Bounds     : left=699960.0, bottom=4490220.0, right=809760.0, top=4600020.0
  Shape      : 10980 rows x 10980 cols
  Value domain: min=0, max=1, unique=[0 1]
  Tight bbox (value==0): left=699960.0, bottom=4566040.0, right=732690.0, top=4600020.0
  Tight bbox shape     : 3398 rows x 3273 cols

mask_T29TQG.tif
  CRS        : EPSG:32629
  Bounds     : left=699960.0, bottom=4590240.0, right=809760.0, top=4700040.0
  Shape      : 10980 rows x 10980 cols
  Value domain: min=0, max=1, unique=[0 1]
  Tight bbox (value==0): left=699960.0, bottom=4590240.0, right=736410.0, top=4651000.0
  Tight bbox shape     : 6076 rows x 3645 cols
