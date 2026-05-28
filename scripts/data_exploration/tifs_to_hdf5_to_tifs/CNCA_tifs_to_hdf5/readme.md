- `create_hdf5.py` edited by Gonçalo Barradas (to be run on the INCD platform; this script read a report file with cloud cover estimates per tile and date and applies a 60% threshold)
- `append_hdf5.py` : to be tested
- `hdf5_utils.py`: auxiliary functions and constants

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
                |-- S2C_MSIL2A_20251007-110951_N0511_R137_T29TPE_20251007T145121.tif
                |-- S2C_MSIL1C_20251007-110951_N0511_R137_T29TPE_mask_omni.tif
            |-- ...
        |-- 2024
            |-- S2C_MSIL2A_2024...
                |-- S2C_MSIL2A_2024...tif
                |-- S2C_MSIL2A_2024..._mask_omni.tif
            |-- ...
        |-- ...
    |-- vector_mask
        |-- mask_continental_portugal_CNCA.gpkg
```

# Assumptions

1. GeoTIFF filenames to be processed for a given tile (e.g. 'T29TNE') need to satisfy the following condition: `f.endswith('.tif') and 'S2C_MSIL2A' in f and tile in f and 'mask_omni' not in f`
1. Bands in GeoTiff files are in the following order: `BAND_NAMES=["B2", "B3", "B4", "B5", "B6", "B7", "B8", "B8a", "B11", "B12"]`. Otherwise, variable `BAND_NAMES` in `hdf5_utils.py` has to be re-ordered. 
1. There will be a single output `hdf5` file per Sentinel-2 tile that contains all years, i.e. there will be 17 `hdf5` files for the whole Continental Portugal and Sentinel-2 time span 
2. The list of tiles is `TILE_NAMES=['T29SMC', 'T29TQF', 'T29SMD', 'T29TQG', 'T29SNB', 'T29TME', 'T29SNC', 'T29SND', 'T29SPB', 'T29SPC', 'T29TNE', 'T29SPD', 'T29TNF', 'T29TNG', 'T29TPE', 'T29TPF', 'T29TPG']` in `hdf5_utils.py`. Otherwise, it needs to be redefined.
2. Ideally the output HDF5 file should store coordinates in CRS EPSG:32629 (WGS 84 / UTM zone 29N). In the current code, the CRS of the output hdf5 file is identical to the CRS of the input GeoTIFF files, but this can be changed in case the CRS of the GeoTIFF files is not EPSG:32629.
4. Vector mask: use CNCA vector mask for Portugal with a 2 km buffer. The input vector mask (any CRS) is used to filters out TIFs with no overlap with a vector mask, rasterizes the mask to identify valid pixels, and writes the sparse pixel time series to an HDF5 file. Only pixels inside the vector mask are stored in the HDF5 file.
5. Input and output *nodata* values (either missing data in GeoTIFF files or pixels outside the vector mask but within the output spatial grid) are defined in `hdf5_utils.INPUT_NODATA_VAL` and `hdf5_utils.OUTPUT_NODATA_VAL`. Currently, `INPUT_NODATA_VAL = 65535` but this should be changed if the *nodata* value of the input GeoTIFF files. The `OUTPUT_NODATA_VAL = 65535` should be left as is.

# Options

1. `create_hdf5.py`: Creates a new HDF5 file from 10-band GeoTIFF files 

    Inputs:
    - 'folder_tifs': Directory containing the 10-band GeoTIFF files.
    - 'vector_mask_path': Path to vector file (shapefile, GeoJSON, etc.) defining the region of interest.
    - 'folder_hdf5': Path for the folder where the output HDF5 files will be saved.
    - 'MIN_DATE' and 'MAX_DATE': Optional date filters to only include TIFs within a certain date range, based on the timestamp in the filename.
    - `band_names`: List of Sentinel-2 band names that will used as column names in the outputted HDF5. Order should be the same order that the bands appear in the GeoTIFF files.
    - `MIN_DATE`, `MAX_DATE`: `None` or `datetime(y, m, d)`, defining the period of interest (if None, all GeoTIFF file are processed)

2. `append_hdf5.py`: Appends new timesteps to an existing HDF5 file created by `create_hdf5.py`. Timestamps already present in the HDF5 are skipped automatically. The spatial
grid (xs, ys) is read from the existing HDF5 file and new TIFs must cover the same pixel footprint.

    Inputs:
    - 'folder_tifs': Directory containing the 10-band GeoTIFF files.
    - 'folder_hdf5': Path to the folder containing the existing HDF5 files (one per tile).
    - 'MIN_DATE' and 'MAX_DATE': Optional date filters to only include TIFs within a certain date range, based on the timestamp in the filename.

3. `reconstruct_tifs.py`: script to test the output hdf5 file. From the hdf5 file it creates one tif file with all bands corresponding to the first timestamp, or to the date closest to  `DATE_OUTPUT` if provided.

# PT_masks

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
