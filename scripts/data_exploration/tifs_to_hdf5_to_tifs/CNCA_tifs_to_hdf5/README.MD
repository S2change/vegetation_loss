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

1. Bands in GeoTiff files are in the following order: `["B2", "B3", "B4", "B5", "B6", "B7", "B8", "B8a", "B11", "B12"]`. Otherwise, variable `band_names` has to be ro-ordered
1. There will be a single output `hdf5` file per Sentinel-2 tile that contains all years, i.e. there will be 17 `hdf5` files for the whole Continental Portugal and Sentinel-2 time span 
2. Ideally the output HDF5 file should store coordinates in CRS EPSG:32629 (WGS 84 / UTM zone 29N). In the current code, the CRS of the output hdf5 file is identical to the CRS of the input GeoTIFF files, but this can be changed in case the CRS of the GeoTIFF files is not EPSG:32629.
4. Vector mask: use CNCA vector mask for Portugal with a 2 km buffer. The input vector mask (any CRS) is used to filters out TIFs with no overlap with a vector mask, rasterizes the mask to identify valid pixels, and writes the sparse pixel time series to an HDF5 file. Only pixels inside the vector mask are stored in the HDF5 file.
5. Input and output *nodata* values (either missing data in GeoTIFF files or pixels outside the vector mask but within the output spatial grid) are defined in `hdf5_utils.INPUT_NODATA_VAL` and `hdf5_utils.OUTPUT_NODATA_VAL`. Currently, `INPUT_NODATA_VAL = 65535` but this should be changed if the *nodata* value of the input GeoTIFF files. The `OUTPUT_NODATA_VAL = 65535` should be left as is.

# Options

1. `create_hdf5.py`: Creates a new HDF5 file from 10-band GeoTIFF files 

    Inputs:
    - `folder_path_tifs`: Directory containing the 10-band GeoTIFF files.
    - `vector_mask_path`: Path to vector file (shapefile, GeoJSON, etc, any CRS) defining the region of interest.
    - `h5_filename`: Path for the output HDF5 file to be created.
    - `band_names`: List of Sentinel-2 band names that will used as column names in the outputted HDF5. Order should be the same order that the bands appear in the GeoTIFF files.
    - `MIN_DATE`, `MAX_DATE`: `None` or `datetime(y, m, d)`, defining the period of interest (if None, all GeoTIFF file are processed)

2. `append_hdf5.py`: Appends new timesteps to an existing HDF5 file created by `create_hdf5.py`. Timestamps already present in the HDF5 are skipped automatically. The spatial
grid (xs, ys) is read from the existing HDF5 file and new TIFs must cover the same pixel footprint.

    Inputs:
    - `folder_path_tifs`: Directory containing the 10-band GeoTIFF files.
    - `h5_filename`: Path to the existing HDF5 file to append to.
    - `MIN_DATE`, `MAX_DATE`: `None` or `datetime(y, m, d)`, defining the period of interest (if None, all GeoTIFF file are processed) 

<!---
# Example File Structure
```
portugal_S2_data
    |-- hdf5
        |-- T29SPB.h5
        |-- T29TQG.h5
        |-- ...
    |-- input_tifs
        |-- T29TQG
            |-- S2SR_image_1497179412353.tif
            |-- S2SR_image_1520335361869.tif
            |-- ...
        |-- T29TNE
            |-- ...
        |-- ...
    |-- vector_mask
        |-- mask_continental_portugal_3763.gpkg
```
--->
