# Assumptions

1. GeoTIFF files are stored all in the same folder
2. The GeoTIFF file name includes the timestamp (e.g. a sequence of 13 digits as in `S2SR_image_1667647823345.tif`)
3. CRS: Ideally GeoTIFF files should use CRS EPSG:32629 (UTM 29N). The CRS of the output hdf5 file is identical to the CRS of the input GeoTIFF files.
4. Vector mask: an input vector mask (any CRS) is used to filters out TIFs with no overlap with a vector mask, rasterizes the mask to identify valid pixels, and writes the sparse pixel time series to an HDF5 file. Only pixels inside the vector mask are stored in the HDF5 file.

# Options

1. `create_hdf5.py`: Creates a new HDF5 file from 10-band GeoTIFF files in a specified folder. 

    Inputs:
    - `folder_path_tifs`: Directory containing the 10-band GeoTIFF files.
    - `vector_mask_path`: Path to vector file (shapefile, GeoJSON, etc, any CRS) defining the region of interest.
    - `h5_filename`: Path for the output HDF5 file to be created.
    - `MIN_DATE`, `MAX_DATE`: `None` or `datetime(y, m, d)`, defining the period of interest (if None, all GeoTIFF file are processed)

2. `append_hdf5.py`: Appends new timesteps to an existing HDF5 file created by `create_hdf5.py`. Timestamps already present in the HDF5 are skipped automatically. The spatial
grid (xs, ys) is read from the existing HDF5 file and new TIFs must cover the same pixel footprint.

    Inputs:
    - `folder_path_tifs`: Directory containing the 10-band GeoTIFF files.
    - `h5_filename`: Path to the existing HDF5 file to append to.
    - `MIN_DATE`, `MAX_DATE`: `None` or `datetime(y, m, d)`, defining the period of interest (if None, all GeoTIFF file are processed) 
