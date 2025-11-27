"""

This script extracts spectral values before and after break dates from a TIF file.

The input TIF file should contain break dates in YYYYMMDD format in one of its bands.
The script reads these break dates and finds the closest Sentinel-2 images before and after
each break date.

Optional polygon masking: If a polygon file is specified, only pixels within the polygon
boundaries will be processed, allowing for spatial subsetting of the analysis.

The script collects band data at two stages:
1. B2 and B11 bands (Blue and SWIR1)
2. Original 4 bands used with pyccd

Output is saved as a multi-band GeoTIFF with pre-break and post-break values for each pixel.

"""

### Execute it from within the data_exploration folder: python extract_B2B11_start_end.py ###

import numpy as np
import xarray as xr
import rioxarray
import os
import pandas as pd
from datetime import datetime
from dask.diagnostics import ProgressBar
import h5py
import time
import rasterio
from rasterio.transform import from_origin
import geopandas as gpd
from rasterio import features

import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
   sys.path.append(module_path)
from pyccd.shared.read_files import read_tif_files_gee


## SCRIPT CONFIGS ##
##################################
# Input TIF file containing break dates
break_date_tif = r"C:\Users\Public\Documents\outputs_ROI\tabular\T29TQF\processed_outputs\rasters\output_raster_ccd_20241101_to_20241231.tif"
break_date_band = 1  # Which band contains the break dates (1-indexed)

# Automatically extract tile from break_date_tif path
# Looks for pattern like T29TQF, T29TQG, etc. in the path
import re
tile_match = re.search(r'T\d{2}[A-Z]{3}', break_date_tif)
if tile_match:
    tiles = [tile_match.group()]
    print(f"Automatically detected tile from path: {tiles[0]}")
else:
    # Fallback to manual specification if pattern not found
    tiles = ['T29TQF']
    print(f"Warning: Could not auto-detect tile from path. Using fallback: {tiles[0]}")

# Optional polygon file to mask the raster (set to None to process all pixels)
polygon_file = None

s2_images_folder_B2_B11 = "C:/Users/Public/Documents/s2_images_B2_B11/"
s2_images_folder_4_bands = "D:/s2_images/"

max_date = datetime(2024, 12, 31) #limit date to collect images

output_h5_folder = "E:/outputs_ROI/hdf5/"
h5_filename = "s2_images-bands-pre-and-post-break.h5"
output_tif = r"C:\Users\isa127909\Desktop\B2B11_tests\04_first_raster_test_T29TQF_20241101_to_20241231.tif" # output path and name for tif file

# value that bands get set to if no change date processed
NODATA = 65535


##################################

def load_break_dates_from_tif(break_date_tif, break_date_band=1):
    """
    Loads break dates from a TIF file.

    Args:
        break_date_tif (str): Path to the TIF file containing break dates in YYYYMMDD format.
        break_date_band (int): Which band to read (1-indexed).

    Returns:
        tuple: (break_dates_array, x_coords, y_coords, transform, crs) where break_dates_array
               contains dates in YYYYMMDD format (0 for no break).
    """
    print(f"Loading break dates from {break_date_tif}, band {break_date_band}")

    # Open the TIF file with rioxarray
    break_dates_da = rioxarray.open_rasterio(break_date_tif, chunks={'x': -1, 'y': 100, 'band': -1})

    # Select the specified band (convert from 1-indexed to 0-indexed)
    break_dates_band = break_dates_da.isel(band=break_date_band - 1)

    # Get the break dates array
    break_dates_array = break_dates_band.values

    # Get coordinates
    x_coords = break_dates_band.x.values
    y_coords = break_dates_band.y.values

    # Get transform and CRS
    transform = break_dates_band.rio.transform()
    crs = break_dates_band.rio.crs

    print(f"Loaded break dates with shape: {break_dates_array.shape}")
    print(f"Break date range: {break_dates_array[break_dates_array > 0].min()} to {break_dates_array[break_dates_array > 0].max()}")
    print(f"Number of pixels with breaks: {np.sum(break_dates_array > 0)}")

    return break_dates_array, x_coords, y_coords, transform, crs


def load_polygon_mask(polygon_file, break_dates_array, x_coords, y_coords, transform, crs):
    """
    Creates a boolean mask from a polygon file that matches the raster dimensions.

    Args:
        polygon_file (str): Path to polygon file (shapefile, geopackage, etc.).
        break_dates_array (np.ndarray): 2D array to match dimensions.
        x_coords (np.ndarray): X coordinates of the raster.
        y_coords (np.ndarray): Y coordinates of the raster.
        transform (affine.Affine): Affine transform of the raster.
        crs: CRS of the raster.

    Returns:
        np.ndarray: Boolean mask array where True = inside polygon, False = outside.
    """
    print(f"Loading polygon mask from {polygon_file}")

    # Read the polygon file
    gdf = gpd.read_file(polygon_file)

    # Reproject to match raster CRS if needed
    if gdf.crs != crs:
        print(f"Reprojecting polygon from {gdf.crs} to {crs}")
        gdf = gdf.to_crs(crs)

    # Create a mask by rasterizing the polygons
    mask = features.rasterize(
        [(geom, 1) for geom in gdf.geometry],
        out_shape=break_dates_array.shape,
        transform=transform,
        fill=0,
        dtype='uint8'
    )

    # Convert to boolean
    mask = mask.astype(bool)

    print(f"Polygon mask created: {np.sum(mask)} pixels inside polygon")

    return mask


def create_dataframe_from_break_dates(break_dates_array, x_coords, y_coords, polygon_mask=None):
    """
    Creates a dataframe from the break dates TIF data.

    Args:
        break_dates_array (np.ndarray): 2D array of break dates in YYYYMMDD format.
        x_coords (np.ndarray): X coordinates.
        y_coords (np.ndarray): Y coordinates.
        polygon_mask (np.ndarray, optional): Boolean mask where True = inside polygon.

    Returns:
        pandas.DataFrame: DataFrame with columns x_coord, y_coord, break_date_yyyymmdd.
    """
    # Find all pixels with valid break dates
    valid_mask = (break_dates_array > 0)

    # Apply polygon mask if provided
    if polygon_mask is not None:
        valid_mask = valid_mask & polygon_mask

    # Get indices of valid pixels
    y_indices, x_indices = np.where(valid_mask)

    # Get corresponding coordinates and break dates
    x_pixel_coords = x_coords[x_indices]
    y_pixel_coords = y_coords[y_indices]
    break_dates = break_dates_array[y_indices, x_indices]

    # Create dataframe
    df = pd.DataFrame({
        'x_coord': x_pixel_coords,
        'y_coord': y_pixel_coords,
        'break_date_yyyymmdd': break_dates
    })

    print(f"Created dataframe with {len(df)} pixels with breaks")

    return df


def yyyymmdd_to_ordinal(yyyymmdd):
    """
    Converts date in YYYYMMDD format to ordinal.

    Args:
        yyyymmdd (int): Date in YYYYMMDD format.

    Returns:
        int: Date as ordinal, or None if invalid.
    """
    if yyyymmdd == 0 or pd.isna(yyyymmdd):
        return None

    try:
        date_str = str(int(yyyymmdd))
        year = int(date_str[:4])
        month = int(date_str[4:6])
        day = int(date_str[6:8])
        date_obj = datetime(year, month, day)
        return date_obj.toordinal()
    except (ValueError, IndexError) as e:
        print(f"Error converting {yyyymmdd} to ordinal: {e}")
        return None


# def find_closest_image(target_ordinal, available_ordinals, search_before=True):
#     """
#     Finds the closest available image to the target date.

#     Args:
#         target_ordinal (int): Target date in ordinal format.
#         available_ordinals (np.ndarray): Array of available image dates in ordinal format.
#         search_before (bool): If True, search for images before the target (pre-break).
#                               If False, search for images after the target (post-break).

#     Returns:
#         int: Index of the closest image, or -1 if none found.
#     """
#     if target_ordinal is None:
#         return -1

#     if search_before:
#         # Find images before or on the break date
#         mask = available_ordinals <= target_ordinal
#         if not np.any(mask):
#             return -1
#         # Get the latest image before the break date (closest to break date)
#         valid_indices = np.where(mask)[0]
#         return valid_indices[-1]  # Last index (latest date before break)
#     else:
#         # Find images after or on the break date
#         mask = available_ordinals >= target_ordinal
#         if not np.any(mask):
#             return -1
#         # Get the earliest image after the break date (closest to break date)
#         valid_indices = np.where(mask)[0]
#         return valid_indices[0]  # First index (earliest date after break)


# def get_indices(df, geotiffs_da):
#     """
#     Gets the indices for the xarray selection with isel. Uses the coordinates x and y from the dataframe
#     and the break dates to find the closest pre- and post-break images.

#     Args:
#         df (pandas.dataframe): DataFrame with x_coord, y_coord, break_date_ordinal columns.
#         geotiffs_da (xarray.DataArray): DataArray with time series of Sentinel-2 images.

#     Returns:
#         tuple: (x_inds, y_inds, time_end_inds, time_start_inds) where time_end_inds are pre-break
#                and time_start_inds are post-break.
#     """
#     points_x_int = xr.DataArray(np.round(df.x_coord.values).astype('int'), dims=['location'])
#     points_y_int = xr.DataArray(np.round(df.y_coord.values).astype('int'), dims=['location'])

#     x_coords = geotiffs_da.x.values
#     y_coords = geotiffs_da.y.values
#     times = geotiffs_da.time.values

#     x_inds = np.searchsorted(x_coords, points_x_int.values, side='left')
#     y_inds = np.searchsorted(y_coords, points_y_int.values, side='left')

#     # Find pre- and post-break images for each pixel
#     time_end_inds = np.zeros(len(df), dtype=int)
#     time_start_inds = np.zeros(len(df), dtype=int)

#     for i, break_ordinal in enumerate(df.break_date_ordinal.values):
#         # Find pre-break image (closest before break date)
#         pre_idx = find_closest_image(break_ordinal, times, search_before=True)
#         if pre_idx == -1:
#             print(f"Warning: No pre-break image found for pixel {i} (break date ordinal: {break_ordinal})")
#             pre_idx = 0  # Default to first image
#         time_end_inds[i] = pre_idx

#         # Find post-break image (closest after break date)
#         post_idx = find_closest_image(break_ordinal, times, search_before=False)
#         if post_idx == -1:
#             print(f"Warning: No post-break image found for pixel {i} (break date ordinal: {break_ordinal})")
#             post_idx = len(times) - 1  # Default to last image
#         time_start_inds[i] = post_idx

#     return x_inds, y_inds, time_end_inds, time_start_inds

def get_indices(df, geotiffs_da):
    """
    Gets the indices for the xarray selection with isel. Uses the coordinates x and y from the dataframe
    and the break dates to find the closest pre- and post-break images.

    Args:
        df (pandas.dataframe) :  DataFrame with x_coord, y_coord, break_date_ordinal columns.
        geotiffs_da (xarray.DataArray) : DataArray with time series of Sentinel-2 images (B2 and B11).

    Returns indices. 
    """

    # COORDENADAS X E Y DOS PONTOS ESCOLHIDOS

    points_x_int = xr.DataArray(np.round(df.x_coord.values).astype('int'), dims=['location'])
    points_y_int = xr.DataArray(np.round(df.y_coord.values).astype('int'), dims=['location'])

    # Use break_date_ordinal for both pre and post
    break_dates = xr.DataArray(np.round(df.break_date_ordinal.values).astype('int'), dims=['z'])

    x_coords = geotiffs_da.x.values
    y_coords = geotiffs_da.y.values
    times = geotiffs_da.time.values

    x_inds = np.searchsorted(x_coords, points_x_int.values, side='left')
    y_inds = np.searchsorted(y_coords, points_y_int.values, side='left')
    time_end_inds = np.searchsorted(times, break_dates.values, side='left') - 1
    time_start_inds = np.searchsorted(times, break_dates.values, side='left')

    return x_inds, y_inds, time_end_inds, time_start_inds

def save_to_hdf5(result, selected_values, output_h5_path):
    """
    Saves the selection of band values to a hdf5 file.

    Args:
        result (np.ndarray) : array with selection of band values.
        selected_values (xarray.DataArray) : DataArray object of selected values.
        output_h5_path (str) : path to directory where the file should be saved.
    """

    with h5py.File(output_h5_path, 'w') as hf:

        data = hf.create_dataset(
            "values",
            shape=result.shape,
            dtype='uint16',
            compression='lzf', #supposed to be the best compression for fast read/write
            chunks=(1, 1024, 1)
        )

        xs = hf.create_dataset("xs", shape=(result.shape[1],), dtype='int32', compression='gzip', compression_opts=9)
        ys = hf.create_dataset("ys", shape=(result.shape[1],), dtype='int32', compression='gzip', compression_opts=9)

        data[:] = result
        xs[:] = selected_values.x.values
        ys[:] = selected_values.y.values

def get_transform_crs(xarray_da):
    """
    Gets the Geo Transform and the CRS for creating a GeoTiff.

    Args:
        xarray_da (xarray DataArray) : tifs opened as a DataArray object.

    Returns the transform and crs.

    """
    xr_geo_transform = xarray_da.spatial_ref.GeoTransform
    xr_geo_transform = xr_geo_transform.split()
    transform = from_origin(float(xr_geo_transform[0]), float(xr_geo_transform[3]), float(xr_geo_transform[1]), -float(xr_geo_transform[-1]))
    crs = xarray_da.rio.crs

    return transform, crs

def create_tiff(xarray_da, x_inds, y_inds, result): #(2, n_points, 6)
    """
    Creates output GeoTIFF with band values.
    """
    #create mask
    mask = np.zeros((xarray_da.sizes['x'], xarray_da.sizes['y']), dtype='uint32')
    mask[:] = NODATA #sets all elements to NODATA

    #get transform and crs
    transform, crs = get_transform_crs(xarray_da)

    with rasterio.open(
        output_tif,
        "w",
        driver="GTiff",
        height=mask.shape[1],
        width=mask.shape[0],
        count=result.shape[0]*result.shape[-1] - 1,  # -1 to exclude duplicate change date
        dtype='uint32',
        crs=crs,
        transform=transform,
        nodata=NODATA,
        compress="deflate"
    ) as dst:

        #loop through result array and write to GeoTiff
        band_number = 1
        for t in range(result.shape[0]): #time dimension - first is before break, then post break

            for b in range(result.shape[-1]): #band dimension

                # Skip post-break change date (t=1, b=6) to avoid duplicate
                if t == 1 and b == 6:  # Post-break change date
                    continue

                #pixels inside mask (analyzed by pyccd) are set to the correct value given by result
                mask[x_inds, -y_inds] = result[t,:,b] #use -y_inds because y_coord is reversed

                # Shift up by 1 pixel to correct displacement
                shifted = np.roll(mask.T, shift=-1, axis=0)
                # Set last row to nodata
                shifted[-1, :] = NODATA

                dst.write(shifted, band_number) #without shifting: dst.write(mask.T, band_number)

                band_number += 1

    print(f"\nFinished writing TIF to: {output_tif}")


def main():

    # Load break dates from TIF file
    break_dates_array, x_coords, y_coords, transform, crs = load_break_dates_from_tif(break_date_tif, break_date_band)

    # Load polygon mask if specified
    polygon_mask = None
    if polygon_file is not None:
        polygon_mask = load_polygon_mask(polygon_file, break_dates_array, x_coords, y_coords, transform, crs)

    # Create dataframe from break dates
    combined_df = create_dataframe_from_break_dates(break_dates_array, x_coords, y_coords, polygon_mask)

    # Convert break dates from YYYYMMDD to ordinal for image lookup
    print("Converting break dates to ordinal format...")
    combined_df['break_date_ordinal'] = combined_df['break_date_yyyymmdd'].apply(yyyymmdd_to_ordinal)

    # Remove pixels with invalid break dates
    valid_mask = combined_df['break_date_ordinal'].notna()
    print(f"Removing {(~valid_mask).sum()} pixels with invalid break dates")
    combined_df = combined_df[valid_mask].reset_index(drop=True)

    print(f"Sample break dates (YYYYMMDD): {combined_df['break_date_yyyymmdd'].head(10).tolist()}")
    print(f"Sample break dates (ordinal): {combined_df['break_date_ordinal'].head(10).tolist()}")
    print(f"Min/Max break dates: {combined_df['break_date_yyyymmdd'].min()} / {combined_df['break_date_yyyymmdd'].max()}")
    print(f"Processing {len(combined_df)} pixels with valid break dates")

    stages = {'Bands B2 and B11': s2_images_folder_B2_B11, 'Original 4 bands':s2_images_folder_4_bands}

    #initialize empty array of shape (2, n_points, 7) -> where 2 corresponds to data from pre and post break, 7th band is change date
    result = np.empty((2, len(combined_df), 7), dtype=np.float32)

    for k,v in stages.items(): #order matters - it goes in alphabetical order of keys (does B2B11 first)

        print("Collecting spectral info from {}".format(k))

        s2_images_folder = v

        #get tif dates
        tif_names, tif_dates = read_tif_files_gee(tile, os.path.join(s2_images_folder, tile), max_date)
        tif_dates_ord = [d.toordinal() for d in tif_dates]

        #crate xarray time variable
        time_var = xr.Variable('time',tif_dates_ord)

        # Load in and concatenate all individual GeoTIFFs
        tifs_xr = [rioxarray.open_rasterio(os.path.join(s2_images_folder, tile, i), chunks={'x':-1, 'y':100, 'band':-1}) for i in tif_names]
        geotiffs_da = xr.concat(tifs_xr, dim=time_var)

        #get indices to perform the selection of data
        x_inds, y_inds, time_end_inds, time_start_inds = get_indices(combined_df, geotiffs_da)

        #initialize Dask progress bar
        ProgressBar().register()

        #perform value selection based on coordinates and dates
        #step 1.2: get values of tend (can be done directly, since we already have access to the indices)
        print('---- Collecting values pre break')
        #select values
        selected_values = geotiffs_da.isel(
            x=xr.DataArray(x_inds, dims='z'),
            y=xr.DataArray(y_inds, dims='z'),
            time=xr.DataArray(time_end_inds, dims='z')
        )
        #fill result array
        if k == 'Bands B2 and B11':
            result[0,:,:2] = selected_values.values
        elif k == 'Original 4 bands':
            result[0,:,2:6] = selected_values.values

        #step 2: get values of tstart (can be done directly, since we already have access to the indices)
        print('---- Collecting values post break')
        #select values
        selected_values = geotiffs_da.isel(
            x=xr.DataArray(x_inds, dims='z'),
            y=xr.DataArray(y_inds, dims='z'),
            time=xr.DataArray(time_start_inds, dims='z')
        )
         #fill result array
        if k == 'Bands B2 and B11':
            result[1,:,:2] = selected_values.values
        elif k == 'Original 4 bands':
            result[1,:,2:6] = selected_values.values

    # Add break dates only to pre-break period (index 6)
    break_dates_array = combined_df['break_date_yyyymmdd'].to_numpy()
    result[0,:,6] = break_dates_array  # Pre-break period
    result[1,:,6] = 0  # Post-break period set to 0 (will be skipped in output)
    
    #reorder result to have bands in natural order (B2, B3, B4, B8, B11, B12, change_date)
    idx_order = [0,2,3,4,1,5,6]
    result = result[:,:,idx_order]

    #save result to hdf5 file
    # output_path_with_filename = os.path.join(output_h5_folder, tile, h5_filename)
    # save_to_hdf5(result, selected_values, output_path_with_filename)

    # save tif
    create_tiff(geotiffs_da, x_inds, y_inds, result)

    
    
if __name__ == '__main__':
    for t in tiles:
        tile = t
        t1 = time.time()
        print("Started execution for tile {}.".format(tile))

        main()

        print("Execution finished - HDF5 file created for tile {}. Execution took {} minutes".format(tile, round((time.time()-t1)/60,2)))

# TODO
#separar os hdf5 do pre e pos break dos outros hdf5 (pasta separada) e.g. pre-post
#nomear o hdf5 com as datas limites que foram usadas
