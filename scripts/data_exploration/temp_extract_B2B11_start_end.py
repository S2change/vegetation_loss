"""

This script is intended to extract the spectral values before and after the most recent break date identified by pyccd.
It uses the end date of the second to last segment and the start date of the last segment to look up for the bands values.

Currently, the script collects band data at two stages: first from the B2 and B11 bands (Blue and SWIR1) and then from the 
original 4 bands with which pyccd was executed.

Note: this script should be an improvised fix to acquire the band values; a more definitive solution should include
acquiring B2 and B11 data as part of the pyccd processing.

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

import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
   sys.path.append(module_path)
from pyccd.shared.read_files import read_tif_files_gee


## SCRIPT CONFIGS ##
##################################
tile = "T29TNE"
parquet_folder = "C:/Users/Public/Documents/outputs_ROI/tabular/"

s2_images_folder_B2_B11 = "C:/Users/Public/Documents/s2_images_B2_B11/"
s2_images_folder_4_bands = "D:/s2_images/"

max_date = datetime(2024, 12, 31) #limit date to collect images

# Manual dates for stable pixels (pixels with no breaks)
# If None, uses tStart for pre-break and tEnd for post-break dates
stable_pixel_start_date = None  # datetime(2018, 1, 1) - example manual start date
stable_pixel_end_date = None    # datetime(2023, 12, 31) - example manual end date

#TODO - NEW BAND WITH YYYYMMDD (break date)

NODATA = 65535

output_h5_folder = "E:/outputs_ROI/hdf5/"

h5_filename = "s2_images-bands-pre-and-post-break.h5"


##################################

def filter_segments(df):
    """
    Takes the pyccd output dataframe (i.e. parquet files) and extracts dates pre and post break for each pixel.
    For pixels with breaks: Pre break date is the tEnd of the second to last segment; Post break date is the tStart of the last segment.
    For stable pixels (no breaks): Uses tStart and tEnd of the single segment, or manual dates if specified.

    Args:
        df (pandas.dataframe) : pandas dataframe of pyccd results.

    Returns a filtered dataframe with both change pixels and stable pixels.
    
    """
    
    # Calculate segment counts efficiently
    segment_counts = df.groupby(['x_coord','y_coord']).size()
    df = df.merge(segment_counts.rename('segment_count'), left_on=['x_coord','y_coord'], right_index=True, how='left')
    
    # Separate change pixels (multiple segments) from stable pixels (single segment)
    change_pixels_df = df[df['segment_count'] > 1].copy()
    stable_pixels_df = df[df['segment_count'] == 1].copy()
    
    # Process change pixels (existing logic)
    if len(change_pixels_df) > 0:
        #create column with maximum start date of the group
        change_pixels_df['max_tstart_group'] = change_pixels_df.groupby(['x_coord','y_coord'])['tStart'].transform('max')
        
        #determine final date of the pixel (gets tEnd with highest value)
        change_pixels_df['final_date_group'] = change_pixels_df.groupby(['x_coord','y_coord'])['tEnd'].transform('max')
        #remove rows where the tBreak is the final date (end of series)
        change_pixels_df = change_pixels_df.loc[change_pixels_df.tBreak!=change_pixels_df.final_date_group].copy()
        change_pixels_df = change_pixels_df.loc[~change_pixels_df.tStart.isnull()]
        #remove rows where the tBreak is within a 20 day margin from the final date
        change_pixels_df = change_pixels_df.loc[change_pixels_df.final_date_group - change_pixels_df.tBreak > 20*24*60*60*1000].copy()
        #compute most recent tEnd
        change_pixels_df['max_tend_group'] = change_pixels_df.groupby(['x_coord','y_coord'])['tEnd'].transform('max')
        #compute max tbreak - just for quality control
        change_pixels_df['max_tbreak_group'] = change_pixels_df.groupby(['x_coord','y_coord'])['tBreak'].transform('max')

        #get only the coordinates and dates for change pixels
        change_df_sub = change_pixels_df[['x_coord','y_coord','max_tstart_group','max_tend_group','max_tbreak_group']].copy()
        change_df_sub = change_df_sub.drop_duplicates()
        change_df_sub['pixel_type'] = 'change'
    else:
        change_df_sub = pd.DataFrame(columns=['x_coord','y_coord','max_tstart_group','max_tend_group','max_tbreak_group','pixel_type'])
    
    # Process stable pixels (new logic)
    if len(stable_pixels_df) > 0:
        # For stable pixels, use manual dates if specified, otherwise use tStart/tEnd
        if stable_pixel_start_date is not None:
            stable_pixels_df['max_tend_group'] = stable_pixel_start_date.timestamp() * 1000  # Convert to milliseconds
        else:
            stable_pixels_df['max_tend_group'] = stable_pixels_df['tStart']  # Use tStart as "pre-break"
            
        if stable_pixel_end_date is not None:
            stable_pixels_df['max_tstart_group'] = stable_pixel_end_date.timestamp() * 1000  # Convert to milliseconds
        else:
            stable_pixels_df['max_tstart_group'] = stable_pixels_df['tEnd']  # Use tEnd as "post-break"
        
        stable_pixels_df['max_tbreak_group'] = -1  # No break detected for stable pixels
        
        #get only the coordinates and dates for stable pixels
        stable_df_sub = stable_pixels_df[['x_coord','y_coord','max_tstart_group','max_tend_group','max_tbreak_group']].copy()
        stable_df_sub = stable_df_sub.drop_duplicates()
        stable_df_sub['pixel_type'] = 'stable'
    else:
        stable_df_sub = pd.DataFrame(columns=['x_coord','y_coord','max_tstart_group','max_tend_group','max_tbreak_group','pixel_type'])
    
    # Combine both types of pixels
    df_combined = pd.concat([change_df_sub, stable_df_sub], ignore_index=True)

    return df_combined

# concatenate all parquet files of a given tile
def combine_parquet_files(parquet_folder, tile):
    """
    Combines all Parquet files in a directory into a single Parquet file.

    Args:
        parquet_folder (str) : Path to the directory containing Parquet files (root).
        tile (str) : tile name to access the correct folder.

    
    Returns a merged dataframe.

    """
    
    input_dir = os.path.join(parquet_folder, tile)

    # List all .parquet files in the directory
    parquet_files = [f for f in os.listdir(input_dir) if f.endswith('.parquet')]

    # Read and concatenate all Parquet files
    dataframes = []
    for file in parquet_files:
        file_path = os.path.join(input_dir, file)
        df = pd.read_parquet(file_path)
        filtered = filter_segments(df)
        dataframes.append(filtered)

    # Combine all dataframes
    combined_df = pd.concat(dataframes, ignore_index=True)

    return combined_df


def get_indices(df, geotiffs_da):
    """
    Gets the indices for the xarray selection with isel. Uses the coordinates x and y from the pyccd dataframe.

    Args:
        df (pandas.dataframe) : pyccd dataframe for the whole tile (after merging small parquets).
        geotiffs_da (xarray.DataArray) : DataArray with time series of Sentinel-2 images (B2 and B11).

    Returns indices. 
    """

    # COORDENADAS X E Y DOS PONTOS ESCOLHIDOS

    points_x_int = xr.DataArray(np.round(df.x_coord.values).astype('int'), dims=['location'])
    points_y_int = xr.DataArray(np.round(df.y_coord.values).astype('int'), dims=['location'])
    end_dates =  xr.DataArray(np.round(df.tend_ordinal.values).astype('int'), dims=['z'])
    start_dates =  xr.DataArray(np.round(df.tstart_ordinal.values).astype('int'), dims=['z'])

    x_coords = geotiffs_da.x.values
    y_coords = geotiffs_da.y.values
    times = geotiffs_da.time.values

    x_inds = np.searchsorted(x_coords, points_x_int.values, side='left')
    y_inds = np.searchsorted(y_coords, points_y_int.values, side='left')
    time_end_inds = np.searchsorted(times, end_dates.values, side='left')
    time_start_inds = np.searchsorted(times, start_dates.values, side='left')

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
    
    """

    #create mask
    mask = np.zeros((xarray_da.sizes['x'], xarray_da.sizes['y']), dtype='uint16')
    mask[:] = NODATA #sets all elements to NODATA

    #get transform and crs
    transform, crs = get_transform_crs(xarray_da)

    with rasterio.open(
        r"C:\Users\g20180450\Desktop\test_tif_mask_v2.tif",  #TODO - CHANGE NAME
        "w",
        driver="GTiff",
        height=mask.shape[1],
        width=mask.shape[0],
        count=result.shape[0]*result.shape[-1],
        dtype='uint16',
        crs=crs,
        transform=transform,
        nodata=NODATA,
        compress="deflate"
    ) as dst:
        
        #loop through result array and write to GeoTiff
        band_number = 1
        for t in range(result.shape[0]): #time dimension - first is before break, then post break

            for b in range(result.shape[-1]): #band dimension

                #pixels inside mask (analyzed by pyccd) are set to the correct value given by result
                mask[x_inds, -y_inds] = result[t,:,b] #use -y_inds because y_coord is reversed

                # Shift up by 1 pixel to correct displacement
                shifted = np.roll(mask.T, shift=-1, axis=0)
                # Set last row to nodata
                shifted[-1, :] = NODATA
                
                dst.write(shifted, band_number) #without shifting: dst.write(mask.T, band_number)

                band_number += 1


def main():

    #combine small parquets into single dataframe
    combined_df = combine_parquet_files(parquet_folder, tile)

    #convert dates from ms to ordinal
    combined_df['tstart_ordinal'] = combined_df['max_tstart_group'].apply(lambda x: datetime.utcfromtimestamp(x/1000).toordinal())
    combined_df['tend_ordinal'] = combined_df['max_tend_group'].apply(lambda x: datetime.utcfromtimestamp(x/1000).toordinal())

    stages = {'Bands B2 and B11': s2_images_folder_B2_B11, 'Original 4 bands':s2_images_folder_4_bands}

    #initialize empty array of shape (2, n_points, 6) -> where 2 corresponds to data from pre and post break
    result = np.empty((2, len(combined_df), 6), dtype=np.float32)
    
    # Print information about pixel types
    if 'pixel_type' in combined_df.columns:
        change_count = len(combined_df[combined_df['pixel_type'] == 'change'])
        stable_count = len(combined_df[combined_df['pixel_type'] == 'stable'])
        print(f"Processing {change_count} change pixels and {stable_count} stable pixels")

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
            result[0,:,2:] = selected_values.values

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
            result[1,:,2:] = selected_values.values

    #reorder result to have bands in natural order (B2, B3, B4, B8, B11, B12)
    idx_order = [0,2,3,4,1,5]
    result = result[:,:,idx_order]

    #save result to hdf5 file
    output_path_with_filename = os.path.join(output_h5_folder, tile, h5_filename)
    #save_to_hdf5(result, selected_values, output_path_with_filename)
    create_tiff(geotiffs_da, x_inds, y_inds, result)

    
    
if __name__ == '__main__':
    for t in ['T29TME']:#['T29SMC','T29SNB','T29SNC','T29SPB','T29SPC','T29TME','T29TNF','T29TNG','T29TPE','T29TQG']:
        tile = t
        t1 = time.time()
        print("Started execution for tile {}.".format(tile))

        main()

        print("Execution finished - HDF5 file created for tile {}. Execution took {} minutes".format(tile, round((time.time()-t1)/60,2)))

# TODO
#separar os hdf5 do pre e pos break dos outros hdf5 (pasta separada) e.g. pre-post
#nomear o hdf5 com as datas limites que foram usadas
