"""

This script is intended to extract the spectral values of bands B2 and B11 (Blue and SWIR1) before and after the
most recent break date identified by pyccd. It uses the end date of the second to last segment and the start date
of the last segment to look up for the B2 and B11 values.

Note: the script should be an improvised fix to acquire the band values; a more definitive solution should include
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

import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
   sys.path.append(module_path)
from pyccd.shared.read_files import read_tif_files_gee


## SCRIPT CONFIGS ##
##################################
tile = "T29TNE"
parquet_folder = "C:/Users/Public/Documents/outputs_ROI/tabular/"

s2_images_folder = "C:/Users/Public/Documents/s2_images_B2_B11/"

max_date = datetime(2024, 12, 31) #limit date to collect images

output_h5_folder = "E:/outputs_ROI/hdf5/"

h5_filename = "s2_images-B2B11-pre-and-post-break.h5"


##################################

def filter_segments(df):
    """
    Takes the pyccd output dataframe (i.e. parquet files) and extracts only the dates pre and post break for each pixel. 
    Pre break date is the tEnd of the second to last segment; Post break date is the tStart of the last segment.

    Args:
        df (pandas.dataframe) : pandas dataframe of pyccd results.

    Returns a filtered dataframe.
    
    """

    #create column with maximum start date of the group
    df['max_tstart_group'] = df.groupby(['x_coord','y_coord'])['tStart'].transform('max')
    
    #determine final date of the pixel (gets tEnd with highest value)
    df['final_date_group'] = df.groupby(['x_coord','y_coord'])['tEnd'].transform('max')
    #remove rows where the tBreak is the final date (end of series)
    df = df.loc[df.tBreak!=df.final_date_group].copy()
    df = df.loc[~df.tStart.isnull()]
    #remove rows where the tBreak is within a 20 day margin from the final date
    df = df.loc[df.final_date_group - df.tBreak > 20*24*60*60*1000].copy()
    #compute most recent tEnd
    df['max_tend_group'] = df.groupby(['x_coord','y_coord'])['tEnd'].transform('max')
    #compute max tbreak - just for quality control
    df['max_tbreak_group'] = df.groupby(['x_coord','y_coord'])['tBreak'].transform('max')

    #get only the coordinates and tBreak
    df_sub = df[['x_coord','y_coord','max_tstart_group','max_tend_group','max_tbreak_group']].copy()
    df_sub = df_sub.drop_duplicates()


    return df_sub

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
    Saves the selection of B2 and B11 values to a hdf5 file.

    Args:
        result (np.ndarray) : array with selection of B2 and B11 values.
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


def main():

    #combine small parquets into single dataframe
    combined_df = combine_parquet_files(parquet_folder, tile)

    #convert dates from ms to ordinal
    combined_df['tstart_ordinal'] = combined_df['max_tstart_group'].apply(lambda x: datetime.utcfromtimestamp(x/1000).toordinal())
    combined_df['tend_ordinal'] = combined_df['max_tend_group'].apply(lambda x: datetime.utcfromtimestamp(x/1000).toordinal())

    #get tif dates
    tif_names, tif_dates = read_tif_files_gee(tile, os.path.join(s2_images_folder, tile), max_date)
    tif_dates_ord = [d.toordinal() for d in tif_dates]

    #crate xarray time variable
    time_var = xr.Variable('time',tif_dates_ord)

    # Load in and concatenate all individual GeoTIFFs
    tifs_xr = [rioxarray.open_rasterio(os.path.join(s2_images_folder, tile, i), chunks={'x':-1, 'y':100, 'band':2}) for i in tif_names]
    geotiffs_da = xr.concat(tifs_xr, dim=time_var)

    #get indices to perform the selection of data
    x_inds, y_inds, time_end_inds, time_start_inds = get_indices(combined_df, geotiffs_da)

    #initialize empty array
    result = np.empty((2, len(combined_df), 2), dtype=np.float32)

    #initialize Dask progress bar
    ProgressBar().register()

    #perform value selection based on coordinates and dates
    #step 1.2: get values of tend (can be done directly, since we already have access to the indices)
    print('Collecting values pre break')
    #select values
    selected_values = geotiffs_da.isel(
        x=xr.DataArray(x_inds, dims='z'),
        y=xr.DataArray(y_inds, dims='z'),
        time=xr.DataArray(time_end_inds, dims='z')
    )
    #fill result array
    result[0,:] = selected_values.values

    #step 2: get values of tstart (can be done directly, since we already have access to the indices)
    print('Collecting values post break')
    #select values
    selected_values = geotiffs_da.isel(
        x=xr.DataArray(x_inds, dims='z'),
        y=xr.DataArray(y_inds, dims='z'),
        time=xr.DataArray(time_start_inds, dims='z')
    )
    #fill result array
    result[1,:] = selected_values.values

    
    #save result to hdf5 file
    output_path_with_filename = os.path.join(output_h5_folder, tile, h5_filename)
    save_to_hdf5(result, selected_values, output_path_with_filename)

    
    
if __name__ == '__main__':
    t1 = time.time()
    print("Started execution for tile {}.".format(tile))

    main()

    print("Execution finished - HDF5 file created for tile {}. Execution took {} minutes".format(tile, round((time.time()-t1)/60,2)))