import pandas as pd
import geopandas as gpd
import os
import glob
from pathlib import Path
import rasterio
from rasterio.transform import from_origin
from rasterio.warp import calculate_default_transform, reproject, Resampling
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import colorsys
import time


def read_parquet_identify_breaks(filepath):
    """
    Separate segments that corresponds to breaks from terminal segments (not breaks)
    Assumption: all segments in the parquet file for the same pixel are in sequence, from the earlier segment to the last one (the terminal one)

    Inputs: 
    * path to parquet file (output of a pyccd task); each row is a ccd segment; the file has columns 'x_coord' and 'y_coord'

    Output dataframe with additional column ['is_break'] ; each row is still a segment
    
    Note: 'is_break' is 0 for a terminal segment (not a break) and 'is_break' is 1 otherwise 
    """
    df = pd.read_parquet(filepath)

    # create mask and new binary column 'is_break'
    # Using .values avoids pandas index alignment overhead
    mask = (df['x_coord'].values == df['x_coord'].shift(-1).values) & (df['y_coord'].values == df['y_coord'].shift(-1).values)
    df['is_break'] = mask.astype(int)

    # special case: pixel with only 1 segment and tBreak != tEnd (sara 6 out 2025)
    single_segment = df.groupby(['x_coord', 'y_coord']).filter(lambda g: len(g) == 1)
    condition = single_segment['tBreak'] != single_segment['tEnd']
    df.loc[condition.index[condition], 'is_break'] = 1

    return df

def filter_pixel_group(group, search_start_ms, search_end_ms):
    """
    Filter a group of rows for a single pixel according to the rules:
    - Only return the row with the highest tBreak value
    - Only consider rows within the date range if specified
    - Returns a tuple: (result_row, was_filtered_out)
      where was_filtered_out is True if pixel had data but was filtered out by date
    
    Uses the global search_start_ms and search_end_ms variables
    """
    # Get the most recent break regardless of filtering
    most_recent_row = group.loc[group['tBreak'].idxmax()]

    # Filter by date range if specified
    if search_start_ms is not None or search_end_ms is not None:
        filtered_group = group.copy()

        if search_start_ms is not None:
            filtered_group = filtered_group[filtered_group['tBreak'] >= search_start_ms]

        if search_end_ms is not None:
            filtered_group = filtered_group[filtered_group['tBreak'] <= search_end_ms]

        # If no rows remain after filtering, return the most recent row but marked as filtered out
        if len(filtered_group) == 0:
            return (most_recent_row, True)

        group = filtered_group

    return (group.loc[group['tBreak'].idxmax()], False)

def ndvi_calculation(nir1, nir2, red1, red2):
    """
    Calculate NDVI from two NIR and two Red values.

    Inputs:
    * nir1, nir2: Two NIR band values
    * red1, red2: Two Red band values

    Returns:
    * NDVI value calculated from the average NIR and Red values
    """
    avg_nir = (nir1 + nir2) / 2
    avg_red = (red1 + red2) / 2

    ndvi = (avg_nir - avg_red) / (avg_nir + avg_red)

    return ndvi

def ndvi_loss_calculation(row, df, min_difference=None):
    """
    Calculate whether NDVI loss occurred between current row and next row.

    Inputs:
    * row: Current row from dataframe
    * df: The full dataframe
    * min_difference: Minimum amount of difference needed to say there was a change

    Returns:
    * 1 if next row has same coordinates AND current NDVI > next NDVI (vegetation loss)
    * -1 if no matching next row or NDVI increased/stayed same
    * 0 if difference between NDVI's is not greater than min_difference (if it was specified)
    """
    current_idx = row.name

    # Check if there is a next row with same coordinates
    if current_idx + 1 >= len(df):
        return -1

    next_row = df.iloc[current_idx + 1]

    if row['x_coord'] != next_row['x_coord'] or row['y_coord'] != next_row['y_coord']:
        return -1

    # NDVI calculations
    current_ndvi = ndvi_calculation(row['nirEnd'], row['nirEnd2'], row['redEnd'], row['redEnd2'])
    next_ndvi = ndvi_calculation(next_row['nirStart'], next_row['nirStart2'], next_row['redStart'], next_row['redStart2'])

    if min_difference is not None:
        ndvi_difference = current_ndvi - next_ndvi
        if abs(ndvi_difference) < min_difference:
            return 0

    if current_ndvi > next_ndvi:
        return 1
    else:
        return -1
    
def combine_parquet_files(parquet_folder, tile=None):
    """
    Combines all Parquet files in a directory into a single dataframe.

    Args:
        parquet_folder (str) : Path to the directory containing Parquet files (root).
        tile (str) : tile name to access the correct folder.

    
    Returns a merged dataframe.

    """
    if tile is not None:
        input_dir = os.path.join(parquet_folder, tile)
    else:
        input_dir = parquet_folder

    # List all .parquet files in the directory
    parquet_files = [f for f in os.listdir(input_dir) if f.endswith('.parquet')]

    # Read and concatenate all Parquet files
    dataframes = []
    for file in parquet_files:
        file_path = os.path.join(input_dir, file)
        df = pd.read_parquet(file_path)
        dataframes.append(df)

    # Combine all dataframes
    combined_df = pd.concat(dataframes, ignore_index=True)

    return combined_df