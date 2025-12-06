import pandas as pd
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from datetime import timedelta

def generate_date_ranges(ranges, auto_intervals=False, months=1):
    """
    Generate date ranges based on a list of input intervals.

    Parameters
    ----------
    ranges : list of tuple(str, str)
        A list of (start_date, end_date) tuples, where each date is
        provided as a string in the format "YYYY-MM-DD".
    auto_intervals : bool, optional
        If True, each input interval will be automatically split into
        sub-intervals of the specified number of months. If False, the
        original intervals are returned unchanged. Default is False.
    months : int, optional
        Size of each automatically generated sub-interval, expressed in
        months. Only used when auto_intervals=True. Default is 1.
    """
    result = []

    for start_str, end_str in ranges:
        start = datetime.strptime(start_str, "%Y-%m-%d").date()
        end = datetime.strptime(end_str, "%Y-%m-%d").date()

        if auto_intervals:
            current_start = start
            while current_start <= end:
                current_end = current_start + relativedelta(months=months) - relativedelta(days=1)
                if current_end > end:
                    current_end = end

                result.append((
                    current_start.strftime("%Y-%m-%d"),
                    current_end.strftime("%Y-%m-%d")
                ))

                current_start = current_end + relativedelta(days=1)

        else:
            result.append((start_str, end_str))

    return result

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

def ndvi_avg_calculation(nir1, nir2, red1, red2):
    """
    Calculate NDVI from two NIR and two Red values.

    Inputs:
    * nir1, nir2: Two NIR band values
    * red1, red2: Two Red band values

    Returns:
    * NDVI value calculated from the average NIR and Red values
    """

    # Don't average if one set has 0's
    if (nir1 == 0 or red1 == 0) and (nir2 != 0 and red2 != 0):
        nir_value = nir2
        red_value = red2
    elif (nir2 == 0 or red2 == 0) and (nir1 != 0 and red1 != 0):
        nir_value = nir1
        red_value = red1
    else:
        nir_value = (nir1 + nir2) / 2
        red_value = (red1 + red2) / 2

    ndvi = (nir_value - red_value) / (nir_value + red_value)

    return ndvi

def ndvi_loss_calculation(current_row, next_row, min_difference=None):
    """
    Calculate whether NDVI loss occurred between current row and next row.

    Inputs:
    * current_row: Current row from dataframe
    * next_row: Row immediately following the current row
    * min_difference: Minimum amount of difference needed to say there was a change

    Returns:
    * 1 if next row has same coordinates AND current NDVI > next NDVI (vegetation loss)
    * -1 if NDVI increased/stayed same
    * 0 if difference between NDVI's is not greater than min_difference (if it was specified)
    """

    # NDVI calculations
    current_ndvi = ndvi_avg_calculation(current_row['nirEnd'], current_row['nirEnd2'], current_row['redEnd'], current_row['redEnd2'])
    next_ndvi = ndvi_avg_calculation(next_row['nirStart'], next_row['nirStart2'], next_row['redStart'], next_row['redStart2'])

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
