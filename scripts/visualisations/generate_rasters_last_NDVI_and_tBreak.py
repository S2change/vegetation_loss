"""
===============================================================================
Description:
    This script processes .parquet files containing temporal and spectral
    information of pixels (e.g., NDVI time series results), generates GeoTIFF
    rasters and QGIS style files (.qml) for spatial visualization of break dates
    (tBreak) and NDVI values.

Execution flow:
    1. Reads .parquet files from a specified folder.
    2. Processes each pixel, determining:
        - Whether there was a temporal break (is_break)
        - The break date (tBreak_used)
        - The NDVI value of the last valid segment (ndvi_last_segment)
    3. Combines all results into a single DataFrame.
    4. Generates GeoTIFF rasters:
        - One raster with the break date (tBreak_used_yyyymmdd)
        - One raster with the NDVI of the last segment
    5. Creates a .qml style file for the date raster for visualization in QGIS.

Outputs:
    - GeoTIFF and .qml files saved in the folder defined in config["folder_path"]
===============================================================================
P.S: The rasters are located in the shared folder at: partilhado/tBreaks_t29tnf_2018_2024.
===============================================================================
"""
#%%
import os
import glob
import pandas as pd
import numpy as np
import rasterio
from rasterio.transform import from_origin
from datetime import datetime
import matplotlib.pyplot as plt
import colorsys
import time
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
   sys.path.append(module_path)
from ccd_results_utils.segment_identification import ndvi_loss_calculation


config = {
    "folder_path": r"C:\Users\Public\Documents\outputs_ROI\tabular\T29TNF",
    "pixel_size": 10,
    "crs_code": "EPSG:32629",
    "generate_date_raster": True,
    "generate_ndvi_raster": True,
    "testing_timer": False,
}

#%% ---------------------- FUNÇÕES AUXILIARES ----------------------
def calculate_ndvi(input_row):
    ndvi = (input_row["nirEnd"] - input_row["redEnd"]) / (input_row["nirEnd"] + input_row["redEnd"])
    return ndvi

def process_pixel_segments(pixel_df, time_series_end):
    """
    Processes all temporal segments of a given pixel and determines whether a
    break occurred, the date of that break (tBreak_used), and the NDVI value
    of the last valid segment.

    This function assumes that each pixel has one or more temporal segments,
    each defined by:
        - tBreak : datetime
            The break date or the end of the previous segment.
        - tEnd : datetime
            The end date of the current segment.
        - nirEnd, redEnd : float
            NIR and Red spectral values used to compute NDVI.

    Decision logic
    ---------------
    1. **No segments (n_segments == 0):**
       - No valid temporal information.
       - Returns: `is_break = 0`, `tBreak_used = NaT`, `ndvi_last_segment = NaN`.

    2. **Single segment (n_segments == 1):**
       - If `tBreak` or `tEnd` is missing, or both are equal:
         → No valid break → `is_break = 0`, `tBreak_used = NaT`, `ndvi_last_segment = NaN`.
       - Otherwise:
         → A single valid segment with a break → `is_break = 1`,
           `tBreak_used = tEnd`, and NDVI calculated as:
           `(nirEnd - redEnd) / (nirEnd + redEnd)`.

    3. **Two or more segments (n_segments >= 2):**
       - If the last segment has `tBreak != tEnd`:
         → Use the **last segment** as valid.
         → `tBreak_used = tEnd` of the last segment.
       - If the last segment has `tBreak == tEnd`:
         → Use the **second-to-last segment** instead:
           `tBreak_used = tEnd` of the second-to-last segment.
       - In both cases, NDVI is calculated as:
         `(nirEnd - redEnd) / (nirEnd + redEnd)` from the chosen segment.

    Parameters
    ----------
    pixel_df : pd.DataFrame
        Pixel data containing multiple temporal segments with columns
        ['tBreak', 'tEnd', 'nirEnd', 'redEnd'].
    time_series_end : datetime
        End date of the time series (currently not used directly).

    Returns
    -------
    pd.Series
        Contains:
        - is_break (int): 1 if a valid break was detected, 0 otherwise.
        - tBreak_used (datetime): date of the selected break.
        - ndvi_last_segment (float): NDVI value of the last valid segment.
    """
    g = pixel_df.sort_values("tEnd").reset_index(drop=True)
    n_segments = len(g)

    # No segments
    if n_segments == 0:
        return pd.Series({"is_break": 0, "tBreak_used": pd.NaT, "ndvi_last_segment": np.nan})
    
    # Last segment has tBreak != tEnd
    last_row = g.iloc[-1]
    last_tBreak, last_tEnd = last_row["tBreak"], last_row["tEnd"]

    if pd.notna(last_tBreak) and pd.notna(last_tEnd) and last_tBreak != last_tEnd:
        ndvi = calculate_ndvi(last_row)
        return pd.Series({"is_break": -1, "tBreak_used": last_tEnd, "ndvi_last_segment": ndvi})

    # Only 1 segment, and no value for tBreak or tEnd or tBreak == tEnd (because tBreak != tEnd already filtered out)
    if n_segments == 1:
        return pd.Series({"is_break": 0, "tBreak_used": pd.NaT, "ndvi_last_segment": np.nan})

    # 2 or more segments
    # loop through remaining segments until finding a segment where break has NDVI decrease, or return no breaks values
    current_row = g.iloc[-2]
    offset = 2
    while True:
        ndvi_check = ndvi_loss_calculation(current_row, last_row)
        if ndvi_check == 1:
            ndvi = calculate_ndvi(current_row)
            return pd.Series({"is_break": 1, "tBreak_used": current_row["tEnd"], "ndvi_last_segment": ndvi})
        else:
            last_row = g.iloc[-offset]
            try:
                current_row = g.iloc[-(offset + 1)]
            except IndexError:
                # if IndexError, means all of the breaks had an increase in NDVI at the break
                return pd.Series({"is_break": 0, "tBreak_used": pd.NaT, "ndvi_last_segment": np.nan})
            offset += 1


def create_qgis_style_file_from_dataframe(df, value_col, output_style_file):
    """
    Generates a QGIS style file (.qml) with a color palette based on dates
    present in a date raster (tBreak raster).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing date values in YYYYMMDD format.
    value_col : str
        Name of the column containing date values.
    output_style_file : str
        Output path for the .qml file.
    """
    valid_values = df[value_col][(df[value_col] > 0) & (pd.notna(df[value_col]))]
    dates = []
    for v in valid_values:
        if isinstance(v, int):
            try:
                dates.append(datetime.strptime(str(v), '%Y%m%d'))
            except ValueError:
                continue
        else:
            dates.append(v)

    dates_by_year = {}
    for date in dates:
        year = date.year
        date_value = int(date.strftime('%Y%m%d'))
        dates_by_year.setdefault(year, []).append(date_value)

    years = sorted(dates_by_year.keys())
    cmap = plt.get_cmap('tab20', len(years))

    qml_content = '''<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis version="3.22.0" minScale="0" maxScale="1e+08" styleCategories="AllStyleCategories">
  <pipe>
    <rasterrenderer opacity="1" type="paletted" band="1">
      <rasterTransparency/>
      <colorPalette>
'''
    for i, year in enumerate(years):
        base_rgb = cmap(i)[:3]
        h, s, v = colorsys.rgb_to_hsv(*base_rgb)
        for date_value in sorted(set(dates_by_year[year])):
            date_obj = datetime.strptime(str(date_value), '%Y%m%d')
            day_of_year = date_obj.timetuple().tm_yday
            days_in_year = 366 if date_obj.year % 4 == 0 and (date_obj.year % 100 != 0 or date_obj.year % 400 == 0) else 365
            position = (day_of_year - 1) / (days_in_year - 1)
            new_v = 0.9 - (position * 0.4)
            new_s = s * (0.5 + position * 0.5)
            new_rgb = colorsys.hsv_to_rgb(h, new_s, new_v)
            rgb = [int(c * 255) for c in new_rgb]
            color_hex = '#{:02x}{:02x}{:02x}'.format(*rgb)
            label = date_obj.strftime('%Y-%m-%d')
            qml_content += f'        <paletteEntry value="{date_value}" color="{color_hex}" label="{label}"/>\n'

    qml_content += '''        <paletteEntry value="0" color="#808080" label="Filtered Out"/>
        <paletteEntry value="-9999" color="#000000" label="No Data" alpha="0"/>
      </colorPalette>
    </rasterrenderer>
  </pipe>
</qgis>'''

    with open(output_style_file, 'w') as f:
        f.write(qml_content)
    print(f"[INFO] QGIS style file saved to: {output_style_file}")


def generate_raster(df, value_col, folder_path, pixel_size, crs_code, nodata_value=np.nan):
    """
    Generates a GeoTIFF raster from X/Y coordinates and a column of values.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing 'x_coord', 'y_coord' and 'value_col'.
    value_col : str
        Name of the column with the values to rasterize.
    folder_path : str
        Folder where the GeoTIFF will be saved.
    pixel_size : float
        Raster pixel size (in meters).
    crs_code : str
        EPSG code of the coordinate system.
    nodata_value : float
        Value assigned to pixels with no data (default: np.nan).

    Returns
    -------
    str : Path to the generated GeoTIFF file.
    """
    x_min_raw, x_max_raw = df["x_coord"].min(), df["x_coord"].max()
    y_min_raw, y_max_raw = df["y_coord"].min(), df["y_coord"].max()

    x_min = np.floor(x_min_raw / pixel_size) * pixel_size
    x_max = np.ceil(x_max_raw / pixel_size) * pixel_size
    y_min = np.floor(y_min_raw / pixel_size) * pixel_size
    y_max = np.ceil(y_max_raw / pixel_size) * pixel_size

    x_coords = np.arange(x_min, x_max, pixel_size)
    y_coords = np.arange(y_max, y_min, -pixel_size)
    n_cols, n_rows = len(x_coords), len(y_coords)

    print(f"[INFO] Raster grid: {n_cols}x{n_rows}")

    if pd.api.types.is_integer_dtype(df[value_col]):
        raster_array = np.zeros((n_rows, n_cols), dtype=np.uint32)
    else:
        raster_array = np.full((n_rows, n_cols), np.nan, dtype=np.float32)

    for _, row in df.iterrows():
        x_val, y_val, val = row["x_coord"], row["y_coord"], row[value_col]
        if np.isnan(x_val) or np.isnan(y_val) or pd.isna(val):
            continue
        xi = int((x_val - x_min) / pixel_size)
        yi = int((y_max - y_val) / pixel_size)
        if 0 <= xi < n_cols and 0 <= yi < n_rows:
            raster_array[yi, xi] = val

    transform = from_origin(x_min, y_max, pixel_size, pixel_size)
    output_file = os.path.join(folder_path, f"{value_col}_all_parquets.tif")

    with rasterio.open(
        output_file,
        "w",
        driver="GTiff",
        height=n_rows,
        width=n_cols,
        count=1,
        dtype=raster_array.dtype,
        crs=crs_code,
        transform=transform,
        nodata=nodata_value
    ) as dst:
        dst.write(raster_array, 1)

    print(f"[INFO] Raster saved at: {output_file}")
    return output_file


#%% ---------------------- FUNÇÕES PRINCIPAIS ----------------------
def process_all_parquets_optimized(config):
    """
    Optimized version that processes parquet files in reverse order to avoid groupby.

    Since segments are ordered oldest to newest in the parquet files, iterating in reverse
    means we encounter the most recent segment first for each pixel. We can then:
    1. Collect only the segments needed to determine break status
    2. Skip all remaining older segments for that pixel

    Returns
    -------
    pd.DataFrame : DataFrame with one row per unique pixel
    """
    parquet_files = glob.glob(os.path.join(config["folder_path"], "*.parquet"))
    all_results = []

    print(f"[INFO] {len(parquet_files)} files found.")

    for idx, file in enumerate(parquet_files, 1):
        print(f"[INFO] Processing file [{idx}/{len(parquet_files)}]: {file}")
        df = pd.read_parquet(file)
        df["tBreak"] = pd.to_datetime(df["tBreak"], unit="ms", errors="coerce")
        df["tEnd"] = pd.to_datetime(df["tEnd"], unit="ms", errors="coerce")

        time_series_end = df["tEnd"].max()

        # Track which pixels we've fully processed
        processed_pixels = set()
        # Store segments we're currently collecting for a pixel
        current_pixel = None
        current_segments = []
        results = []

        # Iterate in reverse (newest to oldest)
        for i in range(len(df) - 1, -1, -1):
            row = df.iloc[i]
            x, y = row["x_coord"], row["y_coord"]
            pixel_key = (x, y)

            # Skip if we've already fully processed this pixel
            if pixel_key in processed_pixels:
                continue

            # If we've moved to a different pixel, process the previous one
            if current_pixel is not None and pixel_key != current_pixel:
                # appended results are the same no matter number of segments
                res = pd.Series({"is_break": 0, "tBreak_used": pd.NaT, "ndvi_last_segment": np.nan})
                results.append((current_pixel[0], current_pixel[1], res["is_break"],
                            res["tBreak_used"], res["ndvi_last_segment"]))
                processed_pixels.add(current_pixel)

                # Start collecting for new pixel
                current_pixel = pixel_key
                current_segments = [row]
            elif current_pixel is None:
                # First pixel encountered
                current_pixel = pixel_key
                current_segments = [row]
            else:
                # Same pixel, add segment
                current_segments.append(row)

            # Check if we can determine the result early
            if len(current_segments) == 1:
                last_seg = current_segments[0]
                last_tBreak = last_seg["tBreak"]
                last_tEnd = last_seg["tEnd"]

                # If last segment has tBreak != tEnd, don't need to process more segments
                if pd.notna(last_tBreak) and pd.notna(last_tEnd) and last_tBreak != last_tEnd:
                    ndvi = calculate_ndvi(last_seg)
                    res = pd.Series({"is_break": -1, "tBreak_used": last_tEnd, "ndvi_last_segment": ndvi})
                    results.append((pixel_key[0], pixel_key[1], res["is_break"],
                                res["tBreak_used"], res["ndvi_last_segment"]))
                    processed_pixels.add(pixel_key)
                    current_pixel = None
                    current_segments = []
                    continue

            # We need at least 2 segments to check NDVI change
            if len(current_segments) >= 2:
                active_segment = current_segments[-1]
                newer_segment = current_segments[-2]

                ndvi_check = ndvi_loss_calculation(active_segment, newer_segment)
                if ndvi_check == 1:
                    # Valid break
                    ndvi = calculate_ndvi(active_segment)
                    res = pd.Series({"is_break": 1, "tBreak_used": active_segment["tEnd"], "ndvi_last_segment": ndvi})
                    results.append((pixel_key[0], pixel_key[1], res["is_break"],
                                    res["tBreak_used"], res["ndvi_last_segment"]))
                    processed_pixels.add(pixel_key)
                    current_pixel = None
                    current_segments = []

        # Processing last pixel
        if current_pixel is not None:
            pixel_df = pd.DataFrame(current_segments[::-1])
            res = process_pixel_segments(pixel_df, time_series_end)
            results.append((current_pixel[0], current_pixel[1], res["is_break"],
                          res["tBreak_used"], res["ndvi_last_segment"]))

        result_df = pd.DataFrame(results, columns=["x_coord", "y_coord", "is_break",
                                                     "tBreak_used", "ndvi_last_segment"])
        all_results.append(result_df)

    df_all = pd.concat(all_results, ignore_index=True)
    df_all["tBreak_used_yyyymmdd"] = (
        df_all["tBreak_used"].dt.strftime("%Y%m%d").fillna("0").astype(int)
    )
    return df_all


def process_all_parquets(config):
    """
    Reads and processes all .parquet files in the specified folder.

    Returns
    -------
    pd.DataFrame : Combined DataFrame containing all results.
    """
    parquet_files = glob.glob(os.path.join(config["folder_path"], "*.parquet"))
    all_results = []

    print(f"[INFO] {len(parquet_files)} files found.")

    for idx, file in enumerate(parquet_files, 1):
        print(f"[INFO] Processing file [{idx}/{len(parquet_files)}]: {file}")
        df = pd.read_parquet(file)
        df["tBreak"] = pd.to_datetime(df["tBreak"], unit="ms", errors="coerce")
        df["tEnd"] = pd.to_datetime(df["tEnd"], unit="ms", errors="coerce")

        time_series_end = df["tEnd"].max()

        grouped = df.groupby(["x_coord", "y_coord"], sort=False)
        records = []
        for (x, y), g in grouped:
            res = process_pixel_segments(g, time_series_end)
            records.append((x, y, res["is_break"], res["tBreak_used"], res["ndvi_last_segment"]))

        result = pd.DataFrame(records, columns=["x_coord", "y_coord", "is_break", "tBreak_used", "ndvi_last_segment"])
        df_result = df.merge(result, on=["x_coord", "y_coord"], how="left")
        all_results.append(df_result)

    df_all = pd.concat(all_results, ignore_index=True)
    df_all["tBreak_used_yyyymmdd"] = (
        df_all["tBreak_used"].dt.strftime("%Y%m%d").fillna("0").astype(int)
    )
    return df_all


def main(config, use_optimized=True):
    """
    Main function to process parquet files and generate rasters.

    Parameters
    ----------
    config : dict
        Configuration dictionary with processing parameters.
    use_optimized : bool, optional
        If True, uses the reverse-iteration approach (default).
        If False, uses the original groupby approach.
    """
    if config["testing_timer"]:
        start_time = time.time()
        print(f"[INFO] Starting processing at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if use_optimized:
        df_all = process_all_parquets_optimized(config)
    else:
        df_all = process_all_parquets(config)

    if config["generate_date_raster"]:
        raster_path = generate_raster(
            df_all, "tBreak_used_yyyymmdd",
            config["folder_path"], config["pixel_size"], config["crs_code"], nodata_value=0
        )
        qml_path = raster_path.replace(".tif", ".qml")
        create_qgis_style_file_from_dataframe(df_all, "tBreak_used_yyyymmdd", qml_path)

    if config["generate_ndvi_raster"]:
        generate_raster(
            df_all, "ndvi_last_segment",
            config["folder_path"], config["pixel_size"], config["crs_code"], nodata_value=np.nan
        )

    if config["testing_timer"]:
        end_time = time.time()
        elapsed_time = end_time - start_time

        hours = int(elapsed_time // 3600)
        minutes = int((elapsed_time % 3600) // 60)
        seconds = elapsed_time % 60

        print("[INFO] Processing successfully completed.")
        print(f"[INFO] Total execution time: {hours:02d}:{minutes:02d}:{seconds:05.2f} (HH:MM:SS)")
#%%
if __name__ == "__main__":
    main(config)

