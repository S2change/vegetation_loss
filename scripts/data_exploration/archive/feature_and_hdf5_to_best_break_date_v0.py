import h5py
import numpy as np
import pandas as pd
import rasterio
from scipy.spatial import KDTree
from datetime import date,datetime, timedelta
import os
import geopandas as gpd
from rasterio.transform import from_origin
from rasterio.features import rasterize
from scipy import stats
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import psutil  # You may need to: pip install psutil
import gc

'''
Query:
(1st input) I have a hdf5 filefor the same area of interest with the structure below where xs and ys are coordinates in CRS EPSG:32629, and 'ts' are ordinal dates.
(2nd input) I have a raster with 3 bands: band1 is date 'before' in format yyymmdd; band2 is date 'after' in the same format, and band3 is either1 or 0.
(3rd input) Delta=number of days, say Delta=45
For all pixels where band3=1, I want to extract the sprectral values for the 6bands from the hdf5 file for all dates in the dhdf5 file from  'before'-Delta until  'after'+Delta
(output) The output can be a dataframe with the pixel coordinates (xs,ys) , the ordinal dates (ts), and the spectral values for all 6 bands (from 'values')
'''

# --- CONFIGURATION ---
GPKG_PATH = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\harmonized\BDR_expanded_v0.gpkg"
H5_PATH = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\T29TNE_6bands_20180630_20211231.h5"
RASTER_FOLDER = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\harmonized_to_tifs"
RASTER_FOLDER_BEST_DATE=r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\tifs_at_best_break_date"
VISUAL_CHIPS_FOLDER=r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\visual"
DELTA = 45 # Days to expand date range on either side of the break date (can be adjusted as needed)
TARGET_CRS = "EPSG:32629"
NUMBER_BANDS_HDF5=6
NIR='band_3' # NIR is band 3 in the HDF5 file
RED='band_2' # RED is band 2 in the HDF5 file
#FID_TO_PROCESS = 309 #87 #309 # Id of the feature to process (adjust as needed)
small_D = 100  # Extent size in meters (100m x 100m) - adjust as needed
big_D=3000 
#prefix = f"BDRexp_{FID_TO_PROCESS}"
RES=10 # Raster resolution in meters (adjust as needed)
WINDOW_SIZE = 16 # Number of dates in the sliding window for break detection (adjust as needed)
NODATA_VAL = 65535 # The value used in the hdf5 file to indicate NoData (adjust if different)
T_TEST_THRESHOLD=0.05
#----------------------

def main():
    
    for var in [GPKG_PATH, H5_PATH]:
        if not os.path.exists(var):
            print(f"Error: File {var} does not exist. Please check the path and try again.")
            return  
        
    for folder in [RASTER_FOLDER, RASTER_FOLDER_BEST_DATE]:
        if not os.path.exists(folder):
            os.makedirs(folder)
    
    # 1. Load the GeoPackage
    gdf = gpd.read_file(GPKG_PATH)
    raster_list = []
    fids_processed = []
    
    print(f"Total features in GPKG: {len(gdf)}")
    # 2. Loop through features and filter out NULL dates
    for index, feat in gdf.iterrows():
        # Check if Date0 and Date1 are both present
        if pd.notnull(feat['Data0']) and pd.notnull(feat['Data1']):
            fid = feat['Id']
            prefix = f"BDRexp_{fid}"
            # Generate the 3 band raster raster for this valid feature
            path = process_feature_to_raster(gpkg_path=GPKG_PATH, target_fid=fid, output_dir=RASTER_FOLDER, prefix=prefix, 
                                            extent_size=big_D,res=RES, target_crs=TARGET_CRS, break_date_yyyyddmm=None)
            if path:
                raster_list.append(path)
                fids_processed.append(fid)
        else:
            # Optional: Log which FIDs were skipped
            print(f"Skipping FID {feat['Id']}: One or both dates are NULL.")

    if not raster_list:
        print("No valid rasters created. Check your input data for NULL dates.")
        return

    # After creating the raster, we can proceed to extract spectral data from the HDF5 file
    # 1. Extract spectral data
    # start/end dates are in band 1 and 2 of the rasters
    # Delta is the interval of days before and after dates
    #list_of_dfs = extract_spectral_data_batch(H5_PATH, raster_paths=raster_list, delta=DELTA)
    data_stream = extract_spectral_data_generator(H5_PATH, raster_list, DELTA)
    
    for i, df in enumerate(data_stream):
        if df is not None:
            current_fid = fids_processed[i]
            '''
            # 3. Iterate through the results to process each feature
            for i, df in enumerate(list_of_dfs):
                if df is not None:
            '''
            # 2. Calculate NDVI time series
            ordinal_dates, ndvi_values = calculate_ndvi_and_changes(df, RED, NIR, nodata_val=NODATA_VAL)
            
            # 3. Detect the break using the new logic
            # Returns a datetime.date object or None
            break_date = detect_breaks_welch(ordinal_dates, ndvi_values, window_size=WINDOW_SIZE, p_threshold=T_TEST_THRESHOLD)
            # --- NEW: Call the plotting function ---
            # plot_ndvi_time_series(ordinal_dates, ndvi_values, break_date, WINDOW_SIZE, prefix)

            if break_date:
                print(f"!!! Break Detected: Most significant drop around {break_date} !!!")

                # format break_date, like '2020-07-13',  as YYYYMMDD
                break_date_yyyyddmm = int(break_date.strftime('%Y%m%d'))
                prefix = f"BDRexp_{fids_processed[i]}"
                # Create 3-band raster with date break (optional)
                raster_path = process_feature_to_raster(gpkg_path=GPKG_PATH, target_fid=current_fid, output_dir=RASTER_FOLDER_BEST_DATE, prefix=prefix, 
                                                        extent_size=big_D,res=RES, target_crs=TARGET_CRS,break_date_yyyyddmm=break_date_yyyyddmm)
                chip_path = process_feature_to_visual_chips(gpkg_path=GPKG_PATH, target_fid=current_fid, output_dir=VISUAL_CHIPS_FOLDER, prefix=prefix, 
                                                        extent_size=big_D,res=RES, target_crs=TARGET_CRS,break_date_yyyyddmm=break_date_yyyyddmm,df=df)
            else:
                print(f"No significant drop detected (all p-values > 0.05 or insufficient data).")
        
        # --- MANUALLY FREE MEMORY ---
            del df 
            gc.collect() 
        else:
            print(f"Skipping feature {i} due to lack of data.")

def plot_ndvi_time_series(ordinal_dates, ndvi_values, break_date, window_size, prefix):
    """
    Plots the NDVI time series and highlights the detected break.
    """
    # 1. Convert ordinals to datetime objects for plotting
    dates = [datetime.fromordinal(int(d)) for d in ordinal_dates]
    
    plt.figure(figsize=(12, 6))
    plt.plot(dates, ndvi_values, marker='o', linestyle='-', color='#2ecc71', label='Mean NDVI', markersize=4)

    if break_date:
        # Convert break_date (date object) to datetime for the vlines function
        break_dt = datetime.combine(break_date, datetime.min.time())
        
        # 2. Highlight the detected break
        plt.axvline(x=break_dt, color='#e74c3c', linestyle='--', linewidth=2, label=f'Break Detected ({break_date})')
        
        # 3. Optional: Highlight the window around the break
        # Find the index of the break date to show the window used
        try:
            break_ordinal = break_date.toordinal()
            idx = ordinal_dates.index(break_ordinal)
            half = window_size // 2
            
            # Start and end dates of the specific window that triggered the break
            win_start = dates[max(0, idx - half + 1)]
            win_end = dates[min(len(dates)-1, idx + half)]
            
            plt.axvspan(win_start, win_end, color='gray', alpha=0.2, label='Detection Window')
        except (ValueError, IndexError):
            pass

    # Formatting
    plt.title(f"NDVI Time Series Analysis - Feature {prefix}", fontsize=14)
    plt.xlabel("Date", fontsize=12)
    plt.ylabel("Mean NDVI", fontsize=12)
    plt.grid(True, which='both', linestyle='--', alpha=0.5)
    plt.legend()
    
    # Date formatting on X-axis
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gcf().autofmt_xdate() # Rotate dates
    
    plt.tight_layout()
    
    # Save the plot
    plot_path = os.path.join(RASTER_FOLDER, f"{prefix}_ndvi_plot.png")
    plt.savefig(plot_path)
    print(f"Plot saved to: {plot_path}")
    plt.show()

def calculate_ndvi_and_changes(df, red_band, nir_band, nodata_val):
    """
    Calculates NDVI for each pixel and timestamp, skipping pixels that match
    the NODATA value in either the Red or NIR bands.
    """
    print(f"Calculating NDVI and aggregating time series (skipping NODATA: {nodata_val})...")

    # 1. Create a mask of valid pixels (where neither band is the NoData value)
    # This prevents 65535 from being treated as a real spectral value
    valid_mask = (df[red_band] != nodata_val) & (df[nir_band] != nodata_val)
    
    # Create a copy of the valid data to avoid SettingWithCopyWarnings
    df_valid = df[valid_mask].copy()

    if df_valid.empty:
        print("Warning: All extracted pixels are NODATA. Check your HDF5 coverage.")
        return [], []

    # 2. Convert to float and calculate NDVI
    red = df_valid[red_band].astype(float)
    nir = df_valid[nir_band].astype(float)
    
    # NDVI Formula: (NIR - RED) / (NIR + RED)
    df_valid['ndvi'] = (nir - red) / (nir + red + 1e-6)

    # 3. Aggregate by timestamp (ts) to get the spatial mean NDVI
    # Grouping by 'ts' and calculating mean only on the valid pixels
    time_series = df_valid.groupby('ts')['ndvi'].mean().sort_index()

    # 4. Extract lists
    ordinal_dates = time_series.index.tolist()
    ndvi_values = time_series.values.tolist()

    return ordinal_dates, ndvi_values

def detect_breaks_welch(ordinal_dates, y, window_size, p_threshold=T_TEST_THRESHOLD):
    """
    Applies a sliding window Welch's t-test and returns the date of the 
    most significant drop if it passes the p-value threshold.
    
    Returns:
        datetime.date or None
    """
    y = np.array(y)
    half = window_size // 2
    results = [] # Store tuple of (p_value, ordinal_date)

    # We need at least 'window_size' elements to run the test
    if len(y) < window_size:
        return None

    for i in range(window_size, len(y) + 1):
        window = y[i - window_size : i]
        
        before = window[:half]
        after = window[half:]
        
        # Welch's t-test: H1: mean(before) > mean(after)
        t_stat, p_val = stats.ttest_ind(before, after, equal_var=False, alternative='greater')
        
        # The center of the window is the likely "break point"
        center_date = ordinal_dates[i - half - 1] 
        results.append((p_val, center_date))
        
    if not results:
        return None

    # Find the result with the SMALLEST p-value (most significant)
    best_p, best_ordinal = min(results, key=lambda x: x[0])

    # Return the date only if it is more significant than the threshold
    if best_p <= p_threshold:
        return date.fromordinal(int(best_ordinal))
    
    return None

def process_feature_to_raster(gpkg_path, target_fid, output_dir, prefix, extent_size, res,target_crs,break_date_yyyyddmm):
    """
    Reads a specific feature, reprojects to EPSG:32629, and generates a raster selecting ONLY pixels totally contained within the feature.

    - break_date_yyyyddmm is the estimated date at the end of 'before' for the most significant drop of mean NDVI
    - if break_date_yyyyddmm is not None, it is the date to be used for band 1 and band 2

    the raster is centered in the feature; extent_size is small_d, big_D
    """

    # 1. Load Data
    gdf = gpd.read_file(gpkg_path)
    
    # Select feature by Id
    selected_feat = gdf[gdf['Id'] == target_fid]
    if selected_feat.empty:
        print(f"Error: Id {target_fid} not found.")
        return None

    # --- REPROJECTION ---
    selected_feat = selected_feat.to_crs(target_crs)
    feature = selected_feat.iloc[0]
    geom = feature.geometry

    # --- INTERIOR PIXEL LOGIC ---
    # We apply a negative buffer (res/2) to ensure the 
    # geometry is strictly 'inside' and set all_touched=False.
    clean_geom = geom.buffer(-res/2) # Buffer by half the resolution to ensure we only get pixels fully inside
    if clean_geom.is_empty:
        # Fallback if the feature is smaller than a pixel
        clean_geom = geom

    # 2. Date Formatting
    if break_date_yyyyddmm:
        d0_int = int(break_date_yyyyddmm)
        d1_int = int(break_date_yyyyddmm)
    else:
        d0 = pd.to_datetime(feature['Data0'])
        d1 = pd.to_datetime(feature['Data1'])
        d0_int = int(d0.strftime('%Y%m%d'))
        d1_int = int(d1.strftime('%Y%m%d'))

    # 3. Calculate Spatial Extent
    centroid = geom.centroid
    left = int(centroid.x - (extent_size / 2))
    right = int(centroid.x + (extent_size / 2))
    bottom = int(centroid.y - (extent_size / 2))
    top = int(centroid.y + (extent_size / 2))

    # 4. Raster Setup
    width = int((right - left) / res)
    height = int((top - bottom) / res)
    transform = from_origin(left, top, res, res)
    
    out_name = f"{prefix}_{d0_int}_{left}_{right}_{bottom}_{top}.tif"
    out_path = os.path.join(output_dir, out_name)

    # 5. Generate Band Arrays
    # IMPORTANT: all_touched=False ensures only pixels with centers 
    # inside the polygon are selected.(already guaranteed by res/2 above)
    mask = rasterize(
        [(clean_geom, 1)],
        out_shape=(height, width),
        transform=transform,
        fill=0,
        all_touched=False, 
        dtype='uint32'
    )

    band1 = np.where(mask == 1, d0_int, 65535).astype('uint32')
    band2 = np.where(mask == 1, d1_int, 65535).astype('uint32')

    # 6. Write GeoTIFF
    profile = {
        'driver': 'GTiff',
        'height': height,
        'width': width,
        'count': 3,
        'dtype': 'uint32',
        'crs': TARGET_CRS,
        'transform': transform,
        'nodata': 65535,
        'compress': 'lzw'
    }

    # burn raster
    with rasterio.open(out_path, 'w', **profile) as dst:
        dst.write(band1, 1) 
        dst.write(band2, 2) 
        dst.write(mask, 3)   

    return out_path
    
def process_feature_to_visual_chips(gpkg_path, target_fid, output_dir, prefix, extent_size, res,target_crs,break_date_yyyyddmm,df):
    """
    Reads a specific feature, reprojects to EPSG:32629, and generates a raster selecting ONLY pixels totally contained within the feature.

    - break_date_yyyyddmm is the estimated date at the end of 'before' for the most significant drop of mean NDVI
    - if break_date_yyyyddmm is not None, it is the date to be used for band 1 and band 2

    the raster is centered in the feature; extent_size is small_d, big_D
    """

    # 1. Load Data
    gdf = gpd.read_file(gpkg_path)
    
    # Select feature by Id
    selected_feat = gdf[gdf['Id'] == target_fid]
    if selected_feat.empty:
        print(f"Error: Id {target_fid} not found.")
        return None

    # --- REPROJECTION ---
    selected_feat = selected_feat.to_crs(target_crs)
    feature = selected_feat.iloc[0]
    geom = feature.geometry

    # --- INTERIOR PIXEL LOGIC ---
    # We apply a negative buffer (res/2) to ensure the 
    # geometry is strictly 'inside' and set all_touched=False.
    clean_geom = geom.buffer(-res/2) # Buffer by half the resolution to ensure we only get pixels fully inside
    if clean_geom.is_empty:
        # Fallback if the feature is smaller than a pixel
        clean_geom = geom

    # 2. Date Formatting
    if break_date_yyyyddmm:
        d0_int = int(break_date_yyyyddmm)
        d1_int = int(break_date_yyyyddmm)
    else:
        d0 = pd.to_datetime(feature['Data0'])
        d1 = pd.to_datetime(feature['Data1'])
        d0_int = int(d0.strftime('%Y%m%d'))
        d1_int = int(d1.strftime('%Y%m%d'))

    # 3. Calculate Spatial Extent
    centroid = geom.centroid
    left = int(centroid.x - (extent_size / 2))
    right = int(centroid.x + (extent_size / 2))
    bottom = int(centroid.y - (extent_size / 2))
    top = int(centroid.y + (extent_size / 2))

    # 4. Raster Setup
    width = int((right - left) / res)
    height = int((top - bottom) / res)
    transform = from_origin(left, top, res, res)
    
    out_name = f"{prefix}_{d0_int}_{left}_{right}_{bottom}_{top}.tif"
    out_path = os.path.join(output_dir, out_name)

    # 5. Generate Band Arrays
    # IMPORTANT: all_touched=False ensures only pixels with centers 
    # inside the polygon are selected.(already guaranteed by res/2 above)
    mask = rasterize(
        [(clean_geom, 1)],
        out_shape=(height, width),
        transform=transform,
        fill=0,
        all_touched=False, 
        dtype='uint32'
    )

    band1 = np.where(mask == 1, d0_int, 65535).astype('uint32')
    band2 = np.where(mask == 1, d1_int, 65535).astype('uint32')

    # 6. Write GeoTIFF
    profile = {
        'driver': 'GTiff',
        'height': height,
        'width': width,
        'count': 3,
        'dtype': 'uint32',
        'crs': TARGET_CRS,
        'transform': transform,
        'nodata': 65535,
        'compress': 'lzw'
    }

    # burn raster
    with rasterio.open(out_path, 'w', **profile) as dst:
        dst.write(band1, 1) 
        dst.write(band2, 2) 
        dst.write(mask, 3)   

    return out_path

import psutil  # You may need to: pip install psutil
import gc

def extract_spectral_data_generator(h5_path, raster_paths, delta, mem_threshold_pct=90):
    '''
    A generator that yields one DataFrame at a time to save memory.
    Stops if system memory usage exceeds mem_threshold_pct.
    '''

    with h5py.File(h5_path, 'r') as h5:
        print("Loading HDF5 coordinates and building KDTree...")
        h5_xs = h5['xs'][:]
        h5_ys = h5['ys'][:]
        h5_ts = h5['ts'][:]
        h5_values = h5['values'] 
        
        h5_coords = np.column_stack((h5_xs, h5_ys))
        tree = KDTree(h5_coords)

        for raster_path in raster_paths:
            # --- MEMORY GUARD ---
            mem_usage = psutil.virtual_memory().percent
            if mem_usage > mem_threshold_pct:
                print(f"!!! CRITICAL: Memory at {mem_usage}%. Stopping batch to prevent crash.")
                break 

            print(f"Processing: {os.path.basename(raster_path)} (RAM: {mem_usage}%)")
            
            with rasterio.open(raster_path) as src:
                b1, b2, b3 = src.read(1), src.read(2), src.read(3)
                transform = src.transform
            
            # Use the robust Date Range Logic from earlier
            interior_indices = np.where(b3 == 1) # correspond to input feature
            if len(interior_indices[0]) == 0:
                yield None
                continue
            
            # Interior pixels (interior to the feature, i.e. b3==1)
            ref_row, ref_col = interior_indices[0][0], interior_indices[1][0]
            date_before_str = str(int(b1[ref_row, ref_col]))
            date_after_str = str(int(b2[ref_row, ref_col]))
            
            dt_start = datetime.strptime(date_before_str, '%Y%m%d') - timedelta(days=delta)
            dt_end = datetime.strptime(date_after_str, '%Y%m%d') + timedelta(days=delta)
            
            start_ordinal = dt_start.toordinal()
            end_ordinal = dt_end.toordinal()
            
            time_mask = (h5_ts >= start_ordinal) & (h5_ts <= end_ordinal)
            time_indices = np.where(time_mask)[0]

            # all pixels from input raster
            rows, cols = np.where(b3 >= 0)
            raster_b3_values = b3[rows, cols]
            raster_xs, raster_ys = rasterio.transform.xy(transform, rows, cols)
            
            dist, pixel_indices = tree.query(np.column_stack((raster_xs, raster_ys)))
            unique_indices, inverse_map = np.unique(pixel_indices, return_inverse=True)

            raster_dfs = []
            for t_idx in time_indices:
                ts_data_unique = h5_values[t_idx, :, unique_indices] 
                ts_data = ts_data_unique[:, inverse_map]
                
                df_block = pd.DataFrame({
                    'xs': raster_xs, 'ys': raster_ys, 
                    'ts': h5_ts[t_idx], 'mask_b3': raster_b3_values
                })
                for b in range(NUMBER_BANDS_HDF5):
                    df_block[f'band_{b+1}'] = ts_data[b, :]
                raster_dfs.append(df_block)

            if raster_dfs:
                final_feature_df = pd.concat(raster_dfs, ignore_index=True)
                yield final_feature_df # SEND DATA BACK TO MAIN IMMEDIATELY
            else:
                yield None

            # Clean up local references for this specific loop iteration
            del raster_dfs
            gc.collect()


def extract_spectral_data_batch(h5_path, raster_paths, delta):
    '''
    Efficiently extracts spectral data for multiple rasters and 
    includes Band 3 values from the input raster in the output.
    '''
    all_results = []

    with h5py.File(h5_path, 'r') as h5:
        print("Loading HDF5 coordinates and building KDTree (once)...")
        h5_xs = h5['xs'][:]
        h5_ys = h5['ys'][:]
        h5_ts = h5['ts'][:]
        h5_values = h5['values'] 
        
        h5_coords = np.column_stack((h5_xs, h5_ys))
        tree = KDTree(h5_coords)

        for raster_path in raster_paths:
            print(f"Processing raster: {os.path.basename(raster_path)}")
            
            with rasterio.open(raster_path) as src:
                b1 = src.read(1)
                b2 = src.read(2)
                b3 = src.read(3)
                transform = src.transform
            
            # 1. Get coordinates for all pixels
            rows, cols = np.where(b3 >= 0) 
            if len(rows) == 0:
                all_results.append(None)
                continue

            # --- NEW: Extract the Band 3 values for these specific rows/cols ---
            raster_b3_values = b3[rows, cols]
            # ------------------------------------------------------------------

            raster_xs, raster_ys = rasterio.transform.xy(transform, rows, cols)
            target_coords = np.column_stack((raster_xs, raster_ys))

            dist, pixel_indices = tree.query(target_coords)
            unique_indices, inverse_map = np.unique(pixel_indices, return_inverse=True)

            # Date Range logic
            date_before_str = str(b1[rows[0], cols[0]])
            date_after_str = str(b2[rows[0], cols[0]])
            dt_start = datetime.strptime(date_before_str, '%Y%m%d') - timedelta(days=delta)
            dt_end = datetime.strptime(date_after_str, '%Y%m%d') + timedelta(days=delta)
            
            start_ordinal = dt_start.toordinal()
            end_ordinal = dt_end.toordinal()
            
            time_mask = (h5_ts >= start_ordinal) & (h5_ts <= end_ordinal)
            time_indices = np.where(time_mask)[0]

            if len(time_indices) == 0:
                all_results.append(None)
                continue

            raster_dfs = []
            for t_idx in time_indices:
                current_ts = h5_ts[t_idx]
                ts_data_unique = h5_values[t_idx, :, unique_indices] 
                ts_data = ts_data_unique[:, inverse_map]
                
                # 2. Add 'label' or 'mask' column to the DataFrame block
                df_block = pd.DataFrame({
                    'xs': raster_xs,
                    'ys': raster_ys,
                    'ts': current_ts,
                    'mask_b3': raster_b3_values # Values from the input raster band 3
                })

                for b in range(NUMBER_BANDS_HDF5):
                    df_block[f'band_{b+1}'] = ts_data[b, :]
                
                raster_dfs.append(df_block)

            if raster_dfs:
                all_results.append(pd.concat(raster_dfs, ignore_index=True))
            else:
                all_results.append(None)

    return all_results

if __name__ == "__main__":
    main()