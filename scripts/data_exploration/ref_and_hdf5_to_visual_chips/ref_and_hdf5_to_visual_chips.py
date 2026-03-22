import logging
import random
import sys
import h5py
import numpy as np
import pandas as pd
import rasterio
from scipy.spatial import KDTree
from datetime import date,datetime, timedelta
import os
import glob
import geopandas as gpd
from rasterio.transform import from_origin
from rasterio.features import rasterize
from scipy import stats
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import psutil  # You may need to: pip install psutil
import gc

# --- CONFIGURATION ---
GPKG_PATH = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\harmonized\BDR_expanded_v0.gpkg"
LABEL_REF_DATASET="BDRexp_v0"

DELTA = 45 # Days to expand date range on either side of the break date (can be adjusted as needed)
TARGET_CRS = "EPSG:32629"
NUMBER_BANDS_HDF5=6
NIR='band_3' # NIR is band 3 in the HDF5 file
RED='band_2' # RED is band 2 in the HDF5 file
#FID_TO_PROCESS = 309 #87 #309 # Id of the feature to process (adjust as needed)
small_D = 100  # Extent size in meters (100m x 100m) - adjust as needed
big_D=4000 # Extent to create raster and extract spectral data from hdf5 (to ultimately create visual chip)
#prefix = f"BDRexp_{FID_TO_PROCESS}"
RES=10 # Raster resolution in meters (adjust as needed)
NODATA_VAL = 65535 # The value used in the hdf5 file to indicate NoData (adjust if different)
PAD_VALUE=0 # output visual chip band value
T_TEST_THRESHOLD,T_TEST_LABEL=0.05,'05'
MIN_REF_FEAT_AREA=20000 # 3ha
MIN_NDVI_DROP, LABEL_MIN_NDVI_DROP=0.05,'05'
WINDOW_MAX_HALF_RANGE, LABEL_WINDOW_MAX_HALF_RANGE=30,'30' # maximum number of days before and after the center date to consider in the t-test (to avoid comparing very distant dates that may not be relevant for the break detection)

#----------------------

''' 
changes from v0:
- only process features included in tile: OK
- organize output per tile: OK
- fixed vchip extension: 4 km: OK
- vchip not centered in reference feature: OK
- only reference features with more than 3 ha: OK
- create companion gdf with same prefix as vchip, with geometry of the reference feature and attributes 'Id' and 'Chg_type' (if exists in input GPKG): OK
- half-windo to apply t-test of 30 days; does not depend on a fixed number of observations before and after, but on a fixed temporal window: OK
- new logic to determine break date: we move a candidate break date from present to past, and for each candidate break date we apply the t-test to the observations that are within a temporal window of +/- 30 days around the candidate break date. The first candidate break date that has a significant t-test result (p-value < 0.05) and a mean NDVI drop of at least 0.05 is selected as the break date. This way we ensure that the detected break date is not too far from the reference dates, and we also ensure that there are enough observations on either side of the break date to perform a valid t-test.
- added a minimum drop of average ndvi to declare a drop date as a break, to avoid detecting breaks that are statistically significant but not relevant in practice because the drop is very small (e.g., less than 0.05 in average NDVI)
'''

tile="T29TNE"
H5_FOLDER = os.path.join(r"H:\outputs_ROI\hdf5",tile)
VISUAL_CHIPS_FOLDER=os.path.join(r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\visual", tile)
PLOTS_FOLDER=os.path.join(r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\plots", tile)
RASTER_FOLDER = os.path.join(r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\harmonized_to_tifs", tile)
S2TILES_PATH = os.path.join(r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5", "sentinel2_tiles_PT_32629.gpkg")

# 1st clear RASTER_FOLDER
os.makedirs(RASTER_FOLDER, exist_ok=True)
os.makedirs(VISUAL_CHIPS_FOLDER, exist_ok=True)
os.makedirs(PLOTS_FOLDER, exist_ok=True)
# clear temporary folders
for folder in [RASTER_FOLDER, PLOTS_FOLDER]:
    for f in os.listdir(folder):
        os.remove(os.path.join(folder, f))


def main():
    '''
    Main function to process the GeoPackage, extract spectral data from HDF5, detect breaks, and create visual chips.
    (1) The main loop visits all features in the GeoPackage, checks for valid dates, creates rasters for valid features, 
    and then extracts spectral data from the HDF5 file for those rasters. 
    (2) Then, the input are the raster files that were created in the previous step, and the extracted spectral data from the HDF5 file for those rasters.
    It then calculates NDVI time series, detects breaks, if any, using Welch's t-test, and creates visual chips with consistent symbology based on the detected break date.
    '''
    H5_PATH = get_most_recent_h5(H5_FOLDER,extension="*.h5")
    for var in [GPKG_PATH, H5_PATH]:
        if not os.path.exists(var):
            print(f"Error: File {var} does not exist. Please check the path and try again.")
            return  
    
    # 1. Load the GeoPackage
    gdf = gpd.read_file(GPKG_PATH)
    raster_list = []
    fids_processed = []
    companion_gdfs = []
    
    print(f"Total features in GPKG: {len(gdf)}")

    # load tile geometry from S2TILES_PATH, filter by tile code in attribute 'name' and get geometry of the tile, to later check if features are within the tile and only process those that are within the tile
    tile_gdf = gpd.read_file(S2TILES_PATH)
    # check if tile_gdf geometry is the same as target CRS, if not reproject it to target CRS
    if tile_gdf.crs != TARGET_CRS:
        tile_gdf = tile_gdf.to_crs(TARGET_CRS)

    # determine just the feature for 'tile' 
    tile_gdf = tile_gdf[tile_gdf['Name'] == tile]
    if not tile_gdf.empty:
        tile_geom = tile_gdf.iloc[0].geometry
    else:        
        print(f"Error: Tile {tile} not found in {S2TILES_PATH}. Please check the tile code and try again.")
        return

    # 2. Loop through features and filter out NULL dates
    for index, feat in gdf.iterrows():
        # 1st convert feature geometry to target CRS if needed; we need to do this before the tile geometry check to ensure both geometries are in the same CRS for accurate spatial operations. If the feature geometry is not in the target CRS, we reproject it.
        if gdf.crs != TARGET_CRS:
            # reproject feat to target CRS; the result should still be of the same type as feat, not just a geometry. I want to preserve teh attributes
            try:
                geom_series = gpd.GeoSeries([feat.geometry], crs=gdf.crs)
                geom_reprojected = geom_series.to_crs(TARGET_CRS).iloc[0]
                feat.geometry = geom_reprojected
            except Exception as e:
                print(f"Error reprojecting feature ID {feat['Id']}: {e}. Skipping this feature.")
                continue

        # to be faster, one could check that the bounding box of the feature is within the tile geometry, to avoid doing a more time consuming check of the whole feature geometry. If the bounding box of the feature is not within the tile geometry, we skip processing this feature.
        if not tile_geom.contains(feat.geometry):
            print(f"Skipping FID {feat['Id']}: Bounding box not within tile {tile}.")
            continue    

        # Check if Date0 and Date1 are both present
        if pd.notnull(feat['Data0']) and pd.notnull(feat['Data1']): # and (feat['Id']>297) and (feat['Id']<312):
            fid = feat['Id']
            prefix = f"{LABEL_REF_DATASET}_{fid}"
            # Generate the 3 band raster raster for this valid feature and save tifs in RASTER_FOLDER with b1=date0 and b2=date1, b3=1 within feature
            path, companion_gdf = process_feature_to_raster(feat, fid, output_dir=RASTER_FOLDER, prefix=prefix, 
                                            extent_size=big_D,res=RES, target_crs=TARGET_CRS,
                                            min_ref_feat_area=MIN_REF_FEAT_AREA,visual_chips_dir=VISUAL_CHIPS_FOLDER)
            if path:
                raster_list.append(path)
                fids_processed.append(fid)
                companion_gdfs.append(companion_gdf)
        else:
            # Optional: Log which FIDs were skipped
            print(f"Skipping FID {feat['Id']}: One or both dates are NULL.")

    if not raster_list:
        print("No valid rasters created. Check your input data for NULL dates.")
        return
    
    #print('raster_list',raster_list)

    # After creating the raster_list, we can proceed to extract spectral data from the HDF5 file for those rasters
    # 1. Extract spectral data
    # start/end dates are in band 1 and 2 of the rasters
    # Delta is the interval of days before and after dates
    data_stream = extract_spectral_data_generator(H5_PATH, raster_paths=raster_list, delta=DELTA,companion_gdfs=companion_gdfs) # with yield: memory efficient
    
    for i, (df, date_before_str,date_after_str,companion_gdf) in enumerate(data_stream):
        if df is not None:
            current_fid = fids_processed[i]
        
            # 2. Calculate NDVI time series
            ordinal_dates, ndvi_values = calculate_ndvi_and_changes(df, RED, NIR, nodata_val=NODATA_VAL)
            
            # 3. Detect the break using the new logic
            # Returns a datetime.date object or None
            break_date = detect_breaks_welch_midpoint(ordinal_dates, ndvi_values, date_before_str, date_after_str,  
                                                    window_max_half_range=WINDOW_MAX_HALF_RANGE,p_threshold=T_TEST_THRESHOLD, min_drop=MIN_NDVI_DROP)
            
            if break_date:
                print(30*"-")
                print(f"!!! Break Detected: Most significant drop around {break_date} !!!")

                # format break_date, like '2020-07-13',  as YYYYMMDD
                break_date_yyyyddmm = int(break_date.strftime('%Y%m%d'))
                prefix = f"{LABEL_REF_DATASET}_{current_fid}_{T_TEST_LABEL}_{LABEL_MIN_NDVI_DROP}_{LABEL_WINDOW_MAX_HALF_RANGE}"  
                # plot 
                plot_ndvi_time_series(ordinal_dates, ndvi_values, break_date, WINDOW_MAX_HALF_RANGE, date_before_str, date_after_str, prefix)
                 # Create visual chips with spectral band data from df (for one single fid)
                create_time_composites_vectorized(df, companion_gdf, break_date_int=break_date_yyyyddmm, output_dir=VISUAL_CHIPS_FOLDER, target_crs=TARGET_CRS, res=RES, 
                                                  nodata_val=NODATA_VAL, pad_value=PAD_VALUE,prefix=prefix)
            else:
                print(f"No significant drop detected (all p-values > {T_TEST_THRESHOLD} or drop less than {MIN_NDVI_DROP}).")
        
        # --- MANUALLY FREE MEMORY ---
            del df 
            gc.collect() 
        else:
            print(f"Skipping feature {i} due to lack of data.")

def get_most_recent_h5(folder_path,extension):
    ''' determine most recent file in folder with some extension'''
    # 1. Create a list of all .h5 files in the directory
    search_pattern = os.path.join(folder_path, extension)
    files = glob.glob(search_pattern)

    if not files:
        print(f"No {extension} files found in the directory.")
        return None

    # 2. Find the file with the maximum modification time
    most_recent_file = max(files, key=os.path.getmtime)
    
    return most_recent_file

def plot_ndvi_time_series(ordinal_dates, ndvi_values, break_date, window_max_half_range, 
                          date_before_str, date_after_str, prefix):
    """
    Plots the NDVI time series, highlighting the temporal window (in days) 
    used for the Welch's t-test detection.
    """
    # 1. Convert ordinals to datetime objects
    dates = [datetime.fromordinal(int(d)) for d in ordinal_dates]
    
    # Parse string bounds
    dt_ref_before = datetime.strptime(date_before_str, '%Y%m%d')
    dt_ref_after = datetime.strptime(date_after_str, '%Y%m%d')

    plt.figure(figsize=(12, 6))
    plt.plot(dates, ndvi_values, marker='o', linestyle='-', color='#2ecc71', 
             label='Mean NDVI', markersize=4, alpha=0.8)

    # 2. Reference Vertical Lines
    plt.axvline(x=dt_ref_before, color='#3498db', linestyle=':', linewidth=1.2, label='Target Start')
    plt.axvline(x=dt_ref_after, color='#f39c12', linestyle=':', linewidth=1.2, label='Target End')

    if break_date:
        # Convert date object to datetime
        break_dt = datetime.combine(break_date, datetime.min.time())
        
        # 3. Highlight the detected break
        plt.axvline(x=break_dt, color='#e74c3c', linestyle='--', linewidth=2, 
                    label=f'Break Detected ({break_date})')
        
        # 4. Highlight the TEMPORAL window used (e.g., +/- 30 days)
        # win_start = break_date - 30 days; win_end = break_date + 30 days
        win_start = break_dt - timedelta(days=window_max_half_range)
        win_end = break_dt + timedelta(days=window_max_half_range)
        
        plt.axvspan(win_start, win_end, color='gray', alpha=0.15, label=f'Temporal Window ({window_max_half_range}d)')

    # Formatting
    plt.title(f"NDVI Time Series - Feature {prefix}", fontsize=13, fontweight='bold')
    plt.xlabel("Date", fontsize=11)
    plt.ylabel("Mean NDVI", fontsize=11)
    plt.grid(True, which='both', linestyle='--', alpha=0.3)
    
    # Legend outside to avoid overlapping data
    plt.legend(loc='upper left', bbox_to_anchor=(1, 1), frameon=False)
    
    # Date formatting
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gcf().autofmt_xdate() 
    
    plt.tight_layout()
    
    # Save the plot
    plot_path = os.path.join(PLOTS_FOLDER, f"{prefix}_ndvi_plot.png")
    plt.savefig(plot_path, dpi=150)
    
    # Clean up memory
    plt.close() 

    return plot_path

def calculate_ndvi_and_changes(df, red_band, nir_band, nodata_val):
    """
    Calculates NDVI for each pixel and timestamp, skipping pixels that match
    the NODATA value in either the Red or NIR bands.
    """
    print(f"Calculating NDVI and aggregating time series (skipping NODATA: {nodata_val})...")

    # 1. Create a mask of valid pixels (where neither band is the NoData value)
    # This prevents NODATA_VAL from being treated as a real spectral value
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

def detect_breaks_welch_midpoint(ordinal_dates, y, date_before_str, date_after_str, 
                                 window_max_half_range=30, p_threshold=0.05, 
                                 min_drop=0.15, min_obs=2):
    """
    Welch's t-test with a dynamic window moving Present -> Past.
    Returns the MIDPOINT date between the last 'before' and first 'after' observation.
    """
    y = np.array(y)
    ordinal_dates = np.array(ordinal_dates)
    
    limit_start = datetime.strptime(str(date_before_str), '%Y%m%d').toordinal()
    limit_end = datetime.strptime(str(date_after_str), '%Y%m%d').toordinal()

    # Iterate backwards through the indices
    # We start from 1 because we need at least one 'before' point at index i-1
    for i in range(len(y) - 1, 0, -1):
        # The 'split' occurs between index i-1 and index i
        # Candidate date for the break is the first 'after' point
        after_start_date = ordinal_dates[i]
        before_end_date = ordinal_dates[i-1]
        
        # Calculate the midpoint between these two observations
        midpoint_ordinal = (before_end_date + after_start_date) // 2
        
        # Check if the midpoint falls within our user-defined target interval
        if not (limit_start <= midpoint_ordinal <= limit_end):
            continue

        # --- WINDOW DEFINITION ---
        # Before: observations in [midpoint - range, midpoint)
        # After:  observations in [midpoint, midpoint + range]
        before_mask = (ordinal_dates < midpoint_ordinal) & (ordinal_dates >= midpoint_ordinal - window_max_half_range)
        after_mask = (ordinal_dates >= midpoint_ordinal) & (ordinal_dates <= midpoint_ordinal + window_max_half_range)
        
        before_y = y[before_mask]
        after_y = y[after_mask]

        # Ensure statistical validity
        if len(before_y) < min_obs or len(after_y) < min_obs:
            continue

        # --- TEST ---
        mean_before = np.mean(before_y)
        mean_after = np.mean(after_y)
        drop_magnitude = mean_before - mean_after
        
        t_stat, p_val = stats.ttest_ind(before_y, after_y, equal_var=False, alternative='greater')
        
        if p_val <= p_threshold and drop_magnitude >= min_drop:
            # Success: Return the midpoint of the transition
            return date.fromordinal(int(midpoint_ordinal))
        
    return None

def process_feature_to_raster(feat, target_fid, output_dir, prefix, 
                             extent_size, res, target_crs, min_ref_feat_area, visual_chips_dir):
    """
    Generates a randomly-shifted raster chip and a companion GeoPackage.
    Ensures the feature is fully contained within the chip extent.
    """

    # --- AREA THRESHOLD CHECK ---
    geom=feat.geometry
    feat_area = geom.area
    if feat_area < min_ref_feat_area:
        logging.info(f"Skipping FID {target_fid}: Area ({feat_area:.1f} m²) < {min_ref_feat_area} m².")
        return None,None

    # --- DATE FORMATTING ---
    d0,d1 = pd.to_datetime(feat['Data0']), pd.to_datetime(feat['Data1'])
    d0_int,d1_int = int(d0.strftime('%Y%m%d')), int(d1.strftime('%Y%m%d'))

    # 2. RANDOM SPATIAL EXTENT LOGIC
    minx, miny, maxx, maxy = geom.bounds
    feat_w = maxx - minx
    feat_h = maxy - miny

    if feat_w > extent_size or feat_h > extent_size:
        # Fallback to centering if feature is larger than the chip
        left = geom.centroid.x - (extent_size / 2)
        bottom = geom.centroid.y - (extent_size / 2)
    else:
        # Calculate wiggle room (slack)
        slack_x = extent_size - feat_w
        slack_y = extent_size - feat_h
        
        # Random offset ensures (minx - offset) is the chip start
        left = minx - random.uniform(0, slack_x)
        bottom = miny - random.uniform(0, slack_y)

    # Snap to resolution grid
    left = int(np.floor(left / res) * res)
    bottom = int(np.floor(bottom / res) * res)
    right = left + extent_size
    top = bottom + extent_size

    # 3. RASTER SETUP
    width = int(extent_size / res)
    height = int(extent_size / res)
    transform = from_origin(left, top, res, res)
    
    # Filename stem
    stem_name = f"{prefix}_{d0_int}_{left}_{bottom}"
    out_path = os.path.join(output_dir, f"{stem_name}.tif")

    # 4. GENERATE MASK & BANDS
    clean_geom = geom.buffer(-res/2)
    if clean_geom.is_empty: clean_geom = geom

    mask = rasterize(
        [(clean_geom, 1)],
        out_shape=(height, width),
        transform=transform,
        fill=0,
        all_touched=False, 
        dtype='uint32'
    )

    # 5. WRITE GEOTIFF
    profile = {
        'driver': 'GTiff', 'height': height, 'width': width, 'count': 3,
        'dtype': 'uint32', 'crs': target_crs, 'transform': transform,
        'nodata': 65535, 'compress': 'lzw'
    }

    with rasterio.open(out_path, 'w', **profile) as dst:
        dst.write(np.where(mask == 1, d0_int, 65535).astype('uint32'), 1) 
        dst.write(np.where(mask == 1, d1_int, 65535).astype('uint32'), 2) # Using d0 for both as per logic
        dst.write(mask, 3)
    
    # We keep 'Id' and 'Chg_type'. 
    cols_to_keep = ['geometry', 'Data0', 'Data1']
    # 'Chg_type' is one of the attributes in the input GPKG, but it may not be present in all versions of the GPKG. We check if it exists before trying to include it in the output GPKG.
    if 'Chg_type' in feat.index and pd.notna(feat['Chg_type']):
        cols_to_keep.append('Chg_type')
        
    # 2. Convert the Series to a GeoDataFrame
    # We wrap [feat[cols_to_keep]] in a list to make it a single-row table
    companion_gdf = gpd.GeoDataFrame([feat[cols_to_keep]], crs=target_crs)

    return out_path, companion_gdf

def extract_spectral_data_generator(h5_path, raster_paths, delta, companion_gdfs, mem_threshold_pct=90):
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

        #for raster_path in raster_paths:
        for i, raster_path in enumerate(raster_paths):
            # pass companion_gdf for the current raster, if it exists; otherwise pass None
            companion_gdf = companion_gdfs[i] if i < len(companion_gdfs) else None
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
                yield None, None, None,None
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
                yield final_feature_df,date_before_str,date_after_str,companion_gdf # SEND DATA BACK TO MAIN IMMEDIATELY
            else:
                yield None, None, None, None

            # Clean up local references for this specific loop iteration
            del raster_dfs
            gc.collect()

def create_time_composites_vectorized(df, companion_gdf, break_date_int, output_dir, target_crs, res=10, nodata_val=NODATA_VAL, pad_value=0, prefix='', 
                                      std_dev_factor=3):
    """
    Creates 'before' and 'after' GeoTIFFs with consistent 3-stdDev symbology
    calculated from the combined time series.
    """

    # 1. Date Conversion (YYYYMMDD -> Ordinal)
    date_str = str(break_date_int)
    break_ordinal = datetime.strptime(date_str, '%Y%m%d').toordinal()

    if 'ts_ordinal' not in df.columns:
        df['ts_ordinal'] = df['ts']

    # 2. Calculate Combined Statistics for Symbology (RGB = 3,2,1)
    # We only use non-nodata pixels for stats to avoid skewing the mean
    stats_config = {}
    for band_idx, color in zip([3, 2, 1], ['R', 'G', 'B']):
        col_name = f'band_{band_idx}'
        valid_data = df[df[col_name] != nodata_val][col_name]
        
        if not valid_data.empty:
            mean_val = valid_data.mean()
            std_val = valid_data.std()
            stats_config[f'min{color}'] = max(0, mean_val - (std_dev_factor * std_val))
            stats_config[f'max{color}'] = min(NODATA_VAL, mean_val + (std_dev_factor * std_val))
        else:
            stats_config[f'min{color}'], stats_config[f'max{color}'] = 0, 4000 # Fallback

    # 3. Setup Spatial Grid
    unique_xs = np.sort(df['xs'].unique())
    unique_ys = np.sort(df['ys'].unique())[::-1]
    width, height = len(unique_xs), len(unique_ys)
    x_map = {x: i for i, x in enumerate(unique_xs)}
    y_map = {y: i for i, y in enumerate(unique_ys)}
    transform = from_origin(unique_xs[0], unique_ys[0], res, res)

    # 4. Split and Sort: check inequalities
    df_b = df[df['ts_ordinal'] < break_ordinal].sort_values('ts_ordinal', ascending=False) #before
    df_a = df[df['ts_ordinal'] >= break_ordinal].sort_values('ts_ordinal', ascending=True) #after, includes EQUAL to break date

    def build_stack(sub_df, suffix):
        stack = np.full((NUMBER_BANDS_HDF5, height, width), pad_value, dtype='uint16')
        if sub_df.empty: return None

        for b in range(1, NUMBER_BANDS_HDF5 + 1):
            band_name = f'band_{b}'
            valid_pixels = sub_df[sub_df[band_name] != nodata_val].drop_duplicates(subset=['xs', 'ys'])
            if not valid_pixels.empty:
                rows = [y_map[y] for y in valid_pixels['ys']]
                cols = [x_map[x] for x in valid_pixels['xs']]
                stack[b-1, rows, cols] = valid_pixels[band_name].values

        out_path = os.path.join(output_dir, f"{prefix}_{suffix}_{break_date_int}.tif")
        profile = {
            'driver': 'GTiff', 'height': height, 'width': width, 'count': NUMBER_BANDS_HDF5,
            'dtype': 'uint16', 'crs': target_crs, 'transform': transform,
            'nodata': pad_value, 'compress': 'lzw'
        }
        with rasterio.open(out_path, 'w', **profile) as dst:
            dst.write(stack)
        
        # Write the customized QML for this specific chip
        save_qgis_style_custom(out_path, stats_config)
        return out_path

    path_before = build_stack(df_b, "before")
    path_after = build_stack(df_a, "after")

    # Write companion GeoPackage with the same prefix  
    # determine stem_name from out_path, like 'BDRexp_309_025_before_20200713.tif' -> 'BDRexp_309_025_before_20200713'
    # BUT REMOVE THE '_before' OR '_after' SUFFIX TO GET A COMMON STEM NAME FOR BOTH BEFORE AND AFTER GPKG
    stem_name = os.path.basename(path_before).replace('.tif', '').replace('_before', '').replace('_after', '') if path_before else os.path.basename(path_after).replace('.tif', '').replace('_before', '').replace('_after', '')
    gpkg_out_path = os.path.join(VISUAL_CHIPS_FOLDER, f"{stem_name}.gpkg")
    companion_gdf.to_file(gpkg_out_path, driver="GPKG")
    save_vector_qml(gpkg_out_path)

    return path_before, path_after

def save_vector_qml(gpkg_path):
    """
    Creates a QML style for a vector layer: 
    Transparent fill, Black outline, Width 20.
    """
    qml_path = gpkg_path.replace(".gpkg", ".qml")
    
    # Note the double-braces {{ }} for the QGIS IDs to avoid f-string errors
    qml_content = f"""<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
    <qgis styleCategories="Symbology|Labeling" version="3.44.7-Solothurn" labelsEnabled="1">
    <renderer-v2 referencescale="-1" enableorderby="0" symbollevels="0" forceraster="0" type="singleSymbol">
    <symbols>
        <symbol alpha="1" frame_rate="10" name="0" is_animated="0" clip_to_extent="1" type="fill" force_rhr="0">
        <data_defined_properties>
            <Option type="Map">
            <Option name="name" value="" type="QString"/>
            <Option name="properties"/>
            <Option name="type" value="collection" type="QString"/>
            </Option>
        </data_defined_properties>
        <layer enabled="1" locked="0" pass="0" id="{{507ea401-5fe0-44db-acf5-e8bc97ac0fc9}}" class="SimpleLine">
            <Option type="Map">
            <Option name="align_dash_pattern" value="0" type="QString"/>
            <Option name="capstyle" value="square" type="QString"/>
            <Option name="customdash" value="5;2" type="QString"/>
            <Option name="customdash_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
            <Option name="customdash_unit" value="MM" type="QString"/>
            <Option name="dash_pattern_offset" value="0" type="QString"/>
            <Option name="dash_pattern_offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
            <Option name="dash_pattern_offset_unit" value="MM" type="QString"/>
            <Option name="draw_inside_polygon" value="0" type="QString"/>
            <Option name="joinstyle" value="bevel" type="QString"/>
            <Option name="line_color" value="0,0,0,255,rgb:0,0,0,1" type="QString"/>
            <Option name="line_style" value="solid" type="QString"/>
            <Option name="line_width" value="0.96" type="QString"/>
            <Option name="line_width_unit" value="MM" type="QString"/>
            <Option name="offset" value="0" type="QString"/>
            <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
            <Option name="offset_unit" value="MM" type="QString"/>
            <Option name="ring_filter" value="0" type="QString"/>
            <Option name="trim_distance_end" value="0" type="QString"/>
            <Option name="trim_distance_end_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
            <Option name="trim_distance_end_unit" value="MM" type="QString"/>
            <Option name="trim_distance_start" value="0" type="QString"/>
            <Option name="trim_distance_start_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
            <Option name="trim_distance_start_unit" value="MM" type="QString"/>
            <Option name="tweak_dash_pattern_on_corners" value="0" type="QString"/>
            <Option name="use_custom_dash" value="0" type="QString"/>
            <Option name="width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
            </Option>
            <data_defined_properties>
            <Option type="Map">
                <Option name="name" value="" type="QString"/>
                <Option name="properties"/>
                <Option name="type" value="collection" type="QString"/>
            </Option>
            </data_defined_properties>
        </layer>
        </symbol>
    </symbols>
    <rotation/>
    <sizescale/>
    <data-defined-properties>
        <Option type="Map">
        <Option name="name" value="" type="QString"/>
        <Option name="properties"/>
        <Option name="type" value="collection" type="QString"/>
        </Option>
    </data-defined-properties>
    </renderer-v2>
    <selection mode="Default">
    <selectionColor invalid="1"/>
    <selectionSymbol>
        <symbol alpha="1" frame_rate="10" name="" is_animated="0" clip_to_extent="1" type="fill" force_rhr="0">
        <data_defined_properties>
            <Option type="Map">
            <Option name="name" value="" type="QString"/>
            <Option name="properties"/>
            <Option name="type" value="collection" type="QString"/>
            </Option>
        </data_defined_properties>
        <layer enabled="1" locked="0" pass="0" id="{{6955d215-37e9-4154-9a4d-a0ba5131644d}}" class="SimpleFill">
            <Option type="Map">
            <Option name="border_width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
            <Option name="color" value="0,0,255,255,rgb:0,0,1,1" type="QString"/>
            <Option name="joinstyle" value="bevel" type="QString"/>
            <Option name="offset" value="0,0" type="QString"/>
            <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
            <Option name="offset_unit" value="MM" type="QString"/>
            <Option name="outline_color" value="35,35,35,255,rgb:0.1372549,0.1372549,0.1372549,1" type="QString"/>
            <Option name="outline_style" value="solid" type="QString"/>
            <Option name="outline_width" value="0.26" type="QString"/>
            <Option name="outline_width_unit" value="MM" type="QString"/>
            <Option name="style" value="solid" type="QString"/>
            </Option>
            <data_defined_properties>
            <Option type="Map">
                <Option name="name" value="" type="QString"/>
                <Option name="properties"/>
                <Option name="type" value="collection" type="QString"/>
            </Option>
            </data_defined_properties>
        </layer>
        </symbol>
    </selectionSymbol>
    </selection>
    <labeling type="simple">
    <settings calloutType="simple">
        <text-style fontItalic="0" fontLetterSpacing="0" fontUnderline="0" fieldName="concat( &quot;Data0&quot; ,'_', &quot;Data1&quot; )" fontWordSpacing="0" fontSizeUnit="Point" multilineHeightUnit="Percentage" tabStopDistanceMapUnitScale="3x:0,0,0,0,0,0" namedStyle="Regular" forcedItalic="0" stretchFactor="100" legendString="Aa" multilineHeight="1" forcedBold="0" textColor="250,250,250,255,hsv:0,0,0.9789425497825589,1" fontSize="10" fontWeight="50" fontKerning="1" fontStrikeout="0" tabStopDistance="80" capitalization="0" isExpression="1" fontSizeMapUnitScale="3x:0,0,0,0,0,0" textOpacity="1" previewBkgrdColor="255,255,255,255,rgb:1,1,1,1" tabStopDistanceUnit="Point" fontFamily="Open Sans" allowHtml="0" textOrientation="horizontal" blendMode="0" useSubstitutions="0">
        <families/>
        <text-buffer bufferJoinStyle="128" bufferNoFill="1" bufferSizeUnits="MM" bufferSizeMapUnitScale="3x:0,0,0,0,0,0" bufferOpacity="1" bufferSize="1" bufferBlendMode="0" bufferDraw="0" bufferColor="250,250,250,255,rgb:0.9803922,0.9803922,0.9803922,1"/>
        <text-mask maskJoinStyle="128" maskSizeUnits="MM" maskSizeMapUnitScale="3x:0,0,0,0,0,0" maskedSymbolLayers="" maskOpacity="1" maskSize="1.5" maskSize2="1.5" maskEnabled="0" maskType="0"/>
        <background shapeRotationType="0" shapeJoinStyle="64" shapeOffsetX="0" shapeRadiiUnit="Point" shapeOpacity="1" shapeSizeType="0" shapeOffsetMapUnitScale="3x:0,0,0,0,0,0" shapeBorderWidthUnit="Point" shapeDraw="0" shapeRadiiX="0" shapeSizeUnit="Point" shapeRadiiMapUnitScale="3x:0,0,0,0,0,0" shapeSVGFile="" shapeOffsetY="0" shapeRadiiY="0" shapeRotation="0" shapeBorderWidth="0" shapeSizeX="0" shapeSizeMapUnitScale="3x:0,0,0,0,0,0" shapeBlendMode="0" shapeBorderWidthMapUnitScale="3x:0,0,0,0,0,0" shapeFillColor="255,255,255,255,rgb:1,1,1,1" shapeSizeY="0" shapeBorderColor="128,128,128,255,rgb:0.5019608,0.5019608,0.5019608,1" shapeType="0" shapeOffsetUnit="Point">
            <symbol alpha="1" frame_rate="10" name="markerSymbol" is_animated="0" clip_to_extent="1" type="marker" force_rhr="0">
            <data_defined_properties>
                <Option type="Map">
                <Option name="name" value="" type="QString"/>
                <Option name="properties"/>
                <Option name="type" value="collection" type="QString"/>
                </Option>
            </data_defined_properties>
            <layer enabled="1" locked="0" pass="0" id="" class="SimpleMarker">
                <Option type="Map">
                <Option name="angle" value="0" type="QString"/>
                <Option name="cap_style" value="square" type="QString"/>
                <Option name="color" value="190,178,151,255,rgb:0.745098,0.6980392,0.5921569,1" type="QString"/>
                <Option name="horizontal_anchor_point" value="1" type="QString"/>
                <Option name="joinstyle" value="bevel" type="QString"/>
                <Option name="name" value="circle" type="QString"/>
                <Option name="offset" value="0,0" type="QString"/>
                <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
                <Option name="offset_unit" value="MM" type="QString"/>
                <Option name="outline_color" value="35,35,35,255,rgb:0.1372549,0.1372549,0.1372549,1" type="QString"/>
                <Option name="outline_style" value="solid" type="QString"/>
                <Option name="outline_width" value="0" type="QString"/>
                <Option name="outline_width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
                <Option name="outline_width_unit" value="MM" type="QString"/>
                <Option name="scale_method" value="diameter" type="QString"/>
                <Option name="size" value="2" type="QString"/>
                <Option name="size_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
                <Option name="size_unit" value="MM" type="QString"/>
                <Option name="vertical_anchor_point" value="1" type="QString"/>
                </Option>
                <data_defined_properties>
                <Option type="Map">
                    <Option name="name" value="" type="QString"/>
                    <Option name="properties"/>
                    <Option name="type" value="collection" type="QString"/>
                </Option>
                </data_defined_properties>
            </layer>
            </symbol>
            <symbol alpha="1" frame_rate="10" name="fillSymbol" is_animated="0" clip_to_extent="1" type="fill" force_rhr="0">
            <data_defined_properties>
                <Option type="Map">
                <Option name="name" value="" type="QString"/>
                <Option name="properties"/>
                <Option name="type" value="collection" type="QString"/>
                </Option>
            </data_defined_properties>
            <layer enabled="1" locked="0" pass="0" id="" class="SimpleFill">
                <Option type="Map">
                <Option name="border_width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
                <Option name="color" value="255,255,255,255,rgb:1,1,1,1" type="QString"/>
                <Option name="joinstyle" value="bevel" type="QString"/>
                <Option name="offset" value="0,0" type="QString"/>
                <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
                <Option name="offset_unit" value="MM" type="QString"/>
                <Option name="outline_color" value="128,128,128,255,rgb:0.5019608,0.5019608,0.5019608,1" type="QString"/>
                <Option name="outline_style" value="no" type="QString"/>
                <Option name="outline_width" value="0" type="QString"/>
                <Option name="outline_width_unit" value="Point" type="QString"/>
                <Option name="style" value="solid" type="QString"/>
                </Option>
                <data_defined_properties>
                <Option type="Map">
                    <Option name="name" value="" type="QString"/>
                    <Option name="properties"/>
                    <Option name="type" value="collection" type="QString"/>
                </Option>
                </data_defined_properties>
            </layer>
            </symbol>
        </background>
        <shadow shadowRadiusMapUnitScale="3x:0,0,0,0,0,0" shadowScale="100" shadowUnder="0" shadowOffsetMapUnitScale="3x:0,0,0,0,0,0" shadowRadiusAlphaOnly="0" shadowRadiusUnit="MM" shadowOpacity="0.69999999999999996" shadowDraw="0" shadowColor="0,0,0,255,rgb:0,0,0,1" shadowOffsetDist="1" shadowBlendMode="6" shadowRadius="1.5" shadowOffsetGlobal="1" shadowOffsetAngle="135" shadowOffsetUnit="MM"/>
        <dd_properties>
            <Option type="Map">
            <Option name="name" value="" type="QString"/>
            <Option name="properties"/>
            <Option name="type" value="collection" type="QString"/>
            </Option>
        </dd_properties>
        <substitutions/>
        </text-style>
        <text-format autoWrapLength="0" multilineAlign="3" decimals="3" addDirectionSymbol="0" placeDirectionSymbol="0" formatNumbers="0" wrapChar="" rightDirectionSymbol=">" leftDirectionSymbol="&lt;" plussign="0" useMaxLineLengthForAutoWrap="1" reverseDirectionSymbol="0"/>
        <placement repeatDistanceUnits="MM" xOffset="0" overrunDistanceMapUnitScale="3x:0,0,0,0,0,0" polygonPlacementFlags="2" maximumDistance="0" maximumDistanceUnit="MM" geometryGenerator="" fitInPolygonOnly="0" overrunDistance="0" centroidWhole="0" overlapHandling="PreventOverlap" predefinedPositionOrder="TR,TL,BR,BL,R,L,TSR,BSR" rotationAngle="0" geometryGeneratorEnabled="0" quadOffset="4" allowDegraded="0" overrunDistanceUnit="MM" prioritization="PreferCloser" preserveRotation="1" offsetUnits="MM" distUnits="MM" maxCurvedCharAngleOut="-25" dist="0" repeatDistance="0" placement="0" yOffset="0" maxCurvedCharAngleIn="25" layerType="PolygonGeometry" priority="5" placementFlags="10" maximumDistanceMapUnitScale="3x:0,0,0,0,0,0" offsetType="0" lineAnchorType="0" rotationUnit="AngleDegrees" lineAnchorPercent="0.5" lineAnchorTextPoint="FollowPlacement" labelOffsetMapUnitScale="3x:0,0,0,0,0,0" geometryGeneratorType="PointGeometry" lineAnchorClipping="0" centroidInside="0" distMapUnitScale="3x:0,0,0,0,0,0" repeatDistanceMapUnitScale="3x:0,0,0,0,0,0"/>
        <rendering upsidedownLabels="0" fontMinPixelSize="3" obstacle="1" scaleVisibility="0" scaleMax="0" obstacleType="1" maxNumLabels="2000" minFeatureSize="0" fontLimitPixelSize="0" unplacedVisibility="0" fontMaxPixelSize="10000" scaleMin="0" obstacleFactor="1" limitNumLabels="0" labelPerPart="0" drawLabels="1" mergeLines="0" zIndex="0"/>
        <dd_properties>
        <Option type="Map">
            <Option name="name" value="" type="QString"/>
            <Option name="properties"/>
            <Option name="type" value="collection" type="QString"/>
        </Option>
        </dd_properties>
        <callout type="simple">
        <Option type="Map">
            <Option name="anchorPoint" value="pole_of_inaccessibility" type="QString"/>
            <Option name="blendMode" value="0" type="int"/>
            <Option name="ddProperties" type="Map">
            <Option name="name" value="" type="QString"/>
            <Option name="properties"/>
            <Option name="type" value="collection" type="QString"/>
            </Option>
            <Option name="drawToAllParts" value="false" type="bool"/>
            <Option name="enabled" value="0" type="QString"/>
            <Option name="labelAnchorPoint" value="point_on_exterior" type="QString"/>
            <Option name="lineSymbol" value="&lt;symbol alpha=&quot;1&quot; frame_rate=&quot;10&quot; name=&quot;symbol&quot; is_animated=&quot;0&quot; clip_to_extent=&quot;1&quot; type=&quot;line&quot; force_rhr=&quot;0&quot;>&lt;data_defined_properties>&lt;Option type=&quot;Map&quot;>&lt;Option name=&quot;name&quot; value=&quot;&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;properties&quot;/>&lt;Option name=&quot;type&quot; value=&quot;collection&quot; type=&quot;QString&quot;/>&lt;/Option>&lt;/data_defined_properties>&lt;layer enabled=&quot;1&quot; locked=&quot;0&quot; pass=&quot;0&quot; id=&quot;{{6ae6cc91-5624-40e2-bc48-691f5e9fc922}}&quot; class=&quot;SimpleLine&quot;>&lt;Option type=&quot;Map&quot;>&lt;Option name=&quot;align_dash_pattern&quot; value=&quot;0&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;capstyle&quot; value=&quot;square&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;customdash&quot; value=&quot;5;2&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;customdash_map_unit_scale&quot; value=&quot;3x:0,0,0,0,0,0&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;customdash_unit&quot; value=&quot;MM&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;dash_pattern_offset&quot; value=&quot;0&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;dash_pattern_offset_map_unit_scale&quot; value=&quot;3x:0,0,0,0,0,0&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;dash_pattern_offset_unit&quot; value=&quot;MM&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;draw_inside_polygon&quot; value=&quot;0&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;joinstyle&quot; value=&quot;bevel&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;line_color&quot; value=&quot;60,60,60,255,rgb:0.2352941,0.2352941,0.2352941,1&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;line_style&quot; value=&quot;solid&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;line_width&quot; value=&quot;0.3&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;line_width_unit&quot; value=&quot;MM&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;offset&quot; value=&quot;0&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;offset_map_unit_scale&quot; value=&quot;3x:0,0,0,0,0,0&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;offset_unit&quot; value=&quot;MM&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;ring_filter&quot; value=&quot;0&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;trim_distance_end&quot; value=&quot;0&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;trim_distance_end_map_unit_scale&quot; value=&quot;3x:0,0,0,0,0,0&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;trim_distance_end_unit&quot; value=&quot;MM&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;trim_distance_start&quot; value=&quot;0&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;trim_distance_start_map_unit_scale&quot; value=&quot;3x:0,0,0,0,0,0&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;trim_distance_start_unit&quot; value=&quot;MM&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;tweak_dash_pattern_on_corners&quot; value=&quot;0&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;use_custom_dash&quot; value=&quot;0&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;width_map_unit_scale&quot; value=&quot;3x:0,0,0,0,0,0&quot; type=&quot;QString&quot;/>&lt;/Option>&lt;data_defined_properties>&lt;Option type=&quot;Map&quot;>&lt;Option name=&quot;name&quot; value=&quot;&quot; type=&quot;QString&quot;/>&lt;Option name=&quot;properties&quot;/>&lt;Option name=&quot;type&quot; value=&quot;collection&quot; type=&quot;QString&quot;/>&lt;/Option>&lt;/data_defined_properties>&lt;/layer>&lt;/symbol>" type="QString"/>
            <Option name="minLength" value="0" type="double"/>
            <Option name="minLengthMapUnitScale" value="3x:0,0,0,0,0,0" type="QString"/>
            <Option name="minLengthUnit" value="MM" type="QString"/>
            <Option name="offsetFromAnchor" value="0" type="double"/>
            <Option name="offsetFromAnchorMapUnitScale" value="3x:0,0,0,0,0,0" type="QString"/>
            <Option name="offsetFromAnchorUnit" value="MM" type="QString"/>
            <Option name="offsetFromLabel" value="0" type="double"/>
            <Option name="offsetFromLabelMapUnitScale" value="3x:0,0,0,0,0,0" type="QString"/>
            <Option name="offsetFromLabelUnit" value="MM" type="QString"/>
        </Option>
        </callout>
    </settings>
    </labeling>
    <blendMode>0</blendMode>
    <featureBlendMode>0</featureBlendMode>
    <layerGeometryType>2</layerGeometryType>
    </qgis>


    """
    with open(qml_path, "w") as f:
        f.write(qml_content)

def save_qgis_style_custom(tif_path, stats):
    """
    Generates QML using a hardcoded template from a 'good' manual setup.
    Double-braces {{ }} are used to prevent f-string interpolation errors.
    """
    qml_path = tif_path.replace(".tif", ".qml")
    
    # Use f-string with double braces for literal QGIS IDs
    qml_content = f"""<!DOCTYPE qgis PUBLIC 'http://mrcc.com/qgis.dtd' 'SYSTEM'>
<qgis minScale="1e+08" styleCategories="AllStyleCategories" hasScaleBasedVisibilityFlag="0" autoRefreshTime="0" version="3.44.7-Solothurn" autoRefreshMode="Disabled" maxScale="0">
  <flags>
    <Identifiable>1</Identifiable>
    <Removable>1</Removable>
    <Searchable>1</Searchable>
    <Private>0</Private>
  </flags>
  <temporal enabled="0" bandNumber="1" fetchMode="0" mode="0">
    <fixedRange>
      <start></start>
      <end></end>
    </fixedRange>
  </temporal>
  <elevation symbology="Line" zscale="1" band="1" enabled="0" zoffset="0" mode="RepresentsElevationSurface">
    <data-defined-properties>
      <Option type="Map">
        <Option name="name" value="" type="QString"/>
        <Option name="properties"/>
        <Option name="type" value="collection" type="QString"/>
      </Option>
    </data-defined-properties>
    <profileLineSymbol>
      <symbol name="" frame_rate="10" alpha="1" clip_to_extent="1" is_animated="0" force_rhr="0" type="line">
        <data_defined_properties>
          <Option type="Map">
            <Option name="name" value="" type="QString"/>
            <Option name="properties"/>
            <Option name="type" value="collection" type="QString"/>
          </Option>
        </data_defined_properties>
        <layer enabled="1" pass="0" locked="0" class="SimpleLine" id="{{9b28795f-17cf-4bf1-9f2e-814e83f689c3}}">
          <Option type="Map">
            <Option name="align_dash_pattern" value="0" type="QString"/>
            <Option name="capstyle" value="square" type="QString"/>
            <Option name="customdash" value="5;2" type="QString"/>
            <Option name="customdash_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
            <Option name="customdash_unit" value="MM" type="QString"/>
            <Option name="dash_pattern_offset" value="0" type="QString"/>
            <Option name="dash_pattern_offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
            <Option name="dash_pattern_offset_unit" value="MM" type="QString"/>
            <Option name="draw_inside_polygon" value="0" type="QString"/>
            <Option name="joinstyle" value="bevel" type="QString"/>
            <Option name="line_color" value="152,125,183,255,rgb:0.5960784,0.4901961,0.7176471,1" type="QString"/>
            <Option name="line_style" value="solid" type="QString"/>
            <Option name="line_width" value="0.6" type="QString"/>
            <Option name="line_width_unit" value="MM" type="QString"/>
            <Option name="offset" value="0" type="QString"/>
            <Option name="offset_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
            <Option name="offset_unit" value="MM" type="QString"/>
            <Option name="ring_filter" value="0" type="QString"/>
            <Option name="trim_distance_end" value="0" type="QString"/>
            <Option name="trim_distance_end_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
            <Option name="trim_distance_end_unit" value="MM" type="QString"/>
            <Option name="trim_distance_start" value="0" type="QString"/>
            <Option name="trim_distance_start_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
            <Option name="trim_distance_start_unit" value="MM" type="QString"/>
            <Option name="tweak_dash_pattern_on_corners" value="0" type="QString"/>
            <Option name="use_custom_dash" value="0" type="QString"/>
            <Option name="width_map_unit_scale" value="3x:0,0,0,0,0,0" type="QString"/>
          </Option>
        </layer>
      </symbol>
    </profileLineSymbol>
    <profileFillSymbol>
      <symbol name="" frame_rate="10" alpha="1" clip_to_extent="1" is_animated="0" force_rhr="0" type="fill">
        <layer enabled="1" pass="0" locked="0" class="SimpleFill" id="{{306fb425-de9d-4ac9-9fbe-d312c9eb78fb}}">
          <Option type="Map">
            <Option name="color" value="152,125,183,255,rgb:0.5960784,0.4901961,0.7176471,1" type="QString"/>
            <Option name="style" value="solid" type="QString"/>
          </Option>
        </layer>
      </symbol>
    </profileFillSymbol>
  </elevation>
  <pipe>
    <provider>
      <resampling enabled="false" maxOversampling="2" zoomedInResamplingMethod="nearestNeighbour" zoomedOutResamplingMethod="nearestNeighbour"/>
    </provider>
    <rasterrenderer blueBand="1" greenBand="2" nodataColor="" alphaBand="-1" opacity="1" redBand="3" type="multibandcolor">
      <rasterTransparency>
        <singleValuePixelList>
          <pixelListEntry min="0" max="0" percentTransparent="100"/>
        </singleValuePixelList>
      </rasterTransparency>
      <minMaxOrigin>
        <limits>None</limits>
        <extent>WholeRaster</extent>
        <statAccuracy>Estimated</statAccuracy>
        <stdDevFactor>3</stdDevFactor>
      </minMaxOrigin>
      <redContrastEnhancement>
            <minValue>{stats['minR']:.1f}</minValue>
            <maxValue>{stats['maxR']:.1f}</maxValue>
            <algorithm>StretchToMinimumMaximum</algorithm>
      </redContrastEnhancement>
      <greenContrastEnhancement>
            <minValue>{stats['minG']:.1f}</minValue>
            <maxValue>{stats['maxG']:.1f}</maxValue>
            <algorithm>StretchToMinimumMaximum</algorithm>
      </greenContrastEnhancement>
      <blueContrastEnhancement>
            <minValue>{stats['minB']:.1f}</minValue>
            <maxValue>{stats['maxB']:.1f}</maxValue>
            <algorithm>StretchToMinimumMaximum</algorithm>
      </blueContrastEnhancement>
    </rasterrenderer>
    <brightnesscontrast gamma="1" brightness="0" contrast="0"/>
    <huesaturation colorizeOn="0" colorizeStrength="100" saturation="0" colorizeBlue="128" grayscaleMode="0" colorizeRed="255" colorizeGreen="128" invertColors="0"/>
    <rasterresampler maxOversampling="2"/>
    <resamplingStage>resamplingFilter</resamplingStage>
  </pipe>
  <blendMode>0</blendMode>
</qgis>"""
    with open(qml_path, "w") as f:
        f.write(qml_content)

if __name__ == "__main__":
    main()