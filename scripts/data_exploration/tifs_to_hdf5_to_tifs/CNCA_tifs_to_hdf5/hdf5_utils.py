import os
import re
import numpy as np
import rasterio
from datetime import date, datetime, timezone

BAND_NAMES= ["B2", "B3", "B4", "B5", "B6", "B7", "B8", "B8a", "B11", "B12"]
TILE_NAMES = ['T29SMC', 'T29TQF', 'T29SMD', 'T29TQG', 'T29SNB', 'T29TME', 'T29SNC', 'T29SND', 'T29SPB', 'T29SPC', 'T29TNE', 'T29SPD', 'T29TNF', 'T29TNG', 'T29TPE', 'T29TPF', 'T29TPG']
TILE_NAMES = ['T29TPG']

INPUT_NODATA_VAL = 65535
OUTPUT_NODATA_VAL = 65535
COORD_NODATA_VAL = -9999   # nodata sentinel for int32 coordinate arrays (xs_new, ys_new) at padding slots

# cloud cover will be computed relatively to the portuguese territory (+2 km buffer)
MAX_CLOUD_COVER_PT = 0.6 # (proportion: 0.6=60%) file_metadata only stores timestamps where cloud_cover_PT is below 60% and date is between MIN_DATE and MAX_DATE

# S2 geotiff files
FOLDER_S2 = r"" # 2025/S2B_MSIL2A_.../S2B_MSIL2A_...tif+S2B_MSIL1C_..._mask_omni.tif
FOLDER_PT_MASKS = r"...\Mascara_PT_S2" # mask_T29SMC.tif, etc
FOLDER_HDF5 = r"dgt\hdf5_2025"

# Date filters
MIN_DATE = date(2025, 1, 1) 
MAX_DATE = date(2025,12,31)

# hdf5 chunck size
N_TS_CHUNK = 12       # number of timestamps per chunk
CHIP_SIZE = 256 * 256  # spatial pixels per chunk (one 256×256 chip flattened)
CHIP_SIDE = 256   # pixels per chip side (256 × 256 = 65 536 px/chip)
PIXEL_RES = 10    # Sentinel-2 native resolution, metres
####################################################
"""
    File structure:

    |--- mascaras_PT_S2/
       |--- mask_T29SMC.tif : pixels of interest have value 0
       |--- mask_T29SMD.tif
       |--- mask_T29SNB.tif
       |--- etc
       
    2025/
    |--- S2C_MSIL2A_20251109-112321_N0511_R037_T29SMC_20251109T130709/
        |--- S2C_MSIL2A_20251109-112321_N0511_R037_T29SMC_20251109T130709.tif: 10 bands; NoData=65535
        |--- S2C_MSIL1C_20251109-112321_N0511_R037_T29SMC_mask_omni.tif: 1 encodes cloud pixel
    |--- S2C_MSIL2A_20251109-112321_N0511_R037_T29SMC_20251109T141914
        |---  S2C_MSIL2A_20251109-112321_N0511_R037_T29SMC_20251109T141914.tif
        |--- S2C_MSIL1C_20251109-112321_N0511_R037_T29SMC_mask_omni.tif
    |--- S2B_MSIL2A_20250826-112119_N0511_R037_T29SNB_20250826T133202
        |--- etc

    2026/
    ...
"""
#############################################

# extract same acquisition exact time (to aggregate parts of geotiff)
def extract_s2_prefix_from_s2_folder_name(f):
    """Strip the trailing processing timestamp token.
    'S2B_MSIL2A_20250826-112119_N0511_R037_T29SNB_20250826T133202'
    -> 'S2B_MSIL2A_20250826-112119_N0511_R037_T29SNB'
    """
    return f.rsplit('_', 1)[0]

# extract day yyyy:mm:dd (to aggregate all acquisitions for the tile on the same day)
def extract_s2_aquisition_day_from_s2_folder_name(f):
    """Return 'YYYYMMDD_TILE' as the day-level grouping key for a S2 folder name.

    Folders acquired on the same calendar day for the same tile are grouped
    together regardless of satellite (S2A/S2B/S2C) or exact acquisition time:
      'S2B_MSIL2A_20250813-110619_N0511_R137_T29TPG_20250813T133202' -> '20250813_T29TPG'
      'S2A_MSIL2A_20250813-112131_N0511_R037_T29TPG_20250813T141914' -> '20250813_T29TPG'
    """
    parts = f.split('_')
    if len(parts) < 3:
        return None
    date_str = parts[2][:8]          # 'YYYYMMDD' from 'YYYYMMDD-HHMMSS'
    prefix = f.rsplit('_', 1)[0]     # strip trailing processing timestamp
    tile = prefix.rsplit('_', 1)[1]  # last token of prefix = tile id
    return f"S2_{parts[1]}_{date_str}_{tile}"


def extract_s2_prefix_dt_timestamp_ms_from_S2_folder_name(f):
    """Return (s2_prefix, date, timestamp_ms) parsed from a S2 folder name."""
    parts = f.split('_')
    if len(parts) < 3:
        return None
    time_str = parts[2][0:8]  # use just day 20251109 from part[2]: 20251109-112321
    s2_prefix = extract_s2_aquisition_day_from_s2_folder_name(f)
    #dt_obj = datetime.strptime(time_str, "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)
    dt_obj = datetime.strptime(time_str, "%Y%m%d").replace(tzinfo=timezone.utc)
    dt = dt_obj.date()
    timestamp_ms = int(dt_obj.timestamp() * 1000) # this is just a coarse value for timestamp_ms, since we're aggregating several images
    return s2_prefix, dt, timestamp_ms

def determine_pt_mask_file(folder_pt_mask, tile):
    """
    Return the full path to the Portugal-boundary raster mask for `tile`.

    Scans all files in `folder_pt_mask` (non-recursively) and returns the one
    whose filename contains `tile` (e.g. 'T29SMC'). Pixels with value 0 mark
    the area of interest (Portugal); all others are outside.

    Inputs:
    - folder_pt_mask : str — directory containing one mask file per tile
    - tile           : str — tile identifier, e.g. 'T29SMC'

    Output:
    - str — full path to the mask file

    Raises FileNotFoundError if no matching file is found.
    Raises ValueError if more than one file matches (ambiguous).
    """
    matches = [
        os.path.join(folder_pt_mask, f)
        for f in os.listdir(folder_pt_mask)
        if os.path.isfile(os.path.join(folder_pt_mask, f)) and tile in f and f.endswith('.tif') and f.startswith('mask')
    ]
    if len(matches) == 0:
        raise FileNotFoundError(f"No PT mask file found for tile {tile} in {folder_pt_mask}")
    if len(matches) > 1:
        raise ValueError(f"Ambiguous PT mask: multiple files match tile {tile} in {folder_pt_mask}: {matches}")
    return matches[0]

def create_dict_of_s2_folder_prefixes(folder_s2, tile, min_date, max_date):
    """
    Walk `folder_s2` recursively and group S2 acquisition folders by their
    acquisition-time prefix (everything except the trailing processing timestamp).

    Two folders with the same acquisition time but different processing runs share
    the same prefix and are stored together in the same list, e.g.:
        'S2C_MSIL2A_20251109-112321_N0511_R037_T29SMC': [
            '.../S2C_MSIL2A_20251109-112321_N0511_R037_T29SMC_20251109T130709',
            '.../S2C_MSIL2A_20251109-112321_N0511_R037_T29SMC_20251109T141914',
        ]

    Inputs:
    - folder_s2 : str — root directory to search recursively (e.g. contains
                  year subfolders 2025/, 2026/, …)
    - tile       : str — tile identifier to filter on, e.g. 'T29SMC'
    - min_date   : datetime.date or None — earliest acquisition date (inclusive)
    - max_date   : datetime.date or None — latest acquisition date (inclusive)

    Output:
    - dict[str, list[str]] — keys are S2 prefixes, values are lists of full
      folder paths that share that prefix, in discovery order
    """
    result = {}
    for root, dirs, _ in os.walk(folder_s2):
        for f in dirs:
            if f.startswith('S2') and '_MSIL2A' in f and tile in f:
                parsed = extract_s2_prefix_dt_timestamp_ms_from_S2_folder_name(f)
                if parsed is None:
                    continue
                s2_prefix, dt, _ = parsed
                if (min_date is None or min_date <= dt) and (max_date is None or dt <= max_date):
                    result.setdefault(s2_prefix, []).append(os.path.join(root, f))
    return result



def parse_filter_sort_files(folder_s2, folder_pt_mask, tile, min_date=None, max_date=None):
    """
    Collect, filter, and sort S2 acquisition metadata for one tile.

    For each unique acquisition time found under `folder_s2` (grouped by S2 prefix,
    so multiple processing runs for the same acquisition are handled together):
      - cloud cover over Portugal is estimated by combining the per-run cloud masks
        (mask_omni, pixel value 1 = cloud) with pixel-wise max and dividing by the
        number of PT-mask interest pixels (value 0 in the PT mask file).
      - acquisitions whose cloud cover exceeds MAX_CLOUD_COVER_PT are discarded.

    The PT mask is also used to derive the tight bounding box (smallest rectangle
    enclosing all value-0 pixels), which is carried in every returned entry so that
    callers can open the S2 tifs with a rasterio Window without re-reading the mask.

    Inputs:
    - folder_s2      : str — root directory searched recursively for S2 acquisition
                       folders (e.g. contains year sub-folders 2025/, 2026/, …)
    - folder_pt_mask : str — directory containing one PT mask file per tile
    - tile           : str — tile identifier, e.g. 'T29SMC'
    - min_date       : datetime.date or None — earliest acquisition date (inclusive)
    - max_date       : datetime.date or None — latest acquisition date (inclusive)

    Output:
    - list of dicts sorted by timestamp_ms, one entry per accepted acquisition:
        filename       : str        — S2 acquisition prefix (shared key for all processing runs)
        paths          : list[str]  — full paths to the L2A tif(s) for this acquisition
        path_pt_mask   : str        — full path to the PT mask tif for this tile
        ordinal        : int        — acquisition date as datetime.date.toordinal()
        timestamp_ms   : int        — acquisition time as UTC milliseconds since epoch
        cloud_cover_pt : float      — fraction of PT-mask pixels covered by clouds
        nrows_pt       : int        — number of rows in the tight bbox
        ncols_pt       : int        — number of columns in the tight bbox
        left_pt        : float      — left edge of tight bbox (CRS units)
        bottom_pt      : float      — bottom edge of tight bbox (CRS units)
        right_pt       : float      — right edge of tight bbox (CRS units)
        top_pt         : float      — top edge of tight bbox (CRS units)
        row_off_pt     : int        — row offset of tight bbox top-left in full tile
        col_off_pt     : int        — col offset of tight bbox top-left in full tile
        transform_pt   : Affine     — affine transform of the tight bbox
        crs            : CRS        — coordinate reference system of the PT mask
        pixel_count_pt : int        — number of interest pixels (value 0) in PT mask
    """
   
    file_metadata = []

    #0. path for PT_mask file: pixels of interest have value 0
    path_pt_mask= determine_pt_mask_file(folder_pt_mask,tile)

    #1. collect paths of s2_folders and filters using dates and tile
    folders_dict=create_dict_of_s2_folder_prefixes(folder_s2, tile, min_date, max_date)

    # 2. loop through folders_dict. For each key, estimate cloud cover from cloud_masks and PT_mask
    # cloud cover files: names like S2C_MSIL1C_20251109-112321_N0511_R037_T29SMC_mask_omni.tif: 1 encodes cloud pixel
    #if there is just one path for each key, then the estimated cloud cover value is just the number of "1" in the cloud cover tif file over the number of "0" in the pt_mask_file

    # Read PT mask once: denominator = number of pixels of interest (value == 0)
    with rasterio.open(path_pt_mask) as src:
        pt_mask = src.read(1)
        pt_transform = src.transform
        crs = src.crs
    pt_pixel_count = int((pt_mask == 0).sum())

    # Tight bounding box of value-0 pixels in the PT mask
    rows0, cols0 = np.where(pt_mask == 0)
    if rows0.size > 0:
        tight_row_off = int(rows0.min())
        tight_col_off = int(cols0.min())
        tight_nrows = int(rows0.max() - rows0.min() + 1)
        tight_ncols = int(cols0.max() - cols0.min() + 1)
        tight_left,  tight_top    = rasterio.transform.xy(pt_transform, tight_row_off, tight_col_off, offset='ul')
        tight_right, tight_bottom = rasterio.transform.xy(pt_transform, rows0.max(), cols0.max(), offset='lr')
        tight_transform = rasterio.windows.transform(
            rasterio.windows.Window(tight_col_off, tight_row_off, tight_ncols, tight_nrows),
            pt_transform,
        )
    else:
        tight_row_off = tight_col_off = 0
        tight_left = tight_bottom = tight_right = tight_top = None
        tight_nrows = tight_ncols = 0
        tight_transform = pt_transform

    # loop through all timestamps
    for _, folder_paths in sorted(folders_dict.items()):
        combined_cloud_mask = None
        combined_b2 = None
        for fp in folder_paths:
            # PT_CLOUD_MASK
            cloud_mask_files = [
                f for f in os.listdir(fp)
                if f.endswith('.tif') and 'mask_omni' in f
            ]
            if len(cloud_mask_files) == 0:
                print(f"WARNING: no mask_omni file found in {fp} — skipping")
                combined_cloud_mask = None
                break
            if len(cloud_mask_files) > 1:
                print(f"WARNING: multiple mask_omni files in {fp}: {cloud_mask_files} — skipping")
                combined_cloud_mask = None
                break
            with rasterio.open(os.path.join(fp, cloud_mask_files[0])) as src:
                mask = src.read(1)
                combined_cloud_mask = mask if combined_cloud_mask is None else np.maximum(combined_cloud_mask, mask)
        if combined_cloud_mask is None:
            continue
        
        for fp in folder_paths:
            # B2
            band_files = [
                f for f in os.listdir(fp)
                if f.endswith('.tif') and 'mask_omni' not in f
            ]
            if len(band_files) == 0:
                print(f"WARNING: no band file found in {fp} — skipping")
                combined_b2 = None
                break
            if len(band_files) > 1:
                print(f"WARNING: multiple band files in {fp}: {band_files} — skipping")
                combined_b2 = None
                break
            with rasterio.open(os.path.join(fp, band_files[0])) as src:
                b2 = src.read(1)
                combined_b2 = b2 if combined_b2 is None else np.minimum(combined_b2, b2)
        if combined_b2 is None:
            continue
        # pt_pixel_count = int((pt_mask == 0).sum())
        a0=int((combined_b2 < 65535).sum()) # number of pixels in PT, in available orbits, and not masked as clouds
        a1=int((combined_cloud_mask ==1).sum()) # number of pixels in PT, in available orbits, and masked as clouds
        count_orbit_pixels_pt= a0+a1 # total number of pixels in available orbits
        # Determine if combined  has no non-cloud pixels in PT, in the orbits
        number_clear_pixels_in_PT = a0 #int(((combined_b2 < 65535) & (pt_mask == 0)).sum())
        #combined_cloud_pixels_pt=int((combined_cloud_mask == 1).sum())
        #cloud_cover = combined_cloud_pixels_pt / pt_pixel_count if pt_pixel_count > 0 else None
        #cloud_cover = (1-number_clear_pixels_in_PT/ pt_pixel_count) if pt_pixel_count > 0 else None
        cloud_cover = (1-number_clear_pixels_in_PT/ count_orbit_pixels_pt) if count_orbit_pixels_pt > 0 else None
        
         
        parsed = extract_s2_prefix_dt_timestamp_ms_from_S2_folder_name(os.path.basename(folder_paths[0]))
        if parsed is None:
            continue
        s2_prefix, dt, timestamp_ms = parsed
        tif_paths = [os.path.join(fp, os.path.basename(fp) + '.tif') for fp in folder_paths]
        s2_original_files = '__'.join(
            os.path.basename(fp) for fp in folder_paths
        )

        print(s2_prefix," has",number_clear_pixels_in_PT,"clear pixels in PT in a total of ",pt_pixel_count," PT pixels, with ",count_orbit_pixels_pt, " within orbits")
        if number_clear_pixels_in_PT>0:
            print("cloud_cover",cloud_cover)

        # only aggregates that have valid pixels in PT and cloud_cover in PT below threshold are stored in file_metadata
        if number_clear_pixels_in_PT>0 and cloud_cover < MAX_CLOUD_COVER_PT:
            file_metadata.append({
                'filename': s2_prefix,
                'paths': tif_paths,
                's2_original_filenames': s2_original_files,
                'path_pt_mask': path_pt_mask,
                'ordinal': dt.toordinal(),
                'timestamp_ms': timestamp_ms,
                'cloud_cover_pt': cloud_cover,
                'nrows_pt': tight_nrows,
                'ncols_pt': tight_ncols,
                'left_pt': tight_left,
                'bottom_pt': tight_bottom,
                'right_pt': tight_right,
                'top_pt': tight_top,
                'row_off_pt': tight_row_off,
                'col_off_pt': tight_col_off,
                'transform_pt': tight_transform,
                'crs': crs,
                'pixel_count_pt': pt_pixel_count,
                'clear_pixel_count_pt': number_clear_pixels_in_PT,
                'count_orbit_pixels_pt': count_orbit_pixels_pt,
            })

    file_metadata.sort(key=lambda x: x['timestamp_ms'])
    return file_metadata
