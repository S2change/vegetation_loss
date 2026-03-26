import os
import re
import rasterio
from datetime import datetime, timezone

BAND_NAMES= ["B2", "B3", "B4", "B5", "B6", "B7", "B8", "B8a", "B11", "B12"] # ["B3", "B4", "B8", "B12"] for testing with fewer bands
TILE_NAMES = ['T29SMC', 'T29TQF', 'T29SMD', 'T29TQG', 'T29SNB', 'T29TME', 'T29SNC', 'T29SND', 'T29SPB', 'T29SPC', 'T29TNE', 'T29SPD', 'T29TNF', 'T29TNG', 'T29TPE', 'T29TPF', 'T29TPG']

INPUT_NODATA_VAL = 65535
OUTPUT_NODATA_VAL = 65535

def parse_and_sort_files(folder, tile, min_date=None, max_date=None):
    """
    Recursively finds Sentinel-2 TIFs in subfolders and extracts metadata.
    Parse timestamps from filenames and return metadata sorted by date.

    Inputs:
    - folder: directory containing the TIF files
    - min_date: datetime.date or None, minimum date to select files
    - max_date: datetime.date or None, maximum date to select files

    Output:
    - List of dicts with keys: 'filename', 'path', 'ordinal', 'timestamp_ms', sorted by 'timestamp_ms' (date)
    - filename is the original filename (path is not included)
    - path is the full path to the file
    - ordinal is the date converted to an integer for sorting
    - timestamp_ms is the original timestamp in milliseconds extracted from the filename    
    """
    file_metadata = []
    
    # 1. Use os.walk to go through all years and subfolders
    for root, dirs, files in os.walk(folder):
        for f in files:
            # 2. Filter: Only TIFs, must be MSIL2A, and ignore mask_omni
            if f.endswith('.tif') and 'S2C_MSIL2A' in f and tile in f and 'mask_omni' not in f:
                
                try:
                    # 3. Extract the date-time string (e.g., 20251007-110951)
                    # We split by underscore and take the 3rd element
                    parts = f.split('_')
                    if len(parts) < 3: continue
                    
                    time_str = parts[2]
                    
                    # 4. Parse into datetime object (UTC)
                    dt_obj = datetime.strptime(time_str, "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)
                    dt = dt_obj.date()
                    
                    # 5. Date Range Filtering
                    if (min_date is None or min_date <= dt) and (max_date is None or dt <= max_date):
                        # Store the FULL path so build_stack can actually find the file
                        full_path = os.path.join(root, f)
                        
                        file_metadata.append({
                            'filename': f, # file basename without path, since that's how we will reference the files later
                            'path': full_path,  # Added path for convenience
                            'ordinal': dt.toordinal(),
                            'timestamp_ms': int(dt_obj.timestamp() * 1000)
                        })
                except (ValueError, IndexError):
                    # Skip files that don't match the Sentinel-2 naming convention
                    continue

    # 6. Sort by timestamp in ascending order
    file_metadata.sort(key=lambda x: x['timestamp_ms'])
    return file_metadata

def read_all_bounds(sorted_pathnames):
    """
    Read bounding boxes for all TIF files.
    Input:
    - sorted_pathnames: list of file paths to TIFs
    Output:
    - Dictionary mapping file basenames to their bounding boxes (left, bottom, right, top)  
    """
    print("Reading extents from all files...")
    all_bounds = {}
    for path in sorted_pathnames:
        with rasterio.open(path) as src:
            # convert path into filename for the key of all_bounds, since that's how we will reference the files later
            f = os.path.basename(path)
            all_bounds[f] = src.bounds
    return all_bounds