import os
import re
import rasterio
from datetime import datetime, timezone

NODATA_VAL = 65535


def parse_and_sort_files(folder, min_date, max_date):
    """Parse timestamps from filenames and return metadata sorted by date."""
    files = [f for f in os.listdir(folder) if f.endswith('.tif')]
    file_metadata = []
    for f in files:
        match = re.search(r'_(\d{13})\.tif', f)
        if match:
            ts_ms = int(match.group(1))
            dt = datetime.fromtimestamp(ts_ms / 1000.0, timezone.utc).date()
            if (min_date is None or min_date <= dt) and (max_date is None or dt <= max_date):
                file_metadata.append({
                    'filename': f,
                    'ordinal': dt.toordinal(),
                    'timestamp_ms': ts_ms
                })
    file_metadata.sort(key=lambda x: x['ordinal'])
    return file_metadata


def read_all_bounds(folder, filenames):
    """Read bounding boxes for all TIF files."""
    print("Reading extents from all files...")
    all_bounds = {}
    for f in filenames:
        with rasterio.open(os.path.join(folder, f)) as src:
            all_bounds[f] = src.bounds
    return all_bounds
