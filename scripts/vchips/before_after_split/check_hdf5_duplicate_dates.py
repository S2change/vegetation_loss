"""
Diagnose duplicate dates in an HDF5 tile file.

Steps:
  1. Check whether the 'ts' array is monotonically ascending, descending, or
     mixed.
  2. Find the first N duplicate-date groups (where N defaults to 10). A group
     is two or more 'ts' entries sharing the same ordinal date.
  3. For each group, compare every pair of timesteps with np.array_equal
     (strict byte-for-byte equality across all bands and pixels).
  4. If any pair in the group differs, write a multi-band spatial GeoTIFF
     for each timestep in the group, named dup_{date}_{run_idx}.tif. Bands
     are written in the same ascending order they appear in the HDF5.

Usage:
    python check_hdf5_duplicate_dates.py <hdf5_path> <tiles_gpkg> <output_dir>

Example:
    python check_hdf5_duplicate_dates.py \\
        /users1/dgt/hdf5/T29SMC.h5 \\
        /users1/cpca070342024/shared/auxiliary_data/sentinel2_tiles_PT_32629.gpkg \\
        ./duplicate_outputs
"""
import os
import sys
from collections import defaultdict
from datetime import datetime

import h5py
import numpy as np
import rasterio as rio
import geopandas as gpd
from affine import Affine

# Match conventions from sibling scripts
HDF5_NODATA = 65535
PIXEL_SIZE = 10
MAX_DUPLICATE_GROUPS = 5


# ============================================================================
# DATE / SEQUENCE HELPERS
# ============================================================================

def ordinal_to_yyyymmdd(ordinal):
    d = datetime.fromordinal(int(ordinal))
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"


def classify_sequence(ts):
    """Classify the sequence of ordinal dates as ascending, descending, mixed,
    or constant. Duplicates are allowed within a monotonic classification —
    'ascending' means non-decreasing."""
    if len(ts) < 2:
        return "single-entry"

    diffs = np.diff(ts)
    if np.all(diffs == 0):
        return "all-identical"
    if np.all(diffs >= 0) and np.any(diffs > 0):
        return "ascending (non-decreasing)"
    if np.all(diffs <= 0) and np.any(diffs < 0):
        return "descending (non-increasing)"
    return "mixed"


def find_duplicate_groups(ts, max_groups):
    """
    Walk ts in order and return the first `max_groups` duplicate-date groups.

    Each group is (ordinal_date, [list of indices]) for entries sharing that
    date. Singletons are skipped. Order of returned groups follows the order
    in which the duplicate's *first* index appears in ts.
    """
    by_date = defaultdict(list)
    for i, ordinal in enumerate(ts):
        by_date[int(ordinal)].append(i)

    groups = [(ordinal, idxs) for ordinal, idxs in by_date.items() if len(idxs) > 1]

    # Order groups by the first index of each, so output reflects file order
    groups.sort(key=lambda g: g[1][0])
    return groups[:max_groups]


# ============================================================================
# TILE GEOMETRY (mirrors chip_creation.py)
# ============================================================================

def infer_tile_grid(tiles_gpkg, tile_id, pixel_size):
    """Look up the tile's bbox from the gpkg and snap to the pixel grid."""
    gdf = gpd.read_file(tiles_gpkg)
    if 'Name' not in gdf.columns:
        raise ValueError(f"Expected 'Name' column in {tiles_gpkg}; got {list(gdf.columns)}")

    matches = gdf[gdf['Name'] == tile_id]
    if len(matches) == 0:
        raise ValueError(f"Tile '{tile_id}' not found in {tiles_gpkg}")

    minx, miny, maxx, maxy = matches.iloc[0].geometry.bounds
    tile_width = int(round((maxx - minx) / pixel_size))
    tile_height = int(round((maxy - miny) / pixel_size))
    return tile_height, tile_width, float(minx), float(maxy)


def compute_pixel_grid_positions(xs, ys, tile_height, tile_width, xmin, ymax, pixel_size):
    """For each HDF5 pixel, compute its (row, col) in the regular tile grid."""
    cols = np.round((xs - xmin) / pixel_size).astype(np.int64)
    rows = np.round((ymax - ys) / pixel_size).astype(np.int64)
    cols = np.clip(cols, 0, tile_width - 1)
    rows = np.clip(rows, 0, tile_height - 1)
    return rows, cols


# ============================================================================
# TIMESTEP COMPARISON
# ============================================================================

def timesteps_strictly_equal(values_ds, idx_a, idx_b):
    """Return True if values_ds[idx_a] and values_ds[idx_b] are byte-for-byte
    identical across all bands and pixels.

    Compares one band at a time to keep peak memory bounded. With 10 bands and
    18M pixels, holding a full timestep is ~360 MB; per-band is ~36 MB. Stops
    at the first differing band.
    """
    n_bands = values_ds.shape[1]
    for b_idx in range(n_bands):
        a: np.ndarray = values_ds[int(idx_a), b_idx, :]  # type: ignore[index]
        b: np.ndarray = values_ds[int(idx_b), b_idx, :]  # type: ignore[index]
        if not np.array_equal(a, b):
            return False
    return True


def group_has_differences(values_ds, indices):
    """
    Return True if any pair of timesteps within the group differs.

    Stops at the first difference found — doesn't compare every pair if the
    first comparison already shows them as distinct.
    """
    first_idx = indices[0]
    for other_idx in indices[1:]:
        if not timesteps_strictly_equal(values_ds, first_idx, other_idx):
            return True
    return False


# ============================================================================
# TIF WRITING
# ============================================================================

def write_timestep_tif(out_path, values_ds, time_idx, rows, cols,
                       tile_height, tile_width, n_bands, transform, crs):
    """Read one timestep from the HDF5 and write it as a multi-band GeoTIFF
    on the regular tile grid. Cells without HDF5 data stay HDF5_NODATA.

    Writes one band at a time to bound peak memory. A full 10-band tile-sized
    grid would be ~2.4 GB; per-band is ~240 MB.
    """
    meta = {
        'driver': 'GTiff',
        'width': tile_width,
        'height': tile_height,
        'count': n_bands,
        'dtype': 'uint16',
        'nodata': HDF5_NODATA,
        'transform': transform,
        'crs': crs,
        'compress': 'lzw',
    }
    with rio.open(out_path, 'w', **meta) as dst:
        for b_idx in range(n_bands):
            band_flat: np.ndarray = values_ds[int(time_idx), b_idx, :]  # type: ignore[index]
            band_grid = np.full((tile_height, tile_width), HDF5_NODATA, dtype=np.uint16)
            band_grid[rows, cols] = band_flat
            # rasterio band indices are 1-based
            dst.write(band_grid, b_idx + 1)
            del band_flat, band_grid


# ============================================================================
# MAIN
# ============================================================================

def main():
    if len(sys.argv) != 4:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    hdf5_path, tiles_gpkg, output_dir = sys.argv[1:4]
    tile_id = os.path.splitext(os.path.basename(hdf5_path))[0]

    print(f"\nHDF5 file: {hdf5_path}")
    print(f"Tile ID: {tile_id}")
    print(f"GeoPackage: {tiles_gpkg}")
    print(f"Output directory: {output_dir}\n")

    with h5py.File(hdf5_path, 'r') as h5f:
        ts: np.ndarray = h5f['ts'][:]      # type: ignore[index]
        xs: np.ndarray = h5f['xs'][:]      # type: ignore[index]
        ys: np.ndarray = h5f['ys'][:]      # type: ignore[index]
        values_ds = h5f['values']          # type: ignore[index]
        n_t, n_bands, n_pixels = values_ds.shape  # type: ignore[misc]
        print(f"Timesteps: {n_t}, bands: {n_bands}, pixels: {n_pixels:,}")

        # Step 1: classify the date sequence
        seq_kind = classify_sequence(ts)
        print(f"Date sequence: {seq_kind}")
        if len(ts) > 0:
            print(f"  First date: {ordinal_to_yyyymmdd(ts[0])}")
            print(f"  Last date:  {ordinal_to_yyyymmdd(ts[-1])}\n")

        # Step 2: find duplicate groups
        groups = find_duplicate_groups(ts, MAX_DUPLICATE_GROUPS)
        if not groups:
            print("No duplicate dates found.")
            return

        print(f"First {len(groups)} duplicate-date group(s):")
        for ordinal, idxs in groups:
            print(f"  {ordinal_to_yyyymmdd(ordinal)} -> indices {idxs}")
        print()

        # Step 3 + 4: compare each group, write tifs if they differ
        # Build the tile grid for spatial output (only needed if we end up
        # writing tifs, but compute once up front to keep the logic simple).
        tile_height, tile_width, xmin, ymax = infer_tile_grid(tiles_gpkg, tile_id, PIXEL_SIZE)
        rows, cols = compute_pixel_grid_positions(xs, ys, tile_height, tile_width, xmin, ymax, PIXEL_SIZE)
        transform = Affine(PIXEL_SIZE, 0.0, xmin, 0.0, -PIXEL_SIZE, ymax)
        crs = 'EPSG:32629'

        os.makedirs(output_dir, exist_ok=True)
        groups_with_differences = 0
        tifs_written = 0

        for ordinal, idxs in groups:
            date_str = ordinal_to_yyyymmdd(ordinal)
            print(f"Comparing group {date_str} ({len(idxs)} entries)...")
            if not group_has_differences(values_ds, idxs):
                print(f"  All {len(idxs)} entries are identical — skipping tif export.")
                continue

            print(f"  Differences found — writing {len(idxs)} tifs.")
            groups_with_differences += 1
            for run_idx, t_idx in enumerate(idxs):
                out_name = f"dup_{date_str}_{run_idx}.tif"
                out_path = os.path.join(output_dir, out_name)
                write_timestep_tif(
                    out_path, values_ds, t_idx, rows, cols,
                    tile_height, tile_width, n_bands, transform, crs,
                )
                tifs_written += 1
                print(f"    {out_path}")

        print(f"\nSummary: {groups_with_differences} of {len(groups)} groups had "
              f"differences. {tifs_written} tif(s) written to {output_dir}.")


if __name__ == "__main__":
    main()
