"""
Print dates from an HDF5 tile.

Two modes:
  - With (x, y) coordinates: print every date where the SELECTION_BAND value
    at the nearest pixel is below HDF5_NODATA (default 65535).
  - Without coordinates: print every date stored in the HDF5 file's `ts`
    array, regardless of pixel-level validity.

Usage:
    python check_hdf5_dates_at_point.py <hdf5_path>
    python check_hdf5_dates_at_point.py <hdf5_path> <x> <y> [<x> <y> ...]

Examples:
    python check_hdf5_dates_at_point.py /users1/dgt/hdf5/T29SMC.h5
    python check_hdf5_dates_at_point.py /users1/dgt/hdf5/T29SMC.h5 508255 4370295
    python check_hdf5_dates_at_point.py T29SMC.h5 508255 4370295 510000 4380000
"""
import os
import sys
from datetime import datetime

import h5py
import numpy as np

# Match the main script's conventions
HDF5_NODATA = 65535
SELECTION_BAND_INDEX = 0  # B2 in ascending order


def ordinal_to_yyyymmdd(ordinal):
    d = datetime.fromordinal(int(ordinal))
    return f"{d.year:04d}-{d.month:02d}-{d.day:02d}"


def find_nearest_pixel(xs, ys, x, y):
    """Return (pixel_index, dx, dy, distance) for the closest pixel in xs/ys."""
    dx = xs - x
    dy = ys - y
    dist_sq = dx * dx + dy * dy
    idx = int(np.argmin(dist_sq))
    return idx, float(xs[idx] - x), float(ys[idx] - y), float(np.sqrt(dist_sq[idx]))


def main():
    if len(sys.argv) < 2 or (len(sys.argv) - 2) % 2 != 0:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    hdf5_path = sys.argv[1]
    coord_args = sys.argv[2:]
    points = [(float(coord_args[i]), float(coord_args[i + 1]))
              for i in range(0, len(coord_args), 2)]

    print(f"\nHDF5 file: {hdf5_path}")

    with h5py.File(hdf5_path, 'r') as h5f:
        xs: np.ndarray = h5f['xs'][:]      # type: ignore[index]
        ys: np.ndarray = h5f['ys'][:]      # type: ignore[index]
        ts: np.ndarray = h5f['ts'][:]      # type: ignore[index]
        values_ds = h5f['values']          # type: ignore[index]
        n_t, n_bands, n_pixels = values_ds.shape  # type: ignore[misc]
        print(f"  {n_t} timesteps, {n_bands} bands, {n_pixels:,} pixels\n")

        # No points: dump every date in the file and exit
        if not points:
            print(f"=== All {n_t} dates in HDF5 ===")
            for ordinal in ts:
                print(f"    {ordinal_to_yyyymmdd(ordinal)}")
            print()
            return

        for x, y in points:
            print(f"=== Point ({x:.0f}, {y:.0f}) ===")
            idx, dx, dy, dist = find_nearest_pixel(xs, ys, x, y)
            actual_x = xs[idx]
            actual_y = ys[idx]
            print(f"  Nearest HDF5 pixel: index={idx}  (x={actual_x:.0f}, y={actual_y:.0f})")
            print(f"  Offset from query: dx={dx:.1f}m  dy={dy:.1f}m  distance={dist:.1f}m")

            # values_ds[:, SELECTION_BAND_INDEX, idx] reads one (n_t,) array —
            # all timesteps of the selection band for this pixel only.
            band_series: np.ndarray = values_ds[:, SELECTION_BAND_INDEX, idx]  # type: ignore[index]
            valid_mask = band_series < HDF5_NODATA
            valid_count = int(valid_mask.sum())
            print(f"  Valid readings: {valid_count} of {n_t}")

            if valid_count == 0:
                print(f"  (no valid readings)\n")
                continue

            valid_ords = ts[valid_mask]
            valid_vals = band_series[valid_mask]
            for ordinal, val in zip(valid_ords, valid_vals):
                print(f"    {ordinal_to_yyyymmdd(ordinal)}  band[{SELECTION_BAND_INDEX}]={int(val)}")
            print()


if __name__ == "__main__":
    main()
