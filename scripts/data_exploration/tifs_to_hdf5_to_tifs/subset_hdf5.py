"""
Create a subset of an HDF5 file with only the first 100 rows (y-dimension)

This script loads a large HDF5 file and creates a smaller version containing
only the first 100 rows along the y-axis, preserving all other dimensions
(time, band, x) and all metadata.

Usage:
    python subset_hdf5.py
"""

import xarray as xr
import sys

# Configuration
INPUT_HDF5 = r"/path/to/your/large_file.h5"  # UPDATE THIS PATH
OUTPUT_HDF5 = r"/path/to/your/subset_file.h5"  # UPDATE THIS PATH
NUM_ROWS = 100

if __name__ == "__main__":
    print(f"Loading HDF5 file: {INPUT_HDF5}")

    # Open the HDF5 file
    ds = xr.open_dataset(INPUT_HDF5, chunks='auto')

    print(f"Original shape: {ds}")
    print(f"\nOriginal dimensions:")
    for dim, size in ds.dims.items():
        print(f"  {dim}: {size}")

    # Subset to first 100 rows (y-dimension)
    print(f"\nSubsetting to first {NUM_ROWS} rows...")
    ds_subset = ds.isel(y=slice(0, NUM_ROWS))

    print(f"\nSubset dimensions:")
    for dim, size in ds_subset.dims.items():
        print(f"  {dim}: {size}")

    # Save to new HDF5 file
    print(f"\nSaving subset to: {OUTPUT_HDF5}")
    ds_subset.to_netcdf(OUTPUT_HDF5, format='NETCDF4', engine='h5netcdf')

    print("Done!")

    # Close datasets
    ds.close()
    ds_subset.close()
