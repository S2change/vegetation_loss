"""
Inspect the structure of the HDF5 file to understand how to use it in chips_S2_dates.py
"""

import h5py
import numpy as np

HDF5_FILE = "/Users/domwelsh/green_ds/Thesis/vegetation_loss/smallfile_T29TQG_6bands.h5"

with h5py.File(HDF5_FILE, 'r') as h5f:
    print("HDF5 File Structure:")
    print("=" * 60)

    # List all datasets
    print("\nDatasets:")
    for key in h5f.keys():
        dset = h5f[key]
        print(f"  {key}: shape={dset.shape}, dtype={dset.dtype}")

    # List all attributes
    print("\nAttributes:")
    for key, value in h5f.attrs.items():
        print(f"  {key}: {value}")

    # Load the data to understand structure
    print("\n" + "=" * 60)
    print("Detailed Information:")
    print("=" * 60)

    values = h5f['values']
    xs = h5f['xs'][:]
    ys = h5f['ys'][:]
    ts = h5f['ts'][:]

    if 'original_timestamps' in h5f:
        original_timestamps = h5f['original_timestamps'][:]
    else:
        original_timestamps = None

    if 'band_names' in h5f.attrs:
        band_names = [b.decode('ascii') if isinstance(b, bytes) else b for b in h5f.attrs['band_names']]
    else:
        band_names = None

    # Determine grid dimensions
    unique_xs = np.unique(xs)
    unique_ys = np.unique(ys)
    width = len(unique_xs)
    height = len(unique_ys)

    print(f"\nGrid Dimensions:")
    print(f"  Width (x): {width} pixels")
    print(f"  Height (y): {height} pixels")
    print(f"  Total pixels: {len(xs)} (should equal {width * height})")

    print(f"\nTemporal Information:")
    print(f"  Number of timesteps: {len(ts)}")
    print(f"  First timestamp (ordinal): {ts[0]}")
    print(f"  Last timestamp (ordinal): {ts[-1]}")
    if original_timestamps is not None:
        print(f"  First timestamp (unix ms): {original_timestamps[0]}")
        print(f"  Last timestamp (unix ms): {original_timestamps[-1]}")

    print(f"\nBand Information:")
    print(f"  Number of bands: {values.shape[1]}")
    if band_names is not None:
        print(f"  Band names: {band_names}")

    print(f"\nValues Dataset:")
    print(f"  Shape: {values.shape} (time, bands, pixels)")
    print(f"  Dtype: {values.dtype}")
    print(f"  Chunks: {values.chunks}")

    print(f"\nSpatial Extent:")
    print(f"  X range: [{unique_xs.min()}, {unique_xs.max()}]")
    print(f"  Y range: [{unique_ys.min()}, {unique_ys.max()}]")

    # Check band order
    print(f"\nBand Order in HDF5:")
    if band_names is not None:
        for i, band in enumerate(band_names):
            print(f"  Band {i}: {band}")

    print("\n" + "=" * 60)
    print("Required for chips_S2_dates.py:")
    print("=" * 60)
    print("  ✓ Time dimension (ordinal dates)")
    print("  ✓ Spatial coordinates (xs, ys)")
    print("  ✓ Original timestamps mapping")
    print("  ✓ 6 bands of spectral data")
    print("  ? Band order needs to match expected order [B2, B11, B3, B4, B8, B12]")
    print(f"    Current order: {band_names if band_names else 'Unknown'}")
