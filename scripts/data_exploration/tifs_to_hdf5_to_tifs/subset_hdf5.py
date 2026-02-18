"""
Create a subset of an HDF5 file with only the first 100 rows (y-dimension)

This script loads a large HDF5 file and creates a smaller version containing
only the first 100 rows along the y-axis, preserving all other dimensions
(time, band, x) and all metadata.

Usage:
    python subset_hdf5_h5py.py
"""

import h5py
import numpy as np

# Configuration
INPUT_HDF5 = r"E:\T29TQG\T29TQG_6bands.h5"  # UPDATE THIS PATH
OUTPUT_HDF5 = r"E:\T29TQG\smallfile_T29TQG_6bands.h5" # UPDATE THIS PATH
NUM_ROWS = 30

if __name__ == "__main__":
    print(f"Loading HDF5 file: {INPUT_HDF5}")

    # Open the HDF5 file
    with h5py.File(INPUT_HDF5, 'r') as h5f:
        # Print original structure
        print("\nOriginal HDF5 structure:")
        print(f"  Datasets: {list(h5f.keys())}")

        # Load datasets
        values = h5f['values']
        xs = h5f['xs'][:]
        ys = h5f['ys'][:]
        ts = h5f['ts'][:]

        # Check if original_timestamps exists
        if 'original_timestamps' in h5f:
            original_timestamps = h5f['original_timestamps'][:]
        else:
            original_timestamps = None

        # Get band names attribute if it exists
        if 'band_names' in h5f.attrs:
            band_names = h5f.attrs['band_names']
        else:
            band_names = None

        print(f"\nOriginal dimensions:")
        print(f"  values: {values.shape} (time, bands, pixels)")
        print(f"  xs: {xs.shape}")
        print(f"  ys: {ys.shape}")
        print(f"  ts: {ts.shape}")
        if original_timestamps is not None:
            print(f"  original_timestamps: {original_timestamps.shape}")

        # Determine grid dimensions
        # xs and ys are flattened, need to reconstruct the grid
        unique_xs = np.unique(xs)
        unique_ys = np.unique(ys)
        width = len(unique_xs)
        height = len(unique_ys)

        print(f"\nGrid dimensions: {height} rows x {width} columns")

        # Calculate pixels to keep (first NUM_ROWS)
        if NUM_ROWS > height:
            print(f"Warning: Requested {NUM_ROWS} rows but file only has {height} rows")
            NUM_ROWS = height

        # Subset to first NUM_ROWS
        print(f"\nSubsetting to first {NUM_ROWS} rows...")

        # Find which y-values to keep (top NUM_ROWS)
        sorted_unique_ys = np.sort(unique_ys)[::-1]  # Sort descending (top to bottom)
        ys_to_keep = sorted_unique_ys[:NUM_ROWS]

        # Create mask for pixels to keep
        pixel_mask = np.isin(ys, ys_to_keep)

        # Subset coordinates
        xs_subset = xs[pixel_mask]
        ys_subset = ys[pixel_mask]
        total_pixels_subset = len(xs_subset)

        print(f"Subset will have {total_pixels_subset} pixels ({NUM_ROWS} x {width})")

        # Save to new HDF5 file
        print(f"\nSaving subset to: {OUTPUT_HDF5}")
        with h5py.File(OUTPUT_HDF5, 'w') as h5f_out:
            # Create datasets with same structure
            dset_values = h5f_out.create_dataset(
                "values",
                (values.shape[0], values.shape[1], total_pixels_subset),
                dtype='uint16',
                chunks=(1, values.shape[1], min(1000000, total_pixels_subset))
            )

            # Copy band names attribute if it exists
            if band_names is not None:
                h5f_out.attrs['band_names'] = band_names

            # Create coordinate and time datasets
            h5f_out.create_dataset("xs", data=xs_subset, dtype='int32')
            h5f_out.create_dataset("ys", data=ys_subset, dtype='int32')
            h5f_out.create_dataset("ts", data=ts, dtype='int32')

            if original_timestamps is not None:
                h5f_out.create_dataset("original_timestamps", data=original_timestamps, dtype='int64')

            # Copy values for subset pixels (all times, all bands)
            print("Copying values for all timesteps...")
            for i in range(values.shape[0]):
                if (i + 1) % 10 == 0:
                    print(f"  Processing timestep {i+1}/{values.shape[0]}")
                # Read all bands for this timestep, then subset pixels
                dset_values[i, :, :] = values[i, :, pixel_mask]

        print("Done!")
        print(f"\nSubset dimensions:")
        print(f"  values: ({values.shape[0]}, {values.shape[1]}, {total_pixels_subset})")
        print(f"  xs: {total_pixels_subset}")
        print(f"  ys: {total_pixels_subset}")
        print(f"  ts: {len(ts)}")
