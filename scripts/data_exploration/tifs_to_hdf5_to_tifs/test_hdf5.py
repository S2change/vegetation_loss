import h5py
import numpy as np

hdf5_path = r'C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\temp\test_tif_to_hdf5\test_1667647823345_6bands.h5'

with h5py.File(hdf5_path, 'r') as f:
    ts = f['ts'][:]
    values_ds = f['values']
    
    unique_ts = np.unique(ts)
    print(f"Unique Timestamps: {unique_ts}")
    
    # Take the first timestamp found
    t0 = unique_ts[0]
    indices = np.where(ts == t0)[0]
    print(f"Points for timestamp {t0}: {len(indices)}")
    
    # Check the actual values in the HDF5 for these points
    # Checking Band 0 (B2)
    sample_values = values_ds[0, 0, indices]
    
    nodata_count = np.count_nonzero(sample_values == 65535)
    valid_count = len(sample_values) - nodata_count
    
    print(f"Values Analysis for Timestamp {t0}:")
    print(f"  - Total points: {len(sample_values)}")
    print(f"  - NoData (65535) points: {nodata_count}")
    print(f"  - Valid (non-65535) points: {valid_count}")
    
    if valid_count > 0:
        real_data = sample_values[sample_values != 65535]
        print(f"  - Sample of real data: {real_data[:5]}")
        print(f"  - Max value in data: {np.max(real_data)}")
    else:
        print("  - ALERT: All values for this timestamp are 65535 in the HDF5 file!")

with h5py.File(hdf5_path, 'r') as f:
    print(f"xs shape: {f['xs'].shape}")
    print(f"ys shape: {f['ys'].shape}")
    print(f"ts shape: {f['ts'].shape}")
    print(f"values shape: {f['values'].shape}")