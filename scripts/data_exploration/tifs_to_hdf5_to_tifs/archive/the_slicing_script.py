import h5py
import os
import numpy as np

folder_path = r'C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\temp\test_tif_to_hdf5'
h5_filename = os.path.join(folder_path, 'satellite_data_intersected.h5')
threshold_ordinal = 736450

with h5py.File(h5_filename, 'r') as h5f:
    # 1. Load the timestamps into memory (they are small)
    ts_array = h5f['ts'][:]
    
    # 2. Find indices where the date is less than your threshold
    # This creates a boolean mask [True, True, False, ...]
    mask = ts_array < threshold_ordinal
    indices = np.where(mask)[0]
    
    print(f"Total slices: {len(ts_array)}")
    print(f"Slices matching criteria: {len(indices)}")

    if len(indices) > 0:
        # 3. Slice the main dataset using the indices
        # This only pulls the matching 'Time' slices into RAM
        filtered_data = h5f['values'][indices, :, :]
        
        print(f"Filtered data shape: {filtered_data.shape}")
        # Result will be (Number of matches, 4, 78110448)
    else:
        print("No slices found matching that date criteria.")