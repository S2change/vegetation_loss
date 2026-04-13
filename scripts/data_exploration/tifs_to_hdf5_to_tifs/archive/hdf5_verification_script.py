import h5py
import os
import numpy as np
import rasterio
from rasterio.transform import from_origin

h5_filename = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\hdf5\T29TNE.h5" #  
h5_filename = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\hdf5\T29TPE.h5"


with h5py.File(h5_filename, 'r') as h5f:
    # 1. Check Structure
    print("--- HDF5 Structure ---")
    for key in h5f.keys():
        print(f"Dataset: {key:10} | Shape: {str(h5f[key].shape):20} | Type: {h5f[key].dtype}")
    
    # 2. Verify Sample Data
    # Let's grab the first image (index 0), first band (B3)
    # Shape of 'values' is (681, 4, 78110448)
    sample_band = h5f['values'][0, 0, :] 
    print(f"\nSample Band Mean: {np.mean(sample_band):.2f}")
    print(f"Sample Band Max:  {np.max(sample_band)}")
    
    # 3. Check Spatial Coordinates
    xs = h5f['xs'][:]
    ys = h5f['ys'][:]
    print(f"X range: {xs.min()} to {xs.max()}")
    print(f"Y range: {ys.min()} to {ys.max()}")
    
    # dates
    ts = h5f['ts'][:]
    print(f"t range: {ts.min()} to {ts.max()}") # ordinal dates

    # 4. (Optional) Export a check-image
    # To do this, we need to know the original width/height 
    # Let's assume you know the width/height from the previous run
    # (Using the logic: pixels = height * width)
    # If width was, say, 8838:
    # check_img = sample_band.reshape((height, width))