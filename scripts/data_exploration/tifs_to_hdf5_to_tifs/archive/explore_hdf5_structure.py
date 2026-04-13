import h5py
import numpy as np
import os
from datetime import datetime, timezone
import re

import rasterio

CHECK_TIF_DATE = True  # Set to True to test date extraction from a TIFF filename
DESCRIBE_hdf5_FILE = True  # Set to False to skip structure printing and go directly to slicing
SLICE_FILE = False # Set to False to skip slicing and just describe the file structure / not working
DESCRIBE_NPY_FILE = True  # Set to True if you want to load and print dates from the .npy file

# --- Configuration ---
folder_input_path = r"H:\outputs_ROI\hdf5\T29TNE"
folder_output_path = r"C:\Users\mlc\Downloads"
input_file_2024= "s2_images-NDVI_XX999YM1NOBS6LDA2ITER1000_START20170408_END20241229_ROINAV.h5"
input_file_2025 = "s2_images-NDVI_XX999YM1NOBS6LDA2ITER1000_START20170408_END20251117_ROI_DGT_mask.h5"
input_npy_file="tif_dates_ord.npy" # check_or_initialize_file() saves this file with the dates in ordinal format, so we can load and check the dates without needing to read the TIFF files again. This is useful for debugging and verifying that the date extraction logic is working correctly.
output_file = "Test_NDVI_Reduced_2017_to_20240831.h5"
cutoff_date = datetime(2024, 8, 31)
tif_file="S2SR_image_1491651247967.tif" # Example file to test date parsing
tif_folder=r"H:\s2_images\T29TNE"

#hdf5 files
in_path_2024 = os.path.join(folder_input_path, input_file_2024)
in_path_2025 = os.path.join(folder_input_path, input_file_2025)
out_path = os.path.join(folder_output_path, output_file)
# tif file
tif_path = os.path.join(tif_folder, tif_file)

def determine_shape_of_tif(tif_path):
    """Determines the shape of a TIFF file using rasterio."""
    with rasterio.open(tif_path) as src:
        return src.shape  # returns (height, width)

def extract_date_from_tif_filename(filename):
    """Extracts the date from a TIFF filename in the format 'S2SR_image_YYYYMMDDHHMMSS.tif'."""
    pattern = re.compile(r'^S2SR_image_(\d{13})\.tif$')
    match = pattern.match(filename)
    if match:
        date_str = match.group(1)
        timestamp_ms = int(date_str)
        timestamp_sec = timestamp_ms / 1000
        #date_obj = datetime.utcfromtimestamp(timestamp_sec)
        date_obj = datetime.fromtimestamp(timestamp_sec, timezone.utc) # mlc 16 FEB 2026
        return timestamp_sec, date_obj
    else:
        raise ValueError(f"Filename '{filename}' does not match the expected pattern.")

def load_dates(npy_path):
    """Loads the dates from a .npy file and returns them as a list of datetime objects."""
    raw_dates = np.load(npy_path)
    date_list = []
    for d in raw_dates:
        # assuming dates are large integers in milliseconds since 1 january 1970, convert to datetime       
        #date_obj = datetime.utcfromtimestamp(d / 1000.0)      
        #date_obj = datetime.fromtimestamp(d / 1000.0)
        date_list.append(d)
    return date_list # Return first 20 dates

def print_structure(name, obj):
    """Callback function to print the name of each object in the file."""
    indent = "  " * name.count('/')
    if isinstance(obj, h5py.Group):
        print(f"{indent}Group: {name}")
    elif isinstance(obj, h5py.Dataset):
        print(f"{indent}Dataset: {name} (Shape: {obj.shape}, Type: {obj.dtype})")
        # Print attributes if they exist
        for attr_name, attr_val in obj.attrs.items():
            print(f"{indent}    Attr -> {attr_name}: {attr_val}")

def describe_file_structure(file_path):
    """Opens the HDF5 file and prints its structure."""
    with h5py.File(file_path, 'r') as f:
        print(f"Describing file: {file_path}")
        f.visititems(print_structure)

def slice_hdf5_by_date(in_path, out_path, cutoff_date):
    '''
    Slices the input HDF5 file based on a cutoff date and saves the result to a new file.
    
    :param in_path: Path to the input HDF5 file
    :param out_path: Path to the output HDF5 file
    :param cutoff_date: The cutoff date (datetime object) up to which data is retained
    '''
    with h5py.File(in_path, 'r') as src, h5py.File(out_path, 'w') as dst:
        # 1. Identify the Time/Date dataset
        # Change 'dates' to the actual name found in your file (e.g., 'time', 'timestamps')
        date_ds_name = 'dates' 
        dates_raw = src[date_ds_name][:]
        
        # 2. Convert raw dates to datetime objects to find the cutoff index
        # Note: Adjust decoding if dates are stored as strings or different integers
        # This example assumes YYYYMMDD integer format common in S2 products
        cutoff_idx = 0
        for i, d in enumerate(dates_raw):
            # Example conversion: if 20240831 is stored as an integer
            d_obj = datetime.strptime(str(int(d)), '%Y%m%d') 
            if d_obj <= cutoff_date:
                cutoff_idx = i + 1 # Include the cutoff date
            else:
                break

        print(f"Slicing file at index {cutoff_idx} (Total slices remaining: {cutoff_idx})")

        # 3. Iterate through all objects and copy/slice
        for name, obj in src.items():
            if isinstance(obj, h5py.Dataset):
                # Check if the dataset has a time dimension (it matches the original dates length)
                if obj.shape[0] == len(dates_raw):
                    # SLICE the data (Time, Y, X) -> (0:cutoff_idx, Y, X)
                    data_subset = obj[0:cutoff_idx, ...]
                    dst.create_dataset(name, data=data_subset, compression="gzip", chunks=True)
                else:
                    # Copy other datasets (like lat/lon/metadata) as they are
                    src.copy(name, dst)
                
                # Copy Attributes (CRS, scale factors, etc.)
                for attr_name, attr_val in obj.attrs.items():
                    dst[name].attrs[attr_name] = attr_val
                    
            elif isinstance(obj, h5py.Group):
                # If your file has nested groups, copy the structure
                src.copy(name, dst)

    print(f"Successfully created: {out_path}")

if CHECK_TIF_DATE:
    try:
        timestamp, tif_date = extract_date_from_tif_filename(tif_file)
        print(f"Extracted date from '{tif_file}': {timestamp}, {tif_date}") # Extracted date from 'S2SR_image_1491651247967.tif': 2017-04-08 11:34:07.967000+00:00
    except ValueError as e:
        print(e)

    print(f"tiff shape: {determine_shape_of_tif(tif_path)}")

if DESCRIBE_NPY_FILE:
    print("###############")
    npy_path = os.path.join(folder_input_path, input_npy_file)
    dates = load_dates(npy_path)
    print("Loaded Dates from .npy file:")
    print(f'Length: {len(dates)}, Dates: {dates[:2]}...{dates[-2:]}')  # Print first and last 2 dates

if DESCRIBE_hdf5_FILE:
    print("###############")
    describe_file_structure(in_path_2024)
    print("###############")
    describe_file_structure(in_path_2025)

if SLICE_FILE:
    slice_hdf5_by_date(in_path_2025, out_path, cutoff_date)  # it's not working: needs a date
    if DESCRIBE_hdf5_FILE:
        describe_file_structure(out_path)

