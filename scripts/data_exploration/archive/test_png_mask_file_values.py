import os

from PIL import Image
import numpy as np

'''
This script is a simple test to check the unique values present in one of the mask files generated from our chipping process.

This is important to confirm that the mask values are as expected (0-4 for our 5 classes) and that there are no unexpected values 
(like 255 which we use for nodata in the imagery) in the mask files, 
which could indicate an issue in the chipping process or in the way the masks were generated.

Make sure to update the path to the mask file you want to test in the "test_file" variable below, 
and ensure that the file exists at that location before running the script.
'''


# Pick one of your generated mask chips
test_folder = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\label"

# List all files in the test folder and pick one (you can also specify a specific file if you want)
test_files = [f for f in os.listdir(test_folder) if f.lower().endswith('.png')]
if not test_files:  
    print(f"No PNG files found in {test_folder}. Please check the path and ensure there are mask files present.")

# I want all of them to compute the overalll distributiion of values across all mask files
# initialize distribution: values range between 0 and 4 for our 5 classes, so we can use a dictionary to count occurrences of each class across all mask files
value_distribution = {0: 0, 1: 0, 2: 0, 3: 0, 4: 0}
for test_file in test_files:
    img = Image.open(os.path.join(test_folder, test_file))
    data = np.array(img)
    # Count occurrences of each value in the current mask file
    for value in np.unique(data):
        if value in value_distribution:
            value_distribution[value] += 1


print(f"Value Distribution Across All {len(test_files)} Mask Files:")
for value, count in value_distribution.items():
    print(f"Value {value}: {count} masks") 