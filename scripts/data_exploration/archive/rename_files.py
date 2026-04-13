import os
import re

# Set your directory
target_dir = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\visual"

# Pattern to match: BDRexp_ (digits) _ (rest of the filename)
# We capture the "728" part and the "after_2021..." part
pattern = re.compile(r"BDRexp_(\d+)(_.*)")

files_renamed = 0

print(f"Starting rename in: {target_dir}")

for filename in os.listdir(target_dir):
    # Only process .tif and .qml files that match our specific naming convention
    if filename.startswith("BDRexp_") and (filename.endswith(".tif") or filename.endswith(".qml")):
        match = pattern.match(filename)
        
        if match:
            # group(1) is '728', group(2) is '_after_20210117.tif'
            feature_id = match.group(1)
            rest_of_name = match.group(2)
            
            # Construct the new name
            new_name = f"BDRexp_v0_{feature_id}_025{rest_of_name}"
            
            # Paths
            old_path = os.path.join(target_dir, filename)
            new_path = os.path.join(target_dir, new_name)
            
            # Rename the file
            os.rename(old_path, new_path)
            print(f"Renamed: {filename} -> {new_name}")
            files_renamed += 1

print(f"\nTask complete. Total files renamed: {files_renamed}")