
r'''
Let's create a script step by step. 
The overall goal is to read 4 by 4 km2 'before' and 'after' 6-band 16-bit geotiff and mask geotiff with classes 0,1,2,... and create 256*256 pixel chips, with before and after 8-bit tif files and a png 'label' file that corresponds to the mask. 
The first function in main will have a loop to read through all inputs in C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\vchips. 
The input files have names like 
*) "C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\vchips\source_rasters\vchip_680435_4497955_20200704_after.tif"
*) "C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\vchips\source_rasters\vchip_680435_4497955_20200704_beforetif"
*) "C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\vchips\mask_rasters\vchip_680435_4497955_20200704_mask.tif"

Let's apply now the transfor to the 'before' and 'after' file. I want to  convert them to 8-bit tif files with the same bands. The original band order is b3, b4, b8, b12, b2, b11, but I want the bands to be re-ordered as b12, b11, b8, b4, b3, b2. The original geotiff have NoData=65535 and the new tif should have NoData=255. To quantize from 16-bit to 8-bit, I want to apply a q0.02-q0.98 rescaling, for each image independently, so that the 2% darkest image are transform into (0,0,0,0,0,0) and the 2% brightest images are converted into (254,254,254,254,254,254), since 255 is reserved for NoData. 
The mask input does not need to be transformed since it is already in 8-bit format (the labels are small numbers starting at 0).
All files in the triplet  'before', 'after' and 'mask' need to be cropped into 256*256 aligned chips (2560 m by 2560 m). There should be an input N that determines how many chips we create. WE can  start by creating a function that works for N=4 and later replace it by a more general function if we want. The output chip stem names should end with _01, _02, _03, _04. For N=4, the 4 chips should include respectively the 4 corners of the original 4 by 4 km input to minimize overlap.
The output 'befofre' and 'after' files should be saved as tif files in folders "C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\before" and "C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\after" respectively.
The ouput mask files should be saved in png format in folder C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\label
Now, all output file names for the same 256*256 chip should have exactly the same stem. They are identified as 'before', 'after' and 'label'  just by the folder they belong to as usual to be accessed later by a dataloader.
'''

NODATA_hdf5 = 65535
NODATA_mask=255

import os
import random
import numpy as np
import rasterio
from rasterio.windows import Window
from pathlib import Path
from PIL import Image

def get_file_pairs(base_dir):
    source_dir = Path(base_dir) / "source_rasters"
    mask_dir = Path(base_dir) / "mask_rasters"
    after_files = list(source_dir.glob("*_after.tif"))
    triplets = []
    
    for after_path in after_files:
        prefix = after_path.name.replace("_after.tif", "")
        # Assuming standard .tif extension for 'before'
        before_path = source_dir / f"{prefix}_before.tif"
        mask_path = mask_dir / f"{prefix}_mask.tif"
        
        if before_path.exists() and mask_path.exists():
            triplets.append({'id': prefix, 'before': str(before_path), 'after': str(after_path), 'mask': str(mask_path)})
    return triplets

def process_triplet(triplet, output_base, num_chips=4):
    # Target: [b12(3), b11(5), b8(2), b4(1), b3(0), b2(4)]
    REORDER_IDX = [3, 5, 2, 1, 0, 4] 
    
    with rasterio.open(triplet['before']) as src_b, \
         rasterio.open(triplet['after']) as src_a, \
         rasterio.open(triplet['mask']) as src_m:
        
        h, w = src_b.height, src_b.width
        offsets = [(0, 0), (w - 256, 0), (0, h - 256), (w - 256, h - 256)]

        # Loop through the 4 corners to create chips
        for i in range(num_chips):
            win = Window(offsets[i][0], offsets[i][1], 256, 256)
            chip_stem = f"{triplet['id']}_{str(i+1).zfill(2)}"

            # --- VALIDATION FIRST ---
            # Check for unexpected values in the mask (e.g., 255 which is our nodata value for imagery, but should not be present in the mask)
            mask_chip = src_m.read(1, window=win)
            # also check for nodata value (65535) in scr_a and scr_b
            before_chip = src_b.read(window=win)
            after_chip = src_a.read(window=win) 
            if np.any(mask_chip == NODATA_mask) or np.any(before_chip == NODATA_hdf5) or np.any(after_chip == NODATA_hdf5):
                print(f"  [Skip] {chip_stem}: Mask contains invalid value 255 or imagery contains nodata value 65535, skipping this chip to avoid issues in training.")
                continue

            # Calculate which classes are present in mask to inform our sampling strategy (e.g., we want to make sure to include chips with rare classes more frequently, and we can afford to skip some chips that only contain the background class 0 to reduce noise in training)
            unique_vals = np.unique(mask_chip)
            has_rare_class = any(val in [1, 2, 3] for val in unique_vals)
            has_burn = 4 in unique_vals

            # SAMPLING LOGIC:
            # 1. If it has Class 1, 2, or 3: ALWAYS save it.
            # 2. If it only has Class 4: Save it.
            # 3. If it ONLY has Class 0: Save it only 10% of the time (to reduce background noise).
            if not has_rare_class and not has_burn:
                if random.random() > 0.10: # Skip 90% of empty chips
                    print(f"  [Skip] {chip_stem}: Only background class 0, skipping to reduce noise.")
                    continue

            # --- PROCESS IMAGERY ---
            for mode, src in [('before', src_b), ('after', src_a)]:
                data = src.read([idx + 1 for idx in REORDER_IDX], window=win).astype(np.float32)
                nodata_mask = (data == 65535)
                data[nodata_mask] = np.nan
                
                final_stack = np.zeros_like(data, dtype=np.uint8)
                for b in range(6):
                    q02, q98 = np.nanpercentile(data[b], [2, 98])
                    denom = q98 - q02 if q98 > q02 else 1.0
                    scaled = np.clip((data[b] - q02) / denom * 254, 0, 254)
                    scaled[nodata_mask[b]] = 255
                    final_stack[b] = scaled.astype(np.uint8)

                out_path = output_base / mode / f"{chip_stem}.tif"
                out_path.parent.mkdir(parents=True, exist_ok=True)
                
                meta = src.meta.copy()
                meta.update({"driver": "GTiff", "height": 256, "width": 256, "count": 6, 
                             "dtype": 'uint8', "nodata": 255, "transform": src.window_transform(win)})
                
                with rasterio.open(out_path, 'w', **meta) as dst:
                    dst.write(final_stack)

            # --- SAVE MASK ---
            label_path = output_base / "label" / f"{chip_stem}.png"
            label_path.parent.mkdir(parents=True, exist_ok=True)
            Image.fromarray(mask_chip.astype(np.uint8), mode='L').save(label_path)

def main():
    input_root = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\vchips"
    output_root = Path(r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data")
    
    triplets = get_file_pairs(input_root)
    print(f"Processing {len(triplets)} triplets...")
    for triplet in triplets:
        process_triplet(triplet, output_root)
    print("Chipping complete.")

if __name__ == "__main__":
    main()