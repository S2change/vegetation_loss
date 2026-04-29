
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
The output 'before' and 'after' files should be saved as tif files in folders "C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\before" and "C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\after" respectively.
The ouput mask files should be saved in png format in folder C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\label
Now, all output file names for the same 256*256 chip should have exactly the same stem. They are identified as 'before', 'after' and 'label'  just by the folder they belong to as usual to be accessed later by a dataloader.
'''

NODATA_hdf5 = 65535
NODATA_mask=255

import os
import random
import shutil
import numpy as np
import rasterio
from rasterio.windows import Window
from pathlib import Path
from PIL import Image

# spectral info
_10BANDS=True # from now on, we always train with 10 bands in vchips that come from Jesus

# for 10 bands, no re-ordering needed since they are already ordered by b12, b11, ..., b2
if not _10BANDS:
    # 6-band pipeline: reorder to [b12, b11, b8, b4, b3, b2]# was b3(0),b4(1),b8(2),b12(3),b2(4),b11(5)
    REORDER_IDX_6 = [3, 5, 2, 1, 0, 4]

# which folders to use to read vchips and write chips; set TRAIN to True to read from vchips\source_rasters and vchips\mask_rasters and write to training_data\before, training_data\after and training_data\label; set TRAIN to False to read from vchips\source_rasters_test and vchips\mask_rasters_test and write to testing_data\before, testing_data\after and testing_data\label
TEST_RATIO=0.3

# Set to True to quantize chips to 8-bit (q0.02-q0.98 stretch, NoData→255).
# Set to False to write chips in the same dtype as the input vchip (e.g. uint16),
# leaving the conversion to the dataloader (_to_uint8 in dataset_swin_GZ.py).
CONVERT_TO_8BIT = False

# working folder
input_root = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\vchips"
input_root = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\v_chips_v1_10bands"


def main():
    train_root = Path(r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data")
    test_root  = Path(r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\testing_data")

    for output_root in [train_root, test_root]:
        if output_root.exists():
            shutil.rmtree(output_root)
            print(f"Cleared {output_root}")

    triplets = get_file_pairs(input_root)
    print(f"Processing {len(triplets)} triplets...")
    for triplet in triplets:
        if random.random() < TEST_RATIO:
            print(f"  [Test] {triplet['id']}: Assigned to testing set.")
            output_root = test_root
        else:
            print(f"  [Train] {triplet['id']}: Assigned to training set.")
            output_root = train_root
        process_triplet(triplet, output_root)
    print("Chipping complete.")

def get_file_pairs(base_dir):
    mask_dir = Path(base_dir) / "mask_rasters"
    triplets = []

    source_dir  = Path(base_dir) / "source_rasters"
    after_files = list(source_dir.glob("*_after.tif"))
    for after_path in after_files:
        prefix = after_path.name.replace("_after.tif", "")
        before_path = source_dir / f"{prefix}_before.tif"
        mask_path   = mask_dir   / f"{prefix}_mask.tif"
        if before_path.exists() and mask_path.exists():
            triplets.append({'id': prefix, 'before': str(before_path),
                                'after': str(after_path), 'mask': str(mask_path)})
    return triplets

def process_triplet(triplet, output_base, num_chips=4):

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
            # Imagery and mask NoData pixels are both handled by the dataloader valid_mask,
            # which excludes them from the loss. Skip only if the entire mask chip is NoData
            # (no valid label pixels at all → nothing to learn from).
            mask_chip = src_m.read(1, window=win)
            if np.all(mask_chip == NODATA_mask):
                print(f"  [Skip] {chip_stem}: label mask is entirely NoData.")
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
                if _10BANDS:
                    data = src.read(window=win)
                else:
                    data = src.read([idx + 1 for idx in REORDER_IDX_6], window=win)

                n_bands = data.shape[0]

                if CONVERT_TO_8BIT:
                    data = data.astype(np.float32)
                    nodata_mask = (data == NODATA_hdf5)
                    data[nodata_mask] = np.nan
                    final_stack = np.zeros(data.shape, dtype=np.uint8)
                    for b in range(n_bands):
                        q02, q98 = np.nanpercentile(data[b], [2, 98])
                        denom = q98 - q02 if q98 > q02 else 1.0
                        scaled = np.clip((data[b] - q02) / denom * (NODATA_mask - 1), 0, NODATA_mask - 1)
                        scaled[nodata_mask[b]] = NODATA_mask
                        final_stack[b] = scaled.astype(np.uint8)
                    out_dtype = 'uint8'
                    out_nodata = NODATA_mask
                else:
                    final_stack = data          # keep original dtype (e.g. uint16)
                    out_dtype   = src.dtypes[0]
                    out_nodata  = src.nodata

                out_path = output_base / mode / f"{chip_stem}.tif"
                out_path.parent.mkdir(parents=True, exist_ok=True)

                meta = src.meta.copy()
                meta.update({"driver": "GTiff", "height": 256, "width": 256, "count": n_bands,
                             "dtype": out_dtype, "nodata": out_nodata,
                             "transform": src.window_transform(win)})

                with rasterio.open(out_path, 'w', **meta) as dst:
                    dst.write(final_stack)

            # --- SAVE MASK ---
            label_path = output_base / "label" / f"{chip_stem}.png"
            label_path.parent.mkdir(parents=True, exist_ok=True)
            Image.fromarray(mask_chip.astype(np.uint8), mode='L').save(label_path)


if __name__ == "__main__":
    main()