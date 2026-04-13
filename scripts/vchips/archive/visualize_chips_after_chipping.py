import os
import random
import numpy as np
import matplotlib.pyplot as plt
import rasterio
from pathlib import Path
from PIL import Image
from matplotlib.colors import ListedColormap
from matplotlib.patches import Patch

'''
This script is designed to visualize a few samples of the training chips that we have generated, 
including the "before" and "after" imagery and the corresponding mask. It will display these in a grid format for easy comparison, 
and it will also show the unique values present in the mask to confirm that they are as expected (0-4 for our 5 classes).
Make sure to update the path to your training_data folder in the main block at the end of the script, 
and ensure that the folder structure and file naming conventions match what the script expects (i.e., "before" and "after" tif files in their respective folders, and mask png files in the label folder, all with matching stems).   
'''

def visualize_training_chips(base_path, num_samples=3):
    base_path = Path(base_path)
    label_dir = base_path / "label"
    before_dir = base_path / "before"
    after_dir = base_path / "after"

    # 1. Get all available chips
    all_labels = list(label_dir.glob("*.png"))
    if not all_labels:
        print("No chips found in the label directory!")
        return
    
    samples = random.sample(all_labels, min(num_samples, len(all_labels)))

    print(samples)

    # 2. Define our 5-class color palette
    # 0: Dark Gray, 1: Red, 2: Green, 3: Blue, 4: Yellow (Burned)
    colors = ['#2c3e50', '#e74c3c', '#2ecc71', '#3498db', '#f1c40f']
    cmap_5 = ListedColormap(colors)
    legend_elements = [Patch(facecolor=colors[i], label=f'Class {i}') for i in range(5)]

    # 3. Create the Plotting Grid
    fig, axes = plt.subplots(len(samples), 3, figsize=(15, 5 * len(samples)))
    
    # Handle the case where num_samples is 1 (axes becomes 1D)
    if len(samples) == 1:
        axes = np.expand_dims(axes, axis=0)

    for idx, lb_path in enumerate(samples):
        stem = lb_path.stem
        bef_path = before_dir / f"{stem}.tif"
        aft_path = after_dir / f"{stem}.tif"

        # Read Imagery (Natural Color: B4, B3, B2 are indices 3, 4, 5 in our new order)
        with rasterio.open(bef_path) as src_b, rasterio.open(aft_path) as src_a:
            # We read bands 3, 4, 5 (1-indexed) and transpose to (H, W, C)
            img_b = src_b.read([3, 4, 5]).transpose(1, 2, 0)
            img_a = src_a.read([3, 4, 5]).transpose(1, 2, 0)
        
        # Read Mask
        mask_data = np.array(Image.open(lb_path))

        # --- Column 1: Before ---
        axes[idx, 0].imshow(img_b)
        axes[idx, 0].set_title(f"Before: {stem}")
        axes[idx, 0].axis('off')

        # --- Column 2: After ---
        axes[idx, 1].imshow(img_a)
        axes[idx, 1].set_title("After")
        axes[idx, 1].axis('off')

        # --- Column 3: Mask ---
        im_m = axes[idx, 2].imshow(mask_data, cmap=cmap_5, vmin=0, vmax=4, interpolation='nearest')
        axes[idx, 2].set_title(f"Mask (Values: {np.unique(mask_data)})")
        axes[idx, 2].axis('off')
        
        # Add legend to the mask column only
        if idx == 0:
            axes[idx, 2].legend(handles=legend_elements, loc='upper left', 
                                bbox_to_anchor=(1.05, 1), title="Classes")

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    # Update this path to your training_data folder
    train_data_root = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data"
    visualize_training_chips(train_data_root, num_samples=4)