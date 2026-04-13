import os
import numpy as np
from PIL import Image


# Define paths
label_dir = r'C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\testing_data\label'
pred_dir = r'C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\testing_data\predictions'

results = []

# Get all filenames from the label directory (stripping extension)
filenames = [os.path.splitext(f)[0] for f in os.listdir(label_dir) if f.endswith('.png')]

# Loop through each file, read the label and prediction, and calculate accuracy for non-background pixels
for name in filenames:
    label_path = os.path.join(label_dir, f"{name}.png")
    pred_path = os.path.join(pred_dir, f"{name}.tif")
    
    if not os.path.exists(pred_path):
        continue

    # Convert to numpy arrays
    label_arr = np.array(Image.open(label_path))
    pred_arr = np.array(Image.open(pred_path))

    # Mask for non-background pixels (where label != 0)
    mask = (label_arr != 0)
    total_non_bg_pixels = np.sum(mask) # This is your non-zero count
    
    if total_non_bg_pixels > 0:
        correct_pixels = np.sum((label_arr == pred_arr) & mask)
        accuracy = (correct_pixels / total_non_bg_pixels) * 100
    else:
        accuracy = 0.0 

    results.append({
        'filename': name, 
        'accuracy': accuracy, 
        'non_zero_pixels': total_non_bg_pixels
    })

# Sort results by accuracy descending
sorted_results = sorted(results, key=lambda x: x['accuracy'], reverse=True)

# Print the report
print(f"{'Filename':<25} | {'Accuracy (%)':<15} | {'Non-Zero Pixels':<15}")
print("-" * 60)
for item in sorted_results:
    print(f"{item['filename']:<25} | {item['accuracy']:>11.2f}% | {item['non_zero_pixels']:>15}")