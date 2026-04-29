import cv2
import numpy as np
import matplotlib.pyplot as plt

# Using raw string for the Windows path
file_path = r"C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\investigacao-projectos-reviews-alunos-juris\projetos\DGT-S2CHANGE_2023\repos\vegetation_loss\scripts\bacdm\data\label\2019_10000032_2.png"

# Load image - IMREAD_UNCHANGED is vital for 0/1 mask files
img = cv2.imread(file_path, cv2.IMREAD_UNCHANGED)

if img is None:
    print(f"Error: Could not find or read the file at:\n{file_path}")
else:
    # 1. Get Basic Info
    height, width = img.shape[:2]
    total_pixels = img.size
    
    # 2. Calculate Distribution
    # This finds every unique value (e.g., 0 and 1) and counts them
    unique, counts = np.unique(img, return_counts=True)
    stats = dict(zip(unique, counts))

    # 3. Print Tabular Summary
    print(f"\nImage Analysis")
    print(f"{'='*40}")
    print(f"Dimensions: {width} x {height}")
    print(f"Total Pixels: {total_pixels:,}")
    print(f"{'-'*40}")
    print(f"{'Class Value':<15} | {'Pixel Count':<15} | {'Percentage'}")
    print(f"{'-'*40}")
    
    for value, count in stats.items():
        percentage = (count / total_pixels) * 100
        print(f"{value:<15} | {count:<15,} | {percentage:.2f}%")
    print(f"{'='*40}\n")

    # 4. Quick Visual Check
    # Since 0 and 1 are too dark to see, we use a colormap to visualize '1's
    plt.figure(figsize=(8, 6))
    plt.imshow(img, cmap='plasma') # 'plasma' makes 0=purple and 1=yellow
    plt.title("Spatial Distribution (Yellow = Class 1, Purple = Class 0)")
    plt.colorbar(ticks=[0, 1])
    plt.axis('off')
    plt.show()