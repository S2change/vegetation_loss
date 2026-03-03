import os
import rasterio

folder_path = r"D:\s2_images\T29TNE"

files = [f for f in os.listdir(folder_path) if f.endswith('.tif')]

if not files:
    print("No .tif files found in the directory.")
    exit()

largest_file, largest_area, largest_bounds = files[0], 0, None
for f in files:
    with rasterio.open(os.path.join(folder_path, f)) as src:
        b = src.bounds
        area = (b.right - b.left) * (b.top - b.bottom)
    if area > largest_area:
        largest_area = area
        largest_file = f
        largest_bounds = b

print(os.path.join(folder_path, largest_file))
print(f"  left={largest_bounds.left:.1f}  right={largest_bounds.right:.1f}  bottom={largest_bounds.bottom:.1f}  top={largest_bounds.top:.1f}")
