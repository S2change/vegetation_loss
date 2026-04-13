import rasterio
import numpy as np
from pathlib import Path
from collections import Counter
import tqdm

def analyze_masks(mask_folder):
    mask_path = Path(mask_folder)
    mask_files = list(mask_path.glob("*.tif"))
    
    if not mask_files:
        print(f"No .tif files found in {mask_folder}")
        return

    total_counts = Counter()
    total_pixels = 0
    
    print(f"Analyzing {len(mask_files)} mask files...")
    
    for fpath in tqdm.tqdm(mask_files):
        try:
            with rasterio.open(fpath) as src:
                data = src.read(1)
                values, counts = np.unique(data, return_counts=True)
                total_counts.update(dict(zip(values, counts)))
                total_pixels += data.size
        except Exception as e:
            print(f"Error reading {fpath.name}: {e}")
            
    print("\n" + "="*40)
    print("      MASK VALUE DISTRIBUTION")
    print("="*40)
    
    # Sort by value (0, 1, 2...)
    for val in sorted(total_counts.keys()):
        count = total_counts[val]
        percentage = (count / total_pixels) * 100
        
        # Highlight 255 if it exists
        alert = " <-- ALERT: INVALID DATA" if val == 255 else ""
        
        print(f"Value {val:>3}: {count:>12,} pixels ({percentage:>6.2f}%){alert}")
    print("="*40)

if __name__ == "__main__":
    folder = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\vchips\mask_rasters"
    analyze_masks(folder)