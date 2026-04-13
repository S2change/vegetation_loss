import os
import rasterio
import numpy as np
from pathlib import Path

def get_active_prediction_rankings(pred_folder):
    pred_path = Path(pred_folder)
    tif_files = list(pred_path.glob("*.tif"))
    
    rankings = []
    empty_count = 0

    print(f"Analyzing {len(tif_files)} prediction files...")

    for fpath in tif_files:
        try:
            with rasterio.open(fpath) as src:
                data = src.read(1)
                
                # Count pixels where class is 1, 2, 3, or 4
                change_pixel_count = np.count_nonzero(data > 0)
                
                if change_pixel_count > 0:
                    rankings.append({
                        'filename': fpath.name,
                        'change_count': int(change_pixel_count),
                        'percent': (change_pixel_count / data.size) * 100
                    })
                else:
                    empty_count += 1
                    
        except Exception as e:
            print(f"Error reading {fpath.name}: {e}")

    # Sort descending by change_count
    sorted_rankings = sorted(rankings, key=lambda x: x['change_count'], reverse=True)

    print("\n--- Predictions Ranked by Activity (Excluding Empty) ---")
    print(f"{'Filename':<45} | {'Pixels > 0':<12} | {'% Coverage'}")
    print("-" * 75)

    for item in sorted_rankings:
        print(f"{item['filename']:<45} | {item['change_count']:>12,} | {item['percent']:>8.2f}%")
    
    print("\n" + "="*30)
    print(f"Total Files Scanned: {len(tif_files)}")
    print(f"Active Files (Shown): {len(sorted_rankings)}")
    print(f"Empty Files (Hidden): {empty_count}")
    print("="*30)
        
    return sorted_rankings

if __name__ == "__main__":
    folder = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\predictions"
    active_list = get_active_prediction_rankings(folder)
    N=20
    print(f"\nLow {N} Active Predictions:")
    for i, item in enumerate(active_list[-N:]):
        print(f"{i+1}. {item['filename']} - {item['change_count']} pixels")