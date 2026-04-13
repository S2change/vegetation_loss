import os
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.transform import from_origin
from rasterio.features import rasterize

'''
Next task. 
I have a hdf5 file for the same area of interest with the structure below where xs and ys are coordinates in CRS EPSG:32629, and 'ts' are ordinal dates 
I want to extract from the hdf5 file the values of all 6 bands for the pixels in the output raster we created before, where band 3 is 1. 
'''


def main():
    # --- Input Parameters ---
    GPKG_PATH = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\harmonized\BDR_expanded_v0.gpkg"
    OUTPUT_FOLDER = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\harmonized_to_tifs"
    FID_TO_PROCESS = 309
    D = 1000  # Note: Changed to 1000 for 1km x 1km as per your comment
    prefix = f"BDRexp_{FID_TO_PROCESS}"
    
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    print(f"Starting process for Id: {FID_TO_PROCESS}...")
    
    try:
        result = process_feature_to_raster(
            gpkg_path=GPKG_PATH,
            target_fid=FID_TO_PROCESS,
            output_dir=OUTPUT_FOLDER,
            prefix=prefix,
            extent_size=D, 
        )
        
        if result:
            print(f"Successfully created: {os.path.basename(result)}")
            
    except Exception as e:
        import traceback
        traceback.print_exc() # Better for debugging DLL/Env issues

def process_feature_to_raster(gpkg_path, target_fid, output_dir, prefix, extent_size, res=10):
    """
    Reads a specific feature, reprojects to EPSG:32629, and generates a 3-band raster
    selecting ONLY pixels totally contained within the feature.
    """
    TARGET_CRS = "EPSG:32629"

    # 1. Load Data
    gdf = gpd.read_file(gpkg_path)
    
    # Select feature by Id
    selected_feat = gdf[gdf['Id'] == target_fid]
    if selected_feat.empty:
        print(f"Error: Id {target_fid} not found.")
        return None

    # --- REPROJECTION ---
    selected_feat = selected_feat.to_crs(TARGET_CRS)
    feature = selected_feat.iloc[0]
    geom = feature.geometry

    # --- INTERIOR PIXEL LOGIC ---
    # We apply a tiny negative buffer (e.g., 0.1m) to ensure the 
    # geometry is strictly 'inside' and set all_touched=False.
    # This ensures only pixels whose center is inside the boundary are picked.
    clean_geom = geom.buffer(-res/2) # Buffer by half the resolution to ensure we only get pixels fully inside
    if clean_geom.is_empty:
        # Fallback if the feature is smaller than a pixel
        clean_geom = geom

    # 2. Date Formatting
    d0 = pd.to_datetime(feature['Data0'])
    d1 = pd.to_datetime(feature['Data1'])
    d0_int = int(d0.strftime('%Y%m%d'))
    d1_int = int(d1.strftime('%Y%m%d'))

    # 3. Calculate Spatial Extent
    centroid = geom.centroid
    left = int(centroid.x - (extent_size / 2))
    right = int(centroid.x + (extent_size / 2))
    bottom = int(centroid.y - (extent_size / 2))
    top = int(centroid.y + (extent_size / 2))

    # 4. Raster Setup
    width = int((right - left) / res)
    height = int((top - bottom) / res)
    transform = from_origin(left, top, res, res)
    
    out_name = f"{prefix}_{d0_int}_{left}_{right}_{bottom}_{top}.tif"
    out_path = os.path.join(output_dir, out_name)

    # 5. Generate Band Arrays
    # IMPORTANT: all_touched=False ensures only pixels with centers 
    # inside the polygon are selected.
    mask = rasterize(
        [(clean_geom, 1)],
        out_shape=(height, width),
        transform=transform,
        fill=0,
        all_touched=False, 
        dtype='uint32'
    )

    band1 = np.where(mask == 1, d0_int, 65535).astype('uint32')
    band2 = np.where(mask == 1, d1_int, 65535).astype('uint32')

    # 6. Write GeoTIFF
    profile = {
        'driver': 'GTiff',
        'height': height,
        'width': width,
        'count': 3,
        'dtype': 'uint32',
        'crs': TARGET_CRS,
        'transform': transform,
        'nodata': 65535,
        'compress': 'lzw'
    }

    with rasterio.open(out_path, 'w', **profile) as dst:
        dst.write(band1, 1) 
        dst.write(band2, 2) 
        dst.write(mask, 3)   

    return out_path

if __name__ == "__main__":
    main()