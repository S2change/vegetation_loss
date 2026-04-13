import geopandas as gpd
import os

# --- Path Configuration ---
shapefile_path = r"C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\investigacao-projectos-reviews-alunos-juris\projetos\DGT-S2CHANGE_2023\partilhado\S2_tile_locations\sentinel2_tiles_PT_terra_tm06.shp"
output_folder = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5"
output_gpkg = os.path.join(output_folder, "sentinel2_tiles_PT_32629.gpkg")

# 1. Ensure the output directory exists
os.makedirs(output_folder, exist_ok=True)

print(f"Reading Shapefile...")
# 2. Load the shapefile
gdf = gpd.read_file(shapefile_path)

# 3. Reproject to EPSG:32629
print(f"Reprojecting to EPSG:32629...")
gdf_32629 = gdf.to_crs("EPSG:32629")

# 4. Save to GeoPackage
print(f"Saving to GeoPackage: {output_gpkg}")
gdf_32629.to_file(output_gpkg, driver="GPKG")

print("Success!")