# read file "C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\sentinel2_tiles_PT_32629.gpkg"
import geopandas as gpd
import os   

def main(): 
    # 1. Read the GeoPackage file
    gpkg_path = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\sentinel2_tiles_PT_32629.gpkg"
    gdf = gpd.read_file(gpkg_path)

    # 2. Extract tile names (assuming the tile name is in a column named 'tile_name')
    if 'Name' not in gdf.columns:
        raise ValueError("Expected column 'Name' not found in GeoPackage.")
    
    tile_names = gdf['Name'].unique().tolist()

    # create list of unique vallues of attribute 'Name' in the GeoPackage file and save to a text file, one per line
    if 'Name' not in gdf.columns:
        raise ValueError("Expected column 'Name' not found in GeoPackage.")     
    unique_names = gdf['Name'].unique().tolist()
    print(f"Unique values in 'Name' column: {unique_names}")

if __name__ == "__main__":    
    main()