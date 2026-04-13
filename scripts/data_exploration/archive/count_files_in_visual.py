import os

# Define the root path
root_dir = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\visual"

def count_gpkg_files(base_path):
    if not os.path.exists(base_path):
        print(f"Error: The path {base_path} does not exist.")
        return

    print(f"{'Tile':<15} | {'Data Source':<20} | {'GPKG Count'}")
    print("-" * 50)

    # Iterate through Sentinel-2 tile folders
    count=0
    for tile in os.listdir(base_path):
        tile_path = os.path.join(base_path, tile)
        
        if os.path.isdir(tile_path):
            # Iterate through reference data source folders
            for source in os.listdir(tile_path):
                source_path = os.path.join(tile_path, source)
                
                if os.path.isdir(source_path):
                    # Count only files ending with .gpkg
                    gpkg_count = len([f for f in os.listdir(source_path) 
                                     if f.lower().endswith('.gpkg')])
                    count+=gpkg_count
                    
                    print(f"{tile:<15} | {source:<20} | {gpkg_count}")

    print(f"\nTotal GPKG files found: {count}")

if __name__ == "__main__":
    count_gpkg_files(root_dir)