import geopandas as gpd
from collections import defaultdict

features_path = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\harmonized\Data_ref_2023_nvg_v2.gpkg"
tiles_path    = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\sentinel2_tiles_PT_32629.gpkg"

features = gpd.read_file(features_path)
tiles    = gpd.read_file(tiles_path)

print(f"Features: {len(features)} | Tiles: {len(tiles)}")
print(f"Features CRS: {features.crs} | Tiles CRS: {tiles.crs}")

# Spatial join: keep only features whose geometry is fully within a tile
joined = gpd.sjoin(features, tiles[['Name', 'geometry']], how='inner', predicate='within')

tile_counts = defaultdict(int)
for name in joined['Name']:
    tile_counts[name] += 1

tile_counts = dict(sorted(tile_counts.items()))

print(f"\nTiles that fully contain at least one feature ({len(tile_counts)} tiles):")
for name, count in tile_counts.items():
    print(f"  {name}: {count} feature(s)")

N = 5
filtered = [name for name, count in tile_counts.items() if count >= N]
print(f"\nTiles with at least {N} features ({len(filtered)} tiles):")
print("  '" + "', '".join(filtered) + "'")
