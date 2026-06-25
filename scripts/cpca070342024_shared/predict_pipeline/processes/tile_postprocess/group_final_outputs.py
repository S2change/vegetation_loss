import fiona
from pathlib import Path

run_name = "01_20230201_20230501_T5_A10"
parent_dir = Path("/users1/cpca070342024/shared/full_country_runs") / run_name
output_file = parent_dir / f"{run_name}.gpkg"

tile_dirs = sorted(
    d for d in parent_dir.iterdir()
    if d.is_dir() and d.name != "gpkg_files"
)

gpkg_files = []
missing = []
for tile_dir in tile_dirs:
    gpkg = tile_dir / "final_outputs" / f"{tile_dir.name}_tile.gpkg"
    if gpkg.exists():
        gpkg_files.append(gpkg)
    else:
        missing.append(str(gpkg))
        print(f"Missing: {gpkg}")

if not gpkg_files:
    raise FileNotFoundError("No gpkg files found.")

# Derive schema and CRS from the first file
with fiona.open(gpkg_files[0]) as src:
    schema = src.schema
    crs = src.crs
    driver = "GPKG"

print(f"Merging {len(gpkg_files)} file(s) into {output_file.name} ...")

total = 0
with fiona.open(output_file, "w", driver=driver, crs=crs, schema=schema) as dst:
    for gpkg in gpkg_files:
        with fiona.open(gpkg) as src:
            features = list(src)
            dst.writerecords(features)
            total += len(features)
            print(f"  {gpkg.parent.parent.name}: {len(features)} features")

print(f"\nDone. {total} total features written to {output_file}")
if missing:
    print(f"Skipped {len(missing)} missing file(s):")
    for m in missing:
        print(f"  {m}")
