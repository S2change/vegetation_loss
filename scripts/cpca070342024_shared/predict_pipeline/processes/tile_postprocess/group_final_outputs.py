"""Merge every tile's final detection .gpkg in a run into one combined .gpkg.

Walks each <parent_dir>/<TILE_ID>/final_outputs/<TILE_ID>_tile.gpkg, reads the
"detections" layer, and concatenates them into <parent_dir>/<run_name>.gpkg.

Config via environment (set by run_group_slurm.sh / submit_tiles_batch.sh):
  GROUP_PARENT_DIR  (required) the run's parent dir — i.e. the batch's
                    BASE_OUTPUT_DIR, which holds one subdir per tile.
  GROUP_RUN_NAME    (optional) base name for the merged file; defaults to the
                    parent dir's own name. Output is <parent_dir>/<run_name>.gpkg.

Runnable standalone too: `GROUP_PARENT_DIR=/path/to/run python group_final_outputs.py`.
"""
import os

import geopandas as gpd
import pandas as pd
from pathlib import Path

parent_dir = Path(os.environ["GROUP_PARENT_DIR"])
run_name = os.environ.get("GROUP_RUN_NAME") or parent_dir.name
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

print(f"Merging {len(gpkg_files)} file(s) into {output_file.name} ...")

# Read each tile's "detections" layer (the layer aggregate_tile.py writes) and
# concat. pd.concat aligns columns by name, so tiles that differ in optional
# columns (e.g. `confidence` only present on OUTPUT_CONFIDENCE runs) merge
# cleanly with NaN fill — mirrors how aggregate_tile.py concatenates blocks.
gdfs = []
total = 0
for gpkg in gpkg_files:
    g = gpd.read_file(str(gpkg), layer="detections")
    gdfs.append(g)
    total += len(g)
    print(f"  {gpkg.parent.parent.name}: {len(g)} features")

merged = gpd.GeoDataFrame(
    pd.concat(gdfs, ignore_index=True),
    geometry="geometry",
    crs=gdfs[0].crs,
)
merged.to_file(output_file, layer="detections", driver="GPKG")

print(f"\nDone. {total} total features written to {output_file}")
if missing:
    print(f"Skipped {len(missing)} missing file(s):")
    for m in missing:
        print(f"  {m}")
