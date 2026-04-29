# vchip_before_after_split

For every vchip mask in a directory, produces a `_before.tif` and `_after.tif` pair containing pre- and post-break Sentinel-2 composites on the same spatial grid as the vchip.

## What it does

1. Reads the bounding box of every HDF5 tile file in the HDF5 directory to build a tile index.
2. For each vchip, parses its coordinates and break date from the filename (`vchip_{x}_{y}_{YYYYMMDD}_mask.tif`) and looks up which tile covers it.
3. Groups vchips by tile so each HDF5 file is opened once.
4. For each vchip, selects up to `MAX_IMAGES_PER_PERIOD` Sentinel-2 timesteps on either side of the break date (within `TEMPORAL_WINDOW_DAYS`) and loads only those timesteps from HDF5.
5. Runs cascading compositing — for every pixel, picks the first non-NODATA observation from the selected timesteps.
6. Writes two GeoTIFFs per vchip, aligned pixel-for-pixel with the input vchip, containing the 10 spectral bands in descending order plus a band recording the date (YYYYMMDD) each pixel was sampled from.

Skips any vchip where both output files already exist, so it is safe to re-run after a partial failure.

## Requirements

- Python 3.10+
- `numpy`, `rasterio`, `h5py`

Input assumptions:
- Vchip filenames follow `vchip_{x}_{y}_{YYYYMMDD}_mask.tif`, where coordinates are in EPSG:32629.
- HDF5 tile files named `{tile_id}.h5` (e.g. `T29SMC.h5`), with datasets `xs`, `ys`, `ts`, `original_timestamps`, `values`, all coordinates in EPSG:32629, and values in ascending band order.

## Usage

Four positional arguments:

```bash
python vchip_before_after_split.py <vchip_dir> <hdf5_dir> <before_output_dir> <after_output_dir>
```
Output directories are created automatically if they don't exist.

## Output

Each vchip produces two GeoTIFFs:
- `{vchip_stem}_before.tif`
- `{vchip_stem}_after.tif`

Both files are `uint32`, with 11 bands in this order:

| Band | Description     |
|------|-----------------|
| 1    | B12             |
| 2    | B11             |
| 3    | B8A             |
| 4    | B8              |
| 5    | B7              |
| 6    | B6              |
| 7    | B5              |
| 8    | B4              |
| 9    | B3              |
| 10   | B2              |
| 11   | date_yyyymmdd   |

NODATA is 65535 (both for reflectance and date bands).

## Running on SLURM

Both `vchip_before_after_split.py` and the related `vchip_before_after_split_slurm.sh` files have been uploaded to the CACN server, in the directory `/users1/cpca070342024/shared/vchips_before_after_scripts`

In the .sh file, the 4 positional arguments are set up to specific paths. If these need to be changed, they should be adjusted in the .sh file.

```bash
# -------------------------------------------------------
VCHIP_DIR="/users1/cpca070342024/shared/vchips/masks_tif"
HDF5_DIR="/users1/dgt/hdf5"
BEFORE_OUTPUT_DIR="/users1/cpca070342024/shared/vchips_before_after_scripts/before_vchips"
AFTER_OUTPUT_DIR="/users1/cpca070342024/shared/vchips_before_after_scripts/after_vchips"
# -------------------------------------------------------
```

 There is also a venv set up in the same `/vchips_before_after_scripts directory`, which already has the required packages installed for this script.

 Once the directory paths are set in the .sh file, run `sbatch vchip_before_after_split_slurm.sh` to start running the Python script. There will be an output file with the format slurm-xxxxxx.out which will show outputs from the script running.

## Configuration inside the script

There are a few other configuration variables near the top of `vchip_before_after_split.py` which can be adjusted. I kept them in the file to be adjusted instead of command line arguments because I expect they might be changed once or twice, but then remain the same across all runs:

- `TEMPORAL_WINDOW_DAYS` (default 45): how many days before/after the break date to search for images.
- `MAX_IMAGES_PER_PERIOD` (default 9): cap on timesteps considered per side.
- `SELECTION_BAND_INDEX` (default 3 — B8/NIR): which band's NODATA value drives the cascading pick.
- `HDF5_NODATA` / `OUTPUT_NODATA` (default 65535): missing data value in both the input HDF5 file and the output tifs
