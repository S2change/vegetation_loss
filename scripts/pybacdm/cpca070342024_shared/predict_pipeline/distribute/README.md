# Full Prediction Pipeline Run

Submitting the submit_tile.sh file with the required inputs will run the entire prediction pipeline for one HDF5 file

**Process:**
  1. Read the tile's chip-chunked HDF5 to discover the block grid shape
     (N_BLOCK_ROWS x N_BLOCK_COLS).
  2. Submit an array job — one task per (block_row, block_col) — that
     runs `predict_block.py` and writes one .npz + .gpkg per block into
     OUTPUT_DIR/block_outputs/.
  3. Submit an aggregator job with `--dependency=afterok:<array_job>` that
     stitches the per-block shards into tile-level outputs in
     OUTPUT_DIR/final_outputs/ (.gpkg .parquet .npz .tif).

### Example submission
In this example subission, the first 5 inputs (TILE_ID, TILE_HDF5, OUTPUT_DIR, START_DATE, END_DATE) are the required inputs that need to be added for every run. The last 4 inputs (MAX_COMPOSITE_DAYS, BLOCK_ROWS, BLOCK_COLS, WRITE_COMPOSITE_TIFS) are optional inputs. This example submission is a submission used for testing only a few blocks from a small HDF5 file. Unless doing a very specific run, only the required inputs are needed when processing a full tile.

```
/users1/cpca070342024/shared/vegetation_loss/scripts/pybacdm/cpca070342024_shared/predict_pipeline/distribute/submit_tile.sh \
    TILE_ID=T29TPE \
    TILE_HDF5_PATH=/users1/cpca070342024/shared/vegetation_loss/scripts/pybacdm/cpca070342024_shared/predict_pipeline/small_test_area/T29TPE_testblock.h5 \
	OUTPUT_DIR=/users1/cpca070342024/shared/predict_outputs/12_T29TPE_new_run \
    START_DATE=2023-02-01 \
	END_DATE=2023-10-01 \
	MAX_COMPOSITE_DAYS=45 \
	BLOCK_ROWS=0-2 \
	BLOCK_COLS=0-2 \
	WRITE_COMPOSITE_TIFS=1
```
<br></br>

## Required Inputs:

| KEY | DEFAULT VALUE (DTYPE) | DESCRIPTION |
| --- | --- | --- |
| `TILE_ID` | (str) | Free-form tile label (e.g. `T29TPE`). Used to name the output files and is written as the `tile_id` field in the output polygons/`.npz`. Not validated against the HDF5 contents. |
| `TILE_HDF5_PATH` | (path) | Chip-chunked tile HDF5 to predict on. This is the actual data source (the tile CRS and acquisition calendar are read from it). |
| `OUTPUT_DIR` | (path) | Base output directory; the `logs/`, `block_outputs/`, and `final_outputs/` subdirs are created under it (see layout below). |
| `START_DATE` | (date, `YYYY-MM-DD`) | Inclusive start of the date window. Used to get the time frame with which to create date cluster and determine change dates to process. |
| `END_DATE` | (date, `YYYY-MM-DD`) | Inclusive end of the date window. Used to get the time frame with which to create date cluster and determine change dates to process. |

<br></br>

## Optional Inputs:

| KEY | DEFAULT VALUE (DTYPE) | DESCRIPTION |
| --- | --- | --- |
| `MODEL` | `efficientnet_b2_16bit_pipeline` (str) | Model package directory name under `<shared>/` (the parent of `distribute/`). Each model package exposes the same interface (`predict.load_model`, `predict.predict_before_after_chips`, `DEFAULT_WEIGHTS`, `CLOSING_RADII` — see `bacdm/__init__.py` for the contract), so switching model is just e.g. `MODEL=bacdm`. |
| `DATA_DTYPE` | `u16` (str enum: `u8`\|`u16`) | Input data dtype for the read->composite->shift chain. `u16` (default) keeps raw uint16 reflectance (nodata 65535) — for models that scale natively (`efficientnet_b2_16bit_pipeline`). `u8` applies the q02/q98 stretch (uint8, nodata 255) — `bacdm` / `efficientnet_b2`. Match this to the model: a u16 model on u8 data (or vice versa) silently produces garbage. |
| `THREADS` | `2` (int) | CPU threads per task. Also sets `--cpus-per-task` so the allocation matches. A thread sweep showed ~95% scaling at 2 threads, ~68% at 4 — 2 is the efficient default. |
| `MAX_CONCURRENT` | `8` (int) | Max array tasks running at once (`--array %N` cap). With `THREADS`, keep `THREADS*MAX_CONCURRENT` well under the node's core count to avoid memory-bandwidth saturation. |
| `WEIGHTS_PATH` | model package's `DEFAULT_WEIGHTS` (path) | `.pth` checkpoint. Default is a checkpoint inside the model dir. |
| `BATCH_SIZE` | `8` (int) | Model batch size. |
| `VOTE_CLASSES` | `1,2` (comma-sep ints) | Non-bg class IDs to vote on. 1 = Cuts, 2 = Fires. Class names are in the model package's config (e.g. `bacdm/AAA_Configs.py`, `efficientnet_b2/configs.py`). |
| `VOTE_THRESHOLD` | `2` (int) | Min votes per pixel to keep a detection. |
| `TARGET_STEP_DAYS` | `45` (int, days) | Spacing (days) between generated dates when using the `START_DATE`/`END_DATE` span form (ignored if `TARGET_DATES` is given explicitly or `USE_DATE_CLUSTERS=1`). |
| `USE_DATE_CLUSTERS` | `1` (bool: 0\|1) | When 1 (default), cluster the tile's acquisition calendar over `START_DATE..END_DATE`: the cluster-gap midpoints become the target dates and each block's timesteps are collapsed to one min-composite per cluster before compositing (cloud-suppressing temporal summary). Requires `START_DATE`+`END_DATE`; `TARGET_DATES` must NOT be set (the dates are derived). Set 0 to use raw timesteps with the fixed-cadence / explicit `TARGET_DATES` paths instead. |
| `CLOSING_RADIUS` | `3` (int) | Post-vote morphological close disk radius (0 = off). |
| `MIN_PATCH_M2` | `2500` (float, m^2) | Block-level patch-area floor (m^2), firm. |
| `MIN_TILE_PATCH_M2` | `5000` (float, m^2) | Master patch-area floor (m^2), post cross-block merge. |
| `OUTPUT_NDVI` | `0` (bool: 0\|1) | When 1, also output per-pixel before/after NDVI alongside the change prediction: added to each block's `.npz` and the tile `.npz` (keys `ndvi_before`/`ndvi_after`), plus per-date NDVI GeoTIFFs (`{TILE}_tile_{date}_ndvi_before/after.tif`). float32, NoData -9999. Default 0 = labels only. |
| `CLIP_MASK_GPKG` | unset, no clip (path) | Path to a polygon `.gpkg`; when set, the final vector map (`.gpkg`/`.parquet`) is clipped to this mask — polygons outside are dropped, edge-straddling ones cut to the inside. Reprojected to the tile CRS automatically. Does not affect the `.npz` / per-date `.tif` rasters. |
| `MAX_COMPOSITE_DAYS` | unset, unbounded (int, days) | Symmetric day-window around each break date for before/after compositing. |
| `BLOCK_ROWS` / `BLOCK_COLS` | unset, whole tile (int or `lo-hi` range) | Process only a rectangular sub-grid of blocks (inclusive 0-based ranges) instead of the whole tile. e.g. `BLOCK_ROWS=1-2 BLOCK_COLS=1-2` processes the middle 2x2 of a 4x4 grid; a bare number selects one index. The aggregator crops its outputs to the selected sub-region. |
| `WRITE_COMPOSITE_TIFS` | `0` (bool: 0\|1) | When 1, dump per-block before/after time-composites as 10-band GeoTIFFs (debug/inspection only). |


## OUTPUT_DIR layout: 
```
OUTPUT_DIR/logs/            (SLURM .out/.err) \
OUTPUT_DIR/block_outputs/   (per-block .npz + .gpkg) \
OUTPUT_DIR/final_outputs/   (tile-level .gpkg/.parquet/.npz/.tif)
```