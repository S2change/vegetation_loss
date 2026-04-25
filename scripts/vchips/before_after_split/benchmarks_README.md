# benchmarks_vchip_before_after_split.py

Instrumented copy of `vchip_before_after_split.py`. The processing logic is identical — every main stage is wrapped in a timing harness so the run produces a per-stage report covering wall time, CPU time, peak memory, and disk bytes read/written.

A `.txt` report is written to the working directory at the end of every run, named `benchmark_report_YYYYMMDD_HHMMSS.txt`.

## Usage

```bash
python benchmarks_vchip_before_after_split.py <vchip_dir> <hdf5_dir> <before_output_dir> <after_output_dir> [--runs N] [--cold]
```

- `--runs N` — repeat the full pipeline N times (default 2). Run 0 is cold; runs 1+ are warm (OS page cache populated).
- `--cold` — pause between runs so the OS file cache can be dropped manually (`sync && sudo purge` on macOS, or `echo 3 > /proc/sys/vm/drop_caches` as root on Linux).

## Stage explanations

The benchmark wraps every measurable step of the pipeline in a `bench.time_block(stage)` context. Each stage records one entry per call.

### Top-level stages (the outer measurements)

| Stage | What it covers |
|---|---|
| `pipeline.total` | The entire `run_pipeline()` call — building the tile index, matching vchips, processing every vchip. One record per run. This is the wall-time denominator for `%wall`. |
| `tile_index.build_total` | One-shot startup cost: opening every HDF5 file in `HDF5_DIR` and reading its `xs`/`ys` to compute the tile bounding box. Runs once per pipeline run. |
| `vchip.match_to_tiles` | Parsing each vchip filename and looking up which tile bbox contains its `(x, y)`. CPU-only, no I/O. |

### Inside the tile index build

| Stage | What it covers |
|---|---|
| `tile_index.read_one_file` | Reading `xs` and `ys` arrays from a single HDF5 tile file. One record per `.h5` file. The `read_MB` summed across these is the dominant startup I/O cost. |

### Per-tile setup (once per tile, before its vchips are processed)

| Stage | What it covers |
|---|---|
| `tile.open_and_load_coords` | Opening the HDF5 file (`h5py.File(...)`) and reading `xs`, `ys`, `ts` fully into memory with `[:]`. Heavy I/O — this is where the big coordinate arrays come in. |
| `tile.values_handle_acquire` | Just creating the `h5f['values']` reference. Should be near-zero (it's a lazy handle, no data is read). If it ever shows nonzero `read_bytes`, h5py is doing more work than expected. |

### Per-vchip work (inside `process_vchip`)

| Stage | What it covers |
|---|---|
| `vchip.process_total` | Wall time for everything done for one vchip — reading metadata, selecting timesteps, loading HDF5 data, compositing, writing both output TIFs. |
| `vchip.read_metadata` | `rio.open(vchip_path)` and reading `transform`, `width`, `height`, `meta`. Should be metadata-only (no pixel reads). |
| `vchip.select_temporal_indices` | Pure-numpy filter on `ts` to pick the pre/post timesteps within the temporal window. CPU-only. |
| `vchip.load_hdf5_total` | Wraps the whole `load_hdf5_for_vchip()` call. Equivalent to `compute_pixel_mask + alloc_result_buffer + slice_loop_total + reverse_band_order` summed. |
| `vchip.cascading_selection` | Running `cascading_selection_optimized()` on the loaded pre/post arrays to pick first-valid-observation per pixel. Pure CPU. |
| `vchip.ordinal_to_yyyymmdd` | Converting the timestamp arrays from Python ordinals (e.g. 738996) to YYYYMMDD integers (e.g. 20250101). |
| `vchip.stack_outputs` | `np.vstack` to glue the spectral bands and the date band into a single 11-band array. |
| `vchip.write_before_tif` | Writing the `_before.tif` to disk via rasterio. Disk write only. |
| `vchip.write_after_tif` | Writing the `_after.tif` to disk via rasterio. Disk write only. |

### Inside `load_hdf5_for_vchip` (the hot path)

| Stage | What it covers |
|---|---|
| `hdf5.compute_pixel_mask` | Computing `pixel_mask` from `xs`/`ys` against the vchip bounds, mapping HDF5 pixel positions to (row, col) within the vchip grid. CPU-only on already-loaded arrays. |
| `hdf5.alloc_result_buffer` | `np.full(...)` allocation of the result array. Should be fast. |
| `hdf5.slice_loop_total` | Wraps the per-timestep loop. Equivalent to summing `slice_per_timestep.first_in_vchip + slice_per_timestep`. |
| `hdf5.slice_per_timestep.first_in_vchip` | The **first** timestep slice within each vchip, separately. Captures any chunk-cache warmup cost on the very first call after entering a new vchip. |
| `hdf5.slice_per_timestep` | All subsequent timestep slices. With ~18 timesteps per vchip, you'll see this stage's calls = `(18 - 1) × n_vchips`. This is where most wall time goes. |
| `hdf5.reverse_band_order` | The `result[:, ::-1, :, :]` view that flips bands to descending order. Free — it's just a stride change, no copy. |

## Nesting structure

The stages nest hierarchically — a parent stage's wall time includes its children. Look at the percentages: `pipeline.total` is 100%, and child stages are slices of that.

```
pipeline.total
├── tile_index.build_total
│   └── tile_index.read_one_file (× n_tiles)
├── vchip.match_to_tiles
└── (per tile)
    ├── tile.open_and_load_coords
    ├── tile.values_handle_acquire
    └── (per vchip)
        └── vchip.process_total
            ├── vchip.read_metadata
            ├── vchip.select_temporal_indices
            ├── vchip.load_hdf5_total
            │   ├── hdf5.compute_pixel_mask
            │   ├── hdf5.alloc_result_buffer
            │   ├── hdf5.slice_loop_total
            │   │   ├── hdf5.slice_per_timestep.first_in_vchip
            │   │   └── hdf5.slice_per_timestep (× n_timesteps - 1)
            │   └── hdf5.reverse_band_order
            ├── vchip.cascading_selection
            ├── vchip.ordinal_to_yyyymmdd
            ├── vchip.stack_outputs
            ├── vchip.write_before_tif
            └── vchip.write_after_tif
```

## Column reference

| Column | Meaning |
|---|---|
| `calls` | How many times this block was timed |
| `wall_s` | Total wall-clock seconds across all calls |
| `cpu_s` | Total CPU seconds (wall − cpu = time spent waiting on I/O / sleeping) |
| `%wall` | Share of `pipeline.total` |
| `mean_ms` | Average wall-ms per call |
| `first_ms` | Wall-ms of the very first call |
| `rest_ms` | Mean wall-ms of all calls *after* the first (catches cold-start effects) |
| `read_MB` | Bytes read from disk during this stage, summed |
| `write_MB` | Bytes written to disk during this stage, summed |
| `pyPeak_MB` | Max Python heap allocation seen during a single call (tracemalloc peak) |
| `rssD_MB` | Max increase in process resident memory during a single call |

### Reading the columns

- **`wall − cpu` is the I/O wait time.** If a stage has `wall_s=200, cpu_s=10`, it spent 95% of its time waiting on disk — that's an I/O-bound stage. If `wall ≈ cpu`, the stage is CPU-bound (often decompression).
- **`first_ms` vs `rest_ms`** highlights cold-start effects. If `first_ms >> rest_ms`, the first call paid a one-time warmup cost (lazy library imports, OS page cache miss, HDF5 chunk cache empty). If `first_ms < rest_ms`, the first call benefited from chunks already loaded by an earlier stage and subsequent calls are paying the steady-state cache-thrash cost.
- **`read_MB` vs theoretical minimum.** For a per-timestep slice, the theoretical minimum is `n_bands × n_chip_pixels × 2 bytes`. If `read_MB / calls` is much larger than that, HDF5 is loading whole chunks because the access pattern straddles chunk boundaries.
- **`pyPeak_MB`** is the largest single-call Python allocation — useful for catching surprise spikes. **`rssD_MB`** is the largest single-call resident-memory increase, which can include allocations from C extensions (numpy, h5py) that `tracemalloc` won't see.

## Report from initial vchip_before_after_split

A run with `--runs 2` against 8 vchips and 17 HDF5 tiles produced:

```
========================================================================================================================
BENCHMARK REPORT
========================================================================================================================
stage                                       calls    wall_s    cpu_s  %wall   mean_ms  first_ms   rest_ms   read_MB  write_MB  pyPeak_MB   rssD_MB
--------------------------------------------------------------------------------------------------------------------------------------------------
pipeline.total                                  2   231.375  224.548 100.00 115687.50 115826.02 115548.99  14718.96    225.71       0.00    909.61
vchip.process_total                            16   213.394  207.907  92.23  13337.12   6709.22  13778.98   5672.28    225.71       0.00    127.80
vchip.load_hdf5_total                          16   209.310  206.662  90.46  13081.89   6282.12  13535.20   5662.29      0.00       0.00    100.66
hdf5.slice_loop_total                          16   205.172  202.543  88.67  12823.23   6225.63  13263.07   5662.29      0.00       0.00     37.00
hdf5.slice_per_timestep                       272   193.511  191.158  83.64    711.44    361.17    712.73   5310.18      0.00       3.20     24.60
tile_index.build_total                          2    15.067   13.951   6.51   7533.51   7174.78   7892.23   9006.96      0.00       0.00     41.80
tile_index.read_one_file                       34    13.035   11.981   5.63    383.40    124.67    391.24   9006.96      0.00     964.49    826.42
hdf5.slice_per_timestep.first_in_vchip         16    11.593   11.319   5.01    724.55    448.07    742.98    352.11      0.00       3.20     19.36
hdf5.compute_pixel_mask                        16     4.013    3.995   1.73    250.82     48.17    264.33      0.00      0.00     223.86    111.90
tile.open_and_load_coords                      14     2.838    2.650   1.23    202.72     92.76    211.18     39.67      0.00     895.20    639.95
vchip.write_before_tif                         16     1.643    0.294   0.71    102.70     38.95    106.95      0.00    112.85       7.05      7.97
vchip.write_after_tif                          16     1.440    0.276   0.62     90.03    124.73     87.71      0.00    112.85       7.05      0.00
vchip.cascading_selection                      16     0.411    0.408   0.18     25.66     27.48     25.54      0.00      0.00      37.86     16.14
vchip.read_metadata                            16     0.399    0.109   0.17     24.91    225.89     11.52     10.00      0.00       0.04      4.09
hdf5.alloc_result_buffer                       16     0.110    0.110   0.05      6.89      7.63      6.85      0.00      0.00      57.60     57.93
vchip.stack_outputs                            16     0.087    0.087   0.04      5.47      5.52      5.46      0.00      0.00      28.16     25.52
vchip.ordinal_to_yyyymmdd                      16     0.043    0.043   0.02      2.70      2.69      2.70      0.00      0.00       4.16      0.00
tile.values_handle_acquire                     14     0.003    0.003   0.00      0.20      0.19      0.20      0.00      0.00       0.01      0.00
vchip.select_temporal_indices                  16     0.002    0.002   0.00      0.11      0.17      0.11      0.00      0.00       0.01      0.39
vchip.match_to_tiles                            2     0.000    0.000   0.00      0.10      0.10      0.10      0.00      0.00       0.00      0.00
hdf5.reverse_band_order                        16     0.000    0.000   0.00      0.01      0.01      0.01      0.00      0.00       0.00      0.00

Total pipeline wall time: 231.375s

Per-call breakdown for 'hdf5.slice_per_timestep.first_in_vchip' (16 calls):
  first call : 448.07 ms  read=47248.0 KB
  rest mean  : 742.98 ms  read=19773.9 KB
  rest median: 727.94 ms
  rest max   : 943.52 ms

Per-call breakdown for 'hdf5.slice_per_timestep' (272 calls):
  first call : 361.17 ms  read=47252.0 KB
  rest mean  : 712.73 ms  read=18961.1 KB
  rest median: 719.26 ms
  rest max   : 1014.14 ms
```

### What this run reveals

- **88% of total wall time is in the HDF5 slice loop.** Everything else combined is under 10%.
- **Each slice reads ~19 MB.** Theoretical minimum for a typical vchip is ~1.3 MB (n_bands × n_chip_pixels × 2 bytes). The ~15× over-read confirms HDF5 is loading whole chunks because the access pattern straddles chunk boundaries.
- **`hdf5.slice_per_timestep` is CPU-bound** (`wall=193.5 s`, `cpu=191.2 s`). The cost is decompression of those over-read chunks, not waiting on disk.
- **`first_ms < rest_ms`** for the slice stage (361 ms vs 712 ms) — the very first call benefits from chunks loaded by `slice_per_timestep.first_in_vchip`. Steady-state calls pay full cache-thrash cost.
- **No meaningful warmup penalty between vchips** (`first_in_vchip` rest_mean ≈ regular slice rest_mean ≈ 720 ms).
- **Memory is fine.** Peak Python heap is 964 MB during tile_index reads, RSS delta peaks at ~900 MB. Well within compute-node limits.
- **Disk write cost is trivial** (~225 MB total, takes ~3 seconds).
