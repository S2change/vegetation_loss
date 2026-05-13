1. `rechunk_hdf5_chip_oriented_N_TS_timestamps.py`: Rewrites an HDF5 time series from temporal chunks  (1,  B, P_all) into chip-oriented chunks (T_CHUNK, B, CHIP_SIZE), just for a set of timestamps (say, N_TS=48)
2.  `rechunk_hdf5_chip_oriented.py`: Similar, but converts all data (all timestamps) from the input file

Notice that re-structuring the chunks implies data padding (-9999)

# Hdf5, compression and chunks

Prompt: data (sentinel 2 images with a time stamp) are acquired every few days. Right now, the new images are added to the current hdf5 files with chunks (1, 10, 2 810 880). This makes sense because one can update the file after a new ne acquisitions. However, for the particular project I'm working on, I need to process the most recent  n_ts timestamps, since I need to build a "before" and an "after" time composite to detect if chenges occur. For this processing step it's clearly better to have the data Chip-chunked (n_ts, 10, 65 536), with n_ts=48 or lower. Since in practice I only need a temporal slice of the original data, would it make sense to have a pre-processing step that would write to the disk the timestamps I need with the adequate chunks of (n_ts, 10, 65 536)? And how can be this done in HPC with a RAM budget of 5 GB per cpu?


<details open>
<summary>Chunking strategies; T29TPG example</summary>

## Chunking strategy comparison

| | Original `(1, 10, 2 810 880)` | Chip-chunked `(48, 10, 65 536)` |
|---|---|---|
| **Chunk uncompressed** | 56 MB | 63 MB |
| **Chunk on disk (LZF)** | ~9 MB | ~7.6 MB |
| **File size** | 427 GB | 338 GB |
| **Total chunks** | 2 232 × 21 = 46 872 | 959 × 47 = 45 073 |
| **Natural parallel unit** | timestep | chip |

---

**Read cost for key access patterns**

| Operation | Original | Chip-chunked |
|---|---|---|
| 1 chip, 48 ts | 48 × 21 chunks = 56 GB decompressed | **1 chunk = 63 MB** |
| 1 chip, all 2 232 ts | 2 232 × 21 chunks = 2.6 TB | **47 chunks = 3 GB** |
| Full tile, 1 ts | 21 chunks = 1.18 GB | 959 chunks = 60 GB |
| Full tile, 48 ts | 1 008 chunks = 9.2 GB disk | 959 chunks = 7.3 GB disk |

---

**HPC memory budget per CPU (5 GB), chip classification workflow**

| Item | Original | Chip-chunked |
|---|---|---|
| Per-ts read buffer (unavoidable) | **1.18 GB** (full tile) | 63 MB (one chip) |
| Accumulator (N chips × 48 ts × 10 bands, float32) | N × 0.12 GB | N × 0.12 GB |
| BACDM model weights + activations | ~1.5 GB | ~1.5 GB |
| Remaining for data at 5 GB | ~2.3 GB → **≤ 19 chips** | ~3.4 GB → **≤ 28 chips** |
| Full-ts buffer forced even for 1 chip | **yes** | no |

---

**HPC parallelization**

| | Original | Chip-chunked |
|---|---|---|
| Split strategy | awkward — each job must read the full tile per ts regardless of how many chips it owns | natural — assign a contiguous chip range `[c_start, c_end)` per job; reads are independent |
| Jobs share data? | all jobs decompress the same chunks (no benefit from splitting) | each job reads a disjoint set of chunks (zero overlap) |
| Embarrassingly parallel? | no — full-tile reads create implicit coupling | **yes** — chip ranges are fully independent |
| Suggested job sizing | — | `ceil(959 / n_cpus)` chips per job |

</details>

---

<details open>
<summary>Pre-processing strategy</summary>

## Pre-processing: extract recent timestamps with chip-oriented chunking

The idea is to pre-process the original hdf5 file to create a temporary file in disk just with `N_TS` timestamps for processing. See script `preprocess_to_n_ts_chip_chunked.py`. For T29TPG and `N_TS=48`, if took 4:50 in local PC (2 batches). The output file is `T29TPG_48ts_20251028_20251229.h5` (4.6 GB).


### Motivation

Sentinel-2 acquisitions are added continuously to a master HDF5 file with
**temporal chunks** `(1, B, P_all)` — one chunk per acquisition, covering all
pixels of the tile. This layout is ideal for appending new data but costly for
inference, which only needs a short temporal window (e.g. the last 48
acquisitions) and accesses data **chip by chip**.

The mismatch between the two layouts:

| Layout | Chunk shape | Best for |
|---|---|---|
| Master file (temporal) | `(1, 10, 2 810 880)` | Appending new acquisitions |
| Inference file (chip) | `(N_ts, 10, 65 536)` | Reading all timestamps of one chip |

Reading one chip's full time series from the master file requires decompressing
**N_ts × 21 source chunks ≈ 56 GB** per chip. With the chip-oriented layout the
same operation costs **1 chunk ≈ 63 MB**.

---

### Pre-processing step: `extract_recent_chip_chunked.py`

Before each inference run, a small pre-processing script reads the last `N_ts`
timestamps from the master file and writes a new HDF5 with chip-oriented
chunks:

```
Input  :  T29TPG.h5            427 GB   chunks (1,  10, 2 810 880)   all timestamps
Output :  T29TPG_48ts_…….h5    ~7 GB   chunks (48, 10, 65 536)      last 48 ts only
```

Because the output contains exactly `N_ts` rows, **every chip occupies exactly
one chunk** — zero wasted reads at inference time.

---

### Memory model

```
Peak RAM = source_ts_buffer + accumulator

  source_ts_buffer = B × P_all × 2 bytes          (one full source timestep)
                   = 10 × 58 805 352 × 2 ≈ 1.18 GB

  accumulator      = N_ts × B × N_chips_batch × CHIP_SIZE² × 2 bytes
                   = 48  × 10 × N_chips_batch × 65 536 × 2
                   ≈ N_chips_batch × 60 MB
```

The script auto-sizes `N_chips_batch` so that
`source_ts_buffer + accumulator ≤ RAM_BUDGET_GB`.

---

### Multi-pass strategy for limited RAM

The two layouts are orthogonal: building one output chip-chunk requires pixels
from 48 different source chunks; holding all of them simultaneously requires
56 GB. When RAM is insufficient the script processes chips in spatial batches,
re-reading the source timestamps once per batch.

```
Batch 1  (chips 0 – 59)
  read source ts 0  → 1.18 GB → extract chips 0–59 → free
  read source ts 1  → 1.18 GB → extract chips 0–59 → free
  …
  read source ts 47 → 1.18 GB → extract chips 0–59 → free
  write output chips 0–59 → free accumulator

Batch 2  (chips 60 – 119)
  read source ts 0  AGAIN → extract chips 60–119 → free
  …
```

Each source timestamp is read **once per spatial batch**. This is equivalent to
an out-of-core matrix transpose: fewer passes require more RAM, more passes
require less RAM.

---

### Local PC vs HPC comparison

| | Local PC (64 GB) | HPC node (5 GB) |
|---|---|---|
| `RAM_BUDGET_GB` | 54 | 4 |
| `N_chips_batch` | ~840 (all chips) | ~60 |
| Spatial batches | **1** | **16** |
| Source ts reads | 48 | 768 |
| Compressed data read from disk | ~0.6 GB | ~9.6 GB |
| Peak RAM | ~55 GB | ~5 GB |
| Estimated wall time | **~1 min** | **~17 min** |

The output file is identical regardless of which machine runs the script.
Only `RAM_BUDGET_GB` needs to change.

---

### Usage

```bash
# Preview the plan without writing anything
python extract_recent_chip_chunked.py --dry

# Full run (local PC)
python extract_recent_chip_chunked.py

# Full run on HPC (edit RAM_BUDGET_GB = 4.0 in the script first)
python extract_recent_chip_chunked.py
```

Key parameters at the top of the script:

| Parameter | Default | Description |
|---|---|---|
| `N_TS` | `48` | Number of most-recent timestamps to extract |
| `RAM_BUDGET_GB` | `54.0` | Set to ~80 % of available RAM |
| `CHIP_SIZE` | `256` | Chip side in pixels (matches inference model input) |
| `COMPRESSION` | `'lzf'` | Set `None` for uncompressed output |

The output filename encodes the date range automatically:
`T29TPG_48ts_20250115_20260430.h5`

</details>


