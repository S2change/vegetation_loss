# Hdf5, compression and chunks

## Originally hdf5 files created at INCD

Original chuncks: (1, 10, 2.8M)

File size (T29TPG):  427.6 GB

## Chip chunked

Chip-chunked (48, 10, 65536), padding=-9999

File size (T29TPG): 329.8 GB

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
	

## Claude's explanation for the different in size of the compressed files

### Two compounding effects both push the output smaller, more than offsetting the 6.4 % padding overhead:

1. Spatial coherence within each chunk (main reason)

Layout	What's in one chunk
- Old (1, 10, 2,810,880)	One timestep, ~2.8 M pixels drawn from the entire tile — forests, farmland, urban, water all mixed
- New (48, 10, 65,536)	48 timesteps of a single 256×256 geographic patch — almost entirely one land-cover type
- LZF (like all general compressors) finds repeated byte patterns. A chunk of 65,536 spectrally similar forest pixels compresses far better than 2.8 M mixed pixels. The spectral values within a chip cluster tightly, so LZF encodes long runs of near-identical bytes.

2. Temporal coherence along the new chunk's first axis

In the new layout, consecutive entries along the time axis (dim 0) are successive dates of the same pixel. Reflectance changes slowly between nearby dates → very high byte-level repetition → LZF makes much shorter codes. In the old layout the time axis had length 1, so this autocorrelation was never visible to the compressor.

### Rough accounting

- Raw data volume is essentially identical (6.4 % more slots, but those are all 65535 — a single repeated value that compresses to almost nothing)
- Better compression ratio is the net gain: 337.8 / 427.6 ≈ 0.79, so the new layout is ~21 % smaller despite the padding
- There is no data loss — you can verify by reading the same chip from both files and comparing values for any timestep where the original had valid data.
