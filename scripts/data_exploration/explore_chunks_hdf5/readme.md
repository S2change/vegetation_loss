# Hdf5, compression and chunks

## Originally hdf5 files created at INCD

Original chuncks: (1, 10, 2.8M)

File size (T29TPG):  427.6 GB

## Chip chunked

Chip-chunked (48, 10, 65536), padding=-9999

File size (T29TPG): 329.8 GB

## Claude's explanation

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
