"""
check_partial_coverage_chip_chunked.py

For each of 48 consecutive timestamps in a chip-chunked HDF5 file, measures
how many pixels are valid and whether they are biased east or west.

Adapted from check_partial_coverage.py for the rechunked file layout where:
  - values  : (T, B, N_chips × 65536)   chip-oriented chunks (48, B, 65536)
  - xs_new  : coordinates in new pixel order;  -9999 at padding slots
  - ts      : ordinal dates, length T

The 48 timestamps to inspect correspond to one temporal chunk.  Set
TS_CHUNK_IDX to choose which chunk (0 = first 48 dates, 1 = next 48, …),
or set TS_START / TS_END directly for any arbitrary 48-timestamp window.

Validity is determined by checking a single band (VALIDITY_BAND, default 0).
For Sentinel-2 all bands share the same nodata mask, so one band suffices and
avoids loading all 10 bands per timestep (~125 MB vs ~1.2 GB per ts).

Usage
-----
    python check_partial_coverage_chip_chunked.py

Output
------
  - Console table : one row per timestep with coverage stats
  - coverage_summary_chip_chunked.png
"""

from collections import Counter
from datetime import date
from pathlib import Path

import h5py
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

# ── Configuration ──────────────────────────────────────────────────────────────
HDF5_PATH      = Path(r"H:\outputs_ROI\hdf5\T29TPG\T29TPG_chip_chunked.h5")
NODATA_VAL     = 65535
VALIDITY_BAND  = 0        # band index used to infer valid/nodata (0-based)
CHUNK_SIZE     = 48       # must match the file's temporal chunk size

PARTIAL_THRESHOLD = 0.80   # below this fraction → "partial" timestep
SKEW_THRESHOLD    = 0.15   # >15 % of tile width from centre → east/west

# ── Load metadata ──────────────────────────────────────────────────────────────
print(f"Opening {HDF5_PATH} …")
with h5py.File(HDF5_PATH, 'r') as f:
    xs_new  = f['xs_new'][:]       # (N_chips*65536,) int32;  -9999 at padding
    ts_ord  = f['ts'][:]           # (T,) ordinal dates
    n_ts    = f['values'].shape[0]
    n_bands = f['values'].shape[1]
    n_pix   = f['values'].shape[2]

# Second-to-last aligned chunk (the 48 timestamps just before the most recent chunk)
n_chunks     = (n_ts + CHUNK_SIZE - 1) // CHUNK_SIZE   # total temporal chunks
TS_CHUNK_IDX = max(0, n_chunks - 4) # change here for a different period/temporal chunk
TS_START     = TS_CHUNK_IDX * CHUNK_SIZE
TS_END       = min(TS_START + CHUNK_SIZE, n_ts)
n_check      = TS_END - TS_START

print(f"  {n_ts} total timesteps | {n_bands} bands | {n_pix:,} padded pixels")
print(f"  Analysing timesteps {TS_START}–{TS_END-1} ({n_check} ts, chunk {TS_CHUNK_IDX})\n")

# Padding mask: xs_new == -9999 marks slots with no real pixel
real    = xs_new != -9999         # (n_pix,) bool — non-padding positions
n_real  = int(real.sum())
xs_real = xs_new[real]            # x-coords of real pixels only

xs_min, xs_max = int(xs_real.min()), int(xs_real.max())
xs_mid   = (xs_min + xs_max) / 2
xs_range = xs_max - xs_min
print(f"Tile X range: {xs_min:,} – {xs_max:,} m  ({xs_range/1000:.1f} km wide)")
print(f"Real pixels:  {n_real:,}  |  Padding slots: {n_pix - n_real:,}\n")

# ── Per-timestep analysis ──────────────────────────────────────────────────────
# Read all 48 timestamps in a single h5py call.
# Because TS_START:TS_END aligns with one temporal chunk boundary, h5py reads
# each of the 959 chip chunks exactly once (959 decompression operations total).
# A per-timestamp loop would cause up to 48 × 959 = 45 000 decompression ops
# by re-reading the same chunks on every iteration.
# Memory: n_check × n_pix × 2 bytes ≈ 6 GB for this tile — fine on a 64 GB machine.
# On a memory-constrained node, replace this with a chip-by-chip loop instead.
print("Reading all timestamps in one call …")
with h5py.File(HDF5_PATH, 'r') as f:
    all_bands = f['values'][TS_START:TS_END, VALIDITY_BAND, :]  # (n_check, n_pix) uint16
print(f"  Loaded {all_bands.nbytes / 1e9:.2f} GB into RAM.\n")

rows = []
for t_local in range(n_check):
    t_abs = TS_START + t_local
    band0 = all_bands[t_local]                       # (n_pix,) uint16 — from RAM

    valid   = real & (band0 != NODATA_VAL)
    n_valid = int(valid.sum())
    frac    = n_valid / n_real

    if n_valid > 0:
        xs_valid = xs_new[valid]
        mean_x   = float(xs_valid.mean())
        skew     = (mean_x - xs_mid) / xs_range
        x_lo     = int(xs_valid.min())
        x_hi     = int(xs_valid.max())
    else:
        mean_x = skew = float('nan')
        x_lo = x_hi = None

    if frac >= PARTIAL_THRESHOLD:
        coverage = 'full'
    elif n_valid == 0:
        coverage = 'empty'
    elif skew > SKEW_THRESHOLD:
        coverage = 'east'
    elif skew < -SKEW_THRESHOLD:
        coverage = 'west'
    else:
        coverage = 'partial-centre'

    rows.append(dict(
        t_abs=t_abs, t_local=t_local,
        ordinal=int(ts_ord[t_abs]),
        date=date.fromordinal(int(ts_ord[t_abs])),
        n_valid=n_valid, frac=frac,
        mean_x=mean_x, skew=skew,
        x_lo=x_lo, x_hi=x_hi, coverage=coverage,
    ))
    print(f"  ts {t_abs:>4} ({rows[-1]['date']})  "
          f"valid {frac:5.1%}  skew {skew:+.2f}  {coverage}")

# ── Console summary ────────────────────────────────────────────────────────────
print(f"\n{'#':>3}  {'Date':<12} {'Valid px':>10} {'Frac':>6}  "
      f"{'MeanX':>10}  {'Skew':>6}  {'X range (km)':>14}  Coverage")
print("-" * 80)
for r in rows:
    x_span = (r['x_hi'] - r['x_lo']) / 1000 if r['x_lo'] is not None else 0
    print(f"{r['t_abs']:>3}  {str(r['date']):<12} {r['n_valid']:>10,} {r['frac']:>6.1%}  "
          f"{r['mean_x']:>10.0f}  {r['skew']:>+6.2f}  {x_span:>13.1f}  {r['coverage']}")

counts = Counter(r['coverage'] for r in rows)
print(f"\nCoverage breakdown across {n_check} timesteps:")
for label, n in sorted(counts.items()):
    print(f"  {label:<20}: {n:>4}  ({100*n/n_check:.0f} %)")

# ── Plot ───────────────────────────────────────────────────────────────────────
colour_map = {
    'full':            'steelblue',
    'east':            'darkorange',
    'west':            'forestgreen',
    'partial-centre':  'red',
    'empty':           'black',
}

dates_dt = [r['date'] for r in rows]
fracs    = [r['frac']  for r in rows]
skews    = [r['skew']  for r in rows]
colours  = [colour_map[r['coverage']] for r in rows]

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 7), sharex=True)

ax1.scatter(dates_dt, fracs, c=colours, s=40, zorder=3)
ax1.axhline(PARTIAL_THRESHOLD, color='grey', ls='--', lw=0.8,
            label=f'partial threshold ({PARTIAL_THRESHOLD:.0%})')
ax1.set_ylabel('Valid pixel fraction\n(of real pixels, excl. padding)')
ax1.set_ylim(0, 1.05)
ax1.legend(fontsize=8)
ax1.grid(axis='y', alpha=0.3)

ax2.scatter(dates_dt, skews, c=colours, s=40, zorder=3)
ax2.axhline( SKEW_THRESHOLD, color='darkorange', ls='--', lw=0.8,
             label=f'+{SKEW_THRESHOLD} (east bias)')
ax2.axhline(-SKEW_THRESHOLD, color='forestgreen', ls='--', lw=0.8,
             label=f'−{SKEW_THRESHOLD} (west bias)')
ax2.axhline(0, color='grey', lw=0.5)
ax2.set_ylabel('East–west skew\n(+= east,  −= west)')
ax2.set_xlabel('Date')
ax2.legend(fontsize=8)
ax2.grid(axis='y', alpha=0.3)

legend_handles = [Patch(color=c, label=l) for l, c in colour_map.items()]
fig.legend(handles=legend_handles, loc='upper right', fontsize=8,
           title='Coverage', bbox_to_anchor=(0.99, 0.99))

fig.suptitle(
    f'Spatial coverage — chunk {TS_CHUNK_IDX} (ts {TS_START}–{TS_END-1}) — '
    f'{HDF5_PATH.stem}',
    fontsize=10,
)
fig.autofmt_xdate()
plt.tight_layout()

out_png = Path(__file__).with_name('coverage_summary_chip_chunked.png')
plt.savefig(out_png, dpi=150, bbox_inches='tight')
print(f"\nPlot saved → {out_png}")
plt.show()
