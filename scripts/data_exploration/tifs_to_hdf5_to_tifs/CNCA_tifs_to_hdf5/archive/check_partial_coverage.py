"""
check_partial_coverage.py

For each timestep in an HDF5 file, measures how many pixels are fully valid
(no band == NODATA) and where they sit in the east-west direction.

Partial-orbit slices appear as timesteps whose valid pixels cover only the
eastern or western half of the tile X range.

Usage
-----
    python check_partial_coverage.py

Output
------
  - Console table: one row per timestep with coverage stats
  - coverage_summary.png: scatter of valid-pixel fraction vs timestep,
    coloured by whether coverage is eastern / western / full
"""

from datetime import date
from pathlib import Path

import h5py
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ── Configuration ──────────────────────────────────────────────────────────────
HDF5_PATH  = Path(r"H:\outputs_ROI\hdf5\T29TQG\T29TQG_bench_uncompressed.h5")
NODATA_VAL = 65535

# A timestep is "partial" when fewer than this fraction of pixels are valid.
PARTIAL_THRESHOLD = 0.80

# East/west split: if the mean X of valid pixels deviates from the tile centre
# by more than this fraction of the tile X range, it's flagged as skewed.
SKEW_THRESHOLD = 0.15   # 15 % of tile width

# ── Load coordinate and timestamp arrays ───────────────────────────────────────
print(f"Opening {HDF5_PATH} …")
with h5py.File(HDF5_PATH, 'r') as f:
    xs       = f['xs'][:]          # (P,) int32, easting in EPSG:32629
    ts_ord   = f['ts'][:]          # (P_ts,) ordinal dates
    n_ts, n_bands, n_pix = f['values'].shape
    print(f"  {n_ts} timesteps | {n_bands} bands | {n_pix:,} pixels\n")

    xs_min, xs_max = xs.min(), xs.max()
    xs_mid  = (xs_min + xs_max) / 2
    xs_range = xs_max - xs_min

    rows = []
    for t in range(n_ts):
        data_t = f['values'][t, :, :]          # (B, P) uint16
        valid  = ~np.any(data_t == NODATA_VAL, axis=0)   # True where all bands valid
        n_valid = int(valid.sum())
        frac    = n_valid / n_pix

        if n_valid > 0:
            xs_valid    = xs[valid]
            mean_x      = float(xs_valid.mean())
            median_x    = float(np.median(xs_valid))
            skew        = (mean_x - xs_mid) / xs_range   # -0.5..+0.5
            x_min_valid = int(xs_valid.min())
            x_max_valid = int(xs_valid.max())
        else:
            mean_x = median_x = skew = float('nan')
            x_min_valid = x_max_valid = None

        # Classify
        if frac >= PARTIAL_THRESHOLD:
            coverage = 'full'
        elif skew > SKEW_THRESHOLD:
            coverage = 'east'
        elif skew < -SKEW_THRESHOLD:
            coverage = 'west'
        else:
            coverage = 'partial-centre'

        rows.append(dict(t=t, ordinal=int(ts_ord[t]), date=date.fromordinal(int(ts_ord[t])),
                         n_valid=n_valid, frac=frac, mean_x=mean_x, skew=skew,
                         x_min=x_min_valid, x_max=x_max_valid, coverage=coverage))

# ── Console report ─────────────────────────────────────────────────────────────
hdr = f"{'#':>3}  {'Date':<12} {'Valid px':>10} {'Frac':>6}  {'MeanX':>10}  {'Skew':>6}  Coverage"
print(hdr)
print("-" * len(hdr))
for r in rows:
    print(f"{r['t']:>3}  {str(r['date']):<12} {r['n_valid']:>10,} {r['frac']:>6.1%}  "
          f"{r['mean_x']:>10.0f}  {r['skew']:>+6.2f}  {r['coverage']}")

# ── Summary counts ─────────────────────────────────────────────────────────────
from collections import Counter
counts = Counter(r['coverage'] for r in rows)
print(f"\nCoverage breakdown across {n_ts} timesteps:")
for label, n in sorted(counts.items()):
    print(f"  {label:<20}: {n:>4} ({100*n/n_ts:.0f} %)")

tile_width_km = xs_range / 1000
print(f"\nTile X range: {xs_min:,} – {xs_max:,} m  ({tile_width_km:.1f} km wide)")
print(f"East/west split point: {xs_mid:,.0f} m")

# ── Plot ───────────────────────────────────────────────────────────────────────
colour_map = {'full': 'steelblue', 'east': 'darkorange',
              'west': 'forestgreen', 'partial-centre': 'red'}

dates_dt = [r['date'] for r in rows]
fracs    = [r['frac'] for r in rows]
colours  = [colour_map[r['coverage']] for r in rows]

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 7), sharex=True)

# Top: valid-pixel fraction
ax1.scatter(dates_dt, fracs, c=colours, s=40, zorder=3)
ax1.axhline(PARTIAL_THRESHOLD, color='grey', ls='--', lw=0.8, label=f'partial threshold ({PARTIAL_THRESHOLD:.0%})')
ax1.set_ylabel('Valid pixel fraction')
ax1.set_ylim(0, 1.05)
ax1.legend(fontsize=8)
ax1.grid(axis='y', alpha=0.3)

# Bottom: east-west skew
skews = [r['skew'] for r in rows]
ax2.scatter(dates_dt, skews, c=colours, s=40, zorder=3)
ax2.axhline( SKEW_THRESHOLD, color='darkorange', ls='--', lw=0.8, label=f'+{SKEW_THRESHOLD} (east bias)')
ax2.axhline(-SKEW_THRESHOLD, color='forestgreen', ls='--', lw=0.8, label=f'−{SKEW_THRESHOLD} (west bias)')
ax2.axhline(0, color='grey', lw=0.5)
ax2.set_ylabel('East–west skew\n(+= east, −= west)')
ax2.set_xlabel('Date')
ax2.legend(fontsize=8)
ax2.grid(axis='y', alpha=0.3)

# Legend patches
from matplotlib.patches import Patch
legend_handles = [Patch(color=c, label=l) for l, c in colour_map.items()]
fig.legend(handles=legend_handles, loc='upper right', fontsize=8,
           title='Coverage type', bbox_to_anchor=(0.99, 0.99))

fig.suptitle(f'Per-timestep spatial coverage — {HDF5_PATH.stem}', fontsize=11)
fig.autofmt_xdate()
plt.tight_layout()

out_png = Path(__file__).with_name('coverage_summary.png')
plt.savefig(out_png, dpi=150, bbox_inches='tight')
print(f"\nPlot saved → {out_png}")
plt.show()
