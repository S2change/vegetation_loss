"""
bench_hdf5_compressed_vs_uncompressed.py  —  chip-batch edition

Benchmarks reading a spatial chip batch within a configurable RAM budget,
matching the actual HPC workflow where each CPU processes ~N chips at a time.

Three strategies compared
-------------------------
A  compressed,   per-timestep : reads one full timestep (all pixels) from the
   compressed HDF5, extracts chip pixels, stores in accumulator, nanmedian.
   Must decompress the full ~1.2 GB chunk even for a small chip batch.

B  uncompressed, per-timestep : same loop from a small flat HDF5 that contains
   only chip pixels × window timesteps.  No decompression; much smaller buffer.

C  uncompressed, all-at-once  : reads all T timesteps of chip pixels in one
   call from the same small file, then nanmedian.  Simplest code path.

RAM budget
----------
Set RAM_BUDGET_GB to your target (e.g. 4.0 for a 5 GB HPC node leaving 1 GB
headroom).  The script auto-sizes the chip batch so that none of the three
strategies exceeds the budget, then creates a matching uncompressed bench file.

Usage
-----
    python bench_hdf5_compressed_vs_uncompressed.py
"""

import csv
import threading
import time
from datetime import date
from pathlib import Path

import h5py
import numpy as np
import psutil

# ── Configuration ──────────────────────────────────────────────────────────────
HDF5_COMPRESSED   = Path(r"H:\outputs_ROI\hdf5\T29TPG\T29TPG.h5")
HDF5_UNCOMPRESSED = HDF5_COMPRESSED.with_name("T29TPG_bench_uncompressed.h5")

REFERENCE_DATE = date(2022, 6, 15)
WINDOW_DAYS    = 45

NODATA_VAL     = 65535
PIXEL_RES      = 10     # Sentinel-2 native resolution, metres
CHIP_SIZE      = 256    # pixels per side

RAM_BUDGET_GB  = 4.0    # target peak RAM per strategy
N_REPEAT       = 3      # timing repetitions per strategy

# ── Peak RAM tracker ───────────────────────────────────────────────────────────

class _PeakRAMTracker:
    """Polls process RSS every 100 ms in a background thread."""
    def __init__(self):
        self._proc = psutil.Process()
        self._stop = threading.Event()

    def __enter__(self):
        self._baseline = self._proc.memory_info().rss
        self._peak     = self._baseline
        self._thread   = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return self

    def _run(self):
        while not self._stop.is_set():
            rss = self._proc.memory_info().rss
            if rss > self._peak:
                self._peak = rss
            self._stop.wait(0.1)

    def __exit__(self, *_):
        self._stop.set()
        self._thread.join()
        self.peak_gb  = self._peak                              / 1024**3
        self.base_gb  = self._baseline                          / 1024**3
        self.delta_gb = (self._proc.memory_info().rss - self._baseline) / 1024**3


def bench(fn, *args, n=N_REPEAT):
    times, peaks, deltas = [], [], []
    result = None
    for k in range(n):
        with _PeakRAMTracker() as mem:
            t0     = time.perf_counter()
            result = fn(*args)
            elapsed = time.perf_counter() - t0
        times.append(elapsed)
        peaks.append(mem.peak_gb)
        deltas.append(mem.delta_gb)
        print(f"    run {k+1}: {elapsed:.1f}s  peak {mem.peak_gb:.2f} GB  "
              f"baseline {mem.base_gb:.2f} GB  retained {mem.delta_gb:+.2f} GB")
    return result, np.array(times), np.array(peaks), np.array(deltas)


# ── Chip selection ─────────────────────────────────────────────────────────────

def select_chip_pixels(xs, ys, n_chips):
    """Return sorted pixel indices for n_chips randomly chosen chips.

    Assigns each flat pixel to a chip grid cell based on its UTM coordinates,
    then randomly draws n_chips cells that have at least 10 % fill.
    """
    chip_m  = CHIP_SIZE * PIXEL_RES                             # metres per chip side
    x_bins  = ((xs.astype(np.int64) - int(xs.min())) // chip_m).astype(np.int32)
    y_bins  = ((int(ys.max()) - ys.astype(np.int64)) // chip_m).astype(np.int32)
    chip_id = x_bins.astype(np.int64) * 1_000_000 + y_bins.astype(np.int64)

    unique_ids, pixel_chip = np.unique(chip_id, return_inverse=True)
    chip_counts = np.bincount(pixel_chip, minlength=len(unique_ids))
    min_fill    = max(1, CHIP_SIZE ** 2 // 10)
    valid       = np.where(chip_counts >= min_fill)[0]

    n_chips = min(n_chips, len(valid))
    if n_chips == 0:
        raise RuntimeError("No chip cells with sufficient pixel fill found.")
    chosen     = np.random.choice(valid, size=n_chips, replace=False)
    pixel_idx  = np.where(np.isin(pixel_chip, chosen))[0]
    return np.sort(pixel_idx), n_chips


def auto_n_chips(n_ts, n_bands, n_pix_full, budget_gb):
    """Return the largest chip count that keeps all three strategies in budget.

    Strategy A needs: full_ts_buffer (B × P_all × 2) + accumulator (T × B × P × 4)
    Strategy C needs: uint16 read (T × B × P × 2) + float32 copy (T × B × P × 4)
    Both are bounded by budget_gb.  Strategy B is always cheaper than A or C.
    """
    full_ts_gb = n_bands * n_pix_full * 2 / 1024**3
    # Strategy A binding: budget = full_ts_gb + T×B×P×4
    p_a = (budget_gb - full_ts_gb) * 1024**3 / (n_ts * n_bands * 4)
    # Strategy C binding: budget = T×B×P×(2+4)
    p_c = budget_gb * 1024**3 / (n_ts * n_bands * 6)
    p_max = max(1, int(min(p_a, p_c)))
    n_chips = max(1, p_max // (CHIP_SIZE ** 2))
    return n_chips, p_max


# ── Read strategies ────────────────────────────────────────────────────────────

def _nanmedian_chunked(accumulator, chunk=500_000):
    """nanmedian along axis-0 in spatial chunks to bound the argsort buffer."""
    n_t, n_b, n_p = accumulator.shape
    result = np.empty((n_b, n_p), dtype=np.float32)
    for s in range(0, n_p, chunk):
        e = min(s + chunk, n_p)
        result[:, s:e] = np.nanmedian(accumulator[:, :, s:e], axis=0)
    return result


def strategy_A_compressed_per_ts(h5_path, ts_src_idx, chip_pix_idx):
    """Read full timestep from compressed HDF5, extract chip pixels, accumulate."""
    n_p   = len(chip_pix_idx)
    with h5py.File(h5_path, 'r') as f:
        n_b   = f['values'].shape[1]
        accum = np.full((len(ts_src_idx), n_b, n_p), np.nan, dtype=np.float32)
        for out_i, src_i in enumerate(ts_src_idx):
            row = f['values'][int(src_i), :, :]           # (B, P_all) — full decompress
            chip = row[:, chip_pix_idx].astype(np.float32)
            chip[chip == NODATA_VAL] = np.nan
            accum[out_i] = chip
            del row
    return _nanmedian_chunked(accum)


def strategy_B_uncompressed_per_ts(h5_path, n_ts, n_p):
    """Read one timestep at a time from the small uncompressed chip file."""
    with h5py.File(h5_path, 'r') as f:
        n_b   = f['values'].shape[1]
        accum = np.full((n_ts, n_b, n_p), np.nan, dtype=np.float32)
        for i in range(n_ts):
            row = f['values'][i, :, :].astype(np.float32)  # (B, P_chip)
            row[row == NODATA_VAL] = np.nan
            accum[i] = row
    return _nanmedian_chunked(accum)


def strategy_C_uncompressed_all_at_once(h5_path):
    """Read all timesteps of chip pixels in one call, then nanmedian."""
    with h5py.File(h5_path, 'r') as f:
        data = f['values'][:, :, :].astype(np.float32)     # (T, B, P_chip)
    data[data == NODATA_VAL] = np.nan
    return _nanmedian_chunked(data)


# ── Build small uncompressed bench file (chip pixels only) ────────────────────

def create_chip_bench_file(src_path, dst_path, ts_src_idx, chip_pix_idx):
    """Write (T_window, B, P_chip) to a flat uncompressed HDF5, one ts at a time."""
    n_ts = len(ts_src_idx)
    n_p  = len(chip_pix_idx)
    print(f"  Creating chip bench file ({n_ts} ts × {n_p:,} chip pixels) …")
    with h5py.File(src_path, 'r') as s, h5py.File(dst_path, 'w') as d:
        n_b  = s['values'].shape[1]
        dset = d.create_dataset('values', shape=(n_ts, n_b, n_p),
                                dtype='uint16', chunks=None, compression=None)
        for out_i, src_i in enumerate(ts_src_idx):
            dset[out_i] = s['values'][int(src_i), :, :][:, chip_pix_idx]
            if (out_i + 1) % 20 == 0 or out_i == n_ts - 1:
                print(f"    {out_i+1}/{n_ts} timesteps …")
        for name in ('xs', 'ys'):
            d.create_dataset(name, data=s[name][:][chip_pix_idx])
        d.create_dataset('ts', data=s['ts'][:][ts_src_idx])
        for k, v in s.attrs.items():
            d.attrs[k] = v
    gb = dst_path.stat().st_size / 1e9
    print(f"  Done → {dst_path}  ({gb:.3f} GB)\n")


# ── Main ───────────────────────────────────────────────────────────────────────

ref_ordinal = REFERENCE_DATE.toordinal()

print(f"Opening {HDF5_COMPRESSED} …")
with h5py.File(HDF5_COMPRESSED, 'r') as f:
    ts_arr     = f['ts'][:]
    n_total_ts = len(ts_arr)
    n_bands    = f['values'].shape[1]
    n_pix_full = f['values'].shape[2]
    chunk_shape      = f['values'].chunks
    compression_info = f['values'].compression
    xs = f['xs'][:]
    ys = f['ys'][:]

mb_per_ts = n_bands * n_pix_full * 2 / 1e6
print(f"  {n_total_ts} timesteps | {n_bands} bands | {n_pix_full:,} pixels/timestep")
print(f"  Chunk shape: {chunk_shape} | Compression: {compression_info}")
print(f"  Full timestep size (uncompressed): {mb_per_ts:.1f} MB")

# Find window timesteps
lo = ref_ordinal - WINDOW_DAYS
hi = ref_ordinal + WINDOW_DAYS
ts_src_idx = np.where((ts_arr >= lo) & (ts_arr <= hi))[0]
if len(ts_src_idx) == 0:
    raise RuntimeError(f"No timesteps within ±{WINDOW_DAYS} d of {REFERENCE_DATE}. "
                       "Adjust REFERENCE_DATE.")
n_ts_win = len(ts_src_idx)
print(f"\nWindow ±{WINDOW_DAYS} d of {REFERENCE_DATE}: {n_ts_win} timesteps\n")

# Auto-size chip batch
n_chips_target, p_max = auto_n_chips(n_ts_win, n_bands, n_pix_full, RAM_BUDGET_GB)
print(f"RAM budget {RAM_BUDGET_GB} GB → target ≤ {n_chips_target} chips "
      f"({p_max:,} pixels, accumulator ≤ "
      f"{n_ts_win * n_bands * p_max * 4 / 1024**3:.2f} GB float32)")

chip_pix_idx, n_chips_actual = select_chip_pixels(xs, ys, n_chips_target)
n_p_chip = len(chip_pix_idx)
print(f"Selected {n_chips_actual} chips → {n_p_chip:,} pixels "
      f"({n_p_chip / n_pix_full * 100:.1f} % of tile)\n")

# Create bench file if needed (keyed by chip count so re-runs with different
# budgets don't silently reuse a file built with different pixels)
bench_tag = HDF5_COMPRESSED.with_name(
    f"{HDF5_COMPRESSED.stem}_bench_{n_chips_actual}chips.h5")
if not bench_tag.exists():
    create_chip_bench_file(HDF5_COMPRESSED, bench_tag, ts_src_idx, chip_pix_idx)
else:
    print(f"Bench file already exists: {bench_tag}\n")

# ── Run benchmarks ─────────────────────────────────────────────────────────────
results = {}

print("── Strategy A: compressed, per-timestep ─────────────────────────────────")
print("   (full ts decompress → extract chip pixels → accumulate → nanmedian)")
_, results['A_t'], results['A_peak'], results['A_delta'] = bench(
    strategy_A_compressed_per_ts, HDF5_COMPRESSED, ts_src_idx, chip_pix_idx)

print("\n── Strategy B: uncompressed chip file, per-timestep ─────────────────────")
print("   (read one ts of chip pixels → accumulate → nanmedian)")
_, results['B_t'], results['B_peak'], results['B_delta'] = bench(
    strategy_B_uncompressed_per_ts, bench_tag, n_ts_win, n_p_chip)

print("\n── Strategy C: uncompressed chip file, all at once ──────────────────────")
print("   (read all ts × chip pixels in one call → nanmedian)")
_, results['C_t'], results['C_peak'], results['C_delta'] = bench(
    strategy_C_uncompressed_all_at_once, bench_tag)

# ── Summary ────────────────────────────────────────────────────────────────────

def fmt(t_arr, p_arr):
    return f"{t_arr.mean():.1f}s (peak {p_arr.mean():.2f} GB)"

sep = "=" * 75
print(f"\n{sep}")
print(f"Tile: {HDF5_COMPRESSED.name}  |  {n_chips_actual} chips  |  "
      f"{n_ts_win} timesteps  |  budget {RAM_BUDGET_GB} GB")
print(sep)
print(f"  A  compressed   per-ts : {fmt(results['A_t'], results['A_peak'])}")
print(f"  B  uncompressed per-ts : {fmt(results['B_t'], results['B_peak'])}")
print(f"  C  uncompressed all-ts : {fmt(results['C_t'], results['C_peak'])}")
print(sep)

def speedup(base, faster):
    r = base.mean() / faster.mean()
    return f"{r:.1f}×" if r >= 1 else f"1/{1/r:.1f}×"

print(f"  B vs A (uncomp per-ts vs comp per-ts): {speedup(results['A_t'], results['B_t'])} faster")
print(f"  C vs A (uncomp all-ts vs comp per-ts): {speedup(results['A_t'], results['C_t'])} faster")
print(f"  C vs B (all-ts vs per-ts, uncompressed): {speedup(results['B_t'], results['C_t'])} faster")

# ── Save CSV ───────────────────────────────────────────────────────────────────
out_csv = Path(__file__).with_suffix('.csv')
with open(out_csv, 'w', newline='') as fh:
    w = csv.writer(fh)
    w.writerow(['strategy', 'label', 'run', 'seconds', 'peak_ram_gb', 'retained_ram_gb'])
    for key, label in [('A', 'compressed_per_ts'),
                        ('B', 'uncompressed_per_ts'),
                        ('C', 'uncompressed_all_at_once')]:
        for i, (t, p, d) in enumerate(
                zip(results[f'{key}_t'], results[f'{key}_peak'], results[f'{key}_delta']), 1):
            w.writerow([key, label, i, f'{t:.3f}', f'{p:.3f}', f'{d:.3f}'])
print(f"\nResults saved → {out_csv}")
