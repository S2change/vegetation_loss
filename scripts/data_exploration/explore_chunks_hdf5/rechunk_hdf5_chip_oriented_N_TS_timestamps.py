"""
extract_recent_chip_chunked.py

Pre-processing step for HPC inference.

Reads the most recent N_TS timestamps from the original compressed HDF5
(chunks 1 × B × P_all, all acquisitions) and writes a new, small HDF5
with chip-oriented chunks (N_TS, B, CHIP_SIZE²).

Why this makes sense
--------------------
* Inference only needs a short temporal window (before/after composite).
* The original file grows indefinitely; only the tail is needed.
* With N_TS = 48 timestamps the output is ~7 GB instead of 427 GB.
* Because the output has exactly N_TS rows, every chip occupies exactly
  ONE chunk — perfect alignment, zero wasted reads at inference time.

Memory model
------------
Peak RAM = source_ts_buffer + accumulator
  source_ts_buffer = B × P_all × 2 bytes  (one full source timestep)
  accumulator      = N_TS × B × N_chips_batch × CHIP_SIZE² × 2 bytes

Set RAM_BUDGET_GB to your available RAM.  The script auto-sizes
N_chips_batch so the budget is respected.

Expected wall time (source = LZF-compressed, ~1.4 s/ts read)
-------------------------------------------------------------
  64 GB machine : 1 spatial batch  ×  N_TS reads  ≈  1 min
   5 GB HPC node: ~16 spatial batches × N_TS reads  ≈  17 min

Output file
-----------
  values            : (N_TS, B, N_chips × CHIP_SIZE²)  uint16
  xs_new / ys_new   : coordinates in new pixel order; -9999 at padding
  sort_order        : new_index → original_pixel_index (-1 for padding)
  chip_x_bin        : chip grid column for each chip
  chip_y_bin        : chip grid row    for each chip
  chip_pixel_count  : real (non-padding) pixels per chip
  ts                : ordinal dates of the N_TS timestamps written
  Attributes mirror the source file plus chip/pixel metadata.

Usage
-----
    python extract_recent_chip_chunked.py           # full run
    python extract_recent_chip_chunked.py --dry     # plan only, no I/O
"""

import argparse
import time
from datetime import date, timedelta
from pathlib import Path

import h5py
import numpy as np

# ── Configuration ──────────────────────────────────────────────────────────────
SRC_PATH      = Path(r"H:\outputs_ROI\hdf5\T29TPG\T29TPG.h5")

N_TS          = 48      # number of most-recent timestamps to extract
CHIP_SIZE     = 256     # pixels per chip side  (256 × 256 = 65 536 px/chip)
PIXEL_RES     = 10      # Sentinel-2 native resolution, metres
NODATA_VAL    = 65535
RAM_BUDGET_GB = 54.0    # 54 GB → 1 batch on a 64 GB machine
                        # set to 4.0 for a 5 GB HPC node (~16 batches)
COMPRESSION   = 'lzf'  # set None for uncompressed output

# ── Chip layout (identical to rechunk_hdf5_chip_oriented.py) ──────────────────

def build_chip_layout(xs, ys):
    """Sort pixels into chip grid cells, pad each to CHIP_SIZE² slots."""
    n_slots = CHIP_SIZE ** 2
    chip_m  = CHIP_SIZE * PIXEL_RES
    x_bins  = ((xs.astype(np.int64) - int(xs.min())) // chip_m).astype(np.int32)
    y_bins  = ((int(ys.max()) - ys.astype(np.int64)) // chip_m).astype(np.int32)
    chip_id = x_bins.astype(np.int64) * 1_000_000 + y_bins.astype(np.int64)

    unique_ids, pixel_chip = np.unique(chip_id, return_inverse=True)
    n_chips     = len(unique_ids)
    chip_counts = np.bincount(pixel_chip, minlength=n_chips).astype(np.int32)
    order       = np.argsort(pixel_chip, kind='stable')

    # Padded sort order: slot i → original pixel index (-1 = padding)
    sort_order_padded = np.full(n_chips * n_slots, -1, dtype=np.int64)
    chip_starts = np.zeros(n_chips + 1, dtype=np.int64)
    np.cumsum(chip_counts, out=chip_starts[1:])
    for c in range(n_chips):
        s, e = chip_starts[c], chip_starts[c + 1]
        sort_order_padded[c * n_slots : c * n_slots + (e - s)] = order[s:e]

    xs_new = np.full(n_chips * n_slots, -9999, dtype=np.int32)
    ys_new = np.full(n_chips * n_slots, -9999, dtype=np.int32)
    real   = sort_order_padded >= 0
    xs_new[real] = xs[sort_order_padded[real]]
    ys_new[real] = ys[sort_order_padded[real]]

    chip_x_bin = (unique_ids // 1_000_000).astype(np.int32)
    chip_y_bin = (unique_ids %  1_000_000).astype(np.int32)

    return sort_order_padded, xs_new, ys_new, chip_x_bin, chip_y_bin, chip_counts


def auto_chips_per_batch(n_bands, n_pix_full, budget_gb):
    """Largest chip batch that keeps peak RAM within budget."""
    src_ts_gb = n_bands * n_pix_full * 2 / 1024**3
    avail_gb  = budget_gb - src_ts_gb
    if avail_gb <= 0:
        raise RuntimeError(
            f"Source timestep alone needs {src_ts_gb:.2f} GB "
            f"but budget is {budget_gb} GB.  Increase RAM_BUDGET_GB."
        )
    n_slots  = CHIP_SIZE ** 2
    n_chips  = max(1, int(avail_gb * 1024**3 / (N_TS * n_bands * n_slots * 2)))
    return n_chips, src_ts_gb


# ── Main ───────────────────────────────────────────────────────────────────────

def extract(dry_run=False):
    t_wall = time.perf_counter()

    # ── Source metadata ───────────────────────────────────────────────────────
    print(f"Source : {SRC_PATH}")
    with h5py.File(SRC_PATH, 'r') as f:
        xs        = f['xs'][:]
        ys        = f['ys'][:]
        ts_all    = f['ts'][:]
        n_ts_src  = f['values'].shape[0]
        n_bands   = f['values'].shape[1]
        n_pix     = f['values'].shape[2]
        src_attrs = dict(f.attrs)

    # Select the most recent N_TS timestamps
    n_ts_use   = min(N_TS, n_ts_src)
    src_ts_idx = np.arange(n_ts_src - n_ts_use, n_ts_src)   # last N_TS indices
    ts_selected = ts_all[src_ts_idx]
    date_first  = date.fromordinal(int(ts_selected[0]))
    date_last   = date.fromordinal(int(ts_selected[-1]))

    mb_per_ts = n_bands * n_pix * 2 / 1e6
    print(f"  {n_ts_src} total ts | {n_bands} bands | {n_pix:,} pixels")
    print(f"  Extracting last {n_ts_use} ts: {date_first} → {date_last}")
    print(f"  Source ts buffer: {mb_per_ts:.0f} MB/ts")

    # ── Chip layout ────────────────────────────────────────────────────────────
    print("\nBuilding chip layout …")
    (sort_order_padded, xs_new, ys_new,
     chip_x_bin, chip_y_bin, chip_pixel_count) = build_chip_layout(xs, ys)

    n_chips   = len(chip_x_bin)
    n_slots   = CHIP_SIZE ** 2
    n_pix_dst = n_chips * n_slots
    padding   = (n_pix_dst - n_pix) / n_pix_dst * 100
    print(f"  {n_chips} chips × {n_slots} slots = {n_pix_dst:,} padded pixels "
          f"(padding {padding:.1f} %)")

    # ── Batch sizing ───────────────────────────────────────────────────────────
    chips_per_batch, src_ts_gb = auto_chips_per_batch(n_bands, n_pix, RAM_BUDGET_GB)
    n_batches    = int(np.ceil(n_chips / chips_per_batch))
    total_reads  = n_batches * n_ts_use
    accum_gb     = N_TS * n_bands * chips_per_batch * n_slots * 2 / 1024**3

    dst_path = SRC_PATH.with_name(
        f"{SRC_PATH.stem}_{n_ts_use}ts"
        f"_{date_first.strftime('%Y%m%d')}_{date_last.strftime('%Y%m%d')}.h5"
    )

    print(f"\nRAM budget: {RAM_BUDGET_GB} GB")
    print(f"  source ts buffer : {src_ts_gb:.2f} GB")
    print(f"  accumulator      : {accum_gb:.2f} GB  "
          f"({chips_per_batch} chips × {n_ts_use} ts)")
    print(f"  peak (estimate)  : {src_ts_gb + accum_gb:.2f} GB")
    print(f"\nPlan:")
    print(f"  {n_batches} spatial batch(es) × {n_ts_use} source reads = "
          f"{total_reads} total reads")
    print(f"  Estimated time: {total_reads * 1.5 / 60:.1f} – "
          f"{total_reads * 2.0 / 60:.1f} min  (1.5–2 s/ts)")
    print(f"\nOutput : {dst_path}")
    print(f"  shape  : ({n_ts_use}, {n_bands}, {n_pix_dst:,})")
    print(f"  chunks : ({n_ts_use}, {n_bands}, {n_slots})  ← one chunk per chip")
    est_size_gb = n_ts_use * n_bands * n_pix_dst * 2 / 1024**3 / 8.3
    print(f"  size   : ~{est_size_gb:.1f} GB compressed")

    if dry_run:
        print("\n[dry-run] Stopping before any file I/O.")
        return

    # ── Create destination ─────────────────────────────────────────────────────
    print(f"\nCreating {dst_path} …")
    with h5py.File(dst_path, 'w') as dst:
        dst.create_dataset(
            'values',
            shape=(n_ts_use, n_bands, n_pix_dst),
            dtype='uint16',
            chunks=(n_ts_use, n_bands, n_slots),  # one chunk per chip
            compression=COMPRESSION,
            fillvalue=NODATA_VAL,
        )
        dst.create_dataset('sort_order',       data=sort_order_padded, dtype='int64')
        dst.create_dataset('xs_new',           data=xs_new,            dtype='int32')
        dst.create_dataset('ys_new',           data=ys_new,            dtype='int32')
        dst.create_dataset('chip_x_bin',       data=chip_x_bin,        dtype='int32')
        dst.create_dataset('chip_y_bin',       data=chip_y_bin,        dtype='int32')
        dst.create_dataset('chip_pixel_count', data=chip_pixel_count,  dtype='int32')
        dst.create_dataset('ts',               data=ts_selected,       dtype='int32')
        dst.attrs['chip_size']  = CHIP_SIZE
        dst.attrs['pixel_res']  = PIXEL_RES
        dst.attrs['n_ts']       = n_ts_use
        dst.attrs['nodata_val'] = NODATA_VAL
        dst.attrs['date_first'] = str(date_first)
        dst.attrs['date_last']  = str(date_last)
        for k, v in src_attrs.items():
            dst.attrs[k] = v
    print("  Metadata written.\n")

    # ── Fill values ────────────────────────────────────────────────────────────
    reads_done = 0
    t0 = time.perf_counter()

    with h5py.File(dst_path, 'a') as dst, h5py.File(SRC_PATH, 'r') as src:
        dset = dst['values']

        for sp in range(n_batches):
            chip_s      = sp * chips_per_batch
            chip_e      = min(chip_s + chips_per_batch, n_chips)
            pix_s       = chip_s * n_slots
            pix_e       = chip_e * n_slots
            n_pix_batch = pix_e - pix_s

            orig_idx   = sort_order_padded[pix_s:pix_e]
            valid      = orig_idx >= 0
            orig_valid = orig_idx[valid]

            print(f"Batch {sp+1}/{n_batches}: chips {chip_s}–{chip_e-1}  "
                  f"({valid.sum():,} real / {n_pix_batch:,} padded pixels)")

            buf = np.full((n_ts_use, n_bands, n_pix_batch), NODATA_VAL, dtype=np.uint16)

            for t_local, t_src in enumerate(src_ts_idx):
                full_ts = src['values'][int(t_src), :, :]   # (B, P_all)
                buf[t_local][:, valid] = full_ts[:, orig_valid]
                del full_ts
                reads_done += 1

                elapsed = time.perf_counter() - t0
                rate    = reads_done / elapsed if elapsed > 0 else 0
                eta_s   = (total_reads - reads_done) / rate if rate > 0 else 0
                print(f"  ts {t_local+1:>3}/{n_ts_use}  "
                      f"({date.fromordinal(int(ts_selected[t_local]))})  "
                      f"{reads_done}/{total_reads} reads  "
                      f"ETA {timedelta(seconds=int(eta_s))}",
                      end='\r')

            dset[:, :, pix_s:pix_e] = buf
            del buf
            print()

    total_elapsed = time.perf_counter() - t_wall
    size_gb = dst_path.stat().st_size / 1e9
    print(f"\nDone in {timedelta(seconds=int(total_elapsed))}.")
    print(f"Output: {dst_path}  ({size_gb:.2f} GB)")


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--dry', action='store_true',
                        help='Print the plan without writing any files.')
    extract(dry_run=parser.parse_args().dry)
