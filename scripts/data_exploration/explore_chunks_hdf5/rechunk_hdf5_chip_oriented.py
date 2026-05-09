"""
rechunk_hdf5_chip_oriented.py

Rewrites an HDF5 time series from temporal chunks  (1,  B, P_all)
into chip-oriented chunks                          (T_CHUNK, B, CHIP_SIZE²).

After rechunking, reading all timestamps for one chip costs one sequential
read per temporal window instead of decompressing the entire tile per timestamp.

Layout of the output dataset
-----------------------------
  values : (T, B, N_chips × CHIP_SIZE²) uint16
            Pixels are sorted so all CHIP_SIZE² slots of chip c occupy
            columns [c*CHIP_SIZE² : (c+1)*CHIP_SIZE²].
            Boundary chips that have fewer than CHIP_SIZE² real pixels are
            zero-padded with NODATA_VAL.

Companion datasets written to the output file
---------------------------------------------
  sort_order   : (N_chips × CHIP_SIZE²,) int64
                 sort_order[i] = original pixel index (in the source file)
                 that maps to new position i.  -1 = padding slot.
  xs_new       : (N_chips × CHIP_SIZE²,) int32  — easting  (padding → -9999)
  ys_new       : (N_chips × CHIP_SIZE²,) int32  — northing (padding → -9999)
  chip_x_bin   : (N_chips,) int32  — chip column in the chip grid
  chip_y_bin   : (N_chips,) int32  — chip row    in the chip grid
  chip_pixel_count : (N_chips,) int32 — real (non-padding) pixels per chip
  ts           : copied from source

Memory budget
-------------
Set RAM_BUDGET_GB to slightly below your available RAM.  The script
auto-sizes the spatial batch (chips per pass) so that
    source_ts_buffer  +  accumulation_buffer  ≤ RAM_BUDGET_GB.

Multi-pass cost
---------------
n_passes ≈ ceil(N_chips / chips_per_batch)
Each pass reads all T timestamps of the source once.  Expected wall time is
roughly  n_passes × T × t_per_ts  where t_per_ts ≈ 1–2 s/ts for a
LZF-compressed file on a fast local disk.

Usage
-----
    python rechunk_hdf5_chip_oriented.py          # full run
    python rechunk_hdf5_chip_oriented.py --dry    # plan only, no writing
"""

import argparse
import sys
import time
from datetime import timedelta
from pathlib import Path

import h5py
import numpy as np

# ── Configuration ──────────────────────────────────────────────────────────────
SRC_PATH      = Path(r"H:\outputs_ROI\hdf5\T29TPG\T29TPG.h5")
DST_PATH      = SRC_PATH.with_name(SRC_PATH.stem + "_chip_chunked.h5")

CHIP_SIZE     = 256     # pixels per chip side (256 × 256 = 65 536 pixels/chip)
PIXEL_RES     = 10      # Sentinel-2 native resolution, metres
T_CHUNK       = 48      # temporal chunk size (≈ 2 months of S2 at ~1.25-day revisit)
NODATA_VAL    = 65535
RAM_BUDGET_GB = 54.0    # 54 GB fits all ~900 chips in one spatial pass on a 64 GB machine
                        # (leaves ~10 GB for OS + Python overhead)
                        # Reduce to 4.0 for HPC nodes with 5 GB RAM (adds ~20 passes)
COMPRESSION   = 'lzf'   # set None for uncompressed output (≈6× larger file)

# ── Chip assignment ─────────────────────────────────────────────────────────────

def build_chip_layout(xs, ys):
    """Assign every pixel to a chip cell and build the padded sort order.

    Returns
    -------
    sort_order_padded : (N_chips * CHIP_SIZE**2,) int64
        Maps each new flat index to an original pixel index.
        -1 for padding slots (no source pixel).
    xs_new, ys_new : (N_chips * CHIP_SIZE**2,) int32
        Coordinates in new layout order; -9999 at padding slots.
    chip_x_bin, chip_y_bin : (N_chips,) int32
        Chip-grid coordinates of each chip.
    chip_pixel_count : (N_chips,) int32
        Number of real pixels in each chip (≤ CHIP_SIZE**2).
    """
    n_slots = CHIP_SIZE ** 2
    chip_m  = CHIP_SIZE * PIXEL_RES                               # metres per chip side
    x_bins  = ((xs.astype(np.int64) - int(xs.min())) // chip_m).astype(np.int32)
    y_bins  = ((int(ys.max()) - ys.astype(np.int64)) // chip_m).astype(np.int32)
    chip_id = x_bins.astype(np.int64) * 1_000_000 + y_bins.astype(np.int64)

    unique_ids, pixel_chip = np.unique(chip_id, return_inverse=True)
    n_chips     = len(unique_ids)
    chip_counts = np.bincount(pixel_chip, minlength=n_chips).astype(np.int32)

    # Sort pixels by chip so each chip's pixels are contiguous
    order = np.argsort(pixel_chip, kind='stable')

    # Build padded sort order: chip c occupies slots [c*n_slots : (c+1)*n_slots]
    sort_order_padded = np.full(n_chips * n_slots, -1, dtype=np.int64)
    chip_starts_src = np.zeros(n_chips + 1, dtype=np.int64)
    np.cumsum(chip_counts, out=chip_starts_src[1:])
    for c in range(n_chips):
        s, e = chip_starts_src[c], chip_starts_src[c + 1]
        sort_order_padded[c * n_slots : c * n_slots + (e - s)] = order[s:e]

    # Coordinates in new order (-9999 for padding)
    xs_new = np.full(n_chips * n_slots, -9999, dtype=np.int32)
    ys_new = np.full(n_chips * n_slots, -9999, dtype=np.int32)
    real   = sort_order_padded >= 0
    xs_new[real] = xs[sort_order_padded[real]]
    ys_new[real] = ys[sort_order_padded[real]]

    chip_x_bin = (unique_ids // 1_000_000).astype(np.int32)
    chip_y_bin = (unique_ids %  1_000_000).astype(np.int32)

    return sort_order_padded, xs_new, ys_new, chip_x_bin, chip_y_bin, chip_counts


def auto_chips_per_batch(n_bands, n_pix_full, budget_gb):
    """Largest chip batch that keeps peak RAM within budget.

    Peak RAM per iteration:
      source_ts  = n_bands × n_pix_full × 2  bytes   (one full timestep, uint16)
      accumulator = T_CHUNK × n_bands × chips × CHIP_SIZE² × 2  bytes
    """
    src_ts_gb = n_bands * n_pix_full * 2 / 1024**3
    avail_gb  = budget_gb - src_ts_gb
    if avail_gb <= 0:
        raise RuntimeError(
            f"Source timestep alone needs {src_ts_gb:.2f} GB "
            f"but budget is {budget_gb} GB.  Increase RAM_BUDGET_GB."
        )
    n_slots = CHIP_SIZE ** 2
    n_chips = max(1, int(avail_gb * 1024**3 / (T_CHUNK * n_bands * n_slots * 2)))
    return n_chips, src_ts_gb


# ── Main conversion ─────────────────────────────────────────────────────────────

def rechunk(dry_run=False):
    t_wall = time.perf_counter()

    # ── Load source metadata ──────────────────────────────────────────────────
    print(f"Source : {SRC_PATH}")
    with h5py.File(SRC_PATH, 'r') as f:
        xs         = f['xs'][:]
        ys         = f['ys'][:]
        ts_arr     = f['ts'][:]
        n_ts       = f['values'].shape[0]
        n_bands    = f['values'].shape[1]
        n_pix      = f['values'].shape[2]
        src_chunk  = f['values'].chunks
        src_comp   = f['values'].compression
        src_attrs  = dict(f.attrs)

    mb_per_ts = n_bands * n_pix * 2 / 1e6
    print(f"  {n_ts} ts  ×  {n_bands} bands  ×  {n_pix:,} pixels")
    print(f"  Source chunks: {src_chunk}  compression: {src_comp}")
    print(f"  Uncompressed size per ts: {mb_per_ts:.1f} MB")

    # ── Chip layout ────────────────────────────────────────────────────────────
    print("\nAssigning pixels to chip grid …")
    (sort_order_padded, xs_new, ys_new,
     chip_x_bin, chip_y_bin, chip_pixel_count) = build_chip_layout(xs, ys)

    n_chips   = len(chip_x_bin)
    n_slots   = CHIP_SIZE ** 2
    n_pix_dst = n_chips * n_slots
    padding   = (n_pix_dst - n_pix) / n_pix_dst * 100
    print(f"  {n_chips} chips  ×  {n_slots} slots  =  {n_pix_dst:,} padded pixels")
    print(f"  Padding overhead: {padding:.1f} %")

    # ── Batch sizing ───────────────────────────────────────────────────────────
    chips_per_batch, src_ts_gb = auto_chips_per_batch(n_bands, n_pix, RAM_BUDGET_GB)
    n_spatial_batches = int(np.ceil(n_chips / chips_per_batch))
    n_ts_batches      = int(np.ceil(n_ts / T_CHUNK))
    total_ts_reads    = n_spatial_batches * n_ts

    accum_gb = T_CHUNK * n_bands * chips_per_batch * n_slots * 2 / 1024**3
    print(f"\nRAM budget: {RAM_BUDGET_GB} GB")
    print(f"  source ts buffer : {src_ts_gb:.2f} GB")
    print(f"  accumulation buf : {accum_gb:.2f} GB  "
          f"({chips_per_batch} chips × {T_CHUNK} ts)")
    print(f"  peak (estimate)  : {src_ts_gb + accum_gb:.2f} GB")
    print(f"\nPlan:")
    print(f"  {n_spatial_batches} spatial batches  ×  {n_ts_batches} temporal batches")
    print(f"  {total_ts_reads:,} source ts reads total")
    print(f"  Estimated time   : {total_ts_reads * 1.5 / 3600:.1f} – "
          f"{total_ts_reads * 2.0 / 3600:.1f} h  (1.5–2 s/ts estimate)")
    print(f"\nOutput : {DST_PATH}")
    print(f"  dataset shape : ({n_ts}, {n_bands}, {n_pix_dst:,})")
    print(f"  chunk shape   : ({T_CHUNK}, {n_bands}, {n_slots})")
    print(f"  compression   : {COMPRESSION}")

    if dry_run:
        print("\n[dry-run] Stopping before any file I/O.")
        return

    # ── Create destination ─────────────────────────────────────────────────────
    print(f"\nCreating {DST_PATH} …")
    with h5py.File(DST_PATH, 'w') as dst:
        dst.create_dataset(
            'values',
            shape=(n_ts, n_bands, n_pix_dst),
            dtype='uint16',
            chunks=(T_CHUNK, n_bands, n_slots),
            compression=COMPRESSION,
            fillvalue=NODATA_VAL,
        )
        dst.create_dataset('sort_order',        data=sort_order_padded, dtype='int64')
        dst.create_dataset('xs_new',            data=xs_new,            dtype='int32')
        dst.create_dataset('ys_new',            data=ys_new,            dtype='int32')
        dst.create_dataset('chip_x_bin',        data=chip_x_bin,        dtype='int32')
        dst.create_dataset('chip_y_bin',        data=chip_y_bin,        dtype='int32')
        dst.create_dataset('chip_pixel_count',  data=chip_pixel_count,  dtype='int32')
        dst.create_dataset('ts',                data=ts_arr,            dtype='int32')
        dst.attrs['chip_size']  = CHIP_SIZE
        dst.attrs['pixel_res']  = PIXEL_RES
        dst.attrs['t_chunk']    = T_CHUNK
        dst.attrs['nodata_val'] = NODATA_VAL
        for k, v in src_attrs.items():
            dst.attrs[k] = v
    print("  Metadata written.\n")

    # ── Fill values dataset ────────────────────────────────────────────────────
    reads_done = 0
    t0 = time.perf_counter()

    with h5py.File(DST_PATH, 'a') as dst, h5py.File(SRC_PATH, 'r') as src:
        dset = dst['values']

        for sp in range(n_spatial_batches):
            chip_s = sp * chips_per_batch
            chip_e = min(chip_s + chips_per_batch, n_chips)
            pix_s  = chip_s * n_slots
            pix_e  = chip_e * n_slots
            n_pix_batch = pix_e - pix_s

            orig_idx   = sort_order_padded[pix_s:pix_e]   # (n_pix_batch,) possibly -1
            valid      = orig_idx >= 0                     # boolean mask, shape (n_pix_batch,)
            orig_valid = orig_idx[valid]                   # original pixel indices, precomputed

            print(f"Spatial batch {sp+1}/{n_spatial_batches} "
                  f"(chips {chip_s}–{chip_e-1}, "
                  f"{valid.sum():,} real pixels of {n_pix_batch:,} slots)")

            for tb in range(n_ts_batches):
                ts_s = tb * T_CHUNK
                ts_e = min(ts_s + T_CHUNK, n_ts)
                n_t  = ts_e - ts_s

                buf = np.full((n_t, n_bands, n_pix_batch), NODATA_VAL, dtype=np.uint16)

                for t_local, t_src in enumerate(range(ts_s, ts_e)):
                    full_ts = src['values'][t_src, :, :]    # (n_bands, P_all) uint16
                    # buf[t_local] is a (n_bands, n_pix_batch) VIEW of buf.
                    # Applying the boolean mask on axis-1 of the view avoids the
                    # numpy advanced-indexing axis-transposition that occurs when
                    # mixing a scalar index and a boolean mask in one expression
                    # (e.g. buf[t_local, :, valid] yields shape (n_valid, n_bands)
                    # not (n_bands, n_valid)).
                    buf[t_local][:, valid] = full_ts[:, orig_valid]
                    del full_ts
                    reads_done += 1

                dset[ts_s:ts_e, :, pix_s:pix_e] = buf
                del buf

                # Progress line
                elapsed = time.perf_counter() - t0
                rate    = reads_done / elapsed if elapsed > 0 else 0
                eta     = (total_ts_reads - reads_done) / rate if rate > 0 else 0
                print(f"  ts {ts_s:>5}:{ts_e:<5}  "
                      f"{reads_done}/{total_ts_reads} reads  "
                      f"{rate:.1f} reads/s  "
                      f"ETA {timedelta(seconds=int(eta))}",
                      end='\r')

            print()  # newline after \r block

    total_elapsed = time.perf_counter() - t_wall
    size_gb = DST_PATH.stat().st_size / 1e9
    print(f"\nDone in {timedelta(seconds=int(total_elapsed))}.")
    print(f"Output size: {size_gb:.1f} GB  ({DST_PATH})")


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dry', action='store_true',
                        help='Print the plan without writing any files.')
    args = parser.parse_args()
    rechunk(dry_run=args.dry)
