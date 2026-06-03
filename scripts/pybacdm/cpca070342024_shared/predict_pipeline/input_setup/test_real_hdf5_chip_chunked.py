"""Tests for the chip-chunked HDF5 reader.

Builds tiny synthetic chip-chunked HDF5 files in a temp directory matching
the schema from `rechunk_hdf5_chip_oriented.py`, then asserts the reader
returns the right block content, handles missing chips correctly, and
applies the q02/q98 stretch the same way as `dataset_swin_GZ._to_uint8`.

Run:
    python test_input_setup.py
"""
import os
import sys
import tempfile

import h5py
import numpy as np


# ============================================================================
# REAL-FILE VALIDATION  (no hdf5_reader dependency)
# Run with:  python hdf5_chip_chunked_validation.py --real
# ============================================================================

import argparse
from pathlib import Path

REAL_HDF5_PATH  = Path(r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\testes_cnca_filtar_hdf5_nuvems\exemplos_geotiff_CNCA\hdf5\T29TPG.h5")
_CHIP_PIXELS    = 256 * 256   # 65 536 slots per chip
_COORD_NODATA   = -9999       # sentinel for xs_new / ys_new at padding slots
_BAND_NODATA    = 65535       # uint16 nodata for values

REQUIRED_DATASETS = [
    "values", "ts", "sort_order",
    "xs_new", "ys_new",
    "chip_x_bin", "chip_y_bin", "chip_pixel_count",
]
REQUIRED_ATTRS = [
    "band_names", "crs", "chip_size", "pixel_res",
    "n_ts", "nodata_val", "date_first", "date_last",
    "bounds_left", "bounds_right", "bounds_bottom", "bounds_top",
]


def _open(path=REAL_HDF5_PATH):
    return h5py.File(path, "r")


def test_real_required_datasets(path=REAL_HDF5_PATH):
    with _open(path) as f:
        missing = [k for k in REQUIRED_DATASETS if k not in f]
    assert not missing, f"Missing datasets: {missing}"
    print("  required datasets present — OK")


def test_real_required_attributes(path=REAL_HDF5_PATH):
    with _open(path) as f:
        missing = [k for k in REQUIRED_ATTRS if k not in f.attrs]
    assert not missing, f"Missing attributes: {missing}"
    print("  required attributes present — OK")


def test_real_values_shape(path=REAL_HDF5_PATH):
    with _open(path) as f:
        n_ts, n_bands, n_pix_dst = f["values"].shape
        n_chips = len(f["chip_x_bin"])
        n_ts_attr = int(f.attrs["n_ts"])
        n_bands_attr = len(f.attrs["band_names"])
    assert n_ts == n_ts_attr, f"values.shape[0]={n_ts} but n_ts attr={n_ts_attr}"
    assert n_bands == n_bands_attr, f"values.shape[1]={n_bands} but band_names length={n_bands_attr}"
    assert n_pix_dst == n_chips * _CHIP_PIXELS, (
        f"values.shape[2]={n_pix_dst} != n_chips*CHIP_PIXELS={n_chips}*{_CHIP_PIXELS}={n_chips*_CHIP_PIXELS}"
    )
    print(f"  values shape ({n_ts}, {n_bands}, {n_pix_dst:,}) = {n_chips} chips × {_CHIP_PIXELS} — OK")


def test_real_values_chunks(path=REAL_HDF5_PATH):
    with _open(path) as f:
        chunks = f["values"].chunks
    assert chunks is not None, "values dataset has no chunking"
    assert chunks[2] == _CHIP_PIXELS, (
        f"chunk spatial dim={chunks[2]}, expected {_CHIP_PIXELS} (one chip per chunk)"
    )
    print(f"  values chunks {chunks} — spatial dim matches CHIP_PIXELS — OK")


def test_real_flat_arrays_length(path=REAL_HDF5_PATH):
    with _open(path) as f:
        n_chips   = len(f["chip_x_bin"])
        expected  = n_chips * _CHIP_PIXELS
        for name in ("sort_order", "xs_new", "ys_new"):
            got = f[name].shape[0]
            assert got == expected, f"{name}.shape[0]={got} != n_chips*CHIP_PIXELS={expected}"
    print(f"  sort_order / xs_new / ys_new length = {expected:,} — OK")


def test_real_chip_arrays_length(path=REAL_HDF5_PATH):
    with _open(path) as f:
        n_chips = len(f["chip_x_bin"])
        for name in ("chip_y_bin", "chip_pixel_count"):
            got = f[name].shape[0]
            assert got == n_chips, f"{name}.shape[0]={got} != n_chips={n_chips}"
    print(f"  chip_x_bin / chip_y_bin / chip_pixel_count length = {n_chips} — OK")


def test_real_sort_order_values(path=REAL_HDF5_PATH):
    with _open(path) as f:
        so            = f["sort_order"][:]
        n_valid_pixels = int(f["chip_pixel_count"][:].sum())
    valid_mask = so >= 0
    assert so[~valid_mask].all() == False or (so[~valid_mask] == -1).all(), \
        "sort_order has negative values other than -1"
    max_idx = int(so[valid_mask].max()) if valid_mask.any() else -1
    assert max_idx < n_valid_pixels, (
        f"sort_order max index {max_idx} >= n_valid_pixels {n_valid_pixels}"
    )
    print(f"  sort_order range [-1, {max_idx}] within [-1, {n_valid_pixels}) -- OK")


def test_real_sort_order_padding_count(path=REAL_HDF5_PATH):
    with _open(path) as f:
        so             = f["sort_order"][:]
        chip_counts    = f["chip_pixel_count"][:]
        n_chips        = len(chip_counts)
    n_valid   = int(chip_counts.sum())
    n_padding = int((so == -1).sum())
    expected_padding = n_chips * _CHIP_PIXELS - n_valid
    assert n_padding == expected_padding, (
        f"padding slots: got {n_padding}, expected {expected_padding} "
        f"(= {n_chips}x{_CHIP_PIXELS} - {n_valid})"
    )
    pct = n_padding / (n_chips * _CHIP_PIXELS) * 100
    print(f"  padding slots {n_padding:,} / {n_chips*_CHIP_PIXELS:,} ({pct:.1f}%) — OK")


def test_real_coord_nodata_alignment(path=REAL_HDF5_PATH):
    with _open(path) as f:
        so     = f["sort_order"][:]
        xs_new = f["xs_new"][:]
        ys_new = f["ys_new"][:]
    padding = so == -1
    assert (xs_new[padding]  == _COORD_NODATA).all(), "xs_new not COORD_NODATA at padding slots"
    assert (ys_new[padding]  == _COORD_NODATA).all(), "ys_new not COORD_NODATA at padding slots"
    assert (xs_new[~padding] != _COORD_NODATA).all(), "xs_new has COORD_NODATA at non-padding slots"
    assert (ys_new[~padding] != _COORD_NODATA).all(), "ys_new has COORD_NODATA at non-padding slots"
    print("  xs_new / ys_new COORD_NODATA alignment with sort_order — OK")


def test_real_chip_ids_unique(path=REAL_HDF5_PATH):
    with _open(path) as f:
        xb = f["chip_x_bin"][:]
        yb = f["chip_y_bin"][:]
    pairs = set(zip(xb.tolist(), yb.tolist()))
    assert len(pairs) == len(xb), \
        f"Duplicate (chip_x_bin, chip_y_bin) pairs: {len(xb) - len(pairs)} duplicates"
    print(f"  {len(pairs)} unique chip (x_bin, y_bin) pairs — OK")


def test_real_ts_monotone(path=REAL_HDF5_PATH):
    with _open(path) as f:
        ts = f["ts"][:]
    assert (np.diff(ts) > 0).all(), f"ts is not strictly increasing: {ts}"
    from datetime import date
    d0 = date.fromordinal(int(ts[0]))
    d1 = date.fromordinal(int(ts[-1]))
    print(f"  ts strictly increasing: {d0} -> {d1} ({len(ts)} timestamps) -- OK")


def test_real_valid_data_exists(path=REAL_HDF5_PATH):
    with _open(path) as f:
        _, n_bands, _ = f["values"].shape
        # Sample the first chip of the first timestamp across all bands
        chunk = f["values"][0, :, 0:_CHIP_PIXELS]  # (n_bands, CHIP_PIXELS)
    valid = chunk != _BAND_NODATA
    n_valid = int(valid.sum())
    assert n_valid > 0, "First chip / first timestamp has no valid (non-nodata) pixels"
    print(f"  first chip ts=0: {n_valid:,} valid pixels across {n_bands} bands — OK")


def test_real_all_chips_have_valid_data(path=REAL_HDF5_PATH):
    """Every chip must have at least one non-nodata pixel somewhere across all
    timestamps and bands.  Reads band 0 across all timestamps (one pass) then
    checks per-chip, so memory cost is n_ts * n_pix_dst * 2 bytes (~375 MB for
    this file)."""
    with _open(path) as f:
        n_chips       = len(f["chip_x_bin"])
        chip_counts   = f["chip_pixel_count"][:]
        sort_order    = f["sort_order"][:]
        # Read all timestamps, band 0: shape (n_ts, n_pix_dst)
        band0         = f["values"][:, 0, :]
    empty_chips = []
    for c in range(n_chips):
        s = c * _CHIP_PIXELS
        e = s + _CHIP_PIXELS
        n_real = int(chip_counts[c])
        if n_real == 0:
            empty_chips.append(c)
            continue
        # Non-padding slots for this chip
        chip_so = sort_order[s:e]
        real_mask = chip_so >= 0          # shape (CHIP_PIXELS,)
        slab = band0[:, s:e]              # (n_ts, CHIP_PIXELS)
        has_valid = (slab[:, real_mask] != _BAND_NODATA).any()
        if not has_valid:
            empty_chips.append(c)
    assert not empty_chips, (
        f"{len(empty_chips)} chip(s) have all-nodata across every timestamp: "
        f"chip indices {empty_chips[:10]}{'...' if len(empty_chips) > 10 else ''}"
    )
    print(f"  all {n_chips} chips have valid data in at least one timestamp/pixel — OK")


def test_real_padding_slots_are_nodata(path=REAL_HDF5_PATH):
    """Padding slots (sort_order == -1) must hold the nodata fill value in
    values.  Checks band 0, timestamp 0 for all chips in one sequential read."""
    with _open(path) as f:
        sort_order = f["sort_order"][:]
        ts0_b0     = f["values"][0, 0, :]   # shape (n_pix_dst,)
        nodata_val = int(f.attrs["nodata_val"])
    padding_mask = sort_order == -1
    bad = (ts0_b0[padding_mask] != nodata_val).sum()
    assert bad == 0, (
        f"{bad} padding slots have a value other than nodata ({nodata_val}) "
        f"in values[ts=0, band=0]"
    )
    print(f"  all {int(padding_mask.sum()):,} padding slots hold nodata in values[0,0] — OK")


def test_real_chip_spatial_square(path=REAL_HDF5_PATH):
    """Each chip's pixels must lie within the expected 256x256 geographic square.

    For chip c with bin indices (chip_x_bin[c], chip_y_bin[c]) the expected
    pixel upper-left corners span:
        x in [x0,  x0 + (chip_side-1)*pixel_res]
        y in [y0 - (chip_side-1)*pixel_res,  y0]
    where
        x0 = x_global_min + chip_x_bin[c] * chip_side * pixel_res
        y0 = y_global_max - chip_y_bin[c] * chip_side * pixel_res
    """
    with _open(path) as f:
        xs_new    = f["xs_new"][:]
        ys_new    = f["ys_new"][:]
        chip_xb   = f["chip_x_bin"][:]
        chip_yb   = f["chip_y_bin"][:]
        pixel_res = int(f.attrs["pixel_res"])
        chip_side = int(f.attrs["chip_size"])

    real         = xs_new != _COORD_NODATA
    x_global_min = int(xs_new[real].min())
    y_global_max = int(ys_new[real].max())
    chip_m       = chip_side * pixel_res   # metres per chip side (2 560 m)
    half_px      = (chip_side - 1) * pixel_res  # max offset within chip (2 550 m)
    n_chips      = len(chip_xb)

    bad_chips = []
    for c in range(n_chips):
        s = c * _CHIP_PIXELS
        e = s + _CHIP_PIXELS
        slot_real = real[s:e]
        if not slot_real.any():
            continue   # no real pixels — caught by test_real_all_chips_have_valid_data

        xs_c = xs_new[s:e][slot_real]
        ys_c = ys_new[s:e][slot_real]

        x0   = x_global_min + int(chip_xb[c]) * chip_m
        y0   = y_global_max - int(chip_yb[c]) * chip_m
        x_lo, x_hi = x0, x0 + half_px
        y_lo, y_hi = y0 - half_px, y0

        if xs_c.min() < x_lo or xs_c.max() > x_hi or \
           ys_c.min() < y_lo or ys_c.max() > y_hi:
            bad_chips.append(dict(
                chip=c, xb=int(chip_xb[c]), yb=int(chip_yb[c]),
                xs_range=(int(xs_c.min()), int(xs_c.max())), x_expected=(x_lo, x_hi),
                ys_range=(int(ys_c.min()), int(ys_c.max())), y_expected=(y_lo, y_hi),
            ))

    assert not bad_chips, (
        f"{len(bad_chips)} chip(s) have pixels outside their {chip_side}x{chip_side} square:\n"
        + "\n".join(
            f"  chip {d['chip']} (xb={d['xb']},yb={d['yb']}): "
            f"xs {d['xs_range']} expected {d['x_expected']}, "
            f"ys {d['ys_range']} expected {d['y_expected']}"
            for d in bad_chips[:5]
        )
    )
    print(f"  all {n_chips} chips have pixels within their {chip_side}x{chip_side} "
          f"spatial square ({chip_m}x{chip_m} m) -- OK")


def test_real_bounds_consistent_with_coords(path=REAL_HDF5_PATH):
    with _open(path) as f:
        xs_new = f["xs_new"][:]
        ys_new = f["ys_new"][:]
        bl = float(f.attrs["bounds_left"])
        br = float(f.attrs["bounds_right"])
        bb = float(f.attrs["bounds_bottom"])
        bt = float(f.attrs["bounds_top"])
        pixel_res = int(f.attrs["pixel_res"])
    real = xs_new != _COORD_NODATA
    xs_valid = xs_new[real]
    ys_valid = ys_new[real]
    # xs_new stores upper-left corner of each pixel
    assert float(xs_valid.min()) >= bl - pixel_res, "xs_valid.min out of bounds_left"
    assert float(xs_valid.max()) <= br,              "xs_valid.max out of bounds_right"
    assert float(ys_valid.min()) >= bb - pixel_res,  "ys_valid.min out of bounds_bottom"
    assert float(ys_valid.max()) <= bt,              "ys_valid.max out of bounds_top"
    print(f"  coordinate bounds [{bl}, {br}] × [{bb}, {bt}] consistent — OK")


def main_real_file(path=REAL_HDF5_PATH):
    print(f"\nValidating real HDF5: {path}")
    test_real_required_datasets(path)
    test_real_required_attributes(path)
    test_real_values_shape(path)
    test_real_values_chunks(path)
    test_real_flat_arrays_length(path)
    test_real_chip_arrays_length(path)
    test_real_sort_order_values(path)
    test_real_sort_order_padding_count(path)
    test_real_coord_nodata_alignment(path)
    test_real_chip_ids_unique(path)
    test_real_ts_monotone(path)
    test_real_valid_data_exists(path)
    test_real_all_chips_have_valid_data(path)
    test_real_padding_slots_are_nodata(path)
    test_real_chip_spatial_square(path)
    test_real_bounds_consistent_with_coords(path)
    print("All real-file validation tests passed.")


if __name__ == "__main__":
    main_real_file()
    