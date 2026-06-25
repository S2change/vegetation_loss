#!/usr/bin/env python3
"""Print the metadata of a chip-chunked tile HDF5 without reading any blocks.

Usage:
    python inspect_hdf5.py /path/to/tile.h5

Dumps, for the given file:
  - every dataset's shape / dtype / chunking / on-disk size
  - the number of Sentinel-2 bands (the `values` band axis, axis 1)
  - the timestep count + ordinal-date span (decoded to ISO)
  - the chip grid extent + derived block grid
  - the uint16 nodata sentinel
  - every file-level attribute

The number of S2 bands is `values.shape[1]` — in the layout this pipeline
expects (see `processes/input_setup/hdf5_reader.py`) that axis is the 10
Sentinel-2 bands. This script reports it as `n_bands` and warns if it isn't 10,
so a mismatched export is obvious at a glance.
"""
from __future__ import annotations

import sys
from datetime import date

import h5py
import numpy as np

# The layout this pipeline expects: values is (N_TS, N_BANDS, n_chips*65536),
# so the band axis is axis 1, and a tile-CRS S2 export has 10 bands.
BAND_AXIS = 1
EXPECTED_S2_BANDS = 10


def _human_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n /= 1024
    return f"{n:.1f} TB"


def _decode(v):
    """Render an attribute value readably (bytes -> str, arrays summarised)."""
    if isinstance(v, bytes):
        return v.decode(errors="replace")
    if isinstance(v, np.ndarray):
        if v.size <= 8:
            return np.array2string(v, separator=", ")
        return f"<array shape={v.shape} dtype={v.dtype}>"
    return v


def inspect(hdf5_path: str) -> None:
    with h5py.File(hdf5_path, "r") as h5f:
        print(f"\n=== HDF5 metadata: {hdf5_path} ===\n")

        # ── Datasets ──────────────────────────────────────────────────────
        print("Datasets:")
        datasets: dict[str, h5py.Dataset] = {}
        for name in h5f:
            obj = h5f[name]
            if not isinstance(obj, h5py.Dataset):
                continue
            datasets[name] = obj
            nbytes = obj.dtype.itemsize * int(np.prod(obj.shape)) if obj.shape else 0
            chunks = obj.chunks if obj.chunks is not None else "contiguous"
            print(f"  {name:20s} shape={str(obj.shape):28s} "
                  f"dtype={str(obj.dtype):8s} chunks={chunks}  "
                  f"~{_human_bytes(nbytes)}")

        # ── Band count (the thing you're checking) ────────────────────────
        print()
        if "values" in datasets and datasets["values"].ndim > BAND_AXIS:
            n_bands = int(datasets["values"].shape[BAND_AXIS])
            n_ts = int(datasets["values"].shape[0])
            tag = "" if n_bands == EXPECTED_S2_BANDS else (
                f"  <-- WARNING: pipeline expects {EXPECTED_S2_BANDS}")
            print(f"S2 bands (values axis {BAND_AXIS}): {n_bands}{tag}")
            print(f"Timesteps   (values axis 0):       {n_ts}")
        else:
            print("No `values` dataset found — cannot report band count.")

        # ── Timestep span ─────────────────────────────────────────────────
        if "ts" in datasets:
            ts = datasets["ts"][:]
            if ts.size:
                lo, hi = int(ts.min()), int(ts.max())
                print(f"Date span:  {date.fromordinal(lo).isoformat()} -> "
                      f"{date.fromordinal(hi).isoformat()}  "
                      f"({ts.size} timesteps)")

        # ── Chip / block grid ─────────────────────────────────────────────
        if "chip_x_bin" in datasets and "chip_y_bin" in datasets:
            cx = datasets["chip_x_bin"][:]
            cy = datasets["chip_y_bin"][:]
            n_chips = int(cx.size)
            if n_chips:
                max_x, max_y = int(cx.max()), int(cy.max())
                # Block grid mirrors hdf5_reader: LIVE 4x4 chips per block.
                live = 4
                n_block_rows = (max_y + live) // live
                n_block_cols = (max_x + live) // live
                print(f"Chips present: {n_chips:,}  "
                      f"(chip grid up to row {max_y}, col {max_x})")
                print(f"Block grid:    {n_block_rows} x {n_block_cols} "
                      f"= {n_block_rows * n_block_cols} blocks")

        # ── nodata ─────────────────────────────────────────────────────────
        nodata = h5f.attrs.get("nodata_val")
        if nodata is not None:
            print(f"nodata_val:    {_decode(nodata)}")

        # ── File attributes ───────────────────────────────────────────────
        print("\nFile attributes:")
        if len(h5f.attrs) == 0:
            print("  (none)")
        for k in sorted(h5f.attrs):
            print(f"  {k:18s} = {_decode(h5f.attrs[k])}")
        print()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python inspect_hdf5.py <hdf5_path>", file=sys.stderr)
        sys.exit(1)
    inspect(sys.argv[1])
