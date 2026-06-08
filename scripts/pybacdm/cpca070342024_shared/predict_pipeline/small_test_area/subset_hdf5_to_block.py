"""Carve a small chip-chunked HDF5 out of a full-tile one, by a .gpkg area.

Given a full-tile chip-chunked HDF5 (the format the prediction pipeline
reads — see input_setup/hdf5_reader.py), a test-cell polygon .gpkg, and the
fire/cut change-reference layers, write a new HDF5 cropped to the SOURCE
block(s) that actually contain change inside the cell, keeping ALL timesteps
and ALL bands. The output is byte-for-byte the same schema as the input, so
the pipeline reads it with no changes — it's just a spatially cropped tile.

Why
---
Lets you build a tiny test tile (one block + a ghost-feeding ring) to exercise
pipeline changes without processing a whole S2 tile.

Change-aware, block-aligned selection
--------------------------------------
The pipeline reads a block's LIVE area ONLY at SOURCE chip rows/cols that are
multiples of LIVE_ROWS/LIVE_COLS (chip_y_start = block_row * 4). So a cell that
isn't aligned to the source's 4-chip block lattice can't be a single clean
block. To guarantee single-block alignment WITHOUT cutting off any change, we
don't crop to the cell polygon directly — instead we find which SOURCE blocks
contain fire/cut change geometry (clipped to the cell) and keep exactly the
union of those blocks as the LIVE area. Blocks with no change are excluded, so
the LIVE area is the tightest block-aligned region that still holds all the
change (usually 1x1; grows to 2x1 / 2x2 only if change straddles the source
block lattice).

Chip geometry (from the input's attrs)
--------------------------------------
The HDF5 stores `values` as (N_TS, N_BANDS, n_chips*65536) uint16, one
65536-pixel (256x256) chip per chunk, with parallel chip_x_bin / chip_y_bin /
chip_pixel_count arrays and per-pixel xs_new / ys_new. A chip at grid
(chip_y_bin, chip_x_bin) has its NW corner at:

    x0   = bounds_left + chip_x_bin * chip_size * pixel_res
    ytop = bounds_top  - chip_y_bin * chip_size * pixel_res

and spans [x0, x0 + step) east, (ytop - step, ytop] north, step = chip_size *
pixel_res. A source block is the 4x4-chip group at chip rows/cols [4k..4k+3].

Ghost padding (whole blocks)
----------------------------
The pipeline reads a 128-px ghost border around each block for context, and
crucially places a block's LIVE area ONLY at chip rows/cols that are multiples
of LIVE_ROWS/LIVE_COLS (chip_y_start = block_row * 4). So to give the polygon's
LIVE 4x4 a real ghost ring AND a clean block index to run, we pad by whole
blocks, not single chips: `--pad-blocks N` (default 1) keeps N blocks (4 chips
each) around the LIVE area. After rebasing, the LIVE area lands at block
(N, N) — with `--pad-blocks 1` that's block (1,1) of a 3x3-block tile, with a
real ghost ring on every side that has source data. You run ONLY that block
(the script prints BLOCK_ROW/BLOCK_COL); the surrounding padding blocks exist
solely to feed its ghost. A 1-chip ring would land the LIVE area at chips 1-4,
which no block_row*4 can address — hence blocks, not chips.

Set `--pad-blocks 0` for a polygon-exact crop: LIVE at block (0,0), ghost
NODATA on the top/left (chip index -1 has no key in the chip lookup, so it's
just absent — no wraparound), like a real tile-corner block. Padding chips
that don't exist in the sparse source aren't added (that side's ghost stays
NODATA, like a real tile-edge block).

Chip-grid re-basing
-------------------
The pipeline indexes blocks as block_row*LIVE_ROWS / block_col*LIVE_COLS from
chip-grid origin (0,0), so the kept chips are re-based: the NW-most kept chip
becomes (0,0) and `bounds_left`/`bounds_top` are shifted to that chip's NW
corner. `bounds_right`/`bounds_bottom` are set to the kept extent. All other
datasets/attrs are copied through; per-pixel datasets (xs_new, ys_new,
sort_order) and `values` are sliced to the kept chips' pixel ranges.

Usage
-----
    python subset_hdf5_to_block.py \
        --src T29TME.h5 \
        --gpkg fire_cut_test_block.gpkg \
        --fires Data_ref_2023_icnf.gpkg \
        --cuts Data_ref_2023_nvg_v2.gpkg \
        --out T29TME_testblock.h5
"""
from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd
import h5py
import numpy as np

_HERE = Path(__file__).resolve().parent

# ── Default input paths ──────────────────────────────────────────────────────
# The cell polygon and change-reference layers live alongside this script
# (same dir on the cluster), so derive them from _HERE. The source tile is on
# the cluster's shared storage; override with --src to point elsewhere.
DEFAULT_SRC = Path("/users1/dgt/hdf5_2023/T29SNB.h5")
DEFAULT_GPKG = _HERE / "fire_cut_test_block.gpkg"
DEFAULT_FIRES = _HERE / "Data_ref_2023_icnf.gpkg"
DEFAULT_CUTS = _HERE / "Data_ref_2023_nvg_v2.gpkg"

CHIP_PIXELS = 65536  # 256 * 256; one chunk along the pixel axis.

# Datasets whose last/only axis is the per-pixel axis (length n_chips*65536).
# These are sliced chip-by-chip alongside `values`.
PER_PIXEL_DATASETS = ("xs_new", "ys_new", "sort_order")
# Datasets indexed per chip (length n_chips). Re-indexed to the kept chips.
PER_CHIP_DATASETS = ("chip_x_bin", "chip_y_bin", "chip_pixel_count")


def _change_source_blocks(change_geoms, cell_poly,
                          bounds_left, bounds_top, step,
                          live_rows, live_cols):
    """Source-tile block (row, col) indices that contain change in the cell.

    A "source block" is the 4x4-chip group the pipeline reads at chip rows/cols
    [4k .. 4k+3] of the SOURCE tile grid (chip_y_start = block_row*LIVE_ROWS).
    For each change geometry (fire/cut), intersected with the test cell so only
    change inside the cell counts, we find which source block(s) it falls in
    and collect them. The LIVE area is exactly the union of these blocks, so
    every change-bearing block is kept and nothing is cut at a block edge — and
    blocks with no change are excluded (the tightest LIVE area that still holds
    all the change).

    block_size_m = live_rows * step (one block = live_rows chips of `step` m).

    Returns a set of (block_row, block_col) in SOURCE block coordinates.
    """
    block_h = live_rows * step
    block_w = live_cols * step
    blocks: set[tuple[int, int]] = set()
    for geom in change_geoms:
        clipped = geom.intersection(cell_poly)
        if clipped.is_empty:
            continue
        gminx, gminy, gmaxx, gmaxy = clipped.bounds
        # Block col from world x: col = floor((x - bounds_left) / block_w).
        col_lo = int((gminx - bounds_left) // block_w)
        col_hi = int((gmaxx - bounds_left) // block_w)
        # Block row from world y: y decreases southward from bounds_top.
        row_lo = int((bounds_top - gmaxy) // block_h)
        row_hi = int((bounds_top - gminy) // block_h)
        for br in range(row_lo, row_hi + 1):
            for bc in range(col_lo, col_hi + 1):
                blocks.add((br, bc))
    return blocks


def _pad_block_ring(live_block_rows, live_block_cols,
                    chip_x: np.ndarray, chip_y: np.ndarray,
                    pad_blocks: int, live_rows: int, live_cols: int):
    """Keep the LIVE source blocks + a `pad_blocks` ring, rebased to (0,0).

    The pipeline reads each block's LIVE area only at chip rows/cols that are
    multiples of LIVE_ROWS/LIVE_COLS (chip_y_start = block_row * LIVE_ROWS),
    and pulls a 128-px ghost from the chips bordering it. So we keep the
    change-bearing source blocks (given as block-grid row/col ranges) plus a
    ring `pad_blocks` blocks thick, then rebase the kept region's NW corner to
    chip (0,0). The LIVE area then starts at chip (pad_blocks*live_rows,
    pad_blocks*live_cols) — i.e. block (pad_blocks, pad_blocks).

    Parameters
    ----------
    live_block_rows, live_block_cols : (lo, hi) inclusive SOURCE block ranges
        spanning the change-bearing blocks.

    Returns
    -------
    (keep_idx, keep_y0_chip, keep_x0_chip, live_block_row, live_block_col,
     live_n_block_rows, live_n_block_cols)

    Chips that don't exist in the sparse source aren't added (a LIVE block
    against the tile edge keeps a NODATA ghost on that side, like a real edge
    block).
    """
    brow_lo, brow_hi = live_block_rows
    bcol_lo, bcol_hi = live_block_cols

    # LIVE chip span = the change-bearing source blocks, in source chip coords.
    live_y0 = brow_lo * live_rows
    live_x0 = bcol_lo * live_cols
    live_y1 = (brow_hi + 1) * live_rows - 1   # inclusive
    live_x1 = (bcol_hi + 1) * live_cols - 1

    pad_y = pad_blocks * live_rows
    pad_x = pad_blocks * live_cols
    keep_y0, keep_y1 = live_y0 - pad_y, live_y1 + pad_y
    keep_x0, keep_x1 = live_x0 - pad_x, live_x1 + pad_x

    within = ((chip_y >= keep_y0) & (chip_y <= keep_y1) &
              (chip_x >= keep_x0) & (chip_x <= keep_x1))
    keep_idx = np.nonzero(within)[0]

    # Rebase to the intended keep-window NW corner (keep_y0, keep_x0), NOT to
    # the min of the chips that happen to exist — otherwise a missing top/left
    # padding chip (sparse source / tile edge) would shift the LIVE area off
    # its block index. With this origin the LIVE area's NW chip lands at
    # (pad_y, pad_x) = block (pad_blocks, pad_blocks).
    live_block_row = pad_blocks
    live_block_col = pad_blocks
    live_n_block_rows = brow_hi - brow_lo + 1
    live_n_block_cols = bcol_hi - bcol_lo + 1
    return (keep_idx, keep_y0, keep_x0, live_block_row, live_block_col,
            live_n_block_rows, live_n_block_cols)


def _load_change_geoms(paths, tile_crs):
    """Read change-reference .gpkg(s), reproject to tile CRS, return geoms."""
    geoms = []
    for p in paths:
        g = gpd.read_file(p)
        if tile_crs is not None and g.crs is not None:
            try:
                g = g.to_crs(tile_crs)
            except Exception:
                pass
        geoms.extend(list(g.geometry.values))
    return geoms


def subset(src_path: Path, gpkg_path: Path, out_path: Path,
           change_paths, pad_blocks: int = 1,
           live_rows: int = 4, live_cols: int = 4):
    gdf = gpd.read_file(gpkg_path)
    if gdf.empty:
        raise SystemExit(f"{gpkg_path} has no features")
    if not change_paths:
        raise SystemExit(
            "No change layers given. Pass --fires / --cuts (the reference "
            "fire/cut .gpkg files) so the LIVE area can be snapped to the "
            "source blocks that actually contain change.")

    with h5py.File(src_path, "r") as src:
        attrs = dict(src.attrs)
        bounds_left = float(attrs["bounds_left"])
        bounds_top = float(attrs["bounds_top"])
        chip_size = int(attrs["chip_size"])
        pixel_res = float(attrs["pixel_res"])
        step = chip_size * pixel_res

        chip_x = np.asarray(src["chip_x_bin"][:])
        chip_y = np.asarray(src["chip_y_bin"][:])
        n_chips = len(chip_x)

        # Reproject the cell polygon into the tile CRS if needed (.gpkg and
        # HDF5 are both EPSG:32629 here, but be safe).
        tile_crs = attrs.get("crs")
        if tile_crs is not None and gdf.crs is not None:
            try:
                gdf = gdf.to_crs(tile_crs)
            except Exception:
                pass  # CRS strings can be finicky; assume already matching.
        # Single cell polygon (union if the .gpkg holds several features).
        cell_poly = gdf.geometry.union_all()
        poly_bounds = tuple(gdf.total_bounds)  # (minx, miny, maxx, maxy)

        # ── Find the SOURCE blocks that actually contain change in the cell ──
        change_geoms = _load_change_geoms(change_paths, tile_crs)
        change_blocks = _change_source_blocks(
            change_geoms, cell_poly, bounds_left, bounds_top, step,
            live_rows, live_cols)
        if not change_blocks:
            raise SystemExit(
                "No fire/cut change geometry falls inside the cell polygon, "
                "so there is no change-bearing block to keep. Check that the "
                "change layers and the cell polygon overlap.")
        brows = sorted({br for br, _ in change_blocks})
        bcols = sorted({bc for _, bc in change_blocks})
        live_block_rows = (brows[0], brows[-1])
        live_block_cols = (bcols[0], bcols[-1])

        # Pad by whole blocks so the LIVE area lands on a clean block index
        # with a real ghost ring; rebase so missing edge padding chips don't
        # shift the LIVE block index. pad_blocks=0 -> LIVE at (0,0), ghost
        # NODATA on top/left.
        (keep, new_y0, new_x0, live_block_row, live_block_col,
         live_nbr, live_nbc) = _pad_block_ring(
            live_block_rows, live_block_cols, chip_x, chip_y,
            pad_blocks, live_rows, live_cols)

        if len(keep) == 0:
            raise SystemExit(
                "No source chips fall in the change-bearing block region "
                f"(source blocks rows {live_block_rows} cols {live_block_cols})."
                " The cell's change may be over a part of the tile with no "
                "imagery.")

        kept_y = chip_y[keep]
        kept_x = chip_x[keep]
        rebased_y = kept_y - new_y0
        rebased_x = kept_x - new_x0
        n_kept = len(keep)

        new_bounds_left = bounds_left + new_x0 * step
        new_bounds_top = bounds_top - new_y0 * step
        new_bounds_right = new_bounds_left + (int(rebased_x.max()) + 1) * step
        new_bounds_bottom = new_bounds_top - (int(rebased_y.max()) + 1) * step

        n_ts = src["values"].shape[0]
        n_bands = src["values"].shape[1]
        print(f"Source: {src_path.name}")
        print(f"  chips: {n_chips}  ts: {n_ts}  bands: {n_bands}")
        print(f"  polygon bounds: "
              f"({poly_bounds[0]:.0f}, {poly_bounds[1]:.0f}, "
              f"{poly_bounds[2]:.0f}, {poly_bounds[3]:.0f})")
        print(f"  change-bearing source blocks: rows {live_block_rows[0]}.."
              f"{live_block_rows[1]} cols {live_block_cols[0]}.."
              f"{live_block_cols[1]}  ({live_nbr}x{live_nbc} = LIVE area)")
        print(f"  pad: {pad_blocks} block ring(s)")
        print(f"Kept {n_kept} chips, rebased grid rows "
              f"[{rebased_y.min()}..{rebased_y.max()}] cols "
              f"[{rebased_x.min()}..{rebased_x.max()}]")
        print(f"  new bounds L,B,R,T: {new_bounds_left:.0f}, "
              f"{new_bounds_bottom:.0f}, {new_bounds_right:.0f}, "
              f"{new_bounds_top:.0f}")

        _write_subset(src, out_path, keep, rebased_x, rebased_y,
                      attrs, new_bounds_left, new_bounds_top,
                      new_bounds_right, new_bounds_bottom)

    print(f"\nWrote {out_path}")
    if live_nbr == 1 and live_nbc == 1:
        print(f"\n  >> Run the LIVE block with: "
              f"BLOCK_ROW={live_block_row} BLOCK_COL={live_block_col}")
        print(f"     (the {pad_blocks}-block padding around it feeds the ghost "
              f"ring; ignore the other block positions)")
    else:
        print(f"\n  >> The polygon's LIVE area spans {live_nbr}x{live_nbc} "
              f"blocks, NW block at (row={live_block_row}, "
              f"col={live_block_col}).")
        print(f"     Run block rows {live_block_row}..{live_block_row + live_nbr - 1}, "
              f"cols {live_block_col}..{live_block_col + live_nbc - 1}.")
    return live_block_row, live_block_col, live_nbr, live_nbc


def _write_subset(src, out_path, keep, rebased_x, rebased_y, attrs,
                  bl, bt, br, bb) -> None:
    values = src["values"]
    n_ts, n_bands, _ = values.shape
    n_kept = len(keep)

    # Pixel slabs for the kept chips (in the OUTPUT order = sorted by `keep`).
    keep = np.asarray(keep)
    pix_starts = keep * CHIP_PIXELS

    with h5py.File(out_path, "w") as dst:
        # ── values: copy chip-by-chip into the compact output layout ────────
        out_vals = dst.create_dataset(
            "values",
            shape=(n_ts, n_bands, n_kept * CHIP_PIXELS),
            dtype=values.dtype,
            chunks=(min(values.chunks[0], n_ts), n_bands, CHIP_PIXELS),
            compression=values.compression,
        )
        for out_i, src_i in enumerate(keep):
            s_src = slice(int(src_i) * CHIP_PIXELS,
                          (int(src_i) + 1) * CHIP_PIXELS)
            s_dst = slice(out_i * CHIP_PIXELS, (out_i + 1) * CHIP_PIXELS)
            out_vals[:, :, s_dst] = values[:, :, s_src]

        # ── per-pixel datasets: same chip slicing ───────────────────────────
        for name in PER_PIXEL_DATASETS:
            if name not in src:
                continue
            ds = src[name]
            out = dst.create_dataset(
                name, shape=(n_kept * CHIP_PIXELS,), dtype=ds.dtype)
            for out_i, src_i in enumerate(keep):
                out[out_i * CHIP_PIXELS:(out_i + 1) * CHIP_PIXELS] = \
                    ds[int(src_i) * CHIP_PIXELS:(int(src_i) + 1) * CHIP_PIXELS]

        # ── per-chip datasets: re-base bins, re-index counts ────────────────
        dst.create_dataset("chip_x_bin", data=rebased_x.astype(
            src["chip_x_bin"].dtype))
        dst.create_dataset("chip_y_bin", data=rebased_y.astype(
            src["chip_y_bin"].dtype))
        if "chip_pixel_count" in src:
            dst.create_dataset(
                "chip_pixel_count",
                data=src["chip_pixel_count"][:][keep])

        # ── everything else: copy through verbatim (ts, original_timestamps,
        #    S2_filename, *_pt, etc.) ──────────────────────────────────────
        handled = {"values", "chip_x_bin", "chip_y_bin",
                   "chip_pixel_count", *PER_PIXEL_DATASETS}
        for name in src:
            if name in handled:
                continue
            ds = src[name]
            dst.create_dataset(name, data=ds[:], dtype=ds.dtype)

        # ── attrs: copy, then override the spatial bounds ───────────────────
        for k, v in attrs.items():
            dst.attrs[k] = v
        dst.attrs["bounds_left"] = bl
        dst.attrs["bounds_top"] = bt
        dst.attrs["bounds_right"] = br
        dst.attrs["bounds_bottom"] = bb


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", type=Path, default=DEFAULT_SRC,
                    help="source full-tile chip-chunked HDF5")
    ap.add_argument("--gpkg", type=Path, default=DEFAULT_GPKG,
                    help="polygon .gpkg defining the area to keep")
    ap.add_argument("--out", type=Path, default=None,
                    help="output HDF5 (default: <src stem>_testblock.h5)")
    ap.add_argument("--fires", type=Path, default=DEFAULT_FIRES,
                    help="fire reference .gpkg (Chg_type fogo)")
    ap.add_argument("--cuts", type=Path, default=DEFAULT_CUTS,
                    help="cut reference .gpkg (Chg_type corte)")
    ap.add_argument("--pad-blocks", type=int, default=1,
                    help="rings of whole blocks (4 chips each) to keep around "
                         "the polygon's LIVE area so it lands on a clean block "
                         "index with a real ghost ring on all sides (default "
                         "1 -> LIVE at block (1,1); 0 = LIVE at (0,0), ghost "
                         "NODATA on top/left)")
    args = ap.parse_args()

    out = args.out or _HERE / (args.src.stem + "_testblock.h5")
    change_paths = [p for p in (args.fires, args.cuts) if p and p.exists()]
    subset(args.src, args.gpkg, out, change_paths,
           pad_blocks=args.pad_blocks)


if __name__ == "__main__":
    main()
