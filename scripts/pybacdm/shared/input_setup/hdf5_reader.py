"""Read 5x5 chip-blocks from a chip-chunked HDF5 file.

The HDF5 layout is what `rechunk_hdf5_chip_oriented.py` produces:

  values            : (N_TS, 10, n_chips * 65_536) uint16  chunks (N_TS, 10, 65_536)
  chip_x_bin        : (n_chips,) int32  — chip column in the chip grid
  chip_y_bin        : (n_chips,) int32  — chip row    in the chip grid
  chip_pixel_count  : (n_chips,) int32  — real pixels per chip (rest are padding)
  ts                : (N_TS,)   int32  — ordinal dates per timestep
  attrs.nodata_val  : the uint16 nodata sentinel (usually 65_535)

This reader builds a sparse `(chip_y_bin, chip_x_bin) -> flat_chip_index`
lookup once per file open. When a 5x5 block is requested, each of the 25
expected chip-grid positions is resolved via the lookup; missing positions
(chips absent from this HDF5 — boundary, water mask, etc.) are returned as
all-nodata.

Stretch is fused into the read path: each chip-timestep is converted from
uint16 to uint8 via the same per-band q02/q98 logic the training data used
(mirrors `pybacdm/shared/bacdm/data/dataset_swin_GZ._to_uint8`).

Distribution: `read_block(block_row, block_col)` is the unit of work for a
SLURM array task. `iter_blocks` is a convenience for single-node runs.
"""
from __future__ import annotations

from typing import Iterator, NamedTuple, Optional

import h5py
import numpy as np

# ============================================================================
# CONFIGURATION
# ============================================================================

# Chip dimensions (must match the chip-chunked HDF5's chip_size attribute).
CHIP_SIZE = 256
CHIP_PIXELS = CHIP_SIZE * CHIP_SIZE      # 65_536

# 5x5 block layout. Live = chips this block predicts on; the 5th row/col is
# ghost data for the south/east neighbour block's shifts. Adjacent blocks
# overlap by LIVE_OVERLAP chips, so blocks stride by LIVE_ROWS / LIVE_COLS
# along the chip grid.
BLOCK_GRID_ROWS = 5
BLOCK_GRID_COLS = 5
LIVE_ROWS = 4
LIVE_COLS = 4

# Uint16 nodata sentinel in the source HDF5. Overridden by the attribute on
# the HDF5 file if present (`attrs['nodata_val']`).
DEFAULT_NODATA_U16 = 65_535
# Uint8 nodata after the q02/q98 stretch. Matches dataset_swin_GZ._to_uint8.
NODATA_U8 = 255

# Percentile-stretch bounds (matches dataset_swin_GZ._to_uint8).
STRETCH_LOW_PCT = 2.0
STRETCH_HIGH_PCT = 98.0

# Default temporal slice when the caller doesn't specify one. None on both
# sides = use the file's full temporal extent.
DEFAULT_TS_START_ORDINAL: Optional[int] = None
DEFAULT_TS_END_ORDINAL:   Optional[int] = None


# ============================================================================
# DATA TYPES
# ============================================================================

class BlockPosition(NamedTuple):
    block_row: int        # block index along the chip-grid Y axis
    block_col: int        # block index along the chip-grid X axis
    chip_y_start: int     # chip-grid Y of the block's top-left chip (== block_row * LIVE_ROWS)
    chip_x_start: int     # chip-grid X of the block's top-left chip (== block_col * LIVE_COLS)
    # UTM origin of the block's NW corner — derived from the actual xs_new/ys_new
    # of the first valid pixel of any present chip in the block (or extrapolated
    # using pixel_res when the block's first chip happens to be off-tile).
    world_origin_x: float  # UTM easting  of pixel (0, 0) of chip (0, 0)
    world_origin_y: float  # UTM northing of pixel (0, 0) of chip (0, 0)
    pixel_res: float       # metres per pixel (Sentinel-2: 10.0)


# ============================================================================
# CORE READING
# ============================================================================

def _read_chip_grid_metadata(h5f: h5py.File) -> tuple[dict, np.ndarray, int]:
    """Return (chip_lookup, ts_array, nodata_val) from an open HDF5 file.

    chip_lookup is a dict mapping (chip_y_bin, chip_x_bin) -> flat_chip_index.
    """
    chip_x = h5f["chip_x_bin"][:]   # type: ignore[index]
    chip_y = h5f["chip_y_bin"][:]   # type: ignore[index]
    ts     = h5f["ts"][:]            # type: ignore[index]

    chip_lookup = {
        (int(y), int(x)): i for i, (y, x) in enumerate(zip(chip_y, chip_x))
    }

    nodata_val = int(h5f.attrs.get("nodata_val", DEFAULT_NODATA_U16))  # type: ignore[arg-type]
    return chip_lookup, np.asarray(ts), nodata_val


def _compute_block_world_origin(h5f: h5py.File,
                                chip_lookup: dict,
                                chip_y_start: int,
                                chip_x_start: int,
                                pixel_res: float,
                                chip_size: int,
                                ) -> tuple[float, float]:
    """Compute the UTM (x, y) of the NW corner of chip (0, 0) within the block.

    Strategy: find any present chip in the block, read one valid pixel from
    its xs_new/ys_new, and extrapolate back to the block's NW corner using
    chip-grid offsets + pixel_res. This grounds the origin in actual data
    coordinates (robust to whichever anchoring the rechunker chose).

    If no chip in the block is present (entirely off-tile), fall back to
    extrapolating from any chip in the file.
    """
    xs_new = h5f["xs_new"]   # type: ignore[index]
    ys_new = h5f["ys_new"]   # type: ignore[index]

    # Try to find a present chip inside this block first (cheaper read pattern).
    anchor_chip_idx = None
    anchor_chip_y = anchor_chip_x = None
    for r in range(BLOCK_GRID_ROWS):
        for c in range(BLOCK_GRID_COLS):
            cy, cx = chip_y_start + r, chip_x_start + c
            if (cy, cx) in chip_lookup:
                anchor_chip_idx = chip_lookup[(cy, cx)]
                anchor_chip_y, anchor_chip_x = cy, cx
                break
        if anchor_chip_idx is not None:
            break

    # Fall back to any chip in the file (block was entirely off-tile / sparse).
    if anchor_chip_idx is None:
        if not chip_lookup:
            raise ValueError("HDF5 has no chips; cannot derive a world origin.")
        (anchor_chip_y, anchor_chip_x), anchor_chip_idx = next(iter(chip_lookup.items()))
    assert anchor_chip_y is not None and anchor_chip_x is not None

    # Read just this chip's xs/ys slab; find the first valid pixel.
    pix_start = anchor_chip_idx * CHIP_PIXELS
    pix_end = pix_start + CHIP_PIXELS
    xs_slab: np.ndarray = xs_new[pix_start:pix_end]   # type: ignore[assignment]
    ys_slab: np.ndarray = ys_new[pix_start:pix_end]   # type: ignore[assignment]
    valid = (xs_slab != -9999) & (ys_slab != -9999)
    if not valid.any():
        raise ValueError(
            f"Anchor chip ({anchor_chip_y},{anchor_chip_x}) has no valid "
            f"xs_new/ys_new pixels; cannot derive a world origin."
        )
    first_valid = int(np.argmax(valid))
    # xs/ys are integers in the rechunker; cast to float for the rest.
    anchor_x = float(xs_slab[first_valid])
    anchor_y = float(ys_slab[first_valid])

    # Local pixel offsets inside the anchor chip (row-major, like reshape(H, W)).
    local_row = first_valid // chip_size
    local_col = first_valid %  chip_size

    # Walk back to the NW corner of chip (anchor_chip_y, anchor_chip_x).
    anchor_chip_origin_x = anchor_x - local_col * pixel_res
    anchor_chip_origin_y = anchor_y + local_row * pixel_res   # UTM north -> +y

    # Walk back to the NW corner of chip (chip_y_start, chip_x_start).
    block_origin_x = anchor_chip_origin_x - (anchor_chip_x - chip_x_start) * chip_size * pixel_res
    block_origin_y = anchor_chip_origin_y + (anchor_chip_y - chip_y_start) * chip_size * pixel_res
    return block_origin_x, block_origin_y


def _stretch_chip_uint16_to_uint8(chip_u16: np.ndarray, nodata_u16: int) -> np.ndarray:
    """Per-band q02/q98 percentile stretch on one chip (any number of timesteps).

    Parameters
    ----------
    chip_u16 : (N, 10, CHIP_SIZE, CHIP_SIZE) uint16
        N timesteps of one chip's raw uint16 data.
    nodata_u16 : int
        The uint16 nodata sentinel (treated as NaN during stretch).

    Returns
    -------
    chip_u8 : (N, 10, CHIP_SIZE, CHIP_SIZE) uint8
        Stretched output. Nodata pixels become NODATA_U8 (255).

    Notes
    -----
    Mirrors the per-band 2-98 percentile stretch in
    `pybacdm/shared/bacdm/data/dataset_swin_GZ._to_uint8`. The training
    pipeline computes percentiles per (chip, image) pair — for a single
    256x256 image. Here we compute per (chip, timestep, band) to match.
    """
    n_ts = chip_u16.shape[0]
    out = np.empty(chip_u16.shape, dtype=np.uint8)

    for t in range(n_ts):
        arr_f = chip_u16[t].astype(np.float32)         # (10, H, W)
        nodata_mask = (chip_u16[t] == nodata_u16)
        arr_f[nodata_mask] = np.nan

        for b in range(arr_f.shape[0]):
            band = arr_f[b]
            # All-nodata band -> output is entirely NODATA_U8.
            if np.all(np.isnan(band)):
                out[t, b] = NODATA_U8
                continue
            q02, q98 = np.nanpercentile(band, [STRETCH_LOW_PCT, STRETCH_HIGH_PCT])
            denom = float(q98 - q02) if q98 > q02 else 1.0
            scaled = np.clip(
                (band - q02) / denom * (NODATA_U8 - 1),
                0, NODATA_U8 - 1,
            )
            scaled[nodata_mask[b]] = NODATA_U8
            out[t, b] = scaled.astype(np.uint8)
    return out


def _select_timesteps(ts: np.ndarray,
                      start_ord: Optional[int],
                      end_ord:   Optional[int]) -> np.ndarray:
    """Return the indices into `ts` that fall in [start_ord, end_ord] (inclusive)."""
    mask = np.ones(ts.shape, dtype=bool)
    if start_ord is not None:
        mask &= (ts >= start_ord)
    if end_ord is not None:
        mask &= (ts <= end_ord)
    return np.where(mask)[0]


def get_block_grid_shape(hdf5_path: str) -> tuple[int, int]:
    """Return (n_block_rows, n_block_cols) for the tile in `hdf5_path`.

    Computed from the max chip_y_bin / chip_x_bin in the file. Blocks stride
    by LIVE_ROWS / LIVE_COLS in chip-grid coords; the rightmost / bottommost
    block may have ghost positions outside the data (filled with nodata).
    """
    with h5py.File(hdf5_path, "r") as h5f:
        chip_x = h5f["chip_x_bin"][:]   # type: ignore[index]
        chip_y = h5f["chip_y_bin"][:]   # type: ignore[index]

    max_y, max_x = int(chip_y.max()), int(chip_x.max())
    # We need blocks covering chip rows [0..max_y] and cols [0..max_x].
    # Block (BR, BC) covers chip rows [BR*LIVE_ROWS .. BR*LIVE_ROWS + LIVE_ROWS - 1]
    # in its LIVE area. So the highest BR we need is ceil((max_y + 1) / LIVE_ROWS).
    n_block_rows = (max_y + LIVE_ROWS) // LIVE_ROWS
    n_block_cols = (max_x + LIVE_COLS) // LIVE_COLS
    return n_block_rows, n_block_cols


# ============================================================================
# PUBLIC API: read_block
# ============================================================================

def read_block(hdf5_path: str,
               block_row: int,
               block_col: int,
               ts_start_ordinal: Optional[int] = DEFAULT_TS_START_ORDINAL,
               ts_end_ordinal:   Optional[int] = DEFAULT_TS_END_ORDINAL,
               ) -> tuple[np.ndarray, np.ndarray, BlockPosition]:
    """Read one 5x5 chip-block, stretch to uint8, return (block, ts, position).

    Parameters
    ----------
    hdf5_path : str
        Path to a chip-chunked HDF5 file (output of rechunk_hdf5_chip_oriented).
    block_row, block_col : int
        Block index in the block grid (output of get_block_grid_shape).
    ts_start_ordinal, ts_end_ordinal : int or None
        Ordinal-date range to keep along the time axis. None on either side
        means "no bound on that side."

    Returns
    -------
    block : (N_TS_kept, 10, 25 * 65_536) uint8
        Chip-block in the same flat-pixel layout step 3 expects. The 25
        chips are packed in row-major order over the 5x5 block grid: index
        (R * 5 + C) for (R, C) in 0..4. Chips not present in the HDF5 are
        filled with NODATA_U8.
    ts : (N_TS_kept,) int64
        Ordinal dates aligned to `block`'s axis 0.
    position : BlockPosition
        The block's identity, useful for downstream output naming.
    """
    chip_y_start = block_row * LIVE_ROWS
    chip_x_start = block_col * LIVE_COLS

    with h5py.File(hdf5_path, "r") as h5f:
        chip_lookup, ts_all, nodata_val = _read_chip_grid_metadata(h5f)
        values = h5f["values"]   # don't slurp the whole thing

        pixel_res = float(h5f.attrs.get("pixel_res", 10.0))   # type: ignore[arg-type]
        world_origin_x, world_origin_y = _compute_block_world_origin(
            h5f, chip_lookup, chip_y_start, chip_x_start,
            pixel_res, CHIP_SIZE,
        )

        ts_indices = _select_timesteps(ts_all, ts_start_ordinal, ts_end_ordinal)
        if len(ts_indices) == 0:
            raise ValueError(
                f"No timesteps in [{ts_start_ordinal}, {ts_end_ordinal}]; "
                f"file spans ordinals [{int(ts_all.min())}, {int(ts_all.max())}]."
            )
        ts_kept = ts_all[ts_indices].astype(np.int64)
        n_ts = len(ts_indices)

        # Output is the stretched uint8 block. Pre-fill with NODATA so
        # missing chips need no extra write.
        block = np.full(
            (n_ts, 10, BLOCK_GRID_ROWS * BLOCK_GRID_COLS * CHIP_PIXELS),
            NODATA_U8, dtype=np.uint8,
        )

        for r in range(BLOCK_GRID_ROWS):
            for c in range(BLOCK_GRID_COLS):
                chip_y = chip_y_start + r
                chip_x = chip_x_start + c
                key = (chip_y, chip_x)
                flat_chip_idx = chip_lookup.get(key)
                if flat_chip_idx is None:
                    # No data at this chip-grid position; output slot stays NODATA.
                    continue

                # Read this chip's uint16 data for the selected timesteps.
                # values has shape (full_n_ts, 10, n_chips * CHIP_PIXELS), with
                # each chip occupying CHIP_PIXELS contiguous columns. We slice
                # along the time axis (h5py advanced-indexing) AND along the
                # pixel axis (contiguous slice) — the latter is what makes
                # this read cheap for a chip-chunked file.
                pix_start = flat_chip_idx * CHIP_PIXELS
                pix_end = pix_start + CHIP_PIXELS
                # h5py: indexing with an int array + slices works; result is
                # (n_ts, 10, CHIP_PIXELS) uint16.
                chip_flat_u16: np.ndarray = values[ts_indices, :, pix_start:pix_end]  # type: ignore[index,assignment]
                # Reshape to (n_ts, 10, CHIP_SIZE, CHIP_SIZE) for the stretch,
                # then back to (n_ts, 10, CHIP_PIXELS) for the block layout.
                chip_u16 = chip_flat_u16.reshape(n_ts, 10, CHIP_SIZE, CHIP_SIZE)
                chip_u8 = _stretch_chip_uint16_to_uint8(chip_u16, nodata_val)
                chip_flat_u8 = chip_u8.reshape(n_ts, 10, CHIP_PIXELS)

                block_chip_idx = r * BLOCK_GRID_COLS + c
                dst_start = block_chip_idx * CHIP_PIXELS
                dst_end = dst_start + CHIP_PIXELS
                block[:, :, dst_start:dst_end] = chip_flat_u8

    position = BlockPosition(
        block_row=block_row,
        block_col=block_col,
        chip_y_start=chip_y_start,
        chip_x_start=chip_x_start,
        world_origin_x=world_origin_x,
        world_origin_y=world_origin_y,
        pixel_res=pixel_res,
    )
    return block, ts_kept, position


# ============================================================================
# PUBLIC API: iter_blocks
# ============================================================================

def iter_blocks(hdf5_path: str,
                ts_start_ordinal: Optional[int] = DEFAULT_TS_START_ORDINAL,
                ts_end_ordinal:   Optional[int] = DEFAULT_TS_END_ORDINAL,
                block_filter=None,
                ) -> Iterator[tuple[np.ndarray, np.ndarray, BlockPosition]]:
    """Iterate over every 5x5 block in the tile.

    Parameters
    ----------
    hdf5_path : str
    ts_start_ordinal, ts_end_ordinal : int or None
        Temporal filter passed through to `read_block`.
    block_filter : callable or None
        Optional `(block_row, block_col) -> bool` predicate. Returning False
        skips the block without reading it. Useful for resuming partial runs
        or for distributing manually.

    Yields
    ------
    (block, ts, position) — same as `read_block`'s return.
    """
    n_rows, n_cols = get_block_grid_shape(hdf5_path)
    for br in range(n_rows):
        for bc in range(n_cols):
            if block_filter is not None and not block_filter(br, bc):
                continue
            yield read_block(hdf5_path, br, bc,
                             ts_start_ordinal=ts_start_ordinal,
                             ts_end_ordinal=ts_end_ordinal)


# ============================================================================
# PUBLIC API: dry_run
# ============================================================================

def dry_run(hdf5_path: str,
            ts_start_ordinal: Optional[int] = DEFAULT_TS_START_ORDINAL,
            ts_end_ordinal:   Optional[int] = DEFAULT_TS_END_ORDINAL,
            n_target_dates: int = 4,
            ms_per_chip: float = 1510.0,
            chips_per_date_per_block: int = 64,
            ) -> dict:
    """Inspect an HDF5 and print a work-plan summary without reading any blocks.

    Returns a dict so callers can also use the numbers programmatically (e.g.
    a SLURM submit script computing `--array=0-N`).
    """
    with h5py.File(hdf5_path, "r") as h5f:
        chip_lookup, ts_all, nodata_val = _read_chip_grid_metadata(h5f)
        values_shape = h5f["values"].shape   # type: ignore[index]
        attrs = dict(h5f.attrs)               # type: ignore[arg-type]

    ts_indices = _select_timesteps(ts_all, ts_start_ordinal, ts_end_ordinal)
    n_block_rows, n_block_cols = _block_grid_from_lookup(chip_lookup)
    n_blocks = n_block_rows * n_block_cols

    # Per-block cost: chips * ms/chip * dates, in seconds.
    block_infer_s = (chips_per_date_per_block * ms_per_chip / 1000.0) * n_target_dates
    total_infer_s = block_infer_s * n_blocks

    summary = {
        "hdf5_path": hdf5_path,
        "n_chips_in_file": len(chip_lookup),
        "n_ts_total": int(values_shape[0]),
        "n_ts_in_window": int(len(ts_indices)),
        "n_bands": int(values_shape[1]),
        "values_pixel_axis": int(values_shape[2]),
        "n_block_rows": n_block_rows,
        "n_block_cols": n_block_cols,
        "n_blocks": n_blocks,
        "n_target_dates": n_target_dates,
        "chips_per_block": chips_per_date_per_block * n_target_dates,
        "block_infer_seconds": block_infer_s,
        "total_infer_seconds_single_node": total_infer_s,
        "total_infer_hours_single_node": total_infer_s / 3600.0,
        "nodata_val": nodata_val,
        "tile_attrs": {k: attrs[k] for k in attrs
                       if k in ("chip_size", "pixel_res", "date_first",
                                "date_last", "n_ts", "crs")},
    }

    # Pretty print.
    print(f"\n=== Dry run: {hdf5_path} ===")
    print(f"  Tile attrs:")
    for k, v in summary["tile_attrs"].items():
        try:
            v_repr = v.decode() if isinstance(v, bytes) else v
        except AttributeError:
            v_repr = v
        print(f"    {k:12s} = {v_repr}")
    print(f"  Chips present:        {summary['n_chips_in_file']:,}")
    print(f"  Timesteps in file:    {summary['n_ts_total']}")
    print(f"  Timesteps in window:  {summary['n_ts_in_window']}  "
          f"(from {ts_start_ordinal} to {ts_end_ordinal})")
    print(f"  Block grid:           {n_block_rows} x {n_block_cols}  "
          f"= {n_blocks} blocks")
    print(f"  Per-block work:       {chips_per_date_per_block * n_target_dates:,} "
          f"chip predictions ({n_target_dates} dates x "
          f"{chips_per_date_per_block} chips/date)")
    print(f"  Est. per-block time:  {block_infer_s/60:.1f} min "
          f"(@ {ms_per_chip:.0f} ms/chip)")
    print(f"  Est. total (1 node):  {total_infer_s/3600:.1f} h")
    for n_nodes in (8, 32, 128):
        print(f"  Est. total ({n_nodes:3d} nodes):  "
              f"{total_infer_s/3600/n_nodes:.1f} h")
    print(f"  SLURM array hint:     --array=0-{n_blocks - 1}")
    return summary


def _block_grid_from_lookup(chip_lookup: dict) -> tuple[int, int]:
    """Same math as get_block_grid_shape but from an already-built lookup."""
    if not chip_lookup:
        return 0, 0
    max_y = max(k[0] for k in chip_lookup)
    max_x = max(k[1] for k in chip_lookup)
    n_block_rows = (max_y + LIVE_ROWS) // LIVE_ROWS
    n_block_cols = (max_x + LIVE_COLS) // LIVE_COLS
    return n_block_rows, n_block_cols


# ============================================================================
# CLI entry: `python -m input_setup.hdf5_reader path/to.h5` for a dry-run
# ============================================================================

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python hdf5_reader.py <hdf5_path>", file=sys.stderr)
        sys.exit(1)
    dry_run(sys.argv[1])
