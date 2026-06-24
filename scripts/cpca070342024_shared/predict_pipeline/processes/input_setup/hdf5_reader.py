"""Read 5x5 chip-blocks from a chip-chunked HDF5 file.

The HDF5 layout is what `rechunk_hdf5_chip_oriented.py` produces:

  values            : (N_TS, 10, n_chips * 65_536) uint16  chunks (N_TS, 10, 65_536)
  chip_x_bin        : (n_chips,) int32  — chip column in the chip grid
  chip_y_bin        : (n_chips,) int32  — chip row    in the chip grid
  chip_pixel_count  : (n_chips,) int32  — real pixels per chip (rest are padding)
  ts                : (N_TS,)   int32  — ordinal dates per timestep
  attrs.nodata_val  : the uint16 nodata sentinel (usually 65_535)

This reader builds a sparse `(chip_y_bin, chip_x_bin) -> flat_chip_index`
lookup once per file open. When a block is requested, the live 4x4 inner
area is loaded as full chips and a 128-px-thick ghost ring (4 edge strips
+ 4 corner squares) is loaded from the chips bordering it. Missing chip
positions (sparse HDF5, off-tile, water mask) are filled with NODATA_U8.

Output layout: `(N_TS, 10, BLOCK_H, BLOCK_W)` where BLOCK_H = BLOCK_W = 1280.
The live area sits at `block[..., GHOST:GHOST+LIVE_H, GHOST:GHOST+LIVE_W]`
and the ghost ring surrounds it. This 2-D layout makes shift extraction in
`composite_shift_chips` a matter of simple slicing.

Stretch is fused into the read path: by default each chip-timestep is
converted from uint16 to uint8 via the same per-band q02/q98 logic the
training data used (mirrors
`pybacdm/shared/bacdm/data/dataset_swin_GZ._to_uint8`), and the block is
uint8 with nodata 255. Pass `stretch=False` to `read_block` / `iter_blocks`
to skip the stretch and keep the block as raw uint16 (nodata = the source
`nodata_val`, usually 65535) — for callers wanting native reflectance.

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

# Block layout:
#   - LIVE_ROWS x LIVE_COLS chips of full 256x256 -> 1024 x 1024 live area
#   - GHOST pixels of border around the live area, from the chips bordering it
#   - block dimensions = LIVE + 2*GHOST = 1024 + 256 = 1280
LIVE_ROWS = 4
LIVE_COLS = 4
LIVE_H = LIVE_ROWS * CHIP_SIZE           # 1024
LIVE_W = LIVE_COLS * CHIP_SIZE           # 1024
GHOST = CHIP_SIZE // 2                   # 128
BLOCK_H = LIVE_H + 2 * GHOST             # 1280
BLOCK_W = LIVE_W + 2 * GHOST             # 1280

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
    chip_y_start: int     # chip-grid Y of the live area's NW chip (== block_row * LIVE_ROWS)
    chip_x_start: int     # chip-grid X of the live area's NW chip (== block_col * LIVE_COLS)
    # UTM origin of the live area's NW corner — derived from the actual
    # xs_new/ys_new of the first valid pixel of any present chip in the live
    # area (or extrapolated using pixel_res when the live area is entirely
    # off-tile). Note: this is the NW corner of the LIVE area, not of the
    # block-including-ghost. The ghost ring extends GHOST pixels NW of this
    # point in pixel-space (GHOST*pixel_res metres in UTM).
    world_origin_x: float
    world_origin_y: float
    pixel_res: float      # metres per pixel (Sentinel-2: 10.0)


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
    """Compute UTM (x, y) of the LIVE area's NW corner (chip (chip_y_start,
    chip_x_start) pixel (0, 0)).

    Primary strategy: the tile is a regular UTM chip grid, so the origin is
    fully determined by the tile's `bounds_left`/`bounds_top` attributes and
    the chip-grid index:

        x = bounds_left + chip_x_start * chip_size * pixel_res
        y = bounds_top  - chip_y_start * chip_size * pixel_res   (north -> +y)

    This is exact and drift-proof. The previous approach extrapolated from the
    "first valid pixel" of a present chip, which silently broke on partial
    chips whose NODATA (-9999) pixels are scattered *within* rows: argmax(valid)
    then lands on a pixel whose flat index doesn't map to its true (row, col)
    under the row-major assumption, producing per-block origin drift of up to
    several km. (Verified: full chips spread 0 m, ragged partial chips spread
    ~4-5 km.) Using the bounds attrs avoids touching individual pixels at all.

    Fallback: if the tile lacks bounds_left/bounds_top (older files), fall back
    to the legacy pixel-extrapolation — but only off a FULL chip (chip_pixel_
    count == CHIP_PIXELS) so the scattered-NODATA bug can't bite, walking from
    that chip's grid position to the live NW corner.
    """
    left = h5f.attrs.get("bounds_left")
    top = h5f.attrs.get("bounds_top")
    if left is not None and top is not None:
        block_origin_x = float(left) + chip_x_start * chip_size * pixel_res
        block_origin_y = float(top) - chip_y_start * chip_size * pixel_res
        return block_origin_x, block_origin_y

    # ── Fallback for files without bounds attrs ───────────────────────────
    # Use a FULL chip only, so the row-major (local_row, local_col) mapping is
    # exact (no scattered NODATA). Read chip_pixel_count to find one.
    if not chip_lookup:
        raise ValueError("HDF5 has no chips; cannot derive a world origin.")
    xs_new = h5f["xs_new"]   # type: ignore[index]
    ys_new = h5f["ys_new"]   # type: ignore[index]
    cpc = h5f["chip_pixel_count"][:] if "chip_pixel_count" in h5f else None  # type: ignore[index]

    anchor = None  # (chip_y, chip_x, flat_idx)
    for (cy, cx), idx in chip_lookup.items():
        if cpc is None or int(cpc[idx]) == CHIP_PIXELS:
            anchor = (cy, cx, idx)
            break
    if anchor is None:
        raise ValueError(
            "No bounds_left/bounds_top attrs and no full chip to anchor a "
            "world origin from; cannot place this block. Re-export the tile "
            "with bounds attributes."
        )
    anchor_chip_y, anchor_chip_x, anchor_chip_idx = anchor
    pix_start = anchor_chip_idx * CHIP_PIXELS
    # Full chip -> pixel (0,0) is flat index 0 and is valid.
    anchor_chip_origin_x = float(xs_new[pix_start])       # type: ignore[index]
    anchor_chip_origin_y = float(ys_new[pix_start])       # type: ignore[index]
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
    """
    n_ts = chip_u16.shape[0]
    out = np.empty(chip_u16.shape, dtype=np.uint8)

    for t in range(n_ts):
        arr_f = chip_u16[t].astype(np.float32)         # (10, H, W)
        nodata_mask = (chip_u16[t] == nodata_u16)
        arr_f[nodata_mask] = np.nan

        for b in range(arr_f.shape[0]):
            band = arr_f[b]
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


def _read_chip(values, ts_indices: np.ndarray, flat_chip_idx: int,
               n_ts: int, nodata_val: int, stretch: bool = True) -> np.ndarray:
    """Read one chip's full (n_ts, 10, CHIP_SIZE, CHIP_SIZE) slab.

    Returns a 2-D-per-band chip ready to splice into the block array.

    With `stretch=True` (default) the chip is converted to uint8 via the
    per-band q02/q98 percentile stretch (nodata -> NODATA_U8). With
    `stretch=False` the raw uint16 is returned unchanged (nodata kept as the
    source `nodata_val` sentinel).
    """
    pix_start = flat_chip_idx * CHIP_PIXELS
    pix_end = pix_start + CHIP_PIXELS
    chip_flat_u16: np.ndarray = values[ts_indices, :, pix_start:pix_end]   # type: ignore[index,assignment]
    chip_u16 = chip_flat_u16.reshape(n_ts, 10, CHIP_SIZE, CHIP_SIZE)
    if stretch:
        return _stretch_chip_uint16_to_uint8(chip_u16, nodata_val)
    return chip_u16


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
               stretch: bool = True,
               ) -> tuple[np.ndarray, np.ndarray, BlockPosition]:
    """Read one block (live 4x4 + 128-px ghost ring).

    Parameters
    ----------
    hdf5_path : str
        Path to a chip-chunked HDF5 file (output of rechunk_hdf5_chip_oriented).
    block_row, block_col : int
        Block index in the block grid (output of get_block_grid_shape).
    ts_start_ordinal, ts_end_ordinal : int or None
        Ordinal-date range to keep along the time axis. None on either side
        means "no bound on that side."
    stretch : bool (default True)
        When True, each chip is converted to uint8 via the per-band q02/q98
        percentile stretch (the training-data preprocessing). When False, the
        block keeps the source uint16 data unchanged — useful for callers that
        want the raw reflectance values (e.g. their own normalisation, or
        writing composites in native units).

    Returns
    -------
    block : (N_TS_kept, 10, BLOCK_H, BLOCK_W)
        2-D pixel-grid layout. The live 4x4 area sits at
        `block[..., GHOST:GHOST+LIVE_H, GHOST:GHOST+LIVE_W]` (1024 x 1024).
        The ghost ring (top/bottom/left/right strips + 4 corner squares)
        surrounds it with 128 px on each side. Missing chip-grid positions
        (sparse HDF5, off-tile) are filled with nodata.
        dtype is uint8 with nodata NODATA_U8 (255) when `stretch=True`, or
        uint16 with nodata the source `nodata_val` (e.g. 65535) when
        `stretch=False`.
    ts : (N_TS_kept,) int64
        Ordinal dates aligned to `block`'s axis 0.
    position : BlockPosition
        Block identity + live-area NW corner UTM coords.
    """
    chip_y_start = block_row * LIVE_ROWS
    chip_x_start = block_col * LIVE_COLS

    with h5py.File(hdf5_path, "r") as h5f:
        chip_lookup, ts_all, nodata_val = _read_chip_grid_metadata(h5f)
        values = h5f["values"]

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

        # Pre-allocate the 2-D block array, filled with NODATA so missing
        # chips need no extra write. dtype + nodata fill depend on `stretch`:
        # uint8/NODATA_U8 for the stretched path, raw uint16/nodata_val
        # otherwise.
        block_dtype = np.uint8 if stretch else np.uint16
        block_nodata = NODATA_U8 if stretch else nodata_val
        block = np.full((n_ts, 10, BLOCK_H, BLOCK_W), block_nodata,
                        dtype=block_dtype)

        # ── 16 inner chips (full 256x256) ────────────────────────────────
        # Placed at block[..., GHOST + 256r : GHOST + 256(r+1),
        #                       GHOST + 256c : GHOST + 256(c+1)].
        for r in range(LIVE_ROWS):
            for c in range(LIVE_COLS):
                flat_chip_idx = chip_lookup.get((chip_y_start + r, chip_x_start + c))
                if flat_chip_idx is None:
                    continue
                chip = _read_chip(values, ts_indices, flat_chip_idx,
                                  n_ts, nodata_val, stretch=stretch)
                y0 = GHOST + r * CHIP_SIZE
                x0 = GHOST + c * CHIP_SIZE
                block[:, :, y0:y0 + CHIP_SIZE, x0:x0 + CHIP_SIZE] = chip

        # ── Top edge strip: 4 chips from chip-grid row (chip_y_start - 1),
        # cols (chip_x_start + 0..3). Their BOTTOM 128 px go into the top
        # strip at block[..., 0:GHOST, GHOST + 256c : GHOST + 256(c+1)].
        for c in range(LIVE_COLS):
            flat_chip_idx = chip_lookup.get((chip_y_start - 1, chip_x_start + c))
            if flat_chip_idx is None:
                continue
            chip = _read_chip(values, ts_indices, flat_chip_idx,
                              n_ts, nodata_val, stretch=stretch)
            x0 = GHOST + c * CHIP_SIZE
            block[:, :, 0:GHOST, x0:x0 + CHIP_SIZE] = chip[:, :, CHIP_SIZE - GHOST:, :]

        # ── Bottom edge strip: chip-grid row (chip_y_start + 4). Their TOP
        # 128 px go into the bottom strip.
        for c in range(LIVE_COLS):
            flat_chip_idx = chip_lookup.get((chip_y_start + LIVE_ROWS, chip_x_start + c))
            if flat_chip_idx is None:
                continue
            chip = _read_chip(values, ts_indices, flat_chip_idx,
                              n_ts, nodata_val, stretch=stretch)
            x0 = GHOST + c * CHIP_SIZE
            block[:, :, GHOST + LIVE_H:, x0:x0 + CHIP_SIZE] = chip[:, :, :GHOST, :]

        # ── Left edge strip: chip-grid col (chip_x_start - 1). Their RIGHT
        # 128 px go into the left strip.
        for r in range(LIVE_ROWS):
            flat_chip_idx = chip_lookup.get((chip_y_start + r, chip_x_start - 1))
            if flat_chip_idx is None:
                continue
            chip = _read_chip(values, ts_indices, flat_chip_idx,
                              n_ts, nodata_val, stretch=stretch)
            y0 = GHOST + r * CHIP_SIZE
            block[:, :, y0:y0 + CHIP_SIZE, 0:GHOST] = chip[:, :, :, CHIP_SIZE - GHOST:]

        # ── Right edge strip: chip-grid col (chip_x_start + 4). Their LEFT
        # 128 px go into the right strip.
        for r in range(LIVE_ROWS):
            flat_chip_idx = chip_lookup.get((chip_y_start + r, chip_x_start + LIVE_COLS))
            if flat_chip_idx is None:
                continue
            chip = _read_chip(values, ts_indices, flat_chip_idx,
                              n_ts, nodata_val, stretch=stretch)
            y0 = GHOST + r * CHIP_SIZE
            block[:, :, y0:y0 + CHIP_SIZE, GHOST + LIVE_W:] = chip[:, :, :, :GHOST]

        # ── 4 corner chips: each contributes the 128x128 quadrant adjacent
        # to the live area.
        corners = [
            # (chip_grid_y, chip_grid_x, src_y_slice, src_x_slice, dst_y_slice, dst_x_slice)
            (chip_y_start - 1, chip_x_start - 1,
             slice(CHIP_SIZE - GHOST, CHIP_SIZE), slice(CHIP_SIZE - GHOST, CHIP_SIZE),
             slice(0, GHOST), slice(0, GHOST)),                              # NW
            (chip_y_start - 1, chip_x_start + LIVE_COLS,
             slice(CHIP_SIZE - GHOST, CHIP_SIZE), slice(0, GHOST),
             slice(0, GHOST), slice(GHOST + LIVE_W, BLOCK_W)),                # NE
            (chip_y_start + LIVE_ROWS, chip_x_start - 1,
             slice(0, GHOST), slice(CHIP_SIZE - GHOST, CHIP_SIZE),
             slice(GHOST + LIVE_H, BLOCK_H), slice(0, GHOST)),                # SW
            (chip_y_start + LIVE_ROWS, chip_x_start + LIVE_COLS,
             slice(0, GHOST), slice(0, GHOST),
             slice(GHOST + LIVE_H, BLOCK_H), slice(GHOST + LIVE_W, BLOCK_W)),  # SE
        ]
        for cy, cx, sys_, sxs, dys, dxs in corners:
            flat_chip_idx = chip_lookup.get((cy, cx))
            if flat_chip_idx is None:
                continue
            chip = _read_chip(values, ts_indices, flat_chip_idx,
                              n_ts, nodata_val, stretch=stretch)
            block[:, :, dys, dxs] = chip[:, :, sys_, sxs]

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
                stretch: bool = True,
                ) -> Iterator[tuple[np.ndarray, np.ndarray, BlockPosition]]:
    """Iterate over every block in the tile.

    Parameters
    ----------
    hdf5_path : str
    ts_start_ordinal, ts_end_ordinal : int or None
        Temporal filter passed through to `read_block`.
    block_filter : callable or None
        Optional `(block_row, block_col) -> bool` predicate. Returning False
        skips the block without reading it.
    stretch : bool (default True)
        Passed through to `read_block` — False keeps blocks as raw uint16.

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
                             ts_end_ordinal=ts_end_ordinal,
                             stretch=stretch)


# ============================================================================
# PUBLIC API: dry_run
# ============================================================================

def dry_run(hdf5_path: str,
            ts_start_ordinal: Optional[int] = DEFAULT_TS_START_ORDINAL,
            ts_end_ordinal:   Optional[int] = DEFAULT_TS_END_ORDINAL,
            n_target_dates: int = 4,
            ms_per_chip: float = 1510.0,
            chips_per_date_per_block: int = 81,
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
