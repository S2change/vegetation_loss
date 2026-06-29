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

Stretch is fused into the read path: if called, each chip-timestep is
converted from uint16 to uint8 via the same per-band q02/q98 logic the
training data used (mirrors
`predict_pipeline/models/bacdm/data/dataset_swin_GZ._to_uint8`), and the block is
uint8 with nodata 255. When `stretch=False` it keep the block as raw uint16 
(nodata = the source `nodata_val`, usually 65535) — for callers wanting native reflectance.

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

# HDF5 raw-data chunk cache (rdcc_nbytes) for the block readers. The rechunker
# (rechunk_hdf5_chip_oriented.py) writes `values` LZF-compressed with chunks of
# (T_CHUNK=48, 10, CHIP_PIXELS) — one chunk per chip spanning 48 timesteps,
# ≈ 48*10*65536*2 ≈ 63 MB uncompressed. HDF5 decompresses a WHOLE chunk to serve
# any element in it. The readers here read each chip's slab once (chip-outer
# order), so this cache mainly lets a chip whose window spans >1 time-chunk reuse
# a chunk across bands/timesteps within that single read rather than across
# clusters. Sized to a couple of chunks (256 MB ceiling, not a fixed alloc);
# bigger than the default 1 MB so a single chip read isn't fighting eviction.
_RDCC_NBYTES = 256 * 1024 * 1024
# rdcc_nslots should be a prime ≫ the number of chunks that can fit in the
# cache, to keep the cache's hash table collision-free.
_RDCC_NSLOTS = 1009


def _open_block_hdf5(hdf5_path: str) -> h5py.File:
    """Open an HDF5 for block reading with an enlarged chunk cache
    (see `_RDCC_NBYTES`)."""
    return h5py.File(hdf5_path, "r",
                     rdcc_nbytes=_RDCC_NBYTES, rdcc_nslots=_RDCC_NSLOTS)


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


def _min_composite_ignoring_nodata(slab: np.ndarray, nodata) -> np.ndarray:
    """Per-pixel/per-band min over axis 0, ignoring `nodata`, in native dtype.

    `slab` is (n, ...) of the block's dtype. Returns (...) where each element is
    the min over the n entries that aren't `nodata`; elements that are `nodata`
    in EVERY entry stay `nodata`. Stays in the native dtype (no int64 promotion,
    which would 4x the slab) — mirrors `aggregate_block_dates`.
    """
    is_nodata = (slab == nodata)
    all_nodata = is_nodata.all(axis=0)
    # Mask nodata up to the dtype max so .min() skips it.
    masked = np.where(is_nodata, np.iinfo(slab.dtype).max, slab)
    mins = masked.min(axis=0)
    mins[all_nodata] = nodata
    return mins


def _block_chip_placements(chip_y_start: int, chip_x_start: int):
    """Yield the chip-placement specs for one block's live 4x4 + ghost ring.

    Each yielded tuple is
        (chip_grid_y, chip_grid_x, src_y_slice, src_x_slice,
         dst_y_slice, dst_x_slice)
    meaning: the source chip at chip-grid (chip_grid_y, chip_grid_x)
    contributes its `[src_y_slice, src_x_slice]` sub-region to the block at
    `[dst_y_slice, dst_x_slice]`. There are 36 specs: 16 full inner chips, 16
    edge-strip halves (4 per side), and 4 corner quadrants. This is the single
    source of truth for block geometry, shared by `_fill_block_from_chips`
    (read-then-place) and `read_block_clustered` (read-once, composite-per-
    cluster, then place) so both stay in lockstep.
    """
    full = slice(0, CHIP_SIZE)
    # 16 inner chips (full 256x256) at the live area.
    for r in range(LIVE_ROWS):
        for c in range(LIVE_COLS):
            y0 = GHOST + r * CHIP_SIZE
            x0 = GHOST + c * CHIP_SIZE
            yield (chip_y_start + r, chip_x_start + c, full, full,
                   slice(y0, y0 + CHIP_SIZE), slice(x0, x0 + CHIP_SIZE))
    # Top edge strip: bottom 128 px of the row above the live area.
    for c in range(LIVE_COLS):
        x0 = GHOST + c * CHIP_SIZE
        yield (chip_y_start - 1, chip_x_start + c,
               slice(CHIP_SIZE - GHOST, CHIP_SIZE), full,
               slice(0, GHOST), slice(x0, x0 + CHIP_SIZE))
    # Bottom edge strip: top 128 px of the row below the live area.
    for c in range(LIVE_COLS):
        x0 = GHOST + c * CHIP_SIZE
        yield (chip_y_start + LIVE_ROWS, chip_x_start + c,
               slice(0, GHOST), full,
               slice(GHOST + LIVE_H, BLOCK_H), slice(x0, x0 + CHIP_SIZE))
    # Left edge strip: right 128 px of the col left of the live area.
    for r in range(LIVE_ROWS):
        y0 = GHOST + r * CHIP_SIZE
        yield (chip_y_start + r, chip_x_start - 1,
               full, slice(CHIP_SIZE - GHOST, CHIP_SIZE),
               slice(y0, y0 + CHIP_SIZE), slice(0, GHOST))
    # Right edge strip: left 128 px of the col right of the live area.
    for r in range(LIVE_ROWS):
        y0 = GHOST + r * CHIP_SIZE
        yield (chip_y_start + r, chip_x_start + LIVE_COLS,
               full, slice(0, GHOST),
               slice(y0, y0 + CHIP_SIZE), slice(GHOST + LIVE_W, BLOCK_W))
    # 4 corner quadrants (128x128 each).
    yield (chip_y_start - 1, chip_x_start - 1,
           slice(CHIP_SIZE - GHOST, CHIP_SIZE), slice(CHIP_SIZE - GHOST, CHIP_SIZE),
           slice(0, GHOST), slice(0, GHOST))                                # NW
    yield (chip_y_start - 1, chip_x_start + LIVE_COLS,
           slice(CHIP_SIZE - GHOST, CHIP_SIZE), slice(0, GHOST),
           slice(0, GHOST), slice(GHOST + LIVE_W, BLOCK_W))                  # NE
    yield (chip_y_start + LIVE_ROWS, chip_x_start - 1,
           slice(0, GHOST), slice(CHIP_SIZE - GHOST, CHIP_SIZE),
           slice(GHOST + LIVE_H, BLOCK_H), slice(0, GHOST))                  # SW
    yield (chip_y_start + LIVE_ROWS, chip_x_start + LIVE_COLS,
           slice(0, GHOST), slice(0, GHOST),
           slice(GHOST + LIVE_H, BLOCK_H), slice(GHOST + LIVE_W, BLOCK_W))   # SE


def _fill_block_from_chips(block: np.ndarray, values, chip_lookup: dict,
                           chip_y_start: int, chip_x_start: int,
                           ts_indices: np.ndarray, nodata_val: int,
                           stretch: bool) -> None:
    """Splice the live 4x4 + 128-px ghost ring into a pre-allocated `block`.

    Reads each chip's slab for the given `ts_indices` and writes it into the
    correct region of `block` (shape `(len(ts_indices), 10, BLOCK_H, BLOCK_W)`,
    already NODATA-filled by the caller). Missing chip-grid positions are left
    as-is (nodata). Used by `read_block`; the placement geometry comes from
    `_block_chip_placements` so it stays in lockstep with the clustered reader.
    """
    n_ts = len(ts_indices)
    for cy, cx, sys_, sxs, dys, dxs in _block_chip_placements(chip_y_start,
                                                              chip_x_start):
        flat_chip_idx = chip_lookup.get((cy, cx))
        if flat_chip_idx is None:
            continue
        chip = _read_chip(values, ts_indices, flat_chip_idx,
                          n_ts, nodata_val, stretch=stretch)
        block[:, :, dys, dxs] = chip[:, :, sys_, sxs]


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
               stretch: bool = False,
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
    stretch : bool (default False)
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

    with _open_block_hdf5(hdf5_path) as h5f:
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

        _fill_block_from_chips(block, values, chip_lookup,
                               chip_y_start, chip_x_start,
                               ts_indices, nodata_val, stretch)

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
# PUBLIC API: read_block_clustered
# ============================================================================

def read_block_clustered(hdf5_path: str,
                         block_row: int,
                         block_col: int,
                         list_of_date_clusters_ordinal,
                         ts_start_ordinal: Optional[int] = DEFAULT_TS_START_ORDINAL,
                         ts_end_ordinal:   Optional[int] = DEFAULT_TS_END_ORDINAL,
                         stretch: bool = False,
                         ) -> tuple[np.ndarray, np.ndarray, BlockPosition]:
    """Read one block, min-compositing per date cluster as we go.

    Functionally equivalent to `read_block(...)` followed by
    `aggregate_block_dates(..., list_of_date_clusters_ordinal)`, but never
    materializes the full `(n_ts, 10, BLOCK_H, BLOCK_W)` array.

    Read order is chip-outer: each of the block's ~36 source chips is read
    exactly ONCE (all of its windowed timesteps), then reduced to one min-
    composite per cluster and placed into the cluster's slice of the pre-
    allocated `(n_clusters, 10, BLOCK_H, BLOCK_W)` output. Reading once per chip
    matters because the HDF5 stores `values` LZF-compressed with a whole chip's
    time axis in one chunk: a cluster-outer order would re-decompress every
    chip's chunk once per cluster (the block's ~36 chunks far exceed any chunk
    cache), making it several-fold slower. Peak full-res memory is one chip's
    time slab `(n_window_ts, 10, CHIP_SIZE, CHIP_SIZE)` (~1.3 MB * n_ts) plus
    the `(n_clusters, ...)` output — far below the old full-block `O(n_ts)`
    spike, since n_clusters << n_ts on a large date range.

    Parameters
    ----------
    list_of_date_clusters_ordinal : list[list[int]]
        Clusters of ordinal dates (e.g. from `determine_clusters_of_dates`).
        Clusters are filtered to the dates actually present in this block's
        kept timesteps (after the [ts_start_ordinal, ts_end_ordinal] window);
        empty clusters are dropped. The remaining clusters' MEDIAN dates become
        the output timesteps, sorted chronologically — matching
        `aggregate_block_dates`.
    Other parameters as `read_block`.

    Returns
    -------
    block_out : (n_clusters, 10, BLOCK_H, BLOCK_W)
        One min-composite per surviving cluster, chronological by median date.
        dtype/nodata match `read_block` (uint8/255 stretched, else uint16/file
        nodata).
    ts_out : (n_clusters,) int64
        Median ordinal date per cluster, aligned to axis 0.
    position : BlockPosition

    Raises
    ------
    ValueError
        If no clustered dates fall in the window (mirrors read_block's empty
        check) so callers get a clear error rather than an empty block.
    """
    chip_y_start = block_row * LIVE_ROWS
    chip_x_start = block_col * LIVE_COLS

    with _open_block_hdf5(hdf5_path) as h5f:
        chip_lookup, ts_all, nodata_val = _read_chip_grid_metadata(h5f)
        values = h5f["values"]

        pixel_res = float(h5f.attrs.get("pixel_res", 10.0))   # type: ignore[arg-type]
        world_origin_x, world_origin_y = _compute_block_world_origin(
            h5f, chip_lookup, chip_y_start, chip_x_start,
            pixel_res, CHIP_SIZE,
        )

        # Window-filter the file's timesteps once, then map ordinal date ->
        # its absolute index into `values` so each cluster can be read directly
        # without ever reading the whole window.
        window_indices = _select_timesteps(ts_all, ts_start_ordinal, ts_end_ordinal)
        if len(window_indices) == 0:
            raise ValueError(
                f"No timesteps in [{ts_start_ordinal}, {ts_end_ordinal}]; "
                f"file spans ordinals [{int(ts_all.min())}, {int(ts_all.max())}]."
            )
        window_ords = ts_all[window_indices].astype(np.int64)
        # First absolute index per ordinal date (dates can repeat in principle).
        date_to_abs_idx: dict[int, int] = {}
        for abs_idx, ordv in zip(window_indices.tolist(), window_ords.tolist()):
            date_to_abs_idx.setdefault(int(ordv), int(abs_idx))

        # Restrict each cluster to dates present in this block's window; drop
        # clusters left empty (a block whose ts window is narrower than the tile
        # calendar can legitimately miss whole clusters).
        block_clusters = []
        for cluster in list_of_date_clusters_ordinal:
            kept = [int(d) for d in cluster if int(d) in date_to_abs_idx]
            if kept:
                block_clusters.append(kept)
        if not block_clusters:
            raise ValueError(
                "None of the clustered dates are in this block's kept "
                "timesteps — check the START/END window matches the tile."
            )

        # Output: one composite per surviving cluster, chronological by median.
        medians = [int(np.median(cl)) for cl in block_clusters]
        order = np.argsort(medians, kind="stable")
        block_clusters = [block_clusters[i] for i in order]
        ts_out = np.array([medians[i] for i in order], dtype=np.int64)

        block_dtype = np.uint8 if stretch else np.uint16
        block_nodata = NODATA_U8 if stretch else nodata_val
        n_clusters = len(block_clusters)
        block_out = np.full((n_clusters, 10, BLOCK_H, BLOCK_W), block_nodata,
                            dtype=block_dtype)

        # The union of timesteps actually used across all surviving clusters,
        # read once per chip. Map each used ordinal -> its row in the per-chip
        # slab, then express each cluster as local rows into that slab.
        used_ords = sorted({d for cl in block_clusters for d in cl})
        chip_ts_indices = np.array([date_to_abs_idx[d] for d in used_ords],
                                   dtype=np.int64)
        ord_to_local = {d: i for i, d in enumerate(used_ords)}
        cluster_local_rows = [
            np.array([ord_to_local[d] for d in cl], dtype=np.int64)
            for cl in block_clusters
        ]

        # Chip-outer: read each source chip once (all used timesteps), then
        # reduce to one min-composite per cluster and place it. Reading once per
        # chip avoids re-decompressing its LZF time-chunk per cluster.
        n_used = len(chip_ts_indices)
        for cy, cx, sys_, sxs, dys, dxs in _block_chip_placements(chip_y_start,
                                                                  chip_x_start):
            flat_chip_idx = chip_lookup.get((cy, cx))
            if flat_chip_idx is None:
                continue   # missing chip — block_out stays nodata there
            # (n_used, 10, CHIP_SIZE, CHIP_SIZE) for this chip, sub-sliced to the
            # region this placement contributes.
            chip = _read_chip(values, chip_ts_indices, flat_chip_idx,
                              n_used, nodata_val, stretch=stretch)
            region = chip[:, :, sys_, sxs]
            for k, rows in enumerate(cluster_local_rows):
                block_out[k, :, dys, dxs] = _min_composite_ignoring_nodata(
                    region[rows], block_nodata)

    position = BlockPosition(
        block_row=block_row,
        block_col=block_col,
        chip_y_start=chip_y_start,
        chip_x_start=chip_x_start,
        world_origin_x=world_origin_x,
        world_origin_y=world_origin_y,
        pixel_res=pixel_res,
    )
    return block_out, ts_out, position


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
