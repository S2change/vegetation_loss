"""Read chip-blocks from a chip-chunked HDF5 file (step 1 + 2 of the
chip-chunked prediction pipeline).

Each block returned is `(N_TS, 10, BLOCK_H, BLOCK_W)` uint8 (BLOCK_H = BLOCK_W
= 1280) — already percentile-stretched per-chip-per-timestep using the same
q02/q98 logic the training pipeline used (mirrors
`bacdm.data.dataset_swin_GZ._to_uint8`).

Layout: a 4x4 live area of full chips (1024 x 1024) sits at
`block[..., GHOST:GHOST+LIVE_H, GHOST:GHOST+LIVE_W]`, surrounded by a
GHOST=128 px ring (4 edge strips + 4 corner squares) sourced from the chips
bordering the live area. This 2-D layout makes shift extraction in
`composite_shift_chips` a matter of simple slicing.

Adjacent blocks overlap by 1 chip on the right/bottom — block (BR, BC)
covers live chip-grid rows [BR*4 .. BR*4+3], and the ghost ring comes from
chips at chip-grid positions (-1, *), (*, -1), (4, *), (*, 4) relative to
the live area's top-left.
"""
from .hdf5_reader import (
    read_block,
    iter_blocks,
    get_block_grid_shape,
    dry_run,
    BlockPosition,
    # Constants callers may want to reuse:
    CHIP_SIZE,
    CHIP_PIXELS,
    LIVE_ROWS,
    LIVE_COLS,
    LIVE_H,
    LIVE_W,
    GHOST,
    BLOCK_H,
    BLOCK_W,
    NODATA_U8,
)
from .determine_clusters_of_dates import (
    determine_clusters_of_dates,
    aggregate_block_dates,
    parse_date_clusters,
    serialize_date_clusters,
)

__all__ = [
    "read_block",
    "iter_blocks",
    "get_block_grid_shape",
    "dry_run",
    "BlockPosition",
    "determine_clusters_of_dates",
    "aggregate_block_dates",
    "parse_date_clusters",
    "serialize_date_clusters",
    "CHIP_SIZE",
    "CHIP_PIXELS",
    "LIVE_ROWS",
    "LIVE_COLS",
    "LIVE_H",
    "LIVE_W",
    "GHOST",
    "BLOCK_H",
    "BLOCK_W",
    "NODATA_U8",
]
