"""Read 5x5 chip-blocks from a chip-chunked HDF5 file (step 1 + 2 of the
chip-chunked prediction pipeline).

Each block returned is `(N_TS, 10, 25 * 65_536)` uint8 — already
percentile-stretched per-chip-per-timestep using the same q02/q98 logic the
training pipeline used (mirrors `bacdm.data.dataset_swin_GZ._to_uint8`). The
block is the input to step 3 (`create_before_after_composites`).

Adjacent blocks overlap by 1 chip on the right/bottom — block (BR, BC)
covers chip-grid rows [BR*4 .. BR*4+4] inclusive, where the 5th row/col is
ghost data that the next block to the south/east will treat as live.
"""
from .hdf5_reader import (
    read_block,
    iter_blocks,
    get_block_grid_shape,
    dry_run,
    BlockPosition,
    # Constants that callers may want to reuse:
    CHIP_PIXELS,
    BLOCK_GRID_ROWS,
    BLOCK_GRID_COLS,
    LIVE_ROWS,
    LIVE_COLS,
)

__all__ = [
    "read_block",
    "iter_blocks",
    "get_block_grid_shape",
    "dry_run",
    "BlockPosition",
    "CHIP_PIXELS",
    "BLOCK_GRID_ROWS",
    "BLOCK_GRID_COLS",
    "LIVE_ROWS",
    "LIVE_COLS",
]
