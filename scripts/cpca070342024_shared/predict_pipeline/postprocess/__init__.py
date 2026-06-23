"""Step 5 + 6 of the chip-chunked prediction pipeline.

Step 5 (`encode_chip_predictions`): emit one `ChipPredictionRecord` per chip
whose label map has any non-background pixels. The record stores per-class
binary masks (RLE-encoded) plus chip identity, date, and the chip's NW
pixel offset within its block.

Step 5b (`VoteAccumulator`): collapse 81 overlapping shifted-chip
predictions per target date into a single voted label map per block.
Each LIVE-area pixel is covered by 4 chips; predictions with >= threshold
agreement survive.

Step 6 has two output paths:
  - `write_task_shard`: per-chip Parquet — kept behind a flag for debug.
  - `write_voted_block`: per-block .npz of voted label maps — default.
"""
from .chip_records import (
    ChipPredictionRecord,
    encode_chip_predictions,
    chip_nw_pixel_offset,
    postprocess_prediction,
)
from .shard import write_task_shard, read_shards, shard_path_for_block
from .vote import (
    VoteAccumulator,
    DEFAULT_THRESHOLD,
    LIVE_H,
    LIVE_W,
)
from .voted_output import (
    write_voted_block,
    read_voted_block,
    voted_path_for_block,
)

__all__ = [
    "ChipPredictionRecord",
    "encode_chip_predictions",
    "chip_nw_pixel_offset",
    "postprocess_prediction",
    "write_task_shard",
    "read_shards",
    "shard_path_for_block",
    "VoteAccumulator",
    "DEFAULT_THRESHOLD",
    "LIVE_H",
    "LIVE_W",
    "write_voted_block",
    "read_voted_block",
    "voted_path_for_block",
]
