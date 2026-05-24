"""Step 5 + 6 of the chip-chunked prediction pipeline.

Step 5 (`encode_chip_predictions`): emit one `ChipPredictionRecord` per chip
whose label map has any non-background pixels. The record stores per-class
binary masks (RLE-encoded) plus chip identity, date, and the chip's NW
pixel offset within its block.

Step 6 (`write_task_shard`): collect a SLURM task's records into one
Parquet file. Per-task shards are independent; downstream aggregation
(pixel-level voting across overlapping shifted chips, etc.) consumes them.
"""
from .chip_records import (
    ChipPredictionRecord,
    encode_chip_predictions,
    chip_nw_pixel_offset,
)
from .shard import write_task_shard, read_shards, shard_path_for_block

__all__ = [
    "ChipPredictionRecord",
    "encode_chip_predictions",
    "chip_nw_pixel_offset",
    "write_task_shard",
    "read_shards",
    "shard_path_for_block",
]
