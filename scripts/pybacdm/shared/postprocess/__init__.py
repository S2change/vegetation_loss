"""Step 5 + 6 of the chip-chunked prediction pipeline.

Step 5 (`encode_patches`): for each non-background connected component in a
chip's prediction map, build a `PatchRecord` summarising it (chip identity,
date, class label, pixel count, chip-local RLE mask, world origin in UTM).

Step 6 (`write_task_shard`): collect a SLURM task's PatchRecords into one
Parquet file. Per-task shards are independent; downstream aggregation glues
them into a per-tile or per-run table.
"""
from .encode import PatchRecord, encode_patches
from .shard import write_task_shard, read_shards, shard_path_for_block

__all__ = [
    "PatchRecord",
    "encode_patches",
    "write_task_shard",
    "read_shards",
    "shard_path_for_block",
]
