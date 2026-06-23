"""Per-task Parquet shard writing + reading helpers.

Step 6 of the chip-chunked prediction pipeline. SLURM array tasks each write
exactly one shard — naming is `(tile, block_row, block_col)` driven so a
glance at a directory tells you which blocks have produced output.

Each shard is a single Parquet file. The schema mirrors
`ChipPredictionRecord.to_dict()`'s output (per-class columns for pixel
counts + RLE masks). Shards are write-once; downstream aggregation is a
separate offline step.
"""
from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .chip_records import ChipPredictionRecord


# Fixed columns the schema always carries (the 6-tuple identity + date +
# world position + chip NW offset + pixel_res). Per-class columns
# (`n_pixels_cls_{id}`, `rle_cls_{id}`) are added dynamically by
# ChipPredictionRecord.to_dict() based on which classes were observed.
_BASE_COLUMNS = [
    "tile_id", "block_row", "block_col",
    "chip_kind", "grid_row", "grid_col",
    "date_ordinal", "date_iso",
    "block_world_origin_x", "block_world_origin_y",
    "chip_nw_px_y", "chip_nw_px_x",
    "pixel_res",
]


def shard_path_for_block(output_dir: str,
                         tile_id: str,
                         block_row: int,
                         block_col: int) -> str:
    """Return the deterministic shard filename for one (tile, block)."""
    fname = f"{tile_id}_block_{block_row:03d}_{block_col:03d}.parquet"
    return os.path.join(output_dir, fname)


def write_task_shard(records: Iterable[ChipPredictionRecord],
                     output_dir: str,
                     tile_id: str,
                     block_row: int,
                     block_col: int,
                     compression: str = "snappy",
                     ) -> str:
    """Buffer ChipPredictionRecords for one task in memory, write one Parquet.

    Parameters
    ----------
    records : iterable of ChipPredictionRecord
        Output of step 5 for every (chip, target_date) the task produced
        non-background predictions for. Empty input is fine — produces an
        empty shard with the base column schema.
    output_dir : str
        Directory the shard is written into. Created if missing.
    tile_id, block_row, block_col : ...
        Identify the shard. Filename is
        `{tile_id}_block_{block_row:03d}_{block_col:03d}.parquet`.
    compression : str
        Parquet compression ('snappy' = default; 'zstd' = smaller, slower).

    Returns
    -------
    The shard's full path on disk.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    path = shard_path_for_block(output_dir, tile_id, block_row, block_col)

    rows = [r.to_dict() for r in records]
    if rows:
        df = pd.DataFrame(rows)
    else:
        # Empty df with just the base column set so downstream readers see
        # the expected fixed columns (per-class columns vary with content).
        df = pd.DataFrame(columns=_BASE_COLUMNS)

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path, compression=compression)
    return path


def read_shards(output_dir: str,
                tile_id: Optional[str] = None,
                ) -> pd.DataFrame:
    """Read every shard in `output_dir` (optionally filtered by tile_id)
    and concatenate into one DataFrame.

    For aggregations over many shards consider duckdb's `read_parquet` over
    a glob instead — it doesn't materialise the DataFrame in Python memory.
    """
    pattern = f"{tile_id}_block_*.parquet" if tile_id else "*_block_*.parquet"
    paths = sorted(Path(output_dir).glob(pattern))
    if not paths:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)
