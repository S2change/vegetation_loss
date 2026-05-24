"""Per-task Parquet shard writing + reading helpers.

Step 6 of the chip-chunked prediction pipeline. SLURM array tasks each write
exactly one shard — naming is `(tile, block_row, block_col)` driven so a
glance at a directory tells you which blocks have produced output.

Each shard is a single Parquet file. The schema mirrors `PatchRecord`'s
fields (RLE flattened to list[uint16]). Shards are write-once; downstream
aggregation (concatenation / per-tile merge) is a separate offline step.
"""
from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .encode import PatchRecord


def shard_path_for_block(output_dir: str,
                         tile_id: str,
                         block_row: int,
                         block_col: int) -> str:
    """Return the deterministic shard filename for one (tile, block)."""
    fname = f"{tile_id}_block_{block_row:03d}_{block_col:03d}.parquet"
    return os.path.join(output_dir, fname)


def write_task_shard(records: Iterable[PatchRecord],
                     output_dir: str,
                     tile_id: str,
                     block_row: int,
                     block_col: int,
                     compression: str = "snappy",
                     ) -> str:
    """Buffer all PatchRecords for one task in memory, write to one Parquet.

    Parameters
    ----------
    records : iterable of PatchRecord
        Output of step 5 for every (chip, target_date) the task processed.
    output_dir : str
        Directory the shard is written into. Created if missing.
    tile_id, block_row, block_col : ...
        Identify the shard. Filename is
        `{tile_id}_block_{block_row:03d}_{block_col:03d}.parquet`.
    compression : str
        Parquet compression. 'snappy' is the cross-tool default; 'zstd' is
        smaller but slower to write.

    Returns
    -------
    The shard's full path on disk. The file is written even if `records` is
    empty (empty shards are explicit "this block had no non-background
    predictions" markers).
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    path = shard_path_for_block(output_dir, tile_id, block_row, block_col)

    rows = [r.to_dict() for r in records]
    if rows:
        df = pd.DataFrame(rows)
    else:
        # Empty df with the same columns the populated case would have, so
        # downstream readers don't have to handle a missing-columns case.
        df = pd.DataFrame(columns=list(PatchRecord.__dataclass_fields__.keys()))
        # `rle_mask` was an ndarray on the dataclass; coerce to object so the
        # empty df schema doesn't try to infer the wrong type.
        df["rle_mask"] = df["rle_mask"].astype(object)

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path, compression=compression)
    return path


def read_shards(output_dir: str,
                tile_id: Optional[str] = None,
                ) -> pd.DataFrame:
    """Read every shard in `output_dir` (optionally filtered by tile_id)
    and concatenate into one DataFrame.

    For an aggregation workflow over many shards consider duckdb's
    `read_parquet` over a glob instead — it doesn't materialise the
    DataFrame in Python memory.
    """
    pattern = f"{tile_id}_block_*.parquet" if tile_id else "*_block_*.parquet"
    paths = sorted(Path(output_dir).glob(pattern))
    if not paths:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)
