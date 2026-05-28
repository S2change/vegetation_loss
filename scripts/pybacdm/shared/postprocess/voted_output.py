"""Per-block voted-output writing.

Step 6 of the chip-chunked prediction pipeline (voting variant). Writes
one `.npz` file per (tile, block) holding the voted label maps for every
valid target date, plus the metadata needed to place them in world
coordinates without re-deriving anything.

File layout (one .npz per block):
  labels:        (n_dates, LIVE_H, LIVE_W) uint8
                 — voted class IDs (0 = no detection / below threshold)
  target_dates:  (n_dates,) int64  — ordinal dates (one per labels slice)
  classes:       (n_classes,) uint8  — non-bg class IDs in channel order
  block_row:     int64 scalar
  block_col:     int64 scalar
  world_origin_x: float64 scalar  — UTM x of LIVE NW corner
  world_origin_y: float64 scalar  — UTM y of LIVE NW corner
  pixel_res:     float64 scalar   — metres / pixel
  threshold:     uint8 scalar     — vote threshold used at finalize time

Compressed via `np.savez_compressed` — gzip-style on sparse uint8 label
maps typically lands at < 1 MB per date.
"""
from __future__ import annotations

import os
from pathlib import Path

import numpy as np


def voted_path_for_block(output_dir: str,
                         tile_id: str,
                         block_row: int,
                         block_col: int) -> str:
    """Return the deterministic .npz filename for one (tile, block)."""
    fname = f"{tile_id}_block_{block_row:03d}_{block_col:03d}.npz"
    return os.path.join(output_dir, fname)


def write_voted_block(output_dir: str,
                      tile_id: str,
                      block_row: int,
                      block_col: int,
                      *,
                      labels: np.ndarray,
                      target_dates: np.ndarray,
                      classes: tuple[int, ...],
                      world_origin_x: float,
                      world_origin_y: float,
                      pixel_res: float,
                      threshold: int,
                      ) -> str:
    """Write one block's voted output to a compressed .npz file.

    Parameters
    ----------
    output_dir : str
        Directory the .npz is written into. Created if missing.
    tile_id, block_row, block_col : ...
        Identify the block. Filename is
        `{tile_id}_block_{block_row:03d}_{block_col:03d}.npz`.
    labels : (n_dates, LIVE_H, LIVE_W) uint8
        Stacked voted label maps — `labels[i]` corresponds to
        `target_dates[i]`. Use 0 for "no detection".
    target_dates : (n_dates,) int64
        Ordinal dates aligned to `labels`'s first axis.
    classes : tuple of int
        Non-background class IDs that the voter tracked. Stored so
        downstream readers know which class IDs may appear in `labels`.
    world_origin_x, world_origin_y, pixel_res : float
        UTM position of the LIVE area's NW corner + pixel size in metres.
    threshold : int
        Vote threshold used to produce `labels`. Stored for traceability.

    Returns
    -------
    The .npz's full path on disk.
    """
    if labels.dtype != np.uint8:
        raise ValueError(f"labels must be uint8, got {labels.dtype}")
    if labels.ndim != 3:
        raise ValueError(
            f"labels must be 3-D (n_dates, H, W), got shape {labels.shape}"
        )
    if target_dates.shape != (labels.shape[0],):
        raise ValueError(
            f"target_dates shape {target_dates.shape} must match "
            f"labels first axis ({labels.shape[0]})"
        )

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    path = voted_path_for_block(output_dir, tile_id, block_row, block_col)

    np.savez_compressed(
        path,
        labels=labels,
        target_dates=target_dates.astype(np.int64, copy=False),
        classes=np.asarray(classes, dtype=np.uint8),
        block_row=np.int64(block_row),
        block_col=np.int64(block_col),
        world_origin_x=np.float64(world_origin_x),
        world_origin_y=np.float64(world_origin_y),
        pixel_res=np.float64(pixel_res),
        threshold=np.uint8(threshold),
    )
    return path


def read_voted_block(path: str) -> dict:
    """Read one .npz back into a plain dict.

    Convenience wrapper that turns np.savez's NpzFile into a regular dict
    so callers don't need to remember to close the file.
    """
    with np.load(path) as npz:
        return {k: npz[k] for k in npz.files}
