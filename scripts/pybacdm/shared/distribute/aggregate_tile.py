"""Stitch per-block voted .npz shards into one tile-level Parquet + .npz.

Runs once after all array tasks for a tile complete (gated by the SLURM
`afterok` dependency in submit_tile.sh). Reads every
`{TILE_ID}_block_*.npz` in `OUTPUT_DIR`, places each block's
`(n_dates, LIVE_H, LIVE_W)` labels at its `(block_row, block_col)`
position in a tile-sized canvas, and writes:

  - `{TILE_ID}_tile.parquet` (PRIMARY) — one row per detected connected
    component (class > 0). Schema: tile_id, date_ordinal, date_iso,
    class_id, component_id, n_pixels, bbox + centroid in tile-pixel
    coords, bbox + centroid in UTM, and the component's RLE.
  - `{TILE_ID}_tile.npz` (AUXILIARY) — the dense `(n_dates, TILE_H, TILE_W)`
    uint8 label map and the metadata needed to re-project it.

The Parquet is the unit of analysis ("here's a patch detected on date X");
the .npz preserves the dense map for visualisation, debugging, and any
operation that needs random pixel access.
"""
import os
import sys
import time
from datetime import date as _date
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))   # shared/
sys.path.insert(0, str(_HERE))          # distribute/ (for tile_components)

from postprocess.voted_output import read_voted_block
from postprocess.vote import LIVE_H, LIVE_W
from tile_components import extract_components


# Parquet column order — fixed for stable schema across runs.
_PARQUET_COLUMNS = [
    "tile_id", "date_ordinal", "date_iso", "class_id", "component_id",
    "n_pixels",
    "bbox_y0", "bbox_x0", "bbox_y1", "bbox_x1",
    "centroid_y", "centroid_x",
    "world_bbox_x0", "world_bbox_y0", "world_bbox_x1", "world_bbox_y1",
    "world_centroid_x", "world_centroid_y",
    "rle_starts", "rle_lengths",
]


def _required_env(name: str) -> str:
    v = os.environ.get(name)
    if v is None or v == "":
        raise SystemExit(f"[aggregate_tile] Missing required env var: {name}")
    return v


def _stitch_blocks(blocks: list[dict], n_rows: int, n_cols: int,
                   block_h: int, block_w: int, n_dates: int,
                   tile_origin_x: float, tile_origin_y: float,
                   pixel_res: float) -> np.ndarray:
    """Place each block's labels at its (block_row, block_col) cell in
    the tile canvas. Warns on per-block world-origin drift > 0.5 m."""
    labels = np.zeros(
        (n_dates, n_rows * block_h, n_cols * block_w), dtype=np.uint8,
    )
    block_w_m = block_w * pixel_res
    block_h_m = block_h * pixel_res
    for b in blocks:
        r = int(b["block_row"])
        c = int(b["block_col"])
        expected_x = tile_origin_x + c * block_w_m
        expected_y = tile_origin_y - r * block_h_m
        got_x = float(b["world_origin_x"])
        got_y = float(b["world_origin_y"])
        if abs(got_x - expected_x) > 0.5 or abs(got_y - expected_y) > 0.5:
            print(f"  [warn] block ({r},{c}) world origin off: "
                  f"expected ({expected_x}, {expected_y}), "
                  f"got ({got_x}, {got_y})")
        y0 = r * block_h
        x0 = c * block_w
        labels[:, y0:y0 + block_h, x0:x0 + block_w] = b["labels"]
    return labels


def _components_to_dataframe(labels: np.ndarray,
                             target_dates: np.ndarray,
                             classes: np.ndarray,
                             tile_id: str,
                             world_origin_x: float,
                             world_origin_y: float,
                             pixel_res: float,
                             ) -> pd.DataFrame:
    """Enumerate every connected component in `labels` and return them
    as a row-per-component DataFrame ready to write as Parquet."""
    rows: list[dict] = []
    for date_idx in range(labels.shape[0]):
        ordinal = int(target_dates[date_idx])
        iso = _date.fromordinal(ordinal).isoformat()
        for cls in classes:
            cls_int = int(cls)
            for comp in extract_components(labels[date_idx], cls_int, ordinal):
                # World coords: x = origin_x + col * res ; y = origin_y - row * res
                w_x0 = world_origin_x + comp.bbox_x0 * pixel_res
                w_x1 = world_origin_x + comp.bbox_x1 * pixel_res
                w_y0 = world_origin_y - comp.bbox_y0 * pixel_res
                w_y1 = world_origin_y - comp.bbox_y1 * pixel_res
                # NW->SE bbox in UTM: x grows east (x0 < x1), y shrinks south (y0 > y1).
                # Use min/max so x0<x1 and y0<y1 in the stored bbox.
                rows.append({
                    "tile_id": tile_id,
                    "date_ordinal": ordinal,
                    "date_iso": iso,
                    "class_id": cls_int,
                    "component_id": comp.component_id,
                    "n_pixels": comp.n_pixels,
                    "bbox_y0": comp.bbox_y0, "bbox_x0": comp.bbox_x0,
                    "bbox_y1": comp.bbox_y1, "bbox_x1": comp.bbox_x1,
                    "centroid_y": comp.centroid_y,
                    "centroid_x": comp.centroid_x,
                    "world_bbox_x0": min(w_x0, w_x1),
                    "world_bbox_y0": min(w_y0, w_y1),
                    "world_bbox_x1": max(w_x0, w_x1),
                    "world_bbox_y1": max(w_y0, w_y1),
                    "world_centroid_x": world_origin_x + comp.centroid_x * pixel_res,
                    "world_centroid_y": world_origin_y - comp.centroid_y * pixel_res,
                    "rle_starts":  comp.rle_starts.tolist(),
                    "rle_lengths": comp.rle_lengths.tolist(),
                })
    if not rows:
        # Empty DataFrame with the right columns so Parquet readers don't
        # have to special-case "no components found" tiles.
        return pd.DataFrame(columns=_PARQUET_COLUMNS)
    df = pd.DataFrame(rows)
    return df[_PARQUET_COLUMNS]


def _write_dense_npz(out_path: str, *,
                     labels: np.ndarray, target_dates: np.ndarray,
                     classes: np.ndarray, tile_id: str,
                     world_origin_x: float, world_origin_y: float,
                     pixel_res: float, threshold: int,
                     n_block_rows: int, n_block_cols: int) -> None:
    np.savez_compressed(
        out_path,
        labels=labels,
        target_dates=target_dates.astype(np.int64),
        classes=classes.astype(np.uint8),
        tile_id=np.bytes_(tile_id),
        world_origin_x=np.float64(world_origin_x),
        world_origin_y=np.float64(world_origin_y),
        pixel_res=np.float64(pixel_res),
        threshold=np.uint8(threshold),
        n_block_rows=np.int64(n_block_rows),
        n_block_cols=np.int64(n_block_cols),
    )


def main() -> None:
    tile_id    = _required_env("TILE_ID")
    output_dir = _required_env("OUTPUT_DIR")

    t_total = time.perf_counter()

    # ── Discover all block shards for this tile ───────────────────────────
    pattern = f"{tile_id}_block_*.npz"
    paths = sorted(Path(output_dir).glob(pattern))
    if not paths:
        raise SystemExit(
            f"[aggregate_tile] No shards matching {pattern} in {output_dir}"
        )
    print(f"Tile:        {tile_id}")
    print(f"Output dir:  {output_dir}")
    print(f"Found {len(paths)} block shards.")

    # ── First pass: read all, validate consistency, infer grid extent ─────
    blocks: list[dict] = []
    max_row = -1
    max_col = -1
    for p in paths:
        d = read_voted_block(str(p))
        blocks.append(d)
        max_row = max(max_row, int(d["block_row"]))
        max_col = max(max_col, int(d["block_col"]))

    n_rows = max_row + 1
    n_cols = max_col + 1
    expected_n = n_rows * n_cols
    if len(blocks) != expected_n:
        present = {(int(b["block_row"]), int(b["block_col"])) for b in blocks}
        missing = [
            (r, c)
            for r in range(n_rows) for c in range(n_cols)
            if (r, c) not in present
        ]
        raise SystemExit(
            f"[aggregate_tile] Block grid is incomplete: found "
            f"{len(blocks)} shards, expected {expected_n} for a "
            f"{n_rows}x{n_cols} grid.\nMissing: {missing}\n"
            f"Re-run those tasks before aggregating."
        )

    # Consistency: same target_dates / classes / threshold / pixel_res /
    # labels.shape across every shard.
    ref = blocks[0]
    ref_dates     = ref["target_dates"]
    ref_classes   = ref["classes"]
    ref_threshold = int(ref["threshold"])
    ref_pres      = float(ref["pixel_res"])
    ref_labels_shape = ref["labels"].shape   # (n_dates, H, W)
    for d in blocks[1:]:
        if not np.array_equal(d["target_dates"], ref_dates):
            raise SystemExit(
                f"[aggregate_tile] target_dates mismatch between shards "
                f"(saw {d['target_dates']} vs {ref_dates}). Re-run with a "
                f"consistent target-date list."
            )
        if not np.array_equal(d["classes"], ref_classes):
            raise SystemExit(
                f"[aggregate_tile] classes mismatch: {d['classes']} vs "
                f"{ref_classes}"
            )
        if int(d["threshold"]) != ref_threshold:
            raise SystemExit(
                f"[aggregate_tile] threshold mismatch: "
                f"{int(d['threshold'])} vs {ref_threshold}"
            )
        if float(d["pixel_res"]) != ref_pres:
            raise SystemExit(
                f"[aggregate_tile] pixel_res mismatch: "
                f"{float(d['pixel_res'])} vs {ref_pres}"
            )
        if d["labels"].shape != ref_labels_shape:
            raise SystemExit(
                f"[aggregate_tile] labels shape mismatch: "
                f"{d['labels'].shape} vs {ref_labels_shape}"
            )

    n_dates, block_h, block_w = ref_labels_shape
    if (block_h, block_w) != (LIVE_H, LIVE_W):
        print(f"  [note] block label shape is ({block_h}, {block_w}); "
              f"vote.py default is ({LIVE_H}, {LIVE_W}). Using the "
              f"observed values.")

    tile_h = n_rows * block_h
    tile_w = n_cols * block_w

    print(f"Block grid:  {n_rows} x {n_cols}  (each block {block_h}x{block_w})")
    print(f"Tile size:   {tile_h} x {tile_w}  ({n_dates} target date(s))")
    print(f"Classes:     {ref_classes.tolist()}")
    print(f"Threshold:   {ref_threshold}")
    print(f"Pixel res:   {ref_pres} m")

    # ── Stitch ────────────────────────────────────────────────────────────
    origin_block = next(
        (b for b in blocks if int(b["block_row"]) == 0 and int(b["block_col"]) == 0),
        None,
    )
    if origin_block is None:
        raise SystemExit(
            "[aggregate_tile] No (0, 0) shard found — required to fix the "
            "tile's world origin. Re-run that block."
        )
    tile_origin_x = float(origin_block["world_origin_x"])
    tile_origin_y = float(origin_block["world_origin_y"])

    print(f"\nStitching {n_rows * n_cols} blocks into "
          f"({n_dates}, {tile_h}, {tile_w}) uint8...")
    t0 = time.perf_counter()
    labels = _stitch_blocks(
        blocks, n_rows, n_cols, block_h, block_w, n_dates,
        tile_origin_x, tile_origin_y, ref_pres,
    )
    print(f"  Stitch time: {time.perf_counter() - t0:.2f} s")

    # ── Per-date detection summary ────────────────────────────────────────
    print("\nPer-date detections in the merged tile:")
    for i in range(n_dates):
        uniq, cnts = np.unique(labels[i], return_counts=True)
        post = {int(u): int(c) for u, c in zip(uniq, cnts) if u != 0}
        iso = _date.fromordinal(int(ref_dates[i])).isoformat()
        print(f"  {iso}: {post}")

    # ── Step A: connected-component enumeration -> Parquet ────────────────
    print("\nExtracting connected components per (date, class)...")
    t0 = time.perf_counter()
    df = _components_to_dataframe(
        labels, ref_dates, ref_classes, tile_id,
        tile_origin_x, tile_origin_y, ref_pres,
    )
    print(f"  Components found: {len(df)}")
    if len(df) > 0:
        per_class = df.groupby(["class_id", "date_iso"])["n_pixels"].agg(
            ["count", "sum"]
        )
        print(f"  By (class, date) — count = n_components, "
              f"sum = total pixels:")
        print(per_class.to_string())
    print(f"  Component extraction time: {time.perf_counter() - t0:.2f} s")

    parquet_path = os.path.join(output_dir, f"{tile_id}_tile.parquet")
    t0 = time.perf_counter()
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, parquet_path, compression="snappy")
    write_parquet_s = time.perf_counter() - t0
    parquet_bytes = os.path.getsize(parquet_path)
    print(f"\nWrote {parquet_path}")
    print(f"  ({parquet_bytes / 1024:.1f} KB in {write_parquet_s:.2f} s)")

    # ── Step B: dense .npz (auxiliary output) ─────────────────────────────
    npz_path = os.path.join(output_dir, f"{tile_id}_tile.npz")
    t0 = time.perf_counter()
    _write_dense_npz(
        npz_path,
        labels=labels, target_dates=ref_dates, classes=ref_classes,
        tile_id=tile_id,
        world_origin_x=tile_origin_x, world_origin_y=tile_origin_y,
        pixel_res=ref_pres, threshold=ref_threshold,
        n_block_rows=n_rows, n_block_cols=n_cols,
    )
    write_npz_s = time.perf_counter() - t0
    npz_bytes = os.path.getsize(npz_path)
    print(f"Wrote {npz_path}")
    print(f"  ({npz_bytes / 1024:.1f} KB in {write_npz_s:.2f} s)")

    print(f"\nTotal aggregate time: {time.perf_counter() - t_total:.2f} s")


if __name__ == "__main__":
    main()
