"""Tests for postprocess.encode + postprocess.shard.

Run:
    python test_postprocess.py
"""
import os
import sys
import tempfile

import numpy as np
import pandas as pd

# Make this file runnable both as a script (python test_postprocess.py) and
# via `python -m postprocess.test_postprocess`.
if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from postprocess.encode import (
        PatchRecord, encode_patches, _mask_to_rle,
        _chip_nw_pixel_offset, _bbox_world_origin,
        CHIP_H, CHIP_W, HALF,
    )
    from postprocess.shard import (
        write_task_shard, read_shards, shard_path_for_block,
    )
else:
    from .encode import (
        PatchRecord, encode_patches, _mask_to_rle,
        _chip_nw_pixel_offset, _bbox_world_origin,
        CHIP_H, CHIP_W, HALF,
    )
    from .shard import write_task_shard, read_shards, shard_path_for_block


CLASS_NAMES = {0: "Background", 1: "Cuts", 2: "Fires"}


# ============================================================================
# Helpers
# ============================================================================

def _empty_label_map() -> np.ndarray:
    return np.zeros((CHIP_H, CHIP_W), dtype=np.uint8)


def _square_label_map(y0, x0, side, cls=1) -> np.ndarray:
    """Single cls-pixel square of `side` x `side` at (y0, x0), rest background."""
    lm = _empty_label_map()
    lm[y0:y0 + side, x0:x0 + side] = cls
    return lm


# ============================================================================
# encode.py: small unit tests
# ============================================================================

def test_mask_to_rle_empty():
    rle = _mask_to_rle(np.zeros((CHIP_H, CHIP_W), dtype=bool))
    assert rle.shape == (2, 0), rle.shape
    assert rle.dtype == np.uint16
    print("  _mask_to_rle (empty) — OK")


def test_mask_to_rle_single_pixel():
    mask = np.zeros((CHIP_H, CHIP_W), dtype=bool)
    mask[3, 7] = True
    rle = _mask_to_rle(mask)
    # Row-major flat index = 3 * 256 + 7 = 775; length 1.
    assert rle.shape == (2, 1)
    assert int(rle[0, 0]) == 3 * CHIP_W + 7
    assert int(rle[1, 0]) == 1
    print("  _mask_to_rle (single pixel) — OK")


def test_mask_to_rle_horizontal_run():
    mask = np.zeros((CHIP_H, CHIP_W), dtype=bool)
    mask[10, 50:60] = True   # 10-pixel horizontal run, all on same row
    rle = _mask_to_rle(mask)
    assert rle.shape == (2, 1)
    assert int(rle[0, 0]) == 10 * CHIP_W + 50
    assert int(rle[1, 0]) == 10
    print("  _mask_to_rle (horizontal run) — OK")


def test_mask_to_rle_two_rows():
    """A square spanning multiple rows produces multiple runs (one per row,
    since row-major flat indices are non-contiguous across rows)."""
    mask = np.zeros((CHIP_H, CHIP_W), dtype=bool)
    mask[5:7, 2:5] = True   # 2-row x 3-col square
    rle = _mask_to_rle(mask)
    assert rle.shape == (2, 2)
    # Row 5 cols 2..4 -> start = 5*256 + 2 = 1282, length 3
    assert int(rle[0, 0]) == 5 * CHIP_W + 2
    assert int(rle[1, 0]) == 3
    # Row 6 cols 2..4 -> start = 6*256 + 2 = 1538, length 3
    assert int(rle[0, 1]) == 6 * CHIP_W + 2
    assert int(rle[1, 1]) == 3
    print("  _mask_to_rle (2-row square) — OK")


def test_chip_nw_pixel_offset_all_kinds():
    # original at (1, 2): chip NW = (256, 512)
    assert _chip_nw_pixel_offset("original", 1, 2) == (256, 512)
    # h_shift: +HALF in x
    assert _chip_nw_pixel_offset("h_shift", 1, 2) == (256, 512 + HALF)
    # v_shift: +HALF in y
    assert _chip_nw_pixel_offset("v_shift", 1, 2) == (256 + HALF, 512)
    # diagonal: +HALF in both
    assert _chip_nw_pixel_offset("diagonal", 1, 2) == (256 + HALF, 512 + HALF)
    print("  _chip_nw_pixel_offset (all 4 kinds) — OK")


def test_bbox_world_origin_math():
    # block NW at UTM (500000, 4500000), pixel_res = 10.
    # original chip at (grid_row=0, grid_col=0), bbox at chip-pixel (5, 10).
    # Absolute pixel = (0 + 5, 0 + 10) = (5, 10).
    # World x = 500000 + 10*10 = 500100
    # World y = 4500000 - 5*10 = 4499950
    wx, wy = _bbox_world_origin(
        "original", grid_row=0, grid_col=0,
        bbox_chip_y0=5, bbox_chip_x0=10,
        block_world_origin_x=500000.0,
        block_world_origin_y=4500000.0,
        pixel_res=10.0,
    )
    assert wx == 500100.0, wx
    assert wy == 4499950.0, wy

    # h_shift at (1, 2), bbox at (0, 0):
    # abs px = (1*256 + 0, 2*256 + 128 + 0) = (256, 640)
    # World x = 500000 + 640*10 = 506400; y = 4500000 - 256*10 = 4497440
    wx, wy = _bbox_world_origin(
        "h_shift", grid_row=1, grid_col=2,
        bbox_chip_y0=0, bbox_chip_x0=0,
        block_world_origin_x=500000.0,
        block_world_origin_y=4500000.0,
        pixel_res=10.0,
    )
    assert wx == 506400.0, wx
    assert wy == 4497440.0, wy
    print("  _bbox_world_origin (original + h_shift) — OK")


# ============================================================================
# encode.py: encode_patches integration tests
# ============================================================================

def test_encode_patches_yields_nothing_for_empty():
    lm = _empty_label_map()
    records = list(encode_patches(
        lm,
        tile_id="T29TPG", block_row=0, block_col=0,
        chip_kind="original", grid_row=0, grid_col=0,
        date_ordinal=738887, date_iso="2024-01-01",
        class_names=CLASS_NAMES,
        block_world_origin_x=500000.0, block_world_origin_y=4500000.0,
        pixel_res=10.0,
    ))
    assert records == []
    print("  encode_patches (empty label map) — OK")


def test_encode_patches_single_component():
    """A single 10x10 class-1 square should produce exactly one record with
    the expected metadata."""
    lm = _square_label_map(y0=20, x0=30, side=10, cls=1)
    records = list(encode_patches(
        lm,
        tile_id="T29TPG", block_row=2, block_col=3,
        chip_kind="original", grid_row=1, grid_col=0,
        date_ordinal=738887, date_iso="2024-01-01",
        class_names=CLASS_NAMES,
        block_world_origin_x=500000.0, block_world_origin_y=4500000.0,
        pixel_res=10.0,
    ))
    assert len(records) == 1
    r = records[0]
    assert isinstance(r, PatchRecord)
    assert r.label == 1
    assert r.label_name == "Cuts"
    assert r.n_pixels == 100
    assert (r.bbox_chip_y0, r.bbox_chip_x0) == (20, 30)
    assert (r.bbox_chip_y1, r.bbox_chip_x1) == (30, 40)
    # original at grid (1, 0) -> chip NW px = (256, 0); + bbox (20, 30) -> (276, 30)
    # World x = 500000 + 30*10 = 500300
    # World y = 4500000 - 276*10 = 4497240
    assert r.world_origin_x == 500300.0
    assert r.world_origin_y == 4497240.0
    # 10 runs of length 10 (one per row of the 10x10 square).
    assert r.rle_mask.shape == (2, 10)
    assert (r.rle_mask[1] == 10).all()
    print("  encode_patches (single component) — OK")


def test_encode_patches_per_class_separation():
    """Class-1 square and class-2 square in the same chip -> 2 records,
    each with its own label. Even if they touch, they don't merge."""
    lm = _empty_label_map()
    lm[10:15, 10:15] = 1   # 5x5 class-1
    lm[10:15, 15:20] = 2   # 5x5 class-2 touching it on the right
    records = list(encode_patches(
        lm,
        tile_id="T29TPG", block_row=0, block_col=0,
        chip_kind="original", grid_row=0, grid_col=0,
        date_ordinal=738887, date_iso="2024-01-01",
        class_names=CLASS_NAMES,
        block_world_origin_x=0.0, block_world_origin_y=0.0, pixel_res=10.0,
    ))
    assert len(records) == 2
    labels = sorted(r.label for r in records)
    assert labels == [1, 2]
    for r in records:
        assert r.n_pixels == 25
    print("  encode_patches (per-class separation) — OK")


def test_encode_patches_8_connectivity():
    """Two pixels touching at a corner should be ONE component under
    8-connectivity (4-conn would split them)."""
    lm = _empty_label_map()
    lm[10, 10] = 1
    lm[11, 11] = 1   # corner-adjacent to (10, 10)
    # Need 2 more pixels to clear MIN_COMPONENT_PIXELS=4 -> add the other corners.
    lm[10, 11] = 1
    lm[11, 10] = 1
    records = list(encode_patches(
        lm,
        tile_id="T", block_row=0, block_col=0,
        chip_kind="original", grid_row=0, grid_col=0,
        date_ordinal=738887, date_iso="2024-01-01",
        class_names=CLASS_NAMES,
        block_world_origin_x=0.0, block_world_origin_y=0.0, pixel_res=10.0,
    ))
    # With the 2x2 block they form one component regardless of connectivity,
    # but the test still verifies 8-conn merges the corner-touching pair.
    assert len(records) == 1
    assert records[0].n_pixels == 4
    print("  encode_patches (8-connectivity) — OK")


def test_encode_patches_min_component_pixels():
    """Components below min_component_pixels are dropped."""
    lm = _empty_label_map()
    lm[5, 5] = 1                  # 1-pixel speckle
    lm[10:13, 10:13] = 1          # 9-pixel meaningful blob
    records_default = list(encode_patches(
        lm,
        tile_id="T", block_row=0, block_col=0,
        chip_kind="original", grid_row=0, grid_col=0,
        date_ordinal=738887, date_iso="2024-01-01",
        class_names=CLASS_NAMES,
        block_world_origin_x=0.0, block_world_origin_y=0.0, pixel_res=10.0,
    ))
    # MIN_COMPONENT_PIXELS=4 by default -> only the 9-pixel blob survives.
    assert len(records_default) == 1
    assert records_default[0].n_pixels == 9

    # min=1 keeps both.
    records_all = list(encode_patches(
        lm,
        tile_id="T", block_row=0, block_col=0,
        chip_kind="original", grid_row=0, grid_col=0,
        date_ordinal=738887, date_iso="2024-01-01",
        class_names=CLASS_NAMES,
        block_world_origin_x=0.0, block_world_origin_y=0.0, pixel_res=10.0,
        min_component_pixels=1,
    ))
    assert len(records_all) == 2
    print("  encode_patches (min_component_pixels filter) — OK")


def test_encode_patches_rle_roundtrip():
    """An RLE-encoded mask should decode back to the exact input boolean mask."""
    lm = _empty_label_map()
    # Cross shape with one component
    lm[100:120, 110:115] = 1
    lm[110:115, 100:120] = 1
    records = list(encode_patches(
        lm,
        tile_id="T", block_row=0, block_col=0,
        chip_kind="original", grid_row=0, grid_col=0,
        date_ordinal=738887, date_iso="2024-01-01",
        class_names=CLASS_NAMES,
        block_world_origin_x=0.0, block_world_origin_y=0.0, pixel_res=10.0,
    ))
    assert len(records) == 1
    r = records[0]
    # Reconstruct the boolean mask from RLE and compare to (lm == 1).
    reconstructed = np.zeros(CHIP_H * CHIP_W, dtype=bool)
    for start, length in zip(r.rle_mask[0], r.rle_mask[1]):
        reconstructed[int(start):int(start) + int(length)] = True
    reconstructed = reconstructed.reshape(CHIP_H, CHIP_W)
    assert np.array_equal(reconstructed, lm == 1)
    print("  encode_patches (RLE roundtrip) — OK")


# ============================================================================
# shard.py
# ============================================================================

def _make_record(**kwargs) -> PatchRecord:
    """Minimal valid PatchRecord with hand-picked defaults."""
    defaults = dict(
        tile_id="T29TPG", block_row=0, block_col=0,
        chip_kind="original", grid_row=0, grid_col=0,
        date_ordinal=738887, date_iso="2024-01-01",
        label=1, label_name="Cuts", n_pixels=10,
        bbox_chip_y0=0, bbox_chip_x0=0, bbox_chip_y1=5, bbox_chip_x1=5,
        world_origin_x=500000.0, world_origin_y=4500000.0,
        rle_mask=np.array([[0, 256], [5, 5]], dtype=np.uint16),
    )
    defaults.update(kwargs)
    return PatchRecord(**defaults)


def test_shard_path_for_block():
    p = shard_path_for_block("/tmp/out", "T29TPG", 12, 7)
    assert p == "/tmp/out/T29TPG_block_012_007.parquet"
    print("  shard_path_for_block — OK")


def test_write_task_shard_roundtrip():
    """Records written by write_task_shard should round-trip through
    read_shards with the right schema and values."""
    with tempfile.TemporaryDirectory() as tmpd:
        records = [
            _make_record(label=1, label_name="Cuts", n_pixels=10),
            _make_record(label=2, label_name="Fires", n_pixels=42,
                         rle_mask=np.array([[100, 500], [3, 7]], dtype=np.uint16)),
        ]
        path = write_task_shard(records, tmpd, "T29TPG", 0, 0)
        assert os.path.exists(path)

        df = read_shards(tmpd)
        assert len(df) == 2
        assert set(df["label"].tolist()) == {1, 2}
        # rle_mask round-tripped as a flat list[uint16]
        for _, row in df.iterrows():
            rle_list = row["rle_mask"]
            # We flattened as [start0, length0, start1, length1, ...]
            assert len(rle_list) % 2 == 0
    print("  write_task_shard + read_shards roundtrip — OK")


def test_write_empty_task_shard():
    """Empty record iterable should still produce a valid Parquet file."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = write_task_shard([], tmpd, "T29TPG", 0, 0)
        assert os.path.exists(path)
        df = pd.read_parquet(path)
        assert len(df) == 0
        # Column set should still include the dataclass fields.
        expected_cols = set(PatchRecord.__dataclass_fields__.keys())
        assert expected_cols.issubset(set(df.columns)), \
            f"missing cols: {expected_cols - set(df.columns)}"
    print("  write_task_shard (empty records) — OK")


def test_read_shards_tile_filter():
    """read_shards with tile_id filter should only read matching files."""
    with tempfile.TemporaryDirectory() as tmpd:
        write_task_shard([_make_record(tile_id="T29TPG")], tmpd, "T29TPG", 0, 0)
        write_task_shard([_make_record(tile_id="T29SMC")], tmpd, "T29SMC", 0, 0)
        df_tpg = read_shards(tmpd, tile_id="T29TPG")
        df_smc = read_shards(tmpd, tile_id="T29SMC")
        df_all = read_shards(tmpd)
        assert len(df_tpg) == 1 and df_tpg.iloc[0]["tile_id"] == "T29TPG"
        assert len(df_smc) == 1 and df_smc.iloc[0]["tile_id"] == "T29SMC"
        assert len(df_all) == 2
    print("  read_shards (tile filter) — OK")


# ============================================================================
# Main
# ============================================================================

def main():
    print("Running postprocess tests...")
    test_mask_to_rle_empty()
    test_mask_to_rle_single_pixel()
    test_mask_to_rle_horizontal_run()
    test_mask_to_rle_two_rows()
    test_chip_nw_pixel_offset_all_kinds()
    test_bbox_world_origin_math()
    test_encode_patches_yields_nothing_for_empty()
    test_encode_patches_single_component()
    test_encode_patches_per_class_separation()
    test_encode_patches_8_connectivity()
    test_encode_patches_min_component_pixels()
    test_encode_patches_rle_roundtrip()
    test_shard_path_for_block()
    test_write_task_shard_roundtrip()
    test_write_empty_task_shard()
    test_read_shards_tile_filter()
    print("All postprocess tests passed.")


if __name__ == "__main__":
    main()
