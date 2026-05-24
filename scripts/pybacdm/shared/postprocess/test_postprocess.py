"""Tests for postprocess.chip_records + postprocess.shard.

Run:
    python test_postprocess.py
"""
import os
import sys
import tempfile

import numpy as np
import pandas as pd

# Make this file runnable both as a script and as a module.
if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from postprocess.chip_records import (
        ChipPredictionRecord, encode_chip_predictions,
        chip_nw_pixel_offset,
        _mask_to_rle,
        CHIP_H, CHIP_W, HALF, BACKGROUND_CLASS,
    )
    from postprocess.shard import (
        write_task_shard, read_shards, shard_path_for_block,
    )
else:
    from .chip_records import (
        ChipPredictionRecord, encode_chip_predictions,
        chip_nw_pixel_offset,
        _mask_to_rle,
        CHIP_H, CHIP_W, HALF, BACKGROUND_CLASS,
    )
    from .shard import write_task_shard, read_shards, shard_path_for_block


# ============================================================================
# Helpers
# ============================================================================

def _empty_label_map() -> np.ndarray:
    return np.zeros((CHIP_H, CHIP_W), dtype=np.uint8)


def _square_label_map(y0, x0, side, cls=1) -> np.ndarray:
    lm = _empty_label_map()
    lm[y0:y0 + side, x0:x0 + side] = cls
    return lm


def _decode_rle(flat: list[int], chip_size: int = 256) -> np.ndarray:
    """Decode a flat [start0, length0, start1, length1, ...] list into a
    (chip_size, chip_size) bool mask."""
    out = np.zeros(chip_size * chip_size, dtype=bool)
    if not flat:
        return out.reshape(chip_size, chip_size)
    arr = np.asarray(flat, dtype=np.uint16).reshape(-1, 2)
    for start, length in arr:
        out[int(start):int(start) + int(length)] = True
    return out.reshape(chip_size, chip_size)


# ============================================================================
# chip_records.py: RLE helper
# ============================================================================

def test_mask_to_rle_empty():
    rle = _mask_to_rle(np.zeros((CHIP_H, CHIP_W), dtype=bool))
    assert rle.shape == (2, 0)
    assert rle.dtype == np.uint16
    print("  _mask_to_rle (empty) — OK")


def test_mask_to_rle_single_pixel():
    mask = np.zeros((CHIP_H, CHIP_W), dtype=bool)
    mask[3, 7] = True
    rle = _mask_to_rle(mask)
    assert rle.shape == (2, 1)
    assert int(rle[0, 0]) == 3 * CHIP_W + 7
    assert int(rle[1, 0]) == 1
    print("  _mask_to_rle (single pixel) — OK")


def test_mask_to_rle_horizontal_run():
    mask = np.zeros((CHIP_H, CHIP_W), dtype=bool)
    mask[10, 50:60] = True
    rle = _mask_to_rle(mask)
    assert rle.shape == (2, 1)
    assert int(rle[0, 0]) == 10 * CHIP_W + 50
    assert int(rle[1, 0]) == 10
    print("  _mask_to_rle (horizontal run) — OK")


# ============================================================================
# chip_records.py: chip NW offset
# ============================================================================

def test_chip_nw_pixel_offset_all_kinds():
    assert chip_nw_pixel_offset("original", 1, 2) == (256, 512)
    assert chip_nw_pixel_offset("h_shift",  1, 2) == (256, 512 + HALF)
    assert chip_nw_pixel_offset("v_shift",  1, 2) == (256 + HALF, 512)
    assert chip_nw_pixel_offset("diagonal", 1, 2) == (256 + HALF, 512 + HALF)
    print("  chip_nw_pixel_offset (all 4 kinds) — OK")


# ============================================================================
# encode_chip_predictions
# ============================================================================

def test_encode_yields_nothing_for_empty():
    lm = _empty_label_map()
    records = list(encode_chip_predictions(
        lm,
        tile_id="T29TPG", block_row=0, block_col=0,
        chip_kind="original", grid_row=0, grid_col=0,
        date_ordinal=738887, date_iso="2024-01-01",
        block_world_origin_x=500000.0, block_world_origin_y=4500000.0,
        pixel_res=10.0,
    ))
    assert records == []
    print("  encode (empty label map -> no records) — OK")


def test_encode_emits_one_record_for_one_class():
    lm = _square_label_map(y0=20, x0=30, side=10, cls=1)
    records = list(encode_chip_predictions(
        lm,
        tile_id="T29TPG", block_row=2, block_col=3,
        chip_kind="original", grid_row=1, grid_col=0,
        date_ordinal=738887, date_iso="2024-01-01",
        block_world_origin_x=500000.0, block_world_origin_y=4500000.0,
        pixel_res=10.0,
    ))
    assert len(records) == 1
    r = records[0]
    assert isinstance(r, ChipPredictionRecord)
    # Identity 6-tuple
    assert r.tile_id == "T29TPG"
    assert r.block_row == 2
    assert r.block_col == 3
    assert r.chip_kind == "original"
    assert r.grid_row == 1
    assert r.grid_col == 0
    # Counts
    assert r.n_pixels_by_class == {1: 100}
    assert set(r.masks_by_class.keys()) == {1}
    # Chip NW offset: original at (1, 0) -> (256, 0)
    assert r.chip_nw_px_y == 256
    assert r.chip_nw_px_x == 0
    print("  encode (single class -> one record) — OK")


def test_encode_emits_one_record_with_multiple_classes():
    """Both Cuts and Fires in the same chip -> one record with two
    per-class masks."""
    lm = _empty_label_map()
    lm[10:15, 10:15] = 1
    lm[10:15, 15:20] = 2
    records = list(encode_chip_predictions(
        lm,
        tile_id="T29TPG", block_row=0, block_col=0,
        chip_kind="original", grid_row=0, grid_col=0,
        date_ordinal=738887, date_iso="2024-01-01",
        block_world_origin_x=0.0, block_world_origin_y=0.0, pixel_res=10.0,
    ))
    assert len(records) == 1
    r = records[0]
    assert r.n_pixels_by_class == {1: 25, 2: 25}
    assert set(r.masks_by_class.keys()) == {1, 2}
    print("  encode (two classes -> one record, two masks) — OK")


def test_encode_no_size_filter():
    """No minimum-component filter: a 1-pixel speckle still produces a record
    (predict.py's `postprocess_prediction` is the only filter upstream)."""
    lm = _empty_label_map()
    lm[5, 5] = 1   # single-pixel speckle
    records = list(encode_chip_predictions(
        lm,
        tile_id="T", block_row=0, block_col=0,
        chip_kind="original", grid_row=0, grid_col=0,
        date_ordinal=738887, date_iso="2024-01-01",
        block_world_origin_x=0.0, block_world_origin_y=0.0, pixel_res=10.0,
    ))
    assert len(records) == 1
    assert records[0].n_pixels_by_class == {1: 1}
    print("  encode (no size filter -> emits 1-pixel speckle) — OK")


def test_encode_rle_roundtrip():
    """RLE encoded mask decodes back to the exact input boolean mask."""
    lm = _empty_label_map()
    # Cross shape
    lm[100:120, 110:115] = 1
    lm[110:115, 100:120] = 1
    records = list(encode_chip_predictions(
        lm,
        tile_id="T", block_row=0, block_col=0,
        chip_kind="original", grid_row=0, grid_col=0,
        date_ordinal=738887, date_iso="2024-01-01",
        block_world_origin_x=0.0, block_world_origin_y=0.0, pixel_res=10.0,
    ))
    assert len(records) == 1
    r = records[0]
    rle = r.masks_by_class[1]
    reconstructed = np.zeros(CHIP_H * CHIP_W, dtype=bool)
    for start, length in zip(rle[0], rle[1]):
        reconstructed[int(start):int(start) + int(length)] = True
    reconstructed = reconstructed.reshape(CHIP_H, CHIP_W)
    assert np.array_equal(reconstructed, lm == 1)
    print("  encode (RLE roundtrip) — OK")


def test_encode_to_dict_flattens_per_class():
    """to_dict() should expand the per-class dicts into separate columns."""
    lm = _empty_label_map()
    lm[0:5, 0:5] = 1
    lm[10:13, 10:13] = 2
    [record] = list(encode_chip_predictions(
        lm,
        tile_id="T", block_row=0, block_col=0,
        chip_kind="original", grid_row=0, grid_col=0,
        date_ordinal=738887, date_iso="2024-01-01",
        block_world_origin_x=0.0, block_world_origin_y=0.0, pixel_res=10.0,
    ))
    d = record.to_dict()
    # The per-class dicts should be gone, replaced by per-class columns.
    assert "n_pixels_by_class" not in d
    assert "masks_by_class" not in d
    assert d["n_pixels_cls_1"] == 25
    assert d["n_pixels_cls_2"] == 9
    assert isinstance(d["rle_cls_1"], list)
    assert isinstance(d["rle_cls_2"], list)
    # Lengths are 2 * n_runs (flat [start, length] pairs).
    assert len(d["rle_cls_1"]) % 2 == 0
    print("  encode.to_dict (per-class flattening) — OK")


def test_encode_decode_roundtrip_via_to_dict():
    """End-to-end: encode_chip_predictions -> to_dict -> _decode_rle should
    reproduce the original per-class boolean masks."""
    lm = _empty_label_map()
    lm[50:60, 100:110] = 1
    lm[200:210, 50:60] = 2
    [record] = list(encode_chip_predictions(
        lm,
        tile_id="T", block_row=0, block_col=0,
        chip_kind="original", grid_row=0, grid_col=0,
        date_ordinal=738887, date_iso="2024-01-01",
        block_world_origin_x=0.0, block_world_origin_y=0.0, pixel_res=10.0,
    ))
    d = record.to_dict()
    cls1_mask = _decode_rle(d["rle_cls_1"])
    cls2_mask = _decode_rle(d["rle_cls_2"])
    assert np.array_equal(cls1_mask, lm == 1)
    assert np.array_equal(cls2_mask, lm == 2)
    print("  encode (to_dict -> _decode_rle roundtrip) — OK")


# ============================================================================
# shard.py
# ============================================================================

def _make_record(**kwargs) -> ChipPredictionRecord:
    """Minimal valid ChipPredictionRecord with hand-picked defaults."""
    defaults = dict(
        tile_id="T29TPG", block_row=0, block_col=0,
        chip_kind="original", grid_row=0, grid_col=0,
        date_ordinal=738887, date_iso="2024-01-01",
        block_world_origin_x=500000.0, block_world_origin_y=4500000.0,
        chip_nw_px_y=0, chip_nw_px_x=0, pixel_res=10.0,
        n_pixels_by_class={1: 10},
        masks_by_class={1: np.array([[0, 256], [5, 5]], dtype=np.uint16)},
    )
    defaults.update(kwargs)
    return ChipPredictionRecord(**defaults)


def test_shard_path_for_block():
    p = shard_path_for_block("/tmp/out", "T29TPG", 12, 7)
    assert p == "/tmp/out/T29TPG_block_012_007.parquet"
    print("  shard_path_for_block — OK")


def test_write_task_shard_roundtrip():
    with tempfile.TemporaryDirectory() as tmpd:
        records = [
            _make_record(),
            _make_record(
                chip_kind="h_shift", grid_row=1, grid_col=2,
                n_pixels_by_class={2: 42},
                masks_by_class={2: np.array([[100, 500], [3, 7]], dtype=np.uint16)},
            ),
        ]
        path = write_task_shard(records, tmpd, "T29TPG", 0, 0)
        assert os.path.exists(path)

        df = read_shards(tmpd)
        assert len(df) == 2
        # Both records share the base columns; per-class columns are present
        # for the classes that appeared.
        for col in (
            "tile_id", "block_row", "block_col",
            "chip_kind", "grid_row", "grid_col",
            "date_ordinal", "date_iso",
            "block_world_origin_x", "block_world_origin_y",
            "chip_nw_px_y", "chip_nw_px_x", "pixel_res",
        ):
            assert col in df.columns, col
        # One row had cls_1, the other had cls_2 — both columns should exist
        # with NaN where the row didn't have that class.
        assert "n_pixels_cls_1" in df.columns
        assert "n_pixels_cls_2" in df.columns
    print("  write_task_shard + read_shards roundtrip — OK")


def test_write_empty_task_shard():
    with tempfile.TemporaryDirectory() as tmpd:
        path = write_task_shard([], tmpd, "T29TPG", 0, 0)
        assert os.path.exists(path)
        df = pd.read_parquet(path)
        assert len(df) == 0
        # The fixed base column set should still be there.
        for col in ("tile_id", "block_row", "block_col"):
            assert col in df.columns
    print("  write_task_shard (empty records) — OK")


def test_read_shards_tile_filter():
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
    test_chip_nw_pixel_offset_all_kinds()
    test_encode_yields_nothing_for_empty()
    test_encode_emits_one_record_for_one_class()
    test_encode_emits_one_record_with_multiple_classes()
    test_encode_no_size_filter()
    test_encode_rle_roundtrip()
    test_encode_to_dict_flattens_per_class()
    test_encode_decode_roundtrip_via_to_dict()
    test_shard_path_for_block()
    test_write_task_shard_roundtrip()
    test_write_empty_task_shard()
    test_read_shards_tile_filter()
    print("All postprocess tests passed.")


if __name__ == "__main__":
    main()
