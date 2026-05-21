"""Tests for the chip-chunked HDF5 reader.

Builds tiny synthetic chip-chunked HDF5 files in a temp directory matching
the schema from `rechunk_hdf5_chip_oriented.py`, then asserts the reader
returns the right block content, handles missing chips correctly, and
applies the q02/q98 stretch the same way as `dataset_swin_GZ._to_uint8`.

Run:
    python test_input_setup.py
"""
import os
import sys
import tempfile

import h5py
import numpy as np

from hdf5_reader import (
    read_block,
    iter_blocks,
    get_block_grid_shape,
    dry_run,
    BlockPosition,
    CHIP_PIXELS,
    BLOCK_GRID_ROWS,
    BLOCK_GRID_COLS,
    LIVE_ROWS,
    LIVE_COLS,
    NODATA_U8,
)

CHIP_SIZE = 256
N_TS = 4
N_BANDS = 10
DEFAULT_NODATA_U16 = 65_535


# ============================================================================
# SYNTHETIC HDF5 BUILDER
# ============================================================================

def _write_synthetic_hdf5(
    path: str,
    present_positions: list[tuple[int, int]],   # list of (chip_y, chip_x)
    n_ts: int = N_TS,
    nodata_val: int = DEFAULT_NODATA_U16,
    start_ordinal: int = 738887,                # 2024-01-01
    ts_stride_days: int = 5,
):
    """Build a minimal chip-chunked HDF5 file matching rechunker output.

    Each chip's 65_536 pixel slots are filled with a linspace whose base
    depends on (chip_idx, t, b) — easy to verify with hand calc, and gives
    percentile() a non-degenerate distribution per band.
    """
    n_chips = len(present_positions)
    n_pixels = n_chips * CHIP_PIXELS

    chip_y = np.array([p[0] for p in present_positions], dtype=np.int32)
    chip_x = np.array([p[1] for p in present_positions], dtype=np.int32)
    chip_pixel_count = np.full(n_chips, CHIP_PIXELS, dtype=np.int32)
    ts = np.arange(start_ordinal, start_ordinal + n_ts * ts_stride_days,
                   ts_stride_days, dtype=np.int32)

    with h5py.File(path, "w") as h5f:
        values = h5f.create_dataset(
            "values",
            shape=(n_ts, N_BANDS, n_pixels),
            dtype=np.uint16,
            chunks=(n_ts, N_BANDS, CHIP_PIXELS),
        )
        for chip_idx in range(n_chips):
            for t in range(n_ts):
                for b in range(N_BANDS):
                    base = chip_idx * 1000 + t * 100 + b * 10
                    chip_slot = np.linspace(base, base + 800, CHIP_PIXELS).astype(np.uint16)
                    s = chip_idx * CHIP_PIXELS
                    values[t, b, s:s + CHIP_PIXELS] = chip_slot

        h5f.create_dataset("chip_x_bin", data=chip_x)
        h5f.create_dataset("chip_y_bin", data=chip_y)
        h5f.create_dataset("chip_pixel_count", data=chip_pixel_count)
        h5f.create_dataset("ts", data=ts)
        # sort_order / xs_new / ys_new aren't read by hdf5_reader.py, but the
        # rechunker writes them — create stubs so the file is well-formed.
        h5f.create_dataset("sort_order",
                           data=np.full(n_pixels, -1, dtype=np.int64))
        h5f.create_dataset("xs_new",
                           data=np.full(n_pixels, -9999, dtype=np.int32))
        h5f.create_dataset("ys_new",
                           data=np.full(n_pixels, -9999, dtype=np.int32))

        h5f.attrs["chip_size"] = CHIP_SIZE
        h5f.attrs["pixel_res"] = 10
        h5f.attrs["n_ts"] = n_ts
        h5f.attrs["nodata_val"] = nodata_val
        # band_names: variable-length string array (matches the real file's
        # H5T_C_S1 / variable schema in the dump).
        h5f.attrs.create(
            "band_names",
            data=[f"B{i}".encode() for i in range(N_BANDS)],
            dtype=h5py.special_dtype(vlen=bytes),
        )
        h5f.attrs["crs"] = "EPSG:32629"


# ============================================================================
# TESTS
# ============================================================================

def test_get_block_grid_shape():
    """Chip-grid extent determines block-grid dimensions."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        # Chips occupy chip-grid (0,0)..(4,7) — 5 rows, 8 cols of chips
        positions = [(y, x) for y in range(5) for x in range(8)]
        _write_synthetic_hdf5(path, positions)
        # max_y=4 -> ceil((4+1)/4) = 2 block rows
        # max_x=7 -> ceil((7+1)/4) = 2 block cols
        n_rows, n_cols = get_block_grid_shape(path)
        assert (n_rows, n_cols) == (2, 2), (n_rows, n_cols)
    print("  get_block_grid_shape — OK")


def test_read_block_all_chips_present():
    """A block where all 25 chip-grid positions exist in the file should be
    fully populated (no NODATA_U8 in any slot)."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        # 5x5 dense block at chip-grid origin
        positions = [(y, x) for y in range(5) for x in range(5)]
        _write_synthetic_hdf5(path, positions)

        block, ts, position = read_block(path, 0, 0)
        assert block.shape == (N_TS, 10,
                               BLOCK_GRID_ROWS * BLOCK_GRID_COLS * CHIP_PIXELS)
        assert block.dtype == np.uint8
        assert ts.shape == (N_TS,)
        assert position == BlockPosition(0, 0, 0, 0)
        # Every slot should have a real value (stretch can produce 254 max for
        # genuine values; the 255 sentinel only appears on real nodata).
        assert not (block == NODATA_U8).any(), \
            f"unexpected NODATA in fully-dense block: " \
            f"{(block == NODATA_U8).sum()} cells"
    print("  read_block (all chips present) — OK")


def test_read_block_missing_chips_filled_with_nodata():
    """Chips absent from the HDF5 should appear as NODATA_U8 in the block."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        # Only chips at (0,0), (1,1), (2,2) exist — sparse cross pattern
        positions = [(0, 0), (1, 1), (2, 2)]
        _write_synthetic_hdf5(path, positions)

        block, ts, position = read_block(path, 0, 0)
        # The 25-chip-block laid out row-major. Slot for chip-grid (R, C) is
        # at block-index (R * 5 + C).
        for r in range(5):
            for c in range(5):
                slot = (r * 5 + c) * CHIP_PIXELS
                end = slot + CHIP_PIXELS
                chip_data = block[:, :, slot:end]
                if (r, c) in positions:
                    # Real data: should have no nodata.
                    assert not (chip_data == NODATA_U8).any(), \
                        f"chip ({r},{c}) had unexpected NODATA"
                else:
                    # Missing chip: every cell should be NODATA_U8.
                    assert (chip_data == NODATA_U8).all(), \
                        f"chip ({r},{c}) was missing but not all-NODATA"
    print("  read_block (missing chips -> nodata) — OK")


def test_read_block_off_tile_chips_filled_with_nodata():
    """Block at the right/bottom edge of the tile should pad missing
    positions (i.e. ones past the data) with NODATA_U8."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        # Chip grid is 4 rows x 4 cols (chip positions 0..3 inclusive).
        # Block (0, 0) has live area covering rows 0..3, cols 0..3 (all present),
        # but the ghost row (r=4) and ghost col (c=4) are OFF-TILE — should pad.
        positions = [(y, x) for y in range(4) for x in range(4)]
        _write_synthetic_hdf5(path, positions)

        # We should still need exactly one block: get_block_grid_shape returns
        # ceil((3+1)/4) = 1 in both dimensions.
        n_rows, n_cols = get_block_grid_shape(path)
        assert (n_rows, n_cols) == (1, 1), (n_rows, n_cols)

        block, ts, position = read_block(path, 0, 0)
        # Live 4x4 should be data; row 4 and col 4 should be NODATA.
        # Inner positions (r in 0..3, c in 0..3) should have real values.
        for r in range(4):
            for c in range(4):
                slot = (r * 5 + c) * CHIP_PIXELS
                end = slot + CHIP_PIXELS
                assert not (block[:, :, slot:end] == NODATA_U8).any(), \
                    f"inner chip ({r},{c}) had unexpected NODATA"
        # Ghost row (r=4): all c.
        for c in range(5):
            slot = (4 * 5 + c) * CHIP_PIXELS
            end = slot + CHIP_PIXELS
            assert (block[:, :, slot:end] == NODATA_U8).all(), \
                f"ghost row chip (4,{c}) wasn't all NODATA"
        # Ghost col (c=4): all r.
        for r in range(5):
            slot = (r * 5 + 4) * CHIP_PIXELS
            end = slot + CHIP_PIXELS
            assert (block[:, :, slot:end] == NODATA_U8).all(), \
                f"ghost col chip ({r},4) wasn't all NODATA"
    print("  read_block (off-tile -> nodata) — OK")


def test_read_block_ts_range_filter():
    """ts_start_ordinal / ts_end_ordinal filter the time axis correctly."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        positions = [(0, 0), (0, 1)]
        # ts_stride_days = 5, start = 2024-01-01 -> ts = [738887, 738892, 738897, 738902]
        _write_synthetic_hdf5(path, positions, n_ts=4, ts_stride_days=5)

        # No filter -> all 4 timesteps
        block, ts, _ = read_block(path, 0, 0)
        assert len(ts) == 4

        # Inclusive filter that keeps only timesteps 1 and 2
        block, ts, _ = read_block(
            path, 0, 0,
            ts_start_ordinal=738892, ts_end_ordinal=738897,
        )
        assert len(ts) == 2
        assert ts.tolist() == [738892, 738897]
        assert block.shape[0] == 2
    print("  read_block (ts range filter) — OK")


def test_iter_blocks_covers_full_grid():
    """iter_blocks should yield exactly n_block_rows * n_block_cols blocks
    in row-major order."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        # 8x12 chip grid -> 2x3 block grid
        positions = [(y, x) for y in range(8) for x in range(12)]
        _write_synthetic_hdf5(path, positions)
        n_rows, n_cols = get_block_grid_shape(path)
        assert (n_rows, n_cols) == (2, 3), (n_rows, n_cols)

        positions_seen = []
        for block, ts, pos in iter_blocks(path):
            positions_seen.append((pos.block_row, pos.block_col))
            assert block.shape == (N_TS, 10,
                                   BLOCK_GRID_ROWS * BLOCK_GRID_COLS * CHIP_PIXELS)

        expected = [(r, c) for r in range(n_rows) for c in range(n_cols)]
        assert positions_seen == expected, positions_seen
    print(f"  iter_blocks (covers full grid, row-major) — OK")


def test_iter_blocks_filter():
    """block_filter should skip blocks without reading them."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        positions = [(y, x) for y in range(8) for x in range(8)]
        _write_synthetic_hdf5(path, positions)
        # Keep only odd-sum blocks
        kept = list(iter_blocks(
            path,
            block_filter=lambda br, bc: (br + bc) % 2 == 1,
        ))
        # 2x2 block grid -> kept = (0,1), (1,0) -> 2 blocks
        assert len(kept) == 2, len(kept)
        seen = {(p.block_row, p.block_col) for _, _, p in kept}
        assert seen == {(0, 1), (1, 0)}, seen
    print("  iter_blocks (block_filter) — OK")


def test_stretch_matches_dataset_swin_gz_semantics():
    """The reader's stretch must produce the same per-band q02/q98 mapping
    as `pybacdm.shared.bacdm.data.dataset_swin_GZ._to_uint8` for the same
    input. Easiest way to verify: build a chip whose values are known
    in advance, read it back, recompute q02/q98 by hand, and check that
    a sample pixel matches the expected mapping within rounding tolerance."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        positions = [(0, 0)]
        _write_synthetic_hdf5(path, positions, n_ts=1)

        # Recompute what the stretch SHOULD produce for chip 0, ts 0, band 0:
        # The synthetic data fills with linspace(base, base+800, CHIP_PIXELS).
        base = 0 * 1000 + 0 * 100 + 0 * 10   # 0
        expected_band = np.linspace(base, base + 800, CHIP_PIXELS).astype(np.uint16)
        q02, q98 = np.nanpercentile(expected_band.astype(np.float32),
                                    [2.0, 98.0])
        denom = float(q98 - q02) if q98 > q02 else 1.0
        scaled = np.clip(
            (expected_band.astype(np.float32) - q02) / denom * (NODATA_U8 - 1),
            0, NODATA_U8 - 1,
        ).astype(np.uint8)

        block, _, _ = read_block(path, 0, 0)
        # Block chip 0 = position (0,0) in 5x5 -> block-idx 0 -> first 65_536 slots.
        actual_band = block[0, 0, :CHIP_PIXELS]
        assert np.array_equal(actual_band, scaled), \
            f"stretch mismatch: first 5 actual={actual_band[:5]} expected={scaled[:5]}"
    print("  stretch matches _to_uint8 semantics — OK")


def test_stretch_passes_nodata_through():
    """Pixels that were uint16 nodata should be uint8 NODATA_U8 in the output."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        positions = [(0, 0)]
        _write_synthetic_hdf5(path, positions, n_ts=1)

        # Manually punch some nodata into the source file.
        with h5py.File(path, "r+") as h5f:
            v = h5f["values"]   # type: ignore[index]
            # First 1000 pixels of chip 0, band 0, ts 0 -> nodata
            v[0, 0, :1000] = DEFAULT_NODATA_U16

        block, _, _ = read_block(path, 0, 0)
        # Those pixels should now be NODATA_U8 in band 0.
        assert (block[0, 0, :1000] == NODATA_U8).all()
        # Other bands at those pixel positions should NOT be nodata.
        for b in range(1, 10):
            assert not (block[0, b, :1000] == NODATA_U8).all(), \
                f"band {b} got nodata where only band 0 should have"
    print("  stretch preserves nodata sentinel — OK")


def test_dry_run_summary_matches_grid():
    """dry_run's reported counts should agree with get_block_grid_shape."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        positions = [(y, x) for y in range(6) for x in range(7)]
        _write_synthetic_hdf5(path, positions)
        n_rows, n_cols = get_block_grid_shape(path)

        # Silence the print output for tests but capture the returned dict.
        import io, contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            summary = dry_run(path, n_target_dates=2)

        assert summary["n_block_rows"] == n_rows
        assert summary["n_block_cols"] == n_cols
        assert summary["n_blocks"] == n_rows * n_cols
        assert summary["n_chips_in_file"] == len(positions)
        assert summary["n_ts_total"] == N_TS
        assert "Block grid:" in buf.getvalue()
    print("  dry_run summary — OK")


def main():
    print("Running input_setup tests...")
    test_get_block_grid_shape()
    test_read_block_all_chips_present()
    test_read_block_missing_chips_filled_with_nodata()
    test_read_block_off_tile_chips_filled_with_nodata()
    test_read_block_ts_range_filter()
    test_iter_blocks_covers_full_grid()
    test_iter_blocks_filter()
    test_stretch_matches_dataset_swin_gz_semantics()
    test_stretch_passes_nodata_through()
    test_dry_run_summary_matches_grid()
    print("All input_setup tests passed.")


if __name__ == "__main__":
    main()
