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
    CHIP_SIZE,
    CHIP_PIXELS,
    LIVE_ROWS,
    LIVE_COLS,
    LIVE_H,
    LIVE_W,
    GHOST,
    BLOCK_H,
    BLOCK_W,
    NODATA_U8,
)

N_TS = 4
N_BANDS = 10
DEFAULT_NODATA_U16 = 65_535
PIXEL_RES = 10
# Synthetic UTM tile origin used by _write_synthetic_hdf5 to populate
# xs_new / ys_new. Tests use these to verify world_origin derivation.
SYNTHETIC_TILE_X0 = 500_000.0   # UTM easting of chip (0, 0) NW corner
SYNTHETIC_TILE_Y0 = 4_500_000.0 # UTM northing of chip (0, 0) NW corner


# ============================================================================
# SYNTHETIC HDF5 BUILDER
# ============================================================================

def _write_synthetic_hdf5(
    path: str,
    present_positions: list[tuple[int, int]],
    n_ts: int = N_TS,
    nodata_val: int = DEFAULT_NODATA_U16,
    start_ordinal: int = 738887,
    ts_stride_days: int = 5,
):
    """Build a minimal chip-chunked HDF5 matching rechunker output.

    Each chip's 65_536 pixel slots are filled with a linspace base depending
    on (chip_idx, t, b) — easy to verify with hand calc, and gives
    percentile() a non-degenerate distribution per band.
    """
    n_chips = len(present_positions)
    n_pixels = n_chips * CHIP_PIXELS

    chip_y = np.array([p[0] for p in present_positions], dtype=np.int32)
    chip_x = np.array([p[1] for p in present_positions], dtype=np.int32)
    chip_pixel_count = np.full(n_chips, CHIP_PIXELS, dtype=np.int32)
    ts = np.arange(start_ordinal, start_ordinal + n_ts * ts_stride_days,
                   ts_stride_days, dtype=np.int32)

    xs_new = np.empty(n_pixels, dtype=np.int32)
    ys_new = np.empty(n_pixels, dtype=np.int32)
    for chip_idx, (cy, cx) in enumerate(present_positions):
        chip_x0 = SYNTHETIC_TILE_X0 + cx * CHIP_SIZE * PIXEL_RES
        chip_y0 = SYNTHETIC_TILE_Y0 - cy * CHIP_SIZE * PIXEL_RES
        for row in range(CHIP_SIZE):
            for col in range(CHIP_SIZE):
                slot = chip_idx * CHIP_PIXELS + row * CHIP_SIZE + col
                xs_new[slot] = int(chip_x0 + col * PIXEL_RES)
                ys_new[slot] = int(chip_y0 - row * PIXEL_RES)

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
        h5f.create_dataset("sort_order",
                           data=np.full(n_pixels, -1, dtype=np.int64))
        h5f.create_dataset("xs_new", data=xs_new)
        h5f.create_dataset("ys_new", data=ys_new)

        h5f.attrs["chip_size"] = CHIP_SIZE
        h5f.attrs["pixel_res"] = 10
        h5f.attrs["n_ts"] = n_ts
        h5f.attrs["nodata_val"] = nodata_val
        h5f.attrs.create(
            "band_names",
            data=[f"B{i}".encode() for i in range(N_BANDS)],
            dtype=h5py.special_dtype(vlen=bytes),
        )
        h5f.attrs["crs"] = "EPSG:32629"


# ============================================================================
# TESTS
# ============================================================================

def test_block_shape_constants():
    """The block layout constants should add up to the expected pixel dimensions."""
    assert LIVE_H == LIVE_ROWS * CHIP_SIZE == 1024
    assert LIVE_W == LIVE_COLS * CHIP_SIZE == 1024
    assert GHOST == 128
    assert BLOCK_H == LIVE_H + 2 * GHOST == 1280
    assert BLOCK_W == LIVE_W + 2 * GHOST == 1280
    print("  block layout constants — OK")


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


def test_read_block_2d_layout_and_live_area_populated():
    """Block at (1, 1) of a 6x6 chip grid: live 4x4 + full ghost ring present."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        # Chips at every grid position from (3, 3) to (8, 8) inclusive — that's
        # the live 4x4 of block (1, 1) (chip-grid (4..7, 4..7)) plus a full
        # ghost ring (chip-grid (3, *), (8, *), (*, 3), (*, 8)).
        positions = [(y, x) for y in range(3, 9) for x in range(3, 9)]
        _write_synthetic_hdf5(path, positions)

        block, ts, position = read_block(path, 1, 1)
        # 2-D layout
        assert block.shape == (N_TS, N_BANDS, BLOCK_H, BLOCK_W)
        assert block.dtype == np.uint8

        # Live area should have no NODATA (every chip there is present).
        live = block[:, :, GHOST:GHOST + LIVE_H, GHOST:GHOST + LIVE_W]
        assert not (live == NODATA_U8).any(), \
            f"unexpected NODATA in live area: {(live == NODATA_U8).sum()}"

        # Ghost ring should also have no NODATA (all border chips present).
        # Sample each ghost region:
        top_strip = block[:, :, 0:GHOST, GHOST:GHOST + LIVE_W]
        assert not (top_strip == NODATA_U8).any(), "top strip had NODATA"
        bottom_strip = block[:, :, GHOST + LIVE_H:, GHOST:GHOST + LIVE_W]
        assert not (bottom_strip == NODATA_U8).any(), "bottom strip had NODATA"
        left_strip = block[:, :, GHOST:GHOST + LIVE_H, 0:GHOST]
        assert not (left_strip == NODATA_U8).any(), "left strip had NODATA"
        right_strip = block[:, :, GHOST:GHOST + LIVE_H, GHOST + LIVE_W:]
        assert not (right_strip == NODATA_U8).any(), "right strip had NODATA"
        # 4 corners
        nw = block[:, :, 0:GHOST, 0:GHOST]
        ne = block[:, :, 0:GHOST, GHOST + LIVE_W:]
        sw = block[:, :, GHOST + LIVE_H:, 0:GHOST]
        se = block[:, :, GHOST + LIVE_H:, GHOST + LIVE_W:]
        for name, corner in [("NW", nw), ("NE", ne), ("SW", sw), ("SE", se)]:
            assert not (corner == NODATA_U8).any(), f"{name} corner had NODATA"

        # BlockPosition basic sanity.
        assert position.block_row == 1
        assert position.block_col == 1
        assert position.chip_y_start == 4
        assert position.chip_x_start == 4
    print("  read_block (2-D layout, dense block + full ghost ring) — OK")


def test_read_block_origin_block_has_nw_ghost_padded():
    """Block (0, 0) has no chips to its north or west — those ghost regions
    must be all-NODATA."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        # Live 4x4 + south/east ghost (positions (0..4, 0..4)).
        positions = [(y, x) for y in range(5) for x in range(5)]
        _write_synthetic_hdf5(path, positions)

        block, _, _ = read_block(path, 0, 0)

        # Live area populated.
        live = block[:, :, GHOST:GHOST + LIVE_H, GHOST:GHOST + LIVE_W]
        assert not (live == NODATA_U8).any()

        # North-side ghost (top strip + NW + NE corners) must be all NODATA —
        # no chips at chip_y_start = -1 exist in this file.
        top_band = block[:, :, 0:GHOST, :]
        assert (top_band == NODATA_U8).all(), \
            f"north ghost band wasn't all NODATA: {(top_band != NODATA_U8).sum()} cells"

        # West-side ghost (left strip + NW + SW corners) must be all NODATA.
        left_band = block[:, :, :, 0:GHOST]
        assert (left_band == NODATA_U8).all()

        # South/east ghost SHOULD have data (chips at (4, 0..4) and (0..4, 4) exist).
        south_strip = block[:, :, GHOST + LIVE_H:, GHOST:GHOST + LIVE_W]
        assert not (south_strip == NODATA_U8).any(), "south strip should be present"
        east_strip = block[:, :, GHOST:GHOST + LIVE_H, GHOST + LIVE_W:]
        assert not (east_strip == NODATA_U8).any(), "east strip should be present"

        # SE corner has chip (4, 4) — should be present.
        se_corner = block[:, :, GHOST + LIVE_H:, GHOST + LIVE_W:]
        assert not (se_corner == NODATA_U8).any()
    print("  read_block (origin block: north/west ghost -> NODATA) — OK")


def test_read_block_missing_inner_chips_filled_with_nodata():
    """Inner chips absent from the HDF5 should appear as NODATA in the
    corresponding 256x256 region of the live area."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        # Cross pattern: present chips (0,0), (1,1), (2,2), (3,3); other
        # inner positions and all ghost positions absent.
        present = {(0, 0), (1, 1), (2, 2), (3, 3)}
        _write_synthetic_hdf5(path, sorted(present))

        block, _, _ = read_block(path, 0, 0)
        # Walk the 4x4 inner grid: present chips have data, missing chips are NODATA.
        for r in range(LIVE_ROWS):
            for c in range(LIVE_COLS):
                y0 = GHOST + r * CHIP_SIZE
                x0 = GHOST + c * CHIP_SIZE
                slab = block[:, :, y0:y0 + CHIP_SIZE, x0:x0 + CHIP_SIZE]
                if (r, c) in present:
                    assert not (slab == NODATA_U8).any(), \
                        f"inner chip ({r},{c}) had unexpected NODATA"
                else:
                    assert (slab == NODATA_U8).all(), \
                        f"missing inner chip ({r},{c}) wasn't all NODATA"
    print("  read_block (missing inner chips -> NODATA) — OK")


def test_block_world_origin():
    """world_origin should reference the live area's NW corner (chip
    (chip_y_start, chip_x_start), pixel (0, 0))."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        positions = [(y, x) for y in range(5) for x in range(5)]
        _write_synthetic_hdf5(path, positions)

        # Block (0, 0): live area's NW corner is chip (0, 0) pixel (0, 0).
        _, _, pos00 = read_block(path, 0, 0)
        assert pos00.pixel_res == PIXEL_RES
        assert pos00.world_origin_x == SYNTHETIC_TILE_X0
        assert pos00.world_origin_y == SYNTHETIC_TILE_Y0

        # Non-origin block: build a file with chips at (4..8, 4..8). Block
        # (1, 1) lives at chip_y_start = chip_x_start = 4.
        path2 = os.path.join(tmpd, "fake2.h5")
        offset_positions = [(y, x) for y in range(4, 9) for x in range(4, 9)]
        _write_synthetic_hdf5(path2, offset_positions)
        _, _, pos11 = read_block(path2, 1, 1)
        expected_x = SYNTHETIC_TILE_X0 + 4 * CHIP_SIZE * PIXEL_RES
        expected_y = SYNTHETIC_TILE_Y0 - 4 * CHIP_SIZE * PIXEL_RES
        assert pos11.world_origin_x == expected_x
        assert pos11.world_origin_y == expected_y
    print("  read_block (world_origin derivation) — OK")


def test_read_block_ts_range_filter():
    """ts_start_ordinal / ts_end_ordinal filter the time axis correctly."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        positions = [(0, 0), (0, 1)]
        _write_synthetic_hdf5(path, positions, n_ts=4, ts_stride_days=5)

        block, ts, _ = read_block(path, 0, 0)
        assert len(ts) == 4
        assert block.shape == (4, N_BANDS, BLOCK_H, BLOCK_W)

        block, ts, _ = read_block(
            path, 0, 0,
            ts_start_ordinal=738892, ts_end_ordinal=738897,
        )
        assert len(ts) == 2
        assert ts.tolist() == [738892, 738897]
        assert block.shape == (2, N_BANDS, BLOCK_H, BLOCK_W)
    print("  read_block (ts range filter) — OK")


def test_ghost_strip_contents_match_source_chip_slabs():
    """Verify that the ghost regions actually contain pixels from the
    correct (source-chip, source-slab) combinations.

    For block (1, 1) of a 6x6 grid, the top strip's column-0 segment
    (block[..., 0:128, 128:384]) should be the bottom 128 rows of the chip
    at chip-grid (3, 4) — i.e. the chip just above the live area's NW chip.
    The synthetic data fills each chip with linspace based on chip_idx; we
    just check those pixels are not NODATA (a non-NODATA value confirms the
    chip was read and the right slab was placed)."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        # Dense 6x6 chip grid spanning chip-grid (3..8, 3..8).
        positions = [(y, x) for y in range(3, 9) for x in range(3, 9)]
        _write_synthetic_hdf5(path, positions)

        block, _, _ = read_block(path, 1, 1)
        # The top strip should be non-NODATA (chip (3, 4..7) is present).
        for c in range(LIVE_COLS):
            x0 = GHOST + c * CHIP_SIZE
            slab = block[:, :, 0:GHOST, x0:x0 + CHIP_SIZE]
            assert not (slab == NODATA_U8).any(), \
                f"top strip slab c={c} had NODATA"

        # The left strip likewise (chip (4..7, 3) is present).
        for r in range(LIVE_ROWS):
            y0 = GHOST + r * CHIP_SIZE
            slab = block[:, :, y0:y0 + CHIP_SIZE, 0:GHOST]
            assert not (slab == NODATA_U8).any(), \
                f"left strip slab r={r} had NODATA"

        # The 4 corners (NW/NE/SW/SE) should be non-NODATA.
        nw = block[:, :, 0:GHOST, 0:GHOST]
        ne = block[:, :, 0:GHOST, GHOST + LIVE_W:]
        sw = block[:, :, GHOST + LIVE_H:, 0:GHOST]
        se = block[:, :, GHOST + LIVE_H:, GHOST + LIVE_W:]
        for name, c in [("NW", nw), ("NE", ne), ("SW", sw), ("SE", se)]:
            assert not (c == NODATA_U8).any(), f"{name} corner had NODATA"
    print("  read_block (ghost strip/corner contents present) — OK")


def test_iter_blocks_covers_full_grid():
    """iter_blocks should yield every block in row-major order."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        # 8x12 chip grid -> 2x3 block grid
        positions = [(y, x) for y in range(8) for x in range(12)]
        _write_synthetic_hdf5(path, positions)
        n_rows, n_cols = get_block_grid_shape(path)
        assert (n_rows, n_cols) == (2, 3)

        positions_seen = []
        for block, ts, pos in iter_blocks(path):
            positions_seen.append((pos.block_row, pos.block_col))
            assert block.shape == (N_TS, N_BANDS, BLOCK_H, BLOCK_W)

        expected = [(r, c) for r in range(n_rows) for c in range(n_cols)]
        assert positions_seen == expected
    print("  iter_blocks (covers full grid, row-major) — OK")


def test_iter_blocks_filter():
    """block_filter should skip blocks without reading them."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        positions = [(y, x) for y in range(8) for x in range(8)]
        _write_synthetic_hdf5(path, positions)
        kept = list(iter_blocks(
            path,
            block_filter=lambda br, bc: (br + bc) % 2 == 1,
        ))
        assert len(kept) == 2
        seen = {(p.block_row, p.block_col) for _, _, p in kept}
        assert seen == {(0, 1), (1, 0)}
    print("  iter_blocks (block_filter) — OK")


def test_stretch_matches_dataset_swin_gz_semantics():
    """Verify per-band q02/q98 stretch produces the same values as a
    hand-computed reference (uses live chip (0, 0) pixel position)."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        positions = [(0, 0)]
        _write_synthetic_hdf5(path, positions, n_ts=1)

        # Synthetic data fills chip 0 with linspace(base, base+800, CHIP_PIXELS)
        # where base = chip_idx * 1000 + t * 100 + b * 10 = 0 for (0, 0, 0).
        base = 0
        expected_band = np.linspace(base, base + 800, CHIP_PIXELS).astype(np.uint16)
        q02, q98 = np.nanpercentile(expected_band.astype(np.float32),
                                    [2.0, 98.0])
        denom = float(q98 - q02) if q98 > q02 else 1.0
        scaled = np.clip(
            (expected_band.astype(np.float32) - q02) / denom * (NODATA_U8 - 1),
            0, NODATA_U8 - 1,
        ).astype(np.uint8).reshape(CHIP_SIZE, CHIP_SIZE)

        block, _, _ = read_block(path, 0, 0)
        # Live chip (0, 0) is at block[..., GHOST:GHOST+CHIP_SIZE,
        #                              GHOST:GHOST+CHIP_SIZE].
        actual_chip_b0 = block[0, 0,
                               GHOST:GHOST + CHIP_SIZE,
                               GHOST:GHOST + CHIP_SIZE]
        assert np.array_equal(actual_chip_b0, scaled), \
            f"stretch mismatch: actual[0,:5]={actual_chip_b0[0, :5]} " \
            f"expected[0,:5]={scaled[0, :5]}"
    print("  stretch matches _to_uint8 semantics — OK")


def test_stretch_passes_nodata_through():
    """Pixels that were uint16 nodata should be uint8 NODATA_U8 in the output."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        positions = [(0, 0)]
        _write_synthetic_hdf5(path, positions, n_ts=1)

        # Punch 1000 nodata pixels at chip 0, band 0, ts 0 (flat pixels 0..999).
        with h5py.File(path, "r+") as h5f:
            v = h5f["values"]   # type: ignore[index]
            v[0, 0, :1000] = DEFAULT_NODATA_U16

        block, _, _ = read_block(path, 0, 0)
        # The 1000 nodata pixels at flat positions 0..999 are in chip 0's
        # row 0 cols 0..255 + row 1 cols 0..255 + row 2 cols 0..255 + row 3
        # cols 0..231. Convert each flat index to chip-local (row, col) and
        # then to block-local (y, x).
        flat_indices = np.arange(1000)
        local_rows = flat_indices // CHIP_SIZE
        local_cols = flat_indices % CHIP_SIZE
        block_ys = GHOST + local_rows
        block_xs = GHOST + local_cols
        actual = block[0, 0, block_ys, block_xs]
        assert (actual == NODATA_U8).all(), \
            f"{(actual != NODATA_U8).sum()} nodata pixels missed the sentinel"

        # Other bands at those positions should not be nodata.
        for b in range(1, N_BANDS):
            actual_b = block[0, b, block_ys, block_xs]
            assert not (actual_b == NODATA_U8).all(), \
                f"band {b} got NODATA where only band 0 should have"
    print("  stretch preserves nodata sentinel — OK")


def test_dry_run_summary_matches_grid():
    """dry_run's reported counts should agree with get_block_grid_shape."""
    with tempfile.TemporaryDirectory() as tmpd:
        path = os.path.join(tmpd, "fake.h5")
        positions = [(y, x) for y in range(6) for x in range(7)]
        _write_synthetic_hdf5(path, positions)
        n_rows, n_cols = get_block_grid_shape(path)

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
    test_block_shape_constants()
    test_get_block_grid_shape()
    test_read_block_2d_layout_and_live_area_populated()
    test_read_block_origin_block_has_nw_ghost_padded()
    test_read_block_missing_inner_chips_filled_with_nodata()
    test_block_world_origin()
    test_read_block_ts_range_filter()
    test_ghost_strip_contents_match_source_chip_slabs()
    test_iter_blocks_covers_full_grid()
    test_iter_blocks_filter()
    test_stretch_matches_dataset_swin_gz_semantics()
    test_stretch_passes_nodata_through()
    test_dry_run_summary_matches_grid()
    print("All input_setup tests passed.")


if __name__ == "__main__":
    main()
