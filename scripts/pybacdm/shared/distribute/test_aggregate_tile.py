"""End-to-end tests for aggregate_tile.py using synthetic per-block .npzes.

Builds fake block shards in a tempdir, runs the aggregator via
subprocess, and verifies:
  - The auxiliary .npz stitches blocks correctly (pixel-level)
  - The primary .parquet enumerates connected components with correct
    counts, bboxes, centroids, world coords, and RLE.
  - Failure modes (missing block, inconsistent metadata) surface as
    non-zero exit + diagnostic text.

Run:
    python test_aggregate_tile.py
"""
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))

from postprocess.voted_output import write_voted_block


# Use smaller-than-real block size to keep tests fast. 64x64 is plenty
# for component enumeration tests.
CHIP_SIZE = 64
TILE_ID = "T_TEST"
PIXEL_RES = 10.0
TILE_ORIGIN_X = 500_000.0
TILE_ORIGIN_Y = 4_500_000.0


def _empty_block_labels(n_dates: int = 2) -> np.ndarray:
    return np.zeros((n_dates, CHIP_SIZE, CHIP_SIZE), dtype=np.uint8)


def _block_labels_with_square(r: int, c: int,
                              date_idx: int, class_id: int,
                              y0: int, x0: int, side: int,
                              n_dates: int = 2) -> np.ndarray:
    """Per-block labels with one square of `class_id` on `date_idx`.
    The other dates / blocks remain all-bg."""
    out = _empty_block_labels(n_dates=n_dates)
    out[date_idx, y0:y0 + side, x0:x0 + side] = class_id
    return out


def _write_block(tmpd: Path, r: int, c: int, labels: np.ndarray,
                 target_dates: np.ndarray, classes: tuple[int, ...] = (1, 2),
                 threshold: int = 2) -> None:
    write_voted_block(
        str(tmpd), TILE_ID, r, c,
        labels=labels,
        target_dates=target_dates,
        classes=classes,
        world_origin_x=TILE_ORIGIN_X + c * CHIP_SIZE * PIXEL_RES,
        world_origin_y=TILE_ORIGIN_Y - r * CHIP_SIZE * PIXEL_RES,
        pixel_res=PIXEL_RES,
        threshold=threshold,
    )


def _write_synthetic_grid_empty(tmpd: Path, n_rows: int, n_cols: int,
                                n_dates: int = 2) -> np.ndarray:
    """Write all-bg block shards (so the merged tile has no components).
    Returns the target_dates array used."""
    target_dates = np.array([738887, 738900][:n_dates], dtype=np.int64)
    for r in range(n_rows):
        for c in range(n_cols):
            _write_block(tmpd, r, c, _empty_block_labels(n_dates=n_dates),
                         target_dates)
    return target_dates


def _run_aggregator(tmpd: Path) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["TILE_ID"] = TILE_ID
    env["OUTPUT_DIR"] = str(tmpd)
    return subprocess.run(
        [sys.executable, str(_HERE / "aggregate_tile.py")],
        env=env, capture_output=True, text=True,
    )


def _decode_rle(starts: np.ndarray, lengths: np.ndarray,
                shape: tuple[int, int]) -> np.ndarray:
    """Decode tile-relative RLE back into a 2-D bool mask."""
    flat = np.zeros(shape[0] * shape[1], dtype=bool)
    for s, l in zip(starts, lengths):
        flat[int(s):int(s) + int(l)] = True
    return flat.reshape(shape)


# ============================================================================
# Tests
# ============================================================================

def test_full_grid_stitches_into_dense_npz():
    """Same shape-correctness check the old test had — the .npz is still
    a first-class output."""
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        n_rows, n_cols, n_dates = 2, 3, 2
        target_dates = np.array([738887, 738900], dtype=np.int64)
        # Each block carries a uniform value (r*7 + c*3 + d*5)%254 + 1
        # so we can verify per-pixel placement.
        for r in range(n_rows):
            for c in range(n_cols):
                lab = _empty_block_labels(n_dates=n_dates)
                for d in range(n_dates):
                    lab[d, :, :] = ((r * 7 + c * 3 + d * 5) % 254) + 1
                _write_block(tmpd, r, c, lab, target_dates)

        res = _run_aggregator(tmpd)
        assert res.returncode == 0, f"stderr:\n{res.stderr}\nstdout:\n{res.stdout}"

        with np.load(tmpd / f"{TILE_ID}_tile.npz") as npz:
            labels = npz["labels"]
            assert labels.shape == (
                n_dates, n_rows * CHIP_SIZE, n_cols * CHIP_SIZE
            )
            assert int(npz["n_block_rows"]) == n_rows
            assert int(npz["n_block_cols"]) == n_cols
            for r in range(n_rows):
                for c in range(n_cols):
                    for d in range(n_dates):
                        expected = ((r * 7 + c * 3 + d * 5) % 254) + 1
                        got = labels[
                            d,
                            r * CHIP_SIZE:(r + 1) * CHIP_SIZE,
                            c * CHIP_SIZE:(c + 1) * CHIP_SIZE,
                        ]
                        assert (got == expected).all(), f"block ({r},{c}) date {d}"
    print("  dense .npz stitches correctly — OK")


def test_components_parquet_single_square_block():
    """One 5x5 class-1 square at LIVE (10, 10) in block (0, 0). Parquet
    must have exactly one component row with the expected geometry."""
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        target_dates = np.array([738887, 738900], dtype=np.int64)
        # Block (0, 0) gets one square; all other blocks all-bg.
        for r in range(2):
            for c in range(2):
                if r == 0 and c == 0:
                    lab = _block_labels_with_square(
                        r=0, c=0, date_idx=0, class_id=1,
                        y0=10, x0=10, side=5,
                    )
                else:
                    lab = _empty_block_labels()
                _write_block(tmpd, r, c, lab, target_dates)

        res = _run_aggregator(tmpd)
        assert res.returncode == 0, res.stderr

        df = pd.read_parquet(tmpd / f"{TILE_ID}_tile.parquet")
        assert len(df) == 1, f"expected 1 component, got {len(df)}"
        row = df.iloc[0]
        assert row["tile_id"] == TILE_ID
        assert row["date_ordinal"] == 738887
        assert row["date_iso"] == "2024-01-02"  # date.fromordinal(738887)
        assert row["class_id"] == 1
        assert row["n_pixels"] == 25
        assert row["bbox_y0"] == 10 and row["bbox_x0"] == 10
        assert row["bbox_y1"] == 15 and row["bbox_x1"] == 15
        # Centroid of a 5x5 at (10..14) is (12, 12).
        assert abs(row["centroid_y"] - 12.0) < 1e-6
        assert abs(row["centroid_x"] - 12.0) < 1e-6
        # World centroid:
        #   x = TILE_ORIGIN_X + 12 * PIXEL_RES = 500_000 + 120 = 500_120
        #   y = TILE_ORIGIN_Y - 12 * PIXEL_RES = 4_500_000 - 120 = 4_499_880
        assert abs(row["world_centroid_x"] - 500_120.0) < 1e-6
        assert abs(row["world_centroid_y"] - 4_499_880.0) < 1e-6
        # World bbox: x grows east (x0<x1), y grows south (y0<y1 after min/max).
        assert abs(row["world_bbox_x0"] - 500_100.0) < 1e-6
        assert abs(row["world_bbox_x1"] - 500_150.0) < 1e-6
        # RLE decode -> reconstructs the same 5x5 square at the same place.
        mask = _decode_rle(
            np.asarray(row["rle_starts"]),
            np.asarray(row["rle_lengths"]),
            (2 * CHIP_SIZE, 2 * CHIP_SIZE),
        )
        assert mask.sum() == 25
        assert mask[10:15, 10:15].all()
        assert not mask[:10, :].any() and not mask[15:, :].any()
    print("  Parquet: single-component bbox/centroid/world/RLE — OK")


def test_components_parquet_spanning_block_boundary():
    """A class-1 region that crosses the boundary between block (0, 0)
    and (0, 1) must yield ONE component in the tile-level Parquet, not
    two, because stitching happens before component enumeration."""
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        target_dates = np.array([738887, 738900], dtype=np.int64)
        # Block (0, 0): square at the east edge (x = CHIP_SIZE-3 .. CHIP_SIZE-1)
        b00 = _block_labels_with_square(
            r=0, c=0, date_idx=0, class_id=1,
            y0=20, x0=CHIP_SIZE - 3, side=3,
        )
        # Block (0, 1): square at the west edge (x = 0 .. 2) — adjacent to b00.
        b01 = _block_labels_with_square(
            r=0, c=1, date_idx=0, class_id=1,
            y0=20, x0=0, side=3,
        )
        _write_block(tmpd, 0, 0, b00, target_dates)
        _write_block(tmpd, 0, 1, b01, target_dates)
        _write_block(tmpd, 1, 0, _empty_block_labels(), target_dates)
        _write_block(tmpd, 1, 1, _empty_block_labels(), target_dates)

        res = _run_aggregator(tmpd)
        assert res.returncode == 0, res.stderr

        df = pd.read_parquet(tmpd / f"{TILE_ID}_tile.parquet")
        assert len(df) == 1, (
            f"expected one merged component across the boundary, "
            f"got {len(df)} rows"
        )
        row = df.iloc[0]
        # bbox in tile coords: y [20, 23), x [CHIP_SIZE-3, CHIP_SIZE+3)
        assert row["bbox_y0"] == 20 and row["bbox_y1"] == 23
        assert row["bbox_x0"] == CHIP_SIZE - 3
        assert row["bbox_x1"] == CHIP_SIZE + 3
        # 6 wide x 3 tall = 18 pixels
        assert row["n_pixels"] == 18
    print("  Parquet: boundary-spanning component merged — OK")


def test_components_parquet_multi_class_multi_date():
    """Two classes on two different dates -> 2 component rows."""
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        target_dates = np.array([738887, 738900], dtype=np.int64)
        lab = _empty_block_labels(n_dates=2)
        lab[0, 5:10, 5:10] = 1   # class 1 on date 0
        lab[1, 30:32, 30:35] = 2  # class 2 on date 1
        _write_block(tmpd, 0, 0, lab, target_dates)
        _write_block(tmpd, 0, 1, _empty_block_labels(), target_dates)
        _write_block(tmpd, 1, 0, _empty_block_labels(), target_dates)
        _write_block(tmpd, 1, 1, _empty_block_labels(), target_dates)

        res = _run_aggregator(tmpd)
        assert res.returncode == 0, res.stderr

        df = pd.read_parquet(tmpd / f"{TILE_ID}_tile.parquet")
        assert len(df) == 2, f"expected 2 components, got {len(df)}"
        # Sort by (date, class) for deterministic assertions.
        df = df.sort_values(["date_ordinal", "class_id"]).reset_index(drop=True)
        assert df.iloc[0]["class_id"] == 1
        assert df.iloc[0]["n_pixels"] == 25
        assert df.iloc[1]["class_id"] == 2
        assert df.iloc[1]["n_pixels"] == 10   # 2 x 5
    print("  Parquet: multi-class multi-date emission — OK")


def test_empty_tile_yields_empty_parquet():
    """A tile with no detections still emits a (zero-row) Parquet with
    the right column set."""
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        _write_synthetic_grid_empty(tmpd, n_rows=2, n_cols=2)

        res = _run_aggregator(tmpd)
        assert res.returncode == 0, res.stderr

        df = pd.read_parquet(tmpd / f"{TILE_ID}_tile.parquet")
        assert len(df) == 0
        # Schema must still carry the expected columns.
        for col in ("tile_id", "date_ordinal", "class_id", "n_pixels",
                    "bbox_y0", "world_centroid_x", "rle_starts"):
            assert col in df.columns, f"missing column {col}"
    print("  Parquet: empty tile -> zero-row Parquet with schema — OK")


def test_missing_block_is_detected():
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        _write_synthetic_grid_empty(tmpd, n_rows=2, n_cols=3)
        # Delete one shard.
        missing_path = tmpd / f"{TILE_ID}_block_001_002.npz"
        missing_path.unlink()

        res = _run_aggregator(tmpd)
        assert res.returncode != 0
        combined = res.stdout + res.stderr
        assert "incomplete" in combined
        assert "(1, 2)" in combined
    print("  Aggregator: missing block surfaces error + lists (r,c) — OK")


def test_inconsistent_target_dates_rejected():
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        _write_block(tmpd, 0, 0, _empty_block_labels(),
                     np.array([100, 200], dtype=np.int64))
        _write_block(tmpd, 0, 1, _empty_block_labels(),
                     np.array([100, 300], dtype=np.int64))

        res = _run_aggregator(tmpd)
        assert res.returncode != 0
        assert "target_dates mismatch" in (res.stdout + res.stderr)
    print("  Aggregator: inconsistent target_dates rejected — OK")


def test_geotiff_per_date_written():
    """Aggregator writes one LZW-compressed GeoTIFF per target date,
    with class 0 tagged as NoData and the correct affine transform."""
    import rasterio
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        target_dates = np.array([738887, 738900], dtype=np.int64)
        # Put a 5x5 class-1 patch in block (0, 0), date 0; rest empty.
        lab = _block_labels_with_square(
            r=0, c=0, date_idx=0, class_id=1,
            y0=10, x0=10, side=5,
        )
        _write_block(tmpd, 0, 0, lab, target_dates)
        for (r, c) in [(0, 1), (1, 0), (1, 1)]:
            _write_block(tmpd, r, c, _empty_block_labels(), target_dates)

        res = _run_aggregator(tmpd)
        assert res.returncode == 0, res.stderr

        # One GeoTIFF per date.
        expected_tifs = [
            tmpd / f"{TILE_ID}_tile_2024-01-02.tif",
            tmpd / f"{TILE_ID}_tile_2024-01-15.tif",
        ]
        for p in expected_tifs:
            assert p.exists(), f"missing {p}"

        # Open the first one and verify everything.
        with rasterio.open(expected_tifs[0]) as src:
            assert src.width == 2 * CHIP_SIZE
            assert src.height == 2 * CHIP_SIZE
            assert src.count == 1
            assert src.dtypes == ("uint8",)
            assert src.nodata == 0
            # Compression tag should report LZW.
            assert src.compression.value == "LZW"
            # Affine transform: (xres, 0, x_origin, 0, -yres, y_origin)
            t = src.transform
            assert abs(t.a - PIXEL_RES) < 1e-9
            assert abs(t.e + PIXEL_RES) < 1e-9   # -yres
            assert abs(t.c - TILE_ORIGIN_X) < 1e-6
            assert abs(t.f - TILE_ORIGIN_Y) < 1e-6
            # The 5x5 class-1 patch must be present at (10, 10).
            data = src.read(1)
            assert data.shape == (2 * CHIP_SIZE, 2 * CHIP_SIZE)
            assert (data[10:15, 10:15] == 1).all()
            # Everywhere else is 0 (nodata).
            mask = np.ones_like(data, dtype=bool)
            mask[10:15, 10:15] = False
            assert (data[mask] == 0).all()
            # Band description should mention the date.
            assert "2024-01-02" in src.descriptions[0]

        # Second GeoTIFF (date 1) is all-zero everywhere.
        with rasterio.open(expected_tifs[1]) as src:
            data = src.read(1)
            assert (data == 0).all()
            assert "2024-01-15" in src.descriptions[0]
    print("  GeoTIFF: per-date file, dims, transform, NoData, descriptions — OK")


def test_geotiff_without_hdf5_path_still_writes():
    """If TILE_HDF5_PATH isn't set in the env, the GeoTIFFs are still
    written but with no CRS tag (only the transform is georef'd)."""
    import rasterio
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        _write_synthetic_grid_empty(tmpd, n_rows=2, n_cols=2)

        env = os.environ.copy()
        env["TILE_ID"] = TILE_ID
        env["OUTPUT_DIR"] = str(tmpd)
        # Make sure TILE_HDF5_PATH isn't lurking in the env from a prior run.
        env.pop("TILE_HDF5_PATH", None)
        res = subprocess.run(
            [sys.executable, str(_HERE / "aggregate_tile.py")],
            env=env, capture_output=True, text=True,
        )
        assert res.returncode == 0, res.stderr

        tif = tmpd / f"{TILE_ID}_tile_2024-01-02.tif"
        assert tif.exists()
        with rasterio.open(tif) as src:
            # CRS should be None / falsy.
            assert not src.crs
            # Transform is still set.
            assert src.transform.a == PIXEL_RES
    print("  GeoTIFF: missing TILE_HDF5_PATH -> CRS-less but still georef'd — OK")


def main():
    print("Running aggregate_tile tests...")
    test_full_grid_stitches_into_dense_npz()
    test_components_parquet_single_square_block()
    test_components_parquet_spanning_block_boundary()
    test_components_parquet_multi_class_multi_date()
    test_empty_tile_yields_empty_parquet()
    test_missing_block_is_detected()
    test_inconsistent_target_dates_rejected()
    test_geotiff_per_date_written()
    test_geotiff_without_hdf5_path_still_writes()
    print("All aggregate_tile tests passed.")


if __name__ == "__main__":
    main()
