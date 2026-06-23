"""End-to-end tests for aggregate_tile.py using synthetic per-block shards.

Builds fake block shards in a tempdir (both the voted .npz AND the per-block
polygon .gpkg, mirroring what predict_block writes), runs the aggregator via
subprocess, and verifies:
  - The auxiliary .npz stitches blocks correctly (pixel-level).
  - The primary tile .gpkg / .parquet carry one polygon per detected patch,
    with boundary-straddling patches dissolved into a single geometry.
  - The per-date GeoTIFFs are written with the right transform / NoData.
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
import geopandas as gpd
import pandas as pd

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))
sys.path.insert(0, str(_HERE))

from postprocess.voted_output import write_voted_block
from polygonize import labels_to_polygons, polygons_to_records


# Small block size keeps the synthetic tile cheap. 64x64 per block.
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
    """Per-block labels with one square of `class_id` on `date_idx`."""
    out = _empty_block_labels(n_dates=n_dates)
    out[date_idx, y0:y0 + side, x0:x0 + side] = class_id
    return out


def _block_origin(r: int, c: int) -> tuple[float, float]:
    return (TILE_ORIGIN_X + c * CHIP_SIZE * PIXEL_RES,
            TILE_ORIGIN_Y - r * CHIP_SIZE * PIXEL_RES)


def _write_block(tmpd: Path, r: int, c: int, labels: np.ndarray,
                 target_dates: np.ndarray, classes: tuple[int, ...] = (1, 2),
                 threshold: int = 2,
                 block_min_area_m2: float = 0.0) -> None:
    """Write BOTH the voted .npz and the per-block polygon .gpkg, exactly
    as predict_block.py does in production.

    `block_min_area_m2` defaults to 0 (keep all patches) so tests with
    small synthetic patches aren't pruned at the block stage; pass a
    value to exercise the block-level floor.
    """
    ox, oy = _block_origin(r, c)
    write_voted_block(
        str(tmpd), TILE_ID, r, c,
        labels=labels,
        target_dates=target_dates,
        classes=classes,
        world_origin_x=ox, world_origin_y=oy,
        pixel_res=PIXEL_RES,
        threshold=threshold,
    )
    # Per-block polygons (LIVE only), in world coords.
    rows: list = []
    for i in range(labels.shape[0]):
        patches = labels_to_polygons(
            labels[i], date_ordinal=int(target_dates[i]),
            classes=classes,
            world_origin_x=ox, world_origin_y=oy, pixel_res=PIXEL_RES,
            min_area_m2=block_min_area_m2,
        )
        rows.extend(polygons_to_records(patches, TILE_ID))
    cols = ["tile_id", "date_ordinal", "date_iso", "class_id",
            "n_pixels", "area_m2", "centroid_x", "centroid_y", "geometry"]
    gdf = gpd.GeoDataFrame(rows, geometry="geometry") if rows else \
        gpd.GeoDataFrame(columns=cols, geometry="geometry")
    if rows:
        gdf = gdf[cols]
    gpkg = tmpd / f"{TILE_ID}_block_{r:03d}_{c:03d}.gpkg"
    gdf.to_file(gpkg, layer="detections", driver="GPKG")


def _write_synthetic_grid_empty(tmpd: Path, n_rows: int, n_cols: int,
                                n_dates: int = 2) -> np.ndarray:
    target_dates = np.array([738887, 738900][:n_dates], dtype=np.int64)
    for r in range(n_rows):
        for c in range(n_cols):
            _write_block(tmpd, r, c, _empty_block_labels(n_dates=n_dates),
                         target_dates)
    return target_dates


def _run_aggregator(tmpd: Path,
                    min_tile_patch_m2: float = 0.0,
                    extra_env: dict | None = None) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["TILE_ID"] = TILE_ID
    env["OUTPUT_DIR"] = str(tmpd)
    env.pop("TILE_HDF5_PATH", None)   # no real HDF5 in tests
    # Default the master floor to 0 so the small synthetic patches most
    # tests use aren't pruned; the dedicated filter test overrides it.
    env["MIN_TILE_PATCH_M2"] = str(min_tile_patch_m2)
    if extra_env:
        env.update({k: str(v) for k, v in extra_env.items()})
    return subprocess.run(
        [sys.executable, str(_HERE / "aggregate_tile.py")],
        env=env, capture_output=True, text=True,
    )


def _read_tile_vector(tmpd: Path) -> gpd.GeoDataFrame:
    return gpd.read_file(str(tmpd / f"{TILE_ID}_tile.gpkg"), layer="detections")


# ============================================================================
# Tests
# ============================================================================

def test_full_grid_stitches_into_dense_npz():
    """The auxiliary .npz stitches blocks correctly (pixel-level)."""
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        n_rows, n_cols, n_dates = 2, 3, 2
        target_dates = np.array([738887, 738900], dtype=np.int64)
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


def test_vector_single_square_block():
    """One 5x5 class-1 square at LIVE (10,10) in block (0,0). The tile
    vector must have exactly one polygon with the expected geometry."""
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        target_dates = np.array([738887, 738900], dtype=np.int64)
        for r in range(2):
            for c in range(2):
                if r == 0 and c == 0:
                    lab = _block_labels_with_square(
                        0, 0, date_idx=0, class_id=1, y0=10, x0=10, side=5)
                else:
                    lab = _empty_block_labels()
                _write_block(tmpd, r, c, lab, target_dates)

        res = _run_aggregator(tmpd)
        assert res.returncode == 0, res.stderr

        gdf = _read_tile_vector(tmpd)
        assert len(gdf) == 1, f"expected 1 polygon, got {len(gdf)}"
        row = gdf.iloc[0]
        assert row["tile_id"] == TILE_ID
        assert row["date_ordinal"] == 738887
        assert row["date_iso"] == "2024-01-02"
        assert row["class_id"] == 1
        assert row["n_pixels"] == 25
        assert abs(row["area_m2"] - 25 * PIXEL_RES * PIXEL_RES) < 1e-6

        # World bbox: cols 10..15 -> x[500100,500150]; rows 10..15 ->
        # y[4499850,4499900].
        minx, miny, maxx, maxy = row["geometry"].bounds
        assert abs(minx - 500_100.0) < 1e-6
        assert abs(maxx - 500_150.0) < 1e-6
        assert abs(maxy - 4_499_900.0) < 1e-6
        assert abs(miny - 4_499_850.0) < 1e-6
        # Centroid at pixel (12.5,12.5).
        assert abs(row["centroid_x"] - 500_125.0) < 1e-6
        assert abs(row["centroid_y"] - 4_499_875.0) < 1e-6

        # GeoParquet mirror exists and matches row count.
        gdf_pq = gpd.read_parquet(tmpd / f"{TILE_ID}_tile.parquet")
        assert len(gdf_pq) == 1
    print("  vector: single polygon geometry + GeoParquet mirror — OK")


def test_vector_spanning_block_boundary_dissolved():
    """A class-1 region crossing the boundary between block (0,0) and
    (0,1) must dissolve into ONE polygon in the tile vector — the two
    edge-adjacent block polygons are welded by unary_union."""
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        target_dates = np.array([738887, 738900], dtype=np.int64)
        # Block (0,0): 3x3 at the east edge (cols CHIP_SIZE-3..CHIP_SIZE-1).
        b00 = _block_labels_with_square(
            0, 0, date_idx=0, class_id=1, y0=20, x0=CHIP_SIZE - 3, side=3)
        # Block (0,1): 3x3 at the west edge (cols 0..2) — geographically
        # adjacent to b00 across the seam.
        b01 = _block_labels_with_square(
            0, 1, date_idx=0, class_id=1, y0=20, x0=0, side=3)
        _write_block(tmpd, 0, 0, b00, target_dates)
        _write_block(tmpd, 0, 1, b01, target_dates)
        _write_block(tmpd, 1, 0, _empty_block_labels(), target_dates)
        _write_block(tmpd, 1, 1, _empty_block_labels(), target_dates)

        res = _run_aggregator(tmpd)
        assert res.returncode == 0, res.stderr

        gdf = _read_tile_vector(tmpd)
        assert len(gdf) == 1, (
            f"expected one dissolved polygon across the boundary, "
            f"got {len(gdf)}"
        )
        row = gdf.iloc[0]
        # 6 wide x 3 tall = 18 px = 1800 m^2.
        assert row["n_pixels"] == 18
        assert abs(row["area_m2"] - 1800.0) < 1e-6
        # World bbox spans the seam: east edge of block 0 is at
        # x = TILE_ORIGIN_X + CHIP_SIZE*PIXEL_RES = 500000 + 640 = 500640.
        # The merged patch x range: [500640 - 3*10, 500640 + 3*10] = [500610, 500670].
        minx, miny, maxx, maxy = row["geometry"].bounds
        assert abs(minx - 500_610.0) < 1e-6
        assert abs(maxx - 500_670.0) < 1e-6
        # Single connected polygon (no multipart).
        assert row["geometry"].geom_type == "Polygon"
    print("  vector: boundary-spanning patch dissolved to one polygon — OK")


def test_vector_multi_class_multi_date():
    """Two classes on two different dates -> 2 polygons."""
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        target_dates = np.array([738887, 738900], dtype=np.int64)
        lab = _empty_block_labels(n_dates=2)
        lab[0, 5:10, 5:10] = 1     # class 1, date 0, 25 px
        lab[1, 30:32, 30:35] = 2   # class 2, date 1, 10 px
        _write_block(tmpd, 0, 0, lab, target_dates)
        _write_block(tmpd, 0, 1, _empty_block_labels(), target_dates)
        _write_block(tmpd, 1, 0, _empty_block_labels(), target_dates)
        _write_block(tmpd, 1, 1, _empty_block_labels(), target_dates)

        res = _run_aggregator(tmpd)
        assert res.returncode == 0, res.stderr

        gdf = _read_tile_vector(tmpd)
        assert len(gdf) == 2, f"expected 2 polygons, got {len(gdf)}"
        gdf = gdf.sort_values(["date_ordinal", "class_id"]).reset_index(drop=True)
        assert gdf.iloc[0]["class_id"] == 1 and gdf.iloc[0]["n_pixels"] == 25
        assert gdf.iloc[1]["class_id"] == 2 and gdf.iloc[1]["n_pixels"] == 10
    print("  vector: multi-class multi-date emission — OK")


def test_empty_tile_yields_empty_vector():
    """A tile with no detections still emits an empty tile .gpkg/.parquet
    with the right schema."""
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        _write_synthetic_grid_empty(tmpd, n_rows=2, n_cols=2)

        res = _run_aggregator(tmpd)
        assert res.returncode == 0, res.stderr

        gdf = _read_tile_vector(tmpd)
        assert len(gdf) == 0
        for col in ("tile_id", "date_ordinal", "class_id", "n_pixels",
                    "area_m2", "centroid_x", "geometry"):
            assert col in gdf.columns, f"missing column {col}"
    print("  vector: empty tile -> zero-row .gpkg with schema — OK")


def test_missing_block_is_detected():
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        _write_synthetic_grid_empty(tmpd, n_rows=2, n_cols=3)
        # Delete one .npz shard (the npz-based completeness check fires first).
        (tmpd / f"{TILE_ID}_block_001_002.npz").unlink()

        res = _run_aggregator(tmpd)
        assert res.returncode != 0
        combined = res.stdout + res.stderr
        assert "incomplete" in combined
        assert "(1, 2)" in combined
    print("  aggregator: missing block surfaces error + lists (r,c) — OK")


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
    print("  aggregator: inconsistent target_dates rejected — OK")


def test_master_size_filter_prunes_small_patch():
    """The master floor drops merged patches below MIN_TILE_PATCH_M2.

    Two class-1 patches on date 0: a 4x4 (1600 m^2) and a 8x8 (6400 m^2).
    Block .gpkg keeps both (block floor 0 here). With master floor 5000,
    only the 6400 m^2 patch survives the tile vector."""
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        target_dates = np.array([738887, 738900], dtype=np.int64)
        lab = _empty_block_labels(n_dates=2)
        lab[0, 5:9, 5:9] = 1       # 16 px  = 1600 m^2 -> pruned by master
        lab[0, 20:28, 20:28] = 1   # 64 px  = 6400 m^2 -> kept
        _write_block(tmpd, 0, 0, lab, target_dates)
        for (r, c) in [(0, 1), (1, 0), (1, 1)]:
            _write_block(tmpd, r, c, _empty_block_labels(), target_dates)

        # Without the master filter (floor 0): both patches survive.
        res0 = _run_aggregator(tmpd, min_tile_patch_m2=0.0)
        assert res0.returncode == 0, res0.stderr
        assert len(_read_tile_vector(tmpd)) == 2

        # With master floor 5000: only the 6400 m^2 patch survives.
        res = _run_aggregator(tmpd, min_tile_patch_m2=5000.0)
        assert res.returncode == 0, res.stderr
        gdf = _read_tile_vector(tmpd)
        assert len(gdf) == 1, f"expected 1 patch after master filter, got {len(gdf)}"
        assert gdf.iloc[0]["n_pixels"] == 64
        assert gdf.iloc[0]["area_m2"] >= 5000.0
    print("  master size filter prunes sub-5000 m^2 patch — OK")


def test_master_filter_measures_boundary_patch_at_full_size():
    """A patch split across a block seam, each half < 5000 m^2 but the
    merged total > 5000 m^2, must SURVIVE the master floor (it's measured
    after the cross-block merge, not per block)."""
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        target_dates = np.array([738887, 738900], dtype=np.int64)
        # Block (0,0): 8 tall x 6 wide at the east edge = 48 px = 4800 m^2.
        b00 = _empty_block_labels()
        b00[0, 20:28, CHIP_SIZE - 6:CHIP_SIZE] = 1
        # Block (0,1): 8 tall x 6 wide at the west edge = 48 px = 4800 m^2.
        b01 = _empty_block_labels()
        b01[0, 20:28, 0:6] = 1
        # Each half 4800 < 5000, merged 9600 > 5000.
        _write_block(tmpd, 0, 0, b00, target_dates)
        _write_block(tmpd, 0, 1, b01, target_dates)
        _write_block(tmpd, 1, 0, _empty_block_labels(), target_dates)
        _write_block(tmpd, 1, 1, _empty_block_labels(), target_dates)

        res = _run_aggregator(tmpd, min_tile_patch_m2=5000.0)
        assert res.returncode == 0, res.stderr
        gdf = _read_tile_vector(tmpd)
        assert len(gdf) == 1, (
            f"boundary patch should survive (merged 9600 m^2 > 5000), "
            f"got {len(gdf)}"
        )
        assert gdf.iloc[0]["n_pixels"] == 96   # 48 + 48
    print("  master filter measures boundary patch post-merge — OK")


def test_geotiff_per_date_written():
    """Aggregator still writes one LZW GeoTIFF per date, class 0 = NoData,
    correct affine transform."""
    import rasterio
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        target_dates = np.array([738887, 738900], dtype=np.int64)
        lab = _block_labels_with_square(
            0, 0, date_idx=0, class_id=1, y0=10, x0=10, side=5)
        _write_block(tmpd, 0, 0, lab, target_dates)
        for (r, c) in [(0, 1), (1, 0), (1, 1)]:
            _write_block(tmpd, r, c, _empty_block_labels(), target_dates)

        res = _run_aggregator(tmpd)
        assert res.returncode == 0, res.stderr

        expected_tifs = [
            tmpd / f"{TILE_ID}_tile_2024-01-02.tif",
            tmpd / f"{TILE_ID}_tile_2024-01-15.tif",
        ]
        for p in expected_tifs:
            assert p.exists(), f"missing {p}"

        with rasterio.open(expected_tifs[0]) as src:
            assert src.width == 2 * CHIP_SIZE
            assert src.height == 2 * CHIP_SIZE
            assert src.count == 1
            assert src.dtypes == ("uint8",)
            assert src.nodata == 0
            assert src.compression.value == "LZW"
            t = src.transform
            assert abs(t.a - PIXEL_RES) < 1e-9
            assert abs(t.e + PIXEL_RES) < 1e-9
            assert abs(t.c - TILE_ORIGIN_X) < 1e-6
            assert abs(t.f - TILE_ORIGIN_Y) < 1e-6
            data = src.read(1)
            assert (data[10:15, 10:15] == 1).all()
            mask = np.ones_like(data, dtype=bool)
            mask[10:15, 10:15] = False
            assert (data[mask] == 0).all()
            assert "2024-01-02" in src.descriptions[0]

        with rasterio.open(expected_tifs[1]) as src:
            assert (src.read(1) == 0).all()
            assert "2024-01-15" in src.descriptions[0]
    print("  GeoTIFF: per-date file, dims, transform, NoData, descriptions — OK")


def test_block_grid_outline_written():
    """The debug block-grid layer has one rectangle per block, correctly
    placed and attributed, and does NOT collide with the per-block glob."""
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        n_rows, n_cols, n_dates = 2, 3, 1
        target_dates = np.array([738887], dtype=np.int64)
        for r in range(n_rows):
            for c in range(n_cols):
                _write_block(tmpd, r, c, _empty_block_labels(n_dates=n_dates),
                             target_dates)

        # Run twice: the second run proves the grid file isn't swept into the
        # per-block glob (the bug the *_blockgrid name avoids).
        assert _run_aggregator(tmpd).returncode == 0
        res = _run_aggregator(tmpd)
        assert res.returncode == 0, res.stderr

        grid_path = tmpd / f"{TILE_ID}_blockgrid.gpkg"
        assert grid_path.exists(), "block-grid .gpkg not written"
        grid = gpd.read_file(str(grid_path), layer="block_grid")

        # One rectangle per block.
        assert len(grid) == n_rows * n_cols
        assert set(grid["block_label"]) == {
            f"{r:03d}_{c:03d}" for r in range(n_rows) for c in range(n_cols)
        }
        # All origins on-grid (fixtures use exact grid origins).
        assert bool(grid["origin_ok"].all())
        assert (grid["origin_drift_m"] == 0).all()

        # Geometry of block (1,2) matches its LIVE extent in world coords.
        row = grid[grid["block_label"] == "001_002"].iloc[0]
        ox, oy = _block_origin(1, 2)
        side = CHIP_SIZE * PIXEL_RES
        minx, miny, maxx, maxy = row.geometry.bounds
        assert abs(minx - ox) < 1e-6 and abs(maxy - oy) < 1e-6
        assert abs(maxx - (ox + side)) < 1e-6
        assert abs(miny - (oy - side)) < 1e-6

        # The grid file must NOT be picked up as a per-block detections file.
        block_glob = sorted(tmpd.glob(f"{TILE_ID}_block_*.gpkg"))
        assert grid_path not in block_glob
    print("  block-grid: one rectangle per block, placed + attributed — OK")


def test_subregion_crops_canvas_and_georef():
    """Processing only a sub-rectangle of blocks crops the stitched canvas to
    that sub-region and georeferences it at the NW-most processed block."""
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        target_dates = np.array([738887], dtype=np.int64)
        # Only write the middle 2x2 (rows 1-2, cols 1-2) of a notional 4x4.
        # Put a detection in block (1,1) so we can check placement.
        for r in (1, 2):
            for c in (1, 2):
                if (r, c) == (1, 1):
                    lab = _block_labels_with_square(r, c, date_idx=0,
                                                    class_id=1, y0=10, x0=10,
                                                    side=8, n_dates=1)
                else:
                    lab = _empty_block_labels(n_dates=1)
                _write_block(tmpd, r, c, lab, target_dates)

        res = _run_aggregator(tmpd, extra_env={
            "PROCESS_ROW_LO": 1, "PROCESS_ROW_HI": 2,
            "PROCESS_COL_LO": 1, "PROCESS_COL_HI": 2,
        })
        assert res.returncode == 0, res.stderr

        # Canvas cropped to 2x2 blocks, NOT the full 0..2 = 3x3.
        with np.load(tmpd / f"{TILE_ID}_tile.npz") as npz:
            labels = npz["labels"]
            assert labels.shape == (1, 2 * CHIP_SIZE, 2 * CHIP_SIZE), labels.shape
            assert int(npz["n_block_rows"]) == 2
            assert int(npz["n_block_cols"]) == 2
            # npz world origin = block (1,1)'s NW corner.
            ox, oy = _block_origin(1, 1)
            assert float(npz["world_origin_x"]) == ox
            assert float(npz["world_origin_y"]) == oy
            # The detection (in block (1,1) = canvas cell (0,0)) lands in the
            # top-left block of the cropped canvas.
            assert (labels[0, 10:18, 10:18] == 1).all()

        # GeoTIFF NW corner matches block (1,1), not (0,0).
        import rasterio
        with rasterio.open(tmpd / f"{TILE_ID}_tile_2024-01-02.tif") as src:
            assert src.transform.c == ox
            assert src.transform.f == oy
            assert src.width == 2 * CHIP_SIZE and src.height == 2 * CHIP_SIZE
    print("  sub-region: canvas cropped + georeferenced at NW block — OK")


def test_subregion_missing_block_rejected():
    """A selected sub-region with a missing block fails loudly."""
    with tempfile.TemporaryDirectory() as tmpd:
        tmpd = Path(tmpd)
        target_dates = np.array([738887], dtype=np.int64)
        # Selection says rows 1-2 cols 1-2, but only write 3 of the 4.
        for (r, c) in [(1, 1), (1, 2), (2, 1)]:
            _write_block(tmpd, r, c, _empty_block_labels(n_dates=1),
                         target_dates)
        res = _run_aggregator(tmpd, extra_env={
            "PROCESS_ROW_LO": 1, "PROCESS_ROW_HI": 2,
            "PROCESS_COL_LO": 1, "PROCESS_COL_HI": 2,
        })
        assert res.returncode != 0
        assert "missing" in res.stderr.lower() or "incomplete" in res.stderr.lower()
    print("  sub-region: missing selected block rejected — OK")


def main():
    print("Running aggregate_tile tests...")
    test_full_grid_stitches_into_dense_npz()
    test_vector_single_square_block()
    test_vector_spanning_block_boundary_dissolved()
    test_vector_multi_class_multi_date()
    test_empty_tile_yields_empty_vector()
    test_missing_block_is_detected()
    test_inconsistent_target_dates_rejected()
    test_master_size_filter_prunes_small_patch()
    test_master_filter_measures_boundary_patch_at_full_size()
    test_geotiff_per_date_written()
    test_block_grid_outline_written()
    test_subregion_crops_canvas_and_georef()
    test_subregion_missing_block_rejected()
    print("All aggregate_tile tests passed.")


if __name__ == "__main__":
    main()
