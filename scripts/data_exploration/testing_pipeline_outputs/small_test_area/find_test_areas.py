"""Find single-block (10240 m) test areas containing co-located fire + cut.

Purpose
-------
Pick small areas to build a tiny test HDF5 from, so pipeline changes can be
exercised without running a whole S2 tile. A useful test area must contain
BOTH a fire and a cut whose dates are close (so a single before/after model
run sees both change classes), and must be the size of one pipeline block's
LIVE area (4x4 chips = 1024 px = 10240 m at 10 m/px) so it maps 1:1 onto the
pipeline's block geometry. The ghost border the pipeline reads around a block
is context only — we qualify on the LIVE area alone.

Inputs (this directory)
------------------------
  Data_ref_2023_icnf.gpkg   — fires  (Chg_type == 'fogo')
  Data_ref_2023_nvg_v2.gpkg — cuts   (Chg_type == 'corte')
Both EPSG:32629, with an event date in the `Data0` column (ISO string).

Method
------
1. Lay a non-overlapping CELL_M (10240 m) grid over the combined extent of
   both layers, snapped to a 10 m lattice so cell edges fall on pixel
   boundaries (the test HDF5 you build from a chosen cell uses the cell's NW
   corner as its world origin, so alignment is by construction).
2. Spatially join fires and cuts to cells (a feature counts toward a cell if
   its geometry intersects the cell).
3. Keep a cell if it holds >= 1 fire and >= 1 cut whose Data0 dates are within
   MAX_DAYS of each other (the closest qualifying fire/cut pair).
4. Write qualifying cells to a .gpkg with attributes describing why each
   qualified (counts, closest date gap, the pair's dates).

Usage
-----
    python find_test_areas.py
    python find_test_areas.py --cell-m 10240 --max-days 60 \
        --out test_areas.gpkg
"""
from __future__ import annotations

import argparse
import math
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import box

_HERE = Path(__file__).resolve().parent

# ── Defaults ────────────────────────────────────────────────────────────────
FIRES_GPKG = _HERE / "Data_ref_2023_icnf.gpkg"   # Chg_type == 'fogo'
CUTS_GPKG = _HERE / "Data_ref_2023_nvg_v2.gpkg"  # Chg_type == 'corte'
DATE_COL = "Data0"                                # event date (ISO string)
# One pipeline block's LIVE area: 4 chips x 256 px x 10 m/px = 10240 m.
DEFAULT_CELL_M = 10240.0
# 10 m pixel lattice — cell edges snap to this so a test HDF5's pixel grid
# lines up with the cell.
PIXEL_RES = 10.0
# "within 2 months" — calendar months vary, so use a fixed day count.
DEFAULT_MAX_DAYS = 62
DEFAULT_OUT = _HERE / "test_areas.gpkg"
TARGET_CRS = "EPSG:32629"


def _load_layer(path: Path, expect_chg: str) -> gpd.GeoDataFrame:
    """Load a reference layer, parse Data0, drop undated rows, reproject."""
    gdf = gpd.read_file(path)
    if gdf.crs is None or gdf.crs.to_epsg() != 32629:
        gdf = gdf.to_crs(TARGET_CRS)
    types = set(gdf["Chg_type"].unique())
    if types != {expect_chg}:
        print(f"  note: {path.name} Chg_type = {types} (expected "
              f"{{'{expect_chg}'}})")
    gdf["event_date"] = pd.to_datetime(gdf[DATE_COL], errors="coerce")
    n_bad = int(gdf["event_date"].isna().sum())
    if n_bad:
        print(f"  dropping {n_bad} undated rows from {path.name}")
        gdf = gdf[gdf["event_date"].notna()].copy()
    return gdf.reset_index(drop=True)


def _build_grid(bounds, cell_m: float) -> gpd.GeoDataFrame:
    """Non-overlapping cell_m grid over `bounds`, snapped to the 10 m lattice.

    Snapping the origin down to a PIXEL_RES multiple keeps every cell edge on
    a pixel boundary, so a test HDF5 built from a cell's NW corner has its
    pixel grid aligned with the cell (no half-pixel offset).
    """
    minx, miny, maxx, maxy = bounds
    # Snap the SW origin down to the pixel lattice.
    x0 = math.floor(minx / PIXEL_RES) * PIXEL_RES
    y0 = math.floor(miny / PIXEL_RES) * PIXEL_RES
    nx = int(math.ceil((maxx - x0) / cell_m))
    ny = int(math.ceil((maxy - y0) / cell_m))

    cells = []
    for j in range(ny):
        for i in range(nx):
            cx0 = x0 + i * cell_m
            cy0 = y0 + j * cell_m
            cells.append({
                "cell_col": i,
                "cell_row": j,
                # NW corner = the test HDF5's intended world origin.
                "origin_x": cx0,
                "origin_y": cy0 + cell_m,   # UTM north -> +y, NW is the top
                "geometry": box(cx0, cy0, cx0 + cell_m, cy0 + cell_m),
            })
    return gpd.GeoDataFrame(cells, crs=TARGET_CRS)


def _events_per_cell(grid: gpd.GeoDataFrame,
                     events: gpd.GeoDataFrame) -> dict[int, np.ndarray]:
    """Map cell index -> array of event_date (datetime64) intersecting it.

    A feature counts toward a cell if its geometry intersects the cell, so a
    change straddling a cell edge contributes to both adjacent cells (matches
    how the pipeline would see it via the ghost border).
    """
    joined = gpd.sjoin(
        events[["event_date", "geometry"]],
        grid[["geometry"]],
        how="inner", predicate="intersects",
    )
    out: dict[int, np.ndarray] = {}
    for cell_idx, grp in joined.groupby("index_right"):
        out[int(cell_idx)] = grp["event_date"].values
    return out


def _closest_gap_days(fire_dates: np.ndarray,
                      cut_dates: np.ndarray):
    """Smallest |fire - cut| in days, plus the pair achieving it.

    Returns (gap_days, fire_date, cut_date) or None if either side is empty.
    """
    if len(fire_dates) == 0 or len(cut_dates) == 0:
        return None
    f = np.sort(fire_dates)
    best = None
    for cd in cut_dates:
        # Nearest fire date to this cut (sorted -> searchsorted neighbours).
        pos = np.searchsorted(f, cd)
        for p in (pos - 1, pos):
            if 0 <= p < len(f):
                gap = abs((f[p] - cd) / np.timedelta64(1, "D"))
                if best is None or gap < best[0]:
                    best = (gap, f[p], cd)
    return best


def find_test_areas(fires: gpd.GeoDataFrame,
                    cuts: gpd.GeoDataFrame,
                    cell_m: float,
                    max_days: int) -> gpd.GeoDataFrame:
    """Return qualifying cells (>=1 fire + >=1 cut within max_days)."""
    bounds = (
        min(fires.total_bounds[0], cuts.total_bounds[0]),
        min(fires.total_bounds[1], cuts.total_bounds[1]),
        max(fires.total_bounds[2], cuts.total_bounds[2]),
        max(fires.total_bounds[3], cuts.total_bounds[3]),
    )
    grid = _build_grid(bounds, cell_m)
    print(f"  grid: {len(grid)} cells of {cell_m:.0f} m over "
          f"{bounds[2] - bounds[0]:.0f} x {bounds[3] - bounds[1]:.0f} m")

    fire_by_cell = _events_per_cell(grid, fires)
    cut_by_cell = _events_per_cell(grid, cuts)

    # Only cells holding both classes can possibly qualify.
    candidate_cells = set(fire_by_cell) & set(cut_by_cell)
    print(f"  {len(candidate_cells)} cells contain both a fire and a cut")

    rows = []
    for cell_idx in sorted(candidate_cells):
        fds = fire_by_cell[cell_idx]
        cds = cut_by_cell[cell_idx]
        best = _closest_gap_days(fds, cds)
        if best is None:
            continue
        gap_days, f_date, c_date = best
        if gap_days > max_days:
            continue
        cell = grid.iloc[cell_idx]
        rows.append({
            "cell_row": int(cell["cell_row"]),
            "cell_col": int(cell["cell_col"]),
            "origin_x": float(cell["origin_x"]),
            "origin_y": float(cell["origin_y"]),
            "n_fires": int(len(fds)),
            "n_cuts": int(len(cds)),
            "closest_gap_days": int(round(gap_days)),
            "fire_date": pd.Timestamp(f_date).date().isoformat(),
            "cut_date": pd.Timestamp(c_date).date().isoformat(),
            "geometry": cell["geometry"],
        })

    result = gpd.GeoDataFrame(rows, crs=TARGET_CRS)
    if len(result):
        # Tightest temporal match first — best test scenes at the top.
        result = result.sort_values(
            ["closest_gap_days", "n_fires", "n_cuts"],
            ascending=[True, False, False],
        ).reset_index(drop=True)
    return result


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--fires", type=Path, default=FIRES_GPKG)
    ap.add_argument("--cuts", type=Path, default=CUTS_GPKG)
    ap.add_argument("--cell-m", type=float, default=DEFAULT_CELL_M,
                    help="cell size in metres (default 10240 = 1 block LIVE)")
    ap.add_argument("--max-days", type=int, default=DEFAULT_MAX_DAYS,
                    help="max days between a fire and cut (default 62 ~ 2mo)")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()

    print(f"Loading fires: {args.fires.name}")
    fires = _load_layer(args.fires, "fogo")
    print(f"Loading cuts:  {args.cuts.name}")
    cuts = _load_layer(args.cuts, "corte")
    print(f"  {len(fires)} fires, {len(cuts)} cuts (dated)")

    print(f"Finding test areas (cell={args.cell_m:.0f} m, "
          f"max_days={args.max_days})...")
    result = find_test_areas(fires, cuts, args.cell_m, args.max_days)

    if not len(result):
        print("No qualifying cells found.")
        return

    result.to_file(args.out, driver="GPKG")
    print(f"\nWrote {len(result)} qualifying cells -> {args.out}")
    print("\nTop candidates (tightest fire/cut date gap first):")
    cols = ["cell_row", "cell_col", "origin_x", "origin_y",
            "n_fires", "n_cuts", "closest_gap_days", "fire_date", "cut_date"]
    with pd.option_context("display.max_rows", 20, "display.width", 200):
        print(result[cols].head(20).to_string(index=False))


if __name__ == "__main__":
    main()
