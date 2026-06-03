"""Tests for distribute.polygonize.

Run:
    python test_polygonize.py
"""
import sys
from pathlib import Path

import numpy as np
from shapely.geometry import box

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from polygonize import (
    labels_to_polygons, block_transform, close_labels, BACKGROUND_CLASS,
)


PIXEL_RES = 10.0
ORIGIN_X = 500_000.0
ORIGIN_Y = 4_500_000.0


def _empty(h=64, w=64):
    return np.zeros((h, w), dtype=np.uint8)


def test_single_square_polygon_geometry():
    """A 5x5 class-1 square at pixel (10,10) -> one polygon with the
    correct world bbox + area + pixel count."""
    lab = _empty()
    lab[10:15, 10:15] = 1   # rows 10..14, cols 10..14
    patches = labels_to_polygons(
        lab, date_ordinal=738887, classes=(1, 2),
        world_origin_x=ORIGIN_X, world_origin_y=ORIGIN_Y, pixel_res=PIXEL_RES,
    )
    assert len(patches) == 1, f"expected 1 patch, got {len(patches)}"
    p = patches[0]
    assert p.class_id == 1
    assert p.date_ordinal == 738887
    assert p.n_pixels == 25
    assert abs(p.area_m2 - 25 * PIXEL_RES * PIXEL_RES) < 1e-6   # 2500 m^2

    # World bbox: col 10..15 -> x = ORIGIN_X + [100, 150]; row 10..15 ->
    # y = ORIGIN_Y - [100, 150] (south). So the square spans
    # x[500100, 500150], y[4499850, 4499900].
    minx, miny, maxx, maxy = p.geometry.bounds
    assert abs(minx - 500_100.0) < 1e-6
    assert abs(maxx - 500_150.0) < 1e-6
    assert abs(maxy - 4_499_900.0) < 1e-6
    assert abs(miny - 4_499_850.0) < 1e-6

    # Centroid at pixel (12.5, 12.5) -> x=500125, y=4499875.
    assert abs(p.centroid_x - 500_125.0) < 1e-6
    assert abs(p.centroid_y - 4_499_875.0) < 1e-6
    print("  single square -> one polygon, correct geo + area — OK")


def test_two_separate_squares_two_polygons():
    lab = _empty()
    lab[5:8, 5:8] = 1
    lab[40:44, 40:44] = 1   # disjoint from the first
    patches = labels_to_polygons(
        lab, date_ordinal=1, classes=(1,),
        world_origin_x=ORIGIN_X, world_origin_y=ORIGIN_Y, pixel_res=PIXEL_RES,
    )
    assert len(patches) == 2
    sizes = sorted(p.n_pixels for p in patches)
    assert sizes == [9, 16]
    print("  two disjoint squares -> two polygons — OK")


def test_multi_class():
    lab = _empty()
    lab[5:10, 5:10] = 1    # 25 px class 1
    lab[20:22, 20:25] = 2  # 10 px class 2
    patches = labels_to_polygons(
        lab, date_ordinal=1, classes=(1, 2),
        world_origin_x=ORIGIN_X, world_origin_y=ORIGIN_Y, pixel_res=PIXEL_RES,
    )
    assert len(patches) == 2
    by_cls = {p.class_id: p for p in patches}
    assert by_cls[1].n_pixels == 25
    assert by_cls[2].n_pixels == 10
    print("  multi-class -> per-class polygons — OK")


def test_background_never_polygonized():
    """All-background label map -> no polygons, even if class 0 passed."""
    lab = _empty()
    patches = labels_to_polygons(
        lab, date_ordinal=1, classes=(0, 1, 2),
        world_origin_x=ORIGIN_X, world_origin_y=ORIGIN_Y, pixel_res=PIXEL_RES,
    )
    assert patches == []
    print("  all-background -> no polygons (class 0 ignored) — OK")


def test_polygon_with_hole():
    """A class-1 ring (hole in the middle) -> one polygon whose area
    excludes the hole."""
    lab = _empty()
    lab[10:20, 10:20] = 1   # 10x10 solid block = 100 px
    lab[13:17, 13:17] = 0   # punch a 4x4 = 16 px hole back to background
    patches = labels_to_polygons(
        lab, date_ordinal=1, classes=(1,),
        world_origin_x=ORIGIN_X, world_origin_y=ORIGIN_Y, pixel_res=PIXEL_RES,
    )
    assert len(patches) == 1
    p = patches[0]
    # 100 - 16 = 84 pixels of class 1.
    assert p.n_pixels == 84, p.n_pixels
    assert len(p.geometry.interiors) == 1   # one hole
    print("  ring shape -> one polygon with a hole, area excludes hole — OK")


def test_transform_origin_mapping():
    """block_transform maps pixel (0,0) NW corner to (origin_x, origin_y)."""
    t = block_transform(ORIGIN_X, ORIGIN_Y, PIXEL_RES)
    # rasterio Affine: (x, y) = t * (col, row). Pixel (0,0) NW corner:
    x, y = t * (0, 0)
    assert abs(x - ORIGIN_X) < 1e-6
    assert abs(y - ORIGIN_Y) < 1e-6
    # One pixel east+south:
    x1, y1 = t * (1, 1)
    assert abs(x1 - (ORIGIN_X + PIXEL_RES)) < 1e-6
    assert abs(y1 - (ORIGIN_Y - PIXEL_RES)) < 1e-6
    print("  block_transform origin + step mapping — OK")


def test_rejects_3d():
    try:
        labels_to_polygons(
            np.zeros((2, 8, 8), dtype=np.uint8),
            date_ordinal=1, classes=(1,),
            world_origin_x=ORIGIN_X, world_origin_y=ORIGIN_Y, pixel_res=PIXEL_RES,
        )
    except ValueError:
        print("  rejects 3-D labels — OK")
        return
    raise AssertionError("expected ValueError for 3-D labels")


# ============================================================================
# close_labels
# ============================================================================

def test_close_fills_small_gap():
    """A 2-px background gap between two class-1 blocks is bridged by
    closing (dilate-then-erode spans gaps narrower than the disk)."""
    lab = _empty()
    lab[10:20, 10:18] = 1
    lab[10:20, 20:28] = 1   # 2-px gap at cols 18,19
    out = close_labels(lab, classes=(1,), closing_radius=3)
    # Interior rows of the gap bridge (the two corner rows stay open — the
    # disk can't fill the corner concavity, which is expected closing
    # behaviour). Check the interior span.
    assert (out[11:19, 18:20] == 1).all(), "closing should bridge the gap interior"
    # Original input not mutated.
    assert (lab[11:19, 18:20] == 0).all()
    print("  close_labels fills small gap — OK")


def test_close_does_not_clobber_other_class():
    """Closing class 1 must not overwrite class-2 pixels sitting in the
    gap region that closing would otherwise fill."""
    lab = _empty()
    lab[10:20, 10:18] = 1
    lab[10:20, 20:28] = 1
    lab[15, 18] = 2          # a class-2 pixel in the gap
    out = close_labels(lab, classes=(1, 2), closing_radius=3)
    # class 2 pixel preserved (closing class 1 only fills background).
    assert out[15, 18] == 2
    print("  close_labels preserves other-class pixels — OK")


def test_close_ignores_background_class():
    lab = _empty()
    lab[5:8, 5:8] = 1
    out = close_labels(lab, classes=(0, 1), closing_radius=2)
    # class 0 in the list is a no-op; class 1 region intact.
    assert (out[5:8, 5:8] == 1).all()
    print("  close_labels ignores class 0 — OK")


# ============================================================================
# min_area_m2 block-level filter
# ============================================================================

def test_min_area_drops_small_patch():
    """A 3x3 (900 m^2) patch is dropped under a 2500 m^2 floor; a 6x6
    (3600 m^2) patch survives."""
    lab = _empty()
    lab[5:8, 5:8] = 1       # 9 px = 900 m^2  -> dropped
    lab[20:26, 20:26] = 1   # 36 px = 3600 m^2 -> kept
    patches = labels_to_polygons(
        lab, date_ordinal=1, classes=(1,),
        world_origin_x=ORIGIN_X, world_origin_y=ORIGIN_Y, pixel_res=PIXEL_RES,
        min_area_m2=2500.0,
    )
    assert len(patches) == 1
    assert patches[0].n_pixels == 36
    print("  min_area_m2 drops sub-threshold patch — OK")


def test_min_area_zero_keeps_all():
    lab = _empty()
    lab[5:8, 5:8] = 1
    patches = labels_to_polygons(
        lab, date_ordinal=1, classes=(1,),
        world_origin_x=ORIGIN_X, world_origin_y=ORIGIN_Y, pixel_res=PIXEL_RES,
        min_area_m2=0.0,
    )
    assert len(patches) == 1
    print("  min_area_m2=0 keeps all patches — OK")


def main():
    print("Running polygonize tests...")
    test_single_square_polygon_geometry()
    test_two_separate_squares_two_polygons()
    test_multi_class()
    test_background_never_polygonized()
    test_polygon_with_hole()
    test_transform_origin_mapping()
    test_rejects_3d()
    test_close_fills_small_gap()
    test_close_does_not_clobber_other_class()
    test_close_ignores_background_class()
    test_min_area_drops_small_patch()
    test_min_area_zero_keeps_all()
    print("All polygonize tests passed.")


if __name__ == "__main__":
    main()
