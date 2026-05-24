"""Synthetic-data tests for create_before_after_composites.

Run:
    python test_composite.py
"""
import sys
from datetime import date

import numpy as np

from composite import (
    create_before_after_composites,
    cascading_select_flat,
    NODATA_U8,
)


def _hand_crafted_block():
    """Construct a tiny (N_TS=5, 10, P=4) block with known per-pixel values.

    Layout:
      timesteps:  ordinal dates 100, 110, 120, 130, 140
      pixels:     4 pixels labeled 0..3
      bands:      all 10 bands share the same value at each (t, pixel)
                  (for easy hand-verification)

    Per-pixel value matrix (255 = nodata):
        pixel  t=100 t=110 t=120 t=130 t=140
           0     11    22    33    44    55
           1    255    22   255    44   255
           2    255   255   255   255   255
           3     11   255   255   255    55
    """
    N_TS, P = 5, 4
    block = np.full((N_TS, 10, P), 0, dtype=np.uint8)
    ts = np.array([100, 110, 120, 130, 140], dtype=np.int64)

    values = np.array([
        [ 11,  22,  33,  44,  55],   # pixel 0
        [255,  22, 255,  44, 255],   # pixel 1
        [255, 255, 255, 255, 255],   # pixel 2
        [ 11, 255, 255, 255,  55],   # pixel 3
    ], dtype=np.uint8)

    for t in range(N_TS):
        for p in range(P):
            block[t, :, p] = values[p, t]

    return block, ts


def test_cascading_select_flat_descending_picks_most_recent():
    """If timesteps are sorted descending and we want 'most recent valid',
    the first non-nodata along axis 0 is the answer."""
    block, ts = _hand_crafted_block()
    # Pre-side, target=125: keep ts < 125 = [100, 110, 120], sort descending
    keep = np.array([2, 1, 0])  # ts indices for [120, 110, 100]
    sel, tstamps, valid = cascading_select_flat(block[keep], ts[keep])

    # pixel 0: most recent valid before 125 is t=120, value=33
    assert sel[0, 0] == 33, f"pixel 0 expected 33, got {sel[0, 0]}"
    assert tstamps[0] == 120
    # pixel 1: t=120 is nodata, t=110 valid (value 22)
    assert sel[0, 1] == 22
    assert tstamps[1] == 110
    # pixel 2: all nodata
    assert sel[0, 2] == NODATA_U8
    assert tstamps[2] == NODATA_U8
    assert not valid[2]
    # pixel 3: t=100 only valid one (value 11)
    assert sel[0, 3] == 11
    assert tstamps[3] == 100
    print("  cascading_select_flat (descending, most-recent) — OK")


def test_cascading_select_flat_ascending_picks_oldest():
    """If timesteps are sorted ascending and we want 'oldest valid',
    the first non-nodata along axis 0 is the answer."""
    block, ts = _hand_crafted_block()
    # Post-side, target=125: keep ts > 125 = [130, 140], sort ascending
    keep = np.array([3, 4])
    sel, tstamps, valid = cascading_select_flat(block[keep], ts[keep])

    # pixel 0: oldest valid after 125 is t=130, value=44
    assert sel[0, 0] == 44
    assert tstamps[0] == 130
    # pixel 1: t=130 valid (44)
    assert sel[0, 1] == 44
    assert tstamps[1] == 130
    # pixel 2: all nodata
    assert sel[0, 2] == NODATA_U8
    assert not valid[2]
    # pixel 3: t=140 only valid (55)
    assert sel[0, 3] == 55
    assert tstamps[3] == 140
    print("  cascading_select_flat (ascending, oldest) — OK")


def test_create_before_after_one_target_date_in_range():
    block, ts = _hand_crafted_block()
    target_dates = np.array([125], dtype=np.int64)
    composites, valid = create_before_after_composites(
        block, ts, target_dates, verbose=False)

    assert composites.shape == (2, 1, 10, 4), composites.shape
    assert valid.tolist() == [True]

    before = composites[0, 0]  # (10, 4)
    after  = composites[1, 0]

    # Before composite for target=125: same as descending cascade test
    assert before[0, 0] == 33, before[0, 0]
    assert before[0, 1] == 22
    assert before[0, 2] == NODATA_U8
    assert before[0, 3] == 11

    # After composite for target=125: same as ascending cascade test
    assert after[0, 0] == 44
    assert after[0, 1] == 44
    assert after[0, 2] == NODATA_U8
    assert after[0, 3] == 55

    print("  create_before_after (single date, in range) — OK")


def test_create_before_after_skip_outside_range(capsys=None):
    block, ts = _hand_crafted_block()
    # 50 is below ts.min(); 200 is above ts.max()
    target_dates = np.array([50, 200, 125], dtype=np.int64)
    composites, valid = create_before_after_composites(
        block, ts, target_dates, verbose=False)

    assert valid.tolist() == [False, False, True]
    # Skipped slots should be entirely nodata.
    assert (composites[:, 0, :, :] == NODATA_U8).all()
    assert (composites[:, 1, :, :] == NODATA_U8).all()
    # Valid slot retains real values for pixel 0.
    assert composites[0, 2, 0, 0] == 33
    print("  create_before_after (skip outside range) — OK")


def test_create_before_after_skip_no_pre_or_post():
    block, ts = _hand_crafted_block()
    # target=100 equals ts.min() — strict `< 100` has no pre timesteps.
    # target=140 equals ts.max() — strict `> 140` has no post timesteps.
    target_dates = np.array([100, 140], dtype=np.int64)
    composites, valid = create_before_after_composites(
        block, ts, target_dates, verbose=False)
    assert valid.tolist() == [False, False]
    print("  create_before_after (skip no pre/post) — OK")


def test_create_before_after_warning_text(capfd=None):
    """Sanity-check the user-facing warning strings."""
    import io
    import contextlib

    block, ts = _hand_crafted_block()
    target_dates = np.array([50, 100], dtype=np.int64)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        create_before_after_composites(block, ts, target_dates, verbose=True)
    output = buf.getvalue()
    assert "outside the data range" in output, output
    assert "no valid pre-date timesteps" in output, output
    print("  create_before_after (warning text) — OK")


def main():
    print("Running composite tests...")
    test_cascading_select_flat_descending_picks_most_recent()
    test_cascading_select_flat_ascending_picks_oldest()
    test_create_before_after_one_target_date_in_range()
    test_create_before_after_skip_outside_range()
    test_create_before_after_skip_no_pre_or_post()
    test_create_before_after_warning_text()
    print("All composite tests passed.")


if __name__ == "__main__":
    main()
