"""Synthetic-data tests for create_before_after_composites (2-D layout).

Run:
    python test_composite.py
"""
import numpy as np

from composite import (
    create_before_after_composites,
    cascading_select,
    NODATA_U8,
)


def _hand_crafted_block():
    """Construct a tiny (N_TS=5, 10, H=2, W=2) block with known per-pixel values.

    Pixel positions in (row, col): (0,0), (0,1), (1,0), (1,1) — same 4
    "pixels" as the old flat-pixel-axis test, just laid out 2-D.

    Per-pixel value matrix across timesteps (255 = nodata):
        pixel(row,col)  t=100 t=110 t=120 t=130 t=140
        (0, 0)            11    22    33    44    55
        (0, 1)           255    22   255    44   255
        (1, 0)           255   255   255   255   255
        (1, 1)            11   255   255   255    55

    All 10 bands share the same value at each (t, pixel).
    """
    N_TS, H, W = 5, 2, 2
    block = np.zeros((N_TS, 10, H, W), dtype=np.uint8)
    ts = np.array([100, 110, 120, 130, 140], dtype=np.int64)

    # Flat-pixel form for clarity, then reshape into (2, 2).
    per_pixel = np.array([
        [ 11,  22,  33,  44,  55],   # (0, 0)
        [255,  22, 255,  44, 255],   # (0, 1)
        [255, 255, 255, 255, 255],   # (1, 0)
        [ 11, 255, 255, 255,  55],   # (1, 1)
    ], dtype=np.uint8)

    for t in range(N_TS):
        for p in range(4):
            r, c = divmod(p, 2)
            block[t, :, r, c] = per_pixel[p, t]
    return block, ts


def test_cascading_select_descending_picks_most_recent():
    block, ts = _hand_crafted_block()
    # Pre-side, target=125: ts < 125 = [100, 110, 120]; sort descending.
    keep = np.array([2, 1, 0])
    sel, tstamps, valid = cascading_select(block[keep], ts[keep])

    assert sel.shape == (10, 2, 2)
    assert tstamps.shape == (2, 2)
    assert valid.shape == (2, 2)

    # (0, 0): most recent valid before 125 = t=120, value 33.
    assert sel[0, 0, 0] == 33
    assert tstamps[0, 0] == 120
    # (0, 1): t=120 is nodata, t=110 valid (22).
    assert sel[0, 0, 1] == 22
    assert tstamps[0, 1] == 110
    # (1, 0): all nodata.
    assert sel[0, 1, 0] == NODATA_U8
    assert tstamps[1, 0] == NODATA_U8
    assert not valid[1, 0]
    # (1, 1): t=100 only valid (value 11).
    assert sel[0, 1, 1] == 11
    assert tstamps[1, 1] == 100
    print("  cascading_select (descending, most-recent) — OK")


def test_cascading_select_ascending_picks_oldest():
    block, ts = _hand_crafted_block()
    # Post-side, target=125: ts > 125 = [130, 140]; sort ascending.
    keep = np.array([3, 4])
    sel, tstamps, valid = cascading_select(block[keep], ts[keep])

    assert sel[0, 0, 0] == 44
    assert tstamps[0, 0] == 130
    assert sel[0, 0, 1] == 44
    assert tstamps[0, 1] == 130
    assert sel[0, 1, 0] == NODATA_U8
    assert not valid[1, 0]
    assert sel[0, 1, 1] == 55
    assert tstamps[1, 1] == 140
    print("  cascading_select (ascending, oldest) — OK")


def test_create_before_after_one_target_date_in_range():
    block, ts = _hand_crafted_block()
    target_dates = np.array([125], dtype=np.int64)
    composites, valid = create_before_after_composites(
        block, ts, target_dates, verbose=False)

    assert composites.shape == (2, 1, 10, 2, 2)
    assert valid.tolist() == [True]

    before = composites[0, 0]  # (10, 2, 2)
    after  = composites[1, 0]

    assert before[0, 0, 0] == 33
    assert before[0, 0, 1] == 22
    assert before[0, 1, 0] == NODATA_U8
    assert before[0, 1, 1] == 11

    assert after[0, 0, 0] == 44
    assert after[0, 0, 1] == 44
    assert after[0, 1, 0] == NODATA_U8
    assert after[0, 1, 1] == 55
    print("  create_before_after (single date, in range) — OK")


def test_create_before_after_skip_outside_range():
    block, ts = _hand_crafted_block()
    target_dates = np.array([50, 200, 125], dtype=np.int64)
    composites, valid = create_before_after_composites(
        block, ts, target_dates, verbose=False)

    assert valid.tolist() == [False, False, True]
    assert (composites[:, 0, :, :, :] == NODATA_U8).all()
    assert (composites[:, 1, :, :, :] == NODATA_U8).all()
    # Valid slot retains real values for pixel (0, 0).
    assert composites[0, 2, 0, 0, 0] == 33
    print("  create_before_after (skip outside range) — OK")


def test_create_before_after_skip_no_pre_or_post():
    block, ts = _hand_crafted_block()
    target_dates = np.array([100, 140], dtype=np.int64)
    composites, valid = create_before_after_composites(
        block, ts, target_dates, verbose=False)
    assert valid.tolist() == [False, False]
    print("  create_before_after (skip no pre/post) — OK")


def test_create_before_after_warning_text():
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


def test_validation_rejects_3d_block():
    """Old flat-pixel-axis block (3-D) should raise ValueError."""
    bad = np.zeros((5, 10, 4), dtype=np.uint8)
    ts = np.array([100, 110, 120, 130, 140], dtype=np.int64)
    target_dates = np.array([125], dtype=np.int64)
    try:
        create_before_after_composites(bad, ts, target_dates, verbose=False)
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for 3-D block")
    print("  validation rejects 3-D block — OK")


def main():
    print("Running composite tests...")
    test_cascading_select_descending_picks_most_recent()
    test_cascading_select_ascending_picks_oldest()
    test_create_before_after_one_target_date_in_range()
    test_create_before_after_skip_outside_range()
    test_create_before_after_skip_no_pre_or_post()
    test_create_before_after_warning_text()
    test_validation_rejects_3d_block()
    print("All composite tests passed.")


if __name__ == "__main__":
    main()
