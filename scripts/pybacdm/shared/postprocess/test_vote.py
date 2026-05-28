"""Tests for postprocess.vote + postprocess.voted_output.

Run:
    python test_vote.py
"""
import os
import sys
import tempfile

import numpy as np

# Make this file runnable both as a script and as a module.
if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from postprocess.vote import (
        VoteAccumulator, CHIP_H, CHIP_W, LIVE_H, LIVE_W, DEFAULT_THRESHOLD,
    )
    from postprocess.voted_output import (
        write_voted_block, read_voted_block, voted_path_for_block,
    )
else:
    from .vote import (
        VoteAccumulator, CHIP_H, CHIP_W, LIVE_H, LIVE_W, DEFAULT_THRESHOLD,
    )
    from .voted_output import (
        write_voted_block, read_voted_block, voted_path_for_block,
    )


# ============================================================================
# VoteAccumulator init
# ============================================================================

def test_init_allocates_correct_shape():
    acc = VoteAccumulator(classes=(1, 2))
    assert acc.votes.shape == (2, LIVE_H, LIVE_W)
    assert acc.votes.dtype == np.uint8
    assert acc.votes.sum() == 0
    assert acc.classes == (1, 2)
    print("  init (classes (1,2) -> shape (2, LIVE_H, LIVE_W) uint8) — OK")


def test_init_sorts_and_dedups_classes():
    acc = VoteAccumulator(classes=(2, 1, 2))
    assert acc.classes == (1, 2)
    print("  init (deduplicates + sorts classes) — OK")


def test_init_drops_background_class():
    """class 0 is background — never tracked."""
    acc = VoteAccumulator(classes=(0, 1, 2))
    assert acc.classes == (1, 2)
    print("  init (drops background class 0) — OK")


def test_init_rejects_only_background():
    try:
        VoteAccumulator(classes=(0,))
    except ValueError:
        print("  init (empty after bg drop -> ValueError) — OK")
        return
    raise AssertionError("expected ValueError for classes=(0,)")


def test_init_custom_live_size():
    acc = VoteAccumulator(classes=(1,), live_h=512, live_w=768)
    assert acc.votes.shape == (1, 512, 768)
    print("  init (custom live_h, live_w) — OK")


# ============================================================================
# VoteAccumulator add (placement math)
# ============================================================================

def _full_chip(cls: int) -> np.ndarray:
    return np.full((CHIP_H, CHIP_W), cls, dtype=np.uint8)


def test_add_original_chip_increments_quadrant():
    """Original chip at grid (0, 0) covers LIVE [0:256, 0:256]."""
    acc = VoteAccumulator(classes=(1,))
    acc.add(_full_chip(1), chip_nw_px_y=0, chip_nw_px_x=0)
    assert (acc.votes[0, :CHIP_H, :CHIP_W] == 1).all()
    assert (acc.votes[0, CHIP_H:, :] == 0).all()
    assert (acc.votes[0, :, CHIP_W:] == 0).all()
    print("  add (original chip at (0,0) -> NW quadrant + 1) — OK")


def test_add_ignores_background_pixels():
    """Background pixels in the chip never touch any class counter."""
    lm = np.zeros((CHIP_H, CHIP_W), dtype=np.uint8)
    lm[100:110, 100:110] = 1   # class 1 patch
    # Everything else is bg (0)
    acc = VoteAccumulator(classes=(1, 2))
    acc.add(lm, chip_nw_px_y=0, chip_nw_px_x=0)
    assert acc.votes[0].sum() == 100   # 10x10 = 100 class-1 votes
    assert acc.votes[1].sum() == 0     # no class-2 votes
    print("  add (bg pixels contribute no votes) — OK")


def test_add_two_classes_in_one_chip():
    lm = np.zeros((CHIP_H, CHIP_W), dtype=np.uint8)
    lm[0:10, 0:10] = 1
    lm[0:10, 10:20] = 2
    acc = VoteAccumulator(classes=(1, 2))
    acc.add(lm, chip_nw_px_y=0, chip_nw_px_x=0)
    assert (acc.votes[0, 0:10, 0:10] == 1).all()
    assert (acc.votes[0, 0:10, 10:20] == 0).all()
    assert (acc.votes[1, 0:10, 10:20] == 1).all()
    assert (acc.votes[1, 0:10, 0:10] == 0).all()
    print("  add (multi-class chip increments per-class) — OK")


def test_add_ghost_using_chip_clips_to_live():
    """Chip at chip_nw_px = (-128, -128) only contributes its SE quarter."""
    acc = VoteAccumulator(classes=(1,))
    acc.add(_full_chip(1), chip_nw_px_y=-128, chip_nw_px_x=-128)
    # The SE 128x128 of the chip maps to LIVE [0:128, 0:128].
    assert (acc.votes[0, :128, :128] == 1).all()
    assert (acc.votes[0, 128:, :] == 0).all()
    assert (acc.votes[0, :, 128:] == 0).all()
    print("  add (diagonal ghost chip at (-128,-128) -> NW 128x128 only) — OK")


def test_add_east_edge_chip_clips_to_live():
    """h_shift at the east edge (c_gap=3, c=3+0.5) lands chip_nw_px_x=128+3*256=896.
    Its east half (128 px) falls past the LIVE east edge — should clip."""
    acc = VoteAccumulator(classes=(1,))
    acc.add(_full_chip(1), chip_nw_px_y=0, chip_nw_px_x=LIVE_W - 128)
    # Only the west half of the chip (128 px wide) overlaps LIVE.
    assert (acc.votes[0, :256, LIVE_W - 128:LIVE_W] == 1).all()
    assert acc.votes[0].sum() == 256 * 128
    print("  add (east-edge chip extends past LIVE -> clipped) — OK")


def test_add_chip_entirely_outside_live_is_noop():
    """A chip whose NW + 256 lands at or before LIVE pixel 0 contributes nothing."""
    acc = VoteAccumulator(classes=(1,))
    acc.add(_full_chip(1), chip_nw_px_y=-256, chip_nw_px_x=-256)
    assert acc.votes.sum() == 0
    print("  add (chip entirely outside LIVE -> no-op) — OK")


def test_add_rejects_wrong_shape():
    acc = VoteAccumulator(classes=(1,))
    try:
        acc.add(np.zeros((100, 100), dtype=np.uint8), 0, 0)
    except ValueError:
        print("  add (wrong shape -> ValueError) — OK")
        return
    raise AssertionError("expected ValueError for wrong-shape label_map")


# ============================================================================
# 4-vote invariant: every LIVE pixel gets exactly 4 increments
# ============================================================================

def test_four_vote_invariant_with_all_class1_chips():
    """Simulate the geometry: every LIVE pixel must receive 4 votes when
    all 81 chips predict class 1 everywhere. This is the load-bearing
    test that ties the voter to the shift geometry."""
    # Mirror the 81-shift enumeration from
    # composite_shift_chips.shift_chips._iter_shift_positions.
    HALF = CHIP_H // 2
    positions = []
    # original: 4x4
    for r in range(4):
        for c in range(4):
            positions.append((r * CHIP_H, c * CHIP_W))
    # h_shift: r in [0,4), c_gap in [-1,4)
    for r in range(4):
        for c_gap in range(-1, 4):
            positions.append((r * CHIP_H, c_gap * CHIP_W + HALF))
    # v_shift: r_gap in [-1,4), c in [0,4)
    for r_gap in range(-1, 4):
        for c in range(4):
            positions.append((r_gap * CHIP_H + HALF, c * CHIP_W))
    # diagonal: r_gap in [-1,4), c_gap in [-1,4)
    for r_gap in range(-1, 4):
        for c_gap in range(-1, 4):
            positions.append((r_gap * CHIP_H + HALF, c_gap * CHIP_W + HALF))

    assert len(positions) == 16 + 20 + 20 + 25 == 81

    acc = VoteAccumulator(classes=(1,))
    for ny, nx in positions:
        acc.add(_full_chip(1), chip_nw_px_y=ny, chip_nw_px_x=nx)

    # Every LIVE pixel saw exactly 4 chips.
    assert (acc.votes[0] == 4).all(), (
        f"min={acc.votes[0].min()}, max={acc.votes[0].max()}"
    )
    print("  4-vote invariant (81 chips of class 1 -> every LIVE pixel = 4) — OK")


# ============================================================================
# finalize (threshold + tiebreak)
# ============================================================================

def test_finalize_threshold_2_keeps_pixels_with_2_or_more_votes():
    acc = VoteAccumulator(classes=(1,))
    # Manually set vote counts.
    acc.votes[0, 0, 0] = 1   # below threshold
    acc.votes[0, 0, 1] = 2   # at threshold
    acc.votes[0, 0, 2] = 4   # well above
    out = acc.finalize(threshold=2)
    assert out[0, 0] == 0   # filtered
    assert out[0, 1] == 1   # kept
    assert out[0, 2] == 1   # kept
    print("  finalize (threshold=2 keeps >=2 votes) — OK")


def test_finalize_configurable_threshold():
    acc = VoteAccumulator(classes=(1,))
    acc.votes[0, 0, 0] = 2
    acc.votes[0, 0, 1] = 3
    out = acc.finalize(threshold=3)
    assert out[0, 0] == 0
    assert out[0, 1] == 1
    print("  finalize (configurable threshold) — OK")


def test_finalize_picks_class_with_more_votes():
    """Two classes both at a pixel: argmax picks the one with more votes."""
    acc = VoteAccumulator(classes=(1, 2))
    acc.votes[0, 5, 5] = 1   # class 1: 1 vote
    acc.votes[1, 5, 5] = 3   # class 2: 3 votes
    out = acc.finalize(threshold=2)
    assert out[5, 5] == 2
    print("  finalize (picks class with more votes) — OK")


def test_finalize_tiebreak_lowest_class_id():
    """When two classes tie, np.argmax picks the lower index = lower class ID."""
    acc = VoteAccumulator(classes=(1, 2))
    acc.votes[0, 5, 5] = 2
    acc.votes[1, 5, 5] = 2
    out = acc.finalize(threshold=2)
    assert out[5, 5] == 1
    print("  finalize (tie -> lowest class ID) — OK")


def test_finalize_no_votes_anywhere_returns_all_zero():
    acc = VoteAccumulator(classes=(1, 2))
    out = acc.finalize(threshold=2)
    assert out.shape == (LIVE_H, LIVE_W)
    assert out.dtype == np.uint8
    assert (out == 0).all()
    print("  finalize (no votes -> all zero) — OK")


def test_finalize_rejects_zero_threshold():
    acc = VoteAccumulator(classes=(1,))
    try:
        acc.finalize(threshold=0)
    except ValueError:
        print("  finalize (threshold=0 -> ValueError) — OK")
        return
    raise AssertionError("expected ValueError for threshold=0")


def test_n_votes_by_class():
    acc = VoteAccumulator(classes=(1, 2))
    acc.votes[0, 0:5, 0:5] = 1
    acc.votes[1, 0:3, 0:3] = 2
    counts = acc.n_votes_by_class()
    assert counts == {1: 25, 2: 18}
    print("  n_votes_by_class — OK")


# ============================================================================
# voted_output.write_voted_block / read_voted_block
# ============================================================================

def test_voted_path_for_block():
    p = voted_path_for_block("/tmp/out", "T29TPG", 12, 7)
    assert p == "/tmp/out/T29TPG_block_012_007.npz"
    print("  voted_path_for_block — OK")


def test_write_voted_block_roundtrip():
    with tempfile.TemporaryDirectory() as tmpd:
        labels = np.zeros((2, LIVE_H, LIVE_W), dtype=np.uint8)
        labels[0, 100:110, 100:110] = 1
        labels[1, 200:210, 200:210] = 2
        target_dates = np.array([738887, 738900], dtype=np.int64)

        path = write_voted_block(
            tmpd, "T29TPG", 3, 5,
            labels=labels,
            target_dates=target_dates,
            classes=(1, 2),
            world_origin_x=500000.0,
            world_origin_y=4500000.0,
            pixel_res=10.0,
            threshold=2,
        )
        assert os.path.exists(path)
        assert path.endswith("T29TPG_block_003_005.npz")

        d = read_voted_block(path)
        assert np.array_equal(d["labels"], labels)
        assert np.array_equal(d["target_dates"], target_dates)
        assert np.array_equal(d["classes"], np.array([1, 2], dtype=np.uint8))
        assert int(d["block_row"]) == 3
        assert int(d["block_col"]) == 5
        assert float(d["world_origin_x"]) == 500000.0
        assert float(d["world_origin_y"]) == 4500000.0
        assert float(d["pixel_res"]) == 10.0
        assert int(d["threshold"]) == 2
    print("  write_voted_block + read_voted_block roundtrip — OK")


def test_write_voted_block_validates_dtype():
    with tempfile.TemporaryDirectory() as tmpd:
        try:
            write_voted_block(
                tmpd, "T", 0, 0,
                labels=np.zeros((1, LIVE_H, LIVE_W), dtype=np.uint16),
                target_dates=np.array([0], dtype=np.int64),
                classes=(1,),
                world_origin_x=0.0, world_origin_y=0.0, pixel_res=10.0,
                threshold=2,
            )
        except ValueError:
            print("  write_voted_block (rejects non-uint8 labels) — OK")
            return
    raise AssertionError("expected ValueError for non-uint8 labels")


def test_write_voted_block_validates_target_dates_length():
    with tempfile.TemporaryDirectory() as tmpd:
        try:
            write_voted_block(
                tmpd, "T", 0, 0,
                labels=np.zeros((2, LIVE_H, LIVE_W), dtype=np.uint8),
                target_dates=np.array([0, 1, 2], dtype=np.int64),
                classes=(1,),
                world_origin_x=0.0, world_origin_y=0.0, pixel_res=10.0,
                threshold=2,
            )
        except ValueError:
            print("  write_voted_block (rejects mismatched target_dates) — OK")
            return
    raise AssertionError("expected ValueError for mismatched target_dates length")


# ============================================================================
# Main
# ============================================================================

def main():
    print("Running vote + voted_output tests...")
    test_init_allocates_correct_shape()
    test_init_sorts_and_dedups_classes()
    test_init_drops_background_class()
    test_init_rejects_only_background()
    test_init_custom_live_size()
    test_add_original_chip_increments_quadrant()
    test_add_ignores_background_pixels()
    test_add_two_classes_in_one_chip()
    test_add_ghost_using_chip_clips_to_live()
    test_add_east_edge_chip_clips_to_live()
    test_add_chip_entirely_outside_live_is_noop()
    test_add_rejects_wrong_shape()
    test_four_vote_invariant_with_all_class1_chips()
    test_finalize_threshold_2_keeps_pixels_with_2_or_more_votes()
    test_finalize_configurable_threshold()
    test_finalize_picks_class_with_more_votes()
    test_finalize_tiebreak_lowest_class_id()
    test_finalize_no_votes_anywhere_returns_all_zero()
    test_finalize_rejects_zero_threshold()
    test_n_votes_by_class()
    test_voted_path_for_block()
    test_write_voted_block_roundtrip()
    test_write_voted_block_validates_dtype()
    test_write_voted_block_validates_target_dates_length()
    print("All vote tests passed.")


if __name__ == "__main__":
    main()
