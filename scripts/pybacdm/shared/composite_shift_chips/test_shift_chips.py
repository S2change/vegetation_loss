"""Synthetic-data tests for generate_shifted_chips (2-D block, 81 shifts).

The test composites are constructed so each ghost / live region of the block
is filled with a distinct uint8 value. That lets us assert which region a
yielded chip pulled from by examining pixel values directly.

Run:
    python test_shift_chips.py
"""
import numpy as np

from shift_chips import (
    ChipPair,
    ChipBundle,
    generate_shifted_chips,
    generate_shifted_chips_bundled,
    chip_nw_pixel_offset,
    _slice_chip,
    _iter_shift_positions,
    CHIP_H, CHIP_W, HALF,
    LIVE_ROWS, LIVE_COLS, LIVE_H, LIVE_W,
    GHOST, BLOCK_H, BLOCK_W,
    N_ORIGINALS, N_H_SHIFTS, N_V_SHIFTS, N_DIAGONALS, BUNDLE_SIZE,
)


# ============================================================================
# Helpers
# ============================================================================

def _hand_crafted_composites(n_dates: int = 1):
    """Build a (2, n_dates, 10, BLOCK_H, BLOCK_W) composite array where each
    block region carries a unique uint8 value:

      live chip (r, c)  ->  value = 10 + r*4 + c    (range 10..25)
      top strip          ->  value = 30
      bottom strip       ->  value = 31
      left strip         ->  value = 32
      right strip        ->  value = 33
      NW corner          ->  value = 40
      NE corner          ->  value = 41
      SW corner          ->  value = 42
      SE corner          ->  value = 43

    All 10 bands share the same per-region tag (easy to reason about).
    Both before and after sides use identical tags.
    """
    composites = np.empty((2, n_dates, 10, BLOCK_H, BLOCK_W), dtype=np.uint8)

    # Ghost ring regions
    composites[..., 0:GHOST, GHOST:GHOST + LIVE_W] = 30                  # top strip
    composites[..., GHOST + LIVE_H:BLOCK_H, GHOST:GHOST + LIVE_W] = 31  # bottom strip
    composites[..., GHOST:GHOST + LIVE_H, 0:GHOST] = 32                  # left strip
    composites[..., GHOST:GHOST + LIVE_H, GHOST + LIVE_W:BLOCK_W] = 33   # right strip
    composites[..., 0:GHOST, 0:GHOST] = 40                                # NW corner
    composites[..., 0:GHOST, GHOST + LIVE_W:BLOCK_W] = 41                 # NE corner
    composites[..., GHOST + LIVE_H:BLOCK_H, 0:GHOST] = 42                 # SW corner
    composites[..., GHOST + LIVE_H:BLOCK_H, GHOST + LIVE_W:BLOCK_W] = 43  # SE corner

    # Live chips
    for r in range(LIVE_ROWS):
        for c in range(LIVE_COLS):
            y0 = GHOST + r * CHIP_H
            x0 = GHOST + c * CHIP_W
            tag = np.uint8(10 + r * LIVE_COLS + c)
            composites[..., y0:y0 + CHIP_H, x0:x0 + CHIP_W] = tag

    return composites


def _expected_live_chip_tag(r: int, c: int) -> int:
    return 10 + r * LIVE_COLS + c


# ============================================================================
# Constant + geometry sanity checks
# ============================================================================

def test_shift_count_constants():
    assert N_ORIGINALS == 16
    assert N_H_SHIFTS  == 20
    assert N_V_SHIFTS  == 20
    assert N_DIAGONALS == 25
    assert BUNDLE_SIZE == 81
    print("  shift count constants (16/20/20/25/81) — OK")


def test_chip_nw_pixel_offset_all_kinds():
    # All formulas relative to live-area NW corner (i.e. live (0,0) starts at (0,0)).
    assert chip_nw_pixel_offset("original", 1, 2) == (256, 512)
    assert chip_nw_pixel_offset("h_shift", 1, 2)  == (256, 512 + HALF)
    assert chip_nw_pixel_offset("v_shift", 1, 2)  == (256 + HALF, 512)
    assert chip_nw_pixel_offset("diagonal", 1, 2) == (256 + HALF, 512 + HALF)
    # Negative gaps extend NW into ghost.
    assert chip_nw_pixel_offset("h_shift", 0, -1)  == (0, -HALF)
    assert chip_nw_pixel_offset("v_shift", -1, 0)  == (-HALF, 0)
    assert chip_nw_pixel_offset("diagonal", -1, -1) == (-HALF, -HALF)
    print("  chip_nw_pixel_offset (all kinds incl. negative gaps) — OK")


def test_iter_shift_positions_counts():
    assert sum(1 for _ in _iter_shift_positions("original")) == N_ORIGINALS
    assert sum(1 for _ in _iter_shift_positions("h_shift"))  == N_H_SHIFTS
    assert sum(1 for _ in _iter_shift_positions("v_shift"))  == N_V_SHIFTS
    assert sum(1 for _ in _iter_shift_positions("diagonal")) == N_DIAGONALS
    print("  _iter_shift_positions counts — OK")


# ============================================================================
# Slice / shift content tests
# ============================================================================

def test_slice_chip_original_interior():
    """Slicing an 'original (1, 2)' chip should return the live chip (1, 2)
    tag throughout — that chip lives entirely in the live area."""
    composites = _hand_crafted_composites()
    side = composites[0, 0]   # (10, BLOCK_H, BLOCK_W)
    nw_y, nw_x = chip_nw_pixel_offset("original", 1, 2)
    chip = _slice_chip(side, nw_y, nw_x)
    assert chip.shape == (10, CHIP_H, CHIP_W)
    expected_tag = _expected_live_chip_tag(1, 2)
    assert (chip == expected_tag).all()
    print("  _slice_chip (original interior) — OK")


def test_slice_chip_h_shift_interior():
    """H-shift at (R=1, c_gap=1) crosses between live chips (1, 1) and (1, 2).
    Left half should be tag of (1, 1), right half should be tag of (1, 2)."""
    composites = _hand_crafted_composites()
    side = composites[0, 0]
    nw_y, nw_x = chip_nw_pixel_offset("h_shift", 1, 1)   # (256, 384)
    chip = _slice_chip(side, nw_y, nw_x)
    assert (chip[:, :, :HALF] == _expected_live_chip_tag(1, 1)).all()
    assert (chip[:, :, HALF:] == _expected_live_chip_tag(1, 2)).all()
    print("  _slice_chip (h_shift interior) — OK")


def test_slice_chip_h_shift_left_edge_uses_ghost():
    """H-shift at (R=2, c_gap=-1) has NW at (512, -128). Left half pulls
    from the left ghost strip (tag 32); right half is live chip (2, 0)."""
    composites = _hand_crafted_composites()
    side = composites[0, 0]
    nw_y, nw_x = chip_nw_pixel_offset("h_shift", 2, -1)
    assert (nw_y, nw_x) == (512, -HALF)
    chip = _slice_chip(side, nw_y, nw_x)
    assert (chip[:, :, :HALF] == 32).all(), "left half should be left ghost strip (32)"
    assert (chip[:, :, HALF:] == _expected_live_chip_tag(2, 0)).all()
    print("  _slice_chip (h_shift c_gap=-1, uses left ghost strip) — OK")


def test_slice_chip_h_shift_right_edge_uses_ghost():
    """H-shift at (R=0, c_gap=3) has NW at (0, 896). Left half = live (0, 3),
    right half pulls from the right ghost strip (tag 33)."""
    composites = _hand_crafted_composites()
    side = composites[0, 0]
    nw_y, nw_x = chip_nw_pixel_offset("h_shift", 0, 3)
    chip = _slice_chip(side, nw_y, nw_x)
    assert (chip[:, :, :HALF] == _expected_live_chip_tag(0, 3)).all()
    assert (chip[:, :, HALF:] == 33).all()
    print("  _slice_chip (h_shift c_gap=3, uses right ghost strip) — OK")


def test_slice_chip_v_shift_top_edge_uses_ghost():
    """V-shift at (r_gap=-1, C=2) has NW at (-128, 512). Top half = top
    ghost strip (30); bottom half = live (0, 2)."""
    composites = _hand_crafted_composites()
    side = composites[0, 0]
    nw_y, nw_x = chip_nw_pixel_offset("v_shift", -1, 2)
    chip = _slice_chip(side, nw_y, nw_x)
    assert (chip[:, :HALF, :] == 30).all()
    assert (chip[:, HALF:, :] == _expected_live_chip_tag(0, 2)).all()
    print("  _slice_chip (v_shift r_gap=-1, uses top ghost strip) — OK")


def test_slice_chip_v_shift_bottom_edge_uses_ghost():
    """V-shift at (r_gap=3, C=1) has NW at (896, 256). Top half = live (3, 1),
    bottom half = bottom ghost strip (31)."""
    composites = _hand_crafted_composites()
    side = composites[0, 0]
    nw_y, nw_x = chip_nw_pixel_offset("v_shift", 3, 1)
    chip = _slice_chip(side, nw_y, nw_x)
    assert (chip[:, :HALF, :] == _expected_live_chip_tag(3, 1)).all()
    assert (chip[:, HALF:, :] == 31).all()
    print("  _slice_chip (v_shift r_gap=3, uses bottom ghost strip) — OK")


def test_slice_chip_diagonal_nw_corner_uses_nw_ghost_corner():
    """Diagonal at (r_gap=-1, c_gap=-1) has NW at (-128, -128) — all 4
    quadrants in the ghost ring. Specifically:
      TL = NW ghost corner (40)
      TR = top ghost strip
      BL = left ghost strip
      BR = live (0, 0)"""
    composites = _hand_crafted_composites()
    side = composites[0, 0]
    nw_y, nw_x = chip_nw_pixel_offset("diagonal", -1, -1)
    chip = _slice_chip(side, nw_y, nw_x)
    assert (chip[:, :HALF, :HALF] == 40).all(), "TL should be NW corner (40)"
    assert (chip[:, :HALF, HALF:] == 30).all(), "TR should be top strip (30)"
    assert (chip[:, HALF:, :HALF] == 32).all(), "BL should be left strip (32)"
    assert (chip[:, HALF:, HALF:] == _expected_live_chip_tag(0, 0)).all()
    print("  _slice_chip (diagonal (-1,-1), uses NW corner + adj strips) — OK")


def test_slice_chip_diagonal_se_corner_uses_se_ghost_corner():
    """Diagonal at (r_gap=3, c_gap=3) has NW at (896, 896):
      TL = live (3, 3)
      TR = right ghost strip
      BL = bottom ghost strip
      BR = SE corner (43)"""
    composites = _hand_crafted_composites()
    side = composites[0, 0]
    nw_y, nw_x = chip_nw_pixel_offset("diagonal", 3, 3)
    chip = _slice_chip(side, nw_y, nw_x)
    assert (chip[:, :HALF, :HALF] == _expected_live_chip_tag(3, 3)).all()
    assert (chip[:, :HALF, HALF:] == 33).all()
    assert (chip[:, HALF:, :HALF] == 31).all()
    assert (chip[:, HALF:, HALF:] == 43).all()
    print("  _slice_chip (diagonal (3,3), uses SE corner + adj strips) — OK")


def test_slice_chip_diagonal_interior():
    """Interior diagonal at (r_gap=1, c_gap=2): 4 live chips meet."""
    composites = _hand_crafted_composites()
    side = composites[0, 0]
    nw_y, nw_x = chip_nw_pixel_offset("diagonal", 1, 2)
    chip = _slice_chip(side, nw_y, nw_x)
    # Quadrants come from live chips (1,2), (1,3), (2,2), (2,3).
    assert (chip[:, :HALF, :HALF] == _expected_live_chip_tag(1, 2)).all()
    assert (chip[:, :HALF, HALF:] == _expected_live_chip_tag(1, 3)).all()
    assert (chip[:, HALF:, :HALF] == _expected_live_chip_tag(2, 2)).all()
    assert (chip[:, HALF:, HALF:] == _expected_live_chip_tag(2, 3)).all()
    print("  _slice_chip (diagonal interior) — OK")


# ============================================================================
# Generator tests
# ============================================================================

def test_generator_yields_81_per_valid_date():
    composites = _hand_crafted_composites(n_dates=2)
    target_dates = np.array([100, 200], dtype=np.int64)
    valid_mask = np.array([True, True], dtype=bool)

    pairs = list(generate_shifted_chips(
        composites, target_dates, valid_mask, verbose=False))

    assert len(pairs) == 81 * 2, len(pairs)

    kinds_per_date = {0: {}, 1: {}}
    for p in pairs:
        kinds_per_date[p.date_idx].setdefault(p.chip_kind, 0)
        kinds_per_date[p.date_idx][p.chip_kind] += 1

    expected = {"original": 16, "h_shift": 20, "v_shift": 20, "diagonal": 25}
    for k in (0, 1):
        assert kinds_per_date[k] == expected, \
            f"date_idx={k} got {kinds_per_date[k]}, expected {expected}"
    print("  generator yields 81 pairs per valid date (2 dates -> 162) — OK")


def test_generator_skips_invalid_dates():
    composites = _hand_crafted_composites(n_dates=3)
    target_dates = np.array([100, 200, 300], dtype=np.int64)
    valid_mask = np.array([True, False, True], dtype=bool)

    pairs = list(generate_shifted_chips(
        composites, target_dates, valid_mask, verbose=False))

    assert len(pairs) == 81 * 2
    assert {p.date_idx for p in pairs} == {0, 2}
    print("  generator skips invalid dates — OK")


def test_generator_metadata_consistency():
    composites = _hand_crafted_composites(n_dates=1)
    target_dates = np.array([12345], dtype=np.int64)
    valid_mask = np.array([True], dtype=bool)

    pairs = list(generate_shifted_chips(
        composites, target_dates, valid_mask, verbose=False))

    for p in pairs:
        assert isinstance(p, ChipPair)
        assert p.date_idx == 0
        assert p.date_ordinal == 12345
        assert p.before.shape == (10, CHIP_H, CHIP_W)
        assert p.after.shape  == (10, CHIP_H, CHIP_W)
        assert p.before.dtype == np.uint8
        # Before/after share the same tag in our synthetic data.
        assert np.array_equal(p.before, p.after), \
            f"before/after diverged for {p.chip_kind} {p.grid_position}"
    print("  generator metadata + before/after parity — OK")


def test_generator_position_coverage():
    """Each shift kind should yield its expected set of (gr, gc) positions exactly once."""
    composites = _hand_crafted_composites()
    target_dates = np.array([100], dtype=np.int64)
    valid_mask = np.array([True], dtype=bool)

    pairs = list(generate_shifted_chips(
        composites, target_dates, valid_mask, verbose=False))

    expected = {
        "original": {(r, c) for r in range(LIVE_ROWS) for c in range(LIVE_COLS)},
        "h_shift":  {(r, c) for r in range(LIVE_ROWS) for c in range(-1, LIVE_COLS)},
        "v_shift":  {(r, c) for r in range(-1, LIVE_ROWS) for c in range(LIVE_COLS)},
        "diagonal": {(r, c) for r in range(-1, LIVE_ROWS) for c in range(-1, LIVE_COLS)},
    }
    for kind, exp_positions in expected.items():
        positions = {p.grid_position for p in pairs if p.chip_kind == kind}
        assert positions == exp_positions, \
            f"{kind}: missing {exp_positions - positions} extra {positions - exp_positions}"
    print("  generator covers full per-kind position set — OK")


def test_each_live_pixel_gets_exactly_4_votes():
    """The most important geometric property: every live-area pixel is
    covered by exactly 4 shifts (one of each kind)."""
    composites = _hand_crafted_composites()
    target_dates = np.array([100], dtype=np.int64)
    valid_mask = np.array([True], dtype=bool)

    pairs = list(generate_shifted_chips(
        composites, target_dates, valid_mask, verbose=False))

    # Build a (LIVE_H, LIVE_W) per-kind counter array.
    counter_total: dict[str, np.ndarray] = {
        kind: np.zeros((LIVE_H, LIVE_W), dtype=np.int32)
        for kind in ("original", "h_shift", "v_shift", "diagonal")
    }
    counter_overall = np.zeros((LIVE_H, LIVE_W), dtype=np.int32)

    for p in pairs:
        nw_y, nw_x = chip_nw_pixel_offset(p.chip_kind, *p.grid_position)
        # The chip covers pixel area [nw_y, nw_y+256) x [nw_x, nw_x+256)
        # relative to the live-area NW corner. Clip to the live area.
        y0 = max(nw_y, 0)
        x0 = max(nw_x, 0)
        y1 = min(nw_y + CHIP_H, LIVE_H)
        x1 = min(nw_x + CHIP_W, LIVE_W)
        if y0 < y1 and x0 < x1:
            counter_total[p.chip_kind][y0:y1, x0:x1] += 1
            counter_overall[y0:y1, x0:x1] += 1

    # Each kind covers every live pixel exactly once:
    for kind, arr in counter_total.items():
        assert (arr == 1).all(), \
            f"{kind} did not cover every live pixel exactly once " \
            f"(min={arr.min()}, max={arr.max()})"
    # Overall: every live pixel gets 4 votes.
    assert (counter_overall == 4).all(), \
        f"not 4 votes everywhere (min={counter_overall.min()}, " \
        f"max={counter_overall.max()})"
    print("  every live pixel covered by exactly 4 chips (1 per kind) — OK")


# ============================================================================
# Bundled-generator tests
# ============================================================================

def test_bundled_matches_per_pair():
    """Bundled generator should produce the same pixel content + metadata as
    the per-pair generator, just in (B, H, W, C) layout."""
    composites = _hand_crafted_composites(n_dates=2)
    target_dates = np.array([100, 200], dtype=np.int64)
    valid_mask = np.array([True, True], dtype=bool)

    pairs = list(generate_shifted_chips(
        composites, target_dates, valid_mask, verbose=False))
    bundles = list(generate_shifted_chips_bundled(
        composites, target_dates, valid_mask, verbose=False))

    assert len(bundles) == 2
    assert len(pairs) == 2 * BUNDLE_SIZE

    pairs_by_date: dict = {0: [], 1: []}
    for p in pairs:
        pairs_by_date[p.date_idx].append(p)

    for bundle in bundles:
        assert isinstance(bundle, ChipBundle)
        assert bundle.before.shape == (BUNDLE_SIZE, CHIP_H, CHIP_W, 10)
        assert bundle.after.shape  == (BUNDLE_SIZE, CHIP_H, CHIP_W, 10)
        assert len(bundle.chip_kinds) == BUNDLE_SIZE
        assert len(bundle.grid_positions) == BUNDLE_SIZE

        pairs_for_date = pairs_by_date[bundle.date_idx]
        assert len(pairs_for_date) == BUNDLE_SIZE

        for i, (kind, pos) in enumerate(
                zip(bundle.chip_kinds, bundle.grid_positions)):
            p = pairs_for_date[i]
            assert p.chip_kind == kind
            assert p.grid_position == pos
            assert p.date_ordinal == bundle.date_ordinal
            # Per-pair chip is (C, H, W); bundle slot is (H, W, C).
            assert np.array_equal(
                p.before.transpose(1, 2, 0), bundle.before[i]
            ), f"before mismatch at i={i} ({kind} {pos})"
            assert np.array_equal(
                p.after.transpose(1, 2, 0), bundle.after[i]
            ), f"after mismatch at i={i} ({kind} {pos})"
    print("  bundled matches per-pair (content + metadata) — OK")


def test_bundled_skips_invalid_dates():
    composites = _hand_crafted_composites(n_dates=3)
    target_dates = np.array([100, 200, 300], dtype=np.int64)
    valid_mask = np.array([True, False, True], dtype=bool)
    bundles = list(generate_shifted_chips_bundled(
        composites, target_dates, valid_mask, verbose=False))
    assert len(bundles) == 2
    assert {b.date_idx for b in bundles} == {0, 2}
    print("  bundled skips invalid dates — OK")


# ============================================================================
# Validation tests
# ============================================================================

def test_validation_rejects_3d_composites():
    """Old flat-pixel-axis composites (4-D) should raise ValueError."""
    bad = np.zeros((2, 1, 10, 65536), dtype=np.uint8)
    target_dates = np.array([100], dtype=np.int64)
    valid_mask = np.array([True], dtype=bool)
    try:
        list(generate_shifted_chips(bad, target_dates, valid_mask, verbose=False))
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for 4-D composites")
    print("  validation rejects 4-D composites — OK")


def test_validation_rejects_wrong_spatial_dims():
    """Composites with wrong BLOCK_H / BLOCK_W should raise ValueError."""
    bad = np.zeros((2, 1, 10, 100, 100), dtype=np.uint8)
    target_dates = np.array([100], dtype=np.int64)
    valid_mask = np.array([True], dtype=bool)
    try:
        list(generate_shifted_chips(bad, target_dates, valid_mask, verbose=False))
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for wrong spatial dims")
    print("  validation rejects wrong spatial dims — OK")


def main():
    print("Running shift_chips tests...")
    test_shift_count_constants()
    test_chip_nw_pixel_offset_all_kinds()
    test_iter_shift_positions_counts()
    test_slice_chip_original_interior()
    test_slice_chip_h_shift_interior()
    test_slice_chip_h_shift_left_edge_uses_ghost()
    test_slice_chip_h_shift_right_edge_uses_ghost()
    test_slice_chip_v_shift_top_edge_uses_ghost()
    test_slice_chip_v_shift_bottom_edge_uses_ghost()
    test_slice_chip_diagonal_nw_corner_uses_nw_ghost_corner()
    test_slice_chip_diagonal_se_corner_uses_se_ghost_corner()
    test_slice_chip_diagonal_interior()
    test_generator_yields_81_per_valid_date()
    test_generator_skips_invalid_dates()
    test_generator_metadata_consistency()
    test_generator_position_coverage()
    test_each_live_pixel_gets_exactly_4_votes()
    test_bundled_matches_per_pair()
    test_bundled_skips_invalid_dates()
    test_validation_rejects_3d_composites()
    test_validation_rejects_wrong_spatial_dims()
    print("All shift_chips tests passed.")


if __name__ == "__main__":
    main()
