"""Synthetic-data tests for generate_shifted_chips.

The block is constructed so each chip's pixels encode its (row, col) position
in the 5x5 grid as `row * 10 + col`. That makes it trivial to assert which
source chip(s) a yielded chip pulled from: every pixel of a slice carries
the source chip's grid coordinates.

Run:
    python test_shift_chips.py
"""
import numpy as np

from shift_chips import (
    ChipPair,
    generate_shifted_chips,
    _extract_chip,
    _h_shift,
    _v_shift,
    _diagonal,
    CHIP_H, CHIP_W, CHIP_PIXELS, HALF,
    BLOCK_GRID_ROWS, BLOCK_GRID_COLS,
    LIVE_ROWS, LIVE_COLS,
)


def _hand_crafted_composites(n_dates: int = 1):
    """Build a (2, n_dates, 10, 25 * 65_536) array where every pixel of
    chip (R, C) holds the encoded value `R * 10 + C` (uint8).

    Both before and after sides use the same encoding so we can verify the
    geometry without worrying about which side we're reading.
    """
    n_chips = BLOCK_GRID_ROWS * BLOCK_GRID_COLS    # 25
    P = n_chips * CHIP_PIXELS
    composites = np.empty((2, n_dates, 10, P), dtype=np.uint8)
    for R in range(BLOCK_GRID_ROWS):
        for C in range(BLOCK_GRID_COLS):
            flat_idx = R * BLOCK_GRID_COLS + C
            start = flat_idx * CHIP_PIXELS
            end = start + CHIP_PIXELS
            tag = np.uint8(R * 10 + C)
            composites[:, :, :, start:end] = tag
    return composites


def test_extract_chip_yields_correct_position_tag():
    composites = _hand_crafted_composites()
    side = composites[0, 0]   # (10, P)
    for R in range(BLOCK_GRID_ROWS):
        for C in range(BLOCK_GRID_COLS):
            chip = _extract_chip(side, R, C)
            assert chip.shape == (10, CHIP_H, CHIP_W)
            assert (chip == R * 10 + C).all(), \
                f"chip ({R},{C}) had values {np.unique(chip)}, expected {R*10+C}"
    print("  _extract_chip (all 25 positions) — OK")


def test_h_shift_geometry():
    composites = _hand_crafted_composites()
    side = composites[0, 0]
    # H-shift at (R=1, c_gap=0): combines chip (1, 0) and chip (1, 1)
    chip = _h_shift(side, R=1, c_gap=0)
    assert chip.shape == (10, CHIP_H, CHIP_W)
    # Left half (cols 0..127) should be value 10 (from chip (1,0))
    assert (chip[:, :, :HALF] == 10).all()
    # Right half (cols 128..255) should be value 11 (from chip (1,1))
    assert (chip[:, :, HALF:] == 11).all()
    print("  _h_shift (R=1, c_gap=0) — OK")


def test_v_shift_geometry():
    composites = _hand_crafted_composites()
    side = composites[0, 0]
    # V-shift at (r_gap=2, C=1): combines chip (2, 1) and chip (3, 1)
    chip = _v_shift(side, r_gap=2, C=1)
    assert chip.shape == (10, CHIP_H, CHIP_W)
    # Top half (rows 0..127) should be value 21 (from chip (2,1))
    assert (chip[:, :HALF, :] == 21).all()
    # Bottom half (rows 128..255) should be value 31 (from chip (3,1))
    assert (chip[:, HALF:, :] == 31).all()
    print("  _v_shift (r_gap=2, C=1) — OK")


def test_diagonal_geometry():
    composites = _hand_crafted_composites()
    side = composites[0, 0]
    # Diagonal at (r_gap=0, c_gap=2): combines chips (0,2), (0,3), (1,2), (1,3)
    chip = _diagonal(side, r_gap=0, c_gap=2)
    assert chip.shape == (10, CHIP_H, CHIP_W)
    # Quadrants:
    #   TL  rows 0..127, cols 0..127     <- chip (0,2)  -> tag 2
    #   TR  rows 0..127, cols 128..255   <- chip (0,3)  -> tag 3
    #   BL  rows 128..255, cols 0..127   <- chip (1,2)  -> tag 12
    #   BR  rows 128..255, cols 128..255 <- chip (1,3)  -> tag 13
    assert (chip[:, :HALF, :HALF]   == 2 ).all(), "TL quadrant wrong"
    assert (chip[:, :HALF, HALF:]   == 3 ).all(), "TR quadrant wrong"
    assert (chip[:, HALF:, :HALF]   == 12).all(), "BL quadrant wrong"
    assert (chip[:, HALF:, HALF:]   == 13).all(), "BR quadrant wrong"
    print("  _diagonal (r_gap=0, c_gap=2) — OK")


def test_generator_yields_64_per_valid_date():
    composites = _hand_crafted_composites(n_dates=2)
    target_dates = np.array([100, 200], dtype=np.int64)
    valid_mask = np.array([True, True], dtype=bool)

    pairs = list(generate_shifted_chips(
        composites, target_dates, valid_mask, verbose=False))

    assert len(pairs) == 64 * 2, f"expected {64 * 2}, got {len(pairs)}"

    kinds_per_date = {0: {}, 1: {}}
    for p in pairs:
        kinds_per_date[p.date_idx].setdefault(p.chip_kind, 0)
        kinds_per_date[p.date_idx][p.chip_kind] += 1

    expected = {"original": 16, "h_shift": 16, "v_shift": 16, "diagonal": 16}
    for k in (0, 1):
        assert kinds_per_date[k] == expected, \
            f"date_idx={k} got {kinds_per_date[k]}, expected {expected}"
    print(f"  generator yields 64 pairs per valid date (2 dates -> 128) — OK")


def test_generator_skips_invalid_dates():
    composites = _hand_crafted_composites(n_dates=3)
    target_dates = np.array([100, 200, 300], dtype=np.int64)
    valid_mask = np.array([True, False, True], dtype=bool)

    pairs = list(generate_shifted_chips(
        composites, target_dates, valid_mask, verbose=False))

    assert len(pairs) == 64 * 2, f"expected {64 * 2}, got {len(pairs)}"
    seen_date_idx = {p.date_idx for p in pairs}
    assert seen_date_idx == {0, 2}, seen_date_idx
    print("  generator skips invalid dates — OK")


def test_generator_metadata_consistency():
    composites = _hand_crafted_composites(n_dates=1)
    target_dates = np.array([12345], dtype=np.int64)
    valid_mask = np.array([True], dtype=bool)

    pairs = list(generate_shifted_chips(
        composites, target_dates, valid_mask, verbose=False))

    # Verify metadata is correctly attached to each pair and that before
    # and after share the same source-chip tag (the synthetic block has
    # identical values for both sides).
    for p in pairs:
        assert isinstance(p, ChipPair)
        assert p.date_idx == 0
        assert p.date_ordinal == 12345
        assert p.before.shape == (10, CHIP_H, CHIP_W)
        assert p.after.shape  == (10, CHIP_H, CHIP_W)
        assert p.before.dtype == np.uint8
        assert p.after.dtype  == np.uint8
        # Before/after carry the same tag in our synthetic data
        assert np.array_equal(p.before, p.after), \
            f"before/after diverged for {p.chip_kind} {p.grid_position}"
    print("  generator metadata + before/after parity — OK")


def test_originals_match_extract_chip():
    composites = _hand_crafted_composites(n_dates=1)
    target_dates = np.array([100], dtype=np.int64)
    valid_mask = np.array([True], dtype=bool)
    side = composites[0, 0]

    pairs = list(generate_shifted_chips(
        composites, target_dates, valid_mask, verbose=False))

    originals = [p for p in pairs if p.chip_kind == "original"]
    assert len(originals) == 16

    seen = set()
    for p in originals:
        r, c = p.grid_position
        assert 0 <= r < LIVE_ROWS and 0 <= c < LIVE_COLS
        seen.add((r, c))
        expected_chip = _extract_chip(side, r, c)
        assert np.array_equal(p.before, expected_chip)
    assert seen == {(r, c) for r in range(LIVE_ROWS) for c in range(LIVE_COLS)}
    print("  originals cover full 4x4 live grid — OK")


def test_h_shift_uses_ghost_col():
    """H-shift at c_gap=3 must pull its right half from chip (R, 4) — the
    ghost column."""
    composites = _hand_crafted_composites()
    side = composites[0, 0]
    # H-shift at (R=2, c_gap=3): combines chip (2, 3) and chip (2, 4)
    chip = _h_shift(side, R=2, c_gap=3)
    # Left half (cols 0..127) should be value 23 (from chip (2,3))
    assert (chip[:, :, :HALF] == 23).all()
    # Right half (cols 128..255) should be value 24 (from chip (2,4) — ghost)
    assert (chip[:, :, HALF:] == 24).all()
    print("  _h_shift uses ghost col 4 at c_gap=3 — OK")


def test_v_shift_uses_ghost_row():
    """V-shift at r_gap=3 must pull its bottom half from chip (4, C) — the
    ghost row."""
    composites = _hand_crafted_composites()
    side = composites[0, 0]
    # V-shift at (r_gap=3, C=1): combines chip (3, 1) and chip (4, 1)
    chip = _v_shift(side, r_gap=3, C=1)
    # Top half (rows 0..127) should be value 31 (from chip (3,1))
    assert (chip[:, :HALF, :] == 31).all()
    # Bottom half (rows 128..255) should be value 41 (from chip (4,1) — ghost)
    assert (chip[:, HALF:, :] == 41).all()
    print("  _v_shift uses ghost row 4 at r_gap=3 — OK")


def test_diagonal_uses_ghost_corner():
    """Diagonal at (r_gap=3, c_gap=3) must pull all four quadrants from the
    bottom-right 2x2 of the block: (3,3), (3,4), (4,3), (4,4)."""
    composites = _hand_crafted_composites()
    side = composites[0, 0]
    chip = _diagonal(side, r_gap=3, c_gap=3)
    # TL quadrant ← chip (3,3) bottom-right quadrant  -> tag 33
    assert (chip[:, :HALF, :HALF] == 33).all(), "TL quadrant wrong"
    # TR quadrant ← chip (3,4) bottom-left quadrant   -> tag 34
    assert (chip[:, :HALF, HALF:] == 34).all(), "TR quadrant wrong"
    # BL quadrant ← chip (4,3) top-right quadrant     -> tag 43
    assert (chip[:, HALF:, :HALF] == 43).all(), "BL quadrant wrong"
    # BR quadrant ← chip (4,4) top-left quadrant      -> tag 44
    assert (chip[:, HALF:, HALF:] == 44).all(), "BR quadrant wrong"
    print("  _diagonal uses ghost corner (4,4) at (r_gap=3, c_gap=3) — OK")


def test_shift_grid_positions_complete():
    """Each shift kind covers its expected 4x4 grid of positions exactly once."""
    composites = _hand_crafted_composites(n_dates=1)
    target_dates = np.array([100], dtype=np.int64)
    valid_mask = np.array([True], dtype=bool)

    pairs = list(generate_shifted_chips(
        composites, target_dates, valid_mask, verbose=False))

    expected_positions = {(r, c) for r in range(LIVE_ROWS)
                                 for c in range(LIVE_COLS)}
    for kind in ("original", "h_shift", "v_shift", "diagonal"):
        positions = {p.grid_position for p in pairs if p.chip_kind == kind}
        assert positions == expected_positions, \
            f"{kind} missing positions: {expected_positions - positions}"
    print("  every shift kind covers full 4x4 grid of positions — OK")


def main():
    print("Running shift_chips tests...")
    test_extract_chip_yields_correct_position_tag()
    test_h_shift_geometry()
    test_v_shift_geometry()
    test_diagonal_geometry()
    test_h_shift_uses_ghost_col()
    test_v_shift_uses_ghost_row()
    test_diagonal_uses_ghost_corner()
    test_generator_yields_64_per_valid_date()
    test_generator_skips_invalid_dates()
    test_generator_metadata_consistency()
    test_originals_match_extract_chip()
    test_shift_grid_positions_complete()
    print("All shift_chips tests passed.")


if __name__ == "__main__":
    main()
