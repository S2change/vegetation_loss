"""Dummy NumPy arrays for testing the chip-chunked prediction pipeline.

Provides:
  - `make_chip_batch` / `make_before_after`: dummy (B, H, W, C) batches that
    match `predict_before_after_chips`'s input contract. Used to exercise
    step 4 of the pipeline (model inference) in isolation.
  - `make_chip_block`: dummy (N_TS, 10, n_chips * 65_536) uint8 block matching
    the output of step 2 (`to_uint8`), plus a paired `ts` ordinal-date array.
    Used to exercise steps 3 (`create_before_after_composites`) and 4
    (`generate_shifted_chips`).
"""
from datetime import date

import numpy as np

# Model input contract — see pybacdm/hpc_incd/bacdm/predict.py
H, W, C = 256, 256, 10
NODATA = 65535          # uint16 NoData sentinel that _to_uint8 maps to 255
S2_REFLECTANCE_MAX = 10_000   # plausible S2 L2A surface-reflectance ceiling

# Chip-block contract — see composite_shift_chips/composite.py
CHIP_PIXELS = 65_536          # 256 * 256 per chip
NODATA_U8 = 255               # uint8 NoData sentinel after _to_uint8 stretch


def make_chip_batch(batch_size: int = 2, *, nodata_frac: float = 0.02,
                    seed: int | None = 0) -> np.ndarray:
    """Return a (B, H, W, C) uint16 dummy chip batch.

    Pixels are sampled in a plausible S2 surface-reflectance range, with
    `nodata_frac` of pixels (whole-pixel mask across all C bands) set to
    NODATA=65535 so the `_to_uint8` percentile-stretch path is exercised.
    """
    rng = np.random.default_rng(seed)
    arr = rng.integers(low=200, high=4000, size=(batch_size, H, W, C),
                       dtype=np.uint16)
    if nodata_frac > 0:
        mask = rng.random(size=(batch_size, H, W)) < nodata_frac
        arr[mask] = NODATA
    return arr


def make_before_after(batch_size: int = 2, *, nodata_frac: float = 0.02,
                      seed: int | None = 0) -> tuple[np.ndarray, np.ndarray]:
    """Return a (before, after) pair of dummy chip batches.

    Both have shape (B, H, W, C) uint16. Uses `seed` for `before` and
    `seed + 1` for `after` so the two batches are different but the pair
    is reproducible from one input seed.
    """
    before = make_chip_batch(batch_size, nodata_frac=nodata_frac, seed=seed)
    after  = make_chip_batch(batch_size, nodata_frac=nodata_frac,
                             seed=None if seed is None else seed + 1)
    return before, after


def make_chip_block(n_ts: int = 48, n_chips: int = 25, *,
                    nodata_frac: float = 0.05,
                    revisit_days: int = 5,
                    start_date: tuple[int, int, int] = (2024, 1, 1),
                    seed: int | None = 0) -> tuple[np.ndarray, np.ndarray]:
    """Return a (block, ts) pair simulating step-2 output for steps 3-4.

    Parameters
    ----------
    n_ts : int
        Number of timesteps along the first axis.
    n_chips : int
        Number of chips packed end-to-end along the flat pixel axis.
        25 corresponds to the 5x5 block from the pipeline flow chart.
    nodata_frac : float
        Fraction of (timestep, pixel) cells set to NODATA_U8 (whole-pixel
        mask broadcast across all 10 bands) so the cascading-selection
        validity branch is exercised.
    revisit_days : int
        Days between consecutive timesteps. Matches Sentinel-2's nominal
        ~5-day revisit at most latitudes.
    start_date : tuple
        (year, month, day) of timestep 0.
    seed : int or None
        Seed for the numpy default_rng.

    Returns
    -------
    block : np.ndarray
        Shape (n_ts, 10, n_chips * 65_536), dtype uint8. Values in
        [1, 254] for valid pixels; NODATA_U8 (255) where a (t, pixel) cell
        was nodata-masked. Band 0 alone is sufficient to detect nodata.
    ts : np.ndarray
        Shape (n_ts,), dtype int64. Ordinal dates, ascending.
    """
    rng = np.random.default_rng(seed)
    n_pixels = n_chips * CHIP_PIXELS
    block = rng.integers(low=1, high=NODATA_U8, size=(n_ts, 10, n_pixels),
                         dtype=np.uint8)

    if nodata_frac > 0:
        mask = rng.random(size=(n_ts, n_pixels)) < nodata_frac
        block[:, :, :][mask[:, None, :].repeat(10, axis=1)] = NODATA_U8

    start_ord = date(*start_date).toordinal()
    ts = np.arange(start_ord, start_ord + n_ts * revisit_days, revisit_days,
                   dtype=np.int64)
    return block, ts


if __name__ == "__main__":
    print("== make_before_after ==")
    before, after = make_before_after(batch_size=2)
    for name, arr in [("before", before), ("after", after)]:
        n_nodata = int((arr == NODATA).any(axis=-1).sum())
        print(f"  {name}: shape={arr.shape}  dtype={arr.dtype}  "
              f"min={arr.min()}  max={arr.max()}  nodata_pixels={n_nodata}")

    print("\n== make_chip_block ==")
    block, ts = make_chip_block(n_ts=10, n_chips=4, nodata_frac=0.05)
    n_nodata = int((block[:, 0, :] == NODATA_U8).sum())
    print(f"  block: shape={block.shape}  dtype={block.dtype}  "
          f"min={block.min()}  max={block.max()}  "
          f"nodata_cells (t,pixel)={n_nodata} of {block.shape[0] * block.shape[2]:,}")
    print(f"  ts:    shape={ts.shape}  dtype={ts.dtype}  "
          f"first={date.fromordinal(int(ts[0]))}  "
          f"last={date.fromordinal(int(ts[-1]))}")
