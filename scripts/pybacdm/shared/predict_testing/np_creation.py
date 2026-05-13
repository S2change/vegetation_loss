"""Dummy NumPy arrays for testing model inference in isolation.

Stage 4 of the planned chip-chunked prediction pipeline takes a batch of
(before, after) chip pairs as input. This module fabricates dummy batches
that match that contract — without HDF5 reads, percentile stretches, or
compositing — so the inference step can be exercised standalone.

Later iterations will add helpers for the earlier pipeline stages.
"""
import numpy as np

# Model input contract — see pybacdm/hpc_incd/bacdm/predict.py
H, W, C = 256, 256, 10
NODATA = 65535          # uint16 NoData sentinel that _to_uint8 maps to 255
S2_REFLECTANCE_MAX = 10_000   # plausible S2 L2A surface-reflectance ceiling


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


if __name__ == "__main__":
    before, after = make_before_after(batch_size=2)
    for name, arr in [("before", before), ("after", after)]:
        n_nodata = int((arr == NODATA).any(axis=-1).sum())
        print(f"{name}: shape={arr.shape}  dtype={arr.dtype}  "
              f"min={arr.min()}  max={arr.max()}  nodata_pixels={n_nodata}")
