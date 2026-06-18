"""Per-pixel NDVI from before/after composites.

Optional auxiliary output of the prediction pipeline. NDVI = (NIR - Red) /
(NIR + Red), computed from the before/after composites alongside the change
prediction so each pixel can carry its pre/post greenness.

Band order is the native HDF5 order [B2, B3, B4, B5, B6, B7, B8, B8a, B11,
B12], so Red = B4 = index 2 and NIR = B8 = index 6.

The composites are in BLOCK coords (LIVE + ghost ring); NDVI is returned for
the LIVE area only, matching the voted `labels` so the two line up pixel-for-
pixel in the per-block .npz.
"""
from __future__ import annotations

import numpy as np

# Indices into the native band axis (see module docstring).
RED_IDX = 2   # B4
NIR_IDX = 6   # B8

# Block geometry (mirrors hdf5_reader / shift_chips).
LIVE_H = 1024
LIVE_W = 1024
GHOST = 128

# NDVI nodata sentinel — NDVI's valid range is [-1, 1], so a value outside it
# is an unambiguous "no data" marker for the float output.
NDVI_NODATA = np.float32(-9999.0)


def compute_ndvi_composites(composites: np.ndarray,
                            valid_dates_mask: np.ndarray,
                            *,
                            nodata: int,
                            live_h: int = LIVE_H,
                            live_w: int = LIVE_W,
                            ghost: int = GHOST,
                            ) -> tuple[np.ndarray, np.ndarray]:
    """Compute before/after NDVI for the LIVE area of each target date.

    Parameters
    ----------
    composites : (2, D, 10, BLOCK_H, BLOCK_W) uint8 or uint16
        Output of `create_before_after_composites`. [0]=before, [1]=after.
    valid_dates_mask : (D,) bool
        Dates flagged False are written all-NDVI_NODATA (no composite).
    nodata : int
        Input nodata sentinel (255 for uint8, 65535 for uint16). Pixels where
        Red or NIR is nodata get NDVI_NODATA.
    live_h, live_w, ghost : int
        LIVE-area geometry; the ghost ring is cropped off.

    Returns
    -------
    ndvi_before, ndvi_after : (D, live_h, live_w) float32
        Per-pixel NDVI in [-1, 1], or NDVI_NODATA where input was nodata or
        the date was skipped. Aligned to the voted labels' LIVE grid.
    """
    if composites.ndim != 5 or composites.shape[0] != 2:
        raise ValueError(
            f"composites must be (2, D, 10, H, W); got {composites.shape}")
    n_dates = composites.shape[1]

    out = np.full((2, n_dates, live_h, live_w), NDVI_NODATA, dtype=np.float32)
    y0, x0 = ghost, ghost
    y1, x1 = ghost + live_h, ghost + live_w

    for side in (0, 1):
        for k in range(n_dates):
            if not bool(valid_dates_mask[k]):
                continue
            red = composites[side, k, RED_IDX, y0:y1, x0:x1].astype(np.float32)
            nir = composites[side, k, NIR_IDX, y0:y1, x0:x1].astype(np.float32)
            invalid = (red == nodata) | (nir == nodata)
            denom = nir + red
            # Avoid divide-by-zero: where denom == 0 the pixel is invalid too.
            invalid |= (denom == 0)
            safe_denom = np.where(denom == 0, 1.0, denom)
            ndvi = (nir - red) / safe_denom
            ndvi[invalid] = NDVI_NODATA
            out[side, k] = ndvi

    return out[0], out[1]
