"""Per-pixel before/after compositing across a chip-block timestep axis.

Step 3 of the chip-chunked prediction pipeline. Takes the uint8 chip-block
produced by step 2 and, for each requested target date Dk, builds:

  - `before[k, pixel]` = most-recent non-nodata observation at any timestep
                        strictly before Dk
  - `after[k, pixel]`  = oldest    non-nodata observation at any timestep
                        strictly after  Dk

Per-pixel validity is judged from a single band (default band 0) to keep the
work cheap and to mirror the legacy `cascading_selection_optimized` helper
in scripts/utils/bacdm_utils/chip_creation.py.

If a target date has no valid timesteps on one (or both) sides, the date is
skipped with a warning explaining the reason; its slot in the output array
is left filled with NODATA_U8.
"""
from datetime import date

import numpy as np

NODATA_U8 = 255
SELECTION_BAND_IDX_DEFAULT = 0


def cascading_select_flat(block_subset: np.ndarray,
                          ordinals_sorted: np.ndarray,
                          selection_band_idx: int = SELECTION_BAND_IDX_DEFAULT,
                          nodata: int = NODATA_U8,
                          ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Pick, per pixel, the first non-nodata timestep along axis 0.

    The caller is responsible for sorting `block_subset` along axis 0 in the
    order they want "first" to mean — descending date order for a "most
    recent" pick, ascending for "oldest".

    Parameters
    ----------
    block_subset : (N, 10, P) uint8
        A subset of the chip block restricted to the timesteps the caller
        wants to consider (e.g., only ts < target_date for a before
        composite), sorted along axis 0 in the desired "first" order.
    ordinals_sorted : (N,) int
        Ordinal dates aligned with axis 0 of block_subset (same sort order).
    selection_band_idx : int
        Index of the band used to detect nodata. Default 0.
    nodata : int
        The uint8 nodata sentinel (255).

    Returns
    -------
    selected : (10, P) uint8
        Per-pixel picked values. Pixels with no valid timestep are filled
        with `nodata`.
    timestamps : (P,) int
        Per-pixel ordinal date of the picked timestep, or `nodata` if none.
    any_valid : (P,) bool
        Per-pixel boolean: True if any timestep was valid (sanity flag for
        callers that want it).
    """
    if block_subset.shape[0] == 0:
        # No timesteps available; entire output is nodata.
        P = block_subset.shape[2]
        return (np.full((10, P), nodata, dtype=np.uint8),
                np.full((P,), nodata, dtype=np.int64),
                np.zeros((P,), dtype=bool))

    # Boolean validity mask along time axis using only the selection band.
    valid_mask = block_subset[:, selection_band_idx, :] != nodata  # (N, P)
    any_valid = valid_mask.any(axis=0)                              # (P,)
    # argmax on a boolean returns the index of the first True; if a column
    # is all-False, argmax returns 0 (we mask those out below).
    first_idx = valid_mask.argmax(axis=0)                           # (P,)

    # Gather the selected pixel values across all 10 bands at once. Using
    # advanced indexing: take block_subset[first_idx[p], :, p] for every p.
    pixel_idx = np.arange(block_subset.shape[2])
    selected = block_subset[first_idx, :, pixel_idx].T              # (10, P)

    timestamps = ordinals_sorted[first_idx].astype(np.int64)        # (P,)

    # Wipe pixels with no valid timestep.
    selected[:, ~any_valid] = nodata
    timestamps[~any_valid] = nodata

    return selected, timestamps, any_valid


def create_before_after_composites(block: np.ndarray,
                                   ts: np.ndarray,
                                   target_dates: np.ndarray,
                                   selection_band_idx: int = SELECTION_BAND_IDX_DEFAULT,
                                   nodata: int = NODATA_U8,
                                   verbose: bool = True,
                                   ) -> tuple[np.ndarray, np.ndarray]:
    """Build before/after composites for each requested target date.

    Parameters
    ----------
    block : (N_TS, 10, P) uint8
        The chip-block output of step 2 (`to_uint8`).
    ts : (N_TS,) int
        Ordinal dates aligned to block's axis 0.
    target_dates : (D,) int
        Ordinal dates to build composites for.
    selection_band_idx : int
        Band used for nodata detection. Default 0.
    nodata : int
        uint8 nodata sentinel. Default 255.
    verbose : bool
        Print a one-line warning per skipped target date.

    Returns
    -------
    composites : (2, D, 10, P) uint8
        composites[0, k] = before composite for target_dates[k].
        composites[1, k] = after  composite for target_dates[k].
        Slots for skipped dates are left filled with `nodata`.
    valid_dates_mask : (D,) bool
        True where both before and after had at least one timestep in
        range AND the target date itself fell inside ts's span.
    """
    if block.ndim != 3 or block.shape[1] != 10:
        raise ValueError(
            f"block must have shape (N_TS, 10, P); got {block.shape}")
    if ts.shape != (block.shape[0],):
        raise ValueError(
            f"ts shape {ts.shape} must equal (N_TS,) = ({block.shape[0]},)")

    n_ts = block.shape[0]
    n_pixels = block.shape[2]
    n_target = len(target_dates)

    composites = np.full((2, n_target, 10, n_pixels), nodata, dtype=np.uint8)
    valid_dates_mask = np.zeros(n_target, dtype=bool)

    ts_min, ts_max = int(ts.min()), int(ts.max())

    for k, target in enumerate(target_dates):
        target = int(target)

        # Check if the target date falls anywhere inside the data range.
        if target < ts_min or target > ts_max:
            if verbose:
                print(
                    f"[warn] Target date {date.fromordinal(target)} skipped: "
                    f"outside the data range (data spans "
                    f"{date.fromordinal(ts_min)} to {date.fromordinal(ts_max)})."
                )
            continue

        # Strict before/after split.
        pre_mask  = ts < target
        post_mask = ts > target

        if not pre_mask.any():
            if verbose:
                print(
                    f"[warn] Target date {date.fromordinal(target)} skipped: "
                    f"no valid pre-date timesteps (need ts < target_date strictly)."
                )
            continue
        if not post_mask.any():
            if verbose:
                print(
                    f"[warn] Target date {date.fromordinal(target)} skipped: "
                    f"no valid post-date timesteps (need ts > target_date strictly)."
                )
            continue

        # Slice block to the pre/post timesteps, then sort axis 0 so the
        # cascading-select "first valid" pick yields the right semantics:
        #   before -> most recent first (descending)
        #   after  -> oldest first      (ascending)
        pre_idx  = np.where(pre_mask)[0]
        post_idx = np.where(post_mask)[0]

        pre_idx_sorted  = pre_idx[np.argsort(ts[pre_idx])[::-1]]
        post_idx_sorted = post_idx[np.argsort(ts[post_idx])]

        before_sel, _, before_any = cascading_select_flat(
            block[pre_idx_sorted], ts[pre_idx_sorted],
            selection_band_idx=selection_band_idx, nodata=nodata,
        )
        after_sel, _, after_any = cascading_select_flat(
            block[post_idx_sorted], ts[post_idx_sorted],
            selection_band_idx=selection_band_idx, nodata=nodata,
        )

        composites[0, k] = before_sel
        composites[1, k] = after_sel
        valid_dates_mask[k] = True

        # Note: `before_any` / `after_any` track per-pixel validity within
        # the per-side cascade. A target date can be "valid" (we ran the
        # cascade) yet still have many fully-nodata pixels — that's fine,
        # those pixels will be model-input nodata and treated as background.
        _ = before_any  # kept for readability; not used downstream
        _ = after_any

    return composites, valid_dates_mask
