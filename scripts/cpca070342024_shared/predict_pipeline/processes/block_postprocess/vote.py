"""Pixel-level voting across overlapping shifted chips.

Every LIVE-area pixel in a block is covered by exactly 4 shifted chips
(one per shift kind: original, h_shift, v_shift, diagonal) thanks to the
4-sided ghost border. This module accumulates per-class vote counts as
chip predictions stream out of the inference loop, then collapses them
into a single label map per target date.

Used in step 5b of the chip-chunked prediction pipeline — between step 5
(per-chip encoding, optional) and step 6 (output write).

Inline-friendly design: one `VoteAccumulator` per (block, target_date),
fed one `(256, 256) uint8` label map at a time via `add()`. No buffering
of the 81 predictions per date. `finalize()` returns the voted
`(LIVE_H, LIVE_W) uint8` label map.

Threshold rule: at each LIVE pixel, take the argmax over non-background
class vote counts. If that max count is below `threshold`, output 0
(background). Class-0 votes are never tracked (no array allocated for
them). Ties between non-bg classes resolve to the lowest class ID (numpy
argmax default).
"""
from __future__ import annotations

from typing import Iterable

import numpy as np

# ============================================================================
# CONFIGURATION
# ============================================================================

CHIP_H = 256
CHIP_W = 256

# Default LIVE area for one block — 4x4 chips, 1024x1024 px. Override via
# `VoteAccumulator(live_h=..., live_w=...)` if you ever shift block geometry.
LIVE_H = 1024
LIVE_W = 1024

# Default vote threshold: keep pixel only if its winning non-bg class got
# at least this many votes. Tunable per-run from run_predict.py.
DEFAULT_THRESHOLD = 2

# Background class index — never tracked in the vote counter.
BACKGROUND_CLASS = 0


# ============================================================================
# VOTE ACCUMULATOR
# ============================================================================

class VoteAccumulator:
    """Streaming per-class vote counter for one block + one target date.

    Holds a `(n_nonbg_classes, LIVE_H, LIVE_W) uint8` array. Each `add()`
    call increments the counter at the chip's LIVE-area footprint (clipped
    for chips that extend into the ghost ring).

    Memory: `n_nonbg_classes * LIVE_H * LIVE_W` bytes. For 2 non-bg classes
    on a 1024x1024 live area that's 2 MB per accumulator.

    Vote counts are uint8 — every live pixel gets exactly 4 votes in the
    current geometry, so the counter maxes at 4. uint8 has plenty of room
    even if a future geometry change pushes that higher.
    """
    __slots__ = ("classes", "_class_to_idx", "live_h", "live_w", "votes",
                 "track_confidence", "prob_sum")

    def __init__(self,
                 classes: Iterable[int],
                 *,
                 live_h: int = LIVE_H,
                 live_w: int = LIVE_W,
                 track_confidence: bool = False,
                 ) -> None:
        """Parameters
        ----------
        classes : iterable of int
            Non-background class IDs to track (e.g. (1, 2) for Cuts +
            Fires). Background votes are dropped on the floor.
        live_h, live_w : int
            Dimensions of the LIVE area within this block. Defaults to
            1024x1024 (4x4 chips of 256x256).
        track_confidence : bool (default False)
            When True, also accumulate a per-class sum of chip confidences
            (`prob_sum`, uint16) in lockstep with the vote counts, so
            `finalize` can emit a per-pixel mean confidence. Off by default
            so the accumulator stays count-only (2 MB for 2 classes).
        """
        cls_list = sorted({int(c) for c in classes if int(c) != BACKGROUND_CLASS})
        if not cls_list:
            raise ValueError(
                "VoteAccumulator needs at least one non-background class"
            )
        self.classes = tuple(cls_list)
        self._class_to_idx = {c: i for i, c in enumerate(self.classes)}
        self.live_h = int(live_h)
        self.live_w = int(live_w)
        self.votes = np.zeros(
            (len(self.classes), self.live_h, self.live_w), dtype=np.uint8,
        )
        # Optional confidence tracking: per-class running sum of chip
        # confidences (0–100 each). uint16 — up to 4 votes × 100 = 400, which
        # overflows uint8, so a wider type is required.
        self.track_confidence = bool(track_confidence)
        self.prob_sum = (
            np.zeros((len(self.classes), self.live_h, self.live_w),
                     dtype=np.uint16)
            if self.track_confidence else None
        )

    def add(self,
            label_map: np.ndarray,
            chip_nw_px_y: int,
            chip_nw_px_x: int,
            prob_map: np.ndarray | None = None,
            ) -> None:
        """Increment vote counts at the chip's LIVE-area footprint.

        Parameters
        ----------
        label_map : (CHIP_H, CHIP_W) uint8
            One chip's predicted class labels (background = 0).
        chip_nw_px_y, chip_nw_px_x : int
            Pixel offset of the chip's NW corner relative to the LIVE
            area's NW corner. Can be negative (ghost-using shifts).
        prob_map : (CHIP_H, CHIP_W) uint8 or None
            One chip's per-pixel confidence (0–100). Required when the
            accumulator was built with track_confidence=True; accumulated
            into `prob_sum` using the SAME per-class mask as the vote count,
            so a class's prob_sum/votes is the mean confidence over exactly
            the chips that voted that class. Ignored when not tracking.

        Pixels outside [0, live_h) x [0, live_w) are silently dropped —
        chips that extend into the ghost ring only contribute votes for
        the portion that overlaps the LIVE area.
        """
        if label_map.shape != (CHIP_H, CHIP_W):
            raise ValueError(
                f"label_map shape {label_map.shape} must equal ({CHIP_H}, {CHIP_W})"
            )
        if self.track_confidence:
            if prob_map is None:
                raise ValueError(
                    "add() needs prob_map when track_confidence=True"
                )
            if prob_map.shape != (CHIP_H, CHIP_W):
                raise ValueError(
                    f"prob_map shape {prob_map.shape} must equal "
                    f"({CHIP_H}, {CHIP_W})"
                )

        # Compute the LIVE-area window this chip covers, and the matching
        # chip-local window. Negative chip_nw_* clips the chip-local start;
        # chip_nw_* + CHIP_H/W past the LIVE edge clips the chip-local end.
        live_y0 = max(0, chip_nw_px_y)
        live_x0 = max(0, chip_nw_px_x)
        live_y1 = min(self.live_h, chip_nw_px_y + CHIP_H)
        live_x1 = min(self.live_w, chip_nw_px_x + CHIP_W)
        if live_y0 >= live_y1 or live_x0 >= live_x1:
            return  # chip is entirely outside the LIVE area

        chip_y0 = live_y0 - chip_nw_px_y
        chip_x0 = live_x0 - chip_nw_px_x
        chip_y1 = chip_y0 + (live_y1 - live_y0)
        chip_x1 = chip_x0 + (live_x1 - live_x0)

        chip_window = label_map[chip_y0:chip_y1, chip_x0:chip_x1]
        prob_window = (
            prob_map[chip_y0:chip_y1, chip_x0:chip_x1]
            if self.track_confidence else None
        )

        # Per-class boolean masks → uint8 increment. Skipping bg here
        # means bg label pixels contribute nothing to any counter, which
        # is the whole point of not allocating a bg channel.
        for cls, idx in self._class_to_idx.items():
            mask = (chip_window == cls)
            if mask.any():
                self.votes[idx, live_y0:live_y1, live_x0:live_x1] += mask
                if self.track_confidence:
                    # Accumulate this chip's confidence only where it voted
                    # `cls` — same mask as the count — so prob_sum/votes is the
                    # mean confidence over the chips that voted that class.
                    self.prob_sum[idx, live_y0:live_y1, live_x0:live_x1] += (
                        prob_window.astype(np.uint16) * mask
                    )

    def finalize(self,
                 threshold: int = DEFAULT_THRESHOLD,
                 return_confidence: bool = False,
                 ):
        """Collapse vote counts into a single `(live_h, live_w) uint8` label.

        At each pixel: argmax over class channels; if the max count is
        below `threshold`, output 0 (background). Otherwise output the
        winning class's original ID.

        Ties between non-bg classes resolve to the lowest class ID (numpy's
        argmax default). This is a tiny minority of pixels in practice
        (sparse positives + agreement filter); a tiebreak by class ID is
        as defensible as anything else.

        return_confidence : bool (default False)
            When True (requires track_confidence at construction), also return
            a per-pixel confidence map: for each surviving pixel, the mean
            confidence over the chips that voted the winning class
            (prob_sum[winner] / votes[winner]), as uint8 0–100. 255 where
            there is no detection (below threshold / background).
        """
        if threshold < 1:
            raise ValueError(f"threshold must be >= 1, got {threshold}")

        # winners[y, x] = index into self.classes of the class with the
        # most votes at that pixel.
        winners = np.argmax(self.votes, axis=0)
        max_counts = np.take_along_axis(
            self.votes, winners[None, :, :], axis=0
        )[0]

        # Map winner-index -> class ID via a small lookup table.
        # 0 is reserved as "no detection" output; class IDs are >= 1.
        lut = np.array((0,) + self.classes, dtype=np.uint8)
        # winners + 1 because lut[0] is the "no detection" slot.
        out = lut[winners + 1]
        kept = max_counts >= threshold
        out[~kept] = 0
        if not return_confidence:
            return out

        if not self.track_confidence:
            raise ValueError(
                "finalize(return_confidence=True) needs track_confidence=True"
            )
        # Mean confidence over the winning class's votes at each surviving
        # pixel: prob_sum[winner] / votes[winner]. 255 (nodata) elsewhere.
        winner_prob = np.take_along_axis(
            self.prob_sum, winners[None, :, :], axis=0
        )[0].astype(np.float32)
        winner_votes = max_counts.astype(np.float32)
        confidence = np.full(out.shape, 255, dtype=np.uint8)
        with np.errstate(invalid="ignore", divide="ignore"):
            mean_conf = np.where(kept, winner_prob / np.maximum(winner_votes, 1), 0.0)
        confidence[kept] = np.rint(mean_conf[kept]).clip(0, 100).astype(np.uint8)
        return out, confidence

    def n_votes_by_class(self) -> dict[int, int]:
        """Sum of vote counts per class, across all LIVE pixels.

        Useful as a cheap sanity check before/after finalize() — total
        votes should equal `4 * (LIVE_H * LIVE_W)` minus the background
        votes (which aren't tracked).
        """
        return {cls: int(self.votes[idx].sum())
                for cls, idx in self._class_to_idx.items()}
