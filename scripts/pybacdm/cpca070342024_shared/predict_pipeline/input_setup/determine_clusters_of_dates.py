"""
determine_clusters_of_dates.py

Groups Sentinel-2 acquisition dates from an HDF5 file into temporal clusters
and returns the cluster boundaries (mid-dates) together with the cluster
members and their median dates. Also provides the block-level aggregation
and (de)serialization helpers the prediction pipeline uses to turn those
clusters into a temporal summary.

Public API
----------
determine_clusters_of_dates(path_to_hdf5, ts_start_ordinal, ts_end_ordinal)
    -> (list_of_potential_change_dates_ordinal,   # cluster-gap midpoints
        list_of_date_clusters_ordinal,            # member dates per cluster
        list_of_median_dates_ordinal)             # median date per cluster

aggregate_block_dates(block, ts_kept, position, list_of_date_clusters_ordinal,
                      nodata=None)
    -> (block_out, ts_out, position)              # one min-composite/cluster

serialize_date_clusters(clusters) -> str          # for the DATE_CLUSTERS env
parse_date_clusters(str) -> list[list[int]]       # inverse

Clustering algorithm
--------------------
Single-link agglomerative clustering with an increasing gap threshold:

  1. Filter the file's `ts` ordinals to [ts_start_ordinal, ts_end_ordinal]
     (None on either side = unbounded) and sort. Each date starts as its
     own cluster.
  2. For theta = 1, 2, ..., MAX_THETA (each pass over the current clusters):
     merge two consecutive (sorted) clusters when the gap between them is
     ≤ theta days AND the merged span stays ≤ MAX_CLUSTER_AMPLITUDE days.
     The amplitude cap stops a long chain of near-dates from snowballing
     into one cluster spanning months.

The loop runs all MAX_THETA passes unconditionally; there is no per-cluster
"keep the clearest date" selection and no target-count stopping rule — every
acquisition in the window stays, just grouped. The change dates are the
midpoints between adjacent clusters: (cluster[k].last + cluster[k+1].first)//2.

Usage (standalone):
    python determine_clusters_of_dates.py <tile.h5> [--start YYYY-MM-DD]
        [--end YYYY-MM-DD] [--for-submit]
    # --for-submit prints two lines (change dates; serialized clusters) for
    # submit_tile.sh's USE_DATE_CLUSTERS path; default prints a readable
    # cluster summary.
"""

import h5py
import numpy as np
from datetime import date as _date

# ── Clustering parameters ─────────────────────────────────────────────────────
MAX_THETA             = 10   # maximum gap (days) for single-link merging
MAX_CLUSTER_AMPLITUDE = 15   # a cluster must not span more than this many days


def determine_clusters_of_dates(path_to_hdf5, ts_start_ordinal, ts_end_ordinal):
    """
    Read acquisition dates from an HDF5 file, filter to the requested window,
    and group them into temporal clusters by iterative single-link clustering.

    Parameters
    ----------
    path_to_hdf5     : str or Path
    ts_start_ordinal : int or None — inclusive lower bound (date.toordinal())
    ts_end_ordinal   : int or None — inclusive upper bound

    Returns
    -------
    list_of_potential_change_dates_ordinal : list of int, length N-1
        Mid-date (ordinal) between the latest date of cluster k and the
        earliest date of cluster k+1 — natural candidate dates for change
        detection.
    list_of_date_clusters_ordinal : list of list of int, length N
        Each sub-list holds the ordinal dates of all acquisitions in that
        cluster, sorted chronologically.
    list_of_median_dates_ordinal : list of int, length N
        Median ordinal date of each cluster.
    """
    with h5py.File(path_to_hdf5, "r") as f:
        ordinals = np.asarray(f["ts"]).astype(int)

    # Filter to window and sort
    mask = np.ones(len(ordinals), dtype=bool)
    if ts_start_ordinal is not None:
        mask &= ordinals >= ts_start_ordinal
    if ts_end_ordinal is not None:
        mask &= ordinals <= ts_end_ordinal

    window_ordinals = sorted(int(o) for o in ordinals[mask])

    # Start: each date is its own cluster
    clusters = [[o] for o in window_ordinals]

    for theta in range(1, MAX_THETA + 1):
        merged, current = [], clusters[0]
        for j in range(1, len(clusters)):
            gap            = clusters[j][0] - current[-1]
            span_if_merged = clusters[j][-1] - current[0]
            amplitude_ok   = (MAX_CLUSTER_AMPLITUDE is None or
                              span_if_merged <= MAX_CLUSTER_AMPLITUDE)
            if gap <= theta and amplitude_ok:
                current = current + clusters[j]
            else:
                merged.append(current)
                current = clusters[j]
        merged.append(current)
        clusters = merged

    list_of_date_clusters_ordinal = clusters
    list_of_median_dates_ordinal  = [int(np.median(cl)) for cl in clusters]
    list_of_potential_change_dates_ordinal = [
        (clusters[k][-1] + clusters[k + 1][0]) // 2
        for k in range(len(clusters) - 1)
    ]

    return (list_of_potential_change_dates_ordinal,
            list_of_date_clusters_ordinal,
            list_of_median_dates_ordinal)


def aggregate_block_dates(block, ts_kept, position,
                          list_of_date_clusters_ordinal,
                          nodata=None):
    """Collapse a block's time axis into one min-composite per date cluster.

    Each cluster of acquisition dates is reduced to a single timestep by
    taking the per-pixel, per-band MINIMUM across the cluster's timesteps,
    ignoring nodata. The resulting block has one timestep per cluster and
    `ts_kept` becomes the clusters' median dates — a compact temporal
    summary that downstream compositing/prediction can treat like any block.

    Why min: for optical reflectance a per-pixel minimum is a cheap, robust
    cloud/haze suppressor — clouds, haze and most sensor artefacts raise
    reflectance, so the darkest observation in a short window is usually the
    clearest ground view. (This mirrors the B2-based "lowest is cleanest"
    intuition the compositor's validity heuristic uses.)

    Parameters
    ----------
    block : (N_TS, 10, H, W) uint8 or uint16
        A block from `hdf5_reader.read_block`. dtype is preserved.
    ts_kept : (N_TS,) int
        Ordinal dates aligned to block's axis 0 (read_block's second return).
    position : BlockPosition
        Returned unchanged — aggregation is purely along the time axis, so the
        block's geometry/world origin do not change.
    list_of_date_clusters_ordinal : list[list[int]]
        Clusters of ordinal dates (e.g. from `determine_clusters_of_dates`).
        Every date in every cluster must be present in `ts_kept`. Clusters are
        emitted in the given order; output timesteps are then sorted by median
        date so axis 0 stays chronological.
    nodata : int or None
        nodata sentinel. None (default) infers it from dtype: 255 for uint8,
        65535 for uint16. Pixels that are nodata across ALL of a cluster's
        timesteps stay nodata in that cluster's composite.

    Returns
    -------
    block_out : (N_CLUSTERS, 10, H, W) — same dtype as `block`
        One min-composite per cluster.
    ts_out : (N_CLUSTERS,) int64
        Median ordinal date of each cluster, chronologically sorted and
        aligned to block_out's axis 0.
    position : BlockPosition
        The same object passed in (unchanged).
    """
    block = np.asarray(block)
    if block.ndim != 4:
        raise ValueError(
            f"block must be (N_TS, 10, H, W); got shape {block.shape}")
    if ts_kept.shape != (block.shape[0],):
        raise ValueError(
            f"ts_kept shape {ts_kept.shape} must equal (N_TS,) = "
            f"({block.shape[0]},)")

    if nodata is None:
        nodata = 65535 if block.dtype == np.uint16 else 255

    # Map each ordinal date -> its axis-0 index in the block. Dates can repeat
    # in principle; take the first occurrence.
    ts_list = [int(t) for t in ts_kept]
    date_to_idx = {}
    for i, t in enumerate(ts_list):
        date_to_idx.setdefault(t, i)

    composites = []   # (10, H, W) per cluster
    medians = []      # ordinal median per cluster
    for cluster in list_of_date_clusters_ordinal:
        if len(cluster) == 0:
            raise ValueError("empty cluster in list_of_date_clusters_ordinal")
        try:
            idxs = [date_to_idx[int(d)] for d in cluster]
        except KeyError as exc:
            raise ValueError(
                f"cluster date {exc.args[0]} "
                f"({_date.fromordinal(int(exc.args[0]))}) is not in ts_kept"
            ) from None

        # Per-pixel, per-band min across the cluster's timesteps, ignoring
        # nodata. nodata is the dtype's max sentinel (255 / 65535), so a plain
        # min already prefers any real value over nodata, and collapses to
        # nodata only where every timestep is nodata. Promote to int for the
        # masked-min so the sentinel arithmetic can't wrap on uint.
        sub = block[idxs].astype(np.int64)          # (n, 10, H, W)
        sub[sub == nodata] = np.iinfo(np.int64).max  # mask nodata out of min
        mins = sub.min(axis=0)                       # (10, H, W)
        all_nodata = mins == np.iinfo(np.int64).max
        mins[all_nodata] = nodata
        composites.append(mins.astype(block.dtype))

        medians.append(int(np.median(cluster)))

    # Sort clusters chronologically by median date so axis 0 is ordered.
    order = np.argsort(medians)
    block_out = np.stack([composites[i] for i in order], axis=0)
    ts_out = np.array([medians[i] for i in order], dtype=np.int64)

    return block_out, ts_out, position


# ── Serialization for the pipeline (submit_tile.sh <-> predict_block.py) ──────
# Clusters travel between processes as an env var. Format: clusters separated
# by ';', dates within a cluster by ',', each date ISO (YYYY-MM-DD). e.g.
#   2023-01-01,2023-01-03;2023-02-10,2023-02-12
# parse_date_clusters() is the inverse, returning list[list[int ordinal]].

def serialize_date_clusters(list_of_date_clusters_ordinal) -> str:
    """Serialize clusters of ordinal dates to the env-var string form."""
    return ";".join(
        ",".join(_date.fromordinal(int(o)).isoformat() for o in cluster)
        for cluster in list_of_date_clusters_ordinal
    )


def parse_date_clusters(s: str):
    """Inverse of serialize_date_clusters: env-var string -> list[list[int]]."""
    s = (s or "").strip()
    if not s:
        return []
    return [
        [_date.fromisoformat(d).toordinal() for d in group.split(",") if d]
        for group in s.split(";") if group
    ]


def _main(argv=None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Cluster a tile's acquisition dates and emit the "
                    "cluster-gap change dates + the cluster membership.",
    )
    parser.add_argument("hdf5_path", help="chip-chunked tile HDF5")
    parser.add_argument("--start", default=None,
                        help="inclusive window start, YYYY-MM-DD")
    parser.add_argument("--end", default=None,
                        help="inclusive window end, YYYY-MM-DD")
    parser.add_argument(
        "--for-submit", action="store_true",
        help="machine-readable output for submit_tile.sh: line 1 = "
             "comma-separated change dates (TARGET_DATES), line 2 = "
             "serialized clusters (DATE_CLUSTERS). No other output on stdout.",
    )
    args = parser.parse_args(argv)

    ts_start = _date.fromisoformat(args.start).toordinal() if args.start else None
    ts_end   = _date.fromisoformat(args.end).toordinal() if args.end else None

    change_dates, clusters, medians = determine_clusters_of_dates(
        args.hdf5_path, ts_start, ts_end
    )

    if args.for_submit:
        # Exactly two lines; nothing else on stdout so the shell can capture
        # them cleanly via `read`. Diagnostics (if any) go to stderr.
        print(",".join(_date.fromordinal(int(d)).isoformat() for d in change_dates))
        print(serialize_date_clusters(clusters))
        return 0

    print(f"N clusters : {len(clusters)}")
    print(f"N mid-dates: {len(change_dates)}\n")
    for k, cl in enumerate(clusters):
        mid_str = _date.fromordinal(change_dates[k - 1]).isoformat() if k > 0 else "-"
        med_str = _date.fromordinal(medians[k]).isoformat()
        cl_str  = ", ".join(_date.fromordinal(o).isoformat() for o in cl)
        print(f"  mid={mid_str}  cluster {k+1:>2} (size {len(cl):>2}, median {med_str}): {cl_str}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
