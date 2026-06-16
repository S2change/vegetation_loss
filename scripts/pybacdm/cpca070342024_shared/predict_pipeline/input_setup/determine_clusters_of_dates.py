"""
determine_clusters_of_dates.py

Groups Sentinel-2 acquisition dates from an HDF5 file into temporal clusters
and returns the cluster boundaries (mid-dates) together with the cluster
members and their median dates.

Public API
----------
determine_clusters_of_dates(path_to_hdf5, ts_start_ordinal, ts_end_ordinal)
    -> (list_of_potential_change_dates_ordinal,
        list_of_date_clusters_ordinal,
        list_of_median_dates_ordinal)

Clustering rules
----------------
For theta = 1, 2, ..., MAX_THETA:
  1. Group surviving dates into single-link clusters: consecutive dates
     (sorted) are merged when their gap ≤ theta days AND the resulting
     cluster span ≤ MAX_CLUSTER_AMPLITUDE days.
  2. Within each cluster keep the date with the highest
     clear_pixel_count_pt; discard the rest.
  3. Stop when the number of surviving dates ≤ MAX_NUMBER_OF_DATES.

Usage (standalone test):
    python determine_clusters_of_dates.py
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
        ordinals             = np.asarray(f["ts"]).astype(int)
        clear_pixel_count_pt = np.asarray(f["clear_pixel_count_pt"])

    # Filter to window
    mask = np.ones(len(ordinals), dtype=bool)
    if ts_start_ordinal is not None:
        mask &= ordinals >= ts_start_ordinal
    if ts_end_ordinal is not None:
        mask &= ordinals <= ts_end_ordinal

    order = np.where(mask)[0]
    order = order[np.argsort(ordinals[order])]

    sel           = list(order)
    earliest      = {i: int(ordinals[i]) for i in sel}
    latest        = {i: int(ordinals[i]) for i in sel}
    cluster_dates = {i: [int(ordinals[i])] for i in sel}

    for theta in range(1, MAX_THETA + 1):
        ords = np.array([ordinals[i] for i in sel])

        clusters, current = [], [sel[0]]
        for j in range(1, len(sel)):
            span_if_merged = latest[sel[j]] - earliest[current[0]]
            amplitude_ok   = (MAX_CLUSTER_AMPLITUDE is None or
                              span_if_merged <= MAX_CLUSTER_AMPLITUDE)
            if ords[j] - ords[j - 1] <= theta and amplitude_ok:
                current.append(sel[j])
            else:
                clusters.append(current)
                current = [sel[j]]
        clusters.append(current)

        new_sel = []
        for cl in clusters:
            best = max(cl, key=lambda i: clear_pixel_count_pt[i])
            earliest[best]      = min(earliest[j] for j in cl)
            latest[best]        = max(latest[j]   for j in cl)
            cluster_dates[best] = sorted(sum([cluster_dates[j] for j in cl], []))
            new_sel.append(best)
        sel = sorted(new_sel, key=lambda i: ordinals[i])

    sel_sorted = sorted(sel, key=lambda i: ordinals[i])

    list_of_date_clusters_ordinal = [
        cluster_dates[i] for i in sel_sorted
    ]
    list_of_median_dates_ordinal = [
        int(np.median(cl)) for cl in list_of_date_clusters_ordinal
    ]
    list_of_potential_change_dates_ordinal = [
        (latest[sel_sorted[k]] + earliest[sel_sorted[k + 1]]) // 2
        for k in range(len(sel_sorted) - 1)
    ]

    return (list_of_potential_change_dates_ordinal,
            list_of_date_clusters_ordinal,
            list_of_median_dates_ordinal)


if __name__ == "__main__":
    import os

    TILE      = "T29SMD"
    HDF5_PATH = os.path.join(r"H:\outputs_ROI\hdf5", TILE, "hdf5", TILE + ".h5")
    MIN_DATE  = None # "2024-05-31"
    MAX_DATE  = None

    ts_start = _date.fromisoformat(MIN_DATE).toordinal() if MIN_DATE else None
    ts_end   = _date.fromisoformat(MAX_DATE).toordinal() if MAX_DATE else None

    change_dates, clusters, medians = determine_clusters_of_dates(
        HDF5_PATH, ts_start, ts_end
    )

    print(f"N clusters : {len(clusters)}")
    print(f"N mid-dates: {len(change_dates)}\n")
    for k, cl in enumerate(clusters):
        mid_str = _date.fromordinal(change_dates[k - 1]).isoformat() if k > 0 else "-"
        med_str = _date.fromordinal(medians[k]).isoformat()
        cl_str  = ", ".join(_date.fromordinal(o).isoformat() for o in cl)
        print(f"  mid={mid_str}  cluster {k+1:>2} (size {len(cl):>2}, median {med_str}): {cl_str}")
