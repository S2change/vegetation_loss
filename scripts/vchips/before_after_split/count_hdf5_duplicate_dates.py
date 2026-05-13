"""
Count duplicate dates across every HDF5 tile file in a directory.

For each .h5 file, reads only the 'ts' array (cheap — small 1D array of
ordinal dates), counts how many entries share dates with another entry,
and writes a tab-separated text report summarising the result.

The report sorts files by total duplicates descending so the noisiest
tiles surface first. Each row also includes the most-duplicated date for
that file so you can spot patterns at a glance.

Usage:
    python count_hdf5_duplicate_dates.py <hdf5_dir> <output_report_path>

Example:
    python count_hdf5_duplicate_dates.py \\
        /users1/dgt/hdf5 \\
        ./hdf5_duplicate_summary.txt
"""
import os
import sys
import glob
from collections import Counter
from datetime import datetime

import h5py
import numpy as np


def ordinal_to_yyyymmdd(ordinal):
    d = datetime.fromordinal(int(ordinal))
    return f"{d.year:04d}-{d.month:02d}-{d.day:02d}"


def analyse_file(hdf5_path):
    """
    Read 'ts' from one HDF5 file and return a stats dict.

    Returns
    -------
    dict with keys:
        tile_id            : str, e.g. 'T29SMC'
        total_timesteps    : int
        unique_dates       : int
        duplicate_groups   : int  (dates appearing more than once)
        duplicate_entries  : int  (n_dups summed across groups, i.e. extra
                                   timesteps beyond the first occurrence)
        most_dup_date      : str, YYYY-MM-DD of the date with the most copies
        most_dup_count     : int, how many entries that date has
        error              : str or None (set if the file failed to open)
    """
    tile_id = os.path.splitext(os.path.basename(hdf5_path))[0]
    try:
        with h5py.File(hdf5_path, 'r') as h5f:
            ts: np.ndarray = h5f['ts'][:]  # type: ignore[index]
    except Exception as exc:
        return {
            'tile_id': tile_id,
            'total_timesteps': 0,
            'unique_dates': 0,
            'duplicate_groups': 0,
            'duplicate_entries': 0,
            'most_dup_date': '',
            'most_dup_count': 0,
            'error': str(exc),
        }

    counts = Counter(int(o) for o in ts)
    duplicate_groups = sum(1 for c in counts.values() if c > 1)
    # "duplicate_entries" counts every entry past the first occurrence of a
    # date — i.e. how many timesteps could be removed if you deduped.
    duplicate_entries = sum(c - 1 for c in counts.values() if c > 1)

    if counts:
        most_dup_ord, most_dup_count = max(counts.items(), key=lambda kv: kv[1])
        most_dup_date = ordinal_to_yyyymmdd(most_dup_ord)
    else:
        most_dup_date = ''
        most_dup_count = 0

    return {
        'tile_id': tile_id,
        'total_timesteps': len(ts),
        'unique_dates': len(counts),
        'duplicate_groups': duplicate_groups,
        'duplicate_entries': duplicate_entries,
        'most_dup_date': most_dup_date,
        'most_dup_count': most_dup_count,
        'error': None,
    }


def write_report(rows, hdf5_dir, output_path):
    """Write a tab-separated text report. Rows already sorted by caller."""
    headers = [
        'tile_id', 'total_timesteps', 'unique_dates',
        'duplicate_groups', 'duplicate_entries',
        'most_dup_date', 'most_dup_count', 'error',
    ]

    # Aggregate totals for the summary line at the top
    total_files = len(rows)
    total_timesteps = sum(r['total_timesteps'] for r in rows)
    total_dup_entries = sum(r['duplicate_entries'] for r in rows)
    files_with_dups = sum(1 for r in rows if r['duplicate_entries'] > 0)
    files_with_errors = sum(1 for r in rows if r['error'])

    with open(output_path, 'w') as f:
        f.write(f"# Duplicate-date report\n")
        f.write(f"# Source directory: {hdf5_dir}\n")
        f.write(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"# Files scanned: {total_files}\n")
        f.write(f"# Files with duplicates: {files_with_dups}\n")
        f.write(f"# Files with read errors: {files_with_errors}\n")
        f.write(f"# Total timesteps across all files: {total_timesteps}\n")
        f.write(f"# Total duplicate entries (extra timesteps): {total_dup_entries}\n")
        f.write(f"#\n")
        f.write("\t".join(headers) + "\n")
        for r in rows:
            f.write("\t".join(str(r[h]) if r[h] is not None else '' for h in headers) + "\n")


def main():
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    hdf5_dir, output_path = sys.argv[1:3]

    h5_files = sorted(glob.glob(os.path.join(hdf5_dir, "*.h5")))
    if not h5_files:
        print(f"No .h5 files found in {hdf5_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"\nScanning {len(h5_files)} HDF5 file(s) in {hdf5_dir}\n")

    rows = []
    for path in h5_files:
        stats = analyse_file(path)
        rows.append(stats)
        if stats['error']:
            print(f"  {stats['tile_id']:10s}  ERROR: {stats['error']}")
        else:
            print(f"  {stats['tile_id']:10s}  "
                  f"timesteps={stats['total_timesteps']:5d}  "
                  f"unique={stats['unique_dates']:5d}  "
                  f"dup_groups={stats['duplicate_groups']:4d}  "
                  f"dup_entries={stats['duplicate_entries']:4d}  "
                  f"max_per_date={stats['most_dup_count']} ({stats['most_dup_date']})")

    # Sort: errors last, then by duplicate_entries descending, ties by tile_id
    rows.sort(key=lambda r: (
        r['error'] is not None,
        -r['duplicate_entries'],
        r['tile_id'],
    ))

    write_report(rows, hdf5_dir, output_path)
    print(f"\nReport written to {output_path}")


if __name__ == "__main__":
    main()
