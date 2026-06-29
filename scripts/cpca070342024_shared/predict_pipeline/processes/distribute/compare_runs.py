#!/usr/bin/env python3
"""Compare two prediction runs for bit-for-bit equivalence.

Given two run OUTPUT_DIRs (e.g. an old run before the chip-outer clustered
reader and a new run after it), assert their per-block voted `.npz` outputs
and, if present, their final tile GeoTIFFs match. Use this to confirm a code
change that is *intended* to be output-preserving (like the streaming clustered
reader) actually is.

What it checks
--------------
1. block_outputs/*.npz  — the authoritative per-block result. The `labels`
   array drives everything downstream (the tile TIFs are just these voted
   labels stitched + rasterized), so equal labels => equal TIFs. Every array
   key in the .npz is compared with np.array_equal (exact, dtype-aware).
2. final_outputs/*.tif  — optional, only if both runs have them and rasterio is
   importable. Compares the raster band data + key profile fields exactly.

Exit status is 0 only when every compared file matches; non-zero otherwise, so
it's usable in a script / CI gate.

Usage
-----
    python compare_runs.py OLD_RUN_DIR NEW_RUN_DIR
    python compare_runs.py OLD_RUN_DIR NEW_RUN_DIR --tifs        # also diff .tif
    python compare_runs.py OLD_RUN_DIR NEW_RUN_DIR --block 3_5   # one block only

OLD_RUN_DIR / NEW_RUN_DIR are the OUTPUT_DIRs passed to submit_tile.sh; the
script looks under <DIR>/block_outputs/ and <DIR>/final_outputs/.
"""
import argparse
import os
import sys
from glob import glob

import numpy as np


def _block_npzs(run_dir: str) -> dict[str, str]:
    """Map block .npz basename -> full path under <run_dir>/block_outputs/."""
    d = os.path.join(run_dir, "block_outputs")
    return {os.path.basename(p): p for p in sorted(glob(os.path.join(d, "*.npz")))}


def _final_tifs(run_dir: str) -> dict[str, str]:
    """Map .tif basename -> full path under <run_dir>/final_outputs/."""
    d = os.path.join(run_dir, "final_outputs")
    return {os.path.basename(p): p for p in sorted(glob(os.path.join(d, "*.tif")))}


def _load_npz(path: str) -> dict:
    with np.load(path) as npz:
        return {k: npz[k] for k in npz.files}


def _compare_npz_pair(name: str, old_path: str, new_path: str) -> list[str]:
    """Return a list of human-readable mismatch messages ([] if identical)."""
    old = _load_npz(old_path)
    new = _load_npz(new_path)
    msgs: list[str] = []

    old_keys, new_keys = set(old), set(new)
    if old_keys != new_keys:
        only_old = old_keys - new_keys
        only_new = new_keys - old_keys
        if only_old:
            msgs.append(f"keys only in OLD: {sorted(only_old)}")
        if only_new:
            msgs.append(f"keys only in NEW: {sorted(only_new)}")

    for k in sorted(old_keys & new_keys):
        a, b = old[k], new[k]
        if a.shape != b.shape:
            msgs.append(f"{k}: shape {a.shape} != {b.shape}")
            continue
        if a.dtype != b.dtype:
            msgs.append(f"{k}: dtype {a.dtype} != {b.dtype}")
            # keep going — values may still differ meaningfully
        if not np.array_equal(a, b):
            n_diff = int(np.count_nonzero(a != b)) if a.shape == b.shape else -1
            extra = ""
            if k == "labels" and a.shape == b.shape:
                # Most useful detail for the label raster: how many pixels and
                # which dates differ.
                per_date = [int(np.count_nonzero(a[i] != b[i]))
                            for i in range(a.shape[0])]
                extra = f"  per-date-diff-pixels={per_date}"
            msgs.append(f"{k}: values differ ({n_diff} elements){extra}")
    return msgs


def _compare_tif_pair(name: str, old_path: str, new_path: str) -> list[str]:
    import rasterio
    msgs: list[str] = []
    with rasterio.open(old_path) as ro, rasterio.open(new_path) as rn:
        if ro.shape != rn.shape:
            msgs.append(f"raster shape {ro.shape} != {rn.shape}")
        if ro.count != rn.count:
            msgs.append(f"band count {ro.count} != {rn.count}")
        for field in ("crs", "transform", "nodata", "dtypes"):
            ov, nv = getattr(ro, field), getattr(rn, field)
            if ov != nv:
                msgs.append(f"profile.{field}: {ov} != {nv}")
        if not msgs:  # only compare pixels if geometry matches
            a, b = ro.read(), rn.read()
            if not np.array_equal(a, b):
                msgs.append(f"raster data differs "
                            f"({int(np.count_nonzero(a != b))} elements)")
    return msgs


def _run_compare(label: str, old_map: dict, new_map: dict, compare_fn):
    """Compare a category (blocks or tifs). Returns (n_ok, n_bad, n_missing)."""
    old_names, new_names = set(old_map), set(new_map)
    common = sorted(old_names & new_names)
    only_old = sorted(old_names - new_names)
    only_new = sorted(new_names - old_names)

    print(f"\n=== {label}: {len(common)} common, "
          f"{len(only_old)} only-old, {len(only_new)} only-new ===")
    for n in only_old:
        print(f"  MISSING in NEW: {n}")
    for n in only_new:
        print(f"  EXTRA in NEW:   {n}")

    n_ok = n_bad = 0
    for n in common:
        msgs = compare_fn(n, old_map[n], new_map[n])
        if msgs:
            n_bad += 1
            print(f"  DIFF  {n}")
            for m in msgs:
                print(f"          {m}")
        else:
            n_ok += 1
    if common:
        print(f"  {n_ok}/{len(common)} identical, {n_bad} differ")
    return n_ok, n_bad, len(only_old) + len(only_new)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("old_run", help="old run OUTPUT_DIR (before the change)")
    ap.add_argument("new_run", help="new run OUTPUT_DIR (after the change)")
    ap.add_argument("--tifs", action="store_true",
                    help="also compare final_outputs/*.tif (needs rasterio)")
    ap.add_argument("--block", default=None,
                    help="compare only the block with this 'ROW_COL' id "
                         "(e.g. --block 3_5); matches the *_block_RRR_CCC.npz "
                         "naming. Default: all blocks.")
    args = ap.parse_args(argv)

    old_blocks = _block_npzs(args.old_run)
    new_blocks = _block_npzs(args.new_run)
    if args.block is not None:
        try:
            r, c = (int(x) for x in args.block.split("_"))
        except ValueError:
            print(f"--block must be ROW_COL (e.g. 3_5), got {args.block!r}",
                  file=sys.stderr)
            return 2
        tag = f"_block_{r:03d}_{c:03d}.npz"
        old_blocks = {k: v for k, v in old_blocks.items() if k.endswith(tag)}
        new_blocks = {k: v for k, v in new_blocks.items() if k.endswith(tag)}
        if not old_blocks and not new_blocks:
            print(f"No block matching {tag} in either run.", file=sys.stderr)
            return 2

    if not old_blocks:
        print(f"No block .npz found under {args.old_run}/block_outputs/",
              file=sys.stderr)
        return 2
    if not new_blocks:
        print(f"No block .npz found under {args.new_run}/block_outputs/",
              file=sys.stderr)
        return 2

    total_bad = total_missing = 0

    _, bad, missing = _run_compare("block_outputs (.npz)",
                                   old_blocks, new_blocks, _compare_npz_pair)
    total_bad += bad
    total_missing += missing

    if args.tifs:
        try:
            import rasterio  # noqa: F401
        except ImportError:
            print("\n--tifs requested but rasterio is not importable; "
                  "skipping TIF comparison.", file=sys.stderr)
        else:
            _, bad, missing = _run_compare("final_outputs (.tif)",
                                           _final_tifs(args.old_run),
                                           _final_tifs(args.new_run),
                                           _compare_tif_pair)
            total_bad += bad
            total_missing += missing

    print("\n" + "=" * 60)
    if total_bad == 0 and total_missing == 0:
        print("RESULT: runs are IDENTICAL across all compared outputs.")
        return 0
    print(f"RESULT: {total_bad} file(s) differ, "
          f"{total_missing} file(s) present in only one run.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
