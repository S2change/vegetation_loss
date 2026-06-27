"""Merge every tile's final detection .gpkg in a run into one combined .gpkg,
and write a batch-level full_summary.txt rolling up all the tiles' run summaries.

Walks each <parent_dir>/<TILE_ID>/final_outputs/<TILE_ID>_tile.gpkg, reads the
"detections" layer, and concatenates them into <parent_dir>/<run_name>.gpkg.
Then reads each <parent_dir>/<TILE_ID>/logs/00_summary.metrics (written by the
per-tile aggregator) and rolls them into <parent_dir>/full_summary.txt.

Config via environment (set by run_group_slurm.sh / submit_tiles_batch.sh):
  GROUP_PARENT_DIR  (required) the run's parent dir — i.e. the batch's
                    BASE_OUTPUT_DIR, which holds one subdir per tile.
  GROUP_RUN_NAME    (optional) base name for the merged file; defaults to the
                    parent dir's own name. Output is <parent_dir>/<run_name>.gpkg.

Runnable standalone too: `GROUP_PARENT_DIR=/path/to/run python group_final_outputs.py`.
"""
import os
from pathlib import Path
from datetime import datetime

import geopandas as gpd
import pandas as pd

parent_dir = Path(os.environ["GROUP_PARENT_DIR"])
run_name = os.environ.get("GROUP_RUN_NAME") or parent_dir.name
output_file = parent_dir / f"{run_name}.gpkg"

tile_dirs = sorted(
    d for d in parent_dir.iterdir()
    if d.is_dir() and d.name != "gpkg_files"
)

gpkg_files = []
missing = []
for tile_dir in tile_dirs:
    gpkg = tile_dir / "final_outputs" / f"{tile_dir.name}_tile.gpkg"
    if gpkg.exists():
        gpkg_files.append(gpkg)
    else:
        missing.append(str(gpkg))
        print(f"Missing: {gpkg}")

if not gpkg_files:
    raise FileNotFoundError("No gpkg files found.")

print(f"Merging {len(gpkg_files)} file(s) into {output_file.name} ...")

# Read each tile's "detections" layer (the layer aggregate_tile.py writes) and
# concat. pd.concat aligns columns by name, so tiles that differ in optional
# columns (e.g. `confidence` only present on OUTPUT_CONFIDENCE runs) merge
# cleanly with NaN fill — mirrors how aggregate_tile.py concatenates blocks.
gdfs = []
total = 0
for gpkg in gpkg_files:
    g = gpd.read_file(str(gpkg), layer="detections")
    gdfs.append(g)
    total += len(g)
    print(f"  {gpkg.parent.parent.name}: {len(g)} features")

merged = gpd.GeoDataFrame(
    pd.concat(gdfs, ignore_index=True),
    geometry="geometry",
    crs=gdfs[0].crs,
)
merged.to_file(output_file, layer="detections", driver="GPKG")

print(f"\nDone. {total} total features written to {output_file}")
if missing:
    print(f"Skipped {len(missing)} missing file(s):")
    for m in missing:
        print(f"  {m}")


# ════════════════════════════════════════════════════════════════════════════
# Batch summary rollup
# ════════════════════════════════════════════════════════════════════════════

def _fmt_hms(secs: int) -> str:
    return f"{secs // 3600:02d}:{(secs % 3600) // 60:02d}:{secs % 60:02d} ({secs} s)"


def _gib(kb: float) -> str:
    return f"{kb / 1024 / 1024:.2f}"


def _pct(a: float, b: float) -> str:
    return f"{100 * a / b:.1f}" if b > 0 else "n/a"


def _read_metrics(path: Path) -> dict:
    """Parse a key=value 00_summary.metrics file into a dict (str values)."""
    out = {}
    for line in path.read_text().splitlines():
        if "=" in line:
            k, _, v = line.partition("=")
            out[k] = v
    return out


def write_batch_summary() -> None:
    """Roll up every tile's 00_summary.metrics into <parent_dir>/full_summary.txt.

    Inputs are taken once from the first tile (identical across a batch).
    Resources sum the CPU-count per part across tiles. Peak memory is the single
    worst block and the single worst aggregation across the whole batch, each
    tagged with the owning tile (and block, for the block peak).
    """
    metrics = []
    for tile_dir in tile_dirs:
        mpath = tile_dir / "logs" / "00_summary.metrics"
        if mpath.exists():
            m = _read_metrics(mpath)
            m["_tile_dir"] = tile_dir.name
            metrics.append(m)

    if not metrics:
        print("No per-tile metrics found; skipping batch full_summary.txt.")
        return

    def f(m, k, default=0.0):
        try:
            return float(m.get(k, "") or default)
        except ValueError:
            return default

    # Resources: total CPU-count per part across tiles.
    block_cpus = sum(int(f(m, "THREADS")) * int(f(m, "N_BLOCK_TASKS")) for m in metrics)
    aggr_cpus = sum(int(f(m, "AGGR_CPUS", 3)) for m in metrics)

    # Timing: batch wall = max tile wall (tiles overlap); CPU = sum across tiles.
    walls = [int(f(m, "WALL_SECS")) for m in metrics if m.get("WALL_SECS")]
    cpus = [int(f(m, "CPU_SECS")) for m in metrics if m.get("CPU_SECS")]
    batch_wall = max(walls) if walls else None
    batch_cpu = sum(cpus) if cpus else None

    # Peak memory: single worst block / worst aggregation across the batch.
    worst_block = max(metrics, key=lambda m: f(m, "BLOCKS_PEAK_KB"), default=None)
    worst_aggr = max(metrics, key=lambda m: f(m, "AGGR_PEAK_KB"), default=None)

    inp = metrics[0]   # shared inputs identical across tiles
    n_tiles = len(metrics)
    summary_path = parent_dir / "full_summary.txt"

    lines = []
    bar = "═" * 68
    lines.append(bar)
    lines.append(f" BATCH SUMMARY — run {run_name}")
    lines.append(f" {n_tiles} tile(s)" + (f", {len(missing)} missing .gpkg" if missing else ""))
    lines.append(f" generated {datetime.now():%Y-%m-%d %H:%M:%S}")
    lines.append(bar)
    lines.append("")
    lines.append("── Inputs (shared across tiles) ────────────────────────────────────")
    lines.append(f"  Model:           {inp.get('MODEL','?')}   (data dtype {inp.get('DATA_DTYPE','?')})")
    lines.append(f"  Date window:     {inp.get('START_DATE','?')} -> {inp.get('END_DATE','?')}")
    lines.append(f"  Read window:     {inp.get('READ_START_DATE') or 'unbounded'} -> "
                 f"{inp.get('READ_END_DATE') or 'unbounded'}")
    nclust = int(f(inp, "DATE_CLUSTERS_N"))
    lines.append(f"  Date clusters:   {'on (' + str(nclust) + ' clusters)' if nclust else 'off (raw timesteps)'}")
    lines.append(f"  Vote classes:    {inp.get('VOTE_CLASSES','?')}   threshold {inp.get('VOTE_THRESHOLD','?')}")
    lines.append(f"  Batch size:      {inp.get('BATCH_SIZE','?')}")
    lines.append(f"  Patch floors:    block {inp.get('MIN_PATCH_M2','?')} m^2 / "
                 f"tile {inp.get('MIN_TILE_PATCH_M2','?')} m^2")
    lines.append(f"  Max comp. days:  {inp.get('MAX_COMPOSITE_DAYS') or 'unbounded'}")
    lines.append(f"  Weights:         {inp.get('WEIGHTS_PATH','?')}")
    lines.append("")
    lines.append("── Resources (summed across tiles) ─────────────────────────────────")
    lines.append(f"  Block CPUs:      {block_cpus}   (sum of THREADS x blocks-run per tile)")
    lines.append(f"  Aggregator CPUs: {aggr_cpus}   (sum over tiles)")
    lines.append(f"  Total CPUs:      {block_cpus + aggr_cpus}")
    lines.append("")
    lines.append("── Timing ──────────────────────────────────────────────────────────")
    lines.append(f"  Batch wall time: {_fmt_hms(batch_wall) if batch_wall is not None else 'n/a'}"
                 "   (longest single tile; tiles run in parallel)")
    lines.append(f"  Total CPU time:  {_fmt_hms(batch_cpu) if batch_cpu is not None else 'n/a'}"
                 "   (all tiles' blocks + aggregators)")
    lines.append("")
    lines.append("── Peak memory (worst across entire batch) ─────────────────────────")
    if worst_block and f(worst_block, "BLOCKS_PEAK_KB") > 0:
        pk = f(worst_block, "BLOCKS_PEAK_KB")
        al = f(worst_block, "BLOCKS_ALLOC_KB")
        lines.append(f"  Blocks (peak):   {_gib(pk)} GiB of {_gib(al)} GiB allocated "
                     f"({_pct(pk, al)}%)  [tile {worst_block.get('TILE_ID','?')}, "
                     f"block task {worst_block.get('BLOCKS_PEAK_ID','?')}]")
    else:
        lines.append("  Blocks (peak):   n/a (no MaxRSS in metrics)")
    if worst_aggr and f(worst_aggr, "AGGR_PEAK_KB") > 0:
        pk = f(worst_aggr, "AGGR_PEAK_KB")
        al = f(worst_aggr, "AGGR_ALLOC_KB")
        lines.append(f"  Tile Aggregation:     {_gib(pk)} GiB of {_gib(al)} GiB allocated "
                     f"({_pct(pk, al)}%)  [tile {worst_aggr.get('TILE_ID','?')}]")
    else:
        lines.append("  Tile Aggregation:     n/a (no MaxRSS in metrics)")
    lines.append(bar)

    summary_path.write_text("\n".join(lines) + "\n")
    print(f"Wrote batch summary: {summary_path}")


write_batch_summary()
