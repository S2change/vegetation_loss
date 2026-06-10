#!/bin/bash
#
# Submit a full-tile prediction pipeline to SLURM.
#
# Steps:
#   1. Read the tile's chip-chunked HDF5 to discover the block grid shape
#      (N_BLOCK_ROWS x N_BLOCK_COLS).
#   2. Submit an array job — one task per (block_row, block_col) — that
#      runs `predict_block.py` and writes one .npz + .gpkg per block into
#      OUTPUT_DIR/block_outputs/.
#   3. Submit an aggregator job with `--dependency=afterok:<array_job>` that
#      stitches the per-block shards into tile-level outputs in
#      OUTPUT_DIR/final_outputs/ (.gpkg .parquet .npz .tif).
#
# OUTPUT_DIR layout:
#   OUTPUT_DIR/logs/          SLURM .out/.err
#   OUTPUT_DIR/block_outputs/ per-block .npz + .gpkg
#   OUTPUT_DIR/final_outputs/ tile-level .gpkg/.parquet/.npz/.tif
#
# Run on the login node:
#   ./submit_tile.sh \
#       TILE_ID=T29TPG \
#       TILE_HDF5_PATH=/users1/cpca070342024/shared/hdf5/T29TPG_48ts_20251028_20251229.h5 \
#       TARGET_DATES=2025-11-15,2025-12-01 \
#       OUTPUT_DIR=/users1/cpca070342024/shared/predict_outputs/T29TPG_run01
#
# Optional knobs (KEY=VALUE):
#   THREADS=2           CPU threads per task. Also sets --cpus-per-task so the
#                       allocation matches. A thread sweep showed ~95% scaling
#                       at 2 threads, ~68% at 4 — 2 is the efficient default.
#   MAX_CONCURRENT=8   max array tasks running at once (--array %N cap).
#                       With THREADS, keep THREADS*MAX_CONCURRENT well under the
#                       node's core count to avoid memory-bandwidth saturation.
#   WEIGHTS_PATH=...    .pth checkpoint (has a default).
#   BATCH_SIZE=8        model batch size.
#   VOTE_CLASSES=1,2    non-bg class IDs to vote on. 1 = Cuts, 2 = Fires. Class names are in AAA_Configs.py
#   VOTE_THRESHOLD=2    min votes per pixel to keep a detection.
#   CLOSING_RADIUS=3    post-vote morphological close disk radius (0 = off).
#   MIN_PATCH_M2=2500   block-level patch-area floor (m^2), firm.
#   MIN_TILE_PATCH_M2=5000  master patch-area floor (m^2), post cross-block merge.
#   MAX_COMPOSITE_DAYS  symmetric day-window around each break date for
#                       before/after compositing (unset = unbounded).
#
# All KEY=VALUE pairs become env vars that the array tasks and aggregator
# inherit (sbatch --export=ALL is the default — we use that).

set -euo pipefail

# Parse KEY=VALUE args into env vars.
for arg in "$@"; do
    if [[ "$arg" == *=* ]]; then
        export "$arg"
    else
        echo "Bad arg: $arg (expected KEY=VALUE)" >&2
        exit 1
    fi
done

# ── Required ──────────────────────────────────────────────────────────────
: "${TILE_ID:?TILE_ID is required}"
: "${TILE_HDF5_PATH:?TILE_HDF5_PATH is required}"
: "${TARGET_DATES:?TARGET_DATES is required (comma-separated YYYY-MM-DD)}"
: "${OUTPUT_DIR:?OUTPUT_DIR is required}"

# ── Optional (with defaults) ──────────────────────────────────────────────
export WEIGHTS_PATH="${WEIGHTS_PATH:-/users1/cpca070342024/shared/model_weights/teste20260429163505_best.pth}"
export BATCH_SIZE="${BATCH_SIZE:-8}"
export VOTE_CLASSES="${VOTE_CLASSES:-1,2}"
export VOTE_THRESHOLD="${VOTE_THRESHOLD:-2}"
# Post-vote close + two-tier patch-area floors (block then master).
export CLOSING_RADIUS="${CLOSING_RADIUS:-3}"
export MIN_PATCH_M2="${MIN_PATCH_M2:-2500}"
export MIN_TILE_PATCH_M2="${MIN_TILE_PATCH_M2:-5000}"

# Symmetric day-window (days) around each break date for before/after
# compositing. Unset = unbounded (any timestep before/after the target). Only
# exported when the caller sets it, so predict_block sees None when unset.
if [[ -n "${MAX_COMPOSITE_DAYS:-}" ]]; then
    export MAX_COMPOSITE_DAYS
fi

# CPU threads per task. Exported so the array wrapper sizes its thread pools
# to this; also passed as --cpus-per-task below so the SLURM allocation has
# that many real cores (otherwise cgroups confine the task to 1 core and the
# threads just fight over it). A thread sweep on a detection-heavy block
# showed 1.9x speedup at 2 threads (95% efficiency), 2.74x at 4 (68%).
export THREADS="${THREADS:-2}"

# Max array tasks allowed to run at once (the `%N` in --array=0-LAST%N).
# Tasks share the node's memory bandwidth + HDF5/filesystem; once
# THREADS*MAX_CONCURRENT approaches the node core count, inference slows from
# bandwidth saturation (observed: 30 tasks x 1 thread -> 2.5x slower/chip).
# Keep the product comfortably under the node's cores. Lower also eases the
# Step-1 read storm. Set >= N_BLOCKS to effectively disable the cap.
MAX_CONCURRENT="${MAX_CONCURRENT:-8}"

# Output layout under OUTPUT_DIR:
#   logs/          SLURM .out/.err per block + the aggregator
#   block_outputs/ per-block .npz + .gpkg (predict_block.py writes here)
#   final_outputs/ tile-level .gpkg/.parquet/.npz/.tif (aggregate_tile.py)
export LOG_DIR="${LOG_DIR:-${OUTPUT_DIR}/logs}"
export BLOCK_OUTPUT_DIR="${BLOCK_OUTPUT_DIR:-${OUTPUT_DIR}/block_outputs}"
export FINAL_OUTPUT_DIR="${FINAL_OUTPUT_DIR:-${OUTPUT_DIR}/final_outputs}"
mkdir -p "$OUTPUT_DIR" "$LOG_DIR" "$BLOCK_OUTPUT_DIR" "$FINAL_OUTPUT_DIR"

# ── Discover block grid shape via a quick venv invocation ─────────────────
# (Reads the HDF5 once on the login node; cheap — just opens attrs and
# the xs/ys arrays.)
DISTRIBUTE_DIR="$(cd "$(dirname "$0")" && pwd)"
SHARED_DIR="$(dirname "$DISTRIBUTE_DIR")"
VENV="${VENV:-/users1/cpca070342024/shared/vchips/venv}"

read N_ROWS N_COLS <<<"$(
    PYTHONPATH="$SHARED_DIR" "$VENV/bin/python" -c "
from input_setup import get_block_grid_shape
r, c = get_block_grid_shape('$TILE_HDF5_PATH')
print(r, c)
"
)"
N_BLOCKS=$((N_ROWS * N_COLS))
LAST_IDX=$((N_BLOCKS - 1))

echo "Tile:           $TILE_ID"
echo "HDF5:           $TILE_HDF5_PATH"
echo "Block grid:     ${N_ROWS} x ${N_COLS}  ($N_BLOCKS blocks)"
echo "Output dir:     $OUTPUT_DIR"
echo "  logs:         $LOG_DIR"
echo "  block out:    $BLOCK_OUTPUT_DIR"
echo "  final out:    $FINAL_OUTPUT_DIR"
echo "Target dates:   $TARGET_DATES"
echo "Batch size:     $BATCH_SIZE"
echo "Vote classes:   $VOTE_CLASSES"
echo "Vote threshold: $VOTE_THRESHOLD"
echo "Closing radius: $CLOSING_RADIUS"
echo "Block floor:    $MIN_PATCH_M2 m^2"
echo "Tile floor:     $MIN_TILE_PATCH_M2 m^2"
echo "Max comp. days: ${MAX_COMPOSITE_DAYS:-unbounded}"
echo "Threads/task:   $THREADS  (= --cpus-per-task)"
echo "Max concurrent: $MAX_CONCURRENT  (cores in use <= THREADS*MAX_CONCURRENT = $((THREADS * MAX_CONCURRENT)))"
echo "Weights:        $WEIGHTS_PATH"
echo

# Export N_COLS so the per-block wrapper can decode SLURM_ARRAY_TASK_ID.
export N_COLS

# Export DISTRIBUTE_DIR so the wrappers can locate the Python entry points.
# SLURM copies batch scripts into /var/spool/slurmd/jobXXXX/ before running,
# so resolving via ${BASH_SOURCE[0]} inside the wrapper points at the spool,
# not the real script dir.
export DISTRIBUTE_DIR

# ── Submit array job (one task per block) ─────────────────────────────────
ARRAY_JOB_ID=$(
    sbatch --parsable \
        --array=0-${LAST_IDX}%${MAX_CONCURRENT} \
        --cpus-per-task="${THREADS}" \
        --export=ALL \
        --output="$LOG_DIR/predict_block_%a.out" \
        --error="$LOG_DIR/predict_block_%a.err" \
        --job-name="predict_${TILE_ID}" \
        "$DISTRIBUTE_DIR/run_block_slurm.sh"
)
echo "Submitted array job:      $ARRAY_JOB_ID  (array 0-${LAST_IDX}%${MAX_CONCURRENT}, ${THREADS} cpu/task)"

# ── Submit aggregator (depends on array success) ──────────────────────────
AGGR_JOB_ID=$(
    sbatch --parsable \
        --dependency=afterok:"$ARRAY_JOB_ID" \
        --export=ALL \
        --output="$LOG_DIR/aggregate.out" \
        --error="$LOG_DIR/aggregate.err" \
        --job-name="aggr_${TILE_ID}" \
        "$DISTRIBUTE_DIR/run_aggregate_slurm.sh"
)
echo "Submitted aggregator job: $AGGR_JOB_ID  (afterok:$ARRAY_JOB_ID)"
echo
echo "Watch with:  squeue -u \$USER"
echo "Per-block logs:    $LOG_DIR/predict_block_<task_id>.out"
echo "Aggregator log:    $LOG_DIR/aggregate.out"
echo "Per-block outputs: $BLOCK_OUTPUT_DIR/  (.npz + .gpkg)"
echo "Final outputs:     $FINAL_OUTPUT_DIR/  (.gpkg .parquet .npz .tif)"
