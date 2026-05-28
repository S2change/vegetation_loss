#!/bin/bash
#
# Submit a full-tile prediction pipeline to SLURM.
#
# Steps:
#   1. Read the tile's chip-chunked HDF5 to discover the block grid shape
#      (N_BLOCK_ROWS x N_BLOCK_COLS).
#   2. Submit an array job — one task per (block_row, block_col) — that
#      runs `predict_block.py` and writes one .npz per block.
#   3. Submit an aggregator job with `--dependency=afterok:<array_job>`
#      that stitches the per-block .npzes into one tile-level .npz.
#
# Run on the login node:
#   ./submit_tile.sh \
#       TILE_ID=T29TPG \
#       TILE_HDF5_PATH=/users1/cpca070342024/shared/hdf5/T29TPG_48ts_20251028_20251229.h5 \
#       TARGET_DATES=2025-11-15,2025-12-01 \
#       OUTPUT_DIR=/users1/cpca070342024/shared/predict_outputs/T29TPG_run01
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

# Each array task and the aggregator log to its own file under LOG_DIR.
export LOG_DIR="${LOG_DIR:-${OUTPUT_DIR}/logs}"
mkdir -p "$OUTPUT_DIR" "$LOG_DIR"

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
echo "Log dir:        $LOG_DIR"
echo "Target dates:   $TARGET_DATES"
echo "Batch size:     $BATCH_SIZE"
echo "Vote classes:   $VOTE_CLASSES"
echo "Vote threshold: $VOTE_THRESHOLD"
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
        --array=0-${LAST_IDX} \
        --export=ALL \
        --output="$LOG_DIR/predict_block_%a.out" \
        --error="$LOG_DIR/predict_block_%a.err" \
        --job-name="predict_${TILE_ID}" \
        "$DISTRIBUTE_DIR/run_block_slurm.sh"
)
echo "Submitted array job:      $ARRAY_JOB_ID  (array 0-$LAST_IDX)"

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
echo "Per-block logs: $LOG_DIR/predict_block_<task_id>.out"
echo "Aggregator log: $LOG_DIR/aggregate.out"
