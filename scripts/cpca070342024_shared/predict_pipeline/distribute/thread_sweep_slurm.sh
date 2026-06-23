#!/bin/bash
#
# Thread-sweep experiment: is Swin-YNet CPU inference compute-bound or
# memory-bandwidth-bound? Runs ONE block (no concurrency) at several thread
# counts and reports ms/chip for each. Isolates the thread variable —
# there is no second task competing, so any speedup from more threads is
# real intra-op parallelism, and any plateau means bandwidth-bound.
#
# Reads the full node so high thread counts have real cores to use (a
# 1-CPU allocation would cap everything at 1 regardless of OMP_NUM_THREADS).
#
# Run on the login node:
#   sbatch thread_sweep_slurm.sh
# then read the per-thread ms/chip from the .out (grep "Total infer time").
#
# Block + tile config is hard-coded below — edit for a different block.
# Pick a DETECTION-HEAVY block (e.g. 2,2) so inference does real work.

#SBATCH --job-name=thread_sweep
#SBATCH --time=1:30:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --partition=fct
#SBATCH --account=cpca070342024
#SBATCH --qos=cpca070342024

set -euo pipefail

module purge
module load gcc13/openmpi/4.1.6
source "${VENV:-/users1/cpca070342024/shared/vchips/venv}/bin/activate"

# ── Experiment config ──────────────────────────────────────────────────────
# SLURM copies the batch script into /var/spool/slurmd/jobXXXX/ before
# running, so ${BASH_SOURCE[0]} resolves to the spool, not the real script
# dir. Use $SLURM_SUBMIT_DIR (the dir you ran `sbatch` from) instead — submit
# this script from the distribute/ directory so it finds predict_block.py.
DISTRIBUTE_DIR="${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
export DISTRIBUTE_DIR
if [ ! -f "$DISTRIBUTE_DIR/predict_block.py" ]; then
    echo "ERROR: predict_block.py not found in DISTRIBUTE_DIR=$DISTRIBUTE_DIR" >&2
    echo "Submit this script from the distribute/ directory (sbatch is run" >&2
    echo "from there), or set SLURM_SUBMIT_DIR / edit DISTRIBUTE_DIR." >&2
    exit 1
fi
echo "DISTRIBUTE_DIR: $DISTRIBUTE_DIR"

export TILE_ID="T29TPG"
export TILE_HDF5_PATH="/users1/cpca070342024/shared/hdf5/T29TPG_48ts_20251028_20251229.h5"
export WEIGHTS_PATH="/users1/cpca070342024/shared/model_weights/teste20260429163505_best.pth"
export TARGET_DATES="2025-11-15,2025-12-01"
export VOTE_CLASSES="1,2"
export VOTE_THRESHOLD="2"
export BATCH_SIZE="8"
export CLOSING_RADIUS="3"
export MIN_PATCH_M2="2500"

# Detection-heavy block to exercise inference. Block (2,2) was dense in
# earlier runs.
export BLOCK_ROW="2"
export BLOCK_COL="2"

# Thread counts to sweep.
THREAD_COUNTS="1 2 4"

# Each iteration writes to its own subdir so the .npz/.gpkg writes don't
# collide (we only care about the inference timing in the .out).
SWEEP_DIR="${SWEEP_DIR:-/users1/cpca070342024/shared/predict_outputs/thread_sweep}"
mkdir -p "$SWEEP_DIR"

echo "=== Thread sweep on block ($BLOCK_ROW,$BLOCK_COL) ==="
echo "Node: $(hostname)  cores: $(nproc)"
echo "Thread counts: $THREAD_COUNTS"
echo

# Don't let a single failing iteration abort the whole sweep — we want
# every thread count attempted. Temporarily relax -e inside the loop.
set +e
for T in $THREAD_COUNTS; do
    echo "############################################################"
    echo "### THREADS=$T   (block $BLOCK_ROW,$BLOCK_COL on $(hostname))"
    echo "############################################################"
    # Set ALL the thread-pool env vars (sized at import) + the torch knob.
    export OMP_NUM_THREADS="$T"
    export MKL_NUM_THREADS="$T"
    export OPENBLAS_NUM_THREADS="$T"
    export NUMEXPR_NUM_THREADS="$T"
    export THREADS="$T"
    export OUTPUT_DIR="$SWEEP_DIR/threads_${T}"
    mkdir -p "$OUTPUT_DIR"

    # Fresh python process each iteration so BLAS pools size to $T at import.
    # Full output goes to the .out (so failures show their traceback); the
    # timing lines are easy to grep afterward via the THREADS banner.
    /usr/bin/time -v python -u "$DISTRIBUTE_DIR/predict_block.py" 2>&1 || \
        echo "  [T=$T] predict_block.py FAILED (see traceback above)"
    echo
done
set -e

echo "============================================================"
echo "=== Sweep complete. Pull the per-thread inference timing with:"
echo "===   grep -E 'THREADS=|Total infer time' <this .out file>"
echo "============================================================"
