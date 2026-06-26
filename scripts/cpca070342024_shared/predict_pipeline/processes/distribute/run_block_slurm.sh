#!/bin/bash
#
# Per-array-task wrapper. Submitted by submit_tile.sh; one instance fires
# per (block_row, block_col). Reads $SLURM_ARRAY_TASK_ID + $N_COLS and
# exports the matching BLOCK_ROW / BLOCK_COL before exec'ing
# predict_block.py.

#SBATCH --time=1:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
# NOTE: --cpus-per-task is set on the sbatch command line by submit_tile.sh
# (to match $THREADS), NOT here, so the two never drift apart.
#SBATCH --partition=dgt
#SBATCH --account=cpca070342024
#SBATCH --qos=cpca070342024

set -euo pipefail

module purge
module load gcc13/openmpi/4.1.6
# Do NOT `module load python/3.10` — see predict_testing/run_predict_slurm.sh
# for the typing_extensions / torch CVMFS gotcha.

# VENV is exported by submit_tile.sh (predict_pipeline/.venv). We do NOT derive
# it from ${BASH_SOURCE[0]}: SLURM copies the batch script into
# /var/spool/slurmd/jobNNN/ before running it, so BASH_SOURCE points at the
# spool copy and a "../.." fallback resolves to /var/spool/.venv (wrong).
: "${VENV:?VENV must be exported by submit_tile.sh}"
source "$VENV/bin/activate"
# Clear any inherited PYTHONPATH: the CVMFS Python env puts its own
# site-packages on PYTHONPATH, which sits AHEAD of the venv on sys.path and
# shadows venv packages (numpy/h5py/typing_extensions load from /cvmfs, which
# breaks the venv's torch via the `TypeIs` import). This is the "typing_extensions
# / torch CVMFS gotcha" — also why we don't `module load python/3.10`.
unset PYTHONPATH

# Pin all thread pools to $THREADS. submit_tile.sh exports THREADS and
# allocates a matching --cpus-per-task, so the process has that many real
# cores and the pools size to use them. Precedence: explicit THREADS >
# SLURM_CPUS_PER_TASK > 1. Left uncapped, PyTorch/NumPy/BLAS default to the
# node's full core count (96), and N concurrent tasks oversubscribe the node.
# These MUST be set before Python imports torch/numpy (pools size at import time).
THREADS="${THREADS:-${SLURM_CPUS_PER_TASK:-1}}"
export OMP_NUM_THREADS="$THREADS"
export MKL_NUM_THREADS="$THREADS"
export OPENBLAS_NUM_THREADS="$THREADS"
export NUMEXPR_NUM_THREADS="$THREADS"
export THREADS   # predict_block.py reads this for torch.set_num_threads
echo "THREADS=$THREADS  (cpus-per-task=${SLURM_CPUS_PER_TASK:-unset})"

: "${SLURM_ARRAY_TASK_ID:?Must be invoked as an array task}"
: "${N_COLS:?N_COLS must be exported by submit_tile.sh}"

export BLOCK_ROW=$(( SLURM_ARRAY_TASK_ID / N_COLS ))
export BLOCK_COL=$(( SLURM_ARRAY_TASK_ID % N_COLS ))

echo "=== task $SLURM_ARRAY_TASK_ID -> block ($BLOCK_ROW, $BLOCK_COL) ==="

: "${DISTRIBUTE_DIR:?DISTRIBUTE_DIR must be exported by submit_tile.sh}"
python "$DISTRIBUTE_DIR/predict_block.py"

echo "Finished task $SLURM_ARRAY_TASK_ID (block $BLOCK_ROW, $BLOCK_COL)"
