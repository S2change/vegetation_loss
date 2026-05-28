#!/bin/bash
#
# Per-array-task wrapper. Submitted by submit_tile.sh; one instance fires
# per (block_row, block_col). Reads $SLURM_ARRAY_TASK_ID + $N_COLS and
# exports the matching BLOCK_ROW / BLOCK_COL before exec'ing
# predict_block.py.

#SBATCH --time=0:30:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --partition=fct
#SBATCH --account=cpca070342024
#SBATCH --qos=cpca070342024

set -euo pipefail

module purge
module load gcc13/openmpi/4.1.6
# Do NOT `module load python/3.10` — see predict_testing/run_predict_slurm.sh
# for the typing_extensions / torch CVMFS gotcha.

source "${VENV:-/users1/cpca070342024/shared/vchips/venv}/bin/activate"

: "${SLURM_ARRAY_TASK_ID:?Must be invoked as an array task}"
: "${N_COLS:?N_COLS must be exported by submit_tile.sh}"

export BLOCK_ROW=$(( SLURM_ARRAY_TASK_ID / N_COLS ))
export BLOCK_COL=$(( SLURM_ARRAY_TASK_ID % N_COLS ))

echo "=== task $SLURM_ARRAY_TASK_ID -> block ($BLOCK_ROW, $BLOCK_COL) ==="

: "${DISTRIBUTE_DIR:?DISTRIBUTE_DIR must be exported by submit_tile.sh}"
python "$DISTRIBUTE_DIR/predict_block.py"

echo "Finished task $SLURM_ARRAY_TASK_ID (block $BLOCK_ROW, $BLOCK_COL)"
