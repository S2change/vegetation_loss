#!/bin/bash

#SBATCH --job-name=inspect_ts_T29TQG
#SBATCH --time=2:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1

# On CIRRUS-B (Minho)  use e.g. HPC_4_Days
# On CIRRUS-A (Lisbon) use e.g. hpc or fct
#SBATCH --partition=fct

#SBATCH --account=cpca070342024
#SBATCH --qos=cpca070342024

module purge
module load gcc13/openmpi/4.1.6
module load python/3.10

source /users1/cpca070342024/shared/vchips/venv/bin/activate

# -------------------------------------------------------
SRC="/users1/cpca070342024/mlc/scripts/inspect_cnca_timestamps.py"
# -------------------------------------------------------

echo "=== inspect_cnca_timestamps ==="
echo "Script : $SRC"
echo "Started: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo

if [ ! -f "$SRC" ]; then
    echo "Error: script not found at $SRC"
    exit 1
fi

python "$SRC" "$@"

echo
echo "Finished: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
echo "Job ID  : $SLURM_JOBID"

# Usage:
# sbatch inspect_cnca_timestamps_slurm.sh --year 2024
# sbatch inspect_cnca_timestamps_slurm.sh --n 50
# sbatch inspect_cnca_timestamps_slurm.sh          # defaults to n=600
