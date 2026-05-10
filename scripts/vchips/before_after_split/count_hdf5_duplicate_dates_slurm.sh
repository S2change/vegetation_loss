#!/bin/bash

#SBATCH --job-name=count_hdf5_duplicate_dates
#SBATCH --time=0:15:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1

# Be sure to request the correct partition to avoid the job to be held in the queue, furthermore
#       on CIRRUS-B (Minho)  choose for example HPC_4_Days
#       on CIRRUS-A (Lisbon) choose for example hpc
#SBATCH --partition=fct

#SBATCH --account=cpca070342024

#SBATCH --qos=cpca070342024

# Used to guarantee that the environment does not have any other loaded module
module purge

# Load software modules
module load gcc13/openmpi/4.1.6
module load python/3.10

source /users1/cpca070342024/shared/vchips/venv/bin/activate

# -------------------------------------------------------
HDF5_DIR="/users1/dgt/hdf5"
OUTPUT_REPORT="./hdf5_duplicate_summary.txt"
# -------------------------------------------------------

src='count_hdf5_duplicate_dates.py'

echo "=== Running ==="
if [ ! -e "$src" ]; then
    echo "Error: Source file $src not found"
    exit 1
fi

if [ ! -d "$HDF5_DIR" ]; then
    echo "Error: HDF5 directory $HDF5_DIR not found"
    exit 1
fi

python "$src" "$HDF5_DIR" "$OUTPUT_REPORT"

echo "Finished with job $SLURM_JOBID"
