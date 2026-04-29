#!/bin/bash

#SBATCH --job-name=vchip_before_after_split
#SBATCH --time=1:00:00
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
HDF5_FILE_PATH="/users1/dgt/hdf5/T29TNE.h5"
VCHIP_FILE_PATH="/users1/cpca070342024/shared/vchips/benchmark_tests/vchips_tests/vchip_667265_4424475_20220126_mask.tif"
# -------------------------------------------------------

src='check_hdf5_chunk_layout.py'

echo "=== Running ==="
if [ ! -e "$src" ]; then
    echo "Error: Source file $src not found"
    exit 1
fi

if [ ! -e "$HDF5_FILE_PATH" ]; then
    echo "Error: HDF5 file $HDF5_FILE_PATH not found"
    exit 1
fi
done

python "$src" "$HDF5_FILE_PATH" "$VCHIP_FILE_PATH"

echo "Finished with job $SLURM_JOBID"