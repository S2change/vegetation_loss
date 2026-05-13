#!/bin/bash

#SBATCH --job-name=chip_creation
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
HDF5_PATH="/users1/dgt/hdf5/T29TNE.h5"
TILE_GPKG="/users1/cpca070342024/shared/auxiliary_data/sentinel2_tiles_PT_32629.gpkg"
START_DATE="20230301"
END_DATE="20230501"
# -------------------------------------------------------

src='chip_creation.py'

echo "=== Running ==="
if [ ! -e "$src" ]; then
    echo "Error: Source file $src not found"
    exit 1
fi

if [ ! -e "$HDF5_PATH" ]; then
    echo "Error: HDF5 file $HDF5_PATH not found"
    exit 1
fi

if [ ! -e "$TILE_GPKG" ]; then
    echo "Error: Geopackage file $TILE_GPKG not found"
    exit 1
fi

python "$src" "$HDF5_PATH" "$TILE_GPKG" "$START_DATE" "$END_DATE"

echo "Finished with job $SLURM_JOBID"
