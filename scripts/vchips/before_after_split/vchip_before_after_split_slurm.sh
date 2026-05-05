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
VCHIP_DIR="/users1/cpca070342024/shared/vchips/masks_tif"
HDF5_DIR="/users1/dgt/hdf5"
BEFORE_OUTPUT_DIR="/users1/cpca070342024/shared/vchips/before_B12_to_B2_nodata_65535"
AFTER_OUTPUT_DIR="/users1/cpca070342024/shared/vchips/after_B12_to_B2_nodata_65535"
TILE_GPKG="/users1/cpca070342024/shared/auxiliary_data/sentinel2_tiles_PT_32629.gpkg"
# -------------------------------------------------------

src='vchip_before_after_split.py'

echo "=== Running ==="
if [ ! -e "$src" ]; then
    echo "Error: Source file $src not found"
    exit 1
fi

for d in "$VCHIP_DIR" "$HDF5_DIR" "$BEFORE_OUTPUT_DIR" "$AFTER_OUTPUT_DIR"; do
    if [ ! -d "$d" ]; then
        echo "Error: Directory $d not found"
        exit 1
    fi
done

if [ ! -e "$TILE_GPKG" ]; then
    echo "Error: Geopackage file $TILE_GPKG not found"
    exit 1
fi

python "$src" "$VCHIP_DIR" "$HDF5_DIR" "$BEFORE_OUTPUT_DIR" "$AFTER_OUTPUT_DIR" "$TILE_GPKG"

echo "Finished with job $SLURM_JOBID"