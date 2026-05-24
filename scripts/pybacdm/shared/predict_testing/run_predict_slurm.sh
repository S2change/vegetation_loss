#!/bin/bash

#SBATCH --job-name=run_predict
#SBATCH --time=0:30:00
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
# NOTE: do not `module load python/3.10` — it prepends CVMFS site-packages
# ahead of the venv's, which shadows the venv's typing_extensions and
# breaks `import torch` (TypeIs missing in the older CVMFS copy).
# The venv ships its own Python interpreter; activate alone is enough.

source /users1/cpca070342024/shared/vchips/venv/bin/activate

src='run_predict.py'

echo "=== Running ==="
if [ ! -e "$src" ]; then
    echo "Error: Source file $src not found"
    exit 1
fi

python "$src"

echo "Finished with job $SLURM_JOBID"
