#!/bin/bash
#
# Aggregator wrapper. Submitted by submit_tile.sh with --dependency=afterok
# on the per-block array job, so this only runs once every block .npz is
# on disk.

#SBATCH --time=0:15:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --partition=fct
#SBATCH --account=cpca070342024
#SBATCH --qos=cpca070342024

set -euo pipefail

module purge
module load gcc13/openmpi/4.1.6
source "${VENV:-/users1/cpca070342024/shared/vchips/venv}/bin/activate"

DISTRIBUTE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python "$DISTRIBUTE_DIR/aggregate_tile.py"

echo "Aggregator finished for tile $TILE_ID."
