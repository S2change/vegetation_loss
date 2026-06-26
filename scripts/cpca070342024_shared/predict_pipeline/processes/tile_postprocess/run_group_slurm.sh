#!/bin/bash
#
# Run-level grouping wrapper. Submitted by submit_tiles_batch.sh with
# --dependency=afterany on every tile's aggregator job, so it runs once all
# tiles have finished (afterany, not afterok, so a failed tile still lets the
# rest be merged). Merges each tile's final_outputs/<TILE>_tile.gpkg into one
# combined <BASE_OUTPUT_DIR>/<run_name>.gpkg via group_final_outputs.py.

#SBATCH --time=00:30:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --partition=dgt
#SBATCH --account=cpca070342024
#SBATCH --qos=cpca070342024

set -euo pipefail

module purge
module load gcc13/openmpi/4.1.6
# VENV is exported by submit_tiles_batch.sh. We do NOT derive it from
# ${BASH_SOURCE[0]} here: SLURM copies the batch script into
# /var/spool/slurmd/jobNNN/ before running it, so BASH_SOURCE points at the
# spool copy and a "../.." fallback resolves to /var/spool/.venv (wrong).
# Require it explicitly instead of falling back to a bad path.
: "${VENV:?VENV must be exported by submit_tiles_batch.sh}"
source "$VENV/bin/activate"
# Clear any inherited PYTHONPATH: the CVMFS Python env puts its own
# site-packages on PYTHONPATH, which sits AHEAD of the venv on sys.path and
# shadows venv packages (numpy/h5py/typing_extensions load from /cvmfs).
unset PYTHONPATH

: "${TILE_POSTPROCESS_DIR:?TILE_POSTPROCESS_DIR must be exported by submit_tiles_batch.sh}"
: "${GROUP_PARENT_DIR:?GROUP_PARENT_DIR must be exported by submit_tiles_batch.sh}"
python "$TILE_POSTPROCESS_DIR/group_final_outputs.py"

echo "Grouping finished for run ${GROUP_RUN_NAME:-$(basename "$GROUP_PARENT_DIR")}."
