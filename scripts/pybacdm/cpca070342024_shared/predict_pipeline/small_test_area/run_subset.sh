#!/bin/bash
#
# Carve a small chip-chunked HDF5 out of a full-tile one, by a polygon .gpkg.
# Thin wrapper around subset_hdf5_to_block.py.
#
# Runs either directly on a login/local machine:
#   ./run_subset.sh SRC=T29TME.h5 GPKG=fire_cut_test_block.gpkg OUT=T29TME_testblock.h5
#
# or as a SLURM batch job (the #SBATCH lines below take effect under sbatch
# and are ignored when run directly):
#   sbatch run_subset.sh SRC=... GPKG=... OUT=...
#
# Args are KEY=VALUE (all optional — the Python script has defaults):
#   SRC         full-tile chip-chunked HDF5      (default: T29TME.h5)
#   GPKG        test-cell polygon                (default: fire_cut_test_block.gpkg)
#   FIRES       fire reference .gpkg (fogo)      (default: Data_ref_2023_icnf.gpkg)
#   CUTS        cut reference .gpkg (corte)      (default: Data_ref_2023_nvg_v2.gpkg)
#   OUT         output HDF5                      (default: <SRC stem>_testblock.h5)
#   PAD_BLOCKS  whole-block ghost rings to keep  (default: 1 -> LIVE at block (1,1))
#   VENV        virtualenv to activate (cluster only)
#
# FIRES/CUTS are the change layers — the LIVE area is snapped to the source
# blocks that actually contain change inside the cell (guarantees single-block
# alignment without cutting any change).

#SBATCH --job-name=subset_hdf5
#SBATCH --time=00:30:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --partition=fct
#SBATCH --account=cpca070342024
#SBATCH --qos=cpca070342024

set -euo pipefail

# Parse KEY=VALUE args into env vars.
for arg in "$@"; do
    if [[ "$arg" == *=* ]]; then
        export "$arg"
    else
        echo "Bad arg: $arg (expected KEY=VALUE)" >&2
        exit 1
    fi
done

# Resolve the script directory. Under SLURM the batch script is spooled to
# /var/spool/slurmd/..., so prefer $SLURM_SUBMIT_DIR (the dir sbatch ran from);
# fall back to the script's own location for direct runs.
HERE="${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"

# Activate the venv only on the cluster (SLURM sets SLURM_JOB_ID). Locally we
# assume the right Python is already on PATH.
if [ -n "${SLURM_JOB_ID:-}" ]; then
    module purge
    module load gcc13/openmpi/4.1.6
    source "${VENV:-/users1/cpca070342024/shared/vchips/venv}/bin/activate"
fi

# Build the python args, passing only the ones that were set so the script's
# own defaults apply otherwise.
ARGS=()
[ -n "${SRC:-}" ]        && ARGS+=(--src        "$SRC")
[ -n "${GPKG:-}" ]       && ARGS+=(--gpkg       "$GPKG")
[ -n "${FIRES:-}" ]      && ARGS+=(--fires      "$FIRES")
[ -n "${CUTS:-}" ]       && ARGS+=(--cuts       "$CUTS")
[ -n "${OUT:-}" ]        && ARGS+=(--out        "$OUT")
[ -n "${PAD_BLOCKS:-}" ] && ARGS+=(--pad-blocks "$PAD_BLOCKS")

echo "Running: python $HERE/subset_hdf5_to_block.py ${ARGS[*]}"
python "$HERE/subset_hdf5_to_block.py" "${ARGS[@]}"
