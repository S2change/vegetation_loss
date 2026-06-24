#!/bin/bash
#
# Aggregator wrapper. Submitted by submit_tile.sh with --dependency=afterok
# on the per-block array job, so this only runs once every block .npz is
# on disk.

#SBATCH --time=00:30:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=3
#SBATCH --partition=dgt
#SBATCH --account=cpca070342024
#SBATCH --qos=cpca070342024

set -euo pipefail

module purge
module load gcc13/openmpi/4.1.6
source "${VENV:-/users1/cpca070342024/shared/vchips/venv}/bin/activate"

: "${DISTRIBUTE_DIR:?DISTRIBUTE_DIR must be exported by submit_tile.sh}"
python "$DISTRIBUTE_DIR/aggregate_tile.py"

echo "Aggregator finished for tile $TILE_ID."

# ── End-to-end tile wall time ─────────────────────────────────────────────
# This aggregator runs afterok the whole block array and is the last step, so
# "now" is the tile's finish. The tile's start is the EARLIEST block start time
# across the array job — read from sacct (the authoritative SLURM record), so
# queue/dependency wait is excluded and it matches reading the logs by hand.
# Best-effort: a single line, and never fail the job if sacct is unavailable.
if [[ -n "${ARRAY_JOB_ID:-}" ]]; then
    # sacct Start is per-task; take the min over the array's task rows. Skip
    # 'Unknown'/'None' (tasks that never started) and the array's parent row.
    first_start="$(
        sacct -j "$ARRAY_JOB_ID" --format=Start --noheader --parsable2 2>/dev/null \
            | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' | sort | head -n1
    )"
    if [[ -n "$first_start" ]]; then
        start_epoch="$(date -d "$first_start" +%s 2>/dev/null || echo "")"
        if [[ -n "$start_epoch" ]]; then
            end_epoch="$(date +%s)"
            elapsed=$(( end_epoch - start_epoch ))
            printf 'Total tile wall time (first block start -> aggregator finish): %02d:%02d:%02d (%d s), tile %s\n' \
                $(( elapsed / 3600 )) $(( (elapsed % 3600) / 60 )) $(( elapsed % 60 )) \
                "$elapsed" "$TILE_ID"
        else
            echo "Total tile wall time: could not parse block start '$first_start'."
        fi
    else
        echo "Total tile wall time: no block start time found in sacct for job $ARRAY_JOB_ID."
    fi
else
    echo "Total tile wall time: ARRAY_JOB_ID not set; cannot compute."
fi
