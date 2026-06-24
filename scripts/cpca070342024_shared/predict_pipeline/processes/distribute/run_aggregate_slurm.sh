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
# VENV is normally exported by submit_tile.sh (predict_pipeline/.venv). The
# fallback derives the same path from this script's location: it lives in
# processes/distribute/, so the pipeline root (holding .venv) is two levels up.
source "${VENV:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/.venv}/bin/activate"
# Clear any inherited PYTHONPATH: the CVMFS Python env puts its own
# site-packages on PYTHONPATH, which sits AHEAD of the venv on sys.path and
# shadows venv packages (numpy/h5py/typing_extensions load from /cvmfs, which
# breaks the venv's torch via the `TypeIs` import).
unset PYTHONPATH

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

# ── End-to-end tile CPU time ──────────────────────────────────────────────
# Total processor time actually consumed across the whole tile — every block
# task PLUS this aggregator — summed from sacct's TotalCPU (user+sys per step).
# This is the compute burned, distinct from the wall time above (which is just
# elapsed clock time). Best-effort; one line; never fails the job.
#
# TotalCPU prints as [DD-]HH:MM:SS[.mmm]; _cpu_secs converts one such value to
# whole seconds. We sum over the array job's task rows and the aggregator's own
# job, skipping the parent/.batch/.extern sub-step rows sacct also emits.
_cpu_secs() {  # echo a TotalCPU field ("[DD-]HH:MM:SS[.ms]") as integer seconds
    local v="$1" days=0 rest hh mm ss
    [[ -z "$v" || "$v" == "CPUTime" ]] && { echo 0; return; }
    if [[ "$v" == *-* ]]; then days="${v%%-*}"; rest="${v#*-}"; else rest="$v"; fi
    rest="${rest%%.*}"                     # drop fractional seconds
    IFS=: read -r hh mm ss <<< "$rest"
    # pad missing fields when sacct emits MM:SS only
    [[ -z "$ss" ]] && { ss="$mm"; mm="$hh"; hh=0; }
    echo $(( 10#$days*86400 + 10#$hh*3600 + 10#$mm*60 + 10#$ss ))
}

_sum_totalcpu() {  # sum TotalCPU over a job's per-task rows (skip sub-steps)
    local jobid="$1" total=0 line
    while IFS='|' read -r jid tcpu; do
        # keep real job/task rows; drop ".batch"/".extern"/"...0" step rows
        [[ "$jid" == *.* ]] && continue
        total=$(( total + $(_cpu_secs "$tcpu") ))
    done < <(sacct -j "$jobid" --format=JobID,TotalCPU --noheader --parsable2 2>/dev/null)
    echo "$total"
}

if [[ -n "${ARRAY_JOB_ID:-}" ]]; then
    blocks_cpu="$(_sum_totalcpu "$ARRAY_JOB_ID")"
    aggr_cpu="$(_sum_totalcpu "${SLURM_JOB_ID:-}")"
    cpu=$(( blocks_cpu + aggr_cpu ))
    if (( cpu > 0 )); then
        printf 'Total tile CPU time (all blocks + aggregator): %02d:%02d:%02d (%d s), tile %s\n' \
            $(( cpu / 3600 )) $(( (cpu % 3600) / 60 )) $(( cpu % 60 )) \
            "$cpu" "$TILE_ID"
    else
        echo "Total tile CPU time: no TotalCPU found in sacct for job $ARRAY_JOB_ID."
    fi
else
    echo "Total tile CPU time: ARRAY_JOB_ID not set; cannot compute."
fi
