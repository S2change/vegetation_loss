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
# VENV is exported by submit_tile.sh. We do NOT derive it from ${BASH_SOURCE[0]}:
# SLURM copies the batch script into /var/spool/slurmd/jobNNN/ before running it,
# so BASH_SOURCE points at the spool copy and a "../.." fallback resolves to
# /var/spool/.venv (wrong). Require it explicitly. For a standalone re-run, pass
# it: `sbatch --export=ALL,VENV=/path/to/predict_pipeline/.venv,... run_aggregate_slurm.sh`.
: "${VENV:?VENV must be exported by submit_tile.sh (or passed for a standalone re-run)}"
source "$VENV/bin/activate"
# Clear any inherited PYTHONPATH: the CVMFS Python env puts its own
# site-packages on PYTHONPATH, which sits AHEAD of the venv on sys.path and
# shadows venv packages (numpy/h5py/typing_extensions load from /cvmfs, which
# breaks the venv's torch via the `TypeIs` import).
unset PYTHONPATH

: "${TILE_POSTPROCESS_DIR:?TILE_POSTPROCESS_DIR must be exported by submit_tile.sh}"
# Capture aggregate_tile.py's output (tee'd so the live log is unchanged) so we
# can read its "AGGREGATOR_PEAK_KB=" marker — the aggregator's own peak RSS,
# which sacct can't give us here (its MaxRSS isn't flushed while it's running).
_aggr_out="$(mktemp)"
python "$TILE_POSTPROCESS_DIR/aggregate_tile.py" 2>&1 | tee "$_aggr_out"

echo "Aggregator finished for tile $TILE_ID."

# ╔══════════════════════════════════════════════════════════════════════════╗
# ║ Run summary                                                                ║
# ║ This aggregator runs afterok the whole block array and is the LAST per-    ║
# ║ tile job, so it's the place to gather end-to-end stats (timing, memory)    ║
# ║ from sacct and write them to logs/00_summary.txt. All metrics are best-    ║
# ║ effort: a missing sacct value degrades to "n/a", never fails the job.     ║
# ╚══════════════════════════════════════════════════════════════════════════╝

# ── helpers ────────────────────────────────────────────────────────────────
_fmt_hms() {  # integer seconds -> "HH:MM:SS (N s)"
    local s="$1"
    printf '%02d:%02d:%02d (%d s)' $(( s / 3600 )) $(( (s % 3600) / 60 )) $(( s % 60 )) "$s"
}

_cpu_secs() {  # a TotalCPU field ("[DD-]HH:MM:SS[.ms]") -> integer seconds
    local v="$1" days=0 rest hh mm ss
    [[ -z "$v" || "$v" == "CPUTime" || "$v" == "TotalCPU" ]] && { echo 0; return; }
    if [[ "$v" == *-* ]]; then days="${v%%-*}"; rest="${v#*-}"; else rest="$v"; fi
    rest="${rest%%.*}"                     # drop fractional seconds
    IFS=: read -r hh mm ss <<< "$rest"
    [[ -z "$ss" ]] && { ss="$mm"; mm="$hh"; hh=0; }   # pad MM:SS-only
    echo $(( 10#$days*86400 + 10#$hh*3600 + 10#$mm*60 + 10#$ss ))
}

_sum_totalcpu() {  # sum TotalCPU over a job's per-task rows (skip sub-steps)
    local jobid="$1" total=0
    while IFS='|' read -r jid tcpu; do
        [[ "$jid" == *.* ]] && continue   # drop ".batch"/".extern"/step rows
        total=$(( total + $(_cpu_secs "$tcpu") ))
    done < <(sacct -j "$jobid" --format=JobID,TotalCPU --noheader --parsable2 2>/dev/null)
    echo "$total"
}

_mem_to_kb() {  # sacct mem field ("4096K"/"512M"/"2G"/"1.5G") -> integer KB
    local v="$1" num unit
    [[ -z "$v" || "$v" == "0" ]] && { echo 0; return; }
    unit="${v: -1}"; num="${v%[KMGT]}"
    case "$unit" in
        K) awk -v n="$num" 'BEGIN{printf "%d", n}' ;;
        M) awk -v n="$num" 'BEGIN{printf "%d", n*1024}' ;;
        G) awk -v n="$num" 'BEGIN{printf "%d", n*1024*1024}' ;;
        T) awk -v n="$num" 'BEGIN{printf "%d", n*1024*1024*1024}' ;;
        *) awk -v n="$v"   'BEGIN{printf "%d", n}' ;;   # bare number = bytes->KB? assume KB
    esac
}

# Peak MaxRSS across a job's rows, returning "<kb>|<jobid-of-peak>". sacct
# records MaxRSS on the STEP rows (".batch"/".extern"/".0"), not the top-level
# job/array-task row (which is usually blank for MaxRSS) — so unlike the CPU
# sum, we must KEEP the step rows here. We strip any ".<step>" suffix when
# reporting the id, so for an array job the peak is attributed to its task
# ("<arrayid>_<task>" = which block). Top-level rows that happen to carry a
# value are honoured too, so this works whichever way the cluster reports it.
_peak_maxrss() {
    local jobid="$1" peak_kb=0 peak_id="" kb
    while IFS='|' read -r jid maxrss; do
        [[ -z "$maxrss" || "$maxrss" == "MaxRSS" || "$maxrss" == "0" ]] && continue
        kb="$(_mem_to_kb "$maxrss")"
        if (( kb > peak_kb )); then peak_kb="$kb"; peak_id="${jid%%.*}"; fi
    done < <(sacct -j "$jobid" --format=JobID,MaxRSS --noheader --parsable2 2>/dev/null)
    echo "${peak_kb}|${peak_id}"
}

# Allocated memory (KB) for a job from AllocTRES (the "mem=" component). This is
# the denominator for "% of total" — the actual SLURM allocation, so it tracks
# cpus-per-task x the cluster's mem-per-cpu without hardcoding a constant.
_alloc_mem_kb() {
    local jobid="$1" tres
    tres="$(sacct -j "$jobid" --format=AllocTRES%-80 --noheader --parsable2 2>/dev/null \
            | grep -oE 'mem=[0-9.]+[KMGT]?' | head -n1)"
    [[ -z "$tres" ]] && { echo 0; return; }
    _mem_to_kb "${tres#mem=}"
}

_kb_to_gib() { awk -v k="$1" 'BEGIN{printf "%.2f", k/1024/1024}'; }
_pct() { awk -v a="$1" -v b="$2" 'BEGIN{ if (b>0) printf "%.1f", 100*a/b; else printf "n/a" }'; }

# ── timing ─────────────────────────────────────────────────────────────────
# Wall time: earliest block start (sacct, excludes queue/dependency wait) -> now.
wall_secs=""
if [[ -n "${ARRAY_JOB_ID:-}" ]]; then
    first_start="$(
        sacct -j "$ARRAY_JOB_ID" --format=Start --noheader --parsable2 2>/dev/null \
            | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}T' | sort | head -n1
    )"
    if [[ -n "$first_start" ]]; then
        start_epoch="$(date -d "$first_start" +%s 2>/dev/null || echo "")"
        [[ -n "$start_epoch" ]] && wall_secs=$(( $(date +%s) - start_epoch ))
    fi
fi

# CPU time: every block task + this aggregator, summed from sacct TotalCPU.
cpu_secs=""
if [[ -n "${ARRAY_JOB_ID:-}" ]]; then
    cpu_secs=$(( $(_sum_totalcpu "$ARRAY_JOB_ID") + $(_sum_totalcpu "${SLURM_JOB_ID:-}") ))
fi

# ── memory ─────────────────────────────────────────────────────────────────
# Blocks: peak MaxRSS across all block tasks (worst-case block — the one that
# would OOM), plus which block task owned it. Aggregator: its own MaxRSS. Each
# is shown as an absolute value and a % of that job's allocated memory.
blocks_peak_kb=0; blocks_peak_id=""; blocks_alloc_kb=0
if [[ -n "${ARRAY_JOB_ID:-}" ]]; then
    IFS='|' read -r blocks_peak_kb blocks_peak_id < <(_peak_maxrss "$ARRAY_JOB_ID")
    blocks_alloc_kb="$(_alloc_mem_kb "$ARRAY_JOB_ID")"
fi
# Aggregator's own peak: read the AGGREGATOR_PEAK_KB marker that aggregate_tile.py
# emitted (its ru_maxrss = kernel peak RSS, in KiB on Linux). This measures the
# Python process that does the memory-heavy stitch — sacct can't, since this
# job's MaxRSS isn't flushed while it's still running (that was the n/a cause).
aggr_peak_kb=0; aggr_alloc_kb=0
aggr_peak_kb="$(grep -oE 'AGGREGATOR_PEAK_KB=[0-9]+' "$_aggr_out" 2>/dev/null | tail -n1 | cut -d= -f2)"
[[ -z "$aggr_peak_kb" ]] && aggr_peak_kb=0
if [[ -n "${SLURM_JOB_ID:-}" ]]; then
    aggr_alloc_kb="$(_alloc_mem_kb "$SLURM_JOB_ID")"
fi
rm -f "$_aggr_out"

# Count the block tasks that actually ran (array task rows, skip sub-steps), so
# the batch rollup can compute block CPU-count as THREADS x tasks-run per tile.
n_block_tasks=0
if [[ -n "${ARRAY_JOB_ID:-}" ]]; then
    n_block_tasks="$(
        sacct -j "$ARRAY_JOB_ID" --format=JobID --noheader --parsable2 2>/dev/null \
            | grep -E '^[0-9]+_[0-9]+$' | sort -u | wc -l | tr -d ' '
    )"
fi

# ── echo the headline timing lines to the aggregator log (as before) ────────
if [[ -n "$wall_secs" ]]; then
    echo "Total tile wall time (first block start -> aggregator finish): $(_fmt_hms "$wall_secs"), tile $TILE_ID"
else
    echo "Total tile wall time: unavailable (sacct/ARRAY_JOB_ID)."
fi
if [[ -n "$cpu_secs" && "$cpu_secs" -gt 0 ]]; then
    echo "Total tile CPU time (all blocks + aggregator): $(_fmt_hms "$cpu_secs"), tile $TILE_ID"
else
    echo "Total tile CPU time: unavailable (sacct/ARRAY_JOB_ID)."
fi

# ── write logs/00_summary.txt ────────────────────────────────────────────────
# "00_" prefix sorts it to the top of the logs/ directory. CPUs/task = THREADS
# (submit_tile.sh sets --cpus-per-task=THREADS); the aggregator is hardcoded to
# 3 cpus (see #SBATCH above). Inputs are reconstructed from the exported env.
SUMMARY="${LOG_DIR:-.}/00_summary.txt"
{
    echo "════════════════════════════════════════════════════════════════════"
    echo " RUN SUMMARY — tile ${TILE_ID:-?}"
    echo " generated $(date '+%Y-%m-%d %H:%M:%S') by aggregator job ${SLURM_JOB_ID:-?}"
    echo "════════════════════════════════════════════════════════════════════"
    echo
    echo "── Inputs ──────────────────────────────────────────────────────────"
    echo "  Tile:            ${TILE_ID:-?}"
    echo "  HDF5:            ${TILE_HDF5_PATH:-?}"
    echo "  Model:           ${MODEL:-?}   (data dtype ${DATA_DTYPE:-?})"
    echo "  Date window:     ${START_DATE:-?} -> ${END_DATE:-?}"
    echo "  Read window:     ${READ_START_DATE:-unbounded} -> ${READ_END_DATE:-unbounded}"
    if [[ -n "${DATE_CLUSTERS:-}" ]]; then
        echo "  Date clusters:   on ($(printf '%s' "$DATE_CLUSTERS" | awk -F';' '{print NF}') clusters)"
    else
        echo "  Date clusters:   off (raw timesteps)"
    fi
    echo "  Block grid:      ${N_ROWS:-?} x ${N_COLS:-?}  (${N_BLOCKS:-?} blocks)"
    echo "  Processing:      ${SELECT_DESC:-?}"
    echo "  Vote classes:    ${VOTE_CLASSES:-?}   threshold ${VOTE_THRESHOLD:-?}"
    echo "  Batch size:      ${BATCH_SIZE:-?}"
    echo "  Patch floors:    block ${MIN_PATCH_M2:-?} m^2 / tile ${MIN_TILE_PATCH_M2:-?} m^2"
    echo "  Max comp. days:  ${MAX_COMPOSITE_DAYS:-unbounded}"
    echo "  Confidence:      ${OUTPUT_CONFIDENCE:-0}"
    echo "  Weights:         ${WEIGHTS_PATH:-?}"
    echo
    echo "── Resources ───────────────────────────────────────────────────────"
    echo "  CPUs/block task: ${THREADS:-?}   (max ${MAX_CONCURRENT:-?} block tasks concurrent)"
    echo "  CPUs/aggregator: ${SLURM_CPUS_PER_TASK:-3}"
    echo
    echo "── Timing ──────────────────────────────────────────────────────────"
    if [[ -n "$wall_secs" ]]; then
        echo "  Total wall time: $(_fmt_hms "$wall_secs")   (first block start -> aggregator finish)"
    else
        echo "  Total wall time: n/a"
    fi
    if [[ -n "$cpu_secs" && "$cpu_secs" -gt 0 ]]; then
        echo "  Total CPU time:  $(_fmt_hms "$cpu_secs")   (all blocks + aggregator)"
    else
        echo "  Total CPU time:  n/a"
    fi
    echo
    echo "── Peak memory ─────────────────────────────────────────────────────"
    if (( blocks_peak_kb > 0 )); then
        echo "  Blocks (peak):   $(_kb_to_gib "$blocks_peak_kb") GiB of $(_kb_to_gib "$blocks_alloc_kb") GiB allocated ($(_pct "$blocks_peak_kb" "$blocks_alloc_kb")%)  [block task ${blocks_peak_id:-?}]"
    else
        echo "  Blocks (peak):   n/a (no MaxRSS in sacct)"
    fi
    if (( aggr_peak_kb > 0 )); then
        echo "  Aggregation:     $(_kb_to_gib "$aggr_peak_kb") GiB of $(_kb_to_gib "$aggr_alloc_kb") GiB allocated ($(_pct "$aggr_peak_kb" "$aggr_alloc_kb")%)"
    else
        echo "  Aggregation:     n/a (no MaxRSS in sacct)"
    fi
    echo "════════════════════════════════════════════════════════════════════"
} > "$SUMMARY"

echo "Wrote run summary: $SUMMARY"

# ── machine-readable metrics for the batch rollup ────────────────────────────
# submit_tiles_batch.sh's grouping job reads one of these per tile and rolls
# them into a batch-level 00_summary.txt. key=value, one per line — robust to
# the human summary's layout changing. Shared inputs are written too so the
# batch summary can show them once (they're identical across tiles).
METRICS="${LOG_DIR:-.}/00_summary.metrics"
{
    echo "TILE_ID=${TILE_ID:-}"
    echo "WALL_SECS=${wall_secs:-}"
    echo "CPU_SECS=${cpu_secs:-}"
    echo "THREADS=${THREADS:-}"
    echo "N_BLOCK_TASKS=${n_block_tasks:-0}"
    echo "AGGR_CPUS=${SLURM_CPUS_PER_TASK:-3}"
    echo "BLOCKS_PEAK_KB=${blocks_peak_kb:-0}"
    echo "BLOCKS_PEAK_ID=${blocks_peak_id:-}"
    echo "BLOCKS_ALLOC_KB=${blocks_alloc_kb:-0}"
    echo "AGGR_PEAK_KB=${aggr_peak_kb:-0}"
    echo "AGGR_ALLOC_KB=${aggr_alloc_kb:-0}"
    # Shared inputs (identical across a batch's tiles).
    echo "MODEL=${MODEL:-}"
    echo "DATA_DTYPE=${DATA_DTYPE:-}"
    echo "START_DATE=${START_DATE:-}"
    echo "END_DATE=${END_DATE:-}"
    echo "READ_START_DATE=${READ_START_DATE:-}"
    echo "READ_END_DATE=${READ_END_DATE:-}"
    echo "DATE_CLUSTERS_N=$( [[ -n "${DATE_CLUSTERS:-}" ]] && printf '%s' "$DATE_CLUSTERS" | awk -F';' '{print NF}' || echo 0 )"
    echo "VOTE_CLASSES=${VOTE_CLASSES:-}"
    echo "VOTE_THRESHOLD=${VOTE_THRESHOLD:-}"
    echo "BATCH_SIZE=${BATCH_SIZE:-}"
    echo "MIN_PATCH_M2=${MIN_PATCH_M2:-}"
    echo "MIN_TILE_PATCH_M2=${MIN_TILE_PATCH_M2:-}"
    echo "MAX_COMPOSITE_DAYS=${MAX_COMPOSITE_DAYS:-}"
    echo "OUTPUT_CONFIDENCE=${OUTPUT_CONFIDENCE:-0}"
    echo "WEIGHTS_PATH=${WEIGHTS_PATH:-}"
} > "$METRICS"
