#!/bin/bash
#
# Block-error report. Submitted by submit_tile.sh with --dependency=afterany on
# the per-block array job, so it ALWAYS runs once the array finishes — whether
# the blocks succeeded or not. (The aggregator uses afterok and so never starts
# if a block fails; this step is the one that runs regardless, so you can see
# *which* blocks failed instead of a silently-stuck aggregator.)
#
# Writes logs/block_errors.txt listing any block (row, col) that did not finish
# successfully, judged from two signals:
#   1. SLURM task state for the array job (sacct) — FAILED / TIMEOUT / OOM /
#      CANCELLED etc. is a failure.
#   2. Missing per-block output .npz ({TILE_ID}_block_{row:03d}_{col:03d}.npz in
#      BLOCK_OUTPUT_DIR) — an expected block with no output is a failure.
# A block flagged by either signal is reported.

#SBATCH --time=00:10:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --partition=dgt
#SBATCH --account=cpca070342024
#SBATCH --qos=cpca070342024

set -euo pipefail

: "${TILE_ID:?TILE_ID must be exported by submit_tile.sh}"
: "${LOG_DIR:?LOG_DIR must be exported by submit_tile.sh}"
: "${BLOCK_OUTPUT_DIR:?BLOCK_OUTPUT_DIR must be exported by submit_tile.sh}"
: "${N_COLS:?N_COLS must be exported by submit_tile.sh}"
: "${ARRAY_JOB_ID:?ARRAY_JOB_ID must be exported by submit_tile.sh}"
# Space-separated linear array indices that were submitted (row*N_COLS + col).
: "${EXPECTED_ARRAY_IDS:?EXPECTED_ARRAY_IDS must be exported by submit_tile.sh}"

REPORT="$LOG_DIR/block_errors.txt"

# ── Gather per-task SLURM states for the array ────────────────────────────
# sacct rows look like "<jobid>_<taskidx>|<State>|<ExitCode>". We capture the
# raw output once and look each task up on demand below — no associative array,
# so this stays portable to older bash. Empty if sacct is unavailable.
SACCT_OUT=""
if ! SACCT_OUT="$(sacct -j "$ARRAY_JOB_ID" \
        --format=JobID,State,ExitCode --noheader --parsable2 2>/dev/null)"; then
    echo "[warn] sacct unavailable for $ARRAY_JOB_ID; relying on output presence only." >&2
    SACCT_OUT=""
fi

# Echo the SLURM state for one array index, or "" if not found. Matches the
# exact task row "<jobid>_<idx>|" and skips ".batch"/".extern" sub-steps; trims
# any trailing detail like "CANCELLED by 12345" to the leading word.
_state_for_idx() {
    local idx="$1"
    printf '%s\n' "$SACCT_OUT" \
        | grep -E "^${ARRAY_JOB_ID}_${idx}\|" \
        | head -n1 \
        | awk -F'|' '{print $2}' \
        | awk '{print $1}'
}

# ── Check each expected block ─────────────────────────────────────────────
failed=()
for idx in $EXPECTED_ARRAY_IDS; do
    row=$(( idx / N_COLS ))
    col=$(( idx % N_COLS ))
    npz="$BLOCK_OUTPUT_DIR/$(printf '%s_block_%03d_%03d.npz' "$TILE_ID" "$row" "$col")"

    state="$(_state_for_idx "$idx")"
    reason=""
    # A task is OK only if SLURM says COMPLETED. Any other terminal state is a
    # failure. If sacct gave us nothing for this idx, fall back to output check.
    if [[ -n "$state" && "$state" != "COMPLETED" ]]; then
        reason="state=$state"
    elif [[ ! -f "$npz" ]]; then
        reason="${state:+state=$state, }missing output"
    fi

    if [[ -n "$reason" ]]; then
        failed+=("$(printf 'block (%d, %d)  [array idx %d]  %s' "$row" "$col" "$idx" "$reason")")
    fi
done

# ── Write the report ──────────────────────────────────────────────────────
{
    echo "Block error report for tile $TILE_ID"
    echo "Array job: $ARRAY_JOB_ID"
    echo "Generated: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "Expected blocks: $(printf '%s\n' $EXPECTED_ARRAY_IDS | wc -w)"
    echo
    if [[ ${#failed[@]} -eq 0 ]]; then
        echo "All blocks processed successfully."
    else
        echo "${#failed[@]} block(s) FAILED to process:"
        printf '  %s\n' "${failed[@]}"
        echo
        echo "Per-block logs: $LOG_DIR/predict_block_<array idx>.out (and .err)"
    fi
} | tee "$REPORT"

echo
echo "Block error report written to: $REPORT"
