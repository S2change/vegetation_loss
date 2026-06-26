#!/bin/bash
#
# Batch wrapper around submit_tile.sh — submit the full prediction pipeline for
# many tiles in one command. Each tile is submitted independently (its own SLURM
# array job + aggregator), so tiles run in parallel and SLURM spreads their block
# tasks across nodes. Per-tile load is still bounded by submit_tile.sh's
# MAX_CONCURRENT cap, so submitting all tiles at once is safe — the scheduler
# queues whatever doesn't fit.
#
# The only thing that differs between tiles is the HDF5 file; every other input
# (START_DATE, END_DATE, clustering settings, ...) is identical and forwarded
# verbatim to each submit_tile.sh call.
#
# Run on the login node. Two ways to choose tiles:
#
#   # 1. All *.h5 in a directory:
#   ./submit_tiles_batch.sh \
#       BASE_OUTPUT_DIR=/users1/cpca070342024/shared/predict_outputs/13_summer2023 \
#       TILE_DIR=/users1/dgt/hdf5/ \
#       START_DATE=2023-07-01 END_DATE=2023-09-15 \
#       USE_DATE_CLUSTERS=1 MAX_THETA=5 MAX_CLUSTER_AMPLITUDE=5
#
#   # 2. An explicit subset (TILE_DIR defaults to /users1/dgt/hdf5/):
#   ./submit_tiles_batch.sh \
#       BASE_OUTPUT_DIR=/users1/cpca070342024/shared/predict_outputs/13_summer2023 \
#       TILES="T29SNB T29TPE T29TPG" \
#       START_DATE=2023-07-01 END_DATE=2023-09-15
#
# After every tile's aggregator finishes, one final grouping job merges all the
# per-tile final_outputs/<TILE>_tile.gpkg into a single combined
# <BASE_OUTPUT_DIR>/<run_name>.gpkg (run_group_slurm.sh -> group_final_outputs.py).
# It depends afterany on every aggregator, so a failed tile still lets the rest
# be merged (the merge reports any missing tile .gpkg).
#
# Wrapper-only knobs (consumed here, NOT forwarded to submit_tile.sh):
#   BASE_OUTPUT_DIR  (required) per-tile output goes to <BASE_OUTPUT_DIR>/<TILE_ID>/.
#   TILE_DIR         directory of tile .h5 files. Default /users1/dgt/hdf5/.
#   TILES            space-separated tile IDs (e.g. "T29SNB T29TPE"). When set,
#                    only these are run; each path = <TILE_DIR>/<ID>.h5. Unset =
#                    every *.h5 in TILE_DIR.
#   GROUP_RUN_NAME   base name for the combined .gpkg. Default: the basename of
#                    BASE_OUTPUT_DIR. Output is <BASE_OUTPUT_DIR>/<run_name>.gpkg.
#
# Per tile the wrapper derives TILE_ID (the .h5 filename without extension) and
# OUTPUT_DIR=<BASE_OUTPUT_DIR>/<TILE_ID>, then calls submit_tile.sh. Passing
# TILE_ID, TILE_HDF5_PATH, or OUTPUT_DIR here is an error (they are per-tile).
#
# Every other KEY=VALUE is passed straight through to each submit_tile.sh call;
# submit_tile.sh validates them, so see its header for the full knob list.

set -euo pipefail

# submit_tile.sh sits next to this script. Resolve via $0 so the wrapper works
# regardless of the invocation cwd (mirrors submit_tile.sh's DISTRIBUTE_DIR).
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SUBMIT_TILE="$SCRIPT_DIR/submit_tile.sh"
if [[ ! -x "$SUBMIT_TILE" ]]; then
    echo "submit_tile.sh not found / not executable at: $SUBMIT_TILE" >&2
    exit 1
fi
# Run-level grouping wrapper (merges all tiles' final .gpkg into one). Submitted
# once after every tile's aggregator finishes (see end of script).
TILE_POSTPROCESS_DIR="$SCRIPT_DIR/processes/tile_postprocess"
RUN_GROUP="$TILE_POSTPROCESS_DIR/run_group_slurm.sh"

# ── Parse KEY=VALUE args ───────────────────────────────────────────────────
# Wrapper-only keys are pulled out; everything else is collected for pass-
# through. Reserved per-tile keys are rejected (computed per tile below).
BASE_OUTPUT_DIR=""
TILE_DIR="/users1/dgt/hdf5/"
TILES=""
GROUP_RUN_NAME=""
passthrough=()

for arg in "$@"; do
    if [[ "$arg" != *=* ]]; then
        echo "Bad arg: $arg (expected KEY=VALUE)" >&2
        exit 1
    fi
    key="${arg%%=*}"
    val="${arg#*=}"
    case "$key" in
        BASE_OUTPUT_DIR) BASE_OUTPUT_DIR="$val" ;;
        TILE_DIR)        TILE_DIR="$val" ;;
        TILES)           TILES="$val" ;;
        GROUP_RUN_NAME)  GROUP_RUN_NAME="$val" ;;
        TILE_ID|TILE_HDF5_PATH|OUTPUT_DIR)
            echo "$key is computed per tile and must not be set on the batch " \
                 "wrapper. Use BASE_OUTPUT_DIR (+ TILES / TILE_DIR) instead." >&2
            exit 1
            ;;
        *) passthrough+=("$arg") ;;
    esac
done

# ── Validate wrapper inputs ────────────────────────────────────────────────
: "${BASE_OUTPUT_DIR:?BASE_OUTPUT_DIR is required}"
if [[ ! -d "$TILE_DIR" ]]; then
    echo "TILE_DIR is not a directory: $TILE_DIR" >&2
    exit 1
fi
TILE_DIR="${TILE_DIR%/}"   # normalize: drop any trailing slash

# ── Build the list of HDF5 paths to process ────────────────────────────────
# Explicit TILES → <TILE_DIR>/<ID>.h5 each. Otherwise glob every *.h5 in
# TILE_DIR (and require at least one).
tile_paths=()
if [[ -n "$TILES" ]]; then
    for id in $TILES; do
        tile_paths+=("$TILE_DIR/$id.h5")
    done
else
    shopt -s nullglob
    for p in "$TILE_DIR"/*.h5; do
        tile_paths+=("$p")
    done
    shopt -u nullglob
    if [[ ${#tile_paths[@]} -eq 0 ]]; then
        echo "No .h5 files found in $TILE_DIR (and no TILES given)." >&2
        exit 1
    fi
fi

echo "Batch submit:"
echo "  submit_tile.sh:  $SUBMIT_TILE"
echo "  base output:     $BASE_OUTPUT_DIR"
echo "  tile dir:        $TILE_DIR"
echo "  tiles:           ${#tile_paths[@]} candidate(s)"
echo "  shared args:     ${passthrough[*]+"${passthrough[*]}"}"
echo

# ── Submit one tile at a time ──────────────────────────────────────────────
# Missing .h5 (e.g. a typo in TILES) is skipped with a warning. A submit_tile.sh
# failure for one tile is recorded but does not abort the batch, so a single bad
# tile can't take down the other 16. Counts are summarized at the end.
n_submitted=0
n_missing=0
n_failed=0
failed_tiles=()
aggr_job_ids=()   # collected per tile to chain the run-level grouping job

for path in "${tile_paths[@]}"; do
    tile_id="$(basename "$path" .h5)"
    out_dir="$BASE_OUTPUT_DIR/$tile_id"

    if [[ ! -f "$path" ]]; then
        echo "[skip] $tile_id — HDF5 not found: $path" >&2
        n_missing=$((n_missing + 1))
        continue
    fi

    echo "=== submitting $tile_id -> $out_dir ==="
    # `|| status=$?` keeps `set -e` from killing the batch on one failure.
    status=0
    "$SUBMIT_TILE" \
        TILE_ID="$tile_id" \
        TILE_HDF5_PATH="$path" \
        OUTPUT_DIR="$out_dir" \
        ${passthrough[@]+"${passthrough[@]}"} \
        || status=$?
    if [[ "$status" -ne 0 ]]; then
        echo "[fail] $tile_id — submit_tile.sh exited $status" >&2
        n_failed=$((n_failed + 1))
        failed_tiles+=("$tile_id")
    else
        n_submitted=$((n_submitted + 1))
        # submit_tile.sh records its aggregator job id here; collect it so the
        # grouping job can depend on every tile's aggregator. Missing/empty
        # (older submit_tile.sh) is non-fatal — that tile just won't gate the
        # group job.
        aggr_id_file="$out_dir/logs/aggregate_job_id.txt"
        if [[ -s "$aggr_id_file" ]]; then
            aggr_job_ids+=("$(< "$aggr_id_file")")
        else
            echo "[warn] $tile_id — no aggregator job id at $aggr_id_file" >&2
        fi
    fi
    echo
done

# ── Run-level grouping job ──────────────────────────────────────────────────
# Merge every tile's final_outputs/<TILE>_tile.gpkg into one combined
# <BASE_OUTPUT_DIR>/<run_name>.gpkg, once all tiles have finished. afterany (not
# afterok) on every aggregator so a single failed tile still lets the rest be
# merged — group_final_outputs.py reports any tile whose .gpkg is missing.
# Submitted before the failure-exit below so partial batches still get grouped.
if [[ ${#aggr_job_ids[@]} -gt 0 && -x "$RUN_GROUP" ]]; then
    dep="$(IFS=:; echo "afterany:${aggr_job_ids[*]}")"
    export TILE_POSTPROCESS_DIR
    # Absolute path: the SLURM job's cwd may differ from this login-node shell.
    export GROUP_PARENT_DIR="$(cd "$BASE_OUTPUT_DIR" && pwd)"
    export GROUP_RUN_NAME="${GROUP_RUN_NAME:-}"
    GROUP_JOB_ID=$(
        sbatch --parsable \
            --dependency="$dep" \
            --export=ALL \
            --output="$BASE_OUTPUT_DIR/group_final_outputs.out" \
            --error="$BASE_OUTPUT_DIR/group_final_outputs.err" \
            --job-name="group_$(basename "$BASE_OUTPUT_DIR")" \
            "$RUN_GROUP"
    )
    echo "Submitted grouping job:   $GROUP_JOB_ID  ($dep over ${#aggr_job_ids[@]} aggregator(s))"
    echo "Combined output will be:  $BASE_OUTPUT_DIR/${GROUP_RUN_NAME:-$(basename "$BASE_OUTPUT_DIR")}.gpkg"
    echo "Grouping log:             $BASE_OUTPUT_DIR/group_final_outputs.out"
else
    echo "No grouping job submitted (no aggregator job ids collected)." >&2
fi

# ── Summary ────────────────────────────────────────────────────────────────
echo "Batch complete: $n_submitted submitted, $n_missing skipped (missing), " \
     "$n_failed failed."
if [[ "$n_failed" -gt 0 ]]; then
    echo "Failed tiles: ${failed_tiles[*]}" >&2
    exit 1
fi
echo "Watch with:  squeue -u \$USER"
