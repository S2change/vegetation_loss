#!/bin/bash
#
# Submit a full-tile prediction pipeline to SLURM.
#
# Steps:
#   1. Read the tile's chip-chunked HDF5 to discover the block grid shape
#      (N_BLOCK_ROWS x N_BLOCK_COLS).
#   2. Submit an array job — one task per (block_row, block_col) — that
#      runs `predict_block.py` and writes one .npz + .gpkg per block into
#      OUTPUT_DIR/block_outputs/.
#   3. Submit an aggregator job with `--dependency=afterok:<array_job>` that
#      stitches the per-block shards into tile-level outputs in
#      OUTPUT_DIR/final_outputs/ (.gpkg .parquet .npz .tif).
#
# OUTPUT_DIR layout:
#   OUTPUT_DIR/logs/          SLURM .out/.err
#   OUTPUT_DIR/block_outputs/ per-block .npz + .gpkg
#   OUTPUT_DIR/final_outputs/ tile-level .gpkg/.parquet/.npz/.tif
#
# Run on the login node (the default USE_DATE_CLUSTERS=1 derives the target
# dates from the acquisition calendar, so just give the START/END window):
#   ./submit_tile.sh \
#       TILE_ID=T29TPG \
#       TILE_HDF5_PATH=/users1/cpca070342024/shared/hdf5/T29TPG_48ts_20251028_20251229.h5 \
#       START_DATE=2023-01-01 \
#       END_DATE=2023-12-31 \
#       OUTPUT_DIR=/users1/cpca070342024/shared/predict_outputs/T29TPG_run01
#
# Target dates — the default USE_DATE_CLUSTERS=1 derives them from the
# acquisition calendar over START_DATE..END_DATE (TARGET_DATES must NOT be
# set). To pick dates yourself instead, set USE_DATE_CLUSTERS=0 and give
# EITHER an explicit TARGET_DATES, OR a START_DATE + END_DATE span (then
# generated automatically, one every TARGET_STEP_DAYS=45 days):
#   ./submit_tile.sh TILE_ID=... TILE_HDF5_PATH=... OUTPUT_DIR=... \
#       USE_DATE_CLUSTERS=0 TARGET_DATES=2025-11-15,2025-12-01
#
# Optional knobs (KEY=VALUE):
#   MODEL=enet_16bit    model package directory name under <shared>/models/
#                       (sibling of distribute/). Each model package exposes the
#                       same interface (predict.load_model,
#                       predict.predict_before_after_chips, DEFAULT_WEIGHTS,
#                       CLOSING_RADII — see bacdm/__init__.py for the
#                       contract), so switching model is just e.g.
#                       MODEL=bacdm. Default: enet_16bit.
#   DATA_DTYPE=u16      input data dtype for the read->composite->shift chain.
#                       u16 (default) keeps raw uint16 reflectance (nodata
#                       65535) — for models that scale natively
#                       (enet_16bit). u8 applies the
#                       q02/q98 stretch (uint8, nodata 255) — bacdm /
#                       enet_8bit. Match this to the model: a u16 model
#                       on u8 data (or vice versa) silently produces garbage.
#   THREADS=2           CPU threads per task. Also sets --cpus-per-task so the
#                       allocation matches. A thread sweep showed ~95% scaling
#                       at 2 threads, ~68% at 4 — 2 is the efficient default.
#   MAX_CONCURRENT=8   max array tasks running at once (--array %N cap).
#                       With THREADS, keep THREADS*MAX_CONCURRENT well under the
#                       node's core count to avoid memory-bandwidth saturation.
#   WEIGHTS_PATH=...    .pth checkpoint. Default: the model package's
#                       DEFAULT_WEIGHTS (a checkpoint inside the model dir).
#   BATCH_SIZE=8        model batch size.
#   VOTE_CLASSES=1,2    non-bg class IDs to vote on. 1 = Cuts, 2 = Fires. Class
#                       names are in the model package's config (e.g.
#                       bacdm/AAA_Configs.py, enet_8bit/configs.py).
#   VOTE_THRESHOLD=2    min votes per pixel to keep a detection.
#   TARGET_STEP_DAYS=45 spacing (days) between generated dates when using the
#                       START_DATE/END_DATE span form (ignored if TARGET_DATES
#                       is given explicitly or USE_DATE_CLUSTERS=1).
#   USE_DATE_CLUSTERS=1 when 1 (default), cluster the tile's acquisition
#                       calendar over START_DATE..END_DATE: the cluster-gap
#                       midpoints become the target dates and each block's
#                       timesteps are collapsed to one min-composite per
#                       cluster before compositing (cloud-suppressing temporal
#                       summary). Requires START_DATE+END_DATE; TARGET_DATES
#                       must NOT be set (the dates are derived). Set 0 to use
#                       raw timesteps with the fixed-cadence / explicit
#                       TARGET_DATES paths instead.
#   MAX_THETA           clustering tuning (USE_DATE_CLUSTERS=1 only): max gap
#                       (days) for single-link merging. Unset = the determine
#                       script's default (10).
#   MAX_CLUSTER_AMPLITUDE  clustering tuning (USE_DATE_CLUSTERS=1 only): max
#                       span (days) a single cluster may cover. Unset = the
#                       determine script's default (15).
#   MIN_PATCH_M2=2500   block-level patch-area floor (m^2), firm.
#   MIN_TILE_PATCH_M2=5000  master patch-area floor (m^2), post cross-block merge.
#   MAX_COMPOSITE_DAYS  symmetric day-window around each break date for
#                       before/after compositing (unset = unbounded).
#   READ_START_DATE / READ_END_DATE  clip the raw HDF5 timestep read to this
#                       date range (YYYY-MM-DD) BEFORE loading the block into
#                       memory. Default: START_DATE / END_DATE, so the read
#                       matches the cluster window — HDF5s often hold timesteps
#                       far past END_DATE that would otherwise be read in and
#                       discarded, wasting memory. Set wider to keep more
#                       timesteps, or empty (e.g. READ_END_DATE=) for no bound
#                       on that side.
#   BLOCK_ROWS / BLOCK_COLS  process only a rectangular sub-grid of blocks
#                       (inclusive 0-based ranges) instead of the whole tile.
#                       e.g. BLOCK_ROWS=1-2 BLOCK_COLS=1-2 processes the middle
#                       2x2 of a 4x4 grid; a bare number selects one index.
#                       Unset = whole tile. The aggregator crops its outputs to
#                       the selected sub-region.
#   WRITE_COMPOSITE_TIFS=0  dump per-block before/after time-composites as
#                       10-band GeoTIFFs (debug/inspection only; off by
#                       default).
#
# All KEY=VALUE pairs become env vars that the array tasks and aggregator
# inherit (sbatch --export=ALL is the default — we use that).

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

# ── Required ──────────────────────────────────────────────────────────────
: "${TILE_ID:?TILE_ID is required}"
: "${TILE_HDF5_PATH:?TILE_HDF5_PATH is required}"
: "${OUTPUT_DIR:?OUTPUT_DIR is required}"

# Date clusters: when on, target dates AND the cluster membership are both
# derived from the tile's acquisition calendar over START_DATE..END_DATE
# (computed once below, exported to all tasks). It's an alternative to the
# fixed-cadence / explicit TARGET_DATES paths, so an explicit TARGET_DATES
# would be contradictory — require START/END and reject TARGET_DATES.
USE_DATE_CLUSTERS="${USE_DATE_CLUSTERS:-1}"
_clusters_on=0
case "$USE_DATE_CLUSTERS" in 1|true|True|yes) _clusters_on=1 ;; esac

# Clustering tuning (only used when USE_DATE_CLUSTERS=1). Left empty by default
# so determine_clusters_of_dates.py applies its own MAX_THETA /
# MAX_CLUSTER_AMPLITUDE; when set, they are passed through as CLI flags below.
#   MAX_THETA              max gap (days) for single-link merging.
#   MAX_CLUSTER_AMPLITUDE  max span (days) a single cluster may cover.
MAX_THETA="${MAX_THETA:-}"
MAX_CLUSTER_AMPLITUDE="${MAX_CLUSTER_AMPLITUDE:-}"

# Target dates: three mutually exclusive sources, all resolved (with Python)
# in the "Resolve TARGET_DATES" block below. Validate the inputs here.
TARGET_STEP_DAYS="${TARGET_STEP_DAYS:-45}"
if [[ "$_clusters_on" == 1 ]]; then
    if [[ -n "${TARGET_DATES:-}" ]]; then
        echo "USE_DATE_CLUSTERS=1 derives the target dates from the date " \
             "clusters, so TARGET_DATES must not also be set." >&2
        exit 1
    fi
    if [[ -z "${START_DATE:-}" || -z "${END_DATE:-}" ]]; then
        echo "USE_DATE_CLUSTERS=1 requires START_DATE and END_DATE " \
             "(YYYY-MM-DD) — the cluster window." >&2
        exit 1
    fi
elif [[ -z "${TARGET_DATES:-}" ]]; then
    if [[ -z "${START_DATE:-}" || -z "${END_DATE:-}" ]]; then
        echo "Provide TARGET_DATES (comma-separated YYYY-MM-DD), OR both " \
             "START_DATE and END_DATE (YYYY-MM-DD) to generate them every " \
             "TARGET_STEP_DAYS days." >&2
        exit 1
    fi
fi

# ── Optional (with defaults) ──────────────────────────────────────────────
# Model package (directory name under <shared>/). Validated — and the
# default WEIGHTS_PATH resolved from it — once SHARED_DIR/VENV are known
# below.
export MODEL="${MODEL:-enet_16bit}"
# Input data dtype for the read->composite->shift chain. u8 applies
# the q02/q98 stretch (uint8, nodata 255) — bacdm / enet_8bit. u16 (default)
# keeps raw uint16 reflectance (nodata 65535) — for models that scale natively
# (e.g. enet_16bit). Validate now so a typo fails at submit.
export DATA_DTYPE="${DATA_DTYPE:-u16}"
case "$DATA_DTYPE" in
    u8|uint8|8|u16|uint16|16) ;;
    *) echo "Invalid DATA_DTYPE='$DATA_DTYPE' (expected u8 or u16)" >&2; exit 1 ;;
esac
export BATCH_SIZE="${BATCH_SIZE:-8}"
export VOTE_CLASSES="${VOTE_CLASSES:-1,2}"
export VOTE_THRESHOLD="${VOTE_THRESHOLD:-2}"
# Two-tier patch-area floors (block then master)
export MIN_PATCH_M2="${MIN_PATCH_M2:-2500}"
export MIN_TILE_PATCH_M2="${MIN_TILE_PATCH_M2:-5000}"
# Read window: clip the raw HDF5 timestep read to this date range BEFORE loading
# the block into memory. Defaults to START_DATE/END_DATE so the read matches the
# cluster window (HDF5s often hold timesteps far beyond END_DATE, which would
# otherwise be read in and then discarded — wasting memory). Set wider to keep
# more timesteps, or empty (READ_START_DATE=) for no bound on that side.
# predict_block converts these to ordinals; empty/unset => unbounded.
export READ_START_DATE="${READ_START_DATE-${START_DATE:-}}"
export READ_END_DATE="${READ_END_DATE-${END_DATE:-}}"

# Symmetric day-window (days) around each break date for before/after
# compositing. Unset = unbounded (any timestep before/after the target). Only
# exported when the caller sets it, so predict_block sees None when unset.
if [[ -n "${MAX_COMPOSITE_DAYS:-}" ]]; then
    export MAX_COMPOSITE_DAYS
fi

# CPU threads per task. Exported so the array wrapper sizes its thread pools
# to this; also passed as --cpus-per-task below so the SLURM allocation has
# that many real cores (otherwise cgroups confine the task to 1 core and the
# threads just fight over it). A thread sweep on a detection-heavy block
# showed 1.9x speedup at 2 threads (95% efficiency), 2.74x at 4 (68%).
export THREADS="${THREADS:-2}"

# Max array tasks allowed to run at once (the `%N` in --array=0-LAST%N).
# Tasks share the node's memory bandwidth + HDF5/filesystem; once
# THREADS*MAX_CONCURRENT approaches the node core count, inference slows from
# bandwidth saturation (observed: 30 tasks x 1 thread -> 2.5x slower/chip).
# Keep the product comfortably under the node's cores. Lower also eases the
# Step-1 read storm. Set >= N_BLOCKS to effectively disable the cap.
MAX_CONCURRENT="${MAX_CONCURRENT:-8}"

# Output layout under OUTPUT_DIR:
#   logs/          SLURM .out/.err per block + the aggregator
#   block_outputs/ per-block .npz + .gpkg (predict_block.py writes here)
#   final_outputs/ tile-level .gpkg/.parquet/.npz/.tif (aggregate_tile.py)
export LOG_DIR="${LOG_DIR:-${OUTPUT_DIR}/logs}"
export BLOCK_OUTPUT_DIR="${BLOCK_OUTPUT_DIR:-${OUTPUT_DIR}/block_outputs}"
export FINAL_OUTPUT_DIR="${FINAL_OUTPUT_DIR:-${OUTPUT_DIR}/final_outputs}"
mkdir -p "$OUTPUT_DIR" "$LOG_DIR" "$BLOCK_OUTPUT_DIR" "$FINAL_OUTPUT_DIR"

# ── Tee this script's own output to a log file ────────────────────────────
# Everything printed from here on (grid discovery, date/cluster resolution,
# the run banner, the submitted job IDs) goes to both the terminal and
# $LOG_DIR/submit_tile.log so the submission summary is recoverable later.
# Placed right after the dirs exist; the few input-validation errors above
# this point still go to the terminal only (they fail before any log dir is
# known). Truncates per run so a re-submit's log reflects that submission.
SUBMIT_LOG="$LOG_DIR/submit_tile.log"
exec > >(tee "$SUBMIT_LOG") 2>&1
_tee_pid=$!   # wait on this at the end so tee flushes before the script exits

# ── Discover block grid shape via a quick venv invocation ─────────────────
# (Reads the HDF5 once on the login node; cheap — just opens attrs and
# the xs/ys arrays.)
# This script lives at the pipeline root (predict_pipeline/). Everything else
# is addressed relative to it:
#   processes/distribute/  SLURM wrappers + Python entry points (DISTRIBUTE_DIR)
#   processes/             python-path root for shared subpackages (SHARED_DIR:
#                          input_setup, composite_shift_chips, postprocess)
#   models/                model packages, imported by bare name (MODELS_DIR)
PIPELINE_ROOT="$(cd "$(dirname "$0")" && pwd)"
DISTRIBUTE_DIR="$PIPELINE_ROOT/processes/distribute"
SHARED_DIR="$PIPELINE_ROOT/processes"
MODELS_DIR="$PIPELINE_ROOT/models"
# Python venv with the pipeline's dependencies (see requirements.txt). Defaults
# to predict_pipeline/.venv/ — the venv living at the pipeline root. Exported so
# the SLURM wrappers (run_*_slurm.sh) activate the same one. Override by setting
# VENV=... on the command line.
VENV="${VENV:-$PIPELINE_ROOT/.venv}"
export VENV
# Clear any inherited PYTHONPATH. On this cluster the CVMFS Python env can put
# its own site-packages (/cvmfs/.../lib/cpu/python3.10) on PYTHONPATH, which
# sits AHEAD of the venv on sys.path and shadows venv packages — e.g. an old
# typing_extensions that breaks the venv's torch (`TypeIs` import). The
# per-command `PYTHONPATH=$MODELS_DIR/$SHARED_DIR python ...` calls below set it
# fresh for those invocations, so this unset doesn't affect them.
unset PYTHONPATH

# ── Resolve TARGET_DATES (+ DATE_CLUSTERS) ────────────────────────────────
# Three mutually exclusive sources, captured into vars first (not piped
# straight into export) so a generation error fails the whole script instead
# of silently exporting an empty list:
#   1. USE_DATE_CLUSTERS=1: cluster the tile's acquisition calendar over
#      START..END; the cluster-gap midpoints become TARGET_DATES and the
#      cluster membership becomes DATE_CLUSTERS (consumed by predict_block).
#   2. START_DATE/END_DATE only: one date every TARGET_STEP_DAYS.
#   3. explicit TARGET_DATES: used as-is.
export DATE_CLUSTERS="${DATE_CLUSTERS:-}"
if [[ "$_clusters_on" == 1 ]]; then
    # The CLI prints exactly two lines: line 1 = TARGET_DATES, line 2 =
    # serialized DATE_CLUSTERS. Read both; the determine script lives in
    # input_setup/ and runs as a plain script (no package import needed).
    # MAX_THETA / MAX_CLUSTER_AMPLITUDE are only forwarded when the caller set
    # them; otherwise the determine script uses its own defaults.
    _cluster_flags=()
    [[ -n "$MAX_THETA" ]] && _cluster_flags+=(--max-theta "$MAX_THETA")
    [[ -n "$MAX_CLUSTER_AMPLITUDE" ]] && \
        _cluster_flags+=(--max-cluster-amplitude "$MAX_CLUSTER_AMPLITUDE")
    _cluster_out="$(
        "$VENV/bin/python" "$SHARED_DIR/input_setup/determine_clusters_of_dates.py" \
            "$TILE_HDF5_PATH" --start "$START_DATE" --end "$END_DATE" \
            ${_cluster_flags[@]+"${_cluster_flags[@]}"} --for-submit
    )" || exit 1
    TARGET_DATES="$(printf '%s\n' "$_cluster_out" | sed -n '1p')"
    DATE_CLUSTERS="$(printf '%s\n' "$_cluster_out" | sed -n '2p')"
    if [[ -z "$TARGET_DATES" || -z "$DATE_CLUSTERS" ]]; then
        echo "Date clustering produced no change dates / clusters for " \
             "$START_DATE..$END_DATE (too few acquisitions in window?)." >&2
        exit 1
    fi
    echo "Derived TARGET_DATES + DATE_CLUSTERS from the acquisition calendar " \
         "over $START_DATE..$END_DATE."
elif [[ -z "${TARGET_DATES:-}" ]]; then
    TARGET_DATES="$(
        "$VENV/bin/python" "$DISTRIBUTE_DIR/target_dates_creation.py" \
            "$START_DATE" "$END_DATE" --step-days "$TARGET_STEP_DAYS"
    )" || exit 1
    if [[ -z "$TARGET_DATES" ]]; then
        echo "No target dates generated for $START_DATE..$END_DATE every " \
             "$TARGET_STEP_DAYS days (span shorter than one step?)." >&2
        exit 1
    fi
    echo "Generated TARGET_DATES from $START_DATE..$END_DATE " \
         "(every $TARGET_STEP_DAYS days):"
fi
export TARGET_DATES
export DATE_CLUSTERS

# ── Validate the model package + resolve default weights ──────────────────
# A model package is any <MODELS_DIR>/<name>/ with a predict.py. When
# WEIGHTS_PATH is unset, ask the package for its DEFAULT_WEIGHTS
if [[ ! -f "$MODELS_DIR/$MODEL/predict.py" ]]; then
    echo "Unknown MODEL '$MODEL' — no $MODELS_DIR/$MODEL/predict.py" >&2
    echo "Available model packages:" >&2
    for p in "$MODELS_DIR"/*/predict.py; do
        [[ -f "$p" ]] && echo "  $(basename "$(dirname "$p")")" >&2
    done
    exit 1
fi
if [[ -z "${WEIGHTS_PATH:-}" ]]; then
    WEIGHTS_PATH="$(
        PYTHONPATH="$MODELS_DIR" "$VENV/bin/python" -c "
import importlib
print(importlib.import_module('$MODEL').DEFAULT_WEIGHTS)
"
    )" || exit 1
fi
export WEIGHTS_PATH
if [[ ! -f "$WEIGHTS_PATH" ]]; then
    echo "Model weights not found: $WEIGHTS_PATH" >&2
    echo "(set WEIGHTS_PATH=... or place the checkpoint at the model's default path)" >&2
    exit 1
fi

read N_ROWS N_COLS <<<"$(
    PYTHONPATH="$SHARED_DIR" "$VENV/bin/python" -c "
from input_setup import get_block_grid_shape
r, c = get_block_grid_shape('$TILE_HDF5_PATH')
print(r, c)
"
)"
N_BLOCKS=$((N_ROWS * N_COLS))
LAST_IDX=$((N_BLOCKS - 1))

# ── Optional sub-region selection ─────────────────────────────────────────
# Process only a rectangular sub-grid of blocks instead of the whole tile.
# Useful when the HDF5 has a ghost margin of blocks you don't want in the
# output (e.g. a 4x4 tile where only the middle 2x2 is real). Specify inclusive
# 0-based ranges:
#     BLOCK_ROWS=1-2 BLOCK_COLS=1-2     # the middle 2x2 of a 4x4 grid
#     BLOCK_ROWS=2   BLOCK_COLS=0-3     # a single row
# Unset = whole tile. The selected blocks must form a rectangle (they always
# do with the lo-hi range form). The same selection is exported to the
# aggregator so it expects exactly these blocks and crops the output to them.
_parse_range() {  # "$1"=range string like "1-2" or "3"; "$2"=upper bound (N-1)
    local spec="$1" hi_bound="$2" lo hi
    if [[ "$spec" == *-* ]]; then
        lo="${spec%%-*}"; hi="${spec##*-}"
    else
        lo="$spec"; hi="$spec"
    fi
    if ! [[ "$lo" =~ ^[0-9]+$ && "$hi" =~ ^[0-9]+$ ]]; then
        echo "Bad range '$spec' (expected N or LO-HI)" >&2; exit 1
    fi
    if (( lo > hi || hi > hi_bound )); then
        echo "Range '$spec' out of bounds (grid max index $hi_bound)" >&2
        exit 1
    fi
    echo "$lo $hi"
}

if [[ -n "${BLOCK_ROWS:-}" || -n "${BLOCK_COLS:-}" ]]; then
    # Capture into a var first and check status: an `exit 1` inside $( ) only
    # kills the subshell, so `read <<<"$(...)"` would otherwise swallow the
    # failure and proceed with empty ranges. Fail the parent explicitly.
    _rows="$(_parse_range "${BLOCK_ROWS:-0-$((N_ROWS-1))}" $((N_ROWS-1)))" || exit 1
    _cols="$(_parse_range "${BLOCK_COLS:-0-$((N_COLS-1))}" $((N_COLS-1)))" || exit 1
    read ROW_LO ROW_HI <<<"$_rows"
    read COL_LO COL_HI <<<"$_cols"
    # Build the explicit list of linear array indices = row*N_COLS + col.
    ARRAY_IDS=""
    for ((r=ROW_LO; r<=ROW_HI; r++)); do
        for ((c=COL_LO; c<=COL_HI; c++)); do
            ARRAY_IDS+="$((r * N_COLS + c)),"
        done
    done
    ARRAY_SPEC="${ARRAY_IDS%,}"           # strip trailing comma
    # Space-separated index list for the block-error report (which array tasks
    # to expect). Same set as ARRAY_SPEC, just space- not comma-separated.
    EXPECTED_ARRAY_IDS="${ARRAY_SPEC//,/ }"
    N_SELECTED=$(( (ROW_HI-ROW_LO+1) * (COL_HI-COL_LO+1) ))
    # Tell the aggregator which sub-rectangle to expect + crop to.
    export PROCESS_ROW_LO="$ROW_LO" PROCESS_ROW_HI="$ROW_HI"
    export PROCESS_COL_LO="$COL_LO" PROCESS_COL_HI="$COL_HI"
    SELECT_DESC="rows ${ROW_LO}-${ROW_HI} cols ${COL_LO}-${COL_HI} (${N_SELECTED} blocks)"
else
    ARRAY_SPEC="0-${LAST_IDX}"
    EXPECTED_ARRAY_IDS="$(seq 0 "$LAST_IDX" | tr '\n' ' ')"
    N_SELECTED=$N_BLOCKS
    SELECT_DESC="all ${N_BLOCKS} blocks"
fi
export EXPECTED_ARRAY_IDS

echo "Tile:           $TILE_ID"
echo "Model:          $MODEL"
echo "Data dtype:     $DATA_DTYPE"
echo "HDF5:           $TILE_HDF5_PATH"
echo "Block grid:     ${N_ROWS} x ${N_COLS}  ($N_BLOCKS blocks)"
echo "Processing:     ${SELECT_DESC}"
echo "Output dir:     $OUTPUT_DIR"
echo "  logs:         $LOG_DIR"
echo "  block out:    $BLOCK_OUTPUT_DIR"
echo "  final out:    $FINAL_OUTPUT_DIR"
echo "Target dates:   $TARGET_DATES"
if [[ "$_clusters_on" == 1 ]]; then
    echo "Date clusters:  on ($(printf '%s' "$DATE_CLUSTERS" | awk -F';' '{print NF}') clusters)"
    echo "  max theta:    ${MAX_THETA:-default}"
    echo "  max amplitude: ${MAX_CLUSTER_AMPLITUDE:-default}"
else
    echo "Date clusters:  off (raw timesteps)"
fi
echo "Batch size:     $BATCH_SIZE"
echo "Vote classes:   $VOTE_CLASSES"
echo "Vote threshold: $VOTE_THRESHOLD"
echo "Closing radii:  per-class ($MODEL.CLOSING_RADII)"
echo "Block floor:    $MIN_PATCH_M2 m^2"
echo "Tile floor:     $MIN_TILE_PATCH_M2 m^2"
echo "Max comp. days: ${MAX_COMPOSITE_DAYS:-unbounded}"
echo "Threads/task:   $THREADS  (= --cpus-per-task)"
echo "Max concurrent: $MAX_CONCURRENT  (cores in use <= THREADS*MAX_CONCURRENT = $((THREADS * MAX_CONCURRENT)))"
echo "Weights:        $WEIGHTS_PATH"
echo

# Export N_COLS so the per-block wrapper can decode SLURM_ARRAY_TASK_ID.
export N_COLS

# Export DISTRIBUTE_DIR so the wrappers can locate the Python entry points.
# SLURM copies batch scripts into /var/spool/slurmd/jobXXXX/ before running,
# so resolving via ${BASH_SOURCE[0]} inside the wrapper points at the spool,
# not the real script dir.
export DISTRIBUTE_DIR

# ── Submit array job (one task per block) ─────────────────────────────────
ARRAY_JOB_ID=$(
    sbatch --parsable \
        --array=${ARRAY_SPEC}%${MAX_CONCURRENT} \
        --cpus-per-task="${THREADS}" \
        --export=ALL \
        --output="$LOG_DIR/predict_block_%a.out" \
        --error="$LOG_DIR/predict_block_%a.err" \
        --job-name="predict_${TILE_ID}" \
        "$DISTRIBUTE_DIR/run_block_slurm.sh"
)
echo "Submitted array job:      $ARRAY_JOB_ID  (array ${ARRAY_SPEC}%${MAX_CONCURRENT}, ${THREADS} cpu/task)"

# Pass the array job id through so downstream jobs (block-error report,
# aggregator) can query sacct for per-task state and the array's start time.
export ARRAY_JOB_ID

# ── Submit block-error report (runs after the array, success OR not) ───────
# afterany (not afterok) so this ALWAYS runs once the blocks finish, even when
# some fail — that's the case where the afterok aggregator silently never
# starts. Writes logs/block_errors.txt listing any block that didn't complete.
ERR_JOB_ID=$(
    sbatch --parsable \
        --dependency=afterany:"$ARRAY_JOB_ID" \
        --export=ALL \
        --output="$LOG_DIR/block_errors.out" \
        --error="$LOG_DIR/block_errors.err" \
        --job-name="blkerr_${TILE_ID}" \
        "$DISTRIBUTE_DIR/run_block_errors_slurm.sh"
)
echo "Submitted block-error job: $ERR_JOB_ID  (afterany:$ARRAY_JOB_ID)"

# ── Submit aggregator (depends on array success) ──────────────────────────
AGGR_JOB_ID=$(
    sbatch --parsable \
        --dependency=afterok:"$ARRAY_JOB_ID" \
        --export=ALL \
        --output="$LOG_DIR/aggregate.out" \
        --error="$LOG_DIR/aggregate.err" \
        --job-name="aggr_${TILE_ID}" \
        "$DISTRIBUTE_DIR/run_aggregate_slurm.sh"
)
echo "Submitted aggregator job: $AGGR_JOB_ID  (afterok:$ARRAY_JOB_ID)"
echo
echo "Watch with:  squeue -u \$USER"
echo "Per-block logs:    $LOG_DIR/predict_block_<task_id>.out"
echo "Block-error report: $LOG_DIR/block_errors.txt  (which blocks, if any, failed)"
echo "Aggregator log:    $LOG_DIR/aggregate.out"
echo "Per-block outputs: $BLOCK_OUTPUT_DIR/  (.npz + .gpkg)"
echo "Final outputs:     $FINAL_OUTPUT_DIR/  (.gpkg .parquet .npz .tif)"
echo "Submission log:    $SUBMIT_LOG"

# Close the redirected stdout/stderr and wait for the tee process to drain so
# the final lines aren't clipped when the script exits.
exec >&- 2>&-
wait "$_tee_pid" 2>/dev/null || true
