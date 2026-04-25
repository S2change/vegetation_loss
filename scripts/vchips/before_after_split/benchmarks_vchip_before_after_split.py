"""
Instrumented copy of vchip_before_after_split.py.

Wraps every main stage with a timing harness so the run produces a per-stage
report covering:
  - wall time, CPU time
  - first-call vs rest-of-calls split (catches lazy-load warmup costs)
  - peak resident memory delta and tracemalloc peak (Python allocations)
  - bytes read / written from disk (psutil io_counters)

Designed to expose hidden costs from lazy loads: a stage that "looks fast" in
wall time but has a large read_bytes delta is doing I/O on someone else's
behalf.

Original script docstring follows:

For each vchip, identifies which Sentinel-2 tile HDF5 file covers it based on
the x/y coordinates in the vchip filename, loads pre/post break S2 composites,
and writes two output GeoTIFFs (before and after) per vchip.

Vchip filename format: vchip_{x}_{y}_{date}_mask.tif
HDF5 filename format:  {tile_id}.h5  (e.g. T29SMC.h5)
All coordinates are in EPSG:32629.

Output bands (same for before and after):
    B12, 11, 8a, 8, 7, 6, 5, 4, 3, 2
"""
import os
import sys
import re
import glob
import time
import tracemalloc
from contextlib import contextmanager
from collections import defaultdict
from datetime import datetime
from statistics import median

import h5py
import numpy as np
import psutil
import rasterio as rio

# ============================================================================
# CONFIGURATION
# ============================================================================

# VCHIP_DIR, HDF5_DIR, BEFORE_OUTPUT, and AFTER_OUTPUT are supplied on the
# command line — see USAGE below.
USAGE = (
    "Usage: python benchmarks_vchip_before_after_split.py "
    "<vchip_dir> <hdf5_dir> <before_output_dir> <after_output_dir> "
    "[--runs N] [--cold]\n"
    "  --runs N   Repeat the full pipeline N times (default 2). Run 0 is\n"
    "             cold, runs 1+ are warm (OS page cache populated).\n"
    "  --cold     Pause between runs so you can drop the OS file cache\n"
    "             manually (`sync && sudo purge` on macOS)."
)

# Temporal compositing parameters
TEMPORAL_WINDOW_DAYS = 45
MAX_IMAGES_PER_PERIOD = 9

HDF5_NODATA = 65535
OUTPUT_NODATA = 65535

# Input HDF5 file has bands in ascending order, output order is reversed to match BACDM setup
# Reversal happens before cascading_selection, so use reverse order for picking
# band to check for pixel's having NoData in cascading_selection
# B12, 11, 8a, 8, 7, 6, 5, 4, 3, 2
SELECTION_BAND_INDEX = 3  # B8 (NIR)

# Output band descriptions, aligned with the reversed (descending) band order
BAND_NAMES = ('B12', 'B11', 'B8A', 'B8', 'B7', 'B6', 'B5', 'B4', 'B3', 'B2')

# ============================================================================
# TIMING HARNESS
# ============================================================================

# Process handle reused for every io_counters/RSS snapshot so we don't keep
# re-resolving it. psutil caches some fields per-instance.
_PROC = psutil.Process()


def _read_io():
    """Return (read_bytes, write_bytes) for the current process.

    On platforms or sandboxes where io_counters is unavailable we fall back
    to (0, 0) so the harness still produces a usable timing report — the
    disk-bytes columns will simply read as zero.
    """
    try:
        c = _PROC.io_counters()
        return c.read_bytes, c.write_bytes
    except (AttributeError, psutil.AccessDenied):
        return 0, 0


def _rss():
    """Current resident set size, in bytes."""
    return _PROC.memory_info().rss


class Bench:
    """Collects timing records keyed by stage name.

    Each call to time_block(stage) appends one record for that stage. Records
    track wall, CPU, RSS delta, tracemalloc peak, and bytes read/written so
    the report can distinguish compute-bound from I/O-bound stages and
    surface hidden lazy-load costs (a stage with low wall but high
    read_bytes is paying for someone else's lazy handle).
    """

    def __init__(self):
        # stage -> list of dicts (one entry per call to time_block)
        self.records = defaultdict(list)

    @contextmanager
    def time_block(self, stage, note=None):
        """Time a region of code and attribute its cost to `stage`."""
        # Snapshot pre-state. tracemalloc is started inside the block so
        # allocations from outside don't pollute the peak measurement.
        rss_before = _rss()
        read_before, write_before = _read_io()
        tracemalloc.start()
        cpu_before = time.process_time()
        wall_before = time.perf_counter()
        try:
            yield
        finally:
            wall = time.perf_counter() - wall_before
            cpu = time.process_time() - cpu_before
            _, py_peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            read_after, write_after = _read_io()
            rss_after = _rss()
            self.records[stage].append({
                'wall': wall,
                'cpu': cpu,
                'rss_delta': rss_after - rss_before,
                'py_peak': py_peak,
                'read_bytes': read_after - read_before,
                'write_bytes': write_after - write_before,
                'note': note,
            })

    def report(self, total_wall=None):
        """Print one row per stage, sorted by total wall time."""
        if not self.records:
            print("\n(no benchmark records collected)")
            return

        print("\n" + "=" * 120)
        print("BENCHMARK REPORT")
        print("=" * 120)

        rows = []
        for stage, records in self.records.items():
            walls = [r['wall'] for r in records]
            cpus = [r['cpu'] for r in records]
            reads = [r['read_bytes'] for r in records]
            writes = [r['write_bytes'] for r in records]
            py_peaks = [r['py_peak'] for r in records]
            rss_deltas = [r['rss_delta'] for r in records]
            n = len(records)
            total = sum(walls)
            rows.append({
                'stage': stage,
                'calls': n,
                'total_wall': total,
                'mean_wall': total / n,
                'first_wall': walls[0],
                # rest_mean is the mean wall time of every call after the first.
                # NaN when there is only one call — the report prints "-" for those.
                'rest_mean_wall': sum(walls[1:]) / (n - 1) if n > 1 else float('nan'),
                'max_wall': max(walls),
                'total_cpu': sum(cpus),
                'total_read_mb': sum(reads) / 1e6,
                'total_write_mb': sum(writes) / 1e6,
                'max_py_peak_mb': max(py_peaks) / 1e6,
                'max_rss_delta_mb': max(rss_deltas) / 1e6,
            })

        rows.sort(key=lambda r: r['total_wall'], reverse=True)

        header = (f"{'stage':42s} {'calls':>6s} {'wall_s':>9s} {'cpu_s':>8s} "
                  f"{'%wall':>6s} {'mean_ms':>9s} {'first_ms':>9s} "
                  f"{'rest_ms':>9s} {'read_MB':>9s} {'write_MB':>9s} "
                  f"{'pyPeak_MB':>10s} {'rssD_MB':>9s}")
        print(header)
        print("-" * len(header))
        for r in rows:
            pct = (r['total_wall'] / total_wall * 100.0) if total_wall else 0.0
            rest = r['rest_mean_wall']
            # rest_mean_wall is NaN when there's only one call; check via self-equality.
            rest_str = f"{rest * 1000:9.2f}" if rest == rest else f"{'-':>9s}"
            print(f"{r['stage'][:42]:42s} {r['calls']:6d} "
                  f"{r['total_wall']:9.3f} {r['total_cpu']:8.3f} "
                  f"{pct:6.2f} {r['mean_wall'] * 1000:9.2f} "
                  f"{r['first_wall'] * 1000:9.2f} {rest_str} "
                  f"{r['total_read_mb']:9.2f} {r['total_write_mb']:9.2f} "
                  f"{r['max_py_peak_mb']:10.2f} {r['max_rss_delta_mb']:9.2f}")

        if total_wall:
            print(f"\nTotal pipeline wall time: {total_wall:.3f}s")

    def report_loop(self, stage):
        """Per-iteration breakdown for a stage timed inside a tight loop.

        Useful for the HDF5 slice loop — flags warmup spikes (first call much
        slower than rest = chunk cache / lazy-load cost) and shows whether
        bytes-read per iteration matches the theoretical minimum.
        """
        records = self.records.get(stage, [])
        if not records:
            return
        walls_ms = [r['wall'] * 1000 for r in records]
        reads_kb = [r['read_bytes'] / 1024 for r in records]
        print(f"\nPer-call breakdown for '{stage}' ({len(records)} calls):")
        print(f"  first call : {walls_ms[0]:.2f} ms  read={reads_kb[0]:.1f} KB")
        if len(walls_ms) > 1:
            tail = walls_ms[1:]
            tail_reads = reads_kb[1:]
            print(f"  rest mean  : {sum(tail) / len(tail):.2f} ms  "
                  f"read={sum(tail_reads) / len(tail_reads):.1f} KB")
            print(f"  rest median: {median(tail):.2f} ms")
            print(f"  rest max   : {max(tail):.2f} ms")
            ratio = walls_ms[0] / (sum(tail) / len(tail)) if tail else 1.0
            if ratio > 2.0:
                print(f"  >>> first call is {ratio:.1f}x slower than rest — "
                      f"likely cache warmup / lazy-load cost")


# ============================================================================
# MAIN
# ============================================================================

def parse_args(argv):
    """Pull --runs N and --cold out of argv, return (positional, runs, cold)."""
    runs = 2
    cold = False
    positional = []
    i = 1
    while i < len(argv):
        a = argv[i]
        if a == '--runs':
            runs = int(argv[i + 1])
            i += 2
        elif a == '--cold':
            cold = True
            i += 1
        else:
            positional.append(a)
            i += 1
    return positional, runs, cold


def main():
    positional, runs, cold = parse_args(sys.argv)
    if len(positional) != 4:
        print(USAGE, file=sys.stderr)
        sys.exit(1)
    vchip_dir, hdf5_dir, before_output, after_output = positional

    # One Bench instance is shared across runs, so the final aggregated report
    # reflects all cold + warm calls. Inspect bench.records directly if you
    # need to separate them.
    bench = Bench()

    for run_idx in range(runs):
        label = "COLD" if run_idx == 0 else f"WARM #{run_idx}"
        print("\n" + "#" * 100)
        print(f"# RUN {run_idx + 1}/{runs} ({label})")
        print("#" * 100)

        if cold and run_idx > 0:
            # Manual cache drop because `purge` on macOS requires sudo and
            # blocking on it inside the harness would be brittle.
            print("\n[--cold] To drop OS file cache between runs, in another "
                  "terminal run: `sync && sudo purge`")
            input("Press Enter once cache is dropped to continue...")

        run_pipeline(vchip_dir, hdf5_dir, before_output, after_output, bench)

    # Total wall is summed from the outer 'pipeline.total' stage so the %wall
    # column means "fraction of all pipeline time across all runs".
    total_wall = sum(r['wall'] for r in bench.records.get('pipeline.total', []))
    bench.report(total_wall=total_wall)
    # The two slice-per-timestep stages get a separate per-iteration report
    # because that's where lazy-load amplification, if it exists, will show up.
    bench.report_loop('hdf5.slice_per_timestep.first_in_vchip')
    bench.report_loop('hdf5.slice_per_timestep')


def run_pipeline(vchip_dir, hdf5_dir, before_output, after_output, bench):
    """One full pass of the production pipeline, end-to-end, instrumented."""
    with bench.time_block('pipeline.total'):
        with bench.time_block('tile_index.build_total'):
            print("Building tile index...")
            tile_index = build_tile_index(hdf5_dir, bench)

        vchip_files = sorted(glob.glob(os.path.join(vchip_dir, "vchip_*_mask.tif")))
        print(f"\nFound {len(vchip_files)} vchip files\n")

        # Group vchips by tile so each HDF5 file is opened and read only once
        with bench.time_block('vchip.match_to_tiles'):
            by_tile = defaultdict(list)
            unmatched = []
            for vchip_path in vchip_files:
                parsed = parse_vchip_filename(vchip_path)
                if parsed is None:
                    print(f"Skipping (unexpected filename): {os.path.basename(vchip_path)}")
                    continue

                x, y, date_str = parsed
                tile_id = find_tile_for_point(x, y, tile_index)

                if tile_id is None:
                    print(f"No tile found for ({x}, {y}) — {os.path.basename(vchip_path)}")
                    unmatched.append(vchip_path)
                    continue

                by_tile[tile_id].append((vchip_path, date_str))

        # Process one tile at a time, loading its coordinate/time arrays just once
        for tile_id, vchips in by_tile.items():
            hdf5_path = tile_index[tile_id]['path']
            print(f"\nOpening tile {tile_id} ({len(vchips)} vchips)...")

            # Time the per-tile open + coordinate/timestamp materialization
            # separately from acquiring the lazy values handle. xs/ys/ts use
            # [:] so they fully load here; values_ds is captured below as a
            # lazy h5py.Dataset reference.
            with bench.time_block('tile.open_and_load_coords', note=tile_id):
                h5f = h5py.File(hdf5_path, 'r')
                xs: np.ndarray = h5f['xs'][:]  # type: ignore[index]
                ys: np.ndarray = h5f['ys'][:]  # type: ignore[index]
                ts: np.ndarray = h5f['ts'][:]  # type: ignore[index]

            # Acquire the lazy values handle separately. Should be sub-ms
            # with ~0 read_bytes; if either is large, h5py is doing more
            # work than expected when constructing the dataset reference.
            with bench.time_block('tile.values_handle_acquire', note=tile_id):
                values_ds = h5f['values']      # type: ignore[index]

            try:
                for vchip_idx, (vchip_path, date_str) in enumerate(vchips):
                    break_ordinal = date_str_to_ordinal(date_str)
                    print(f"  {os.path.basename(vchip_path)}  break date: {date_str}")
                    process_vchip(
                        vchip_path, xs, ys, ts, values_ds,
                        break_ordinal, before_output, after_output,
                        bench, vchip_idx,
                    )
            finally:
                # Original code used `with h5py.File(...)` so the file closed
                # automatically. Here we open without `with` so the timing
                # block boundary is explicit; close in finally to keep the
                # same lifetime guarantee.
                h5f.close()

        if unmatched:
            print(f"\nWarning: {len(unmatched)} vchips had no matching tile")


# ============================================================================
# TILE INDEX
# ============================================================================

def build_tile_index(hdf5_dir, bench):
    """
    Read the bounding box of every HDF5 tile file in hdf5_dir.

    Only xs and ys are read — the large values array is never touched.

    Returns
    -------
    dict mapping tile_id (str, e.g. 'T29SMC') to
        {'path': str, 'xmin': float, 'xmax': float, 'ymin': float, 'ymax': float}
    """
    index = {}
    h5_files = glob.glob(os.path.join(hdf5_dir, "*.h5"))
    if not h5_files:
        raise FileNotFoundError(f"No .h5 files found in {hdf5_dir}")

    for path in h5_files:
        tile_id = os.path.splitext(os.path.basename(path))[0]  # e.g. 'T29SMC'
        # Per-file timing here lets the report flag any one tile that's an
        # outlier (e.g. on slow storage or with anomalous coordinate sizes).
        with bench.time_block('tile_index.read_one_file', note=tile_id):
            with h5py.File(path, 'r') as h5f:
                xs: np.ndarray = h5f['xs'][:]  # type: ignore[index]
                ys: np.ndarray = h5f['ys'][:]  # type: ignore[index]

        index[tile_id] = {
            'path': path,
            'xmin': float(xs.min()),
            'xmax': float(xs.max()),
            'ymin': float(ys.min()),
            'ymax': float(ys.max()),
        }
        print(f"  {tile_id}: x=[{index[tile_id]['xmin']:.0f}, {index[tile_id]['xmax']:.0f}]  "
              f"y=[{index[tile_id]['ymin']:.0f}, {index[tile_id]['ymax']:.0f}]")

    print(f"Tile index built: {len(index)} tiles")
    return index


def find_tile_for_point(x, y, tile_index):
    """
    Return the tile_id whose bounding box contains (x, y).

    If the point falls in more than one tile (edge overlap), the first match
    is returned. Returns None if no tile covers the point.

    Parameters
    ----------
    x, y : float
        Coordinates in EPSG:32629
    tile_index : dict
        Output of build_tile_index()

    Returns
    -------
    str or None
    """
    for tile_id, bbox in tile_index.items():
        if bbox['xmin'] <= x <= bbox['xmax'] and bbox['ymin'] <= y <= bbox['ymax']:
            return tile_id
    return None


# ============================================================================
# VCHIP HELPERS
# ============================================================================

VCHIP_PATTERN = re.compile(r"vchip_(-?\d+)_(-?\d+)_(\d{8})_mask\.tif$")

def parse_vchip_filename(filename):
    """
    Extract (x, y, date_str) from a vchip filename.

    Returns None if the filename does not match the expected pattern.
    """
    m = VCHIP_PATTERN.search(os.path.basename(filename))
    if m is None:
        return None
    x, y, date_str = int(m.group(1)), int(m.group(2)), m.group(3)
    return x, y, date_str


def date_str_to_ordinal(date_str):
    """Convert a YYYYMMDD string to a Python ordinal date integer."""
    return datetime.strptime(date_str, "%Y%m%d").toordinal()


def ordinal_array_to_yyyymmdd(ordinal_array, nodata):
    """
    Convert a 2D array of Python ordinal dates to YYYYMMDD integers.

    Pixels equal to `nodata` are preserved as `nodata` in the output.
    """
    result = np.full_like(ordinal_array, nodata, dtype=np.int64)
    unique_ordinals = np.unique(ordinal_array)
    for ordinal in unique_ordinals:
        if ordinal == nodata:
            continue
        d = datetime.fromordinal(int(ordinal))
        yyyymmdd = d.year * 10000 + d.month * 100 + d.day
        result[ordinal_array == ordinal] = yyyymmdd
    return result


# ============================================================================
# HDF5 LOADING
# ============================================================================

def load_hdf5_for_vchip(xs, ys, values_ds, vchip_transform, vchip_width, vchip_height,
                        time_indices, bench, vchip_idx):
    """
    Load specific timesteps from an already-open HDF5 tile, placing pixels onto
    the vchip's grid.

    The output array matches the vchip's width/height/transform exactly, so any
    HDF5 pixel that falls outside the vchip extent is ignored, and any vchip
    cell with no matching HDF5 pixel is left as HDF5_NODATA.

    Parameters
    ----------
    xs, ys : ndarray
        Pre-loaded coordinate arrays for the tile.
    values_ds : h5py.Dataset
        Open reference to the tile's (time, band, pixel) values dataset.
    vchip_transform : affine.Affine
        Transform from the input vchip TIF (defines output pixel grid).
    vchip_width, vchip_height : int
        Output grid dimensions from the vchip.
    time_indices : ndarray
        Indices into the HDF5 ts/values arrays to load (pre- and post-break combined).
    bench : Bench
        Timing harness instance.
    vchip_idx : int
        Position of this vchip within its tile group, only used in note strings.

    Returns
    -------
    dict with keys:
        values       : (n_t, n_bands, vchip_height, vchip_width) uint16 array
        n_bands      : int
    or None if no HDF5 pixels fall within the vchip.
    """
    with bench.time_block('hdf5.compute_pixel_mask'):
        # Vchip geographic bounds
        xmin = vchip_transform.c
        ymax = vchip_transform.f
        pixel_size_x = vchip_transform.a
        pixel_size_y = -vchip_transform.e  # transform.e is negative for north-up
        xmax = xmin + vchip_width * pixel_size_x
        ymin = ymax - vchip_height * pixel_size_y

        _, n_bands, _ = values_ds.shape  # type: ignore[misc]

        # Keep HDF5 pixels whose centres fall inside the vchip
        pixel_mask = (xs >= xmin) & (xs < xmax) & (ys > ymin) & (ys <= ymax)
        pixel_indices = np.where(pixel_mask)[0]

        if len(pixel_indices) == 0:
            return None

        xs_chip = xs[pixel_mask]
        ys_chip = ys[pixel_mask]

        # Map each HDF5 pixel to its (row, col) in the vchip grid
        # Col 0 = smallest x (left), row 0 = largest y (top)
        cols = np.floor((xs_chip - xmin) / pixel_size_x).astype(int)
        rows = np.floor((ymax - ys_chip) / pixel_size_y).astype(int)
        cols = np.clip(cols, 0, vchip_width - 1)
        rows = np.clip(rows, 0, vchip_height - 1)

    # Load only the requested timesteps
    with bench.time_block('hdf5.alloc_result_buffer'):
        n_t = len(time_indices)
        result = np.full((n_t, n_bands, vchip_height, vchip_width), HDF5_NODATA, dtype=np.uint16)

    # The hot loop. Each iteration is timed individually so the report can
    # split first-call (cold chunk cache) from rest, and so per-call
    # read_bytes can be compared against the theoretical minimum
    # (n_bands * len(pixel_indices) * 2). If actual >> theoretical, HDF5 is
    # reading whole chunks because pixel_indices straddles chunk boundaries.
    with bench.time_block('hdf5.slice_loop_total'):
        for i, t_idx in enumerate(time_indices):
            stage = 'hdf5.slice_per_timestep.first_in_vchip' if i == 0 else 'hdf5.slice_per_timestep'
            with bench.time_block(stage):
                pixel_data: np.ndarray = values_ds[int(t_idx), :, pixel_indices]  # type: ignore[index]  # (n_bands, n_chip_pixels)
                # Mixing a slice with advanced indexing produces a (n_chip_pixels, n_bands)-shaped view, so pixel_data must be transposed to match.
                result[i, :, rows, cols] = pixel_data.T

    # HDF5 stores bands in ascending order; output expects descending order
    # This is a strided view, not a copy — cheap to create but consumers
    # downstream may pay for the reversed stride, so it's worth timing
    # what comes after this in cascading_selection.
    with bench.time_block('hdf5.reverse_band_order'):
        result = result[:, ::-1, :, :]

    # Annotate the slice_loop_total record with shape info so the report can
    # compare actual read_bytes against the theoretical minimum.
    theoretical = n_bands * len(pixel_indices) * 2 * len(time_indices)
    note = (f"vchip_idx={vchip_idx} n_t={len(time_indices)} "
            f"n_pixels={len(pixel_indices)} theoretical_min_bytes={theoretical}")
    bench.records['hdf5.slice_loop_total'][-1]['note'] = note

    return {
        'values': result,
        'n_bands': n_bands,
    }


# ============================================================================
# COMPOSITE AND SAVE
# ============================================================================

def process_vchip(vchip_path, xs, ys, ts, values_ds,
                  break_ordinal, before_output_dir, after_output_dir,
                  bench, vchip_idx):
    """
    Compute pre/post composites for a single vchip and write output TIFs.

    Parameters
    ----------
    vchip_path : str
        Path to the input vchip mask TIF (used for spatial bounds and metadata).
    xs, ys : ndarray
        Pre-loaded coordinate arrays for the tile this vchip belongs to.
    ts : ndarray
        Pre-loaded ordinal timestamps for the tile.
    values_ds : h5py.Dataset
        Open reference to the tile's (time, band, pixel) values dataset.
    break_ordinal : int
        Break date as a Python ordinal.
    before_output_dir : str
    after_output_dir : str
    bench : Bench
        Timing harness instance.
    vchip_idx : int
        Position of this vchip within its tile group; passed through to
        load_hdf5_for_vchip for note annotation.
    """
    with bench.time_block('vchip.process_total'):
        stem = os.path.splitext(os.path.basename(vchip_path))[0]
        before_path = os.path.join(before_output_dir, f"{stem}_before.tif")
        after_path = os.path.join(after_output_dir, f"{stem}_after.tif")

        # Read vchip grid (transform, width, height) — this is the canonical output grid.
        # rio.open is lazy, so this should only read metadata; if read_bytes
        # spikes here something is forcing a pixel read.
        with bench.time_block('vchip.read_metadata'):
            with rio.open(vchip_path) as src:
                vchip_meta = src.meta.copy()
                vchip_transform = src.transform
                vchip_width = src.width
                vchip_height = src.height

        # Select temporal indices before loading pixel data
        with bench.time_block('vchip.select_temporal_indices'):
            pre_indices, post_indices, pre_ordinals, post_ordinals = select_temporal_indices(
                ts, break_ordinal, TEMPORAL_WINDOW_DAYS, MAX_IMAGES_PER_PERIOD
            )
        if pre_indices is None or post_indices is None:
            print(f"    No images found in temporal window — skipping")
            return

        print(f"    {len(pre_indices)} pre-break and {len(post_indices)} post-break timesteps selected")

        # Load only the required timesteps, placed onto the vchip grid
        all_indices = np.concatenate([pre_indices, post_indices])
        with bench.time_block('vchip.load_hdf5_total'):
            chip = load_hdf5_for_vchip(
                xs, ys, values_ds, vchip_transform, vchip_width, vchip_height,
                all_indices, bench, vchip_idx,
            )
        if chip is None:
            print(f"  No HDF5 pixels found within vchip bounds — skipping")
            return

        values = chip['values']   # (n_pre+n_post, n_bands, vchip_height, vchip_width)
        n_bands = chip['n_bands']

        # Split values back into pre and post
        n_pre = len(pre_indices)
        pre_data = values[:n_pre]
        post_data = values[n_pre:]

        # Cascading composite: pick first valid observation per pixel
        with bench.time_block('vchip.cascading_selection'):
            pre_selected, post_selected, pre_ts, post_ts = cascading_selection_optimized(
                pre_data, post_data, pre_ordinals, post_ordinals,
                SELECTION_BAND_INDEX, HDF5_NODATA, OUTPUT_NODATA
            )
        # pre_selected / post_selected: (n_bands, vchip_height, vchip_width), dtype int64
        # pre_ts / post_ts: (vchip_height, vchip_width) ordinal dates

        # Convert ordinal timestamps to YYYYMMDD integers (NODATA pixels stay as OUTPUT_NODATA)
        with bench.time_block('vchip.ordinal_to_yyyymmdd'):
            pre_dates = ordinal_array_to_yyyymmdd(pre_ts, OUTPUT_NODATA)
            post_dates = ordinal_array_to_yyyymmdd(post_ts, OUTPUT_NODATA)

        # Stack spectral bands + date band
        with bench.time_block('vchip.stack_outputs'):
            pre_output = np.vstack([pre_selected, pre_dates[np.newaxis, :, :]])
            post_output = np.vstack([post_selected, post_dates[np.newaxis, :, :]])

        # Build output metadata — grid inherits directly from the vchip
        # uint32 needed because YYYYMMDD values (~20250101) exceed uint16 range
        out_meta = vchip_meta.copy()
        out_meta.update({
            'count': n_bands + 1,
            'dtype': 'uint32',
            'nodata': OUTPUT_NODATA,
        })

        os.makedirs(before_output_dir, exist_ok=True)
        os.makedirs(after_output_dir, exist_ok=True)

        output_descriptions = BAND_NAMES[:n_bands] + ('date_yyyymmdd',)

        with bench.time_block('vchip.write_before_tif'):
            with rio.open(before_path, 'w', **out_meta) as dst:
                dst.write(pre_output.astype(np.uint32))
                dst.descriptions = output_descriptions
        print(f"  Wrote before: {before_path}")

        with bench.time_block('vchip.write_after_tif'):
            with rio.open(after_path, 'w', **out_meta) as dst:
                dst.write(post_output.astype(np.uint32))
                dst.descriptions = output_descriptions
        print(f"  Wrote after:  {after_path}")


# ============================================================================
# chip_creation.py functions
# ============================================================================

# These functions are in /scripts/utils/bacdm_utils/chip_creation.py
# Copied to here so that utils script does not need to be copied to CACN machine

def select_temporal_indices(all_ordinals, break_ordinal, window_days, max_images):
    """
    Select temporal indices for pre and post break periods.

    Returns:
    --------
    tuple of (pre_indices, post_indices, pre_ordinals, post_ordinals)
    """
    # Pre-break selection
    pre_mask = (all_ordinals <= break_ordinal) & (all_ordinals >= break_ordinal - window_days)
    pre_indices = np.where(pre_mask)[0]
    pre_ordinals = all_ordinals[pre_indices]

    # Sort by date descending and take max_images
    sorted_idx = np.argsort(pre_ordinals)[::-1][:max_images]
    pre_indices = pre_indices[sorted_idx]
    pre_ordinals = pre_ordinals[sorted_idx]

    # Post-break selection
    post_mask = (all_ordinals > break_ordinal) & (all_ordinals <= break_ordinal + window_days)
    post_indices = np.where(post_mask)[0]
    post_ordinals = all_ordinals[post_indices]

    # Sort by date ascending and take max_images
    sorted_idx = np.argsort(post_ordinals)[:max_images]
    post_indices = post_indices[sorted_idx]
    post_ordinals = post_ordinals[sorted_idx]

    if len(pre_indices) == 0 or len(post_indices) == 0:
        return None, None, None, None

    return pre_indices, post_indices, pre_ordinals, post_ordinals


def cascading_selection_optimized(pre_data, post_data, pre_ordinals, post_ordinals,
                                  selection_band_idx, s2_nodata, output_nodata):
    """
    Optimized cascading selection working directly with numpy arrays.

    Parameters:
    -----------
    pre_data : ndarray
        Shape (n_pre_timesteps, n_bands, height, width)
    post_data : ndarray
        Shape (n_post_timesteps, n_bands, height, width)
    pre_ordinals : ndarray
        Ordinal dates for pre-break timesteps
    post_ordinals : ndarray
        Ordinal dates for post-break timesteps
    selection_band_idx : int
        Band index to use for selection

    Returns:
    --------
    tuple of (pre_selected, post_selected, pre_timestamps, post_timestamps)
        Each 'selected' is shape (n_bands, height, width)
        Each 'timestamps' is shape (height, width) with ordinal dates
    """
    n_bands, height, width = pre_data.shape[1], pre_data.shape[2], pre_data.shape[3]

    # Extract selection band
    pre_selection_band = pre_data[:, selection_band_idx, :, :]  # (n_pre, h, w)
    post_selection_band = post_data[:, selection_band_idx, :, :]  # (n_post, h, w)

    # Find first valid timestep for each pixel (cascading)
    pre_valid_mask = pre_selection_band < s2_nodata  # (n_pre, h, w)
    pre_first_valid_idx = pre_valid_mask.argmax(axis=0)  # (h, w)
    pre_any_valid = pre_valid_mask.any(axis=0)  # (h, w)

    post_valid_mask = post_selection_band < s2_nodata
    post_first_valid_idx = post_valid_mask.argmax(axis=0)
    post_any_valid = post_valid_mask.any(axis=0)

    # Create output arrays
    pre_selected = np.full((n_bands, height, width), output_nodata, dtype=np.int64)
    post_selected = np.full((n_bands, height, width), output_nodata, dtype=np.int64)
    pre_timestamps = np.full((height, width), output_nodata, dtype=np.int64)
    post_timestamps = np.full((height, width), output_nodata, dtype=np.int64)

    # Gather data using advanced indexing
    # Create meshgrid for row and column indices
    row_indices, col_indices = np.meshgrid(np.arange(height), np.arange(width), indexing='ij')

    for band_idx in range(n_bands):
        # For each pixel, select the value from its first valid timestep
        pre_selected[band_idx] = pre_data[pre_first_valid_idx, band_idx, row_indices, col_indices]
        post_selected[band_idx] = post_data[post_first_valid_idx, band_idx, row_indices, col_indices]

    # Get timestamps
    pre_timestamps[:] = pre_ordinals[pre_first_valid_idx]
    post_timestamps[:] = post_ordinals[post_first_valid_idx]

    # Apply validity mask
    pre_selected[:, ~pre_any_valid] = output_nodata
    post_selected[:, ~post_any_valid] = output_nodata
    pre_timestamps[~pre_any_valid] = output_nodata
    post_timestamps[~post_any_valid] = output_nodata

    return pre_selected, post_selected, pre_timestamps, post_timestamps


if __name__ == "__main__":
    main()
