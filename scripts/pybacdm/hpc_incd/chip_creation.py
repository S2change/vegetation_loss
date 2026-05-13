"""
Iterate 256x256 chip pairs across a Sentinel-2 HDF5 tile for sequential
date-pair change prediction.

For every pair of consecutive timesteps (t, t+1) within the requested date
range, walks the tile in horizontal stripes of 256x256 chips. Each chip pair
is handed to a placeholder `process_chip_pair` function which currently just
prints memory usage.

The HDF5 stores a non-rectangular subset of the tile — pixels outside the
subset (e.g. ocean, masked-out areas) are simply absent from the flat pixel
axis. A pixel-lookup map is built once at startup so each chip cell can be
mapped to either an HDF5 flat-axis index or NODATA. Chips that fall entirely
on absent pixels are skipped.

The full tile bbox is taken from a GeoPackage of S2 tile polygons, so the
chip grid matches the canonical S2 tile extent regardless of how sparse the
HDF5 subset is.

HDF5 layout assumed:
  - 'xs', 'ys'        : (n_pixels,) float coordinates per pixel, EPSG:32629
  - 'ts'              : (n_t,) ordinal dates
  - 'values'          : (n_t, n_bands, n_pixels) uint16 reflectance
  - The flat pixel axis is *probably* row-major over the present subset, so
    walking chips in horizontal stripes keeps each chip's flat indices
    relatively contiguous and minimizes chunk reads. With non-rectangular
    subsets this is approximate, not guaranteed.

GeoPackage layout assumed (matches vchip_before_after_split.py):
  - column 'Name' with tile IDs (e.g. 'T29SMC') matching HDF5 filenames
  - geometry in EPSG:32629

Usage:
    python chip_creation.py <hdf5_path> <tiles_gpkg> <start_yyyymmdd> <end_yyyymmdd> \\
                            <weights_path> <output_dir>

Example:
    python chip_creation.py /users1/dgt/hdf5/T29SMC.h5 \\
        /users1/cpca070342024/shared/auxiliary_data/sentinel2_tiles_PT_32629.gpkg \\
        20200301 20200501 \\
        /users1/cpca070342024/shared/weights/best_model.pth \\
        /users1/cpca070342024/shared/predictions/T29SMC
"""
import os
import sys
import time
from datetime import datetime

import h5py
import numpy as np
import psutil
import geopandas as gpd
import torch
import rasterio
from rasterio.transform import from_origin

from predict import load_model, predict_before_after_chips

# ============================================================================
# CONFIGURATION
# ============================================================================

CHIP_HEIGHT = 256
CHIP_WIDTH = 256

# Fraction of a chip that overlaps its neighbour. The stride between
# chip origins is `(1 - OVERLAP_PERCENT) * CHIP_SIZE`. Must be in [0, 1).
OVERLAP_PERCENT = 0.5

# Sentinel-2 pixel size in metres
PIXEL_SIZE = 10

HDF5_NODATA = 65535

# Prediction output settings — single-band uint8 GeoTIFFs, one per chip pair.
OUTPUT_CRS = 'EPSG:32629'
OUTPUT_NODATA = 255
OUTPUT_COMPRESS = 'lzw'
# Whether to re-run inference and overwrite an output GeoTIFF when it already
# exists. Set False to skip chip pairs whose output is already on disk.
SKIP_EXISTING_OUTPUTS = False

# Model batch size: how many chip pairs are stacked into one forward pass.
# A (B, 10, 256, 256) float32 input pair is ~5.2 MB per chip; activations
# dominate (~200-500 MB on CPU, smaller on GPU). Start at 8 and tune from
# memory diagnostics.
MODEL_BATCH_SIZE = 8


# BENCHMARKING

# Cap on chip pairs processed per run. Set to None to process every chip pair.
# Useful for diagnostics / dry-runs without committing to a full tile sweep.
MAX_CHIPS = 8

# Number of pixels along the chunk's flat pixel axis. Used to attribute pixel
# indices to chunks for the cache-effectiveness diagnostic.
CHUNK_PIXEL_AXIS_SIZE = 2_810_880

# HDF5 chunk cache size in bytes. The cache holds *decompressed* chunks. With
# 10 bands and 2,810,880 pixels per chunk, one decompressed chunk is ~56 MB,
# so 512 MB holds roughly 9 chunks. Default h5py value is 1 MB, which is too
# small for these chunks to ever stay cached.
HDF5_CACHE_BYTES = 512 * 1024 * 1024
# Hash slot count for the chunk cache. Should be ~10x the number of chunks
# the cache can hold; 10007 is a common prime that works for caches up to
# ~1000 chunks.
HDF5_CACHE_SLOTS = 10007


# ============================================================================
# CGROUP / SLURM ALLOCATION HELPERS
# ============================================================================

def read_cgroup_value(path):
    """Read an integer from a cgroup pseudo-file, or return None on failure."""
    try:
        with open(path) as f:
            raw = f.read().strip()
        if raw == 'max':
            return None  # unlimited
        return int(raw)
    except (FileNotFoundError, PermissionError, ValueError):
        return None


def get_memory_allocation():
    """
    Return (limit_bytes, current_bytes, source) for the cgroup the process
    runs in.

    Falls back to system-wide values from psutil if no cgroup is found
    (typical on a non-SLURM machine like a developer laptop).
    """
    # cgroup v2 (newer HPC clusters / recent Linux)
    v2_limit = read_cgroup_value('/sys/fs/cgroup/memory.max')
    v2_current = read_cgroup_value('/sys/fs/cgroup/memory.current')
    if v2_current is not None:
        return v2_limit, v2_current, 'cgroup v2'

    # cgroup v1 (older HPC clusters)
    v1_limit = read_cgroup_value('/sys/fs/cgroup/memory/memory.limit_in_bytes')
    v1_current = read_cgroup_value('/sys/fs/cgroup/memory/memory.usage_in_bytes')
    if v1_current is not None:
        # cgroup v1 reports a sentinel value when "unlimited"; treat large
        # values close to total system memory as effectively unlimited.
        if v1_limit is not None and v1_limit > 2**62:
            v1_limit = None
        return v1_limit, v1_current, 'cgroup v1'

    # No cgroup -> system-wide via psutil
    vm = psutil.virtual_memory()
    return vm.total, vm.used, 'system'


def rss_mb():
    """Process resident memory in MB. Useful for tracking startup growth."""
    return psutil.Process().memory_info().rss / 1e6


def get_cpu_allocation():
    """
    Return (allocated_cpus, source) for the SLURM job, or fall back to the
    machine's CPU count.
    """
    # Linux scheduler affinity reflects what cgroups + taskset actually grant
    try:
        affinity = os.sched_getaffinity(0)  # type: ignore[attr-defined]
        return len(affinity), 'sched_getaffinity'
    except (AttributeError, OSError):
        pass

    # SLURM env var as backup
    slurm_cpus = os.environ.get('SLURM_CPUS_PER_TASK')
    if slurm_cpus:
        return int(slurm_cpus), 'SLURM_CPUS_PER_TASK'

    # Last resort: whole machine
    return os.cpu_count() or 1, 'os.cpu_count'


# ============================================================================
# DATE / TIMESTEP HELPERS
# ============================================================================

def date_str_to_ordinal(date_str):
    """YYYYMMDD string to a Python ordinal date integer."""
    return datetime.strptime(date_str, "%Y%m%d").toordinal()


def select_timesteps_in_range(ts, start_ordinal, end_ordinal):
    """
    Return indices into `ts` whose ordinal dates fall in [start, end], sorted
    ascending.
    """
    mask = (ts >= start_ordinal) & (ts <= end_ordinal)
    indices = np.where(mask)[0]
    return indices[np.argsort(ts[indices])]


# ============================================================================
# TILE GEOMETRY AND PIXEL LOOKUP
# ============================================================================

def infer_tile_grid(tiles_gpkg, tile_id, pixel_size):
    """
    Look up the tile's bbox from a GeoPackage of S2 tile polygons and snap it
    to the pixel grid.

    Parameters
    ----------
    tiles_gpkg : str
        Path to the GeoPackage. Must have a 'Name' column with tile IDs.
    tile_id : str
        Tile identifier to look up (e.g. 'T29SMC').
    pixel_size : float
        Pixel size in metres, used to compute the grid dimensions.

    Returns
    -------
    tile_height, tile_width : int
        Number of rows and columns in the full grid.
    xmin, ymax              : float
        Top-left corner of the tile (geographic origin).
    """
    gdf = gpd.read_file(tiles_gpkg)
    if 'Name' not in gdf.columns:
        raise ValueError(f"Expected 'Name' column in {tiles_gpkg}; got {list(gdf.columns)}")

    matches = gdf[gdf['Name'] == tile_id]
    if len(matches) == 0:
        raise ValueError(f"Tile '{tile_id}' not found in {tiles_gpkg}")

    # Tile polygons in the gpkg can overlap; if multiple rows match the same
    # Name, take the first.
    minx, miny, maxx, maxy = matches.iloc[0].geometry.bounds

    tile_width = int(round((maxx - minx) / pixel_size))
    tile_height = int(round((maxy - miny) / pixel_size))
    return tile_height, tile_width, float(minx), float(maxy)


def build_pixel_lookup(xs, ys, tile_height, tile_width, xmin, ymax, pixel_size):
    """
    Build a (tile_height, tile_width) int32 array mapping each grid cell to
    its flat HDF5 pixel index, or -1 if no HDF5 pixel exists at that cell.

    Memory: 4 bytes per cell. A 10,000 x 10,000 grid = 400 MB. Acceptable for
    a process that's about to load the much larger 'values' dataset.
    """
    cols = np.round((xs - xmin) / pixel_size).astype(np.int64)
    rows = np.round((ymax - ys) / pixel_size).astype(np.int64)
    cols = np.clip(cols, 0, tile_width - 1)
    rows = np.clip(rows, 0, tile_height - 1)

    lookup = np.full((tile_height, tile_width), -1, dtype=np.int32)
    flat_indices = np.arange(len(xs), dtype=np.int32)
    lookup[rows, cols] = flat_indices
    return lookup


def iter_nonempty_chip_origins(lookup, chip_height, chip_width, overlap_percent=0.0):
    """
    Yield (row_origin, col_origin) for chips that walk the tile in horizontal
    stripes AND have at least one HDF5-present pixel.

    Outer loop: vertical position (rows).
    Inner loop: horizontal position (cols).

    Edge chips are shifted inward so every chip is a full chip_height x
    chip_width.

    The stride between consecutive chip origins is
    `(1 - overlap_percent) * chip_size`, rounded to an int. So overlap=0.0
    tiles cleanly; overlap=0.5 spaces origins half a chip apart, meaning
    each chip overlaps its neighbour by 50%. Overlap is clamped to give a
    minimum stride of 1 pixel.
    """
    if not 0.0 <= overlap_percent < 1.0:
        raise ValueError(f"overlap_percent must be in [0, 1); got {overlap_percent}")

    tile_height, tile_width = lookup.shape

    row_stride = max(1, int(round(chip_height * (1 - overlap_percent))))
    col_stride = max(1, int(round(chip_width * (1 - overlap_percent))))

    # Last valid origin so the chip still fits inside the tile
    last_row = tile_height - chip_height
    last_col = tile_width - chip_width

    row_origins = list(range(0, last_row + 1, row_stride))
    if not row_origins or row_origins[-1] != last_row:
        row_origins.append(last_row)

    col_origins = list(range(0, last_col + 1, col_stride))
    if not col_origins or col_origins[-1] != last_col:
        col_origins.append(last_col)

    for r in row_origins:
        for c in col_origins:
            chip_lookup = lookup[r:r + chip_height, c:c + chip_width]
            # Skip chips whose lookup is entirely -1 (no HDF5 pixels present)
            if (chip_lookup != -1).any():
                yield r, c


# ============================================================================
# CHIP LOADING
# ============================================================================

def load_chip(values_ds, time_idx, row_origin, col_origin, lookup, n_bands,
              chunks_seen=None,
              chip_height=CHIP_HEIGHT, chip_width=CHIP_WIDTH):
    """
    Load a chip_height x chip_width chip for one timestep, using the pixel
    lookup to find which HDF5 indices to fetch.

    Cells outside the HDF5 subset stay HDF5_NODATA in the output.

    If `chunks_seen` is a set, each unique (time_idx, chunk_idx) pair touched
    by this load is added to it — used by the chunk-cache diagnostic to count
    how many distinct chunks have been requested across the whole run.

    Returns
    -------
    ndarray of shape (n_bands, chip_height, chip_width), dtype uint16.
    """
    chip_lookup = lookup[row_origin:row_origin + chip_height,
                         col_origin:col_origin + chip_width]

    # Coordinates of the cells that have an HDF5 source
    present_mask = chip_lookup != -1
    if not present_mask.any():
        # Caller should have filtered these out, but handle gracefully
        return np.full((n_bands, chip_height, chip_width), HDF5_NODATA, dtype=np.uint16)

    pixel_indices = chip_lookup[present_mask].astype(np.int64)
    cell_rows, cell_cols = np.where(present_mask)

    if chunks_seen is not None:
        for chunk_idx in np.unique(pixel_indices // CHUNK_PIXEL_AXIS_SIZE):
            chunks_seen.add((int(time_idx), int(chunk_idx)))

    # Single HDF5 read for all present pixels in the chip:
    # values_ds[t, :, pixel_indices] -> (n_bands, n_present)
    flat: np.ndarray = values_ds[int(time_idx), :, pixel_indices]  # type: ignore[index]

    chip = np.full((n_bands, chip_height, chip_width), HDF5_NODATA, dtype=np.uint16)
    # `chip[:, rows, cols]` has the advanced-indexing axes adjacent, so numpy
    # keeps the slice axis in place: LHS view shape is (n_bands, n_present),
    # which already matches `flat` — no transpose needed.
    chip[:, cell_rows, cell_cols] = flat
    return chip


# ============================================================================
# OUTPUT WRITING
# ============================================================================

def chip_output_path(output_dir, tile_id, ordinal_t, ordinal_t_next,
                     row_origin, col_origin):
    """
    Deterministic filename for one chip pair's prediction GeoTIFF.

    Format: {tile_id}_{YYYYMMDD_t}_{YYYYMMDD_t_next}_r{row:05d}_c{col:05d}.tif
    e.g.    T29SMC_20200315_20200402_r00256_c00512.tif
    """
    date_t = datetime.fromordinal(int(ordinal_t)).strftime("%Y%m%d")
    date_t_next = datetime.fromordinal(int(ordinal_t_next)).strftime("%Y%m%d")
    fname = (f"{tile_id}_{date_t}_{date_t_next}"
             f"_r{row_origin:05d}_c{col_origin:05d}.tif")
    return os.path.join(output_dir, fname)


def save_prediction_tif(pred, out_path, row_origin, col_origin,
                        xmin_tile, ymax_tile):
    """
    Write a (H, W) uint8 prediction to a single-band GeoTIFF, georeferenced
    to its position inside the tile.

    The tile's top-left is (xmin_tile, ymax_tile); the chip's top-left in
    pixels is (row_origin, col_origin) inside that tile.
    """
    chip_xmin = xmin_tile + col_origin * PIXEL_SIZE
    chip_ymax = ymax_tile - row_origin * PIXEL_SIZE
    transform = from_origin(chip_xmin, chip_ymax, PIXEL_SIZE, PIXEL_SIZE)

    with rasterio.open(
        out_path, 'w',
        driver='GTiff',
        height=pred.shape[0], width=pred.shape[1],
        count=1, dtype='uint8',
        crs=OUTPUT_CRS,
        transform=transform,
        compress=OUTPUT_COMPRESS,
        nodata=OUTPUT_NODATA,
    ) as dst:
        dst.write(pred, 1)


# ============================================================================
# MODEL INFERENCE + DIAGNOSTICS
# ============================================================================

def print_chip_diagnostic(label, ordinal_t, ordinal_t_next,
                          row_origin, col_origin, chips_bytes,
                          chunks_seen=None, baseline_read_bytes=0,
                          extra=""):
    """
    One-line memory + cache diagnostic for a chip pair. Same fields the
    placeholder used, plus an `extra` string for inference-specific info.

      - alloc_avail/alloc_used  : cgroup (SLURM) limit and current use
      - cum_chunks_requested    : unique (time_idx, chunk_idx) pairs touched
      - cum_disk_read_MB        : bytes read since startup (psutil)
      - avg_disk_per_chunk      : disk bytes / unique chunk request.
                                  Near the compressed-chunk size => cache
                                  ineffective; well below => cache helping.
    """
    proc = psutil.Process()
    proc_rss = proc.memory_info().rss

    cg_limit, cg_used, _ = get_memory_allocation()
    if cg_limit is not None:
        cg_avail = max(cg_limit - cg_used, 0)
        cg_pct_used = 100.0 * cg_used / cg_limit if cg_limit else 0.0
        mem_str = (f"alloc_avail={cg_avail / 1e9:5.2f} GB  "
                   f"alloc_used={cg_pct_used:.1f}%")
    else:
        mem_str = f"alloc_used={cg_used / 1e9:5.2f} GB (no limit)"

    date_t = datetime.fromordinal(int(ordinal_t)).strftime("%Y-%m-%d")
    date_t_next = datetime.fromordinal(int(ordinal_t_next)).strftime("%Y-%m-%d")

    cache_str = ""
    if chunks_seen is not None:
        try:
            current_read = proc.io_counters().read_bytes  # type: ignore[attr-defined]
        except (AttributeError, psutil.AccessDenied):
            current_read = baseline_read_bytes
        cum_disk = current_read - baseline_read_bytes
        cum_chunks = len(chunks_seen)
        avg_per_chunk = (cum_disk / cum_chunks) if cum_chunks else 0.0
        cache_str = (f"  cum_chunks={cum_chunks:5d}  "
                     f"cum_disk_read={cum_disk / 1e6:7.1f} MB  "
                     f"avg_disk/chunk={avg_per_chunk / 1e6:5.2f} MB")

    print(
        f"  {label} @ ({row_origin:5d},{col_origin:5d})  "
        f"{date_t} -> {date_t_next}  "
        f"chips={chips_bytes / 1e6:6.2f} MB  "
        f"proc_rss={proc_rss / 1e6:7.1f} MB  "
        f"{mem_str}"
        f"{cache_str}"
        f"{extra}"
    )


def process_chip_batch(batch, model, output_dir, tile_id,
                       xmin_tile, ymax_tile,
                       chunks_seen=None, baseline_read_bytes=0):
    """
    Run BACDM inference on a batch of chip pairs, save predictions as
    GeoTIFFs, and print per-chip diagnostics including model wall time and
    pre/post-inference RSS delta.

    `batch` is a list of dicts produced by main():
      {'chip_t', 'chip_t_next'   : (n_bands, H, W) uint16
       'ord_t',  'ord_t_next'    : int ordinal dates
       'row_origin', 'col_origin': int chip origin in tile pixels}
    """
    if not batch:
        return

    # Stack into (B, H, W, C) — predict_before_after_chips expects that layout.
    # load_chip gives us (C, H, W), so transpose each chip on the way in.
    before = np.stack([p['chip_t'].transpose(1, 2, 0) for p in batch])
    after  = np.stack([p['chip_t_next'].transpose(1, 2, 0) for p in batch])

    rss_before = rss_mb()
    t0 = time.perf_counter()
    preds = predict_before_after_chips(before, after, model)  # (B, H, W) uint8
    infer_s = time.perf_counter() - t0
    rss_after = rss_mb()

    per_chip_s = infer_s / len(batch)
    rss_delta = rss_after - rss_before
    gpu_str = ""
    if torch.cuda.is_available():
        gpu_str = (f"  gpu_alloc={torch.cuda.memory_allocated() / 1e9:5.2f} GB"
                   f"  gpu_resv={torch.cuda.memory_reserved() / 1e9:5.2f} GB")

    # Save each prediction and print one diagnostic line per chip pair.
    for i, p in enumerate(batch):
        out_path = chip_output_path(
            output_dir, tile_id, p['ord_t'], p['ord_t_next'],
            p['row_origin'], p['col_origin'],
        )
        save_prediction_tif(preds[i], out_path,
                            p['row_origin'], p['col_origin'],
                            xmin_tile, ymax_tile)

        chips_bytes = p['chip_t'].nbytes + p['chip_t_next'].nbytes
        extra = (f"  infer={per_chip_s * 1000:6.1f} ms/chip"
                 f"  batch_rss_delta={rss_delta:+6.1f} MB"
                 f"{gpu_str}")
        print_chip_diagnostic(
            "chip pair", p['ord_t'], p['ord_t_next'],
            p['row_origin'], p['col_origin'], chips_bytes,
            chunks_seen=chunks_seen,
            baseline_read_bytes=baseline_read_bytes,
            extra=extra,
        )


# ============================================================================
# MAIN
# ============================================================================

def main():
    if len(sys.argv) != 7:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    hdf5_path = sys.argv[1]
    tiles_gpkg = sys.argv[2]
    start_str = sys.argv[3]
    end_str = sys.argv[4]
    weights_path = sys.argv[5]
    output_dir = sys.argv[6]
    start_ord = date_str_to_ordinal(start_str)
    end_ord = date_str_to_ordinal(end_str)

    os.makedirs(output_dir, exist_ok=True)

    # Tile ID is the HDF5 filename stem, e.g. 'T29SMC' from 'T29SMC.h5'
    tile_id = os.path.splitext(os.path.basename(hdf5_path))[0]

    print(f"\nHDF5 file:    {hdf5_path}")
    print(f"Tile ID:      {tile_id}")
    print(f"GeoPackage:   {tiles_gpkg}")
    print(f"Date range:   {start_str} -> {end_str}")
    print(f"Chip size:    {CHIP_WIDTH} x {CHIP_HEIGHT}, overlap: {OVERLAP_PERCENT * 100:.0f}%")
    print(f"Weights:      {weights_path}")
    print(f"Output dir:   {output_dir}")
    print(f"Batch size:   {MODEL_BATCH_SIZE}")
    print(f"Skip existing outputs: {SKIP_EXISTING_OUTPUTS}")

    # Report what the SLURM cgroup actually grants this job
    cpu_count, cpu_source = get_cpu_allocation()
    mem_limit, mem_used, mem_source = get_memory_allocation()
    print(f"CPU allocation: {cpu_count} cores  (source: {cpu_source})")
    if mem_limit is not None:
        print(f"Memory allocation: {mem_limit / 1e9:.2f} GB total, "
              f"{mem_used / 1e9:.2f} GB used at start  (source: {mem_source})")
    else:
        print(f"Memory allocation: unlimited, {mem_used / 1e9:.2f} GB used at start  "
              f"(source: {mem_source})")

    print(f"\n[RSS] After imports + cgroup setup:    {rss_mb():7.1f} MB")

    # Load the model once, before the HDF5 file is opened. Doing it before the
    # chunk cache is allocated keeps peak memory predictable: model weights +
    # torch runtime are paid up-front, then HDF5 cache grows on demand.
    model = load_model(weights_path)
    print(f"[RSS] After model loaded:              {rss_mb():7.1f} MB")
    if torch.cuda.is_available():
        print(f"[GPU] After model loaded: "
              f"alloc={torch.cuda.memory_allocated() / 1e9:.2f} GB  "
              f"reserved={torch.cuda.memory_reserved() / 1e9:.2f} GB")

    with h5py.File(hdf5_path, 'r',
                   rdcc_nbytes=HDF5_CACHE_BYTES,
                   rdcc_nslots=HDF5_CACHE_SLOTS) as h5f:
        print(f"[RSS] After opening HDF5 file:         {rss_mb():7.1f} MB")

        xs: np.ndarray = h5f['xs'][:]      # type: ignore[index]
        ys: np.ndarray = h5f['ys'][:]      # type: ignore[index]
        ts: np.ndarray = h5f['ts'][:]      # type: ignore[index]
        print(f"[RSS] After loading xs/ys/ts:          {rss_mb():7.1f} MB")

        values_ds = h5f['values']          # type: ignore[index]
        _, n_bands, n_pixels = values_ds.shape  # type: ignore[misc]

        tile_height, tile_width, xmin, ymax = infer_tile_grid(tiles_gpkg, tile_id, PIXEL_SIZE)
        print(f"[RSS] After gpkg lookup:               {rss_mb():7.1f} MB")
        print(f"Tile grid: {tile_width} x {tile_height}, pixel size {PIXEL_SIZE}m")
        print(f"Origin (top-left): x={xmin:.0f}, y={ymax:.0f}")
        print(f"HDF5 present pixels: {n_pixels:,} of {tile_width * tile_height:,} "
              f"({100 * n_pixels / (tile_width * tile_height):.1f}% coverage)")

        print("Building pixel lookup...")
        lookup = build_pixel_lookup(xs, ys, tile_height, tile_width, xmin, ymax, PIXEL_SIZE)
        print(f"  Lookup size: {lookup.nbytes / 1e6:.1f} MB")
        print(f"[RSS] After pixel lookup built:        {rss_mb():7.1f} MB")

        # xs / ys are no longer needed — every spatial lookup goes through
        # the pixel lookup map from this point on. Free their memory before
        # the chip loop allocates the chunk cache.
        del xs, ys
        print(f"[RSS] After freeing xs/ys:             {rss_mb():7.1f} MB")

        time_indices = select_timesteps_in_range(ts, start_ord, end_ord)
        print(f"Timesteps in range: {len(time_indices)}")
        if len(time_indices) < 2:
            print("Need at least 2 timesteps to form a pair — exiting.")
            return

        chip_origins = list(iter_nonempty_chip_origins(
            lookup, CHIP_HEIGHT, CHIP_WIDTH, overlap_percent=OVERLAP_PERCENT
        ))
        print(f"[RSS] After chip-origin enumeration:   {rss_mb():7.1f} MB")
        print(f"Non-empty chips per timestep: {len(chip_origins)}")
        total_pairs = (len(time_indices) - 1) * len(chip_origins)
        if MAX_CHIPS is None:
            print(f"Total chip pairs to process: {total_pairs}\n")
        else:
            print(f"Total chip pairs available: {total_pairs}, "
                  f"capped to {MAX_CHIPS} by MAX_CHIPS\n")

        # Chunk-cache diagnostic state. chunks_seen accumulates unique
        # (time_idx, chunk_idx) pairs the access pattern has touched, and
        # baseline_read_bytes pins the io counter at "now" so reported
        # cum_disk_read counts only the chip-loading work, not startup I/O.
        chunks_seen = set()
        try:
            baseline_read_bytes = psutil.Process().io_counters().read_bytes  # type: ignore[attr-defined]
        except (AttributeError, psutil.AccessDenied):
            baseline_read_bytes = 0

        print(f"[RSS] Baseline before chip loop:       {rss_mb():7.1f} MB\n")

        chips_processed = 0
        chips_skipped_existing = 0
        batch = []

        def flush(batch):
            """Run inference on the buffered chip pairs and clear the buffer."""
            if not batch:
                return
            process_chip_batch(
                batch, model, output_dir, tile_id,
                xmin, ymax,
                chunks_seen=chunks_seen,
                baseline_read_bytes=baseline_read_bytes,
            )
            batch.clear()

        for pair_idx in range(len(time_indices) - 1):
            if MAX_CHIPS is not None and chips_processed >= MAX_CHIPS:
                break

            t_idx = int(time_indices[pair_idx])
            t_next_idx = int(time_indices[pair_idx + 1])
            ord_t = int(ts[t_idx])
            ord_t_next = int(ts[t_next_idx])
            date_t = datetime.fromordinal(ord_t).strftime("%Y-%m-%d")
            date_t_next = datetime.fromordinal(ord_t_next).strftime("%Y-%m-%d")
            print(f"=== Pair {pair_idx + 1}/{len(time_indices) - 1}: "
                  f"{date_t} -> {date_t_next} ===")

            for row_origin, col_origin in chip_origins:
                if MAX_CHIPS is not None and chips_processed >= MAX_CHIPS:
                    break

                out_path = chip_output_path(
                    output_dir, tile_id, ord_t, ord_t_next,
                    row_origin, col_origin,
                )
                if SKIP_EXISTING_OUTPUTS and os.path.exists(out_path):
                    chips_skipped_existing += 1
                    chips_processed += 1
                    continue

                chip_t = load_chip(values_ds, t_idx, row_origin, col_origin, lookup, n_bands,
                                   chunks_seen=chunks_seen)
                chip_t_next = load_chip(values_ds, t_next_idx, row_origin, col_origin, lookup, n_bands,
                                        chunks_seen=chunks_seen)

                batch.append({
                    'chip_t': chip_t,
                    'chip_t_next': chip_t_next,
                    'ord_t': ord_t,
                    'ord_t_next': ord_t_next,
                    'row_origin': row_origin,
                    'col_origin': col_origin,
                })
                chips_processed += 1

                if len(batch) >= MODEL_BATCH_SIZE:
                    flush(batch)

        # Final partial batch (if any chip pairs are still buffered)
        flush(batch)

        print(f"\nProcessed {chips_processed} chip pairs.")
        if SKIP_EXISTING_OUTPUTS:
            print(f"Skipped (output already existed): {chips_skipped_existing}")
        print(f"[RSS] After chip loop:                 {rss_mb():7.1f} MB")


if __name__ == "__main__":
    main()
