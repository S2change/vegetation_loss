"""
process_bacdm.py

Runs the BACDM change detection model over a full HDF5 tile using a sliding
temporal window. For each 256x256 spatial chip, the model is applied to every
consecutive before/after pair of observations. The before image is always a
single acquisition; the after image uses cascading selection (first cloud-free
pixel within a forward lookahead window). Results are accumulated into
per-pixel summary statistics and written to output GeoTIFFs.

HDF5 Structure Expected:
    values: (time, bands, pixels)
    xs, ys: (pixels,) — coordinate arrays (projected, e.g. UTM metres)
    ts:     (time,)   — ordinal dates
    original_timestamps: (time,) — unix timestamps in milliseconds
    attrs:  band_names, crs, bounds_left/right/bottom/top

Input bands expected in HDF5 (in order): B2, B3, B4, B8, B11, B12
    (6 bands matching AAA_Configs.channel_nums and normalization parameters)

Output GeoTIFFs (one per tile, written to OUTPUT_DIR):
    Band 1: total_flags        — number of windows where pixel was flagged
    Band 2: total_valid_pairs  — number of windows where pixel had valid before AND after
    Band 3: max_consec_flags   — longest consecutive run of flags (valid steps only)
    Band 4: flagged_proportion — total_flags / total_valid_pairs (0 where no valid pairs)

Parallelisation (HPC):
    Each task processes one spatial block (BLOCK_SIZE x BLOCK_SIZE pixels).
    Pass the block index as a command-line argument:
        python process_bacdm.py <block_index>
    If no argument is given, all blocks are processed sequentially (useful for testing).
"""

import os
import sys
import time
import h5py
import numpy as np
import torch
from torchvision import transforms
from rasterio.crs import CRS
from rasterio.transform import from_origin
import rasterio

# ---------------------------------------------------------------------------
# Add parent directory to path so AAA_Configs and bacdm imports resolve
# ---------------------------------------------------------------------------
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if module_path not in sys.path:
    sys.path.insert(0, module_path)

import AAA_Configs
from bacdm.swin_ynet import Encoder

# ============================================================================
# CONFIGURATION
# ============================================================================

HDF5_FILE   = r"E:\T29TQG\T29TQG_6bands_lzf_compression.h5"
OUTPUT_DIR  = r"E:\T29TQG\bacdm_output"
WEIGHT_PATH = r".\bacdm\logs\B12118A432.pth"

# Chip dimensions (must match model input size)
CHIP_SIZE   = 256

# Spatial block size in pixels. Each HPC task loads one block into memory,
# then slices chips from it. BLOCK_SIZE should be a multiple of CHIP_SIZE.
BLOCK_SIZE  = 1024  # covers 4x4 = 16 chips per block with no overlap

# Overlap between adjacent chips in pixels (0 = no overlap, 128 = 50%)
OVERLAP     = 128

# How many timesteps to look ahead when building the "after" composite.
# Cascading selection picks the first cloud-free pixel within this window.
LOOKAHEAD   = 5

# S2 nodata value as stored in HDF5
S2_NODATA   = 65535

# Output nodata value
OUTPUT_NODATA = -1

# Optional date range filter (datetime objects or None)
MIN_DATE = None
MAX_DATE = None

# Change detection threshold: model output (0-255) above this is flagged
CHANGE_THRESHOLD = 127

# ============================================================================
# MODEL LOADING
# ============================================================================

def load_model(weight_path, use_cuda):
    """Load BACDM Encoder model from weights file."""
    model = Encoder()
    map_loc = 'cuda' if use_cuda else 'cpu'
    model.load_state_dict(torch.load(weight_path, map_location=map_loc))
    if use_cuda:
        model = model.cuda()
    model.eval()
    return model


def get_normalise_transform():
    """Return the same normalisation transform used during training."""
    return transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize(AAA_Configs.normalization_mean,
                             AAA_Configs.normalization_std)
    ])

# ============================================================================
# HDF5 BLOCK LOADER
# ============================================================================

class HDF5BlockLoader:
    """
    Loads spatial blocks from an HDF5 tile into memory.

    The HDF5 stores pixels as a flat sparse array with associated xs/ys
    coordinates. This class maps those coordinates onto a regular 2-D grid
    and provides block-level reads so that chips can be extracted cheaply
    via array slicing.
    """

    def __init__(self, hdf5_path, min_date=None, max_date=None):
        print(f"Initialising HDF5BlockLoader: {hdf5_path}")
        self.hdf5_path = hdf5_path

        with h5py.File(hdf5_path, 'r') as h5f:
            xs_all = h5f['xs'][:]
            ys_all = h5f['ys'][:]
            ts_all = h5f['ts'][:]
            orig_ts_all = h5f['original_timestamps'][:]

            band_names = [b.decode('ascii') if isinstance(b, bytes) else b
                          for b in h5f.attrs['band_names']]
            print(f"  HDF5 band order: {band_names}")

            self.n_bands      = h5f['values'].shape[1]
            self.n_timesteps_total = h5f['values'].shape[0]

            self.crs        = CRS.from_wkt(str(h5f.attrs['crs']))
            self.tile_left  = float(h5f.attrs['bounds_left'])
            self.tile_right = float(h5f.attrs['bounds_right'])
            self.tile_bottom= float(h5f.attrs['bounds_bottom'])
            self.tile_top   = float(h5f.attrs['bounds_top'])

        # --- temporal filter ---
        time_mask = np.ones(len(ts_all), dtype=bool)
        if min_date is not None:
            time_mask &= ts_all >= min_date.toordinal()
        if max_date is not None:
            time_mask &= ts_all <= max_date.toordinal()

        self.time_indices = np.where(time_mask)[0]   # indices into HDF5 time axis
        self.ts           = ts_all[time_mask]
        self.orig_ts      = orig_ts_all[time_mask]
        self.n_timesteps  = len(self.ts)

        print(f"  Timesteps after date filter: {self.n_timesteps} / {self.n_timesteps_total}")

        # --- build regular grid ---
        self.unique_xs = np.unique(xs_all)
        self.unique_ys = np.unique(ys_all)          # ascending order
        self.grid_w    = len(self.unique_xs)
        self.grid_h    = len(self.unique_ys)

        # pixel size (assume uniform)
        self.pixel_size_x = float(np.diff(self.unique_xs).min()) if self.grid_w > 1 else 10.0
        self.pixel_size_y = float(np.diff(self.unique_ys).min()) if self.grid_h > 1 else 10.0

        # coordinate → grid index lookup
        self.x_to_col = {x: i for i, x in enumerate(self.unique_xs)}
        self.y_to_row = {y: i for i, y in enumerate(self.unique_ys)}

        # map every HDF5 pixel to its (row, col) in the full grid
        self.pixel_rows = np.array([self.y_to_row[y] for y in ys_all], dtype=np.int32)
        self.pixel_cols = np.array([self.x_to_col[x] for x in xs_all], dtype=np.int32)

        # rasterio affine transform for the full tile grid
        # unique_ys is ascending; rasterio expects top-left origin with negative y step
        self.tile_transform = from_origin(
            self.unique_xs.min(),
            self.unique_ys.max(),
            self.pixel_size_x,
            self.pixel_size_y
        )

        print(f"  Full tile grid: {self.grid_h} rows x {self.grid_w} cols")

    def get_blocks(self, block_size):
        """
        Yield (block_row, block_col, row_start, row_end, col_start, col_end)
        for every block across the full tile grid.
        """
        for br in range(0, self.grid_h, block_size):
            row_end = min(br + block_size, self.grid_h)
            for bc in range(0, self.grid_w, block_size):
                col_end = min(bc + block_size, self.grid_w)
                yield br, bc, br, row_end, bc, col_end

    def load_block(self, row_start, row_end, col_start, col_end):
        """
        Load all timesteps for the pixels within the given grid row/col range.

        Returns:
            block_data : ndarray, shape (T, bands, block_h, block_w), dtype uint16
                         Pixels not in the HDF5 mask are filled with S2_NODATA.
            block_transform : Affine
                         Rasterio affine transform for the top-left of this block.
        """
        block_h = row_end - row_start
        block_w = col_end - col_start
        T       = self.n_timesteps

        # find HDF5 pixels that fall inside this block
        in_block = (
            (self.pixel_rows >= row_start) & (self.pixel_rows < row_end) &
            (self.pixel_cols >= col_start) & (self.pixel_cols < col_end)
        )
        local_pixel_indices  = np.where(in_block)[0]   # indices into flat HDF5 pixel axis
        local_rows = self.pixel_rows[local_pixel_indices] - row_start
        local_cols = self.pixel_cols[local_pixel_indices] - col_start

        block_data = np.full((T, self.n_bands, block_h, block_w),
                             S2_NODATA, dtype=np.uint16)

        if len(local_pixel_indices) == 0:
            # block is entirely outside the vector mask — return nodata
            block_transform = from_origin(
                self.unique_xs[col_start],
                self.unique_ys[row_start + block_h - 1],  # top y for this block
                self.pixel_size_x,
                self.pixel_size_y
            )
            return block_data, block_transform

        # Read all timesteps for the selected pixels in one HDF5 access per timestep.
        # Sorting pixel indices improves HDF5 read performance (reduces seeking).
        sort_order          = np.argsort(local_pixel_indices)
        sorted_pixel_idx    = local_pixel_indices[sort_order]
        sorted_local_rows   = local_rows[sort_order]
        sorted_local_cols   = local_cols[sort_order]

        with h5py.File(self.hdf5_path, 'r') as h5f:
            for t_local, t_global in enumerate(self.time_indices):
                timestep_data = h5f['values'][int(t_global), :, sorted_pixel_idx]
                # timestep_data: (bands, n_pixels)
                for b in range(self.n_bands):
                    block_data[t_local, b, sorted_local_rows, sorted_local_cols] = \
                        timestep_data[b]

        # Affine transform: top-left corner of this block
        # unique_ys is ascending, so the top of the block is the largest y in the block
        block_top  = self.unique_ys[row_end - 1]   # largest y in this block row range
        block_left = self.unique_xs[col_start]
        block_transform = from_origin(block_left, block_top,
                                      self.pixel_size_x, self.pixel_size_y)

        return block_data, block_transform

# ============================================================================
# CHIP EXTRACTION
# ============================================================================

def get_chip_windows(block_h, block_w, chip_size, overlap):
    """
    Yield (row_start, row_end, col_start, col_end) for every chip within a block.
    Chips at the edge of the block are shifted inward to always be chip_size x chip_size.
    """
    step = chip_size - overlap
    for r in range(0, block_h, step):
        r = min(r, block_h - chip_size)
        if r < 0:
            continue
        for c in range(0, block_w, step):
            c = min(c, block_w - chip_size)
            if c < 0:
                continue
            yield r, r + chip_size, c, c + chip_size

# ============================================================================
# CASCADING FORWARD SELECTION (after composite)
# ============================================================================

def cascading_forward_select(block_data, t_after_start, t_after_end):
    """
    For each pixel, find the first cloud-free observation in the lookahead
    window [t_after_start, t_after_end) and return it as the "after" image.

    A pixel is considered cloud-free if ALL bands are < S2_NODATA.

    Parameters:
        block_data     : (T, bands, H, W) uint16
        t_after_start  : first timestep index for lookahead (inclusive)
        t_after_end    : last timestep index for lookahead (exclusive)

    Returns:
        after_img      : (bands, H, W) uint16 — S2_NODATA where no valid obs found
        after_valid    : (H, W) bool          — True where a valid obs was found
    """
    _, bands, H, W = block_data.shape
    after_img   = np.full((bands, H, W), S2_NODATA, dtype=np.uint16)
    after_valid = np.zeros((H, W), dtype=bool)

    for t in range(t_after_start, t_after_end):
        obs = block_data[t]                              # (bands, H, W)
        obs_valid = np.all(obs < S2_NODATA, axis=0)     # (H, W)
        # only fill pixels not yet assigned
        to_fill = obs_valid & ~after_valid
        after_img[:, to_fill]  = obs[:, to_fill]
        after_valid[to_fill]   = True

        if after_valid.all():
            break   # all pixels filled — no need to look further

    return after_img, after_valid

# ============================================================================
# MODEL INFERENCE
# ============================================================================

def run_model(model, before_img, after_img, normalise_fn, use_cuda):
    """
    Run BACDM model on a single before/after pair.

    Parameters:
        before_img, after_img : (bands, H, W) uint16 numpy arrays
        normalise_fn          : torchvision transform (ToTensor + Normalize)

    Returns:
        change_prob : (H, W) uint8 numpy array, 0-255
    """
    # Model expects (H, W, bands) input to ToTensor, which reorders to (bands, H, W)
    before_hwc = before_img.transpose(1, 2, 0).astype(np.float32)
    after_hwc  = after_img.transpose(1, 2, 0).astype(np.float32)

    im1 = normalise_fn(before_hwc).unsqueeze(0)   # (1, bands, H, W)
    im2 = normalise_fn(after_hwc).unsqueeze(0)

    if use_cuda:
        im1 = im1.cuda()
        im2 = im2.cuda()

    with torch.no_grad():
        outputs = model(im1, im2)
        result  = outputs[0][0]
        prob    = result[0].unsqueeze(0)           # (1, H, W)

    change_prob = prob.detach().cpu().numpy().squeeze()   # (H, W) float
    change_prob = np.clip(change_prob * 255, 0, 255).astype(np.uint8)
    return change_prob

# ============================================================================
# STATISTICS ACCUMULATION
# ============================================================================

def make_stats_arrays(H, W):
    """Initialise per-pixel accumulation arrays for one chip."""
    return {
        'total_flags':       np.zeros((H, W), dtype=np.int32),
        'total_valid_pairs': np.zeros((H, W), dtype=np.int32),
        'max_consec_flags':  np.zeros((H, W), dtype=np.int32),
        'current_consec':    np.zeros((H, W), dtype=np.int32),  # running counter, not output
    }


def accumulate(stats, change_prob, valid_pair_mask):
    """
    Update accumulation arrays for one sliding window step.

    Parameters:
        stats           : dict of accumulation arrays (modified in place)
        change_prob     : (H, W) uint8 — raw model output
        valid_pair_mask : (H, W) bool  — True where both before and after were valid
    """
    flagged = (change_prob > CHANGE_THRESHOLD) & valid_pair_mask

    stats['total_valid_pairs'] += valid_pair_mask.astype(np.int32)
    stats['total_flags']       += flagged.astype(np.int32)

    # consecutive flags — reset counter for pixels that are valid but not flagged
    stats['current_consec'][flagged]                         += 1
    stats['current_consec'][valid_pair_mask & ~flagged]       = 0
    # pixels that were invalid this step: don't update consecutive counter

    np.maximum(stats['max_consec_flags'],
               stats['current_consec'],
               out=stats['max_consec_flags'])


def finalise_stats(stats):
    """
    Compute flagged_proportion and return final output arrays.

    Returns:
        ndarray, shape (4, H, W), dtype float32
            Band 0: total_flags
            Band 1: total_valid_pairs
            Band 2: max_consec_flags
            Band 3: flagged_proportion  (OUTPUT_NODATA where no valid pairs)
    """
    H, W = stats['total_flags'].shape
    out  = np.full((4, H, W), OUTPUT_NODATA, dtype=np.float32)

    out[0] = stats['total_flags'].astype(np.float32)
    out[1] = stats['total_valid_pairs'].astype(np.float32)
    out[2] = stats['max_consec_flags'].astype(np.float32)

    valid = stats['total_valid_pairs'] > 0
    out[3, valid] = (stats['total_flags'][valid] /
                     stats['total_valid_pairs'][valid]).astype(np.float32)

    return out

# ============================================================================
# OUTPUT WRITING
# ============================================================================

def write_chip_output(output_path, chip_stats, chip_transform, crs):
    """Write the 4-band statistics raster for one chip."""
    _, H, W = chip_stats.shape
    profile = {
        'driver':    'GTiff',
        'dtype':     'float32',
        'count':     4,
        'height':    H,
        'width':     W,
        'crs':       crs,
        'transform': chip_transform,
        'nodata':    OUTPUT_NODATA,
        'compress':  'lzw',
    }
    with rasterio.open(output_path, 'w', **profile) as dst:
        dst.write(chip_stats)
        dst.descriptions = (
            'total_flags',
            'total_valid_pairs',
            'max_consec_flags',
            'flagged_proportion',
        )

# ============================================================================
# BLOCK PROCESSING
# ============================================================================

def process_block(block_idx, loader, model, normalise_fn, use_cuda):
    """
    Process one spatial block: load data, slide temporal window over every
    chip, accumulate statistics, write output chips.

    Parameters:
        block_idx : int — index into the list returned by loader.get_blocks()
    """
    blocks = list(loader.get_blocks(BLOCK_SIZE))
    if block_idx >= len(blocks):
        print(f"Block index {block_idx} out of range ({len(blocks)} blocks total).")
        return

    br, bc, row_start, row_end, col_start, col_end = blocks[block_idx]
    block_h = row_end - row_start
    block_w = col_end - col_start

    print(f"\nBlock {block_idx}/{len(blocks)-1}  "
          f"rows [{row_start},{row_end})  cols [{col_start},{col_end})")

    t0 = time.time()
    block_data, block_transform = loader.load_block(row_start, row_end,
                                                    col_start, col_end)
    print(f"  Loaded block in {time.time()-t0:.1f}s  "
          f"shape={block_data.shape}  "
          f"size={block_data.nbytes/1e6:.0f} MB")

    T = loader.n_timesteps
    if T < 2:
        print("  Not enough timesteps to form a pair. Skipping block.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    chips_written = 0

    for r0, r1, c0, c1 in get_chip_windows(block_h, block_w, CHIP_SIZE, OVERLAP):
        chip_data = block_data[:, :, r0:r1, c0:c1]   # (T, bands, 256, 256)
        stats     = make_stats_arrays(CHIP_SIZE, CHIP_SIZE)

        # --- sliding temporal window ---
        # before = single obs at t; after = first valid in [t+1, t+LOOKAHEAD]
        for t in range(T - 1):
            before_obs   = chip_data[t]                          # (bands, H, W)
            before_valid = np.all(before_obs < S2_NODATA, axis=0)  # (H, W)

            t_after_end  = min(t + 1 + LOOKAHEAD, T)
            after_obs, after_valid = cascading_forward_select(
                chip_data, t + 1, t_after_end
            )

            valid_pair_mask = before_valid & after_valid

            # skip step entirely if no pixel has a valid pair
            if not valid_pair_mask.any():
                continue

            change_prob = run_model(model, before_obs, after_obs,
                                    normalise_fn, use_cuda)
            accumulate(stats, change_prob, valid_pair_mask)

        # --- write chip output ---
        chip_stats = finalise_stats(stats)

        # affine transform for this chip within the tile
        chip_left = loader.unique_xs[col_start + c0]
        chip_top  = loader.unique_ys[row_start + r0 + CHIP_SIZE - 1]  # largest y in chip
        chip_transform = from_origin(chip_left, chip_top,
                                     loader.pixel_size_x, loader.pixel_size_y)

        out_name = f"block{block_idx:04d}_r{row_start+r0}_c{col_start+c0}.tif"
        write_chip_output(os.path.join(OUTPUT_DIR, out_name),
                          chip_stats, chip_transform, loader.crs)
        chips_written += 1

    print(f"  Block {block_idx} done — {chips_written} chips written  "
          f"({time.time()-t0:.1f}s total)")

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    use_cuda = AAA_Configs.USE_CUDA and torch.cuda.is_available()
    device   = 'cuda' if use_cuda else 'cpu'
    print(f"Device: {device}")

    print("Loading model...")
    model       = load_model(WEIGHT_PATH, use_cuda)
    normalise_fn = get_normalise_transform()

    print("Initialising HDF5 loader...")
    loader = HDF5BlockLoader(HDF5_FILE, min_date=MIN_DATE, max_date=MAX_DATE)

    # --- HPC mode: pass block index as command-line argument ---
    # --- Local mode: process all blocks sequentially         ---
    if len(sys.argv) > 1:
        block_idx = int(sys.argv[1])
        process_block(block_idx, loader, model, normalise_fn, use_cuda)
    else:
        blocks = list(loader.get_blocks(BLOCK_SIZE))
        print(f"\nNo block index provided — processing all {len(blocks)} blocks sequentially.")
        for i in range(len(blocks)):
            process_block(i, loader, model, normalise_fn, use_cuda)

    print("\nDone.")
