"""
Diagnostic for HDF5 chunk straddling.

Reports:
  1. Dataset shape, chunk shape, and dtype for the 'values' dataset.
  2. Whether xs/ys are stored in row-major spatial order or scrambled.
  3. (Optional, if a vchip path is given) how many chunks one vchip's pixels span.

Run:
    python check_hdf5_chunk_layout.py <hdf5_path> [<vchip_path>]
"""
import os
import sys
import h5py
import numpy as np
import rasterio as rio


def main():
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: python check_hdf5_chunk_layout.py <hdf5_path> [<vchip_path>]",
              file=sys.stderr)
        sys.exit(1)

    hdf5_path = sys.argv[1]
    vchip_path = sys.argv[2] if len(sys.argv) == 3 else None

    print(f"\n=== HDF5 file: {hdf5_path} ===\n")

    with h5py.File(hdf5_path, 'r') as h5f:
        values_ds = h5f['values']
        xs: np.ndarray = h5f['xs'][:]    # type: ignore[index]
        ys: np.ndarray = h5f['ys'][:]    # type: ignore[index]
        ts: np.ndarray = h5f['ts'][:]    # type: ignore[index]

        # ---- 1. Dataset basics ----
        shape = values_ds.shape          # type: ignore[union-attr]
        chunks = values_ds.chunks        # type: ignore[union-attr]
        dtype = values_ds.dtype          # type: ignore[union-attr]
        compression = values_ds.compression  # type: ignore[union-attr]
        n_t, n_bands, n_pixels = shape

        print("Dataset 'values':")
        print(f"  shape        = (n_timesteps={n_t}, n_bands={n_bands}, n_pixels={n_pixels:,})")
        print(f"  chunks       = {chunks}")
        print(f"  dtype        = {dtype}")
        print(f"  compression  = {compression}")
        print(f"  n_timesteps  = {n_t}")
        print(f"  n_pixels (xs) = {len(xs):,}")
        print(f"  n_pixels (ys) = {len(ys):,}")
        if chunks is not None:
            n_chunks_pixel_axis = (n_pixels + chunks[2] - 1) // chunks[2]
            chunk_uncompressed_bytes = chunks[0] * chunks[1] * chunks[2] * dtype.itemsize
            print(f"  chunk pixel-axis size = {chunks[2]:,}")
            print(f"  chunks along pixel axis = {n_chunks_pixel_axis}")
            print(f"  uncompressed chunk size = {chunk_uncompressed_bytes / 1e6:.2f} MB")

        # ---- 2. Spatial order check on xs/ys ----
        print("\n=== Pixel storage order ===\n")

        unique_ys = np.unique(ys)
        unique_xs = np.unique(xs)
        print(f"  Unique y values: {len(unique_ys):,}  (range {unique_ys.min():.0f} -> {unique_ys.max():.0f})")
        print(f"  Unique x values: {len(unique_xs):,}  (range {unique_xs.min():.0f} -> {unique_xs.max():.0f})")

        if len(unique_ys) > 1:
            pixel_size_y = float(np.diff(unique_ys).min())
        else:
            pixel_size_y = float('nan')
        if len(unique_xs) > 1:
            pixel_size_x = float(np.diff(unique_xs).min())
        else:
            pixel_size_x = float('nan')
        print(f"  Implied pixel size: {pixel_size_x:.2f} x {pixel_size_y:.2f}")
        print(f"  Implied tile dims: width={len(unique_xs)} x height={len(unique_ys)}")

        # Look at the first 16 entries to see if they form a contiguous run.
        print("\nFirst 16 (x, y) pairs in the file:")
        for i in range(min(16, len(xs))):
            print(f"  [{i:6d}]  x={xs[i]:.0f}  y={ys[i]:.0f}")

        # Decide row-major vs scrambled by checking how many *pixels* are between
        # adjacent storage entries that share the same y. If the file is row-major,
        # consecutive entries within a row should differ by 1 unit of pixel_size_x.
        if pixel_size_x == pixel_size_x:  # not NaN
            adjacent_dx = np.diff(xs[:1000])
            same_y = np.diff(ys[:1000]) == 0
            in_row_steps = adjacent_dx[same_y]
            if len(in_row_steps) > 0:
                exact_step_count = int(np.sum(in_row_steps == pixel_size_x))
                print(f"\n  Of first 1000 entries, {len(in_row_steps)} share the same y as their predecessor.")
                print(f"  Of those, {exact_step_count} step exactly +{pixel_size_x:.0f} in x "
                      f"({100 * exact_step_count / max(len(in_row_steps), 1):.1f}%).")
                if exact_step_count / max(len(in_row_steps), 1) > 0.9:
                    print(f"  -> Pixels look ROW-MAJOR (within a row, x increments by one pixel).")
                else:
                    print(f"  -> Pixels do NOT look row-major. Storage order is scrambled or different.")

        # ---- 3. Per-vchip chunk-straddle diagnostic ----
        if vchip_path is None:
            print("\n(no vchip provided; skipping chunk-straddle check)\n"
                  "Re-run with `python check_hdf5_chunk_layout.py <hdf5_path> <vchip_path>`")
            return

        print(f"\n=== Chunk-straddle check for vchip: {os.path.basename(vchip_path)} ===\n")

        with rio.open(vchip_path) as src:
            t = src.transform
            xmin = t.c
            ymax = t.f
            psx = t.a
            psy = -t.e
            xmax = xmin + src.width * psx
            ymin = ymax - src.height * psy
            print(f"  vchip dims    : {src.width} x {src.height}  ({src.width * src.height} pixels)")
            print(f"  vchip bounds  : x=[{xmin:.0f}, {xmax:.0f}]  y=[{ymin:.0f}, {ymax:.0f}]")

        pixel_mask = (xs >= xmin) & (xs < xmax) & (ys > ymin) & (ys <= ymax)
        pixel_indices = np.where(pixel_mask)[0]
        print(f"  HDF5 pixels covered : {len(pixel_indices)}")

        if len(pixel_indices) == 0:
            print("  WARNING: no HDF5 pixels fall in this vchip — wrong tile?")
            return

        if chunks is None:
            print("  Dataset is not chunked — chunk-straddle check N/A.")
            return

        chunk_size = chunks[2]
        chunks_touched = np.unique(pixel_indices // chunk_size)
        print(f"  Chunk size (pixel axis): {chunk_size:,}")
        print(f"  Chunks touched         : {len(chunks_touched)}")
        if len(chunks_touched) <= 32:
            print(f"  Chunk indices          : {chunks_touched.tolist()}")

        chunk_uncompressed_bytes = n_bands * chunk_size * dtype.itemsize
        wanted_bytes_per_t = n_bands * len(pixel_indices) * dtype.itemsize
        loaded_bytes_per_t = len(chunks_touched) * chunk_uncompressed_bytes
        if wanted_bytes_per_t > 0:
            ratio = loaded_bytes_per_t / wanted_bytes_per_t
        else:
            ratio = float('inf')

        print(f"\n  Per-timestep theoretical minimum read : {wanted_bytes_per_t / 1e6:6.2f} MB")
        print(f"  Per-timestep actual chunk-driven read : {loaded_bytes_per_t / 1e6:6.2f} MB")
        print(f"  Over-read ratio                       : {ratio:.1f}x")

        if ratio > 5:
            print("\n  -> ACCESS PATTERN STRADDLES MANY CHUNKS.")
            print("     Each timestep loads many full chunks just to extract a few pixels each.")
        elif ratio > 1.5:
            print("\n  -> Mild straddle. Some over-read, but not the dominant cost.")
        else:
            print("\n  -> Access is well-aligned with chunk layout.")


if __name__ == "__main__":
    main()
