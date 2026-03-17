import numpy as np
import rasterio
import rasterio.transform
from rasterio.windows import from_bounds

tif_a = r"E:\T29TQG\CNCA_tifs_to_hdf5_tests\T29TQG_tifs_for_testing\append_to_hdf5\S2SR_image_1522063466000.tif"
tif_b = r"E:\T29TQG\CNCA_tifs_to_hdf5_tests\T29TQG_reconstructed_tifs\appended_hdf5\T29TQG_CNCA_appended_2_2017-04-15.tif"

with rasterio.open(tif_a) as a, rasterio.open(tif_b) as b:
    # Check CRS and resolution are compatible
    if a.crs != b.crs:
        print(f"MISMATCH: CRS {a.crs} vs {b.crs} — cannot compare overlap.")
        exit()
    if a.res != b.res:
        print(f"MISMATCH: resolution {a.res} vs {b.res} — cannot compare overlap.")
        exit()
    if a.count != b.count:
        print(f"MISMATCH: band count {a.count} vs {b.count} — cannot compare overlap.")
        exit()
    if a.dtypes != b.dtypes:
        print(f"MISMATCH: dtypes {a.dtypes} vs {b.dtypes}")

    # Find overlapping extent
    overlap_left   = max(a.bounds.left,   b.bounds.left)
    overlap_bottom = max(a.bounds.bottom, b.bounds.bottom)
    overlap_right  = min(a.bounds.right,  b.bounds.right)
    overlap_top    = min(a.bounds.top,    b.bounds.top)

    if overlap_left >= overlap_right or overlap_bottom >= overlap_top:
        print("No spatial overlap between the two TIFs.")
        exit()

    print(f"Overlap extent: X=[{overlap_left}, {overlap_right}]  Y=[{overlap_bottom}, {overlap_top}]")
    print(f"  TIF A full extent: {a.bounds}")
    print(f"  TIF B full extent: {b.bounds}")

    # Read only the overlapping window from each file
    win_a = from_bounds(overlap_left, overlap_bottom, overlap_right, overlap_top, a.transform)
    win_b = from_bounds(overlap_left, overlap_bottom, overlap_right, overlap_top, b.transform)

    data_a = a.read(window=win_a)
    data_b = b.read(window=win_b)

    # Shapes may differ by 1 pixel due to floating point — crop to the smaller
    min_rows = min(data_a.shape[1], data_b.shape[1])
    min_cols = min(data_a.shape[2], data_b.shape[2])
    data_a = data_a[:, :min_rows, :min_cols]
    data_b = data_b[:, :min_rows, :min_cols]

    print(f"Comparing overlap region: {data_a.shape[1]} rows x {data_a.shape[2]} cols x {data_a.shape[0]} bands")

    if np.array_equal(data_a, data_b):
        print("IDENTICAL: pixel values in the overlapping region are exactly the same.")
    else:
        diff = data_a != data_b
        n_diff = int(diff.sum())
        n_total = diff.size
        print(f"DIFFERENT: {n_diff} / {n_total} pixels differ ({100 * n_diff / n_total:.4f}%)")
        for band_idx in range(data_a.shape[0]):
            band_diff = diff[band_idx]
            if band_diff.any():
                delta = np.abs(data_a[band_idx].astype(np.float32) - data_b[band_idx].astype(np.float32))
                print(f"  Band {band_idx + 1}: {band_diff.sum()} differing pixels — "
                      f"max delta={delta.max():.1f}, mean delta={delta[band_diff].mean():.4f}")

        # Show coordinates of first 5 differing pixels (any band)
        any_band_diff = diff.any(axis=0)
        diff_rows, diff_cols = np.where(any_band_diff)
        print(f"\nFirst {min(5, len(diff_rows))} differing pixel locations:")
        overlap_transform = a.window_transform(win_a)
        for i in range(min(5, len(diff_rows))):
            row, col = diff_rows[i], diff_cols[i]
            x, y = rasterio.transform.xy(overlap_transform, row, col)
            vals_a = data_a[:, row, col].tolist()
            vals_b = data_b[:, row, col].tolist()
            print(f"  pixel ({row}, {col})  coords ({x:.1f}, {y:.1f})  A={vals_a}  B={vals_b}")
