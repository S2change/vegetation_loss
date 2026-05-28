"""
Check bounding box, CRS, shape, and value domain for all TIF files in the
Portugal-boundary mask folder.
"""
import os
import numpy as np
import rasterio

MASK_FOLDER = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\testes_cnca_filtar_hdf5_nuvems\exemplo_T29SMD_SMC\Mascara_PT_S2"


def main():
    tif_files = sorted(
        f for f in os.listdir(MASK_FOLDER)
        if f.lower().endswith('.tif')
    )
    if not tif_files:
        print(f"No TIF files found in {MASK_FOLDER}")
        return

    for fname in tif_files:
        path = os.path.join(MASK_FOLDER, fname)
        with rasterio.open(path) as src:
            bounds = src.bounds
            crs = src.crs
            nrows, ncols = src.height, src.width
            transform = src.transform
            data = src.read(1)

        unique_vals = np.unique(data)
        print(f"\n{fname}")
        print(f"  CRS        : {crs}")
        print(f"  Bounds     : left={bounds.left}, bottom={bounds.bottom}, right={bounds.right}, top={bounds.top}")
        print(f"  Shape      : {nrows} rows x {ncols} cols")
        print(f"  Value domain: min={data.min()}, max={data.max()}, unique={unique_vals}")

        rows0, cols0 = np.where(data == 0)
        if rows0.size == 0:
            print(f"  Tight bbox (value==0): no pixels with value 0")
        else:
            # pixel corners of the extreme rows/cols
            xs_left,  ys_top    = rasterio.transform.xy(transform, rows0.min(), cols0.min(), offset='ul')
            xs_right, ys_bottom = rasterio.transform.xy(transform, rows0.max(), cols0.max(), offset='lr')
            tight_nrows = int(rows0.max() - rows0.min() + 1)
            tight_ncols = int(cols0.max() - cols0.min() + 1)
            print(f"  Tight bbox (value==0): left={xs_left}, bottom={ys_bottom}, right={xs_right}, top={ys_top}")
            print(f"  Tight bbox shape     : {tight_nrows} rows x {tight_ncols} cols")


if __name__ == "__main__":
    main()
