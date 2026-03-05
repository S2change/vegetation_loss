import os
import re
import numpy as np
import h5py
import rasterio
from rasterio.features import rasterize
import rasterio.transform
from datetime import datetime, timezone
from shapely.geometry import box
import geopandas as gpd

'''
This script reads pairs of 4-band and 2-band GeoTIFF files from specified folders, filters out
files with no overlap with a vector mask, rasterizes the mask to identify valid pixels, and writes
the sparse pixel time series to an HDF5 file. Only pixels inside the vector mask are stored.

The output HDF5 file contains:
- values: (time, bands, pixels) - sparse pixel array for masked pixels only
- xs, ys: (pixels,) - coordinate arrays for masked pixels
- ts: (time,) - ordinal dates
- original_timestamps: (time,) - unix timestamps in milliseconds
- band_names attribute: band names in order

Inputs:
- 'folder_path_4bands': Directory containing the 4-band GeoTIFF files (e.g., B3, B4, B8, B12).
- 'folder_path_2bands': Directory containing the 2-band GeoTIFF files (e.g., B2, B11).
- 'vector_mask_path': Path to vector file (shapefile, GeoJSON, etc.) defining the region of interest.
- 'h5_filename': Path for the output HDF5 file to be created.

Note: TIFs with no overlap with the mask bounding box are discarded. TIFs that partially
overlap are kept — the boolean pixel mask determines which pixels are written to the HDF5.
'''

folder_path_4bands = r"D:\s2_images\T29TQG"
folder_path_2bands = r"C:\Users\Public\Documents\s2_images_B2_B11\T29TQG"
vector_mask_path   = r"C:\path\to\your\mask.shp"
h5_filename        = os.path.join(r"E:\T29TQG", 'T29TQG_6bands_masked.h5')

##########
folder_path = r'C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\temp\test_tif_to_hdf5'
folder_path_4bands = os.path.join(folder_path, '4bands')
folder_path_2bands = os.path.join(folder_path, '2bands')
vector_mask_path   = os.path.join(folder_path, 'portugal_continental_32629.gpkg')
h5_filename = os.path.join(folder_path, 'test_1667647823345_6bands.h5')

folder_path_4bands = r"H:\s2_images\T29TQG"
folder_path_2bands = r"D:\s2_images_B2_B11\T29TQG"
h5_filename = os.path.join(folder_path, 'T29TQG_6bands.h5') # output in folder_path (to be changed)
#########

# Folder '4bands' (B3, B4, B8, B12) followed by Folder '2bands' (B2, B11)
band_names = ["B3", "B4", "B8", "B12", "B2", "B11"]
NODATA_VAL = 65535
APPLY_MASK_FILTER = False # Set to True to exclude files with mismatched headers (potentially due to mask filtering)

def parse_and_sort_files(folder):
    """Parse timestamps from filenames and return metadata sorted by date."""
    files = [f for f in os.listdir(folder) if f.endswith('.tif')]
    file_metadata = []
    for f in files:
        match = re.search(r'_(\d{13})\.tif', f)
        if match:
            ts_ms = int(match.group(1))
            dt = datetime.fromtimestamp(ts_ms / 1000.0, timezone.utc).date()
            file_metadata.append({
                'filename': f,
                'ordinal': dt.toordinal(),
                'timestamp_ms': ts_ms
            })
    file_metadata.sort(key=lambda x: x['ordinal'])
    return file_metadata


def read_all_bounds(folder, filenames):
    """Read bounding boxes for all TIF files."""
    print("Reading extents from all files...")
    all_bounds = {}
    for f in filenames:
        with rasterio.open(os.path.join(folder, f)) as src:
            all_bounds[f] = src.bounds
    return all_bounds


def get_reference_tif(folder, filenames, all_bounds):
    """Find the largest TIF by bounding box area and return its metadata."""
    print("Finding largest TIF as spatial reference...")
    largest_file = max(filenames, key=lambda f: (
        (all_bounds[f].right - all_bounds[f].left) * (all_bounds[f].top - all_bounds[f].bottom)
    ))
    with rasterio.open(os.path.join(folder, largest_file)) as ref_src:
        ref_crs = ref_src.crs
        ref_transform = ref_src.transform
        ref_meta = ref_src.meta.copy()
    print(f"  Reference TIF: {largest_file}")
    print(f"  Bounds: {all_bounds[largest_file]}")
    return largest_file, ref_crs, ref_transform, ref_meta


def clip_vector_mask(vector_mask_path, ref_bounds, ref_crs):
    """Load vector mask, reproject if needed, and clip to reference TIF extent."""
    print(f"Loading vector mask: {vector_mask_path}")
    vector_mask = gpd.read_file(vector_mask_path)
    if vector_mask.crs != ref_crs:
        vector_mask = vector_mask.to_crs(ref_crs)

    tile_polygon = box(ref_bounds.left, ref_bounds.bottom, ref_bounds.right, ref_bounds.top)
    tile_gdf = gpd.GeoDataFrame({"geometry": [tile_polygon]}, crs=ref_crs)
    clipped_mask = gpd.clip(vector_mask, tile_gdf)

    if clipped_mask.empty:
        raise ValueError("Vector mask does not overlap the reference TIF extent.")

    mask_left, mask_bottom, mask_right, mask_top = clipped_mask.total_bounds
    print(f"  Mask bounding box: X=[{mask_left:.1f}, {mask_right:.1f}]  Y=[{mask_bottom:.1f}, {mask_top:.1f}]")
    return clipped_mask


def filter_by_mask_overlap(all_bounds, clipped_mask):
    """Discard TIFs with no overlap with the mask bounding box."""
    print("Filtering files against mask bounding box...")
    mask_left, mask_bottom, mask_right, mask_top = clipped_mask.total_bounds

    outlier_files = []
    aligned_files = []
    for f, b in all_bounds.items():
        no_overlap = (b.right < mask_left or b.left > mask_right or
                      b.top < mask_bottom or b.bottom > mask_top)
        if no_overlap:
            outlier_files.append(f)
        else:
            aligned_files.append(f)

    if outlier_files:
        print(f"WARNING: Discarding {len(outlier_files)} file(s):")
        for fname in outlier_files[:5]:
            b = all_bounds[fname]
            print(f"  - {fname}  bounds=({b.left:.1f}, {b.bottom:.1f}, {b.right:.1f}, {b.top:.1f})")
        if len(outlier_files) > 5:
            print(f"  ... and {len(outlier_files) - 5} more")

    print(f"Continuing with {len(aligned_files)} files")
    return aligned_files


def rasterize_mask(clipped_mask, ref_meta, ref_transform):
    """Rasterize the clipped vector mask on the reference TIF grid."""
    print("Rasterizing vector mask...")
    clipped_mask = clipped_mask.copy()
    clipped_mask["raster_value"] = 1
    shapes = [(geom, val) for geom, val in zip(clipped_mask.geometry, clipped_mask["raster_value"])]
    rasterized = rasterize(
        shapes=shapes,
        out_shape=(ref_meta['height'], ref_meta['width']),
        transform=ref_meta['transform'],
        fill=0,
        dtype="uint8"
    ).astype(bool)

    total_masked_pixels = int(rasterized.sum())
    print(f"  Total masked pixels: {total_masked_pixels}")

    mask_rows, mask_cols = np.where(rasterized)
    xs_flat, ys_flat = rasterio.transform.xy(ref_transform, mask_rows, mask_cols, offset='ul')
    xs_flat = np.array(xs_flat, dtype=np.int32)
    ys_flat = np.array(ys_flat, dtype=np.int32)

    return total_masked_pixels, xs_flat, ys_flat


def check_header_compatibility(folder_4b, folder_2b, filenames,apply_mask_filter=True):
    """Verify that 4-band and 2-band files have matching spatial headers."""
    if apply_mask_filter:
        print("Checking 4-band/2-band header compatibility...")
    header_failed = []
    header_passed = []
    for filename in filenames:
        if apply_mask_filter:
            with rasterio.open(os.path.join(folder_4b, filename)) as src4, \
                rasterio.open(os.path.join(folder_2b, filename)) as src2:
                if (src4.bounds != src2.bounds or
                        src4.transform != src2.transform or
                        src4.shape != src2.shape):
                    header_failed.append(filename)
                else:
                    header_passed.append(filename)
        else:
            header_passed.append(filename)

    if header_failed:
        print(f"Excluding {len(header_failed)} file(s) with mismatched headers:")
        for fname in header_failed:
            print(f"  - {fname}")

    print(f"Header check complete. {len(header_passed)} file(s) ready for HDF5 write.")
    return header_passed


def write_hdf5(h5_filename, sorted_files, file_metadata, folder_4b, folder_2b,
               band_names, total_masked_pixels, xs_flat, ys_flat):
    """Write sparse pixel time series to HDF5."""
    nbands = len(band_names)
    with h5py.File(h5_filename, 'w') as h5f:
        dset_values = h5f.create_dataset(
            "values",
            shape=(len(sorted_files), nbands, total_masked_pixels),
            dtype='uint16',
            chunks=(1, nbands, min(1000000, total_masked_pixels)),
            compression="lzf"
        )

        h5f.attrs['band_names'] = [n.encode('ascii') for n in band_names]
        h5f.create_dataset("xs", data=xs_flat, dtype='int32')
        h5f.create_dataset("ys", data=ys_flat, dtype='int32')
        h5f.create_dataset("ts", data=[m['ordinal'] for m in file_metadata], dtype='int32')
        h5f.create_dataset("original_timestamps",
                           data=[m['timestamp_ms'] for m in file_metadata],
                           dtype='int64')

        for i, filename in enumerate(sorted_files):
            print(f"Processing {i+1}/{len(sorted_files)}: {filename}")

            with rasterio.open(os.path.join(folder_4b, filename)) as src4, \
                 rasterio.open(os.path.join(folder_2b, filename)) as src2:
                tif_rows, tif_cols = rasterio.transform.rowcol(src4.transform, xs_flat, ys_flat)
                tif_rows = np.array(tif_rows)
                tif_cols = np.array(tif_cols)

                valid = ((tif_rows >= 0) & (tif_rows < src4.height) &
                         (tif_cols >= 0) & (tif_cols < src4.width))

                data4 = src4.read()  # (4, H, W)
                data2 = src2.read()  # (2, H, W)
                data_all = np.concatenate([data4, data2], axis=0)  # (6, H, W)

                # --- NEW: EFFICIENT MASKING LOGIC ---
                # Create a 2D mask (H, W) where True means at least one band is NoData
                # axis=0 refers to the bands dimension
                nodata_mask = np.any(data_all == NODATA_VAL, axis=0)
                
                # Apply mask to all 6 bands at once: if one is 65535, all become 65535
                data_all[:, nodata_mask] = NODATA_VAL

                out = np.full((nbands, total_masked_pixels), NODATA_VAL, dtype=np.uint16)
                out[:, valid] = data_all[:, tif_rows[valid], tif_cols[valid]]
                dset_values[i, :, :] = out


if __name__ == "__main__":
    file_metadata = parse_and_sort_files(folder_path_4bands)
    sorted_files = [m['filename'] for m in file_metadata]

    all_bounds = read_all_bounds(folder_path_4bands, sorted_files)

    largest_file, ref_crs, ref_transform, ref_meta = get_reference_tif(
        folder_path_4bands, sorted_files, all_bounds
    )

    clipped_mask = clip_vector_mask(vector_mask_path, all_bounds[largest_file], ref_crs)

    aligned_files = filter_by_mask_overlap(all_bounds, clipped_mask)
    file_metadata = [m for m in file_metadata if m['filename'] in set(aligned_files)]

    ref_meta.update({"count": 1})
    total_masked_pixels, xs_flat, ys_flat = rasterize_mask(clipped_mask, ref_meta, ref_transform)

    sorted_files = check_header_compatibility(folder_path_4bands, folder_path_2bands, aligned_files, apply_mask_filter=APPLY_MASK_FILTER)
    file_metadata = [m for m in file_metadata if m['filename'] in set(sorted_files)]

    write_hdf5(h5_filename, sorted_files, file_metadata, folder_path_4bands, folder_path_2bands,
            band_names, total_masked_pixels, xs_flat, ys_flat)

    print(f"Done! Created {h5_filename} with {total_masked_pixels} masked pixels and {len(sorted_files)} timesteps.")
