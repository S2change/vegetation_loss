import os
import sys
import re
import numpy as np
import h5py
import rasterio
from rasterio.features import rasterize
from rasterio.transform import xy
from datetime import datetime
from shapely.geometry import box
import geopandas as gpd
from hdf5_utils import parse_and_sort_files, read_all_bounds, INPUT_NODATA_VAL, OUTPUT_NODATA_VAL, BAND_NAMES, TILE_NAMES

# Paths
root_folder = 
folder_tifs = 
vector_mask_path = 
folder_hdf5 = 
CLOUD_REPORT = 

# Date filters
MIN_DATE = None 
MAX_DATE = None


def load_valid_l2a_from_report(report_path, threshold=60):
    
    valid_files = {}
    pattern = re.compile(
            r"S2[ABC]_MSIL1C_([0-9]{8}-[0-9]{6}).*?Nuvem na Área 0:\s*([\d\.]+)"
    )

    with open(report_path) as f:
        for line in f:
            m = pattern.search(line)
            if not m:
                continue
            timestamp = m.group(1)
            cloud = float(m.group(2))
            if cloud < threshold:
                valid_files[timestamp] = cloud
    return valid_files

def filter_l2a_files(file_metadata, valid_timestamps):
    
    filtered = []
    for m in file_metadata:
        match = re.search(r"MSIL2A_([0-9]{8}-[0-9]{6})", m['filename'])
        if match:
            timestamp = match.group(1)
            if timestamp in valid_timestamps:
                filtered.append(m)
    return filtered


def main():
    valid_l2a = load_valid_l2a_from_report(CLOUD_REPORT, threshold=60)

    for tile in TILE_NAMES:
        print(f"\nProcessing tile {tile}...")
        h5_filename = os.path.join(folder_hdf5, f'{tile}.h5')

        file_metadata = parse_and_sort_files(folder_tifs, tile, MIN_DATE, MAX_DATE)
        file_metadata = filter_l2a_files(file_metadata, valid_l2a.keys())
        sorted_files = [m['path'] for m in file_metadata]

        if not sorted_files:
            print(f"No files found for tile {tile}. Skipping.")
            continue

        print("Reading extents from all files...")
        all_bounds = read_all_bounds(sorted_files)
        largest_file, ref_crs, ref_transform, ref_meta = get_reference_tif(sorted_files, all_bounds)
        clipped_mask = clip_vector_mask(vector_mask_path, all_bounds[os.path.basename(largest_file)], ref_crs)
        aligned_files = filter_by_mask_overlap(all_bounds, clipped_mask)
        file_metadata = [m for m in file_metadata if m['filename'] in set(aligned_files)]
        aligned_paths = [m['path'] for m in file_metadata if m['filename'] in set(aligned_files)]

        ref_meta.update({"count": 1})
        total_masked_pixels, xs_flat, ys_flat = rasterize_mask(clipped_mask, ref_meta, ref_transform)

        write_hdf5(h5_filename, aligned_paths, file_metadata, BAND_NAMES,
                   total_masked_pixels, xs_flat, ys_flat, ref_transform, ref_crs, valid_l2a)

        print(f"Done! Created {h5_filename} with {total_masked_pixels} masked pixels and {len(aligned_files)} timesteps.")


def get_reference_tif(filepaths, all_bounds):
    print("Finding largest TIF as spatial reference...")
    largest_file = max(filepaths, key=lambda f: (
        (all_bounds[os.path.basename(f)].right - all_bounds[os.path.basename(f)].left) *
        (all_bounds[os.path.basename(f)].top - all_bounds[os.path.basename(f)].bottom)
    ))
    with rasterio.open(largest_file) as ref_src:
        ref_crs = ref_src.crs
        ref_transform = ref_src.transform
        ref_meta = ref_src.meta.copy()
    print(f"  Reference TIF: {largest_file}")
    print(f"  Bounds: {all_bounds[os.path.basename(largest_file)]}")
    return largest_file, ref_crs, ref_transform, ref_meta

def clip_vector_mask(vector_mask_path, ref_bounds, ref_crs):
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
    print("Filtering files against mask bounding box...")
    mask_left, mask_bottom, mask_right, mask_top = clipped_mask.total_bounds
    aligned_files, outlier_files = [], []
    for f, b in all_bounds.items():
        no_overlap = (b.right < mask_left or b.left > mask_right or
                      b.top < mask_bottom or b.bottom > mask_top)
        if no_overlap:
            outlier_files.append(f)
        else:
            aligned_files.append(f)
    if outlier_files:
        print(f"WARNING: Discarding {len(outlier_files)} file(s)")
    print(f"Continuing with {len(aligned_files)} files")
    return aligned_files

def rasterize_mask(clipped_mask, ref_meta, ref_transform):
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
    xs_flat, ys_flat = xy(ref_transform, mask_rows, mask_cols, offset='ul')
    return total_masked_pixels, np.array(xs_flat, dtype=np.int32), np.array(ys_flat, dtype=np.int32)


def write_hdf5(h5_filename, sorted_files, file_metadata, band_names,
               total_masked_pixels, xs_flat, ys_flat, ref_transform, ref_crs, valid_l2a):
    nbands = len(band_names)
    with h5py.File(h5_filename, 'w') as h5f:
        dset_values = h5f.create_dataset(
            "values",
            shape=(len(sorted_files), nbands, total_masked_pixels),
            dtype='uint16',
            maxshape=(None, nbands, total_masked_pixels),
            chunks=(1, nbands, 2810880),
            compression="lzf"
        )
        h5f.attrs['band_names'] = [n.encode('ascii') for n in band_names]
        h5f.attrs['crs'] = ref_crs.to_wkt()
        h5f.attrs['bounds_left'] = float(xs_flat.min())
        h5f.attrs['bounds_right'] = float(xs_flat.max()) + ref_transform.a
        h5f.attrs['bounds_bottom'] = float(ys_flat.min()) + ref_transform.e
        h5f.attrs['bounds_top'] = float(ys_flat.max())
        h5f.create_dataset("xs", data=xs_flat, dtype='int32')
        h5f.create_dataset("ys", data=ys_flat, dtype='int32')
        h5f.create_dataset("ts", data=[m['ordinal'] for m in file_metadata], dtype='int32', maxshape=(None,))
        h5f.create_dataset("original_timestamps",
                           data=[m['timestamp_ms'] for m in file_metadata],
                           dtype='int64', maxshape=(None,))

        for i, m in enumerate(file_metadata):
            filename = m['path']
            # extrai timestamp para buscar % de nuvens
            timestamp = m.get('timestamp')
            if timestamp is None:
                match = re.search(r"S2[ABC]_MSIL2A_(\d{8}-\d{6})", m['filename'])
                timestamp = match.group(1) if match else None
            cloud_pct = valid_l2a.get(timestamp, None)
            if cloud_pct is not None:
                print(f"Processing {i+1}/{len(sorted_files)}: {filename}  |  Cloud: {cloud_pct:.1f}%")
            else:
                print(f"Processing {i+1}/{len(sorted_files)}: {filename}  |  Cloud: N/A")

            with rasterio.open(filename) as src:
                tif_rows, tif_cols = rasterio.transform.rowcol(src.transform, xs_flat, ys_flat)
                tif_rows, tif_cols = np.array(tif_rows), np.array(tif_cols)
                valid = ((tif_rows >= 0) & (tif_rows < src.height) &
                         (tif_cols >= 0) & (tif_cols < src.width))
                data_all = src.read()
                if data_all.shape[0] != nbands:
                    raise ValueError(f"File {filename} has {data_all.shape[0]} bands, expected {nbands}")
                nodata_mask = np.any(data_all == INPUT_NODATA_VAL, axis=0)
                data_all[:, nodata_mask] = OUTPUT_NODATA_VAL
                out = np.full((nbands, total_masked_pixels), OUTPUT_NODATA_VAL, dtype=np.uint16)
                out[:, valid] = data_all[:, tif_rows[valid], tif_cols[valid]]
                dset_values[i, :, :] = out

# ---------------------------------------------
if __name__ == "__main__":
    main()
