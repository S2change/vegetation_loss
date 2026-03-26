import os
import sys
import numpy as np
import h5py
import rasterio
from rasterio.features import rasterize
import rasterio.transform
from datetime import datetime
from shapely.geometry import box
import geopandas as gpd

from hdf5_utils import parse_and_sort_files, read_all_bounds, INPUT_NODATA_VAL, OUTPUT_NODATA_VAL, BAND_NAMES, TILE_NAMES

'''
Creates a new HDF5 file from 10-band GeoTIFF files in a specified folder.

Filters out TIFs with no overlap with a vector mask, rasterizes the mask to identify
valid pixels, and writes the sparse pixel time series to an HDF5 file. Only pixels
inside the vector mask are stored.

Inputs:
- 'folder_tifs': Directory containing the 10-band GeoTIFF files.
- 'vector_mask_path': Path to vector file (shapefile, GeoJSON, etc.) defining the region of interest.
- 'h5_folder': Path for the folder where the output HDF5 files will be saved.
- 'MIN_DATE' and 'MAX_DATE': Optional date filters to only include TIFs within a certain date range, based on the timestamp in the filename.
- `BAND_NAMES`: List of Sentinel-2 band names that will used as column names in the outputted HDF5. Order should be the same order that the bands appear in the GeoTIFF files

The output HDF5 file (per tile) contains:
- values: (time, bands, pixels) - sparse pixel array for masked pixels only
- xs, ys: (pixels,) - coordinate arrays for masked pixels
- ts: (time,) - ordinal dates
- original_timestamps: (time,) - unix timestamps in milliseconds
- band_names attribute: band names in order

Note: TIFs with no overlap with the mask bounding box are discarded. TIFs that partially
overlap are kept — the boolean pixel mask determines which pixels are written to the HDF5.
'''

root_folder = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5"
folder_tifs = os.path.join(root_folder, "input_tifs")
vector_mask_path = os.path.join(root_folder, "vector_mask", "mask_continental_portugal_3763.gpkg")
h5_folder = os.path.join(root_folder, "hdf5")

MIN_DATE = None # or datetime(2017, 1, 1) # set a minimum date to filter out files with earlier timestamps; if None, all files are included regardless of date
MAX_DATE = None # or datetime(2030, 1, 1) # set a maximum date to filter out files with later timestamps; if None, all files are included regardless of date


def main():
    """
    Main Steps:
    - Parses and sorts input directory of GeoTIFFs just for the specified tile, extracting timestamps from filenames
    - Identifies largest tif, uses that as a reference tif
    - Clip input vector_mask_path by reference tif
    - Filters out tifs that have no overlap with reference tif
    - Rasterize mask to get bool pixels and total pixel count
    - Iterate through input GeoTIFFs, saving pixels that overlap rasterized mask to the HDF5
    - If any of the bands for a pixel have NODATA, then all bands are set to NODATA
    """
    for tile in TILE_NAMES:
        print(f"Processing tile {tile}...")
        h5_filename      = os.path.join(h5_folder, f'{tile}.h5')

        file_metadata = parse_and_sort_files(folder_tifs, tile, MIN_DATE, MAX_DATE)
        sorted_files = [m['path'] for m in file_metadata] # we need the full path to open the files, so we stored it in file_metadata for convenience

        if not sorted_files:
            print(f"No files found for tile {tile} in the specified date range. Skipping.")
            continue

        # all_bounds is a dict mapping file basenames to their bounding boxes (left, bottom, right, top)
        all_bounds = read_all_bounds(sorted_files)

        # largest_file is the file path of the largest tif, and we also get the reference CRS, transform, and metadata from that file
        largest_file, ref_crs, ref_transform, ref_meta = get_reference_tif(sorted_files, all_bounds)

        # Clip vector mask to reference tif extent to speed up later steps and avoid edge cases with files that only partially overlap the mask
        clipped_mask = clip_vector_mask(vector_mask_path, all_bounds[os.path.basename(largest_file)], ref_crs)

        # Filter out files with no overlap with the mask bounding box, but keep those that partially overlap (the boolean pixel mask will take care of which pixels to keep)
        # Since all_bounds keys are basenames, aligned_files will also be basenames, so we will need to match them with the full paths in file_metadata when we write the HDF5
        aligned_files = filter_by_mask_overlap(all_bounds, clipped_mask)
        file_metadata = [m for m in file_metadata if m['filename'] in set(aligned_files)]
        aligned_paths = [m['path'] for m in file_metadata if m['filename'] in set(aligned_files)]

        # Update ref_meta to have count=1 since we will write one band at a time to the HDF5
        ref_meta.update({"count": 1})
        total_masked_pixels, xs_flat, ys_flat = rasterize_mask(clipped_mask, ref_meta, ref_transform)

        # aligned_paths is a list of file paths that have some overlap with the mask, and file_metadata is filtered to only include metadata for those files, so we can pass both to write_hdf5 to read the files and write the HDF5
        write_hdf5(h5_filename, aligned_paths, file_metadata, BAND_NAMES,
                total_masked_pixels, xs_flat, ys_flat, ref_transform, ref_crs)

        print(f"Done! Created {h5_filename} with {total_masked_pixels} masked pixels and {len(aligned_files)} timesteps.")
    

def get_reference_tif(filepaths, all_bounds):
    """
    Returns the path of the largest TIF by bounding box area and return its metadata.

    Inputs:
    - filenames: list of file path to TIFs
    - all_bounds: dict mapping file basenames to their bounding boxes (left, bottom, right, top)

    Outputs:
    - largest_file: file path of the largest TIF
    - ref_crs: CRS of the largest TIF
    - ref_transform: Affine transform of the largest TIF
    - ref_meta: Metadata dict of the largest TIF 
    """
    print("Finding largest TIF as spatial reference...")
    largest_file = max(filepaths, key=lambda f: (
        (all_bounds[os.path.basename(f)].right - all_bounds[os.path.basename(f)].left) * (all_bounds[os.path.basename(f)].top - all_bounds[os.path.basename(f)].bottom)
    ))
    with rasterio.open(largest_file) as ref_src:
        ref_crs = ref_src.crs
        ref_transform = ref_src.transform
        ref_meta = ref_src.meta.copy()
    print(f"  Reference TIF: {largest_file}")
    print(f"  Bounds: {all_bounds[os.path.basename(largest_file)]}")
    return largest_file, ref_crs, ref_transform, ref_meta

def clip_vector_mask(vector_mask_path, ref_bounds, ref_crs):
    """Load vector mask, reproject if needed, and clip to reference TIF extent."""
    print(f"Loading vector mask: {vector_mask_path}")
    try:
        vector_mask = gpd.read_file(vector_mask_path)
    except Exception as e:
        print(f"Error loading vector mask: {e}")
        sys.exit(1)

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


def write_hdf5(h5_filename, sorted_files, file_metadata, 
               band_names, total_masked_pixels, xs_flat, ys_flat, ref_transform, ref_crs):
    """Write sparse pixel time series to HDF5."""
    nbands = len(band_names)
    with h5py.File(h5_filename, 'w') as h5f:
        dset_values = h5f.create_dataset(
            "values",
            shape=(len(sorted_files), nbands, total_masked_pixels),
            dtype='uint16',
            maxshape=(None, nbands, total_masked_pixels),  # None = Additional time steps can be appended in future
            chunks=(1, nbands, min(1000000, total_masked_pixels)),
            compression="lzf"
        )

        h5f.attrs['band_names'] = [n.encode('ascii') for n in band_names]
        h5f.attrs['crs'] = ref_crs.to_wkt()
        h5f.attrs['bounds_left']   = float(xs_flat.min())
        h5f.attrs['bounds_right']  = float(xs_flat.max()) + ref_transform.a
        h5f.attrs['bounds_bottom'] = float(ys_flat.min()) + ref_transform.e  # e is negative
        h5f.attrs['bounds_top']    = float(ys_flat.max())
        h5f.create_dataset("xs", data=xs_flat, dtype='int32')
        h5f.create_dataset("ys", data=ys_flat, dtype='int32')
        h5f.create_dataset("ts", data=[m['ordinal'] for m in file_metadata], dtype='int32', maxshape=(None,))
        h5f.create_dataset("original_timestamps",
                           data=[m['timestamp_ms'] for m in file_metadata],
                           dtype='int64',
                           maxshape=(None,))

        for i, filename in enumerate(sorted_files):
            print(f"Processing {i+1}/{len(sorted_files)}: {filename}")

            with rasterio.open(filename) as src:
                tif_rows, tif_cols = rasterio.transform.rowcol(src.transform, xs_flat, ys_flat)
                tif_rows = np.array(tif_rows)
                tif_cols = np.array(tif_cols)

                valid = ((tif_rows >= 0) & (tif_rows < src.height) &
                         (tif_cols >= 0) & (tif_cols < src.width))

                data_all = src.read()  # (10, H, W)
                # if shape does not math number of bands from BAND_NAMES, raise an error
                if data_all.shape[0] != nbands:
                    raise ValueError(f"File {filename} has {data_all.shape[0]} bands, but expected {nbands} based on BAND_NAMES.")  

                # Create a 2D mask (H, W) where True means at least one band is NoData
                nodata_mask = np.any(data_all == INPUT_NODATA_VAL, axis=0)

                # Apply mask to all 10 bands at once: if one is NoData, all become NoData
                data_all[:, nodata_mask] = OUTPUT_NODATA_VAL

                out = np.full((nbands, total_masked_pixels), OUTPUT_NODATA_VAL, dtype=np.uint16)
                out[:, valid] = data_all[:, tif_rows[valid], tif_cols[valid]]
                dset_values[i, :, :] = out


if __name__ == "__main__":
    main()
