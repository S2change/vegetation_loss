# Satellite Data HDF5 Pipeline

.
└── test_tif_to_hdf5/
    ├── satellite_data_6bands.h5           # Central HDF5 storage (B2, B3, B4, B8, B11, B12)
    ├── 2bands/                            # Original source files (B11, B12)
    │   ├── S2_2b_1491651247967.tif
    │   └── ... (matching 13-digit timestamps)
    ├── 4bands/                            # Original source files (B2, B3, B4, B8)
    │   ├── S2_4b_1491651247967.tif
    │   └── ... (matching 13-digit timestamps)
    └── reconstructed_tifs/                 # Output from reconstruction script
        ├── sentinel2_6bands_1491651247967.tif
        └── ... (unified 6-band GeoTIFFs)

Usage Guide
1. HDF5 Creation
Consolidates scattered .tif files into a single, high-performance HDF5 archive.

Script: tif_to_hdf5.py

Logic: Matches files from /4bands and /2bands using 13-digit timestamps.

Spatial Correction: Uses offset='ul' to ensure coordinates represent pixel corners, preventing spatial drift.

2. TIFF Reconstruction
Extracts data from the HDF5 to create unified 6-band GeoTIFFs.

Script: reconstruct_tifs.py

Features: * Sets NoData to 65535 in the file header.

Reorders bands to: B2, B3, B4, B8, B11, B12.

Options for LZW compression to save disk space.

3. QGIS Visualization & QA
Automated script for the QGIS Python Console to verify results.

Script: qgis_vis_comparison.py

Visualization: Automatically sets an RGB 8-4-3 (False Color Infrared) composite.

Stretch: Applies a 2% cumulative cut stretch while ignoring NoData values.

Organization: Groups layers into 6bands, 4bands, and 2bands for easy toggle-comparison.

Comparison Checklist
When verifying the reconstructed data against the originals in QGIS:

Alignment: Toggle the top layer; there should be zero "shifting" or "jumping" of pixels.

Values: Use the "Identify" tool to confirm pixel values are identical across versions.

NoData: Ensure the white/black borders are transparent (indicated by the 65535 metadata).