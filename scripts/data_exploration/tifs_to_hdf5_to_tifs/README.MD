# Sentinel-2 HDF5 Data Pipeline

This project consolidates Sentinel-2 spectral bands into HDF5 format and reconstructs them into unified GeoTIFFs.

## 📂 Project Structure

- **satellite_data_6bands.h5**: Central HDF5 storage.
- **2bands/**: Original SWIR source files (B11, B12).
- **4bands/**: Original VNIR source files (B2, B3, B4, B8).
- **reconstructed_tifs/**: Output unified 6-band GeoTIFFs.

```
└── tifs_to_hdf5_to_tifs/
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
```


---

## 🚀 Usage Guide

### 1. HDF5 Creation
Consolidates .tif files into one HDF5 archive. Matches files using a 13-digit timestamp. [tif_to_hdf5.py](tif_to_hdf5.py)
* **Spatial Fix:** Uses `offset='ul'` (Upper-Left) to prevent the half-pixel shift.

### 2. TIFF Reconstruction
Generates 6-band GeoTIFFs from the HDF5.
* **Band Order:** B2, B3, B4, B8, B11, B12.
* **NoData:** Sets value to **65535** in the file header for correct transparency.

### 3. QGIS Visualization
Python script for the QGIS Console to verify results.
* **Symbology:** Sets RGB 8-4-3 False Color.
* **Stretch:** Automatically ignores 65535 for clear rendering.

---

## 🛠 Technical Specifications

| Parameter | Value |
| --- | --- |
| Data Type | uint16 |
| NoData Value | 65535 |
| Pixel Alignment | Upper-Left Corner |

---

## 🔍 Quality Assurance
- **Alignment:** Reconstructed layers must overlay originals with zero drift.
- **Transparency:** NoData borders should be transparent.
- **Integrity:** Pixel values must match original source data.
