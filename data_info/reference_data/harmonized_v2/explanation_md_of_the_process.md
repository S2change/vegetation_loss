# Visual Chip Mask Generation Workflow

## 1. Purpose

This document describes the workflow used to generate interpreted masks from visual change-detection chips, hereafter referred to as **VChips**. The workflow was designed to transform manually interpreted before/after visual chips into a consistent set of geospatial outputs suitable for review, storage, and later use in classification or validation workflows.

The process starts from the pre-change and post-change image chips delivered by Professor Manuel Campagnolo. Each visual chip contains a pair of raster layers representing the same area before and after a reference date. The interpreter compares both images in QGIS and digitizes the areas where a change or non-change category is observed. The final output is a set of raster and vector masks aligned with the original chip geometry.

## 2. General logic of the workflow

The workflow follows four main stages:

1. **Maximum chip extent generation**: create a polygon representing the valid spatial footprint of the visible raster chips.
2. **Manual interpretation in QGIS**: use the generated footprint as an editable base layer and assign a change category to each interpreted polygon.
3. **Batch mask generation**: convert the interpreted polygons into standardized vector and raster masks, while also copying the original before/after raster chips.
4. **Review and reloading of interpreted chips**: load each generated mask together with its corresponding before/after rasters for visual checking and correction.

The scripts are intended to be executed inside QGIS because they depend on the currently loaded and visible QGIS layers.

## 3. Input data

### 3.1 Before/after image chips

The input image chips are raster layers loaded in QGIS. Each chip is expected to have two temporal phases:

- `before`: image before the observed or reference change date.
- `after`: image after the observed or reference change date.

The expected raster layer naming pattern is:

```text
<chip_identifier>_before_<YYYYMMDD>
<chip_identifier>_after_<YYYYMMDD>
```

Example:

```text
BDRexp_v1_832_05_05_30_before_20200522
BDRexp_v1_832_05_05_30_after_20200522
```

The date is kept in the chip key so that chips from different dates are not accidentally mixed. During export, the output name is rebuilt using the raster centroid and date:

```text
vchip_<center_x>_<center_y>_<YYYYMMDD>
```

This produces standardized outputs such as:

```text
vchip_525525_4405445_20220712_before.tif
vchip_525525_4405445_20220712_after.tif
vchip_525525_4405445_20220712_mask.tif
vchip_525525_4405445_20220712_mask.gpkg
```

### 3.2 Editing GeoPackage

The manual interpretation is stored in a GeoPackage layer named:

```text
Edicion
```

The required thematic field is:

```text
Chg_type
```

The uploaded `Edicion.gpkg` has the correct structure for this workflow: a polygon/multipolygon geometry layer with an `id` field and a `Chg_type` field. In the provided version, the layer is empty, so it should be understood as an editing template or working layer rather than a completed interpretation file.

## 4. Change classes

The field `Chg_type` stores the interpreted class for each polygon. Values must be written exactly as expected by the script, because the batch generation step converts these text labels into numeric mask values.

| `Chg_type` value | Numeric mask value | Meaning |
|---|---:|---|
| `nao_alteracao` | 0 | No change / stable area |
| `corte` | 1 | Cut / logging / removal |
| `outro` | 2 | Other type of change |
| `agricultura` | 3 | Agricultural change |
| `fogo` | 4 | Fire-related change |
| `nodata` | 255 | NoData / outside interpreted mask |

Unknown `Chg_type` values are skipped by the mask generation script when `SKIP_UNKNOWN_CHG_TYPE = True`. Therefore, class names should be checked carefully before running the batch process.

## 5. Step 1 — Maximum chip extent generation

**Script:** `vchips_polygon_process.py`

This script creates the spatial footprint used as the base for manual interpretation. It reads all visible raster layers in the current QGIS project, creates a binary valid-data mask for each raster, polygonizes the valid pixels, and dissolves them into a single union geometry.

The output is:

```text
visible_rasters_union.gpkg
visible_rasters_union
```

The resulting layer represents the maximum valid extent of the visible raster chips. It can be copied into the editing GeoPackage or used as a spatial reference for digitizing the interpreted mask polygons.

Operational logic:

1. Identify visible raster layers in QGIS.
2. Read band 1 of each raster.
3. Build a valid-data mask where valid pixels are assigned value `1` and NoData pixels are assigned value `0`.
4. Polygonize the valid-data mask.
5. Select polygons with value `1`.
6. Dissolve all valid polygons into a single multipolygon geometry.
7. Save the final footprint as `visible_rasters_union.gpkg`.

This step is important because it defines the spatial area over which the interpreter should digitize the final change/no-change polygons.

## 6. Step 2 — Manual interpretation in QGIS

After the maximum chip extent has been generated, the interpreter manually edits the `Edicion` layer in QGIS.

The typical interpretation procedure is:

1. Load the `before` and `after` raster chips.
2. Load or copy the generated footprint into the editable GeoPackage.
3. Compare the pre-change and post-change images visually.
4. Digitize or edit polygons representing the interpreted areas.
5. Assign one valid `Chg_type` value to each polygon.
6. Save edits before running the batch generation script.

The interpreter should only keep visible the raster chips that correspond to the current batch to avoid exporting unwanted rasters. This is important because the batch generation script processes visible raster layers from the QGIS project.

## 7. Step 3 — Batch mask generation

**Script:** `raster_mask_vchip_generation.py`

This is the main batch export step. It reads the interpreted `Edicion` layer and the visible before/after raster chips, then generates standardized outputs for each unique chip.

### 7.1 Output folder structure

The script writes outputs to:

```text
mask_outputs/
├── mask_polygons/
├── mask_rasters/
└── source_rasters/
```

Where:

- `mask_polygons/` stores the interpreted vector mask as a GeoPackage.
- `mask_rasters/` stores the rasterized mask as a single-band GeoTIFF.
- `source_rasters/` stores copied versions of the original before/after raster chips using the standardized `vchip_*` naming scheme.

### 7.2 Source raster export

For each visible raster, the script:

1. Parses the raster phase (`before` or `after`) and date.
2. Calculates the raster centroid.
3. Builds a standardized base name:

```text
vchip_<center_x>_<center_y>_<YYYYMMDD>
```

4. Copies the raster to `source_rasters/`.
5. Copies the associated `.aux.xml` file when available.
6. Exports QGIS visualization styles as `.qml` and `.sld` files when enabled.

The `.aux.xml`, `.qml`, and `.sld` files are auxiliary visualization and metadata files. They are not the primary raster data, but they help preserve how the layers were displayed in QGIS.

### 7.3 Vector mask export

For each unique chip, the script:

1. Uses the representative raster as the spatial reference.
2. Reprojects the `Edicion` geometries to the raster CRS if needed.
3. Validates and repairs geometries when necessary.
4. Converts `Chg_type` into `mask_val` using the class dictionary.
5. Clips the interpreted polygons to the raster extent.
6. Saves the clipped polygons to a GeoPackage.

The vector mask has the following standard fields:

| Field | Description |
|---|---|
| `src_fid` | Source feature ID from the editing layer |
| `chg_type` | Original interpreted class label |
| `mask_val` | Numeric value used for rasterization |

Expected output example:

```text
mask_polygons/vchip_525525_4405445_20220712_mask.gpkg
```

### 7.4 Raster mask export

The vector mask is rasterized so that it matches the corresponding raster chip exactly. The output mask:

- has the same raster size as the reference chip;
- uses the same geotransform and projection;
- is a single-band `Byte` GeoTIFF;
- uses LZW compression;
- burns the `mask_val` attribute into the raster;
- uses `255` as the NoData value.

Expected output example:

```text
mask_rasters/vchip_525525_4405445_20220712_mask.tif
```

## 8. Step 4 — Review and reloading of interpreted VChips

**Script:** `cod_re_revision_vchips.py`

This script supports visual review of previously generated VChip masks. It opens a QGIS dialog listing the available mask GeoPackages and allows the user to load each chip for checking.

The script searches for masks using the following naming pattern:

```text
vchip_<center_x>_<center_y>_<YYYYMMDD>_mask.gpkg
```

It distinguishes between:

- original masks stored in `mask_polygons/`;
- reviewed masks stored in `Revisado_10_bandas_pngs/mask_polygons/`.

If a reviewed version exists, it is loaded preferentially. Otherwise, the original mask is loaded.

For each selected chip, the script loads:

1. the `after` raster;
2. the `before` raster;
3. the mask polygon layer.

Because QGIS places the last loaded layer on top, this loading order places the mask polygon above the before/after rasters for visual checking.

The review script accepts both naming conventions for before/after rasters:

```text
vchip_<center_x>_<center_y>_<YYYYMMDD>_before.tif
vchip_<center_x>_<center_y>_<YYYYMMDD>_after.tif
```

and the older convention:

```text
vchip_<center_x>_<center_y>_<YYYYMMDD>_mask_before.tif
vchip_<center_x>_<center_y>_<YYYYMMDD>_mask_after.tif
```

The 10-band rasters are displayed with an RGB visualization based on bands 4, 8, and 9, and the mask polygon is displayed as an outline.

## 9. Recommended execution order

The recommended execution order is:

```text
1. vchips_polygon_process.py
   → Generate visible_rasters_union.gpkg from visible raster chips.

2. Manual QGIS editing
   → Copy/use the generated extent and edit the Edicion layer.
   → Assign a valid Chg_type to each polygon.

3. raster_mask_vchip_generation.py
   → Export source rasters, vector masks, raster masks, and style files.

4. cod_re_revision_vchips.py
   → Reload each chip with before/after rasters and mask polygons for review.
```

## 10. Final deliverables

The final delivery is provided as a compressed package named:

```text
Entregavel
```

This compressed delivery package contains the complete set of materials needed to understand, reproduce, and review the VChip mask-generation process. Its top-level structure is:

```text
Entregavel/
├── Cods/
├── gpkg/
├── explanation_md_of_the_process.md
└── v_chips_v2_10bands.rar
```

Where:

- `Cods/` contains the QGIS/Python scripts used in the workflow:
  - `vchips_polygon_process.py`
  - `raster_mask_vchip_generation.py`
  - `cod_re_revision_vchips.py`
- `gpkg/` contains the GeoPackage files associated with the interpretation and editing process, including the editing layer used to store the interpreted polygons and their `Chg_type` values.
- `explanation_md_of_the_process.md` is this Markdown document, included as the technical explanation of the workflow.
- `v_chips_v2_10bands.rar` contains the complete interpreted VChip dataset. This archive includes the 179 analyzed VChips and preserves the generated mask outputs and original before/after visual context.

Inside `v_chips_v2_10bands.rar`, the interpreted VChip outputs are organized as:

```text
mask_outputs/
├── mask_polygons/
│   └── vchip_<center_x>_<center_y>_<YYYYMMDD>_mask.gpkg
├── mask_rasters/
│   └── vchip_<center_x>_<center_y>_<YYYYMMDD>_mask.tif
└── source_rasters/
    ├── vchip_<center_x>_<center_y>_<YYYYMMDD>_before.tif
    ├── vchip_<center_x>_<center_y>_<YYYYMMDD>_after.tif
    ├── optional .aux.xml files
    ├── optional .qml files
    └── optional .sld files
```

The `v_chips_v2_10bands.rar` archive is the main data component of the delivery. It keeps the interpreted masks and the corresponding before/after raster chips together, avoiding the risk of separating the mask products from the visual evidence used during interpretation.

If a GeoPackage is being transferred while QGIS is still open, or before the database has been fully closed, any related sidecar files should also be preserved when present:

```text
Edicion.gpkg
Edicion.gpkg-shm
Edicion.gpkg-wal
```

These sidecar files are part of the SQLite/GeoPackage write-ahead logging mechanism and may contain edits that have not yet been fully checkpointed into the main `.gpkg` file.


## 11. Practical quality-control checks

Before considering a batch complete, the following checks are recommended:

- Confirm that each interpreted polygon has a valid `Chg_type` value.
- Confirm that the `before` and `after` rasters belong to the same chip and date.
- Confirm that only the intended rasters are visible in QGIS before running the batch generation script.
- Confirm that the vector mask and raster mask have the same spatial extent as the source chip.
- Confirm that the raster mask uses the expected numeric values: `0`, `1`, `2`, `3`, `4`, and `255` for NoData.
- Visually reload the outputs using the review script and check that the mask polygons align with the interpreted change areas.

## 12. Summary

This workflow converts visual before/after chip interpretation into standardized geospatial training or validation assets. The key idea is that the interpretation is performed manually in QGIS using the `Edicion` layer, while the scripts automate the repetitive and error-prone tasks of footprint generation, output naming, raster copying, vector mask export, rasterization, styling, and review loading.

The resulting package preserves both the interpretation mask and the original visual context used to create it: the before image, the after image, the vector mask, the raster mask, and the auxiliary visualization files.


