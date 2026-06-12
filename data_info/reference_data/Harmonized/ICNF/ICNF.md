# ICNF – Detailed Processing Workflow

This document describes the complete processing, topology review, text normalization, and harmonization workflow applied to the **ICNF burned-area datasets** used as spatial reference data in the harmonization framework.

The workflow processes the annual ICNF burned-area layers from 2020 to 2024, documents within-year polygon overlaps, standardizes text attributes, creates the common harmonized fields, assigns administrative and Sentinel-2 tile information, and writes the final annual layers into a single GeoPackage.

---

## 1. Scope and purpose

The ICNF workflow prepares the official burned-area perimeters produced by ICNF for use as spatial reference data. Its main objectives are to:

- process the annual ICNF burned-area layers using a consistent workflow;
- identify and document polygon overlaps within each annual layer;
- support manual review of flagged overlaps;
- standardize text values;
- derive the common harmonized fields used by BDR, BDR Expanded, NVG, and ICNF;
- assign administrative codes and Sentinel-2 tile information;
- preserve the annual temporal context of each layer;
- generate a final multi-layer GeoPackage ready for integration and comparative analysis.

In this context, harmonization does not reinterpret fire events or manually redefine the official burned-area boundaries. It standardizes the spatial and attribute structure required to use ICNF together with the other reference datasets.

---

## 2. Code and project organization

The ICNF processing code is organized into:

- `Codes/core/`: shared spatial-processing functions, including topology and reprojection operations;
- `Codes/pipelines/`: the main ICNF harmonization workflow;
- `Codes/runners/`: executable scripts used to run each processing stage;
- `Codes/utils/`: supporting utilities, including text normalization.

### 2.1 Repository organization

The ICNF workflow and the suggested folder organization is next:

```text
ICNF/
├── Codes (https://github.com/S2change/vegetation_loss/tree/main/scripts/ref_datasets/ICNF/Codes)/
│   ├── core/
│   │   ├── reproject_layer.py
│   │   └── topology_icnf.py
│   ├── pipelines/
│   │   └── process_layer_ICNF.py
│   ├── runners/
│   │   ├── run_topology_icnf.py
│   │   ├── run_normalize_string.py
│   │   └── run_process_layer_ICNF.py
│   └── utils/
│       └── normalize_string.py
├── Data(DGT-S2CHANGE_2023/partihaldo/ref__datasets/harmonized/ICNF/Data)/
│   ├── ardida_2020.*
│   ├── ardida_2021.*
│   ├── ardida_2022.*
│   ├── ardida_2023.*
│   ├── ardida_2024.*
│   ├── NUTS/
│   │   └── areas_administrativas.shp
│   └── S2_tiles/
│       └── sentinel2_tiles_PT_terra_tm06.shp
├── Results(DGT-S2CHANGE_2023/partihaldo/ref__datasets/harmonized/ICNF/Results)/
│   ├── Topologia_revisado/
│   ├── Normalized_text_columns/
│   └── Harmonizacion_datos/
└── Docs/
    └── ICNF.md
```

The principal project locations are:

- processing code: `scripts/ref_datasets/ICNF/Codes/`;
- source and auxiliary data: `DGT-S2CHANGE_2023/partihaldo/ref__datasets/harmonized/ICNF/Data/`;
- processing results: `DGT-S2CHANGE_2023/partihaldo/ref__datasets/harmonized/ICNF/Results/`;

---

## 3. Processing order

The ICNF workflow must be executed in the following order.

### Stage 1 — Within-year overlap detection

**Runner**

`Codes/runners/run_topology_icnf.py`

**Main inputs**

- `Data/ardida_2020.shp`
- `Data/ardida_2021.shp`
- `Data/ardida_2022.shp`
- `Data/ardida_2023.shp`
- `Data/ardida_2024.shp`

**Main outputs**

- `Results/Topologia_revisado/ardida_2020_overlap.shp`
- `Results/Topologia_revisado/ardida_2021_overlap.shp`
- `Results/Topologia_revisado/ardida_2022_overlap.shp`
- `Results/Topologia_revisado/ardida_2023_overlap.shp`
- `Results/Topologia_revisado/ardida_2024_overlap.shp`

Each annual layer is processed independently. The topology module:

1. reads the annual polygon layer;
2. applies `buffer(0)` to repair invalid polygon geometries in the processing output;
3. creates an internal feature identifier;
4. overlays the layer with itself;
5. retains intersections between different polygons;
6. calculates the overlap area;
7. flags overlaps larger than 1 m²;
8. writes the overlap flag to the annual output layer.

The topology field is written as `Tplgy_error` in the Python workflow and is stored as `Tplgy_erro` in the Shapefile because of the Shapefile field-name length limit.

A flagged overlap is not automatically interpreted as a digitizing error. ICNF polygons may overlap because different fire events occurred in the same location. Therefore, the flagged outputs are reviewed manually to distinguish valid event overlaps from apparent topology artefacts before continuing with the next processing stage.

### Stage 2 — Text normalization

**Runner**

`Codes/runners/run_normalize_string.py`

**Main inputs**

- `Results/Topologia_revisado/ardida_2020_overlap.shp`
- `Results/Topologia_revisado/ardida_2021_overlap.shp`
- `Results/Topologia_revisado/ardida_2022_overlap.shp`
- `Results/Topologia_revisado/ardida_2023_overlap.shp`
- `Results/Topologia_revisado/ardida_2024_overlap.shp`

**Main outputs**

- `Results/Normalized_text_columns/ardida_2020_overlap_textnorm.shp`
- `Results/Normalized_text_columns/ardida_2021_overlap_textnorm.shp`
- `Results/Normalized_text_columns/ardida_2022_overlap_textnorm.shp`
- `Results/Normalized_text_columns/ardida_2023_overlap_textnorm.shp`
- `Results/Normalized_text_columns/ardida_2024_overlap_textnorm.shp`

This stage normalizes the values of all text fields by:

- trimming leading and trailing whitespace;
- converting text to lowercase;
- removing accents and diacritics.

The normalization is applied to text values, not to column names.

### Stage 3 — Annual harmonization and final GeoPackage

**Runner**

`Codes/runners/run_process_layer_ICNF.py`

**Main inputs**

- `Results/Normalized_text_columns/ardida_2020_overlap_textnorm.shp`
- `Results/Normalized_text_columns/ardida_2021_overlap_textnorm.shp`
- `Results/Normalized_text_columns/ardida_2022_overlap_textnorm.shp`
- `Results/Normalized_text_columns/ardida_2023_overlap_textnorm.shp`
- `Results/Normalized_text_columns/ardida_2024_overlap_textnorm.shp`
- `Data/NUTS/areas_administrativas.shp`
- `Data/S2_tiles/sentinel2_tiles_PT_terra_tm06.shp`

**Main vector output**

`Results/Harmonizacion_datos/ICNF_2020_2024_harmonized.gpkg`

The GeoPackage contains one harmonized layer per year:

- `ICNF_2020`
- `ICNF_2021`
- `ICNF_2022`
- `ICNF_2023`
- `ICNF_2024`

**Harmonization reports**

- `Results/Harmonizacion_datos/icnf_2020_harmonization_report.xlsx`
- `Results/Harmonizacion_datos/icnf_2021_harmonization_report.xlsx`
- `Results/Harmonizacion_datos/icnf_2022_harmonization_report.xlsx`
- `Results/Harmonizacion_datos/icnf_2023_harmonization_report.xlsx`
- `Results/Harmonizacion_datos/icnf_2024_harmonization_report.xlsx`

The harmonization pipeline processes each year separately and writes the result into the corresponding annual layer of the same GeoPackage.

---

## 4. Input datasets

### 4.1 ICNF annual burned-area layers

The workflow starts from five annual ICNF burned-area polygon layers:

- `Data/ardida_2020.shp`
- `Data/ardida_2021.shp`
- `Data/ardida_2022.shp`
- `Data/ardida_2023.shp`
- `Data/ardida_2024.shp`

Each layer represents the burned perimeters mapped for a specific year and includes event attributes such as:

- `DH_Inicio`;
- `DH_Fim`;
- `PI_DICOFRE`;
- other original ICNF event and burned-area attributes.

Each annual input is processed independently throughout the topology, normalization, and harmonization stages.

### 4.2 Administrative reference layer

Administrative information is obtained from:

`Data/NUTS/areas_administrativas.shp`

The field `dtmnfr` is used to fill missing `PI_DICOFRE` values through a centroid-based spatial join.

### 4.3 Sentinel-2 tile layer

Sentinel-2 tile information is obtained from:

`Data/S2_tiles/sentinel2_tiles_PT_terra_tm06.shp`

The tile layer is used to derive the harmonized field `S2_tile`.

---

## 5. Topology processing and manual review

The topology stage is implemented in:

- `Codes/core/topology_icnf.py`
- `Codes/runners/run_topology_icnf.py`

The analysis is performed independently for each year. It detects polygon intersections between different features of the same annual layer and applies a minimum overlap-area threshold of 1 m².

The output field is:

- `Tplgy_erro`

Values:

- `True` or `1`: the polygon overlaps at least one other polygon by more than the configured threshold;
- `False` or `0`: no overlap larger than the threshold was detected.

The flag identifies polygons requiring review; it does not by itself distinguish between:

- a legitimate overlap between different fire events;
- a narrow boundary overlap;
- a sliver polygon;
- an apparent digitizing artefact.

For that reason, the topology outputs are manually reviewed before being used in the normalization and harmonization stages.

The original files stored in `Data/` remain unchanged. The topology outputs written to `Results/Topologia_revisado/` contain the processing geometry after the `buffer(0)` repair operation.

---

## 6. Attribute normalization

Text normalization is implemented in:

- `Codes/utils/normalize_string.py`
- `Codes/runners/run_normalize_string.py`

All text columns in the reviewed topology outputs are standardized by converting their values to lowercase, removing accents and diacritics, and trimming surrounding whitespace.

The normalized annual layers are written to:

`Results/Normalized_text_columns/`

These files are the direct inputs to the final harmonization stage.

---

## 7. Harmonization to the common reference schema

The ICNF harmonization is implemented in:

`Codes/pipelines/process_layer_ICNF.py`

The current runner calls the pipeline with:

```python
keep_only_harmonized=True
```

Therefore, the final annual layers retain only the harmonized fields and geometry.

For each annual layer, the pipeline:

1. validates that `DH_Inicio` and `DH_Fim` exist;
2. creates `Data0` and `Data1`;
3. verifies that the layer represents a single evaluation year;
4. creates the annual temporal evaluation window;
5. creates the common identifiers and change attributes;
6. derives `Validation_flag` from `Tplgy_erro`;
7. preserves or completes `Pi_dicofre`;
8. assigns `S2_tile` only when the polygon is fully contained in exactly one tile;
9. retains only the harmonized fields;
10. normalizes text values;
11. reprojects the final layer to `EPSG:3763`;
12. writes the annual layer to `ICNF_2020_2024_harmonized.gpkg`;
13. writes an annual field-change report.

---

## 8. Final outputs

### 8.1 Harmonized GeoPackage

The principal vector output is:

`Results/Harmonizacion_datos/ICNF_2020_2024_harmonized.gpkg`

It contains the five annual harmonized layers:

```text
ICNF_2020
ICNF_2021
ICNF_2022
ICNF_2023
ICNF_2024
```

Each layer is processed independently and retains its own annual temporal evaluation window.

### 8.2 Harmonization reports

The workflow creates one Excel report per annual layer:

```text
Results/Harmonizacion_datos/icnf_2020_harmonization_report.xlsx
Results/Harmonizacion_datos/icnf_2021_harmonization_report.xlsx
Results/Harmonizacion_datos/icnf_2022_harmonization_report.xlsx
Results/Harmonizacion_datos/icnf_2023_harmonization_report.xlsx
Results/Harmonizacion_datos/icnf_2024_harmonization_report.xlsx
```

Each report documents whether the original fields were retained, renamed, dropped, or whether a harmonized field was added.

---

## 9. Description of final output attributes

When `keep_only_harmonized=True`, each annual ICNF layer contains the following fields plus geometry.

### **Src**

- **Type:** string
- **Fixed value:** `icnf`
- **Meaning:** source identifier for the ICNF reference dataset.

### **Id**

- **Type:** integer
- **Logic:** sequential values from `1` to `N` within each annual layer.
- **Meaning:** internal feature identifier for the processed annual layer.

### **Uid**

- **Type:** string
- **Format:** `icnf_XXXXXXX`
- **Logic:** generated from the zero-based feature index, padded to seven digits.
- **Meaning:** unique polygon identifier within each annual layer.

Because each annual layer is processed independently, the `Uid` sequence restarts for every year.

### **Data0**

- **Type:** date string in `YYYY-MM-DD` format or NULL
- **Derived from:** `DH_Inicio`
- **Meaning:** start date of the mapped fire-event interval.

### **Data1**

- **Type:** date string in `YYYY-MM-DD` format or NULL
- **Derived from:** `DH_Fim`
- **Meaning:** end date of the mapped fire-event interval.

### **Temp_eval_start**

- **Type:** date string in `YYYY-MM-DD` format
- **Logic:** `YYYY-01-01`, where `YYYY` is the single year inferred from `DH_Inicio` or, when required, `DH_Fim`.
- **Meaning:** beginning of the annual evaluation period.

### **Temp_eval_end**

- **Type:** date string in `YYYY-MM-DD` format
- **Logic:** `YYYY-12-31`.
- **Meaning:** end of the annual evaluation period.

The pipeline stops if it cannot infer a year or if more than one evaluation year is detected in the same annual input layer.

### **Chg_type**

- **Type:** string
- **Fixed value:** `fogo`
- **Meaning:** disturbance type assigned to the ICNF burned-area polygons.

### **Area_ha**

- **Type:** float
- **Logic:** `geometry.area / 10_000`
- **Meaning:** polygon area expressed in hectares.

The area calculation assumes that the input layer uses a projected CRS with metric units.

### **Validation_flag**

- **Type:** string
- **Derived from:** `Tplgy_erro`
- **Mapping:**
  - `0` or `False` → `no topology error`
  - `1` or `True` → `topology error`
  - missing or unrecognized value → `unknown`
- **Meaning:** harmonized quality flag derived from the reviewed topology field.

The source field `Tplgy_erro` is not retained in the final layer when `keep_only_harmonized=True`.

### **Pi_dicofre**

- **Type:** string or administrative-code value
- **Primary source:** existing `PI_DICOFRE`
- **Completion rule:** missing or empty values are filled using the polygon centroid and a `within` spatial join to the `dtmnfr` field in `Data/NUTS/areas_administrativas.shp`.
- **Meaning:** administrative unit code used for aggregation and reporting.

If the centroid does not fall within an administrative polygon, the value remains NULL.

### **S2_tile**

- **Type:** string or NULL
- **Source:** `Data/S2_tiles/sentinel2_tiles_PT_terra_tm06.shp`
- **Logic:** assigned only when the complete ICNF polygon is spatially `within` exactly one Sentinel-2 tile.
- **Meaning:** Sentinel-2 tile associated with the complete burned-area polygon.

The value remains NULL when:

- the polygon is not fully contained in any tile;
- the polygon crosses a tile boundary;
- the polygon is fully contained in more than one overlapping tile footprint.

Tile values are standardized to lowercase.

### **geometry**

- **Type:** polygon or multipolygon geometry
- **Final CRS:** `EPSG:3763`
- **Meaning:** geometry of the harmonized ICNF burned-area feature.

---

## 10. Optional preservation of original attributes

The harmonization function supports:

```python
keep_only_harmonized=False
```

When this option is used, the output retains the original ICNF attributes in addition to the harmonized fields.

However, the current operational runner uses:

```python
keep_only_harmonized=True
```

Therefore, the standard final output contains only the harmonized schema and geometry.

---

## 11. Status

The ICNF workflow is complete and organized into three sequential processing stages:

1. within-year overlap detection and review;
2. text normalization;
3. annual harmonization and export to a single multi-layer GeoPackage.

The final ICNF layers for 2020–2024 are ready for integration and comparative analysis with BDR, BDR Expanded, and NVG.
