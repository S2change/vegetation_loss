# Harmonized Reference Database

This README provides a short operational guide for using the harmonized reference datasets and a concise description of the columns in the complete aggregated output.

The harmonized datasets include **BDR**, **BDR Expanded**, **NVG**, and **ICNF**.

“Harmonized” means that the original datasets were converted into a common structure, including standardized field names, identifiers, temporal information, text values, spatial reference system, and other shared attributes. Detailed documentation for each dataset is available in the corresponding Markdown files in this folder.

---

## How to use

There are two operational options, depending on the intended output.

| Option | Purpose | Main tool/output |
|---|---|---|
| **Option 1 — Export selected data** | Select, filter, combine, and export information from the individual harmonized BDR, BDR Expanded, NVG, and ICNF datasets. | [`export_data_generation.py`](https://github.com/S2change/vegetation_loss/blob/main/scripts/ref_datasets/Export_generation/Codes/pipelines/export_data_generation.py) |
| **Option 2 — Create a complete aggregated GeoPackage** | Concatenate the harmonized source layers into one complete aggregated GeoPackage. | [`all_harmonized_merged.gpkg`](https://ulisboa-my.sharepoint.com/personal/mlc_office365_ulisboa_pt/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Fmlc%5Foffice365%5Fulisboa%5Fpt%2FDocuments%2FDocuments%2Finvestigacao%2Dprojectos%2Dreviews%2Dalunos%2Djuris%2Fprojetos%2FDGT%2DS2CHANGE%5F2023%2Fpartilhado%2Fref%5F%5Fdatasets%2Fharmonized%2FExport%5Fgeneration%2FResults%2FMerged%5Fharmonized%5Flayers&viewid=310b2a31%2Df22d%2D4b5f%2D8b7d%2De60e201167df&ga=1) |

Detailed documentation:

- [Export Generation workflow](https://github.com/S2change/vegetation_loss/blob/main/data_info/reference_data/Harmonized/Export_generation/Export_data_generation.md)
- [Export Generation data folder](https://ulisboa-my.sharepoint.com/personal/mlc_office365_ulisboa_pt/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Fmlc_office365_ulisboa_pt%2FDocuments%2FDocuments%2Finvestigacao-projectos-reviews-alunos-juris%2Fprojetos%2FDGT-S2CHANGE_2023%2Fpartilhado%2Fref__datasets%2Fharmonized%2FExport_generation%2FData&ga=1)

---

<details>
<summary><strong>Option 1 — Export selected harmonized data in QGIS</strong></summary>

This is the main operational option when the user wants to create a dataset for a specific year, month, source combination, or set of output attributes.

The export tool is:

[`scripts/ref_datasets/Export_generation/Codes/pipelines/export_data_generation.py`](https://github.com/S2change/vegetation_loss/blob/main/scripts/ref_datasets/Export_generation/Codes/pipelines/export_data_generation.py)

It is executed inside QGIS, using the harmonized layers stored in:

[`Export_generation/Data/`](https://ulisboa-my.sharepoint.com/personal/mlc_office365_ulisboa_pt/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Fmlc_office365_ulisboa_pt%2FDocuments%2FDocuments%2Finvestigacao-projectos-reviews-alunos-juris%2Fprojetos%2FDGT-S2CHANGE_2023%2Fpartilhado%2Fref__datasets%2Fharmonized%2FExport_generation%2FData&ga=1)

General steps:

1. Open the Export Generation QGIS project.
2. Check that the harmonized BDR, BDR Expanded, NVG, and ICNF layers are loaded.
3. Run [`export_data_generation.py`](https://github.com/S2change/vegetation_loss/blob/main/scripts/ref_datasets/Export_generation/Codes/pipelines/export_data_generation.py) in the QGIS Python environment.
4. Select one or more sources.
5. Select the required year and, optionally, month.
6. Select the output attributes.
7. Save the result as a GeoPackage layer.

Example output layers:

```text
harmonized_2023
harmonized_2024
harmonized_2025
```

The current interface supports:

- one selected year;
- all available years;
- an optional month filter.

If a period such as **2023–2025** is required, export each year separately and then concatenate the annual outputs if a single layer is needed.

</details>

---

<details>
<summary><strong>Option 2 — Concatenate all harmonized source layers</strong></summary>

This option creates a complete aggregated dataset by concatenating the harmonized source layers.

The merge tool is:

[`scripts/ref_datasets/Export_generation/Codes/pipelines/merge_harmonized_layers.py`](https://github.com/S2change/vegetation_loss/blob/main/scripts/ref_datasets/Export_generation/Codes/pipelines/merge_harmonized_layers.py)

It is executed through:

[`scripts/ref_datasets/Export_generation/Codes/runners/run_merge_harmonized_layers.py`](https://github.com/S2change/vegetation_loss/blob/main/scripts/ref_datasets/Export_generation/Codes/runners/run_merge_harmonized_layers.py)

The output is:

[`Export_generation/Results/Merged_harmonized_layers/all_harmonized_merged.gpkg`](https://ulisboa-my.sharepoint.com/personal/mlc_office365_ulisboa_pt/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Fmlc_office365_ulisboa_pt%2FDocuments%2FDocuments%2Finvestigacao-projectos-reviews-alunos-juris%2Fprojetos%2FDGT-S2CHANGE_2023%2Fpartilhado%2Fref__datasets%2Fharmonized%2FExport_generation%2FResults%2FMerged_harmonized_layers%2Fall_harmonized_merged.gpkg&ga=1)

This path assumes that the data are organized following the structure described in the [Export Generation workflow](https://github.com/S2change/vegetation_loss/blob/main/data_info/reference_data/Harmonized/Export_generation/Export_data_generation.md).

This operation is a feature and attribute concatenation. It is not a spatial overlay, intersection, or dissolve.

The resulting layer uses a maximal schema: shared fields are stored once, while source-specific fields are retained and contain `NULL` for records to which they do not apply.

</details>

---

<details>
<summary><strong>Example — Updating the workflow when ICNF 2025 becomes available</strong></summary>

If ICNF publishes a new 2025 burned-area dataset, the user should first harmonize the new ICNF data and then update the export inputs.

General steps:

1. Place the ICNF 2025 input layer in the ICNF [`Data/`](https://ulisboa-my.sharepoint.com/personal/mlc_office365_ulisboa_pt/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Fmlc_office365_ulisboa_pt%2FDocuments%2FDocuments%2Finvestigacao-projectos-reviews-alunos-juris%2Fprojetos%2FDGT-S2CHANGE_2023%2Fpartilhado%2Fref__datasets%2Fharmonized%2FICNF%2FData&ga=1) folder.
2. Run the ICNF topology-review stage.
3. Run the text-normalization stage.
4. Run the ICNF harmonization stage.
5. Add the resulting `ICNF_2025` layer to the updated harmonized ICNF GeoPackage.
6. Copy or update the ICNF GeoPackage in [`Export_generation/Data/`](https://ulisboa-my.sharepoint.com/personal/mlc_office365_ulisboa_pt/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Fmlc_office365_ulisboa_pt%2FDocuments%2FDocuments%2Finvestigacao-projectos-reviews-alunos-juris%2Fprojetos%2FDGT-S2CHANGE_2023%2Fpartilhado%2Fref__datasets%2Fharmonized%2FExport_generation%2FData&ga=1).

Then, use Option 1 or Option 2 depending on the required output.

If ICNF is the only source with data for 2025, the 2025 output will contain ICNF records only.

Detailed source workflows:

- [BDR processing workflow](https://github.com/S2change/vegetation_loss/blob/main/data_info/reference_data/Harmonized/BDR_and_BDR_expanded/BDR_300.md)
- [BDR Expanded processing workflow](https://github.com/S2change/vegetation_loss/blob/main/data_info/reference_data/Harmonized/BDR_and_BDR_expanded/BDR_expanded.md)
- [NVG processing workflow](https://github.com/S2change/vegetation_loss/blob/main/data_info/reference_data/Harmonized/NVG/NVG_Processing_Workflow.md)
- [ICNF processing workflow](https://github.com/S2change/vegetation_loss/blob/main/data_info/reference_data/Harmonized/ICNF/ICNF.md)

</details>

---

## Complete aggregated output

The complete aggregated product is:

[`Export_generation/Results/Merged_harmonized_layers/all_harmonized_merged.gpkg`](https://ulisboa-my.sharepoint.com/personal/mlc_office365_ulisboa_pt/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Fmlc%5Foffice365%5Fulisboa%5Fpt%2FDocuments%2FDocuments%2Finvestigacao%2Dprojectos%2Dreviews%2Dalunos%2Djuris%2Fprojetos%2FDGT%2DS2CHANGE%5F2023%2Fpartilhado%2Fref%5F%5Fdatasets%2Fharmonized%2FExport%5Fgeneration%2FResults%2FMerged%5Fharmonized%5Flayers&viewid=310b2a31%2Df22d%2D4b5f%2D8b7d%2De60e201167df&ga=1)

It combines the harmonized outputs from **BDR**, **BDR Expanded**, **NVG**, and **ICNF**.

The aggregated layer uses a maximal schema. Therefore, `NULL` values are expected when a field does not apply to a given source.

---

## Shared output columns

These are the main shared columns expected in the complete aggregated output and in most exported layers.

| Field | Description |
|---|---|
| `Src` | Harmonized source or source subset represented by the feature. |
| `Id` | Internal identifier inherited or generated by the source-specific workflow. |
| `Uid` | Feature identifier generated by the source-specific workflow. Use with `Src` or `source_layer` for cross-source uniqueness checks. |
| `Data0` | Initial or pre-event date. The exact meaning depends on the source. |
| `Data1` | Final or post-event date. The exact meaning depends on the source. |
| `Temp_eval_start` | Beginning of the temporal evaluation window. |
| `Temp_eval_end` | End of the temporal evaluation window. |
| `Chg_type` | Harmonized change or disturbance type. |
| `Area_ha` | Feature area in hectares. |
| `Validation_flag` | Source-specific quality or validation result. |
| `Pi_dicofre` | Administrative DICOFRE code. |
| `S2_tile` | Sentinel-2 tile assigned under the source-specific rule. It may be `NULL` when no unique assignment is available. |
| `source_layer` | Input layer used by [`merge_harmonized_layers.py`](https://github.com/S2change/vegetation_loss/blob/main/scripts/ref_datasets/Export_generation/Codes/pipelines/merge_harmonized_layers.py). This field is not automatically created by the QGIS export tool. |

---

## Meaning of `Src`

`Src` is the main field used to distinguish the origin of each feature.

| `Src` value | Meaning |
|---|---|
| `bdr` | Harmonized records from the original BDR dataset. |
| `bdr_expanded` | Harmonized records added through the BDR Expanded workflow. |
| `icnf` | Harmonized official ICNF burned-area polygons. |
| `nvg_s2` | NVG operational polygons associated with a harmonized Sentinel-2/CCDC change result. |
| `nvg_propios` | NVG operational polygons retained without a matched Sentinel-2/CCDC result. Their temporal and CCDC-specific fields may therefore be `NULL`. |

In simple terms:

```text
nvg_s2      = NVG_proprios geometry with matched Sentinel-2/CCDC temporal and validation information
nvg_propios = NVG_proprios geometry retained without a matched Sentinel-2/CCDC result
```

In the NVG harmonization workflow, the Sentinel-2/CCDC-derived change information is transferred to the spatial units of `NVG_proprios`. This is done because `NVG_proprios` provides more spatially coherent operational geometries than the pixel-level Sentinel-2/CCDC outputs.

Therefore, `nvg_s2` does not represent the original Sentinel-2 pixel geometry. It represents `NVG_proprios` geometries enriched with matched Sentinel-2/CCDC temporal and validation attributes.

The source attributes of `nvg_propios` come from:

[`NVG/Data/NVG_proprios_2015_2023_clean.gpkg`](https://ulisboa-my.sharepoint.com/personal/mlc_office365_ulisboa_pt/_layouts/15/onedrive.aspx?id=%2Fpersonal%2Fmlc%5Foffice365%5Fulisboa%5Fpt%2FDocuments%2FDocuments%2Finvestigacao%2Dprojectos%2Dreviews%2Dalunos%2Djuris%2Fprojetos%2FDGT%2DS2CHANGE%5F2023%2Fpartilhado%2Fref%5F%5Fdatasets%2Fharmonized%2FNVG%2FData&viewid=310b2a31%2Df22d%2D4b5f%2D8b7d%2De60e201167df&ga=1)

---

<details>
<summary><strong>Source-specific notes</strong></summary>

### BDR and BDR Expanded

Common source-specific fields include:

| Field | Description |
|---|---|
| `Classe_0` | Land-cover or thematic class associated with the initial state. |
| `Classe_1` | Land-cover or thematic class associated with the final state. |
| `Buffer_id` | Identifier associated with the BDR or BDR Expanded buffer/source unit. |

### ICNF

For ICNF:

| Field | ICNF interpretation |
|---|---|
| `Data0` | Start date of the mapped fire event, derived from `DH_Inicio`. |
| `Data1` | End date of the mapped fire event, derived from `DH_Fim`. |
| `Temp_eval_start` | First day of the annual ICNF evaluation period: `YYYY-01-01`. |
| `Temp_eval_end` | Last day of the annual ICNF evaluation period: `YYYY-12-31`. |
| `Chg_type` | Fixed value: `fogo`. |

### NVG

For NVG:

| Field | NVG interpretation |
|---|---|
| `Data0` | Standardized representation of `Data0_p10`. Normally populated for `nvg_s2`; may be `NULL` for `nvg_propios`. |
| `Data1` | Standardized representation of `Data1_p90`. Normally populated for `nvg_s2`; may be `NULL` for `nvg_propios`. |
| `Validation_flag` | CCDC quality classification at `Id` level, such as `ccdc ok` or `ccdc no ok`. |
| `Id_gleba` | Operational NVG management-unit identifier, used especially for unmatched `nvg_propios` polygons. |

In merged and exported outputs, `Data0_p10` and `Data1_p90` are standardized as `Data0` and `Data1`.

</details>

---

## Additional notes

- Use `Src` to distinguish datasets and NVG subsets.
- Use `source_layer` for technical provenance in the complete merged product.
- Use the shared harmonized fields for cross-source analyses.
- Use the source-specific documentation for detailed interpretation of fields unique to BDR, BDR Expanded, NVG, or ICNF.
