
## PyCCD Accuracy Assessment

### `validate_ccd_against_icnf.py` (Sara Caetano; E3.3 – Relatório de validação dos mapas nacionais: E3.3B)
Cross-references burned area polygons from the ICNF dataset with change detection results from the CCD algorithm (MBPV_v0).

Inputs:
- `ICNF_PATH`: path to the ICNF burned area shapefile
- `CCD_FOLDER`: directory containing the bimonthly national CCD maps with the detected polygons (.gpkg)
- `MASK_PATH`: path to the spatial mask (.gpkg) used to restrict analysis
- `WINDOW_DAYS`: temporal window (in days) for matching detections with ICNF polygons

Outputs:
- Prints to the console the summary of intersection and coverage metrics between ICNF and CCD detections
- Generates a DataFrame with the following metrics:
    - Total ICNF area
    - Total MBPV_v0 area
    - ICNF area inside/outside the DGT_loss_vegetation mask
    - Intersection area between ICNF (inside mask) and MBPV_v0
    - ICNF area (inside mask) not detected by MBPV_v0

### `raster_avaliacao_exatidao.py` (Dominic Welsh; E3.3 – Relatório de validação dos mapas nacionais: E3.3A)

Script that conducts accuracy assessment of change detection results from raster data.

Inputs:
- RASTER_FILE: path to raster file containing change detection dates in YYYYMMDD format
- REFERENCE_FILE: path to the shapefile/geopackage of the reference dataset used for validation (e.g. DBR_DGT_300)

Outputs:
- Creates CSV files with accuracy assessment results in a new folder
- Prints accuracy metrics (F1-score, omission and commission errors) to console
- Files saved in the same folder as RASTER_FILE, in new directory /{raster_name}_accuracy_assessment

### `avaliacao_exatidao_pyccd.py` (Daniel Moraes)

Conducts accuracy assessment of the pyccd results.

**Usage**

`python avaliacao_exatidao_pyccd.py`

Inputs:
- `FOLDER_PARQUET`: directory containing the parquet files (pyccd's results)
- `BDR_DGT`: path to the shp/gpkg of the reference dataset used for validation

Outputs:
- creates a `csv` file with the dataframe resulting from the accuracy assessment
    - file is saved in the `accuracy_assessment` folder inside `FOLDER_PARQUET`
- outputs accuracy metrics (F1-score, omission and commission errors) to the console
