
## PyCCD Accuracy Assessment

### `raster_avaliacao_exatidao.py`

Script that conducts accuracy assessment of change detection results from raster data.

Inputs:
- RASTER_FILE: path to raster file containing change detection dates in YYYYMMDD format
- REFERENCE_FILE: path to the shapefile/geopackage of the reference dataset used for validation (e.g. DBR_DGT_300)

Outputs:
- Creates CSV files with accuracy assessment results in a new folder
- Prints accuracy metrics (F1-score, omission and commission errors) to console
- Files saved in the same folder as RASTER_FILE, in new directory /{raster_name}_accuracy_assessment

### `avaliacao_exatidao_pyccd.py`

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
