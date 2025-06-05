
## PyCCD Accuracy Assessment
- `avaliacao_exatidao_pyccd.py`

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