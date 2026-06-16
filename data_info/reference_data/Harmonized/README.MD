# Harmonized Reference Database

This folder contains the harmonized reference datasets used for vegetation-change analysis in Portugal:

1. NVG
2. ICNF
3. BDR-TNE-300
4. BDR-TNE-300-Expanded

“Harmonized” means that the original datasets were converted into a common structure, including standardized field names, identifiers, temporal information, text values, spatial reference system, and other shared attributes. Detailed documentation for each dataset is available in the corresponding Markdown files in this folder.

# Attributes of the harmonized reference dataset (maximal set of attributes)

```
Field    Type
fid    INTEGER
geom    MULTIPOLYGON
Id    TEXT
Cod_ug    TEXT
Cod_talhao    TEXT
Id_gleba    TEXT
Ciclo    TEXT
Rotacao    TEXT
Dt_referen    TEXT
Dt_plant    TEXT
Ocupacao    TEXT
Idade_ref    TEXT
Idade_plan    TEXT
__excluded_by_dropped    TEXT
__drop_pt_count    TEXT
Src    TEXT
Uid    TEXT
Eccd1    TEXT
Eccd2    TEXT
Chg_type    TEXT
Pi_dicofre    TEXT
Pix_total    TEXT
Pix_null_data0    TEXT
Pix_null_data1    TEXT
Null_prop_data0    TEXT
Null_prop_data1    TEXT
Data0    TEXT
Data1    TEXT
Data0_q1    TEXT
Data0_q3    TEXT
Data0_p90    TEXT
Data1_p10    TEXT
Data1_q1    TEXT
Data1_q3    TEXT
Data_iqr_days_data0    TEXT
Data_iqr_days_data1    TEXT
Data0_min    TEXT
Data1_max    TEXT
Data1_data0_difference    TEXT
Temp_eval_start    TEXT
Temp_eval_end    TEXT
Pix_total_ccdc    TEXT
Pix_ok_ccdc    TEXT
Ok_ratio_ccdc    TEXT
Validation_flag    TEXT
S2_tile    TEXT
Area_ha    REAL
Classe_0    TEXT
```
Classe_1    TEXT
Buffer_id    TEXT
source_layer    TEXT
