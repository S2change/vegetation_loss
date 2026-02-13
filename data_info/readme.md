# Information about our data sets

## Overview/diagram: the main steps of the project

[diagram](https://ulisboa-my.sharepoint.com/:p:/r/personal/mlc_office365_ulisboa_pt/Documents/Documents/investigacao-projectos-reviews-alunos-juris/projetos/DGT-S2CHANGE_2023/partilhado/overview_s2change.pptx?d=w8d41864a55fd482bac2b518cfb2e24a6&csf=1&web=1&e=2SM1V6)

## Files per S2 tile:
*TIFF files* → **from Apr 2017 aprox to 2025-11-20, one file per date and S2 tile**<br>
- TIFFs B3, B4, B8, B12 (downloaded from GEE) —  `D:\s2_images`<br>
- TIFFs B2 and B11 (downloaded from GEE) — `C:\Users\Public\Documents\s2_images_B2_B11`<br>

   [Script: Add link script to convert TIFF into HDF5]

*HDF5 files* → **from Apr 2017 aprox to end of 2024, one file per S2 tile**<br>
- HDF5 for B3, B4, B8, B12 ( shape: (dates, bands, n_points) ) — `E:\outputs_ROI\hdf5`<br> Add note about copying these files to HPC (was it necessary to copy/remove each tile?)
- HDF5 for B2 and B11 — directory doesn't exist yet.

   [Script: Add link script to create parquets from HDF5]

*Parquets PyCCD outputs* → **from Apr 2017 aprox to end of 2024, 480 files (tasks) per S2 tile** (2 versions?)
- Parameters: chisq = 0.999 / alpha = 2 / lasso_iter = 1000<br>
- `C:\Users\Public\Documents\outputs_ROI\tabular`<br>
- 480 parquet files (480 tasks) per tile
- What are the variables available for each segment in the parquet files?

   [Script: process parquets to extract N=10 window around each reference change `data_exploration\Extraction_S2_2N_observations`? ]

*dates, bands, for N=10 window around each reference change for BDR-DGT-300,  ICNF 2020--2024, and BDR_NVG*<br>
- `C:\Users\Public\Documents\ref_datasets\amostras_por_pixel`<br>

   [Script: to convert Parquets into rasters: `ccd_to_raster.py`/ `data_exploration\temp_extract_B2B11_start_end.py`, `data_exploration\B2B11_extract_raster.py`]​

*Rasters and polygons generated from PyCCD parquet outputs* → **bimonthly from 2023-01-01 to 2024-12-31**<br>
- Parameters: tol = 10 days / min_area = 0.5 ha / connectivity = 8<br>
- Rasters (organized per tile) — `C:\Users\Public\Documents\outputs_ROI\tabular\T29SMD\processed_outputs\rasters`<br>
- Polygons (organized per tile) — `C:\Users\Public\Documents\outputs_ROI\tabular\T29SMD\processed_outputs\vectors`<br>
- Polygons also available as a single polygon map for forest and shrubland areas across mainland Portugal (rasters not available at this scale) — `C:\Users\Public\Documents\outputs_ROI\tabular\MBPV_v1`

## Reference data (available in oneDrive, folder ref_datasets)
1. BDR-DGT-300
2. BDR-NVG
   - Original NVG data base in polygon format: BRD_NVG_V01_polygons_3763
   - BDR corrected and validated by Inês Silveira in point format: folder BRD_NVG_S2_V02
   - Updated BRD_NVG_S2_V02 with cleaner date attribute names, date format, and added additional attributes filled with NULL values that were needed for future analysis: BDR_NVG_S2_V02_Updated_Attributes
   - Vectorial version created by Dominic in polygon format: BDR_NVG_S2_V02_Polygons
      - Polygons were created by buffering the points by 5.01 meters with a square end cap style, dissolving the polygons based on data_0 and data_1 values, and then buffering by -0.01 meters
4. ICNF áreas ardidas 2020--2024
   - download from https://geocatalogo.icnf.pt/catalogo_tema5.html

