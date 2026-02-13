# Information about our data sets

## Overview/diagram: the main steps of the project

[diagram](https://ulisboa-my.sharepoint.com/:p:/r/personal/mlc_office365_ulisboa_pt/Documents/Documents/investigacao-projectos-reviews-alunos-juris/projetos/DGT-S2CHANGE_2023/partilhado/overview_s2change.pptx?d=w8d41864a55fd482bac2b518cfb2e24a6&csf=1&web=1&e=2SM1V6)

## Statistics for INCD and local machine for all tiles

Processing file statistics for 17 tiles: [Tabela de processamento INCD](https://ulisboa-my.sharepoint.com/:x:/r/personal/mlc_office365_ulisboa_pt/_layouts/15/Doc.aspx?sourcedoc=%7BE6821FD1-3EA3-4430-8AFD-FE1853792839%7D&file=tiles_incd.xlsx&action=default&mobileredirect=true). See https://github.com/S2change/vegetation_loss/tree/main/documents/HPC_resources

## Files per S2 tile:
*TIFF files* → **from Apr 2017 aprox to 2025-11-20, one file per date and S2 tile**<br>
- TIFFs B3, B4, B8, B12 (downloaded from GEE) —  `D:\s2_images`<br>
- TIFFs B2 and B11 (downloaded from GEE) — `C:\Users\Public\Documents\s2_images_B2_B11`<br>

   [Script: Add link script to convert TIFF into HDF5]

*HDF5 files* 
- HDF5 for B3, B4, B8, B12 ( shape: (dates, bands, n_points)<br> 
   - v1 (**from Apr 2017 aprox to end of 2024, one file per S2 tile**): `E:\outputs_ROI\hdf5\T29SMD\s2_images-NDVI_XX999YM1NOBS6LDA2ITER1000_START20170408_END20241229_ROINAV.h5`<br>
   - v2 (**from Apr 2017 aprox to ~2025/11/20, one file per S2 tile**): `E:\outputs_ROI\hdf5\T29SMD\s2_images-NDVI_XX999YM1NOBS6LDA2ITER1000_START20170408_END20251117_ROI_DGT_mask.h5`<br>

⚠️ HPC Workflow Note ⚠️<br>
Due to storage constraints on the HPC system, HDF5 files were copied in small batches (typically up to 3 S2 tiles at a time) before processing. After processing, the corresponding files were removed from the HPC storage to free disk space.

   [Script: Add link script to create parquets from HDF5]

*Parquets PyCCD outputs*
- Parameters: chisq = 0.999 / alpha = 2 / lasso_iter = 1000<br>
- 480 parquet files (480 tasks) per tile
   - v1 (**from Apr 2017 aprox to end of 2024, 480 files (tasks) per S2 tile**): `E:\old_parquets_2017_2024\tabular`<br>
   - v2 (**from Apr 2017 aprox to ~2025/11/20, 480 files (tasks) per S2 tile**): `C:\Users\Public\Documents\new_parquets_2017_2025\tabular`<br>
- Each segment in the parquet files contains the following attributes:

   | Variable | Type | Description |
   |----------|--------|--------------------------------------------------|
   | tStart | int64 | Segment start time (timestamp in milliseconds) |
   | tEnd | int64 | Segment end time (timestamp in milliseconds) |
   | tBreak | int64 | Change detection time (timestamp in milliseconds) |
   | changeProb | int | Change detection probability |
   | x_coord | int | X coordinate (projected CRS) |
   | y_coord | int | Y coordinate (projected CRS) |
   | coeficientes | array[float] | Harmonic regression coefficients |
   | intercept_values | float | Model intercept value |
   | greenStart | float | Green band fitted value at segment start |
   | greenStart2 | float | Green band second-to-last fitted value at segment start |
   | greenEnd | float | Green band fitted value at segment end |
   | greenEnd2 | float | Green band second-to-last fitted value at segment end |
   | redStart | float | Red band fitted value at segment start |
   | redStart2 | float | Red band second-to-last fitted value at segment start |
   | redEnd | float | Red band fitted value at segment end |
   | redEnd2 | float | Red band second-to-last fitted value at segment end |
   | nirStart | float | NIR band fitted value at segment start |
   | nirStart2 | float | NIR band second-to-last fitted value at segment start |
   | nirEnd | float | NIR band fitted value at segment end |
   | nirEnd2 | float | NIR band second-to-last fitted value at segment end |
   | swir2Start | float | SWIR2 band fitted value at segment start |
   | swir2Start2 | float | SWIR2 band second-to-last fitted value at segment start |
   | swir2End | float | SWIR2 band fitted value at segment end |
   | swir2End2 | float | SWIR2 band second-to-last fitted value at segment end |
   

   [Script: process parquets to extract N=10 window around each reference change `data_exploration\Extraction_S2_2N_observations`? ]

*dates, bands, for N=10 window around each reference change for BDR-DGT-300,  ICNF 2020--2024, and BDR_NVG*<br>
- `C:\Users\Public\Documents\ref_datasets\amostras_por_pixel`<br>

   [Script: to convert Parquets into rasters: `ccd_to_raster.py`]​

*Rasters and polygons generated from PyCCD parquet outputs* → **bimonthly from 2023-01-01 to ~2025-11-20**<br>
- Parameters: tol = 10 days / min_area = 0.5 ha / connectivity = 8<br>
- Rasters (organized per tile) — `C:\Users\Public\Documents\new_parquets_2017_2025\tabular\T29SMD\processed_outputs\rasters`<br>
- Polygons (organized per tile) — `C:\Users\Public\Documents\new_parquets_2017_2025\tabular\T29SMD\processed_outputs\vectors`<br>
- Polygons also available as a single polygon map for forest and shrubland areas across mainland Portugal (rasters not available at this scale) — `C:\Users\Public\Documents\outputs_ROI\tabular\MBPV`

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

