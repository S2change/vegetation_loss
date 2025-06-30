# Validation of [MBPV_v0](../data_info/vegetation_loss_products) for tile T29TNE with [BDR_DGT_300](../data_info/reference_data/BDR_DGT_300) reference data.

(Detailed report to be available soon)

## Parameters

### Identifying Clusters

Tile Used - T29TNE_0999 \
Start Date - 2019-01-01 \
End Date - 2020-12-31 \
CRS - EPSG: 32629 \
Date range for grouping pixels together - 30 \
Minimum polygon area - 0.5 ha

### Accuracy Assessment

Break day tolerance - 60 \
Tile Used - T29TNE_0999 \
Reference File - BDR_CCDC_TNE_Adjusted.shp


## Results Table

| Filename | F1 Score | Omission Error | Commission Error | Total VP | Total FP | Total FN | Total VN |
|----------|----------|----------------|------------------|----------|----------|----------|----------|
| T29TNE_0999_201901-201902.tif | 95.94 | 0 | 7.81 | 248 | 21 | 0 | 0 |
| T29TNE_0999_201903-201904.tif | 88.34 | 8.82 | 14.33 | 1106 | 185 | 107 | 0 |
| T29TNE_0999_201905-201906.tif | 75.6 | 15.61 | 31.54 | 254 | 117 | 47 | 0 |
| T29TNE_0999_201907-201908.tif | 99.25 | 0.41 | 1.1 | 5860 | 65 | 24 | 0 |
| T29TNE_0999_201909-201910.tif | 89.68 | 9.46 | 11.16 | 1990 | 250 | 208 | 0 |
| T29TNE_0999_201911-201912.tif | 71.5 | 27.11 | 29.83 | 207 | 88 | 77 | 0 |
| T29TNE_0999_202001-202002.tif | 94.42 | 4.35 | 6.77 | 923 | 67 | 42 | 0 |
| T29TNE_0999_202003-202004.tif | 71.06 | 21.55 | 35.05 | 415 | 224 | 114 | 0 |
| T29TNE_0999_202005-202006.tif | 86.92 | 14.01 | 12.14 | 1578 | 218 | 257 | 0 |
| T29TNE_0999_202007-202008.tif | 99.17 | 0.53 | 1.13 | 4903 | 56 | 26 | 0 |
| T29TNE_0999_202009-202010.tif | 98.98 | 0.42 | 1.62 | 12681 | 209 | 53 | 0 |
| T29TNE_0999_202011-202012.tif | 62.89 | 34.62 | 39.42 | 355 | 231 | 188 | 0 |
| GRAND_TOTAL | 95.5 | 3.61 | 5.37 | 30520 | 1731 | 1143 | 0 |
