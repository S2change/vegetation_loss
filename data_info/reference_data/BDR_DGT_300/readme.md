**WARNING JULY 28, 2025**: The `BDR_CCDC_TNE_Adjusted` shapefile data set that has been used is not complete. Apparently, it derives from some spatial operation with COSc, which clipped polygons from the original reference data set. Therefore, the 200 m buffers around the 300 center points are not all covered. The correct shapefile is `BDR_CCDC_TNE_V3` and is available in the same  `\ref__datasets\BDR_TNE_300` subfolder in the shared OneDrive project folder. This complete data set covers totally the area of the 300 buffers for the TNE tile.

This reference data set refers to S2 tile T29TNE and period Septembre 2018 -  September 2021 and is described in the following reference.

- Moraes D., Barbosa B., Costa H., Moreira F.D., Benevides P., Caetano M., Campagnolo M. Continuous forest loss monitoring in a dynamic landscape of Central Portugal with Sentinel-2 data,  (2024), International Journal of Applied Earth Observation and Geoinformation, 130, DOI: 10.1016/j.jag.2024.103913

Fields of the reference data set:

| Id | Name | Alias | Type | Type name | Length | Precision |
| :-- | :-- | :-- | :-- | :-- | :-- | :-- |
| 0 | ID |  | Decimal (double) | Real | 23 | 15 |
| 1 | buffer_ID |  | Integer (64 bit) | Integer64 | 18 | 0 |
| 2 | altera |  | Text (string) | String | 80 | 0 |
| 3 | tipo_1 |  | Text (string) | String | 80 | 0 |
| 4 | classe_0 |  | Text (string) | String | 80 | 0 |
| 5 | data_0 |  | Text (string) | String | 80 | 0 |
| 6 | classe_1 |  | Text (string) | String | 80 | 0 |
| 7 | data_1 |  | Text (string) | String | 80 | 0 |
| 8 | tipo_2 |  | Text (string) | String | 80 | 0 |
| 9 | classe_2 |  | Text (string) | String | 80 | 0 |
| 10 | data_2 |  | Text (string) | String | 80 | 0 |
| 11 | classe_3 |  | Text (string) | String | 80 | 0 |
| 12 | data_3 |  | Text (string) | String | 80 | 0 |
| 13 | classe2018 |  | Text (string) | String | 80 | 0 |
| 14 | classe2019 |  | Text (string) | String | 80 | 0 |
| 15 | classe2020 |  | Text (string) | String | 80 | 0 |
| 16 | classe2021 |  | Text (string) | String | 80 | 0 |
| 17 | area |  | Decimal (double) | Real | 23 | 15 |
