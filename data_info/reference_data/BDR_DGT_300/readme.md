**WARNING JULY 28, 2025**: The `BDR_CCDC_TNE_Adjusted` shapefile data set that has been used is not complete. Apparently, it derives from some spatial operation with COSc, which clipped polygons from the original reference data set. Therefore, the 200 m buffers around the 300 center points are not all covered. The correct shapefile is `BDR_CCDC_TNE_V3` and is available in the same  `\ref__datasets\BDR_TNE_300` subfolder of the OneDrive project shared folder. This complete data set covers totally the area of the 300 buffers for the TNE tile:
> \ref__datasets\BDR_TNE_300\BDR_CCDC_TNE_V3 # to be used as reference data set
> \ref__datasets\BDR_TNE_300\BDR_CCDC_TNE_Adjusted # not to be used as reference data set (incomplete)

António is editing the `BDR_CCDC_TNE_V3` reference data set to extend the polygons of class `altera=='Sem Alteracao'` to a larger área (approx 1 km2) around each point, for a subset of the 300 sample points.

This reference data set refers to S2 tile T29TNE and period Septembre 2018 -  September 2021 and is described in the following reference.

- Moraes D., Barbosa B., Costa H., Moreira F.D., Benevides P., Caetano M., Campagnolo M. Continuous forest loss monitoring in a dynamic landscape of Central Portugal with Sentinel-2 data,  (2024), International Journal of Applied Earth Observation and Geoinformation, 130, DOI: 10.1016/j.jag.2024.103913


Fields of the reference data set `BDR_CCDC_TNE_V3`:

| Id   | Name        | Alias | Type             | Type name | Length | Precision | Comment |
|------|-------------|-------|------------------|-----------|--------|-----------|---------|
| 0    | ID          |       | Integer (64 bit) | Integer64 | 10     | 0         |         |
| 1    | buffer_ID   |       | Integer (64 bit) | Integer64 | 10     | 0         |         |
| 2    | altera      |       | Text (string)    | String    | 80     | 0         |         |
| 3    | tipo_1      |       | Text (string)    | String    | 80     | 0         |         |
| 4    | classe_0    |       | Text (string)    | String    | 80     | 0         |         |
| 5    | data_0      |       | Text (string)    | String    | 80     | 0         |         |
| 6    | classe_1    |       | Text (string)    | String    | 80     | 0         |         |
| 7    | data_1      |       | Text (string)    | String    | 80     | 0         |         |
| 8    | tipo_2      |       | Text (string)    | String    | 50     | 0         |         |
| 9    | classe_2    |       | Text (string)    | String    | 80     | 0         |         |
| 10   | data_2      |       | Text (string)    | String    | 80     | 0         |         |
| 11   | classe_3    |       | Text (string)    | String    | 31     | 0         |         |
| 12   | data_3      |       | Text (string)    | String    | 20     | 0         |         |
| 13   | classe2018  |       | Text (string)    | String    | 80     | 0         |         |
| 14   | classe2019  |       | Text (string)    | String    | 80     | 0         |         |
| 15   | classe2020  |       | Text (string)    | String    | 80     | 0         |         |
| 16   | classe2021  |       | Text (string)    | String    | 80     | 0         |         |
| 17   | area        |       | Decimal (double) | Real      | 23     | 15        |         |
| 18   | notas       |       | Text (string)    | String    | 254    | 0         |         |
