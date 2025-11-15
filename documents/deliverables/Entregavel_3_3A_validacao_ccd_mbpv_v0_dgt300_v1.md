# Validation of [MBPV_v0](../data_info/vegetation_loss_products) for tile T29TNE with [BDR_DGT_300](../data_info/reference_data/BDR_DGT_300) reference data.

(Detailed report to be available soon)

## Parameters

### Identifying Clusters

Tile Used - T29TNE_0999 \
Start Date - 2018-09-01 \
End Date - 2021-10-31 \
CRS - EPSG: 32629 \
Date range for grouping pixels together - 10 \
Minimum polygon area - 0.5 ha

### Accuracy Assessment

Break day tolerance - 60 \
Tile Used - T29TNE \
Reference File - BDR_CCDC_TNE_v3.shp


## Results Table

| filename                                      | f1_score | omission_error | commission_error | total_VP | total_FP | total_FN | total_VN | had_polygon_mask |
|-----------------------------------------------|----------|----------------|------------------|----------|----------|----------|----------|------------------|
| output_raster_ccd_20180901_to_20181031.tif    | 29.92    | 52.89          | 78.08            | 212      | 755      | 238      | 0        | True             |
| output_raster_ccd_20181101_to_20181231.tif    | 42.33    | 35.54          | 68.48            | 156      | 339      | 86       | 0        | True             |
| output_raster_ccd_20190101_to_20190228.tif    | 55.86    | 23.46          | 56.02            | 336      | 428      | 103      | 0        | True             |
| output_raster_ccd_20190301_to_20190430.tif    | 67.53    | 17.52          | 42.83            | 1144     | 857      | 243      | 0        | True             |
| output_raster_ccd_20190501_to_20190630.tif    | 58.29    | 20.82          | 53.88            | 327      | 382      | 86       | 0        | True             |
| output_raster_ccd_20190701_to_20190831.tif    | 95.92    | 1.49           | 6.55             | 5824     | 408      | 88       | 0        | True             |
| output_raster_ccd_20190901_to_20191031.tif    | 85.56    | 9.51           | 18.86            | 2065     | 480      | 217      | 0        | True             |
| output_raster_ccd_20191101_to_20191231.tif    | 62.53    | 26.2           | 45.75            | 338      | 285      | 120      | 0        | True             |
| output_raster_ccd_20200101_to_20200229.tif    | 70.58    | 17.37          | 38.4             | 1075     | 670      | 226      | 0        | True             |
| output_raster_ccd_20200301_to_20200430.tif    | 65.36    | 20.58          | 44.47            | 683      | 547      | 177      | 0        | True             |
| output_raster_ccd_20200501_to_20200630.tif    | 76.5     | 16.34          | 29.53            | 1802     | 755      | 352      | 0        | True             |
| output_raster_ccd_20200701_to_20200831.tif    | 97.11    | 2              | 3.77             | 4954     | 194      | 101      | 0        | True             |
| output_raster_ccd_20200901_to_20201031.tif    | 98.16    | 1.15           | 2.52             | 12812    | 331      | 149      | 0        | True             |
| output_raster_ccd_20201101_to_20201231.tif    | 52.01    | 44.73          | 50.89            | 388      | 402      | 314      | 0        | True             |
| output_raster_ccd_20210101_to_20210228.tif    | 54.59    | 41.83          | 48.57            | 972      | 918      | 699      | 0        | True             |
| output_raster_ccd_20210301_to_20210430.tif    | 84.65    | 8.73           | 21.07            | 502      | 134      | 48       | 0        | True             |
| output_raster_ccd_20210501_to_20210630.tif    | 83.83    | 8.28           | 22.81            | 1262     | 373      | 114      | 0        | True             |
| output_raster_ccd_20210701_to_20210831.tif    | 83.53    | 7.05           | 24.15            | 936      | 298      | 71       | 0        | True             |
| output_raster_ccd_20210901_to_20211031.tif    | 59.77    | 18.76          | 52.73            | 849      | 947      | 196      | 0        | True             |
| **GRAND_TOTAL**                               | **74.03**| **14.64**      | **34.65**         | **36637**| **19426**| **6284** | **0**    | **N/A**          |
