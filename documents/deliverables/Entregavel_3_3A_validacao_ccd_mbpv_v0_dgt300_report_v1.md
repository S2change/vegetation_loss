# Entregável 3.3.B: Report for Creating Change Clusters and Conducting Accuracy Assessment for Tile T29TNE (revised nov 2025)

This report reviews the processes used to create visualizations of the breaks detected by the CCDC model and assessing the accuracy of those breaks.

## Creating the clusters

Grouping the pixels that experienced a similar break together was done through the script [raster_polygon_processing.py](../../scripts/visualisations/graph_raster_to_polygons.py). This is a graph based algorithm where the input variables are the locations and chage detection dates for the pixels, the date range tolerance for connecting neighbor pixels (10 days), and the minimum area (0.5 ha) clusters need to cover to be saved to the final output. For this process, the date range was 2018-09-01 to 2021-10-31. This range was selected because it is the most recent dates that overlap with the reference data set that will be used in the accuracy assessment. The selected date range tolerance for validation was 30 days.

Starting with the T29TNE CCDC results, the results were broken into groups of every two months (set-oct 2018, nov-dec 2018, jan-fev 2019, ..., set-out 2021). In each group, the most recent break date was saved for each pixel, and then a raster file where the value of each pixel was the recent break date in the format YYYYMMDD. Next step was to create a graph where nodes are pixels and edges connect pixels that are neighbors (we used 8-connectivity) and have dates that differ at most 10 days. Then, we apply the connected components algorithm fo the graph to get clusters of pixels.  Using the pixel values, the mean date for each cluster is computed and assigned to that cluster. Then rasterio's shapes module was used to create polygons of connected pixels with the same assigned date. Finally, all polygons with areas under 0.5 ha were deleted.

The end result of this process were 2 directories, one containing raster files and one containing polygon files for every 2 months between the date ranges.

## Accuracy Assessment

The accuracy assessment was done through the script [raster_avaliacao_exatidao.py](../../scripts/validation/raster_avaliacao_exatidao.py). The input variable theta is the tolerance margin between the predicted break date and the reference break date, and for our assessment it was set to 60 days. It also uses the 2 directories of raster and polygon files of the clusters of areas where breaks occurred.

For every 2 month period, the script first uses the polygon file to create a mask of the raster file, which creates a raster file only containing the pixels that were grouped together with areas largers than 0.5 ha. This new masked raster is then spatially joined with the BDR reference data. The difference between the two dates from the predicted and the reference data is calculated, and the results are classified in the accuracy assessment based on whether or not the difference is within the tolerance margin.

The results table is below, with an average F1 Score of 74.03. Because the assessment was only ran on pixels where a change was detected, there are no True Negative results, which is to be expected.

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
