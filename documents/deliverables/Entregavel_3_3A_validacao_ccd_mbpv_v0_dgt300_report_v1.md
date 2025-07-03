# Report for Creating Change Clusters and Conducting Accuracy Assessment for Tile T29TNE

This report reviews the processes used to create visualizations of the breaks detected by the CCDC model and assessing the accuracy of those breaks.

## Creating the clusters

Grouping the pixels that experienced a similar break together was done through the script [raster_polygon_processing.py](../../scripts/visualisations/raster_polygon_processing.py). The input variables determine the date range of interest, the date range tolerance for grouping pixels together, and the minimum area clusters need to cover to be saved to the final output. For this process, the date range was 2019-01-01 to 2020-12-31. This range was selected because it is the most recent dates that overlap with the reference data set that will be used in the accuracy assessment. The selected date range tolerance was 30 days, and the minimum area was 0.5 hectares.

Starting with the T29TNE CCDC results, the results were broken into groups of every two months. In each group, the most recent break date was saved for each pixel, and then a raster file where the value of each pixel was the recent break date in the format YYYYMMDD. Next step was to create a polygon file, in order to group pixels together and determine which groups should be saved because they were over 0.5 ha. Using the pixel values, all pixels within 30 days of each other were were assigned the same date, which was the most recent date out of the pixels grouped together. Then rasterio's shapes module was used to create polygons of connected pixels with the same assigned date. Finally, all polygons with areas under 0.5 ha were deleted.

The end result of this process were 2 directories, one containing raster files and one containing polygon files for every 2 months between the date ranges.

## Accuracy Assessment

The accuracy assessment was done through the script [raster_avaliacao_exatidao.py](../../scripts/validation/raster_avaliacao_exatidao.py). The input variable theta is the tolerance margin between the predicted break date and the reference break date, and for our assessment it was set to 60 days. It also uses the 2 directories of raster and polygon files of the clusters of areas where breaks occurred.

For every 2 month period, the script first uses the polygon file to create a mask of the raster file, which creates a raster file only containing the pixels that were grouped together with areas largers than 0.5 ha. This new masked raster is then spatially joined with the BDR reference data. The difference between the two dates from the predicted and the reference data is calculated, and the results are classified in the accuracy assessment based on whether or not the difference is within the tolerance margin.

The results table is below, with an average F1 Score of 95.5. Because the assessment was only ran on pixels where a change was detected, there are no True Negative results, which is to be expected.

## Results Table

| Filename | F1 Score | Omission Error | Commission Error | Total VP | Total FP | Total FN | Total VN |
|----------|----------|----------------|------------------|----------|----------|----------|----------|
| 2019-01 - 2019-02 | 95.94 | 0 | 7.81 | 248 | 21 | 0 | 0 |
| 2019-03 - 2019-04 | 88.34 | 8.82 | 14.33 | 1106 | 185 | 107 | 0 |
| 2019-05 - 2019-06 | 75.6 | 15.61 | 31.54 | 254 | 117 | 47 | 0 |
| 2019-07 - 2019-08 | 99.25 | 0.41 | 1.1 | 5860 | 65 | 24 | 0 |
| 2019-09 - 2019-10 | 89.68 | 9.46 | 11.16 | 1990 | 250 | 208 | 0 |
| 2019-11 - 2019-12 | 71.5 | 27.11 | 29.83 | 207 | 88 | 77 | 0 |
| 2020-01 - 2020-02 | 94.42 | 4.35 | 6.77 | 923 | 67 | 42 | 0 |
| 2020-03 - 2020-04 | 71.06 | 21.55 | 35.05 | 415 | 224 | 114 | 0 |
| 2020-05 - 2020-06 | 86.92 | 14.01 | 12.14 | 1578 | 218 | 257 | 0 |
| 2020-07 - 2020-08 | 99.17 | 0.53 | 1.13 | 4903 | 56 | 26 | 0 |
| 2020-09 - 2020-10 | 98.98 | 0.42 | 1.62 | 12681 | 209 | 53 | 0 |
| 2020-11 - 2020-12 | 62.89 | 34.62 | 39.42 | 355 | 231 | 188 | 0 |
| GRAND_TOTAL | 95.5 | 3.61 | 5.37 | 30520 | 1731 | 1143 | 0 |
