# Information about our data sets

## Files per S2 tile:
- tiff files bands B3, B4, B8, B12 (downloaded from GEE); ISA (all); from Apr 2017 approx to end of 2024
- tiff files bands B2 and B11 (downloaded from GEE); ISA (all); from Apr 2017 approx to end of 2024
- hdf5 for B3,B4,B8,B12; ISA (all), INCD (some) -> shape: (dates, bands, n_points)
- hdf5 for B2, B11
- parquets pyccd with chisq=0.999: ISA (all); INCD (all); oneDrive (some)
- rasters and polygons generated from PyCCD parquet outputs; tol=30 days, min_area=0.5 ha; bimonthly from 2023-01-01 to 2024-12-31; organized per tile — also available as a single raster and polygon map for forest and shrubland areas across mainland Portugal; ISA.
- dates, bands, for N=10 window around each reference change for BDR-DGT-300,  ICNF 2020--2024, and BDR_NVG (ISA)

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

