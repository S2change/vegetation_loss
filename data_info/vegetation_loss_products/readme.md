Products:
- MBPV_v1: (*mapa bimestral de perdas de vegetação*). This is an experimental product for 2023-2024 that is solely based on the PyCCD estimated breaks. Pixels with similar dates are clustered and clusters with area smaller than 0.5 ha are discarded. A password is needed to unzip the file `MBPV_v2.zip`.

  The steps to produce this data set are the following
  1. Process Sentinel-2 time series up to 2014-12-31 with PyCCD.
     - Input. hdf5 file with 4 bands B3, B4, B8, B12 (NA=65355), CRS=32629 + hdf5 files for x,y, DGT vegetation loss mask 
     - Ouputs: 
  
