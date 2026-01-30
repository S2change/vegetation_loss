# chips/
This directory is for scripts that are used to create chips that can be processed through DL models

### chips_S2_dates.py
Takes a raster file which has bands for break date and is break (ie files produced from ccd_to_raster.py) and creates chips with S2 spectral readings from before after after the break date. Readings are Bands 2, 3, 4, 8, 11, and 12

Inputs:
- TIF with band for break date and is break. Usually the tif files produced from ccd_to_raster.py
- Path to directory with S2 images that contain B2 and B11 readings
- Path to directory with S2 images that contain B3, B4, B8, and B12 readings

Outputs:
- Individual 16 band tif files for each chip. 
    - Bands 1-6: Pre-break spectral values
    - Band 7: break date for the pixel (same break date used for all pixels in each chip)
    - Band 8-13: Post-break spectral values
    - Band 14: is_break value from ccd_to_raster.py output (-99 = No no data, -1 = uncertain break, 0 = had data but no break, 1 = valid break)
    - Band 15: Pre-break timestamp of S2 reading used for this pixel
    - Band 16: Post-break timestamp of S2 reading used for this pixel