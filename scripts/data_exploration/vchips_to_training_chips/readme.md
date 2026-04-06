# Creating chips for training from vchips

See description of vchips at (https://github.com/S2change/vegetation_loss/tree/main/data_info/reference_data/vchips)

`vchips_to_training_chips.py` reads vchips triplets and creates N=4 256*256 chips:

1. Inputs: vchips in geotiff format
   a. Before and After: 4 by 4 km2 'before' and 'after' 6-band 16-bit geotiff
   b. mask geotiff with classes 0,1,2,...
   c. The input files have names like

   *) "C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\vchips\source_rasters\vchip_680435_4497955_20200704_after.tif"
   *) "C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\vchips\source_rasters\vchip_680435_4497955_20200704_before.tif"
   *) "C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\vchips\mask_rasters\vchip_680435_4497955_20200704_mask.tif"

2. Outputs:
   a. All files in the triplet  'before', 'after' and 'mask' need to be cropped into 256*256 aligned chips (2560 m by 2560 m).
   b. Before and After: 8-bit tif files with the same bands
   c. The output 'before' and 'after' files should be saved as tif files 
   d. The ouput mask files should be saved in png format
   e. All 3 files have the same stem name, e.g. `vchip_680435_4497955_20200704_04` (x,y,date,chip index within vchip)

   *) "C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\before" 
   *) "C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\after" 
   *) C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\label
