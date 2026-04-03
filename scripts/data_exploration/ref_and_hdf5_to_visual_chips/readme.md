# Vchips (visual chips) 

## Vchip creation

To create vchips the rationale is. Using reference data sets, select one feature at the time. 
For that *reference feature*, with `Data0` and `Data1`, search for the most significant drop of `NDVI` between `Data0` and `Data1`.
If there is a significant drop, then use the date to create a `before` and an `after` time composite of the sprectral data (read `hdf5`file). 
See scripts/data_exploration/ref_and_hdf5_to_visual_chips/ref_and_hdf5_to_visual_chips.py.

Using the pairs `before` and `after` and other sources, vchips were annotated manually. See description of the vchips and visual masks at (https://github.com/S2change/vegetation_loss/tree/main/data_info/reference_data/vchips)


## Convert vchips into chips for training

vchips are tuples (`before`, `after`, `mask`) where `before` an `after` are 16-bit geotiff and `mask` is 8-bit (`NoData` is 65535). The inputs for BACDN are 8-bit tiff files for `before` and `after` and 8-bit `png` file for the mask (`NoData`is 255)

To convert 16-bit into 8-bit bands, we follow the suggestions from the authors of the BACDM paper:

     Dear Colleagues,
     Thank you for your interest in our work; you are actually the first to raise this specific technical question regarding the input data. To convert the Sentinel-2 reflectances from GEE (0-10000) to the 8-bit range (0-255) used in our model, we applied a normalization strategy based on the percent clip method, where the values are clipped at the 1.5% and 98.5% percentiles and then linearly stretched to 0-255. While our model expects int8 inputs, this was primarily a practical choice to reduce the data volume and storage footprint during training; in fact, the images could also be normalized directly to a 0.0-1.0 range and the model retrained accordingly. 

Best regards,

Inputs for BACDM are organized in the following structure:

```
data
|--- before
     |--- prefix_date_location_01.tif
     |--- prefix_date_location_02.tif
     ...
|--- after
     |--- prefix_date_location_01.tif
     |--- prefix_date_location_02.tif
     ...
|--- label
     |--- prefix_date_location_01.png
     |--- prefix_date_location_02.png
     ...
```
