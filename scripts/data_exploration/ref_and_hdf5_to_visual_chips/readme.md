# Vchips (visual chips) 

## Vchip creation

To create vchips the rationale is. Using reference data sets, select one feature at the time. 
For that *reference feature*, with `Data0` and `Data1`, search for the most significant drop of `NDVI` between `Data0` and `Data1`.
If there is a significant drop, then use the date to create a `before` and an `after` time composite of the sprectral data (read `hdf5`file). 
See scripts/data_exploration/ref_and_hdf5_to_visual_chips/ref_and_hdf5_to_visual_chips.py.

Using the pairs `before` and `after` and other sources, vchips were annotated manually. See description of the vchips and visual masks at (https://github.com/S2change/vegetation_loss/tree/main/data_info/reference_data/vchips)


## Convert vchips into chips for training

vchips are tuples (`before`, `after`, `mask`) where `before` an `after` are 16-bit geotiff and `mask` is 8-bit (`NoData` is 65535). The inputs for BACDN are 8-bit tiff files for `before` and `after` and 8-bit `png` file for the mask (`NoData`is 255)

To convert 16-bit into 8-bit bands, we follow the suggestions from the BACDM paper. 

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
