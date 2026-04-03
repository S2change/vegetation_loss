# Vchips (visual chips) 

## Vchip creation

To create vchips the rationale is. Using reference data sets, select one feature at the time. 
For that *reference feature*, with `Data0` and `Data1`, search for the most significant drop of `NDVI` between `Data0` and `Data1`.
If there is a significant drop, then use the date to create a `before` and an `after` time composite of the sprectral data (read `hdf5`file). 
See [script](scripts/data_exploration/ref_and_hdf5_to_visual_chips/ref_and_hdf5_to_visual_chips.py)

Using the pairs `before` and `after` and other sources, vchips were annotated manually. See (https://github.com/S2change/vegetation_loss/tree/main/data_info/reference_data/vchips)


## Convert vchips into chips for training

