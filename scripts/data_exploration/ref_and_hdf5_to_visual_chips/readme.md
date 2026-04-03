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

     Dear Manuel Campagnolo,
     Thank you for your message and for the careful follow-up questions.
     1. The first author (Liu Peng) has recently taken a position in an institution affiliated with the military, so it is currently not very convenient to communicate with him directly. Below, I summarize the final scheme that we discussed internally and agreed upon previously.
     2. Regarding data normalization of the image chips, we experimented with a number of different strategies. For spectral stretching in particular, we tested several options, including direct truncation (e.g., clipping to a fixed range such as 350–3500, these two numbers are determined by calculated
     the histogram of all BA pixels in the dataset). After comparison, we finally adopted a percent clipping strategy using quantiles 1.5 and 98.5 per band.
     3. For the percent clipping, we evaluated multiple ways of computing the quantiles, including: 
     a) computing quantiles from all PRE- and POST-fire image chips jointly for each band across the dataset (which helps preserve the spectral characteristics of burned areas); 
     b) computing quantiles separately for PRE- and POST-fire image chips across the dataset for each band (which may affect the burned-area spectral characteristics), and 
     c) computing quantiles independently for each image chip and each band. Based on comparative experiments at a continental scale, 
     we found that the last strategy—i.e., computing the quantiles independently for each image chip and each band—provided the strongest generalization performance. Therefore, this approach was adopted in our final workflow.

Kind regards,
Yongxue
     
     Dear Colleagues,
     Thank you for your interest in our work; you are actually the first to raise this specific technical question regarding the input data. T
     o convert the Sentinel-2 reflectances from GEE (0-10000) to the 8-bit range (0-255) used in our model, we applied a normalization strategy based on the percent clip method, 
     where the values are clipped at the 1.5% and 98.5% percentiles and then linearly stretched to 0-255. 
     While our model expects int8 inputs, this was primarily a practical choice to reduce the data volume and storage footprint during training; in fact, 
     the images could also be normalized directly to a 0.0-1.0 range and the model retrained accordingly. 

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
