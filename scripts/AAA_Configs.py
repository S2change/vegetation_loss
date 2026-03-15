from pathlib import Path
import os
import sys

USE_CUDA = True #False

normalization_mean = (0.485, 0.456, 0.406, 0.456, 0.406, 0.485)
# (0.485, 0.456, 0.406, 0.485, 0.456, 0.406, 0.485, 0.456, 0.406, 0.485)
normalization_std = (0.229, 0.224, 0.225, 0.224, 0.225, 0.229)
# (0.229, 0.224, 0.225, 0.229, 0.224, 0.225, 0.229, 0.224, 0.225, 0.229)

channel_nums = 6
selected_nums = [0, 1, 2, 3, 4, 5]
# [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

seednumber = 42
LearningRate = 0.01
EPOCH = 100
batch_size = 16 # 28
num_workers = 16

#Train_im_pathA = "G:/BACDM/data/before/" # 2019and2020_before
#Train_im_pathB = "G:/BACDM/data/after/" # 2019and2020_after
#Train_lb_path = "G:/BACDM/data/label/" # 2019and2020_label
#Train_weight_path = "G:/BACDM/logs/" # 权重保存的路径

Train_pretrained_path = None

# Input directory containing all available 16-band GeoTIFF files
#chip_source_folder = r'H:\new_parquets_2017_2025\tabular\T29TNF\processed_outputs\chips' 
working_dir = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5"
# chip_source_folder = os.path.join(working_dir, 'chips')
# Temp directory to store the selected  16-band TIF files
# Input_dir = r".\chips_test\TQG_burn_area" # Dominic tests
# suffix_test_files = "TNF_BA_20241147792"
suffix_test_files = "TNE_buf_468" # around buffer_id=... (BDR_expanded_v0)
chip_source_folder = os.path.join(working_dir, 'chips','all')
Input_dir =  os.path.join(working_dir, "chips", "selected", suffix_test_files)
# where before and after 6-channel geo-referenced tifs are stored
Test_im_pathA = os.path.join(working_dir, "chips", "before",suffix_test_files)
Test_im_pathB = os.path.join(working_dir, "chips", "after",suffix_test_files)
# where predicted change maps will be saved (both as png and geotiff)
Test_det_path = os.path.join(working_dir, "chips", "predictions",suffix_test_files) # change this suffix if you want to save predictions in a different folder
# ICNF burned areas or another vector georeferenced file for tests:
#shp_path,DATA0 = r'H:\ref_datasets\BDR_ICNF\ardida_2024\ardida_2024.shp', "DH_Inicio" # the field name in the shapefile that contains the date information for symbology and labeling; this is used in the qgis_read_reference_BDR_expanded.py script to apply categorized symbology by month and labeling by day of month; make sure to update this field name if your shapefile has a different field for date information, and ensure that the date format in that field is compatible with the expressions used for symbology and labeling in the QGIS script
shp_path, DATA0 =  os.path.join(working_dir, "harmonized", "BDR_expanded_v0.gpkg"), "Data0" 
temp_raster_reference= os.path.join(working_dir, "harmonized_to_tifs") # temporary raster version of the reference vector file; this is used in the qgis_read_reference_BDR_expanded.py script to extract raster values at the locations of the reference vector features for comparison with our predictions; make sure to update this path if you have a different location for the temporary raster version of your reference vector file, and ensure that the rasterization process in the QGIS script correctly aligns with the extent and resolution of your input chips for accurate comparison
# CCD results (rasters or vectors) for comparison with our predictions
# CCD_raster_results_path = None #r"H:\new_parquets_2017_2025\tabular\T29TNF\processed_outputs\rasters" # bimonthly, for 2023 and 2024
# CCD_vector_results_path = r"H:\new_parquets_2017_2025\tabular\T29TNF\processed_outputs\vectors" # bimonthly, for 2023 and 2024

# where the model weights are stored
Test_weight_path = r".\bacdm\logs\B12118A432.pth"


