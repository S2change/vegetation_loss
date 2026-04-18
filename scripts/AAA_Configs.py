from pathlib import Path
import os
import sys

USE_CUDA = True #False

# used in dataset_swin_GZ.py for normalization of the input images (imagenet mean and std for the 6 selected bands, calculated from the training data?) 
normalization_mean = (0.485, 0.456, 0.406, 0.456, 0.406, 0.485)
# (0.485, 0.456, 0.406, 0.485, 0.456, 0.406, 0.485, 0.456, 0.406, 0.485)
normalization_std = (0.229, 0.224, 0.225, 0.224, 0.225, 0.229)
# (0.229, 0.224, 0.225, 0.229, 0.224, 0.225, 0.229, 0.224, 0.225, 0.229)

channel_nums = 6
selected_nums = [0, 1, 2, 3, 4, 5]
# [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

seednumber = 42
#LearningRate = 0.01 # mc: was  0.01 # see below
EPOCH = 100 #100 # testes #100
ALPHA=0.5 #CE_loss vs DICE_loss weight in the overall objective function for training; you can experiment with different values for alpha to see how it affects training performance and model learning, especially in terms of how well the model learns from the imbalanced classes in your dataset; for example, if you find that the model is struggling to learn from the rarer classes, you might try increasing the weight of the dice loss (which can help with imbalanced data) by using a lower value for alpha, such as 0.5 or 0.3, to give more emphasis to the dice loss during training; conversely, if you find that the model is learning well from all classes and you want to prioritize overall accuracy, you might try a higher value for alpha, such as 0.9, to give more emphasis to the cross-entropy loss during training
batch_size = 16 # 28
num_workers = 16
NUM_CLASSES = 5 # for our 5 classes (0-4) in the training data; update this if you have a different number of classes in your training data
CLASS_WEIGHTS=[1.0, 10.0, 10.0, 10.0, 2.0] # [1.0, 20., 50.0, 50.0, 5.0] # Adjust these based on your earlier distribution (90%, 0.23%, etc.) and experimentation; higher numbers for rarer classes to help the model learn better from the imbalanced classes in our dataset, especially since we have a very high imbalance with Class 0 being much more prevalent than the other classes; these weights can be adjusted based on experimentation and the specific distribution of classes in your training data for potentially improved performance

# Original BACDM data paths (update these paths if you have the data stored in a different location, or if you want to use a different dataset for training and testing)
# ----------------- Train --------------------------#
#working_dir = r"C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\investigacao-projectos-reviews-alunos-juris\projetos\DGT-S2CHANGE_2023\repos\vegetation_loss\scripts"
#Train_im_pathA = os.path.join(working_dir, "bacdm","data", "before")  #"G:/BACDM/data/before/" # 2019and2020_before
#Train_im_pathB = os.path.join(working_dir, "bacdm","data", "after")  # 2019and2020_after
#Train_lb_path = os.path.join(working_dir, "bacdm","data", "label")  # 2019and2020_label
working_dir = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5"
Train_im_pathA = os.path.join(working_dir, "training_data", "before")  
Train_im_pathB = os.path.join(working_dir, "training_data", "after")  
Train_lb_path = os.path.join(working_dir, "training_data", "label")  

# OUtput model weights path (update this path if you want to save the trained model weights in a different location)
Train_weight_path = os.path.join(working_dir, "bacdm", "bacdm_weights","341_FS_0110101002_LR01_alpha50_")  # OUTPUT prefix to pth and txt file names; see train.py # 权重保存的路径

# 6 bands original weights path (encoder-only pretrained weights from BACDM paper)
Train_pretrained_path = os.path.join(working_dir, "logs", "B12118A432.pth")

# Resume from a previously saved fine-tuned checkpoint (full encoder+decoder weights).
# Set to None to start fresh from Train_pretrained_path (encoder only).
# Set to a .pth path saved by train.py to continue training from that checkpoint.
Resume_checkpoint_path = None
#Resume_checkpoint_path = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\bacdm_weights\vchips2020_90_CE_Dice_20260408033235_99.pth"

LearningRate = 0.01
#LearningRate = 0.00001  # use a smaller LR when resuming

# ----------------- Test --------------------------#
# where the model weights are stored # <<<<<<<<<<<<<<< choose best model
# modelo razoável, mas faz patches "gordos"
#Test_weight_path = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\bacdm_weights\vchips2020_90_B_20260406213205_70.pth" # r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\bacdm_weights\vchips2020_90_D_20260407140152_9.pth"
#Test_weight_path = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\bacdm_weights\vchips2020_90_CE_Dice_20260408033235_99.pth"
# 10 abril; melhor modelo até agora; os patches já tem fronteiras mais bem definidas, mas com problemas nas reentrâncias
# Test_weight_path = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\bacdm_weights\vchips2020_90_FS_0103101002_LR01_alpha50_20260409234429_99.pth"
# really good model: sometimes slightly fat and not weel defined holes; trained with corrected vchips
Test_weight_path = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\bacdm\bacdm_weights\chips_b346_FS_0103101002_LR01_alpha50_20260411123748_99.pth"
Test_weight_path=r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\bacdm\bacdm_weights\341_FS_0105101002_LR01_alpha50_20260416221435_99.pth"
Test_weight_path=r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\bacdm\bacdm_weights\341_FS_0110101002_LR01_alpha50_20260417214946_99.pth"

# Input directory containing all available 16-band GeoTIFF files
working_dir = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5"
# where before and after 6-channel geo-referenced tifs are stored
Test_im_pathA = os.path.join(working_dir, "testing_data", "before")
Test_im_pathB = os.path.join(working_dir, "testing_data", "after")
# where predicted change maps will be saved (both as png and geotiff)
Test_det_path = os.path.join(working_dir, "testing_data", "predictions") # change this suffix if you want to save predictions in a different folder
# chip_source_folder = os.path.join(working_dir, 'chips')
# Temp directory to store the selected  16-band TIF files
# Input_dir = r".\chips_test\TQG_burn_area" # Dominic tests
# suffix_test_files = "TNF_BA_20241147792"
# suffix_test_files = "TNE_buf_468" # around buffer_id=... (BDR_expanded_v0)
# chip_source_folder = os.path.join(working_dir, 'chips','all')
# Input_dir =  os.path.join(working_dir, "chips", "selected", suffix_test_files)
# ICNF burned areas or another vector georeferenced file for tests:
#shp_path,DATA0 = r'H:\ref_datasets\BDR_ICNF\ardida_2024\ardida_2024.shp', "DH_Inicio" # the field name in the shapefile that contains the date information for symbology and labeling; this is used in the qgis_read_reference_BDR_expanded.py script to apply categorized symbology by month and labeling by day of month; make sure to update this field name if your shapefile has a different field for date information, and ensure that the date format in that field is compatible with the expressions used for symbology and labeling in the QGIS script
# shp_path, DATA0 =  os.path.join(working_dir, "harmonized", "BDR_expanded_v0.gpkg"), "Data0" 
# temp_raster_reference= os.path.join(working_dir, "harmonized_to_tifs") # temporary raster version of the reference vector file; this is used in the qgis_read_reference_BDR_expanded.py script to extract raster values at the locations of the reference vector features for comparison with our predictions; make sure to update this path if you have a different location for the temporary raster version of your reference vector file, and ensure that the rasterization process in the QGIS script correctly aligns with the extent and resolution of your input chips for accurate comparison
# CCD results (rasters or vectors) for comparison with our predictions
# CCD_raster_results_path = None #r"H:\new_parquets_2017_2025\tabular\T29TNF\processed_outputs\rasters" # bimonthly, for 2023 and 2024
# CCD_vector_results_path = r"H:\new_parquets_2017_2025\tabular\T29TNF\processed_outputs\vectors" # bimonthly, for 2023 and 2024



