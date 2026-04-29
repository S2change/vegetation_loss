from pathlib import Path
import os
import sys
import numpy as np

USE_CUDA = True #False

#-----------------------
# running out of VRAM or a single GPU operation taking too long. Switching from 6 → 10 bands increases per-sample memory.
batch_size = 8 #16 (original) # 28
num_workers = 16

#---- input data (CNCA)
channel_nums = 10

if channel_nums==6:
    # used in dataset_swin_GZ.py for normalization of the input images (imagenet mean and std for the 6 selected bands, calculated from the training data?) 
    normalization_mean = (0.485, 0.456, 0.406, 0.456, 0.406, 0.485)
    # (0.485, 0.456, 0.406, 0.485, 0.456, 0.406, 0.485, 0.456, 0.406, 0.485)
    normalization_std = (0.229, 0.224, 0.225, 0.224, 0.225, 0.229)
    # (0.229, 0.224, 0.225, 0.229, 0.224, 0.225, 0.229, 0.224, 0.225, 0.229)
    selected_nums = [0, 1, 2, 3, 4, 5]
    # [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
elif channel_nums==10:
    # from the original BACDM code
    normalization_mean = (0.485, 0.456, 0.406, 0.485, 0.456, 0.406, 0.485, 0.456, 0.406, 0.485)
    normalization_std = (0.229, 0.224, 0.225, 0.229, 0.224, 0.225, 0.229, 0.224, 0.225, 0.229)
    selected_nums = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
else:
    sys.exit('channel_nums is wrong')


seednumber = 42
#LearningRate = 0.01 # mc: was  0.01 # see below
EPOCH = 50 # testes #100
#----------------------- weights for loss function
ALPHA=0.45 #CE_loss vs DICE_loss weight in the overall objective function for training; you can experiment with different values for alpha to see how it affects training performance and model learning, especially in terms of how well the model learns from the imbalanced classes in your dataset; for example, if you find that the model is struggling to learn from the rarer classes, you might try increasing the weight of the dice loss (which can help with imbalanced data) by using a lower value for alpha, such as 0.5 or 0.3, to give more emphasis to the dice loss during training; conversely, if you find that the model is learning well from all classes and you want to prioritize overall accuracy, you might try a higher value for alpha, such as 0.9, to give more emphasis to the cross-entropy loss during training
BOUNDARY_LOSS_WEIGHT=0.1

# Tversky loss parameters (alpha=FN penalty, beta=FP penalty).
# TVERSKY_DEFAULT applies to every class not named in _TVERSKY_BY_NAME,
# and to all original (ungrouped) classes.
TVERSKY_DEFAULT  = (0.6, 0.4)
_TVERSKY_BY_NAME = {
    'Cuts':  (0.8, 0.2),   # recall-focused: preserve patch extent
    'Fires': (0.5, 0.5),   # symmetric Dice: model already converges well
}

# Inference threshold for the Cuts class.  Pixels where P(Cuts) exceeds this
# value are labelled Cuts even if it is not the argmax class.  Values below 0.5
# recover boundary pixels the model abandons during later training epochs.
# Set to None to fall back to plain argmax for all classes.
CUTS_THRESHOLD = 0.3

# Original class names and RGBA colors (always defined; used when CLASS_GROUPING is None)
ORIG_CLASS_NAMES  = {0: 'Background', 1: 'ClearCuts', 2: 'OtherCuts', 3: 'AgriLoss', 4: 'Fires'}
ORIG_CLASS_COLORS = {0: (0,0,0,0), 1: (255,0,0,255), 2: (0,255,0,255), 3: (0,0,255,255), 4: (255,255,0,255)}

# Class grouping: maps new_class_id -> list of original class ids.
# Set to None to train with the original 5 classes.
CLASS_GROUPING        = {0: [0, 3], 1: [1, 2], 2: [4]}  # agriculture goes to background
CLASS_GROUPING_NAMES  = {0: 'Background', 1: 'Cuts',  2: 'Fires'}
CLASS_GROUPING_COLORS = {0: (0,0,0,0),    1: (255,255,0,255), 2: (255,0,0,255)}

if CLASS_GROUPING is not None:
    NUM_CLASSES = len(CLASS_GROUPING)
    _max_orig = max(c for lst in CLASS_GROUPING.values() for c in lst)
    CLASS_REMAP = np.zeros(_max_orig + 1, dtype=np.int64)
    for _new_c, _old_list in CLASS_GROUPING.items():
        for _old_c in _old_list:
            CLASS_REMAP[_old_c] = _new_c
    CLASS_WEIGHTS = [1.0, 4.0, 2.0]   # Cuts reduced from 10→4 to stop competing with Tversky FP penalty
    CLASS_NAMES  = CLASS_GROUPING_NAMES
    CLASS_COLORS = CLASS_GROUPING_COLORS
else:
    NUM_CLASSES = 5
    CLASS_REMAP = None
    CLASS_WEIGHTS = [1.0, 4.0, 4.0, 4.0, 2.0]
    CLASS_NAMES  = ORIG_CLASS_NAMES
    CLASS_COLORS = ORIG_CLASS_COLORS

# Build {class_id: (alpha, beta)} from CLASS_NAMES and _TVERSKY_BY_NAME.
# Original classes (CLASS_GROUPING=None) all fall back to TVERSKY_DEFAULT.
if CLASS_GROUPING is not None:
    TVERSKY_PARAMS = {k: _TVERSKY_BY_NAME.get(v, TVERSKY_DEFAULT)
                      for k, v in CLASS_NAMES.items()}
else:
    TVERSKY_PARAMS = {}

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
WEIGHTS_DIR= os.path.join(working_dir, "bacdm", "bacdm_weights")
WEIGHTS_PREFIX = "240a_cutsTversky_ema_W010402_LR01_G00311224_val_pCuts30"
WEIGHTS_PREFIX = "teste"
Train_weight_path = os.path.join(WEIGHTS_DIR,WEIGHTS_PREFIX)  # OUTPUT prefix to pth and txt file names; see train.py # 权重保存的路径

# 6 bands original weights path (encoder-only pretrained weights from BACDM paper)
if channel_nums==6:
    Train_pretrained_path = os.path.join(working_dir, "logs", "B12118A432.pth")
if channel_nums==10:
    Train_pretrained_path = os.path.join(working_dir, "logs", "B12118A8765432.pth")

# Resume from a previously saved fine-tuned checkpoint (full encoder+decoder weights).
# Set to None to start fresh from Train_pretrained_path (encoder only).
# Set to a .pth path saved by train.py to continue training from that checkpoint.
Resume_checkpoint_path = None
LearningRate = 0.01
#Resume_checkpoint_path = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\bacdm\bacdm_weights\303b_10bands_softLR_0110101002_LR01_G00311224_A4510_20260423150715_68.pth"
#LearningRate = 0.01  # use a smaller LR when resuming

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
# 3 classes
Test_weight_path=r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\bacdm\bacdm_weights\173_10bands_FS_0110101002_LR01_G00311224_A4510_20260420160941_49.pth"
# 3 classes, 10 bandas
Test_weight_path=r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\bacdm\bacdm_weights\303_10bands_FS_0110101002_LR01_G00311224_A4510_20260423150715_68.pth"
Test_weight_path=r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\bacdm\bacdm_weights\303d_10b_tversky_ema_W010402_LR01_G00311224_val20260428135907_best.pth"
Test_weight_path=r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\bacdm\bacdm_weights\teste20260429163505_best.pth"

# Input directory containing all available 16-band GeoTIFF files
working_dir = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5"
# where before and after 6-channel geo-referenced tifs are stored
Test_im_pathA = os.path.join(working_dir, "testing_data", "before")
Test_im_pathB = os.path.join(working_dir, "testing_data", "after")
Val_lb_path   = os.path.join(working_dir, "testing_data", "label")   # labels for validation during training
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



