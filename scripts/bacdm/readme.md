# Files from BACDM, adapted

1. AAA_Configs.py is at the root
2. Test.py and MYTestData are adapted so the output of test can be a tif file (not just png)
3. Inputs labels are now png with values 0 (no change), 1, 2, ... 
4. Number of classes and weights are constants in `AAA_Configs.py`

# References
1. Code for the BACDM model is from https://zenodo.org/records/15788378, with the corresponding paper "Faster, better, and more accurate mapping of burned areas using Sentinel-2 multispectral images" by Liu et al. (https://doi.org/10.1016/j.rse.2025.115137).
2. Swin Transformer. Swin Transformer: Hierarchical Vision Transformer using Shifted Windows`  (https://arxiv.org/pdf/2103.14030)

## Main model
1. swin_ynet.py
2. YTYAttention.py

## Test (predict)
1. test.py
2. For weights, see \logs

## Train 
1. train.py
2. Use before, after, and label from \data
