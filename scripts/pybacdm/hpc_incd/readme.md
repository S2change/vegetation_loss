# BACDM files on INCD

## Files to copy to HPC — keep this exact structure:

```
<deploy_root>/
├── AAA_Configs.py
└── bacdm/
    ├── predict.py
    ├── swin_ynet.py
    ├── YTYAttention.py
    └── data/
        └── dataset_swin_GZ.py
```

Plus the `.pth` weights file anywhere accessible (path is passed at runtime).

## Why this structure matters

- `swin_ynet.py` contains from `bacdm.YTYAttention import *`. 
- `predict.py` adds `<deploy_root>/ to sys.path`, so Python resolves bacdm as the subfolder — `YTYAttention` is found there.
- `AAA_Configs` is found directly in `<deploy_root>/`. If you flatten everything into one folder that import breaks.

## Python packages to install on HPC:

```
torch
torchvision
numpy
rasterio
einops        # used by swin_ynet.py
timm          # used by swin_ynet.py (DropPath, trunc_normal_)
```

## Minimal requirements.txt:

```
torch>=2.0
torchvision>=0.15
numpy
rasterio
einops
```
timm

