Overview:
- [Awesome Remote Sensing Foundation Models](https://github.com/Jack-bo1220/Awesome-Remote-Sensing-Foundation-Models?tab=readme-ov-file#relevant-projects). Links to many foundation models for RS, and in particular *Remote Sensing Vision Foundation Models*.

Some GFM for EO
- masked autoencoder-based models
   1. [Clay v1.5](https://clay-foundation.github.io/model/index.html). Clay Foundation Model: An open source AI model for Earth. *Clay v1.5 is our masked autoencoder-based model designed to handle inputs from a variety of satellite sensors, including Sentinel-2, Landsat, Sentinel-1 SAR, LINZ, NAIP and MODIS. It supports inputs of any size and any number of bands.*
   2. [SatMAE](https://sustainlab-group.github.io/SatMAE/).  Pre-training framework for temporal or multi-spectral satellite imagery based on Masked Autoencoder (MAE).
- segmentation (?)
   1. [SpectralGPT](https://github.com/danfenghong/IEEE_TPAMI_SpectralGPT). Spectral Remote Sensing Foundation Model. 1) accommodates input images with varying sizes, resolutions, time series, and regions in a progressive training fashion, enabling full utilization of extensive RS big data; 2) leverages 3D token generation for spatial-spectral coupling; 3) captures spectrally sequential patterns via multi-target reconstruction; 4) trains on one million spectral RS images, yielding models with over 600 million parameters. Our evaluation highlights significant performance improvements with pretrained SpectralGPT models, signifying substantial potential in advancing spectral RS big data applications within the field of geoscience across four downstream tasks: single/multi-label scene classification, semantic segmentation, and change detection.
   3. Dynamic One-For-All foundation model for Remote sensing and Earth observation [DOFA](https://github.com/zhu-xlab/DOFA/blob/master/README.md). Goal: object detection and instance segmentation
   4. [GFM](https://github.com/mmendiet/GFM). Towards Geospatial Foundation Models via Continual Pretraining. *Our approach outperforms previous state-of-the-art geospatial pretraining methods in an extensive evaluation on seven downstream datasets covering various tasks such as change detection, classification, multi-label classification, semantic segmentation, and super-resolution.*
   5. [MSR-BACD](https://zenodo.org/records/15336666). Goal: change detection caused by burned areas. Input: 12 S2 bands (before+after)
   6. IBM-NASA Prithvi Models Family at https://huggingface.co/ibm-nasa-geospatial
      - Burned scars model: [Prithvi-EO-2.0-300M-BurnScars](https://huggingface.co/ibm-nasa-geospatial/Prithvi-EO-2.0-300M-BurnScars). Input: 6 S2 bands (after)
   7. A global Swin-Unet Sentinel-2 surface reflectance-based cloud and cloud shadow detection algorithm for the NASA Harmonized Landsat Sentinel-2 (HLS) dataset: https://www.sciencedirect.com/science/article/pii/S2666017225000197
   8. [SATLAS](https://allenai.org/blog/satlaspretrain-models-foundation-models-for-satellite-and-aerial-imagery-1679ebe4bbfb) SatlasPretrain Models: foundation models for satellite and aerial imagery

Data sets
- [Eurosat](https://github.com/phelber/EuroSAT). EuroSAT: Land Use and Land Cover Classification with Sentinel-2

Papers:
- Transfer learning in environmental remote sensing, RSE, 2024: https://www.sciencedirect.com/science/article/pii/S0034425723004765#s0005 . Review paper; lists some FM for remote sensing
- TRANSFORMER MODELS FOR MULTI-TEMPORAL LAND COVER CLASSIFICATION USING REMOTE SENSING IMAGES: https://isprs-annals.copernicus.org/articles/X-1-W1-2023/981/2023/isprs-annals-X-1-W1-2023-981-2023.pdf . Uses Swin Transformer
- Martins, V. S., Roy, D. P., Huang, H., Boschetti, L., Zhang, H. K., & Yan, L. (2022). Deep learning high resolution burned area mapping by transfer learning from Landsat-8 to PlanetScope. Remote Sensing of Environment, 280, 113203. https://doi.org/10.1016/j.rse.2022.113203


