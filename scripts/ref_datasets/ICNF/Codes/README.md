# ICNF Processing Codes

This folder contains the Python code used to process and harmonize the ICNF reference dataset.

The code is organized into:

* `core/`: shared spatial-processing functions, including reprojection and topology operations;
* `pipelines/`: main processing and harmonization workflows;
* `runners/`: executable scripts used to run each processing stage;
* `utils/`: supporting utilities, including text normalization.

The ICNF workflow is divided into sequential processing stages, including topology correction, text normalization, spatial attribution, and generation of the final harmonized layers for the annual ICNF datasets.
