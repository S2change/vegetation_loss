# NVG Processing Codes

This folder contains the Python code used to process and harmonize the NVG reference dataset.

The code is organized into:

* `pipelines/`: main processing and harmonization workflows;
* `runners/`: executable scripts used to run each processing stage;
* `utils/`: supporting utilities, including text normalization;
* `legacy/`: previous processing scripts retained for traceability but not used in the current workflow.

The NVG workflow is divided into sequential processing stages, from the preparation and validation of the source data to the generation of the final harmonized layer.
