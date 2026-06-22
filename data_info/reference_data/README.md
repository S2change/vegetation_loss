# Reference data

This folder contains the reference datasets used in the project. The available reference layers are briefly described below.

## Harmonized reference data

The harmonized versions of BDR-TNE-300, BDR-TNE-300-Expanded, ICNF, and NVG are documented in:

`Harmonized/README.MD`

## Legacy data sets 

### BDR-TNE-300

BDR-TNE-300 is a reference dataset used to assess vegetation-cover disturbances detected through the BDR/CCDC workflow.

The dataset contains spatial units associated with detected or evaluated changes and provides information about disturbance occurrence, temporal characteristics, land-cover classes, and disturbance type.

It is used as a reference source for validating and comparing vegetation-change information across the project.

### BDR-TNE-300-Expanded

BDR-TNE-300-Expanded is an expert-based reference dataset developed to complement the automated disturbance information provided by `BDR_CCDC_TNE_v3`.

The dataset extends the spatial context of selected BDR-TNE-300 units and supports detailed visual interpretation of vegetation-cover changes. Its main purposes are to:

* validate ambiguous disturbance cases;
* refine the timing of detected changes;
* identify the disturbance type;
* assign pre-disturbance and post-disturbance land-cover classes;
* document situations in which the CCDC date does not represent the first visible onset of change.

The interpretation was based on the combined use of:

* multi-temporal Sentinel-2 imagery;
* monthly Sentinel-2 mosaics;
* high-resolution orthophotos;
* COS land-cover information;
* ICNF burned-area information;
* CCDC outputs;
* Google Earth imagery when additional validation was required.

The reviewed reference layer includes interpreted temporal fields such as `Data_0` and `Data_1`, disturbance information, class attribution, and expert notes.

#### Examples

The figures illustrating the BDR-TNE-300-Expanded photointerpretation cases are available in:

`Harmonized/Images/Images_BDR_expanded/README.md`

### ICNF

The ICNF reference data comprise spatial information obtained from the Instituto da Conservação da Natureza e das Florestas.

These layers provide official information related to forest and vegetation events and are used to support the identification, interpretation, and validation of vegetation-cover changes.

The ICNF datasets complement the other reference sources by providing independent information about mapped forest disturbances and related events.

### NVG

NVG is a reference dataset used to represent and validate vegetation-cover change information produced through the NVG workflow.

The dataset contains spatial and temporal information associated with vegetation changes and is used to compare, confirm, and contextualize change information derived from the other project layers.


<<<<<<< HEAD
The harmonized versions of BDR-TNE-300, BDR-TNE-300-Expanded, ICNF, and NVG are documented in:

`Harmonized/README.MD`

=======
>>>>>>> f0ff7609390503001a9b6013df43a902db71b74c
