UPDATE THIS README (feb 2026): the main functionality was moved to `chips`?

Move `B2B11_extract_raster.py`to `chips`folder

# Scripts to explore data sets 

## Data sets (see information in \data_info)
- Reference data 
- Sentinel-2 data 
- pyCCD ouputs 


## Scripts:

<details>
  <summary>Create S2 temporal composite (12 bands + date): extract_B2B11_start_end.py</summary>

Script (scripts/data_exploration/extract_B2B11_start_end.py)  is intended to extract the spectral values before and after the most recent break date identified by pyccd.
It uses the end date of the second to last segment and the start date of the last segment to look up for the bands values.

Currently, the script collects band data at two stages: first from the B2 and B11 bands (Blue and SWIR1) and then from the 
original 4 bands with which pyccd was executed.

Note: this script should be an improvised fix to acquire the band values; a more definitive solution should include
acquiring B2 and B11 data as part of the pyccd processing.

</details>

<details>
  <summary>Extraction of 2N observations around the reference data change date (for quality control)</summary>

#### Inputs
    - Reference data: geopackage; shapefile
    - sentinel-2 bands: hdf5
    
#### Output: parquet

#### Description: Os dados estão organizados por pixel com base nas geometrias de entrada (reference_data), extraindo séries temporais de observações antes e depois da data de quebra (ou da média entre duas datas -- data_0 e data_1 --, se aplicável).
- Para cada pixel, são guardadas:
    * As N observações anteriores e N posteriores à data central (data_mid);
    * Os valores das bandas: g (green), r (red), n (near-infrared), s (SWIR);
    * As datas correspondentes a essas observações.

#### Organização das colunas no dataset
* g_a1 até g_aN: valores da banda g antes da data de quebra (o sufixo _a indica "antes"); a coluna g_a10 corresponde à observação mais próxima da data de quebra — podendo até ser a própria data, caso haja correspondência;
* g_d1 até g_dN: valores da banda g depois da data de quebra (o sufixo _d indica "depois"); a coluna g_d1 representa a primeira observação após a quebra;
* A mesma lógica aplica-se às outras bandas: r, n e s;
* dts_a1 até dts_a10: datas anteriores à quebra, com dts_a10 sendo a data imediatamente anterior (ou igual) à data de quebra;
* dts_d1 até dts_d10: datas posteriores à quebra, com dts_d1 sendo a data imediatamente seguinte.

#### Localização dos datasets para cada BDR
    -> BDR DGT (PC ISA: C:/ref_datasets/amostras_por_pixel/BDR DGT)
    -> BDR NVG (PC ISA: C:/ref_datasets/amostras_por_pixel/BDR NVG)
    -> BDR ICNF (PC ISA: C:/ref_datasets/amostras_por_pixel/BDR ICNF)

</details>

---
