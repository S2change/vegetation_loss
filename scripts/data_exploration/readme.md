# Scripts to explore data sets 

## Data sets (see information in \data_info)
- Reference data 
- Sentinel-2 data 
- pyCCD ouputs 


## Scripts:

### Extraction of 2N observations around the reference data change date (for quality control)

<details>
  <summary>(Extraction_S2_2N_observations)</summary>

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
