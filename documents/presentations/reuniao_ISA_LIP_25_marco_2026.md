# Informação sobre organização dos dados no CNCA

Presentes: Manuel (ISA), Gonçalo (LIP), Danilo (ISA)

## Relatório

Como os ficheiros GeoTIGFF das imagens Sentinel-2 (MSIL2A) estão organizadas no CNCA. 

Irei então rever o script que faz o "parsing" dos nomes dos ficheiros em https://github.com/S2change/vegetation_loss/tree/main/scripts/data_exploration/tifs_to_hdf5_to_tifs/CNCA_tifs_to_hdf5 para adaptar à estrutura de ficheiros no CNCA (ver abaixo). Informarei quando a versão revista do script para fazer o parsing estiver pronta para poderem então aplicar e testar.  

Entretanto, já verifiquei que posso extrair o "timestamp" (em milissegundos) simplesmente a partir do nome do ficheiro GeoTIFF e por isso não é preciso ir consultar o ficheiro de metadados.

O objetivo é criar com 'create_hdf5.py'  um único ficheiro hdf5 por tile Sentinel-2, cobrindo todo o território de Portugal Continental, com um buffer de 2 km que o CNCA já usou para descarregar os GeoTiffs. Esse ficheiro único deve conter os dados pra a tile  e para a totalidade dos anos.  Depois será preciso testar a funcionalidade de 'append_hdf5.py' para adicionar ao ficheiro hdf5 a informação de geotiffs que sejam entretanto descarregadas, sem haver repetições de 'timestamps'.

A previsão é que este processo esteja concluido até 1/4/2026.

Este email serve de relatório de atividades que a DGT pediu até 25/3/2026

## Metados Sentinel-2 (parte)

  ```
  <PRODUCT_START_TIME>2024-02-08T11:22:31.024Z</PRODUCT_START_TIME>
  <PRODUCT_STOP_TIME>2024-02-08T11:22:31.024Z</PRODUCT_STOP_TIME>
  <PRODUCT_URI>S2A_MSIL2A_20240208T112231_N0510_R037_T29TQF_20240208T165452.SAFE</PRODUCT_URI>
  <PROCESSING_LEVEL>Level-2A</PROCESSING_LEVEL>
  <PRODUCT_TYPE>S2MSI2A</PRODUCT_TYPE>
  <PROCESSING_BASELINE>05.10</PROCESSING_BASELINE>
  <PRODUCT_DOI>https://doi.org/10.5270/S2_-znk9xsj</PRODUCT_DOI>
  <GENERATION_TIME>2024-02-08T16:54:52.000000Z</GENERATION_TIME>
  <PREVIEW_IMAGE_URL>Not applicable</PREVIEW_IMAGE_URL>
  <PREVIEW_GEO_INFO>Not applicable</PREVIEW_GEO_INFO>
  <Datatake datatakeIdentifier="GS2A_20240208T112231_045079_N05.10">
  <SPACECRAFT_NAME>Sentinel-2A</SPACECRAFT_NAME>
  <DATATAKE_TYPE>INS-NOBS</DATATAKE_TYPE>
  <DATATAKE_SENSING_START>2024-02-08T11:22:31.024Z</DATATAKE_SENSING_START>
  <SENSING_ORBIT_NUMBER>37</SENSING_ORBIT_NUMBER>
  <SENSING_ORBIT_DIRECTION>DESCENDING</SENSING_ORBIT_DIRECTION>
  ```

