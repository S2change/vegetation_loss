# Apresentações e relatórios externos informais

## 2026

- 23 de fevereiro de 2026 (reunião DGT/ISA: ponto situação Hugo Costa, Mário Caetano, Manuel Campagnolo)

  [Apresentação (Power Point)](https://ulisboa-my.sharepoint.com/:p:/g/personal/mlc_office365_ulisboa_pt/IQDURNhbmW2SQrFi8nPL0tsBAfloWxbFupcqW0wXhLyQJPk?e=cvkeCb)

- 11 de março de 2026 (reunião DGT/ISA/LIP/CNCA) on-line

  [Apresentação (Power Point)](https://ulisboa-my.sharepoint.com/:p:/g/personal/mlc_office365_ulisboa_pt/IQA5M3rzjtdETb9qQKzFzQOqAcT7_hqvMQwoyf91kM8beLM?e=i59Wiq)

  <details>
    
    <summary>Agenda and actions</summary>

    ## Agendas Discussed
    
    - **INCD → CNCA transition**: INCD legally renamed to CNCA; HPC platform Cirrus unchanged; LIP continues HPC support
    - **CCD algorithm (Approach A) review**: Low false detection but high omission; unsuitable as primary detection method; useful for building reference dataset
    - **Deep Learning model (Approach B)**: Pre-trained foundation model with fine-tuning; preliminary results show cleaner change patches than CCD; training on Deucalion cluster (Calient)
    - **DL processing pipeline**: Sentinel-2 images → spatial chips (overlapping before/after pairs) → model outputs change probability → spatial & temporal aggregation → final binary map
    - **HDF5 role**: Retained as key intermediate format for fast chip generation; persistent HDF5 per tile (Option B) preferred over on-the-fly creation
    - **Sentinel-2 data pipeline (LIP/CNCA)**: Full 2015–2025 time series processed; 10 bands; cloud masking via Omnicloud; data served via STAC Catalog on S3
    - **Tile overlap handling**: Clipping tile boundaries agreed for current phase; focus on largest tile TNE first
    - **DGT mask strategy**: Prefer applying masks at processing stage rather than embedding in shared HDF5
    - **GPU on Cirrus**: DL inference needs GPUs but modest memory; strategy is to parallelise many chips
    - **GDAL vs xarray+HDF5**: Current xarray+HDF5 stack kept; no time to revisit
    - Both ISA and LIP contracts end ~June 2026
    
    ---
    
    ## Action Items
    
    - [ ]  **ISA** — Share HDF5 creation code (GitHub repo) with Jorge, CC: Pedro, Ricardo, Gonçalo
    - [ ]  **LIP** — Review HDF5 code; assess feasibility of persistent update/append functionality
    - [ ]  **LIP**— Confirm final HDF5 strategy (persistent vs on-the-fly) and communicate to ISA → *ASAP*
    - [ ]  **LIP** — Make Sentinel-2 time series (2015–2025, 10 bands) available via STAC Catalog with cloud metadata
    - [ ]  **ISA** — Continue fine-tuning DL model; target model ready → *End April 2026*
    - [ ]  **ISA + LIP** — Work in parallel on chip generation pipeline on Cirrus; discuss storage for temp chip files
    - [ ]  **ISA + LIP** — Produce joint written roadmap of remaining tasks, owners, and schedule → *25 March 2026*

    </details>

- 25 de março de 2026 (reunião DGT/LIP) on-line

  - [Relatório sumário](reuniao_ISA_LIP_25_marco_2026.md)
  - [Detalhes](https://www.notion.so/DGT-ISA-LIP-Technical-Session-HDF5-Code-Walkthrough-25-March-2026-32e70b5874e5812da74ddffdb5edb17e)

## 2025

- (18 de junho de 2025) Reunião com DGT/CNCA: [Presentation (pdf)](Reuniao_DGT_CNCA_ISA_18_junho_2025.pdf)

    <details>
    
    <summary>Tópicos e questões</summary>
    
      1. Tópicos:
      - Descrição do problema: deteção de alterações, classificação, validação. Ver [diagrama](https://ulisboa-my.sharepoint.com/:p:/r/personal/mlc_office365_ulisboa_pt/_layouts/15/Doc.aspx?sourcedoc=%7B8D41864A-55FD-482B-AC2B-518CFB2E24A6%7D&file=overview_s2change.pptx&action=edit&mobileredirect=true)
      - (nov 2024) [Relatório sobre Processamento do PyCCD em plataforma de computação avançada (INCD)](../reports_sub_contracts/Entregavel_2.3.pdf)
      - (fev 2025) [Relatório sobre Processamento do PyCCD em plataforma de computação avançada (MACC Deucalion)](../reports_sub_contracts/Entregavel_3.1.pdf)
      - Teste comparativo entre MACC e INCD com as configurações de desempenho ideais para 1 milhão de pixels:
        ![image](https://github.com/user-attachments/assets/902356a0-bcb9-403f-95d8-2aacd3424379)
        ![image](https://github.com/user-attachments/assets/8be68233-9168-42fe-b86c-91d99266a37c)
    
        **Nota**: No INCD, a paralelização dos batches dentro de cada rank (usando ProcessPoolExecutor) não trouxe ganhos significativos de desempenho (testou-se com 5 nodes, 24 ntasks per node e 4 cpus per task e ficou + lento), ao passo que no MACC, este método resultou numa melhoria notável no tempo de execução devido à maior capacidade de CPU por node.
    
        ----> MACC 4x + rápido que INCD (para 1 milhão de pixels).
    
      - Recursos computacionais (INCD) para a componente de deteção de alterações com pyCCD. Sentinel-2 tile processing status: see [status per Sentinel-2 tile](https://ulisboa-my.sharepoint.com/my?csf=1&web=1&e=vp9h5C&FolderCTID=0x012000C2AFBA48F7C2154CB26FDFA64A376290&id=%2Fpersonal%2Fmlc%5Foffice365%5Fulisboa%5Fpt%2FDocuments%2FDocuments%2Finvestigacao%2Dprojectos%2Dreviews%2Dalunos%2Djuris%2Fprojetos%2FDGT%2DS2CHANGE%5F2023%2Fpartilhado%2Flog%5Fincd%5Fmacc) incluiding size (# pixels), memory, core-minutes in INCD, output parquet file size.
    
        ----> Resources still available in INCD: 12,674,231.63 cores-minutes
    
      2. Questões:
      - acesso a recursos no CNCA
      - deep learning para classificação: recursos em GPU
    
    </details>

## 2024

-  (20 fev 2024) [Base Dados Referência NVG](Apresentacao_BD_NVG_IS_20fev.pdf). Descrição e organização das tabelas da BDR NVG original.
- (20 fev 2024)  [Deteção de alterações com CCD](PPT_CCD_20fev.pptx)
- (23 maio 2024) [PyCCD: Análise do desempenho e dos tempos de computação](presentations/PPT_CCD_23maio2024.pdf). Discussão de diferentes formatos de input/output para uma máquina local (**não HPC**). Componentes do pyCCD mais exigentes em recursos computacionais. Estratágias para reduzir o tempo de computação (leitura dados, LASSO, ...)
- (23 de maio de 2024) [Melhoramento da BDR NVG usando informação espetral](Apresentacao_DatasCorte_DGT_23maio2024.pdf)  Implementação de técnicas baseadas nas quedas médias de NVDI em cada sub-talhão (posteriormente, o trabalho evoluiu para análise ao nível do pixel).
- (4 de julho de 2024) [Reunião DGT/INCD/LIP](reuniao_DGT_4_julho_2024.pdf). Descrição do problema de criação do produto de perdas de vegetação: dados de referência, dados de satélite, algoritmos (CCD, etapas de processamento), estimativa de recursos computacionais


