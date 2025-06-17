# Apresentações no quadro do projeto

-  (20 fev 2024) [Base Dados Referência NVG](Apresentacao_BD_NVG_IS_20fev.pdf). Descrição e organização das tabelas da BDR NVG original.
- (20 fev 2024)  [Deteção de alterações com CCD](PPT_CCD_20fev.pptx)
- (23 maio 2024) [PyCCD: Análise do desempenho e dos tempos de computação](presentations/PPT_CCD_23maio2024.pdf). Discussão de diferentes formatos de input/output para uma máquina local (**não HPC**). Componentes do pyCCD mais exigentes em recursos computacionais. Estratágias para reduzir o tempo de computação (leitura dados, LASSO, ...)
- (23 de maio de 2024) [Melhoramento da BDR NVG usando informação espetral](Apresentacao_DatasCorte_DGT_23maio2024.pdf)  Implementação de técnicas baseadas nas quedas médias de NVDI em cada sub-talhão (posteriormente, o trabalho evoluiu para análise ao nível do pixel).
- (4 de julho de 2024) [Reunião DGT/INCD/LIP](reuniao_DGT_4_julho_2024.pdf). Descrição do problema de criação do produto de perdas de vegetação: dados de referência, dados de satélite, algoritmos (CCD, étapas de processamento), estimativa de recursos computacionais
- (18 de junho de 2025) Reunião com DGT/CNCA:
  
  1. Tópicos:
  - Descrição do problema: deteção de alterações, classificação, validação. Ver [diagrama](https://ulisboa-my.sharepoint.com/:p:/r/personal/mlc_office365_ulisboa_pt/_layouts/15/Doc.aspx?sourcedoc=%7B8D41864A-55FD-482B-AC2B-518CFB2E24A6%7D&file=overview_s2change.pptx&action=edit&mobileredirect=true)
  - (nov 2024) [Relatório sobre Processamento do PyCCD em plataforma de computação avançada (INCD)](../reports_sub_contracts/Entregavel_2.3.pdf)
  - (fev 2025) [Relatório sobre Processamento do PyCCD em plataforma de computação avançada (MACC Deucalion)](../reports_sub_contracts/Entregavel_3.1.pdf)
  - Teste comparativo entre MACC e INCD com as configurações de desempenho ideais para cada plataforma (p/ 1 milhão de pixels):
    ![image](https://github.com/user-attachments/assets/902356a0-bcb9-403f-95d8-2aacd3424379)
    ![image](https://github.com/user-attachments/assets/8be68233-9168-42fe-b86c-91d99266a37c)



  - Recursos computacionais (INCD) para a componente de deteção de alterações com pyCCD. Sentinel-2 tile processing status: see [status per Sentinel-2 tile](https://ulisboa-my.sharepoint.com/my?csf=1&web=1&e=vp9h5C&FolderCTID=0x012000C2AFBA48F7C2154CB26FDFA64A376290&id=%2Fpersonal%2Fmlc%5Foffice365%5Fulisboa%5Fpt%2FDocuments%2FDocuments%2Finvestigacao%2Dprojectos%2Dreviews%2Dalunos%2Djuris%2Fprojetos%2FDGT%2DS2CHANGE%5F2023%2Fpartilhado%2Flog%5Fincd%5Fmacc) incluiding size (# pixels), memory, core-minutes in INCD, output parquet file size.


  2. Questões:
  - acesso a recursos no CNCA
  - deep learning para classificação: recuros em GPU
