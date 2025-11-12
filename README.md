# S2CHANGE 

**Desenvolvimento de mapas de perdas recentes de floresta e mato em Portugal derivados de imagens de satélite**

Contrato N.º 3044 de cooperação entre a Direção Geral do Território e o Instituto Superior de Agronomia

## Referências

<details markdown="block">
<summary> Descrição do projeto e referências</summary>

* Gestor do Contrato: Hugo Costa, DGT
* Responsável pela execução: Manuel Campagnolo, CEF, DCEB, ISA
* Procedimento CEXC/2152/2023
* O contrato tem enquadramento no subprojecto «P1.5- Dados de deteção remota para a gestão
florestal» (DRFloresta) do projeto Agenda Transform (Agenda para a transformação digital das
cadeias de valor florestais numa economia portuguesa mais resiliente e hipocarbónica), apoiado
pelo Plano de Recuperação e Resiliência (PRR), sob o cabimento n.º CI42300913 e compromisso n.º
CI52301229
* O contrato tem por objecto a realização de atividades de investigação e
desenvolvimento (I&D) para o desenvolvimento de metodologias eficazes à escala nacional e
eficientes a nível computacional para a criação sistemática de um produto nacional em formato
vetorial de delimitação de manchas superiores a 0.5 ha de perda recente de floresta e mato com
base em análise automática de imagens de satélite
* Data de início: contrato assinado a 20 de outubro de 2023; inicial data de vigência: 19/10/2025; aditamento: 31/03/2026
* Centro de custos do ISA: 5207 (S2CHANGE)
</details>

## Apresentações e recursos HPC

<details markdown="block">
<summary> Reunião DGT 20 fevereiro de 2024</summary>

* Apresentação Sara Caetano. Resultados que permitem comparar a aplicação da metodologia de deteção de alterações (CCD), com o algoritmo Python pyccd, a imagens Sentinel-2 obtidas no GEE (com máscara de nuvens produzida pelo algoritmo S2cloudness) com as imagens Sentinel-2 préprocessadas pela Theia: [ficheiro powerpoint](documents/presentations/PPT_CCD_20fev.pptx)
* Apresentação Inês Silveira sobre a base de dados de referência Navigator; análise em particular da distribuição de datas de cortes dentro do mesmo talhão e da possibilidade de associar um sub-talhão a cada data de corte; análise preliminar sobre a possibilidade de associar uma alteração de sinal a operações de rechega e outras [ficheiro pdf](documents/presentations/Apresentacao_BD_NVG_IS_20fev.pdf)

</details>

<details markdown="block">
<summary> Reunião DGT 23 de maio de 2024</summary>

* Apresentação Sara Caetano [ficheiro pdf](documents/presentations/PPT_CCD_23maio2024.pdf)
* Apresentação Inês Silveira [ficheiro pdf](documents/presentations/Apresentacao_DatasCorte_DGT_23maio2024.pdf)

</details>

<details markdown="block">
<summary> Reuniões DGT/INCD/LIP</summary>

* [Apresentação 4/7/2024](documents/presentations/reuniao_DGT_4_julho_2024.pdf)
* [Apresentação 18/6/2025](documents/presentations/Reuniao_DGT_CNCA_ISA_18_junho_2025.pdf)

</details>


<details markdown="block">
<summary> HPC resources</summary>

* See [HPC resources](documents/HPC_resources) 
</details>

## Tarefas

**Tarefa 1** – Seleção e justificação das metodologias a operacionalizar nas Tarefas 2, 3 e 4, dados de input, especificações técnicas dos outputs, e potenciais adaptações tecnológicas a implementar na cadeia de produção da DGT.

*Duração: Mês 1-6*

entregáveis:
  * E1.1 – [Relatório com a descrição do problema, condicionantes, dados de input e especificações técnicas dos outputs](documents/deliverables/Entregavel_1_1.pdf) (10 de dezembro de 2023).
  * E1.2 – [Relatório com seleção e justificação das metodologias a operacionalizar](documents/deliverables/Entregavel_1_2.pdf) (1 de maio de 2024); [versão revista](documents/deliverables/Entregavel_1_2_v2.pdf) (14 de maio de 2024)
  * E1.3 – [Relatório sobre potenciais adaptações tecnológicas a implementar na cadeia de produção da DGT](documents/deliverables/entregavel_1.3_v3.pdf) (5 de julho de 2024).

**Tarefa 2** - Construção da uma base de dados de referência (BDR) para calibração e validação espacial e temporal das metodologias a operacionalizar com base em dados resultantes de interpretação de imagens aéreas e de satélite, do Instituto de Conservação da Natureza e Florestas (ICNF) e outras fontes consideradas relevantes.

*Duração: Meses 2-18* 

entregáveis:  
  * E2.1 – [Relatório com metodologia de criação da BDR](documents/deliverables/Entregavel_2_1.pdf) (1 de maio de 2024); [versão revista](documents/deliverables/Entregavel_2_1_v2.pdf) (14 de maio de 2024)
  * E2.2 – [Metadados da base de dados de referência em formato ESRI shapefile ou Geopackage para uma tile Sentinel-2 sobre Portugal Continental](documents/deliverables/Entregavel_2_2_BDR_navigator_sentinel2_metadados_v4.pdf) (7 de julho de 2024).
  * E2.3 – Extensão da base de dados para outras regiões de Portugal Continental. Ver [BRD_NVG](data_info/reference_data/NVG) para acesso aos dados (protegido) e à documentação.

**Tarefa 3** – Adaptação e implementação operacional de uma metodologia automática com base em imagens de satélite para a criação sistemática de um produto nacional de delimitação de manchas vetoriais superiores a 0.5 ha de perda recente de floresta e mato, com uma periodicidade de pelo menos dois meses.

*Duração: Meses 9-20*

entregáveis:
  * E3.1 – [Manual de utilização operacional da metodologia implementada na cadeia de produção da DGT](documents/deliverables/Entregavel_3.1_v3.pdf) (20 de outubro de 2024).
  * E3.2 – Demonstrador prático: mapas nacionais vetoriais a delimitar manchas de perda recente de floresta e mato superiores a 0.5 ha com uma frequência bimestral relativos a um período contínuo de dois anos entre 2023 e 2025.

    Mapas vectoriais bimestrais para os anos 2023 e 2024 foram elaborados como resultado de processamento de séries temporais Sentinel-2 através do algoritmo CCD para Portugal Continental e para a máscara de potenciais perdas de vegetação fornecida pela DGT (aprox. 500 M pixels Sentinel-2 de resolução 10 m). O processamento foi realizado em ambiente HPC usando a plataforma INCD/CNCA/LIP (Cirrus). Os mapas (em formato shapefile) estão disponível neste [link](data_info/vegetation_loss_products) (requer password). (30 de junho 2025)
  * E3.3 – Relatório de validação dos mapas nacionais.
  
    Foram feitos dois exercícios de validação descritos abaixo. (30 de junho 2025)
    - Usando [BDR_DGT_300](data_info/reference_data/BDR_DGT_300) e os mapas bimestrais de perdas de vegetação. Ver [script](scripts/validation/raster_avaliacao_exatidao.py) e [relatório E3.3A](documents/deliverables/Entregavel_3_3A_validacao_ccd_mbpv_v0_dgt300_report_v1.md).
    - Fazendo uma comparação para os mapas bimestrais de perdas de vegetação para 2023-2024 com as áreas ardidas [ICNF](data_info/reference_data/ICNF) para Portugal Continental. Ver [script](scripts/validation/validate_ccd_against_icnf.py) e [relatório E3.3B](documents/deliverables/Entregavel_3_3B_validacao_ccd_mbpv_v0_icnf_v1.pdf).
  * E3.4 – Aplicação informática que possa ser integrada na cadeia de produção da DGT.

    Os scripts (que foram desenvolvidos para serem compatíveis com o pipeline de processamento de dados Sentinel-2 em ambiente HPC) estão disponíveis neste repositório. Em particular, foi criado e testado o código para as  tarefas abaixo. (30 de junho 2025)
    - Processamento da série temporal para Portugal Continental 2017-2024 e aplicação do algoritmo de deteção de alteração CCD, cujo output é um conjunto de ficheiros em formato `parquet` em que cada linha corresponde a um pixel Sentinel-2 e a um segmento identificado pelo algoritmo. O processamento foi aplicado aos pixels pertencentes à máscara de potenciais perdas de vegetação para Portugal Continental fornecida pela DGT (aprox. 500 M pixels Sentinel 2 com resolução 10 m). [Script](scripts/pyccd)
    - (*) Conversão dos resultado os formato `parquet` para mapas bimestrais em formato `geotiff`. Cada pixel agora possui quatro bandas: [Script](scripts/visualisations/ccd_to_raster.py)
      - `last_tEnd`: data final do segmento antes da quebra.
      - `last_tBreak`: data da quebra mais recente.
      - `is_break`: indicador booleano que sinaliza se ocorreu uma alteração: `1` se for uma quebra conhecida, `0` se não houve alteração, `-1` se não for possível determinar se ocorreu uma quebra ou um aumento (casos em que o segmento pós-quebra não foi formado).
      - `ndvi_last_tEnd`: valor do NDVI calculado em last_tEnd.
    - (**) Criação de mapas vectoriais bimestrais a partir dos mapas raster. [Script](scripts/visualisations/graph_raster_to_polygons.py)
    - (***) Criação dos mapas nacionais bimestrais de perdas de vegetação. [Script](scripts/visualisations/ccd_polygons_to_national_maps.py)
    - Nota: os dois últimos processamentos (*) e (**) estão combinados num único script por conveniência. [Script](scripts/visualisations/graph_raster_to_polygons.py).
    - Validação: ver link em E3.3
    

**Tarefa 4** – Adaptação e implementação operacional na cadeia de produção da DGT de uma metodologia automática com base em imagens de satélite para a identificação sistemática do agente causador das perdas recentes de floresta e mato delimitadas no produto da tarefa 3, com uma periodicidade de pelo menos dois meses.

*Duração: Meses 13-24*

entregáveis:
  * E4.1 – Manual de utilização operacional da metodologia implementada na cadeia de produção da DGT.
  * E4.2 – Demonstrador prático: mapas nacionais a identificar o agente causador das perdas recentes de floresta e mato superiores a 0.5 ha produzidas na tarefa 3.
  * E4.3 – Relatório de validação dos mapas nacionais.
  * E4.4 – Aplicação informática que possa ser integrada na cadeia de produção da DGT. 

## Aditamento (set 2025)

- Entregáveis E2.3, E3.2, E3.3 e E4.1; contrato 20/4/2025; aditamento: 20/11/2025 (10% do calor contratado)
- Entregáveis E3.4, E4.2, E4.3 e E4.4; contrato 19/10/2025; aditamento: 19/03/2026 (10% do calor contratado)

Ver [cronograma](cronograma_aditamento_contrato_set_2025.png).

## Produtos

<details markdown="block">
<summary> Bases de dados de referência e produtos cartográficos de perdas de vegetação </summary>

* [Link](data_info)

</details>


