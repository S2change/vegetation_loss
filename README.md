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

## Equipa e recursos HPC

<details markdown="block">
<summary> Equipa </summary>

- Sara Caetano
- Manuel Campagnolo (coordenação)
- Jesus Céspedes
- Daniel Moraes
- António Sequeira
- Inês Silveira
- Dominic Welsh

</details>

<details markdown="block">
<summary> HPC resources</summary>

* See [HPC resources](documents/HPC_resources) 
</details>

## Tarefas

**Tarefa 1** – Seleção e justificação das metodologias a operacionalizar nas Tarefas 2, 3 e 4, dados de input, especificações técnicas dos outputs, e potenciais adaptações tecnológicas a implementar na cadeia de produção da DGT.

*Duração: Mês 1-6*

Entregáveis:
  * E1.1 – Relatório com a descrição do problema, condicionantes, dados de input e especificações técnicas dos outputs. [Entregável 1.1 (pdf), dezembro 2023](documents/deliverables/Entregavel_1_1.pdf)
  * E1.2 – Relatório com seleção e justificação das metodologias a operacionalizar. [Entregável 1.2 (pdf), maio 2024](documents/deliverables/Entregavel_1_2_v2.pdf).
  * E1.3 – Relatório sobre potenciais adaptações tecnológicas a implementar na cadeia de produção da DGT[Entregável 1.3 (pdf), julho 2024](documents/deliverables/entregavel_1.3_v3.pdf).

**Tarefa 2** - Construção da uma base de dados de referência (BDR) para calibração e validação espacial e temporal das metodologias a operacionalizar com base em dados resultantes de interpretação de imagens aéreas e de satélite, do Instituto de Conservação da Natureza e Florestas (ICNF) e outras fontes consideradas relevantes.

*Duração: Meses 2-18* 

Entregáveis:  
  * E2.1 – Relatório com metodologia de criação da BDR. [Entregável 2.1 (pdf), maio 2024](documents/deliverables/Entregavel_2_1_v2.pdf) 
  * E2.2 – Metadados da base de dados de referência em formato ESRI shapefile ou Geopackage para uma tile Sentinel-2 sobre Portugal Continental. [Entregável 2.2 (pdf), julho 2024](documents/deliverables/Entregavel_2_2_BDR_navigator_sentinel2_metadados_v4.pdf)
  * E2.3 – Extensão da base de dados para outras regiões de Portugal Continental. Ver [BRD_NVG](data_info/reference_data/NVG) para acesso aos dados (protegido) e à documentação.

**Tarefa 3** – Adaptação e implementação operacional de uma metodologia automática com base em imagens de satélite para a criação sistemática de um produto nacional de delimitação de manchas vetoriais superiores a 0.5 ha de perda recente de floresta e mato, com uma periodicidade de pelo menos dois meses.

*Duração: Meses 9-20*

Entregáveis:
  * E3.1 – Manual de utilização operacional da metodologia implementada na cadeia de produção da DGT. [Entregável 3.1 (pdf)](documents/deliverables/Entregavel_3.1_v3.pdf) (20 de outubro de 2024).
  * E3.2 – Demonstrador prático: mapas nacionais vetoriais a delimitar manchas de perda recente de floresta e mato superiores a 0.5 ha com uma frequência bimestral relativos a um período contínuo de dois anos entre 2023 e 2025. [Entregável 3.2 (pdf)](documents/deliverables/Entregavel_3_2_v1.pdf)
  * E3.3 – Relatório de validação dos mapas nacionais. [Entregável 3.3 (pdf), novembro 2025](documents/deliverables/Entregavel_3_3_AB_v1.pdf)
  * E3.4 – Aplicação informática que possa ser integrada na cadeia de produção da DGT:

    Os scripts (que foram desenvolvidos para serem compatíveis com o pipeline de processamento de dados Sentinel-2 em ambiente HPC) estão disponíveis neste repositório. Em particular, foi criado e testado o código para as  tarefas abaixo:
    - Processamento da série temporal para Portugal Continental 2017-2024 e aplicação do algoritmo de deteção de alteração CCD, cujo output é um conjunto de ficheiros em formato `parquet` em que cada linha corresponde a um pixel Sentinel-2 e a um segmento identificado pelo algoritmo. O processamento foi aplicado aos pixels pertencentes à máscara de potenciais perdas de vegetação para Portugal Continental fornecida pela DGT (aprox. 500 M pixels Sentinel 2 com resolução 10 m). [Script](scripts/pyccd)
    - Conversão dos resultado os formato `parquet` para mapas bimestrais em formato `geotiff`. Cada pixel agora possui quatro bandas: [Script](scripts/visualisations/ccd_to_raster.py): `last_tEnd`: data final do segmento antes da quebra; `last_tBreak`: data da quebra mais recente;  `is_break`: indicador booleano que sinaliza se ocorreu uma alteração: `1` se for uma quebra conhecida, `0` se não houve alteração, `-1` se não for possível determinar se ocorreu uma quebra ou um aumento (casos em que o segmento pós-quebra não foi formado); `ndvi_last_tEnd`: valor do NDVI calculado em last_tEnd.
    - Criação de mapas vectoriais bimestrais a partir dos mapas raster. [Script](scripts/visualisations/graph_raster_to_polygons.py)
    - Criação dos mapas nacionais bimestrais de perdas de vegetação. [Script](scripts/visualisations/ccd_polygons_to_national_maps.py)
    - Validação: ver entregável em **E3.3** com links para scripts.
      
**Tarefa 4** – Adaptação e implementação operacional na cadeia de produção da DGT de uma metodologia automática com base em imagens de satélite para a identificação sistemática do agente causador das perdas recentes de floresta e mato delimitadas no produto da tarefa 3, com uma periodicidade de pelo menos dois meses.

*Duração: Meses 13-24*

Entregáveis:
  * E4.1 – Manual de utilização operacional da metodologia implementada na cadeia de produção da DGT. [Entregável 4.1 (pdf), novembro 2025](documents/deliverables/Entregavel_41_nov_2025.pdf)
  * E4.2 – Demonstrador prático: mapas nacionais a identificar o agente causador das perdas recentes de floresta e mato superiores a 0.5 ha produzidas na tarefa 3.
  * E4.3 – Relatório de validação dos mapas nacionais.
  * E4.4 – Aplicação informática que possa ser integrada na cadeia de produção da DGT. 

## Aditamentos (setembro 2025 e março 2026)

- Entregáveis E2.3, E3.2, E3.3 e E4.1; contrato 20/4/2025; aditamento: 20/11/2025 (10% do valor contratado)
- Entregáveis E3.4, E4.2, E4.3 e E4.4; contrato 19/10/2025; aditamento: 30/06/2026 (10% do valor contratado)

Ver [cronograma](cronograma_aditamento_contrato_set_2025.png).

## Produtos

<details markdown="block">
<summary> Bases de dados de referência e produtos cartográficos de perdas de vegetação </summary>

* [Link](data_info)

</details>


