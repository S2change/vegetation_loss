Presentes: Gonçalo Barradas, João Pina, Manuel Campagnolo

---

# Criação e atualização de ficheiros hdf5

- Gonçalo vai testar o script `append_hdf5.py` sobre tile T29TQG usando o critério corrente de seleção de tiffs. Assim será possível verificar que podem ser adicionadas novos "timestamps" a um ficheiro hdf5 na ordem correta. Antes de testarem o append_hdf5 é preciso verificar que o código não remove timestamps duplicadas, pois o código foi escrito no pressuposto que não havia timestamps idênticos na construção do hdf5 para a uma tile.

  
- Problemas identificados nos hdf5 atuais relativamente ao número muito elevado de timestamps. Sem qualquer filtragem, o hdf5 acumula uma "layer temporal (timestamp)" por cada ficheiro geotiff (ESA). Para uma mesma data e para a mesma tile, podem existir vários ficheiros por dois motivos:
  - os ficheiros geotiff (ESA) são particionados em mais do que um ficheiro (todos com mesma timestamp)
  - cada tile é coberta por mais do que uma órbita, cada uma com o seu timestamp

- Uma consequência prática importante é poder haver duplicações de timestamps legítimas e que devem ser preservadas no ficheiro hdf5. Assim, dever-se-ia analisar dois ficheiros geotiff (mesma tile, mesmo timestamp) nessas condições para um timestamp de 2025 ou anterior (que esteja incluído nos hdf5) para verificar que de facto ambos os "bocados" da tile/timestamp ficaram escritos no hdf5

- O cálculo efetivo da cobertura por nuvens depende dos fatores listados acima, e também da máscara espacial aplicada (Portugal Continental + 2 km). Para uma filtragem de nuvens efetiva, o cálculo da cobertura de nuvens deve ser revisto usando todos esses fatores. Foi decidido para já não fazer essa alteração e usar os ficheiros hdf5 existentes para se perceber melhor o que poderá ser melhorado.

- Se houver no futuro nova produção de ficheiros hdf5, será importante considerar a inclusão de novos atributos (proporção de coberto de nuvens, área efetiva sobre território português

# Processamento dos dados e predição de alterações com modelo de deep-learning

Os principais passos previstos de préprocessamento e processamento para produção das Carta de Perdas de Vegetação na plataforma INCD estão descritos em https://github.com/S2change/vegetation_loss/blob/main/scripts/data_exploration/HPC_parallelization/general_pipeline.md

Foram discutidas as seguintes questões técnicas importantes:

- acesso a GPU para apliucar o modelo preditivo de deep-learning: O servidor não tem nodos de GPU. Por essa razão a aplicação do modelo para predição terá que ser realizada em CPU
- RAM por CPU: 5 GB
- Número de nodes e cores a usar no processamento: MC indicou que tinham sido usados para o processamento dos dados com algoritmo pyCCD (em 2025) 5 nodes e 96 cores/node, num total de 480 tasks
- Ficou combinado que MC faria testes de aplicação do modelo preditivo em CPU no INCD para avaliar recursos (tempo e memória) ; com essa informação disponível, será feita nova reunião técnica para avaliar a melhor forma de aceder aos resursos para processar os dados em tempo útil.
  
