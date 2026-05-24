Ver Email Ricardo Pinho, 14 de maio de 2026

ver também [20260514_Notas_LIP-ISA_DGT_tarefa3.pdf](https://github.com/S2change/vegetation_loss/blob/main/documents/presentations/2026/20260514_Notas_LIP-ISA_DGT_tarefa3.pdf)

---

Após analise interna na DGT dos últimos emails e relatórios, redigimos algumas notas e contributos para as questões levantadas que partilho no documento anexo: 20260514_Notas_LIP-ISA_DGT_tarefa3.pdf

Relativamente aos passos previstos apresentados no relatório de 12.maio apresentam-se as seguintes notas da DGT:

## testar o script append_hdf5.py sobre tile T29TQG usando o critério corrente de seleção de tiffs.


Avançar de imediato com os testes e verificação dos resultados com eventuais correções do script de forma a fique assegurada a possibilidade de adicionar novas imagens aos hdf5 e definida a forma mais adequada para o concretizar.
Agradecíamos que nos fossem reportadas as conclusões desta ação para posterior debate numa reunião técnica com a DGT.

## diagnosticar e apresentar medidas corretivas para os problemas identificados nos hdf5 atuais relativamente ao número muito elevado de timestamps.


Com base nas notas apresentadas no documento anexo e conclusões no último relatório, deverão ser corrigidos os desnecessários timestamps nos HDF5, para evitar o uso de recursos computacionais acrescidos comparativamente ao uso de hdf5 com apenas os timestamps necessários.

## Se houver no futuro nova produção de ficheiros hdf5, será importante considerar a inclusão de novos atributos (proporção de coberto de nuvens, área efetiva sobre território português).


Estes atributos deveriam constar dos hdf5 tal como nas imagens processadas, conforme são incluídos com os scripts internos facultados pela DGT.

## Acesso a GPU para aplicar o modelo preditivo de deep-learning: O servidor não tem nodos de GPU. Por essa razão a aplicação do modelo para predição terá que ser realizada em CPU.


Deveriam ser avaliadas as possibilidades e as mais-valia do uso de GPU no modelo preditivo de deep-learning, relativamente à performance do processo com CPU. Em caso afirmativo, pede-se ao LIP/CNCA para estudar a possibilidade de uso de GPU no Deucalion ou outra plataforma HPC com recursos GPU.


