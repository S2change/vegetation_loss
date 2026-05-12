Presentes: Manuel Campagnolo, Gonçalo Barradas, João Pina


# Criação de ficheiros hdf5

- Gonçalo vai testar o script `append_hdf5.py` sobre tile T29TQG usando o critério corrente de seleção de tiffs. Assim será possível verificar que podem ser adicionadas novos "timestamps" a um ficheiro hdf5 na ordem correta

- Problemas identificados nos hdf5 atuais relativamente ao número muito elevado de timestamps. Sem qualquer filtragem, o hdf5 acumula uma "layer temporal (timestamp)" por cada ficheiro geotiff (ESA). Para uma mesma data e para a mesma tile, podem existir vários ficheiros por dois motivos:
  - os ficheiros geotiff (ESA) são particionados em mais do que um ficheiro (todos com mesma timestamp)
  - cada tile é coberta por mais do que uma órbita, cada uma com o seu timestamp

Uma consequência prática importante é poder haver duplicações de timestamps legítimas e que devem ser preservadas no ficheiro hdf5.

Assim, o cálculo efetivo da cobertura por nuvens depende desses fatores, e também da máscara espacial aplicada (Portugal Continental + 2 km). Para uma filtragem de nuvens efetiva, o cálciulo da cobertura de nuvens deve ser revisto usando todos esses fatores. Foi decidido para já não fazer essa alteração e usar os ficheiros hdf5 existentes para se perceber melhor o que poderá ser melhorado.

Se houver no futuro nova produção de ficheiros hdf5, será importante considerar a inclusão de novos atributos (proporção de coberto de nuvens, área efetiva sobre território português


# Processamento

## Limite RAM/CPU: 5 GB/cpu

## nodes e cores

- 5 cores
- 96 modes/core

## Acesso a GPU

- Para carregar modelo:

```
def load_model(weights_path, device=None):
    """Load Swin-YNet from a .pth checkpoint and return it in eval mode.

    Parameters
    ----------
    weights_path : str or Path
    device       : torch.device or None — auto-detected when None

    Returns
    -------
    model : nn.Module, on `device`, in eval mode
    """
    if device is None:
        device = torch.device(
            'cuda' if torch.cuda.is_available() and getattr(AAA_Configs, 'USE_CUDA', False)
            else 'cpu'
        )
    model = Encoder(num_classes=AAA_Configs.NUM_CLASSES).to(device)
    state = torch.load(weights_path, map_location=device, weights_only=False)
    model.load_state_dict(state)
    model.eval()
    return model
```

Para carregar dados e aplicar modelo:

```
if isinstance(model_or_path, (str, Path)):
        model = load_model(model_or_path, device)
    else:
        model = model_or_path

    # Stack individual chips into (B, C, H, W) tensors
    t_before = torch.stack([_chip_to_tensor(before_batch[i])
                            for i in range(len(before_batch))]).to(device)
    t_after  = torch.stack([_chip_to_tensor(after_batch[i])
                            for i in range(len(after_batch)) ]).to(device)

    with torch.no_grad():
        outputs = model(t_before, t_after)
  ...
```

  
