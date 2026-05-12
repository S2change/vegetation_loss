# Criação de ficheiros hdf5

## Tiles e órbitas, e máscara Portugal

## Estimação da proporção de nuvens (CLOUD_REPORT)

## Atributos adicionais a considerar incluir em hdf5

- cobertura de nuvens
- órbita?

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

  
