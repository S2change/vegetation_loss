import os
import pandas as pd
import numpy as np
import h5py
import matplotlib.pyplot as plt
import ast

# Coordenadas de interesse
coordenadas = (4561570,592599) #ponto fogo setembro
# coordenadas = (4541639,577945)
ys1, xs1 = coordenadas

# Load NDVI HDF5
datas_ndvi = np.load(r'E:\outputs_ROI\hdf5\T29TNF\tif_dates_ord.npy')

with h5py.File(r'E:\outputs_ROI\hdf5\T29TNF\s2_images-NDVI_XX999YM1NOBS6LDA200ITER1000_START20170408_END20241229_ROINAV.h5', 'r') as f:
    x_coords = f['xs'][:]
    y_coords = f['ys'][:]
    ndvi_dataset = f['values']

    # Converte para float64 para evitar overflow
    x_coords_f = x_coords.astype(np.float64)
    y_coords_f = y_coords.astype(np.float64)
    
    dist = np.sqrt((x_coords_f - xs1)**2 + (y_coords_f - ys1)**2)
    idx_point = np.argmin(dist)
    
    xs, ys = x_coords[idx_point], y_coords[idx_point]
    
    print(f"Coordenada pedida: X={xs1}, Y={ys1}")
    print(f"Coordenada encontrada: X={xs}, Y={ys}")
    print(f"Índice do ponto: {idx_point}")

    # Extração dos valores (dataset: time, band, n_pixels)
    red_values = ndvi_dataset[:, 1, idx_point]
    nir_values = ndvi_dataset[:, 2, idx_point]

    # Cálculo do NDVI
    ndvi_values = ((nir_values - red_values) / (nir_values + red_values)) * 10000
#%%
pasta_parquet = r'C:\Users\Public\Documents\outputs_ROI\tabular\T29TNF'

parquet_files = [f for f in os.listdir(pasta_parquet) if f.endswith('.parquet')]

linhas_encontradas = []

for fname in parquet_files:
    caminho = os.path.join(pasta_parquet, fname)
    try:
        df = pd.read_parquet(caminho, engine="pyarrow")
        filtro = df[(df['x_coord'] == xs) & (df['y_coord'] == ys)]
        if not filtro.empty:
            linhas_encontradas.append(filtro)
            print(f"✅ Coordenadas encontradas em: {fname}")
    except Exception as e:
        print(f"Erro ao ler {fname}: {e}")

if not linhas_encontradas:
    raise ValueError("❌ Coordenadas não encontradas em nenhum ficheiro Parquet.")

# Junta todas as linhas em um único DataFrame
df_coordenadas = pd.concat(linhas_encontradas, ignore_index=True)

# Converte datas NDVI de ordinal para datetime
datas_observadas = pd.to_datetime([pd.Timestamp.fromordinal(int(d)) for d in datas_ndvi])

# Aplica máscara de NDVI válido
mask = (ndvi_values != 0) & (ndvi_values < 30000)
datas_filtradas = datas_observadas[mask]
ndvi_filtrado = ndvi_values[mask]

import matplotlib.pyplot as plt
import pandas as pd

plt.style.use('ggplot')
fig, ax = plt.subplots(figsize=(14, 4), dpi=90)

# 1️⃣ NDVI observado
ax.plot(datas_filtradas, ndvi_filtrado, 'go', markersize=4, label='NDVI observado')
plt.title(rf"Ponto_{ys}_{xs}")
# 2️⃣ Linhas verticais de tStart, tEnd e tBreak
for i, row in df_coordenadas.iterrows():
    # # tStart
    # tstart_raw = row.get('tStart')
    # if pd.notna(tstart_raw):
    #     tstart_dt = pd.to_datetime(tstart_raw, unit='ms')
    #     ax.axvline(tstart_dt, color='g', linestyle='--', label='tStart' if i == 0 else "")
    #     ax.text(
    #         tstart_dt, ax.get_ylim()[1], 
    #         tstart_dt.strftime('%d-%m-%Y'),
    #         rotation=90, ha='right', va='top', fontsize=8, color='g'
    #     )

    # tEnd
    tend_raw = row.get('tEnd')
    if pd.notna(tend_raw):
        tend_dt = pd.to_datetime(tend_raw, unit='ms')
        ax.axvline(tend_dt, color='r', linestyle='--', label='tEnd' if i == 0 else "")
        ax.text(
            tend_dt, ax.get_ylim()[1],
            tend_dt.strftime('%d-%m-%Y'),
            rotation=90, ha='left', va='top', fontsize=8, color='r'
        )

    # tBreak
    tbreak_raw = row.get('tBreak')
    if pd.notna(tbreak_raw):
        tbreak_dt = pd.to_datetime(tbreak_raw, unit='ms')
        ax.axvline(tbreak_dt, color='b', linestyle='--', label='tBreak' if i == 0 else "")
        ax.text(
            tbreak_dt, ax.get_ylim()[1], 
            tbreak_dt.strftime('%d-%m-%Y'),
            rotation=90, ha='center', va='bottom', fontsize=8, color='b'
        )

# 3️⃣ Labels e grid
ax.set_ylabel('NDVI')
ax.set_xlabel('Data')

ax.grid(True)
ax.legend()
plt.tight_layout()
plt.show()

import geopandas as gpd
from shapely.geometry import Point
import pandas as pd

# Supondo que você tem o ponto do HDF5
point_x = xs
point_y = ys

# Converte colunas de tempo (de ms para datetime)
for col in ['tStart', 'tBreak', 'tEnd']:
    if col in df_coordenadas.columns:
        df_coordenadas[col] = pd.to_datetime(df_coordenadas[col], unit='ms', errors='coerce')

# Formata as datas para string legível (ou mantém datetime se preferires)
df_coordenadas['tStart_str'] = df_coordenadas['tStart'].dt.strftime('%d-%m-%Y')
df_coordenadas['tBreak_str'] = df_coordenadas['tBreak'].dt.strftime('%d-%m-%Y')
df_coordenadas['tEnd_str']   = df_coordenadas['tEnd'].dt.strftime('%d-%m-%Y')

# Cria uma coluna geometry (todos os pontos iguais)
df_coordenadas['geometry'] = [Point(point_x, point_y)] * len(df_coordenadas)

# Cria GeoDataFrame
gdf = gpd.GeoDataFrame(
    df_coordenadas[['tStart_str', 'tEnd_str', 'tBreak_str', 'geometry']],
    crs="EPSG:32629"  # ajusta o CRS conforme o teu sistema de coordenadas
)

# Caminho de saída
shapefile_path = rf"C:\Users\Public\Documents\outputs_ROI\ponto_{ys}_{xs}.shp"

# Salva o shapefile
gdf.to_file(shapefile_path)

print(f"✅ Shapefile criado com {len(gdf)} segmentos:")
print(f"📁 {shapefile_path}")
