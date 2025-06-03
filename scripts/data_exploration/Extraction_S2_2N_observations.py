import geopandas as gpd
import numpy as np
import h5py
import pandas as pd
from datetime import datetime, timedelta
import os
import glob

# ------------------ CONFIGURAÇÕES ------------------
dataset = "ICNF"  # Ou "NVG"
tile_id = "T29TME"
anos = ['2020', '2021', '2022', '2023', '2024'] if dataset == "ICNF" else [None]  # só um ano para NVG
band_names = ['g', 'r', 'n', 's']
N_OBS = 10
# ---------------------------------------------------

# ------------------ FUNÇÕES ------------------------
def salvar_parquet(df: pd.DataFrame, output_path: str):
    df_simplificado = df.drop(columns=["buffer_ID", "ID"], errors="ignore")
    df_simplificado.to_parquet(output_path, index=False)
    print(f"Parquet file saved at: {output_path}")

def carregar_shapefile(path: str, crs_epsg: int = 32629) -> gpd.GeoDataFrame:
    return gpd.read_file(path).to_crs(epsg=crs_epsg)

def carregar_datas(path: str) -> list:
    datas_ndvi = np.load(path)
    return [datetime.fromordinal(int(d)).strftime('%Y%m%d') for d in datas_ndvi if d != 65535]

def criar_gdf_pixels(x_coords, y_coords, crs_epsg=32629):
    gdf_pixels = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy(x_coords, y_coords),
        crs=f"EPSG:{crs_epsg}"
    )
    gdf_pixels['idx_h5'] = range(len(x_coords))
    return gdf_pixels

def processar_pixel(pixel_idx, x, y, pixel_vals, datas_pixel, data_0_dt, band_names, data_1_dt=None, N_OBS=10):
    row = {
        'x': x,
        'y': y,
        'idx_h5': pixel_idx,
        'data_0': data_0_dt.strftime('%Y%m%d'),
        'data_1': data_1_dt.strftime('%Y%m%d') if data_1_dt else None,
    }
    usar_data_1 = data_1_dt and data_1_dt != data_0_dt
    data_target_dt = data_0_dt + timedelta(days=((data_1_dt - data_0_dt).days // 2)) if usar_data_1 else data_0_dt
    row['data_mid'] = data_target_dt.strftime('%Y%m%d')

    df_base = pd.DataFrame({
        'data': [datetime.strptime(d, '%Y%m%d') for d in datas_pixel],
        'valor': pixel_vals[:, 0]
    }).query("valor != 65535").sort_values('data').reset_index(drop=True)

    if df_base.empty:
        row['dts_a'], row['dts_d'] = [], []
        for b in band_names:
            row[f'{b}_a'], row[f'{b}_d'] = [], []
        return row

    antes = df_base[df_base['data'] <= data_target_dt].sort_values('data', ascending=False).head(N_OBS)
    depois = df_base[df_base['data'] > data_target_dt].sort_values('data').head(N_OBS)
    row['dts_a'] = sorted(antes['data'].dt.strftime('%Y%m%d'))
    row['dts_d'] = depois['data'].dt.strftime('%Y%m%d').tolist()

    for b, band_name in enumerate(band_names):
        df_band = pd.DataFrame({
            'data': [datetime.strptime(d, '%Y%m%d') for d in datas_pixel],
            'valor': pixel_vals[:, b]
        }).query("valor != 65535").sort_values('data')
        antes_band = df_band[df_band['data'] <= data_target_dt].sort_values('data', ascending=False).head(N_OBS)
        depois_band = df_band[df_band['data'] > data_target_dt].sort_values('data').head(N_OBS)
        row[f'{band_name}_a'] = antes_band.sort_values('data')['valor'].tolist()
        row[f'{band_name}_d'] = depois_band['valor'].tolist()

    return row

def expandir_listas_em_colunas(df: pd.DataFrame, prefixos: list, max_itens: int = 10) -> pd.DataFrame:
    df_expandido = df.copy()
    for col in df.columns:
        for prefix in prefixos:
            if col.startswith(prefix) and df[col].apply(lambda x: isinstance(x, list)).any():
                for i in range(max_itens):
                    df_expandido[f'{col}{i+1}'] = df[col].apply(lambda x: x[i] if isinstance(x, list) and len(x) > i else None)
                df_expandido.drop(columns=[col], inplace=True)
    return df_expandido

def reorganizar_colunas(df):
    colunas_principais = ['x', 'y', 'buffer_ID', 'ID']
    colunas_presentes = [c for c in colunas_principais if c in df.columns]
    outras_colunas = [c for c in df.columns if c not in colunas_presentes]
    return df[colunas_presentes + outras_colunas]

def processar_geometrias_otimizado(gdf_poligonos, datas_pixel, x_coords, y_coords, valores, band_names, N_OBS=10):
    gdf_pixels = criar_gdf_pixels(x_coords, y_coords, crs_epsg=gdf_poligonos.crs.to_epsg())
    join = gpd.sjoin(gdf_pixels, gdf_poligonos, how='inner', predicate='within')

    ids_todas = set(gdf_poligonos['id'].unique())
    ids_com_pix = set(join['id'].unique())
    print("Ignored geometries:", sorted(ids_todas - ids_com_pix))

    resultados = []
    for label, group in join.groupby('id'):
        feature = gdf_poligonos.loc[gdf_poligonos['id'] == label].iloc[0]
        print(f"Processing geometry ID: {label}, buffer_ID: {feature['id_gleba']}, number of pixels: {len(group)}")

        try:
            data_0_dt = datetime.strptime(feature['data_0'], '%Y%m%d')
        except:
            continue
        data_1_dt = None
        if 'data_1' in feature and feature['data_1']:
            try:
                data_1_dt = datetime.strptime(feature['data_1'], '%Y%m%d')
            except:
                pass

        for pixel_idx in group['idx_h5']:
            linha = processar_pixel(pixel_idx, x_coords[pixel_idx], y_coords[pixel_idx],
                                    valores[:, :, pixel_idx], datas_pixel, data_0_dt, band_names, data_1_dt, N_OBS)
            if linha:
                linha['ID'] = label
                linha['buffer_ID'] = feature.get('id_gleba', label)
                resultados.append(linha)
    return resultados

# ------------------- EXECUÇÃO ----------------------
anos_para_processar = anos if dataset == "ICNF" else [None]

for ano in anos_para_processar:
    print(f"\n--- Processing {dataset} | Year: {ano or 'N/A'} ---\n")

    # Caminho para o shapefile
    if dataset == "ICNF":
        shp_path = fr'C:\Users\Public\Documents\ref_datasets\BDR_ICNF\tiles_separados\{ano}\{tile_id}.shp'
    else:  # NVG
        shp_path = fr'C:\Users\Public\Documents\ref_datasets\BRD_NVG\tiles_separados\{tile_id}.shp'

    datas_path = fr'C:\Users\Public\Documents\outputs_ROI\hdf5\{tile_id}\tif_dates_ord.npy'
    h5_dir = fr'C:\Users\Public\Documents\outputs_ROI\hdf5\{tile_id}'
    h5_files = glob.glob(os.path.join(h5_dir, 's2_images-NDVI_*ROINAV.h5'))

    if not h5_files:
        print(f"No .h5 file found in {h5_dir}")
        continue

    h5_path = h5_files[0]
    print(f"Selected HDF5: {h5_path}")

    try:
        gdf = carregar_shapefile(shp_path)
    except Exception as e:
        print(f"Error loading shapefile: {e}")
        continue

    if 'id' not in gdf.columns:
        gdf['id'] = gdf.index
    if 'id_gleba' not in gdf.columns:
        gdf['id_gleba'] = gdf['id']

    # Colunas de datas
    if dataset == "ICNF":
        gdf = gdf[gdf['DH_Inicio'].notna()]
        gdf['data_0'] = pd.to_datetime(gdf['DH_Inicio']).dt.strftime('%Y%m%d')
        gdf['data_1'] = pd.to_datetime(gdf['DH_Fim'], errors='coerce').dt.strftime('%Y%m%d') if 'DH_Fim' in gdf.columns else None
    else:  # NVG
        gdf['data_0'] = pd.to_datetime(gdf['data_0']).dt.strftime('%Y%m%d')
        gdf['data_1'] = pd.to_datetime(gdf['data_1'], errors='coerce').dt.strftime('%Y%m%d')

    datas_pixel = carregar_datas(datas_path)

    with h5py.File(h5_path, 'r') as f:
        x_coords = f['xs'][:]
        y_coords = f['ys'][:]
        valores = f['values']

        resultados = processar_geometrias_otimizado(gdf, datas_pixel, x_coords, y_coords, valores, band_names, N_OBS)

    df_final = pd.DataFrame(resultados)
    prefixos = ['dts_a', 'dts_d'] + [f'{b}_a' for b in band_names] + [f'{b}_d' for b in band_names]
    df_final = expandir_listas_em_colunas(df_final, prefixos=prefixos, max_itens=N_OBS)
    df_final = reorganizar_colunas(df_final)

    nome_parquet = f"{dataset}_{tile_id}_{ano or 'UNICO'}.parquet"
    output_parquet = os.path.join(fr"C:\Users\Public\Documents\ref_datasets\amostras_por_pixel\BDR_{dataset}", tile_id, nome_parquet)
    os.makedirs(os.path.dirname(output_parquet), exist_ok=True)
    salvar_parquet(df_final, output_parquet)

    print(f"Completed: {nome_parquet}")
