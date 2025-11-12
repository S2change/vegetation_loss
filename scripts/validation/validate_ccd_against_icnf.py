"""
This script cross-references burned area data provided by ICNF with detections from the CCD algorithm.
For each ICNF polygon located within the spatial mask (GeoPackage), it searches for spatial intersections
with CCD polygons (which are already restricted to the same mask) within a ±60-day time window.
It calculates the intersection areas in hectares and records the corresponding CCD detection dates.
Additionally, it calculates the total ICNF area inside and outside the mask, as well as the ICNF area
within the mask that is not covered by CCD detections.

Finally, it generates a summary including the following metrics:
  - "Área total ICNF",
  - "Área total MBPV_v0",
  - "Área ICNF dentro da máscara DGT_loss_vegetation",
  - "Área ICNF fora da máscara DGT_loss_vegetation"
  - "Área de interseção ICNF (dentro da máscara) e MBPV_v0",
  - "Área ICNF (dentro da máscara) não coberta por MBPV_v0"
"""

import geopandas as gpd
import pandas as pd
import os
from datetime import timedelta
from tqdm import tqdm

# ==========================
# -------- INPUTS ----------
# ==========================
ANO = 2024

# ICNF
BASE_ICNF_FOLDER = r"C:\Users\Public\Documents\ref_datasets\BDR_ICNF"
ICNF_PATH = os.path.join(BASE_ICNF_FOLDER, f"ardida_{ANO}", f"ardida_{ANO}.shp")

# CCD
CCD_FOLDER = r"C:\Users\Public\Documents\outputs_ROI\tabular\merged_polygons"

# Máscara
MASK_PATH = r"D:\CCDC_Mask_dissolve.gpkg"

# Janela temporal para interseção (dias)
WINDOW_DAYS = 60

# ==========================
# ------- FUNÇÕES ----------
# ==========================
def carregar_icnf(icnf_path):
    """
    Carrega o ficheiro vetorial do ICNF (áreas ardidas) e prepara os dados.
    
    - Lê o shapefile correspondente ao ano definido.
    - Converte o atributo 'DH_Inicio' em data.
    - Corrige geometrias inválidas.
    - Calcula a área de cada polígono em hectares.
    
    Output: 
        GeoDataFrame com os dados do ICNF processados.
    """
    icnf = gpd.read_file(icnf_path)
    icnf["data_icnf"] = pd.to_datetime(icnf["DH_Inicio"], errors="coerce")
    icnf.loc[~icnf.geometry.is_valid, "geometry"] = icnf.loc[~icnf.geometry.is_valid].geometry.buffer(0)
    icnf["area_ha"] = icnf.geometry.area / 10000
    return icnf

def carregar_ccd(ccd_folder, ano, crs):
    """
    Carrega os ficheiros MBPV_v0 (CCD) correspondentes ao ano indicado.
    
    - Procura todos os ficheiros .gpkg do ano especificado.
    - Converte o atributo 'date_value' (no formato YYYYMMDD) em datetime.
    - Calcula a área de cada polígono em hectares.
    - Concatena todos os ficheiros num único GeoDataFrame.
    
    Output: 
        GeoDataFrame com todos os dados CCD do ano.
    """
    ccd_files = [f for f in os.listdir(ccd_folder) if f.endswith(".gpkg") and str(ano) in f]
    print(f"• {len(ccd_files)} ficheiros CCD encontrados para o ano {ano}")
    
    ccd_list = []
    for f in ccd_files:
        print(f" - {f}")
        gdf = gpd.read_file(os.path.join(ccd_folder, f))
        
        # Garantir que date_value é string e converter para datetime no formato YYYYMMDD
        gdf["date_value"] = pd.to_datetime(
            gdf["date_value"].astype(str),
            format="%Y%m%d",
            errors="coerce"
        )
        
        # --- PRINT PARA VERIFICAR DATAS ---
        print(f"Exemplo de datas convertidas do ficheiro {f}:")
        print(gdf["date_value"].head(2))
        print()
        
        ccd_list.append(gdf)
    
    ccd_all = pd.concat(ccd_list, ignore_index=True).to_crs(crs)
    ccd_all["area_ha"] = ccd_all.geometry.area / 10000
    return ccd_all

def aplicar_mascara(icnf, mask):
    """
    Aplica a máscara DGT_loss_vegetation aos polígonos ICNF.
    
    - Calcula a interseção de cada polígono ICNF com a máscara.
    - Remove geometrias vazias.
    - Calcula a área de cada interseção em hectares.
    
    Output: 
        GeoDataFrame com os polígonos ICNF limitados à máscara.
    """
    mask_union = mask.unary_union
    icnf_masked = icnf.copy()
    icnf_masked["geometry"] = icnf_masked.geometry.intersection(mask_union)
    icnf_masked = icnf_masked[~icnf_masked.is_empty]
    icnf_masked["area_ha_mask"] = icnf_masked.geometry.area / 10000
    return icnf_masked

def calcular_interseccao(icnf_masked, ccd_all, dias=60):
    """
    Calcula a área de interseção entre os polígonos ICNF (dentro da máscara)
    e os polígonos CCD (MBPV_v0), considerando uma janela temporal de ±dias.
    
    - Para cada polígono ICNF, seleciona os polígonos CCD com data dentro da janela.
    - Calcula a soma das áreas de interseção (em hectares).
    - Adiciona uma coluna com o valor total de interseção por polígono.
    
    Output:
        GeoDataFrame do ICNF com coluna 'area_intersec_mask'.
    """
    icnf_masked["area_intersec_mask"] = 0.0
    for idx, fire in tqdm(icnf_masked.iterrows(), total=len(icnf_masked), desc="Calculando interseções ICNF x CCD"):
        fire_geom = fire.geometry
        fire_date = fire["data_icnf"]
        if pd.isna(fire_date):
            continue
        date_min = fire_date - timedelta(days=dias)
        date_max = fire_date + timedelta(days=dias)
        ccd_window = ccd_all[(ccd_all["date_value"] >= date_min) & (ccd_all["date_value"] <= date_max)]
        intersec_total = sum(
            (fire_geom.intersection(ccd_poly.geometry).area / 10000)
            for _, ccd_poly in ccd_window.iterrows()
            if fire_geom.intersects(ccd_poly.geometry) and not fire_geom.intersection(ccd_poly.geometry).is_empty
        )
        icnf_masked.at[idx, "area_intersec_mask"] = intersec_total
    return icnf_masked

def gerar_resumo(icnf, ccd_all, icnf_masked):
    """
    Calcula as principais métricas de comparação entre os dados do ICNF e do MBPV_v0.

    - Determina as áreas totais do ICNF e do MBPV_v0.
    - Calcula as áreas do ICNF dentro e fora da máscara DGT_loss_vegetation.
    - Mede a área de interseção (dentro da máscara) entre ICNF e MBPV_v0.
    - Calcula a área ICNF (dentro da máscara) não coberta por deteções MBPV_v0.

    Output:
        DataFrame pandas com os valores agregados (em hectares).
    """
    area_total_icnf = icnf["area_ha"].sum()
    area_total_ccd = ccd_all["area_ha"].sum()
    area_icnf_within_mask = icnf_masked["area_ha_mask"].sum()
    area_icnf_outside_mask = area_total_icnf - area_icnf_within_mask
    area_intersec_total = icnf_masked["area_intersec_mask"].sum()
    area_nao_coberta = area_icnf_within_mask - area_intersec_total

    resumo = pd.DataFrame({
        "Área (ha)": [
            area_total_icnf,
            area_total_ccd,
            area_icnf_within_mask,
            area_icnf_outside_mask,
            area_intersec_total,
            area_nao_coberta,
        ]
    }, index=[
        "Área total ICNF",
        "Área total MBPV_v0",
        "Área ICNF dentro da máscara DGT_loss_vegetation",
        "Área ICNF fora da máscara DGT_loss_vegetation",
        "Área de interseção ICNF (dentro da máscara) e MBPV_v0",
        "Área ICNF (dentro da máscara) não coberta por MBPV_v0",
    ])
    return resumo

# ==========================
# ------- EXECUÇÃO ---------
# ==========================
def main():
    icnf = carregar_icnf(ICNF_PATH)
    print(f"Ficheiro ICNF carregado: {ICNF_PATH}")
    
    ccd_all = carregar_ccd(CCD_FOLDER, ANO, icnf.crs)

    mask = gpd.read_file(MASK_PATH).to_crs(icnf.crs)
    icnf_masked = aplicar_mascara(icnf, mask)

    icnf_masked = calcular_interseccao(icnf_masked, ccd_all, dias=WINDOW_DAYS)

    resumo = gerar_resumo(icnf, ccd_all, icnf_masked)
    
    # Print das métricas
    print(f"\nAno: {ANO}\n")
    print(resumo.round(2))

if __name__ == "__main__":
    main()
