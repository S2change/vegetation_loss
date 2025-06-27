"""
Script that conducts accuracy assessment of change detection results from raster data.

Inputs:
    RASTER_FILE: path to raster file containing change detection dates in YYYYMMDD format
    REFERENCE_FILE: path to the shapefile/geopackage of the reference dataset used for validation 

Outputs:
    - Creates CSV files with accuracy assessment results in a new folder
    - Prints accuracy metrics (F1-score, omission and commission errors) to console
    - Files saved in the same folder as RASTER_FILE, in new directory /{raster_name}_accuracy_assessment
"""

# ---------------------------------
#      PARAMETROS DA VALIDACAO
# ---------------------------------
# Tolerance margin between model breaks and analyst dates
theta = 60 # +/- theta days of tolerance
# band used for magnitude filtering
bandFilter = None #not implemented yet - do not touch
# Reference file is the same whether running single or batch
REFERENCE_FILE = r'/Users/domwelsh/green_ds/Thesis/BDR_TNE_300/BDR_CCDC_TNE_Adjusted.shp'

# ---------------------------------
#      RUNNING SINGLE FILE
# ---------------------------------
# These variables are only used if RASTER_DIRECTORY = None
RASTER_FILE = r'/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/processed_outputs/BDR_300_artigo_tol30_05ha_rasters/BDR_300_artigo_202301-202302.tif'
# Add polygon file path to create and use mask raster file with the polygons. Set to None if full raster file should be used
POLYGON_FILE = r'/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/processed_outputs/BDR_300_artigo_tol30_05ha_polygons/BDR_300_artigo_202301-202302_tol30_05ha_polygons.shp'
# Add output path for where masked raster file should be kept
# Code will also check for existing mask file, and use that if it exists
MASK_OUTPUT_PATH = r'/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/processed_outputs/masked_rasters/mask_test.tif'

# ---------------------------------
#      RUNNING BATCH
# ---------------------------------
# If RASTER_DIRECTORY is not None, then the script will process all raster files in the directory
# These variables are used instead of the variables in RUNNING SINGLE FILE
RASTER_DIRECTORY = r'/Users/domwelsh/green_ds/Thesis/T29TNE_0999/2019_2020_processed_outputs/T29TNE_0999_tol30_05ha_rasters'  # Path to directory containing multiple raster files
POLYGON_DIRECTORY = r'/Users/domwelsh/green_ds/Thesis/T29TNE_0999/2019_2020_processed_outputs/T29TNE_0999_tol30_05ha_polygons' # Path to directory containing polygon files (optional)
MASK_OUTPUT_DIRECTORY = r'/Users/domwelsh/green_ds/Thesis/T29TNE_0999/2019_2020_processed_outputs/T29TNE_0999_tol30_05ha_masked_rasters'  # Path to directory where masked rasters should be saved (required if POLYGON_DIRECTORY is provided)
# ---------------------------------
# ---------------------------------

import os
from datetime import datetime
import pandas as pd
import numpy as np
import geopandas as gpd
import rasterio
import rasterio.transform
from rasterio.mask import mask

import warnings
warnings.filterwarnings('ignore')


def yyyymmdd_to_datetime(date_int):
  """
  Convert YYYYMMDD integer format to datetime object.

  Args:
      date_int: Integer in YYYYMMDD format (e.g., 20200315)
      
  Returns:
      datetime object or pd.NaT for invalid dates
  """
  date_str = str(int(date_int))
  if len(date_str) == 8:  # YYYYMMDD
      year = int(date_str[:4])
      month = int(date_str[4:6])
      day = int(date_str[6:8])
      return datetime(year, month, day)
  else:
      return pd.NaT  # Return NaT for invalid dates
  

def find_matching_polygon(raster_filename, polygon_directory):
    """
    Find polygon file that matches the raster filename.
    
    Args:
      raster_filename: Name of the raster file (without extension)
      polygon_directory: Directory containing polygon files
      
    Returns:
      str or None: Full path to matching polygon file, or None if not found
    """
    if not polygon_directory or not os.path.exists(polygon_directory):
      return None
    
    # Get all polygon files in directory
    polygon_extensions = ['.shp', '.gpkg']
    polygon_files = []
    for ext in polygon_extensions:
      polygon_files.extend([f for f in os.listdir(polygon_directory) if f.endswith(ext)])
    
    # Find matching polygon file (starts with same name as raster)
    raster_base = os.path.splitext(raster_filename)[0]
    for polygon_file in polygon_files:
      polygon_base = os.path.splitext(polygon_file)[0]
      if polygon_base.startswith(raster_base):
        return os.path.join(polygon_directory, polygon_file)
    
    return None
  

def mask_raster_with_polygons(raster_file, polygon_file, output_file=None, 
                             crop=True, filled=True, invert=False):
  """
  Create a new masked raster file using polygon boundaries.
  
  This function reads a raster file and a polygon file, then creates a new
  raster where pixels outside the polygon boundaries are set to NoData.
  
  Args:
    raster_file (str): Path to input raster file
    polygon_file (str): Path to polygon shapefile/geopackage for masking
    output_file (str, optional): Path for output masked raster. If None,
                                creates filename based on input raster name
    crop (bool): If True, crop the raster to the extent of the polygons.
                If False, keep original raster extent. Default: True
    filled (bool): If True, pixels outside polygons become NoData.
                  If False, pixels inside polygons become NoData. Default: True
    invert (bool): If True, invert the mask (mask becomes the area to keep).
                  Default: False
  
  Returns:
    str: Path to the created masked raster file
      
  Raises:
    FileNotFoundError: If input files don't exist
    ValueError: If CRS reprojection fails or geometries are invalid
  """
  
  # Validate input files exist
  if not os.path.exists(raster_file):
    raise FileNotFoundError(f"Raster file not found: {raster_file}")
  if not os.path.exists(polygon_file):
    raise FileNotFoundError(f"Polygon file not found: {polygon_file}")
  
  # Generate output filename if not provided
  if output_file is None:
    raster_dir = os.path.dirname(raster_file)
    raster_name = os.path.splitext(os.path.basename(raster_file))[0]
    polygon_name = os.path.splitext(os.path.basename(polygon_file))[0]
    output_file = os.path.join(raster_dir, f"{raster_name}_masked_by_{polygon_name}.tif")
  
  if os.path.exists(output_file):
    print(f"Masked file already exists: {output_file}")
    return output_file
  
  # Ensure output directory exists
  output_dir = os.path.dirname(output_file)
  if output_dir and not os.path.exists(output_dir):
    os.makedirs(output_dir)
  
  print(f"Reading polygon file: {polygon_file}")
  # Read polygon file
  polygons_gdf = gpd.read_file(polygon_file)
  
  # Check if polygons have valid geometries
  if polygons_gdf.empty:
    raise ValueError("Polygon file contains no features")
  
  # Remove any invalid geometries
  valid_geoms = polygons_gdf.geometry.is_valid
  if not valid_geoms.all():
    print(f"Warning: Found {(~valid_geoms).sum()} invalid geometries. Removing them.")
    polygons_gdf = polygons_gdf[valid_geoms]
  
  if polygons_gdf.empty:
    raise ValueError("No valid geometries found in polygon file")
  
  print(f"Reading raster file: {raster_file}")
  # Open raster and get its CRS
  with rasterio.open(raster_file) as src:
    raster_crs = src.crs
    
    # Reproject polygons to match raster CRS if needed
    if polygons_gdf.crs != raster_crs:
      print(f"Reprojecting polygons from {polygons_gdf.crs} to {raster_crs}")
      try:
        polygons_gdf = polygons_gdf.to_crs(raster_crs)
      except Exception as e:
        raise ValueError(f"Failed to reproject polygons to raster CRS: {e}")
    
    # Extract geometries for masking
    geometries = polygons_gdf.geometry.values
    
    print("Applying mask to raster...")
    # Apply mask
    try:
      masked_image, masked_transform = mask(
        src, 
        geometries, 
        crop=crop, 
        filled=filled,
        invert=invert
      )
    except Exception as e:
      raise ValueError(f"Failed to apply mask: {e}")
    
    # Prepare metadata for output file
    masked_meta = src.meta.copy()
    masked_meta.update({
      "driver": "GTiff",
      "height": masked_image.shape[1],
      "width": masked_image.shape[2],
      "transform": masked_transform,
      "compress": "lzw"  # Add compression to reduce file size
    })
  
  print(f"Writing masked raster to: {output_file}")
  # Write masked raster to new file
  with rasterio.open(output_file, "w", **masked_meta) as dest:
    dest.write(masked_image)
  
  print(f"Successfully created masked raster: {output_file}")
  return output_file


def spatialJoin(pathPoligonosDGT, dfCCDC):
  """
  Perform spatial join between change detection results and reference polygons.

  Args:
    pathPoligonosDGT: Path to reference polygons shapefile/geopackage from validation dataset
    dfCCDC: DataFrame containing change detection results with coordinates and dates
    
  Returns:
    tuple: (filtered_subset, full_subset)
      - filtered_subset: DataFrame with selected columns for analysis
      - full_subset: Complete DataFrame with all spatial join results
  """
  # 1) ABRIR OS ARQUIVOS
  ## Poligonos DGT
  gdfVal = gpd.read_file(pathPoligonosDGT)
  gdfVal.to_crs(crs = 'EPSG:3763', inplace = True) # Originalmente eles estao em WGS84 29N converte para ETRS
  ## Pontos ISA

  # 2) CONVERTER O DF PARA GEO DF
  gdfCCDC = gpd.GeoDataFrame(dfCCDC, geometry = gpd.points_from_xy(dfCCDC.longitude, dfCCDC.latitude), crs=32629) # old csvs - crs=4326
  gdfCCDC.to_crs(crs=4326, inplace=True)

  ## criar a bordadura
  ###idBord = identity.copy() # cria uma copia do identity gerado acima
  idBord = gdfVal.copy()
  idBord['geometry'] = idBord.geometry.buffer(-10) # reduz a geometria em 10 metros
  idBord.drop(list(idBord.columns)[:-1], axis = 1, inplace = True) #remove todas as colunas menos a da geometria
  idBord['bordadura'] = 1 # cria uma nova coluna para poder identificar a borda dura
  ## novo identity para termos a area da borda dura

  ###identity = gpd.overlay(identity, idBord, how='identity')
  identity = gpd.overlay(gdfVal, idBord, how = 'identity')

  # Como o poligono inicial nao tinha a coluna de bordadura, há feições onde
  # temos 1 e Nulos, com a linha abaixo invertemos o campo onde era Nullo passa a True
  # e onde era 1 passa para False, ou 1 e 0
  identity.bordadura = identity.bordadura.isnull()
  # Convertemos o resultado para WGS84
  identity.to_crs(crs = 'EPSG: 4326', inplace = True)

  
  ## As datas da DGT estao no formato (20200103) e precisam ser convertidas
  for dataCol in ['data_0', 'data_1', 'data_2', 'data_3']:
    # primeiro converter para datetime
    maskZero = pd.Series(np.zeros(len(identity),dtype=bool))
    erro = identity[dataCol].isnull()
    identity.loc[erro, dataCol] = 0
    # converter tudo para inteiros e onde for 0 indicar 1970
    identity[dataCol] = identity[dataCol].astype(int)
    maskZero = identity.loc[:, dataCol] == 0
    identity.loc[maskZero, dataCol] = 19700101
    # converter para datetime
    identity[dataCol] = pd.to_datetime(identity[dataCol], format = '%Y%m%d')
    identity.loc[maskZero, dataCol] = np.nan


  # 4) SPATIAL JOIN ENTRE OS CENTROIDES DO CCDC COM OS BUFFERS DE 200 METROS
  subset = gpd.sjoin(gdfCCDC, identity, how='inner')
  subset.reset_index(inplace = True)
  subset['buffer_ID'] = subset.buffer_ID.astype('int')

  
  #Descobrir quais linhas precisam ser duplicadas.
  #Pressupondo que não é possível ter informação da 'data_3' sem existir a 'data_1'
  #é possível filtrar e verificar a negação de quais dados são nulos e depois somar
  #o reultado.
  #0 = False False: não há data_1 e nem data_3
  #1 = True  False: existe data_1 e não data_3
  #2 = True  True: existem data_1 e Data_3
  
  cond = ~subset.filter(items=['data_1', 'data_3']).isnull()
  subset['analistas'] = cond.sum(axis=1)
  subset.loc[subset['analistas'] == 0, 'exists_event'] = False # Analista nao identificou nada
  subset.loc[subset['analistas'] > 0, 'exists_event'] = True # Analista identificou alteracao

  
  #CRIA UM DF TEMPORARIO PARA COPIAR AS LINHAS ONDE EXISTEM A 'DATA_3' E INSERE ESTA DATA NO CAMPO 'DATA1_Z'
  #DEPOIS ADICIONA ISTO AO DATA FRAME ORIGINAL
  
  subset['data1_z'] = ''
  # criar coluna para as datas anteriores
  # subset['data0_z'] = ''
  subset['nome'] = '' # teste para nomear os analistas
  subset['tipo'] = ''
  subset['classeAnterior'] = ''
  subset['classeAtual'] = ''
  dfTemp = pd.DataFrame(columns = subset.columns)
  for row in subset.itertuples():
    # verifica se há duas datas e duplica a linha
    if row.analistas == 2:
      dfTemp = pd.concat([dfTemp, subset[subset.index==row.Index]],ignore_index=False)#dfTemp.append(subset[subset.index == row.Index], ignore_index=False)
  dfTemp.data1_z = dfTemp.data_3
  # capturar o valor da data_2
  # defTemp.data0_z = dfTemp.data_2
  dfTemp.nome = 'B' # teste para nomear os analistas
  dfTemp.tipo = dfTemp.tipo_2
  dfTemp.classeAtual = dfTemp.classe_3
  dfTemp.classeAnterior = dfTemp.classe_2

  subset.data1_z = subset.data_1
  # capturar o valor da data_0
  # subset.data0_z = subset.data_0
  subset.nome = 'A' # teste para nomear os analistas
  subset.tipo = subset.tipo_1
  subset.classeAtual = subset.classe_1
  subset.classeAnterior = subset.classe_0

  subset = pd.concat([subset, dfTemp],ignore_index=False)#subset.append(dfTemp, ignore_index=False)

  # Contagem do numero de breaks
  subset['Valid_breaks'] = np.ceil(subset.groupby(['coord_ccdc', 'nome'])['changeProb'].transform('sum'))

  # Updated to handle NaN values
  subset['data1_z'] = subset['data1_z'].replace('', np.nan)
  subset['data1_z'] = pd.to_datetime(subset['data1_z'], errors='coerce')

  # COLUNA DO DELTA MIN
  subset['delta_min'] = (subset.data1_z - subset.tBreak).dt.days
  subset.drop(['data_1', 'data_3', 'tipo_1', 'tipo_2','classe_0', 'classe_1','classe_2', 'classe_3'], axis = 1, inplace = True)

  # verificar quais colunas tem magnitude de indices
  mags = [ t for t in subset.columns if 'magnitude' in t and not 'B' in t]
  ordem = [ 'coord_ccdc','buffer_ID', 'altera', 'changeProb'] + mags + ['tBreak', 'data1_z',
         'bordadura', 'classe2018', 'classe2019', 'classe2020','classe2021', 'classeAnterior','tipo',
         'classeAtual', 'analistas', 'nome', 'exists_event', 'Valid_breaks' , 'delta_min', 'geometry']


  return subset[ordem], subset


def preprocessRaster(raster_path):
  """
  Extract change detection data from raster file and convert to DataFrame format.

  Reads a raster file where Band 1 contains change detection dates in YYYYMMDD format.
  Pixels with nodata values represent locations where no change was detected.

  Args:
    raster_path: Path to raster file containing change detection results
                (Band 1 = tBreak dates in YYYYMMDD format)

  Returns:
    pandas.DataFrame: Contains columns:
      - longitude, latitude: Geographic coordinates of change pixels
      - tBreak: Change detection dates as datetime objects
      - changeProb: Change probability (set to 1 for all detected changes)
      - coord_ccdc: Tuple of (latitude, longitude) for indexing
  """
    
  with rasterio.open(raster_path) as src:
    # Extract necessary data from raster
    band_data = src.read(1)
    transform = src.transform
    rows, cols = np.where(band_data != src.nodata)
    xs, ys = rasterio.transform.xy(transform, rows, cols)
    tbreak_values = band_data[rows, cols]
  
  # Create the DataFrame
  df = pd.DataFrame({
    'longitude': xs,
    'latitude': ys,
    'tBreak': [yyyymmdd_to_datetime(val) for val in tbreak_values],
    'changeProb': 1,  # All pixels represent detected changes
  })
  
  # Create coord_ccdc as tuple of (latitude, longitude)
  df['coord_ccdc'] = list(zip(df.latitude, df.longitude))
  
  # Remove any rows with no breaks
  df = df.dropna(subset=['tBreak'])
  
  return df


def valPol(df, theta):
  """
  Perform accuracy assessment by calculating confusion matrix metrics.

  Compares model-detected change dates with analyst reference dates within 
  a specified tolerance window to classify as True/False Positives/Negatives.

  Args:
    df: DataFrame from spatialJoin containing model results and reference data
    theta: Tolerance threshold in days for matching model and reference dates

  Returns:
    tuple: (summary_df, full_df)
      - summary_df: DataFrame with key columns for analysis
      - full_df: Complete DataFrame with all validation metrics (VP, FP, FN, VN)
  """

  # transforma a coluna de delta min para valor absoluto e cria uma nova coluna com o mínimo delta min por ponto
  df.reset_index(inplace = True)
  original_delta_min = df['delta_min'].copy()
  df['delta_min'] = abs(df['delta_min'].fillna(99999)) # substitui os nullos para evitar que sejam os minimos
  df['Min_delta_min'] = df.groupby(['coord_ccdc', 'nome'])['delta_min'].transform('min') # calcula o valor minimo por ponto
  df['delta_min'] = abs(original_delta_min) # retorna o valor absoluto da coluna original
  df['Min_delta_min'] = df['Min_delta_min'].replace(99999,np.nan) # substitui os 99999 por nullos

  bf = df.copy()

  bf['Valid_breaks'] = bf.groupby(['coord_ccdc', 'nome']).transform('count')[['tBreak']] # verifica os breaks validos por pontos
  # SE O TBREAK FOR OBJETO ELE JAMAIS SERA NULO, CONVERTER PARA DATA.
  bf.tBreak = pd.to_datetime(bf.tBreak)
  bf.analistas = bf.analistas.astype(int)
  bf.exists_event = bf.exists_event.astype(int)
  bf.buffer_ID = bf.buffer_ID.astype(int)

  ## ALGUMAS MASCARAS INICIAIS NECESSARIAS
  # mascara dos breaks a mais que analistas ainda em reformulacao

  # PARA O CASO DE TER SOMENTE UM BREAK FP E DOIS ANALISTAS PARA NAO TER DUPLICACAO
  mask = pd.Series(np.zeros(len(bf),dtype=bool), index= bf.index)
  mask.loc[(bf.analistas == 2) & (bf.Valid_breaks < bf.analistas) ] = True #& (bf.delta_min > theta)

  bf.loc[mask, 'Min_delta_min'] = bf.loc[mask].groupby(['coord_ccdc'])['delta_min'].transform('min')

  # Contabilizar
  # colocar todos os VP (delta_min <=31)
  #VP
  bf.loc[( (bf.delta_min <= theta) & (~bf.tBreak.isnull()) & (bf.analistas > 0) ), 'VP'] = 1
  # #FP
  # # sem a condição da magnitude ou (changeProb ==1) serao selecionados os que devem ser negativos
  # bf.loc[( (bf.analistas == 0) & (bf.ndvi_magnitude != 0) & (~bf.tBreak.isnull())), 'FP' ] = 1 #FP puro
  # bf.loc[( (bf.delta_min > theta) & (bf.ndvi_magnitude != 0) & ( (bf.delta_min == bf.Min_delta_min) & (~bf.Min_delta_min.isnull()) ) )  , 'FP' ] = 1
  # bf.loc[( (bf.delta_min > theta) & (bf.ndvi_magnitude != 0) & (bf.analistas == 1)  ) & (~bf.tBreak.isnull()), 'FP' ] = 1
  #FP
  # sem a condição da magnitude ou (changeProb ==1) serao selecionados os que devem ser negativos
  bf.loc[( (bf.analistas == 0) & (~bf.tBreak.isnull())), 'FP' ] = 1 #FP puro
  bf.loc[( (bf.delta_min > theta) & ( (bf.delta_min == bf.Min_delta_min) & (~bf.Min_delta_min.isnull()) ) )  , 'FP' ] = 1
  bf.loc[( (bf.delta_min > theta) & (bf.analistas == 1)  ) & (~bf.tBreak.isnull()), 'FP' ] = 1
  #FN
  bf.loc[( (bf.analistas > 0)  & (bf.tBreak.isnull()) ), 'FN' ] = 1 # FN puro
  # falsos negativos que precisam ser contabilizado para os FPs
  bf.loc[(bf.analistas == 1) & (bf.Valid_breaks == 1) & (bf.FP == 1), 'FN'] = 1 # parece funcionar
  bf.loc[(bf.analistas == 2) & (bf.Valid_breaks == 3) & (bf.FP == 1) , 'FN'] = 1

  #VN
  bf.loc[( (bf.analistas == 0) & (bf.tBreak.isnull()) ), 'VN' ] = 1

  # converter os NaN para 0
  bf[['VP', 'FP', 'FN', 'VN']] = bf[['VP', 'FP', 'FN', 'VN']].fillna(0)

  # verificar os breaks que nao foram classificados
  # para isso gero uma coluna total onde somo todas as metricas, as linhas onde ha 0 nao foram classificadas
  bf['total'] = bf.VP + bf.FP +bf.FN + bf.VN
  mask = pd.Series(np.zeros(len(bf),dtype=bool), index= bf.index) #mascara
  # agrupar por coordenada e t break, assim as somente os breaks que nao foram validados para nenhum analista terao valor 0
  mask.loc[(bf.groupby(['coord_ccdc','tBreak'])['total'].transform('sum')==0) & (bf.analistas == 2) & (bf.Valid_breaks > bf.analistas)] = True
  # neste grupo selecionado devo procurar aquele que tem menor distancia para um analista e classificar como FP
  mask2 = bf[mask].groupby(['coord_ccdc'])['delta_min'].transform('min') == bf.delta_min[mask]
  # agora classificar os candidatos que atendem as duas mascaras
  bf.loc[(mask & mask2), ['FP']] = 1

  # Ajuste FN
  # se for na célula anterior isso contará para o total e a mascara anterior não será feita em alguns pontos onde deve ser feita
  bf.loc[((bf.FP ==1) & (bf.analistas == 1) & (bf.delta_min == bf.Min_delta_min) & (bf.Valid_breaks == 2))   , 'FN' ] = 1
  bf.loc[((bf.FP ==1) & (bf.analistas == 1) & (bf.delta_min == bf.Min_delta_min) & (bf.Valid_breaks == 3))   , 'FN' ] = 1
  bf.loc[(bf.analistas == 2) & (bf.Valid_breaks == 1) & (bf.VP == 0), 'FN'] = 1
  bf.loc[(bf.analistas == 2) & (bf.Valid_breaks == 2) & (bf.FP == 1), 'FN'] = 1
  #return bf
  # Bloco para corrigir o problema de quando as duas datas DGT estão mais próximas do mesmo break
  # listar as coordenadas que tem o problema com mesmo break classificado
  listCoord = list(bf.coord_ccdc[(bf.groupby(['coord_ccdc','tBreak'])['total'].transform('sum') == 0) & (bf.analistas == 2) & (bf.Valid_breaks == 2)])
  #return listCoord
  # dividir o data frame em dois para poder limpar as linhas com problema
  bf_filter = bf.loc[~bf.coord_ccdc.isin(listCoord)].copy()
  # limpeza
  bf_remove_lines = bf.loc[bf.coord_ccdc.isin(listCoord)].copy()
  # zerar todas as métricas para poder recalcular
  bf_remove_lines.loc[:, ['VP','VN','FP', 'FN']] = 0
  #return bf_remove_lines
  bf_removed = bf_remove_lines.groupby(['buffer_ID']).apply(testeRemove).copy() # função de remoção
  #return bf_removed
  try:
    bf_removed = bf_removed.drop(columns=['buffer_ID']).reset_index() # evitar problema de indece dup.
  except:
    pass
  # Agora teremos somente duas linhas por ponto que são obrigatóriamente FP ou VP
  #VP
  bf_removed.loc[( (bf_removed.delta_min <= theta) ), 'VP'] = 1
  #FP, FN
  bf_removed.loc[( (bf_removed.delta_min > theta) ), ['FP', 'FN']] = 1
  # unir os dois dfs novamente
  bf_final = pd.concat([bf_filter, bf_removed])#bf_filter.append(bf_removed)

  # remover aqueles que nao possuem metrica
  bf_final = bf_final[(bf_final.VP > 0) | (bf_final.FP > 0) | (bf_final.FN > 0) | (bf_final.VN > 0) ].copy()
  # remover aqueles que apresentam as classes especificas
  bf_final = bf_final[~(bf_final.tipo.isin(['Agricultura','Agua']))].copy()


  # verificar quais colunas tem magnitude de indices
  mags = [ t for t in bf_final.columns if 'magnitude' in t and not 'B' in t]
  # colunas para retornar um DF mais limpo
  c = ['buffer_ID', 'coord_ccdc', 'changeProb'] + mags + ['tBreak',
       'data1_z', 'analistas', 'nome', 'exists_event', 'Valid_breaks',
       'delta_min', 'Min_delta_min', 'VP', 'FP', 'FN', 'VN'] #geometry
  # também poderá retornar o DF todo classificado, em processo.
  return bf_final[c], bf_final


def testeRemove(groupedby):
  """
  Remove duplicate rows when multiple analyst dates match the same model break.

  Handles cases where two reference dates are equally close to the same detected break
  by keeping only the row with minimum temporal distance.

  Args:
    groupedby: DataFrame group containing rows for a single buffer_ID
    
  Returns:
    DataFrame: Filtered group with duplicate rows removed
  """
  min_delta_min = groupedby['Min_delta_min'].min()
  #remove rows only if there is more than 1 row per point, the number of analyst dates is not zero and min_delta_min is greater than zero.
  if len(groupedby) > 1 and groupedby.analistas.min() > 0 and min_delta_min >= 0:
    # Updated section with check on matching rows
    # Add a check to see if there are any rows matching the condition
    matching_rows = groupedby.loc[groupedby['delta_min']==min_delta_min][['tBreak','data1_z']]
      
    if len(matching_rows) > 0:  # Only proceed if matching rows
      Bj, Ai = matching_rows.values[0]
      mask = ((groupedby['tBreak'] == Bj) | (groupedby['data1_z'] == Ai)) & (groupedby['delta_min']!=min_delta_min)
      groupedby = groupedby[~mask]

  return groupedby


def runValidation(RASTER_FILE, REFERENCE_FILE, POLYGON_FILE, MASK_OUTPUT_PATH, theta):
  """
  Execute complete accuracy assessment workflow for raster-based change detection results.

  Performs the full validation pipeline: reads raster data, converts to DataFrame format,
  performs spatial join with reference data, calculates accuracy metrics, and saves results.

  Args:
    RASTER_FILE: Path to raster file containing change detection dates in YYYYMMDD format
    REFERENCE_FILE: Path to reference dataset shapefile/geopackage for validation
    theta: Tolerance margin for validation in days (integer)

  Returns:
    None - Prints validation metrics to console and saves CSV files:
      - pre_proc.csv: Preprocessed raster data
      - VAL_{raster_name}.csv: Complete validation results
          
  Output metrics:
    - F1-score: Harmonic mean of precision and recall
    - Omission error: False negative rate  
    - Commission error: False positive rate
  """

  print('A correr validação dos resultados do ccd...')
  #pegar data do fim da serie temporal (ultima imagem)

  raster_name = os.path.basename(RASTER_FILE)
  raster_path = os.path.dirname(RASTER_FILE)

  results_path = os.path.join(raster_path, f"{raster_name}_accuracy_assessment")
  if not os.path.exists(results_path):
    os.makedirs(results_path)

  if POLYGON_FILE:
    raster_input = mask_raster_with_polygons(RASTER_FILE, POLYGON_FILE, MASK_OUTPUT_PATH)
  else:
    raster_input = RASTER_FILE
  
  #correr pre-processamento
  csv_s2 = preprocessRaster(raster_input)
  csv_preprocessed_path = os.path.join(results_path, 'pre_proc.csv')
  csv_s2.to_csv(csv_preprocessed_path)

  """## Spatial join
  Faz join dos pontos do csv com a informação de referencia da DGT (300 buffers). É associada aos pontos a informação da validação - data de alteração, tipo, classes, etc.
  """
  #executa o join
  ccdcVal, ccdcVal_T = spatialJoin(REFERENCE_FILE, csv_s2)
  """## Validação
  Faz a validação da deteção - compara resultado do modelo (ccd) com dados de referência DGT
  """ 
  #faz a validação da deteção
  DF_FINAL, DF_FINAL_T = valPol(ccdcVal_T, theta) #funcoes.valPol
  """**Resultados da validação**"""
  #delimita análise apenas para pontos referentes a transições entre Pinheiro Bravo e Eucalipto para Superfície sem vegetação, herbáceas e matos
  #elimina também pontos da bordadura
  df_aux = DF_FINAL_T.copy()
  df_aux = df_aux.loc[(df_aux.altera=="Sem Alteracao")|((df_aux.altera=="Com Alteracao")&(df_aux.classeAnterior.isin(['Pinheiro bravo','Eucalipto']))&(df_aux.classeAtual.isin(['Superficie sem vegetacao escura','Superficie sem vegetacao clara','Vegetacao herbacea espontanea','Matos'])))]
  df_aux = df_aux.loc[df_aux.bordadura==0]
  #imprime f1-score, erro e omissão e erro de comissão
  cm = df_aux.FP.sum()/(df_aux.FP.sum()+df_aux.VP.sum())
  om = df_aux.FN.sum()/(df_aux.FN.sum()+df_aux.VP.sum())
  f1 = 2*(1-om)*(1-cm)/(2-om-cm)
  print("Métricas de validação para ficheiro:")

  print(raster_name)
  print('F1-score = {}%'.format(round(100*f1,2)))
  print('Omission error = {}%'.format(round(100*om,2)))
  print('Commission error = {}%'.format(round(100*cm,2)))

  DF_FINAL_T.to_csv(os.path.join(results_path, f'VAL_{raster_name}.csv'), index=False)


def runBatchValidation(raster_directory, reference_file, polygon_directory=None, 
                      mask_output_directory=None, theta=60):
    """
    Execute accuracy assessment for all raster files in a directory.
    
    Args:
        raster_directory: Directory containing raster files to process
        reference_file: Path to reference dataset (same for all files)
        polygon_directory: Directory containing polygon files for masking (optional)
        mask_output_directory: Directory for saving masked rasters (required if polygon_directory provided)
        theta: Tolerance margin for validation in days
        
    Returns:
        pandas.DataFrame: Summary of results for all processed files
    """
    
    if not os.path.exists(raster_directory):
        raise FileNotFoundError(f"Raster directory not found: {raster_directory}")
    
    # Get all raster files
    raster_files = [f for f in os.listdir(raster_directory) if f.endswith('.tif')]
    
    if not raster_files:
        print(f"No .tif files found in {raster_directory}")
        return None
    
    print(f"Found {len(raster_files)} raster files to process")
    
    # Create master results directory
    master_results_dir = os.path.join(raster_directory, "batch_accuracy_assessment")
    if not os.path.exists(master_results_dir):
        os.makedirs(master_results_dir)
    
    # Track results
    batch_results = []
    failed_files = []
    
    for i, raster_file in enumerate(raster_files, 1):
        print(f"\n{'='*60}")
        print(f"Processing file {i}/{len(raster_files)}: {raster_file}")
        print(f"{'='*60}")
        
        try:
            raster_path = os.path.join(raster_directory, raster_file)
            
            # Find matching polygon file if polygon directory provided
            polygon_path = None
            mask_output_path = None
            
            if polygon_directory:
                polygon_path = find_matching_polygon(raster_file, polygon_directory)
                if polygon_path:
                    print(f"Found matching polygon: {os.path.basename(polygon_path)}")
                    if mask_output_directory:
                        mask_filename = f"mask_{os.path.splitext(raster_file)[0]}.tif"
                        mask_output_path = os.path.join(mask_output_directory, mask_filename)
                    else:
                        print("Warning: MASK_OUTPUT_DIRECTORY not provided, using default location")
                        mask_output_path = None
                else:
                    print(f"No matching polygon found for {raster_file}")
            
            # Run validation for this file
            success = runSingleValidation(raster_path, reference_file, polygon_path, 
                                        mask_output_path, theta, batch_results)
            
            if not success:
                failed_files.append(raster_file)
                
        except Exception as e:
            print(f"Error processing {raster_file}: {str(e)}")
            failed_files.append(raster_file)
            continue
    
    # Save batch summary
    if batch_results:
        summary_df = pd.DataFrame(batch_results)
        
        # Calculate grand totals
        total_VP = summary_df['total_VP'].sum()
        total_FP = summary_df['total_FP'].sum()
        total_FN = summary_df['total_FN'].sum()
        total_VN = summary_df['total_VN'].sum()
        
        # Calculate grand total metrics
        if (total_FP + total_VP) > 0:
            grand_cm = total_FP / (total_FP + total_VP)
        else:
            grand_cm = 0
            
        if (total_FN + total_VP) > 0:
            grand_om = total_FN / (total_FN + total_VP)
        else:
            grand_om = 0
            
        if (2 - grand_om - grand_cm) > 0:
            grand_f1 = 2 * (1 - grand_om) * (1 - grand_cm) / (2 - grand_om - grand_cm)
        else:
            grand_f1 = 0
        
        # Add grand total row
        grand_total_row = {
            'filename': 'GRAND_TOTAL',
            'f1_score': round(100 * grand_f1, 2),
            'omission_error': round(100 * grand_om, 2),
            'commission_error': round(100 * grand_cm, 2),
            'total_VP': total_VP,
            'total_FP': total_FP,
            'total_FN': total_FN,
            'total_VN': total_VN,
            'had_polygon_mask': 'N/A'
        }
        
        # Add grand total row to dataframe
        summary_df = pd.concat([summary_df, pd.DataFrame([grand_total_row])], ignore_index=True)
        
        summary_path = os.path.join(master_results_dir, "batch_summary.csv")
        summary_df.to_csv(summary_path, index=False)
        
        print(f"\n{'='*60}")
        print("BATCH PROCESSING COMPLETE")
        print(f"{'='*60}")
        print(f"Successfully processed: {len(batch_results)} files")
        print(f"Failed: {len(failed_files)} files")
        if failed_files:
            print(f"Failed files: {', '.join(failed_files)}")
        print(f"Results with grand totals saved to: {summary_path}")
        return summary_df
    else:
        print("No files were successfully processed")
        return None


def runSingleValidation(raster_file, reference_file, polygon_file, mask_output_path, theta, batch_results=None):
    """
    Modified version of runValidation that can be called from batch processing.
    
    Args:
        raster_file: Path to single raster file
        reference_file: Path to reference dataset
        polygon_file: Path to polygon file (can be None)
        mask_output_path: Path for masked raster output (can be None)
        theta: Tolerance margin in days
        batch_results: List to append results to (for batch processing)
        
    Returns:
        bool: True if successful, False if failed
    """
    try:
        print('Running validation for raster results...')
        
        raster_name = os.path.basename(raster_file)
        raster_path = os.path.dirname(raster_file)

        results_path = os.path.join(raster_path, f"{raster_name}_accuracy_assessment")
        if not os.path.exists(results_path):
            os.makedirs(results_path)

        if polygon_file:
            raster_input = mask_raster_with_polygons(raster_file, polygon_file, mask_output_path)
        else:
            raster_input = raster_file
        
        # Run preprocessing
        csv_s2 = preprocessRaster(raster_input)
        csv_preprocessed_path = os.path.join(results_path, 'pre_proc.csv')
        csv_s2.to_csv(csv_preprocessed_path)

        # Execute spatial join
        ccdcVal, ccdcVal_T = spatialJoin(reference_file, csv_s2)
        
        # Perform validation
        DF_FINAL, DF_FINAL_T = valPol(ccdcVal_T, theta)
        
        # Calculate metrics
        df_aux = DF_FINAL_T.copy()
        df_aux = df_aux.loc[(df_aux.altera=="Sem Alteracao")|((df_aux.altera=="Com Alteracao")&(df_aux.classeAnterior.isin(['Pinheiro bravo','Eucalipto']))&(df_aux.classeAtual.isin(['Superficie sem vegetacao escura','Superficie sem vegetacao clara','Vegetacao herbacea espontanea','Matos'])))]
        df_aux = df_aux.loc[df_aux.bordadura==0]
        
        cm = df_aux.FP.sum()/(df_aux.FP.sum()+df_aux.VP.sum())
        om = df_aux.FN.sum()/(df_aux.FN.sum()+df_aux.VP.sum())
        f1 = 2*(1-om)*(1-cm)/(2-om-cm)
        
        print("Validation metrics for file:")
        print(raster_name)
        print('F1-score = {}%'.format(round(100*f1,2)))
        print('Omission error = {}%'.format(round(100*om,2)))
        print('Commission error = {}%'.format(round(100*cm,2)))

        DF_FINAL_T.to_csv(os.path.join(results_path, f'VAL_{raster_name}.csv'), index=False)
        
        # Add to batch results if provided
        if batch_results is not None:
            batch_results.append({
                'filename': raster_name,
                'f1_score': round(100*f1, 2),
                'omission_error': round(100*om, 2),
                'commission_error': round(100*cm, 2),
                'total_VP': int(df_aux.VP.sum()),
                'total_FP': int(df_aux.FP.sum()),
                'total_FN': int(df_aux.FN.sum()),
                'total_VN': int(df_aux.VN.sum()),
                'had_polygon_mask': polygon_file is not None
            })
        
        return True
        
    except Exception as e:
        print(f"Error in validation: {str(e)}")
        return False


# Execution logic - check if batch processing or single file
if RASTER_DIRECTORY is not None:
    # Batch processing mode
    print("Running in BATCH MODE")
    runBatchValidation(RASTER_DIRECTORY, REFERENCE_FILE, POLYGON_DIRECTORY, 
                      MASK_OUTPUT_DIRECTORY, theta)
else:
    # Single file mode (original functionality)
    print("Running in SINGLE FILE MODE")
    runValidation(RASTER_FILE, REFERENCE_FILE, POLYGON_FILE, MASK_OUTPUT_PATH, theta)