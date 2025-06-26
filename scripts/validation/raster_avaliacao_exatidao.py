"""
Script that conducts accuracy assessment of change detection results from raster data.

Inputs:
    RASTER_FILE: path to raster file containing change detection dates in YYYYMMDD format
    BDR_DGT: path to the shapefile/geopackage of the reference dataset used for validation 

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

RASTER_FILE = r'/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/processed_outputs/BDR_300_artigo_tol30_05ha_rasters/BDR_300_artigo_202301-202302.tif'
BDR_DGT = r'/Users/domwelsh/green_ds/Thesis/BDR_TNE_300/BDR_CCDC_TNE_Adjusted.shp'

import os
from datetime import datetime
import pandas as pd
import numpy as np
import geopandas as gpd
import rasterio
import rasterio.transform

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


def runValidation(RASTER_FILE, BDR_DGT, theta):
  """
  Execute complete accuracy assessment workflow for raster-based change detection results.

  Performs the full validation pipeline: reads raster data, converts to DataFrame format,
  performs spatial join with reference data, calculates accuracy metrics, and saves results.

  Args:
      RASTER_FILE: Path to raster file containing change detection dates in YYYYMMDD format
      BDR_DGT: Path to reference dataset shapefile/geopackage for validation
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
  
  #correr pre-processamento
  csv_s2 = preprocessRaster(RASTER_FILE)
  csv_preprocessed_path = os.path.join(results_path, 'pre_proc.csv')
  csv_s2.to_csv(csv_preprocessed_path)

  """## Spatial join
  Faz join dos pontos do csv com a informação de referencia da DGT (300 buffers). É associada aos pontos a informação da validação - data de alteração, tipo, classes, etc.
  """
  #executa o join
  ccdcVal, ccdcVal_T = spatialJoin(BDR_DGT, csv_s2)
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
#%%


runValidation(RASTER_FILE, BDR_DGT, theta)