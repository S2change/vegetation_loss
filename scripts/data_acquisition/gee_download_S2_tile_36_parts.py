"""
This script performs the download, processing, cloud/shadow masking, and mosaic creation of Sentinel-2 (S2) images More actions
for a specific tile using the Google Earth Engine (GEE) API.

The processing steps include:
1. **Image Filtering**: Selection of images from the Sentinel-2 SR (Surface Reflectance) collection based on a defined date range and tile.
2. **Cloud/Shadow Masking**: Uses the S2_CLOUD_PROBABILITY (s2cloudless) collection to mask areas affected by clouds and shadows.
3. **Parallel Download**: Images are divided into subtiles (6x6) and transferred in parallel in GeoTIFF format.
4. **Mosaic Creation**: Subtiles are combined into mosaics and clipped using a shapefile of mainland Portugal (GPKG).
5. **NODATA Handling**: Pixels with value 0 across all bands are set to NODATA (value 65535).

Dependencies: Earth Engine Python API, Rasterio, GeoPandas, NumPy, Requests

Main configurations (defined in the inputs):
- Sentinel-2 tile to be processed (e.g., 'T29SPD')
- Date range for image selection
- Selected bands
- Path to the GeoPackage containing geographic boundaries for clipping

Performance: The script uses parallel processing (up to 24 simultaneous processes) to speed up image download and export.
"""
# -*- coding: utf-8 -*- 
import ee 
import os 
import requests 
from concurrent.futures import ProcessPoolExecutor  # Importação de ProcessPoolExecutor
import time 
import rasterio 
from rasterio.merge import merge 
from rasterio.mask import mask 
import glob 
import geopandas as gpd 
import shutil
from datetime import datetime
import numpy as np

# Registra a hora de início
hora_inicio = datetime.now()

# Exibe a hora formatada
print("Hora de inicio do script:", hora_inicio.strftime("%H:%M:%S"), flush=True)

# ---------------------------------
#             INPUTS
# ---------------------------------
tile_to_test = 'T29SPD'  # Escolher o tile
date_start = '2017-01-01'  # Escolher a data inicial para fazer o download das imagens S2
date_end = '2024-12-31'  # Escolher a data final para fazer o download das imagens S2
bandas = ['B3', 'B4', 'B8', 'B12']  # 12 bandas S2
NODATA=65535
# ---------------------------------
#   Inicialização do Earth Engine
# ---------------------------------
ee.Initialize(project='ee-testeccd1234')

# ---------------------------------
#       Funções Auxiliares
# ---------------------------------
def addNDVI(image):
    ndvi = image.normalizedDifference(['B8', 'B4']).multiply(10000).int16()
    ndvi_clipped = ndvi.expression(
        '((ndvi < 0) ? 0 : (ndvi >= 5000 ? 5000 : ndvi))',
        {'ndvi': ndvi}
    ).int16()
    return image.addBands(ndvi_clipped.rename('ndvi'))

def filterS2cloudless(S2SRCol, S2CloudCol):
    CLOUD_FILTER = 60
    CLD_PRB_THRESH = 50
    NIR_DRK_THRESH = 0.2
    CLD_PRJ_DIST = 1
    BUFFER = 50

    S2SRCol = S2SRCol.filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', CLOUD_FILTER))

    joined = ee.ImageCollection(ee.Join.saveFirst('s2cloudless').apply(
        primary=S2SRCol,
        secondary=S2CloudCol,
        condition=ee.Filter.equals(
            leftField='system:index',
            rightField='system:index'
        )))

    def add_cloud_bands(img):
        cld_prb = ee.Image(img.get('s2cloudless')).select('probability')
        is_cloud = cld_prb.gt(CLD_PRB_THRESH).rename('clouds')
        return img.addBands(ee.Image([cld_prb, is_cloud]))

    def add_shadow_bands(img):
        not_water = img.select('SCL').neq(6)
        SR_BAND_SCALE = 1e4
        dark_pixels = img.select('B8').lt(NIR_DRK_THRESH*SR_BAND_SCALE).multiply(not_water).rename('dark_pixels')
        shadow_azimuth = ee.Number(90).subtract(ee.Number(img.get('MEAN_SOLAR_AZIMUTH_ANGLE')))
        cld_proj = (img.select('clouds').directionalDistanceTransform(shadow_azimuth, CLD_PRJ_DIST*10)
            .reproject(crs=img.select(0).projection(), scale=100)
            .select('distance')
            .mask()
            .rename('cloud_transform'))
        shadows = cld_proj.multiply(dark_pixels).rename('shadows')
        return img.addBands(ee.Image([dark_pixels, cld_proj, shadows]))

    def add_cld_shdw_mask(img):
        img_cloud = add_cloud_bands(img)
        img_cloud_shadow = add_shadow_bands(img_cloud)
        is_cld_shdw = img_cloud_shadow.select('clouds').add(img_cloud_shadow.select('shadows')).gt(0)
        is_cld_shdw = (is_cld_shdw.focalMin(2).focalMax(BUFFER*2/20)
            .reproject(crs=img.select([0]).projection(), scale=20)
            .rename('cloudmask'))
        return img_cloud_shadow.addBands(is_cld_shdw)

    def apply_cld_shdw_mask(img):
        not_cld_shdw = img.select('cloudmask').Not()
        return img.updateMask(not_cld_shdw).unmask(NODATA)

    s2_sr = joined.map(add_cld_shdw_mask).map(apply_cld_shdw_mask)

    return s2_sr

def getImageCollection(params_ImgCol):
    S2 = ee.ImageCollection(params_ImgCol['nameImage']).filterDate(params_ImgCol['date_start'], params_ImgCol['date_end'])

    if params_ImgCol['cloudFilter'] == 's2cloudless':
        s2_cloudless_col = (ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY')
                            .filterDate(params_ImgCol['date_start'], params_ImgCol['date_end']))
        S2filtered = filterS2cloudless(S2, s2_cloudless_col)
    elif params_ImgCol['cloudFilter'] == 'NoFilter':
        S2filtered = S2

    if 'ndvi' in map(str.lower, params_ImgCol['indices']):
        S2filtered = S2filtered.map(addNDVI)

    return S2filtered

def addDateBand(image):
    dateBand = ee.Image(ee.Date(image.date()).millis()).rename('image_date')
    return image.addBands(dateBand.toInt64())

def download_image(image, fileName, base_folder, tile, date_millis):
    try:
        url = image.getDownloadURL({
            'scale': 10,
            'crs': 'EPSG:32629',
            'region': image.geometry(),
            'format': 'GeoTIFF',
            "nodata": NODATA
        })
        
        response = requests.get(url)
        
        if response.status_code == 200:
            tile_folder = os.path.join(base_folder, tile)
            date_folder = os.path.join(tile_folder, str(date_millis))
            if not os.path.exists(date_folder):
                os.makedirs(date_folder)
                
            file_path = os.path.join(date_folder, fileName)
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            print(f"Image {fileName} downloaded successfully to {file_path}.", flush=True)
        else:
            print(f"Failed to download image {fileName}. Status code: {response.status_code}", flush=True)
        
        # Atraso após cada download
        time.sleep(2)

    except Exception as e:
        print(f"Error downloading image {fileName}: {str(e)}", flush=True)

def exportImageForSingleImage(image, i, tile, base_folder):
    try:
        # Extração de informações da imagem
        geometry = image.geometry()
        date_millis = image.date().getInfo()['value']
        bounds = geometry.bounds()
        coords = bounds.getInfo()
        xmin, ymin, xmax, ymax = coords['coordinates'][0][0][0], coords['coordinates'][0][0][1], coords['coordinates'][0][2][0], coords['coordinates'][0][2][1]

        step_x = (xmax - xmin) / 6
        step_y = (ymax - ymin) / 6
        regions = []
        
        for i in range(6):
            for j in range(6):
                top_left = ee.Geometry.Rectangle(xmin + i * step_x, ymin + j * step_y, xmin + (i + 1) * step_x, ymin + (j + 1) * step_y)
                regions.append((top_left, f'part_{i * 6 + j + 1}'))

        for region, label in regions:
            image_part = image.clip(region)

            if image_part.bandNames().getInfo():
                fileName = f'S2SR_image_{label}_{date_millis}_tile_{tile}.tif'
                # Faz o download de cada parte da imagem
                download_image(image_part, fileName, base_folder, tile, date_millis)

    except Exception as e:
        print(f'Error exporting image {i} for tile {tile}: {str(e)}')

def combine_tiffs_to_mosaic(input_folder, output_folder, geopackage_path, date_millis):
    tiff_files = glob.glob(os.path.join(input_folder, f"*{date_millis}*.tif"))
    
    if not tiff_files:
        print("No TIFF files found for the specified date.", flush=True)
        return
    
    print(f"Found {len(tiff_files)} TIFF files for the date {date_millis}. Combining them into a mosaic...", flush=True)
    
    # Abrir os arquivos TIFF e combiná-los num mosaico
    src_files_to_mosaic = [rasterio.open(file) for file in tiff_files]
    mosaic, out_transform = merge(src_files_to_mosaic)
    
    # Copiar os metadados do primeiro ficheiro TIFF
    out_meta = src_files_to_mosaic[0].meta.copy()
    out_meta.update({
        "driver": "GTiff",
        "height": mosaic.shape[1],
        "width": mosaic.shape[2],
        "transform": out_transform,
        "nodata": NODATA,
        "compress": "LZW"
    })
    
    # Fechar os arquivos originais
    for src in src_files_to_mosaic:
        src.close()
    
    # Criar a pasta de saída, se não existir
    os.makedirs(output_folder, exist_ok=True)
    
    # Salvar o mosaico completo antes do corte
    output_file = os.path.join(output_folder, f"S2SR_image_{date_millis}.tif")
    with rasterio.open(output_file, "w", **out_meta) as dest:
        dest.write(mosaic)

    # Carregar o GeoPackage e extrair a geometria
    gdf = gpd.read_file(geopackage_path)
    geometries = gdf.geometry.values
    
    try:
        # Abrir o arquivo salvo para aplicar a máscara
        with rasterio.open(output_file, "r") as src:
            out_image, out_transform = mask(
                src,
                geometries,
                crop=True,
                nodata=NODATA,
                all_touched=False,
                filled=False
            )
        
        # Se o resultado do mask não tem pixels válidos, significa que estava fora do GeoPackage
        if np.all(out_image == NODATA):
            print(f"Mosaic is completely outside the GeoPackage area. Deleting output folder: {output_folder}", flush=True)
            shutil.rmtree(input_folder, ignore_errors=True)  # Apaga a pasta
            return

        # Atualizar os metadados com a nova extensão da imagem recortada
        out_meta.update({
            "height": out_image.shape[1],
            "width": out_image.shape[2],
            "transform": out_transform
        })

        # Definir pixels com zero em todas as bandas como NoData
        mask_nodata = np.all(out_image == 0, axis=0)  # Verifica onde todas as bandas são zero
        out_image[:, mask_nodata] = NODATA  # Define esses pixels como NoData

        # Salvar o mosaico final recortado
        with rasterio.open(output_file, "w", **out_meta) as dest:
            dest.write(out_image)
        
        print(f"Mosaic saved to {output_file}.", flush=True)
        
        print(f"Removing subtiles folder: {input_folder}", flush=True)
        shutil.rmtree(input_folder, ignore_errors=True)

    except ValueError:
        # Se o raster não tem interseção com o geopackage, apagar a pasta de saída
        print(f"No valid data found inside the GeoPackage area. Deleting output folder: {output_folder}", flush=True)
        shutil.rmtree(input_folder, ignore_errors=True)

def process_images_in_parallel(imageList, tile_to_test, base_folder):
    with ProcessPoolExecutor(max_workers=24) as executor:
        futures = []


        for i in range(imageList.size().getInfo()):
            image = ee.Image(imageList.get(i))
            futures.append(executor.submit(exportImageForSingleImage, image, i, tile_to_test, base_folder))

        for future in futures:
            future.result() 

def process_and_mosaic_images(imageList, tile_to_test, base_folder):
    num_images = imageList.size().getInfo()  # número de imagens na coleção

    # Processar as imagens em paralelo
    print("Processing all images in parallel...")
    process_images_in_parallel(imageList, tile_to_test, base_folder)

    # Após o processamento paralelo, criar os mosaicos
    for i in range(num_images):
        image = ee.Image(imageList.get(i)) 
        date_millis = image.date().getInfo()['value']  # Obter a data em milissegundos

        print(f"Generating mosaic for image {i + 1} of {num_images} for tile {tile_to_test}.", flush=True)
        
        # Criar caminhos para guardar e combinar o mosaico
        tile_folder = os.path.join(base_folder, tile_to_test)
        mosaic_folder = os.path.join(tile_folder, str(date_millis))
        geopackage_path = r"C:/Users/scaetano/Downloads/portugal_continental.gpkg"

        # Combinar as imagens e gerar o mosaico
        combine_tiffs_to_mosaic(mosaic_folder, tile_folder, geopackage_path, date_millis)

    print("All images processed and mosaics generated.", flush=True)

# ---------------------------------
# EXECUÇÃO PRINCIPAL
# ---------------------------------
if __name__ == '__main__':
    try:
        # start_time = time.time()
        base_folder = 'D:\s2_images'
        params_ImgCol = {
            'nameImage': "COPERNICUS/S2_SR_HARMONIZED",
            'date_start': date_start,
            'date_end': date_end,
            'indices': ['ndvi'],
            'cloudFilter': 's2cloudless',
            'bandas': bandas,
            'banda': 'ndvi'
        }
    
        s2_col_filtered = getImageCollection(params_ImgCol)
        s2_col_with_date = s2_col_filtered.map(addDateBand)
        s2_col_sorted = s2_col_with_date.sort('image_date')
    
        s2_col_selection = s2_col_sorted.select(bandas)
        s2_col_selection = s2_col_selection.filterMetadata('MGRS_TILE', 'EQUALS', tile_to_test[1:])
    
        print(f'Number of images selected for tile {tile_to_test}: {s2_col_selection.size().getInfo()}', flush=True)
        max_workers=24
        print(f'Usando {max_workers} CPUs ', flush=True)
    
        imageList = s2_col_selection.toList(s2_col_selection.size())
    
        process_and_mosaic_images(imageList, tile_to_test, base_folder)
    
        end_time = datetime.now()
        print("Hora de fim do script:", end_time.strftime("%H:%M:%S"), flush=True)
        # print(f"Total execution time: {end_time - start_time:.2f} seconds.", flush=True)
    
    except Exception as e:
        print(f'Error: {e}')
