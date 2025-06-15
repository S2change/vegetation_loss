Scripts to download S2 data from GEE:
1. `gee_download_S2_tile_36_parts.py`: to download a complete tile from GEE, apply S2cloudness, from date_start to date_end. The script clips the downloaded images using the geometry defined in the GeoPackage portugal_continental_32629.gpkg
2. `gee_download_S2_from_tile_and_polygon.py` : download S2 bands for a given vectorial mask and tile, from date_start to date_end
3. `gee_download_ndvi_from_rectangle.py` to read Sentinel-2 data for a given rectangle between date_star and date_end

