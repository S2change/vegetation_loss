"""
This script processes satellite imagery data to extract labeled pixel samples for a given year and tile (e.g., "T29TNE").
It uses reference shapefiles marking areas with and without land cover change to sample valid pixels within those polygons,
extracting spectral bands from corresponding .tif files. The output includes both a CSV file and a Shapefile containing
the sampled pixels, their coordinates, spectral values, and change labels (0 = no change, 1 = change).

Main steps:
1. Load and filter reference shapefiles based on the target year.
2. Sample valid pixels within the polygons using the corresponding raster images.
3. Create a balanced dataset between "change" and "no change" classes.
4. Export the resulting dataset to CSV and Shapefile formats for further analysis.
"""

import os
import rasterio
from shapely.geometry import Point
import geopandas as gpd
import pandas as pd
import numpy as np

# =====================
# INPUTS
# =====================
year = [2018, 2019, 2020, 2021]
tile_name = "T29TNE"
base_dir = r"C:\Users\Public\Documents\Satellite_Embedding_V1"
path_shp_change = os.path.join(r"C:\Users\Public\Documents\ref_datasets\BDR_CCDC_TNE_v3\BDR_CCDC_TNE_v3.shp")
path_shp_nochange = os.path.join(r"C:\Users\Public\Documents\ref_datasets\BDR_CCDC_TNE_v3\BDR_CCDC_TNE_Expanded.gpkg")
random_state = 42
pixels_total = 2500

# Options:
# "nochange" → only no change
# "change"   → only change
# "both"     → no change + change
mode = "change"
#%%
def load_shapefiles(path_shp_change, path_shp_nochange, year):
    
    """
    Load polygons with and without change and filter by target year.

    Inputs:
        path_shp_change (str):
            Path to the polygons with change (e.g., .shp/.gpkg).
        path_shp_nochange (str):
            Path to the polygons without change.
        year (int):
            Target year for filtering (matches both data_0 and data_1).

    Outputs:
        - tuple[geopandas.GeoDataFrame, geopandas.GeoDataFrame]:
            (gdf_change, gdf_nochange)
            - gdf_change: polygons with change in the given year.
            - gdf_nochange: polygons labeled "No change".
    """
    gdf_change = gpd.read_file(path_shp_change)
    gdf_nochange = gpd.read_file(path_shp_nochange)

    gdf_change["ano_0"] = pd.to_datetime(gdf_change["data_0"], errors="coerce").dt.year
    gdf_change["ano_1"] = pd.to_datetime(gdf_change["data_1"], errors="coerce").dt.year
    gdf_change = gdf_change[
        (gdf_change["ano_0"] == year) & (gdf_change["ano_1"] == year)
    ].copy()
    
    # Keep only polygons without change and create an independent copy
    gdf_nochange = gdf_nochange[gdf_nochange["Change"] == "No change"].copy()
    
    # 🔑 Check coluna tipo_1
    colunas = [c.lower() for c in gdf_change.columns]
    if "tipo_1" not in colunas:
        print("Warning: column 'tipo_1' not found. Available columns:", gdf_change.columns.tolist())

    return gdf_change, gdf_nochange
#%%
def to_label(val):
    """
    Convert various attribute values (numbers, strings, NaN) to 0/1 label.

    Inputs:
        val (Any):
            Value from a shapefile attribute. Can be numeric, string, or null.

    Outputs:
        - int:
            0 for "no change", 1 for "change".
    """
    if pd.isna(val):
        return 0
    if isinstance(val, (int, float, np.integer, np.floating)):
        return 0 if int(val) == 0 else 1
    s = str(val).strip().lower()
    positives = {"1", "change", "com alteracao",
                 "alteracao", "há alteração", "ha alteracao"}
    return 1 if s in positives else 0
#%%
def sample_pixels(gdf, label_value, n_pixels_total, tif_paths, random_state, year, max_redistributions=10):
    """
    Sample valid pixels from rasters within given polygons.
    The function maintains detailed progress prints and redistributes quotas
    among polygons when some polygons cannot provide enough pixels.

    Inputs:
        gdf (geopandas.GeoDataFrame):
            Polygons to sample from. Must contain a geometry column and an "ID" field.
        label_value (int):
            Fixed label to assign to sampled pixels (0 = no change, 1 = change).
        n_pixels_total (int):
            Target number of pixels to collect for this group.
        tif_paths (list[str]):
            List of .tif raster file paths to sample from.
        random_state (int):
            Seed for reproducibility of random sampling.
        year (int):
            Year tag stored in the "year" output column.
        max_redistributions (int, default=10):
            Maximum number of redistribution attempts across polygons.

    Outputs:
        - pandas.DataFrame:
            Table with sampled pixels and attributes, with columns:
                - band_0, band_1, ... band_(N-1): spectral bands per pixel
                - x, y (float): coordinate of the sampled pixel
                - label (int): 0/1 as provided by label_value
                - polygon_id (from input polygon "ID" field)
                - year (int): equals year
    """
    if len(gdf) == 0:
        print("No polygons available to process.")
        return pd.DataFrame()

    np.random.seed(random_state)
    pixel_data = []
    total_collected = 0
    gdf = gdf.copy()
    
    # Assign initial quota of pixels per polygon: half of the total pixels go to "no change" and half to "change"
    pixels_per_polygon = max(1, n_pixels_total // len(gdf))
    gdf["pixels_missing"] = pixels_per_polygon
    
    # Initialize the list of polygons to process
    remaining_polygons = gdf
    attempt = 1

    while not remaining_polygons.empty and n_pixels_total > 0 and attempt <= max_redistributions:
        print(f"\nAttempt {attempt} | Remaining pixels: {n_pixels_total} | Candidate polygons: {len(remaining_polygons)}")
        new_remaining = []

        for idx, row in remaining_polygons.iterrows():
            # Stop if global target already reached
            if n_pixels_total <= 0:
                print("\nGlobal target reached. Stopping collection.")
                break
            poly_id = row["ID"]
            pixels_needed = min(int(row["pixels_missing"]), n_pixels_total)
            pixels_collected = 0

            print(f"\n- Polygon ID={poly_id} | Label={label_value} ({'CHANGE' if label_value==1 else 'NO CHANGE'}) | Pixels needed: {pixels_needed} | Remaining pixels: {n_pixels_total}")

            for tif_path in tif_paths:
                with rasterio.open(tif_path) as src:
                    poly_geom = row.geometry
                    if gdf.crs != src.crs:
                        poly_geom = gpd.GeoSeries([row.geometry], crs=gdf.crs).to_crs(src.crs).iloc[0]
                    if not poly_geom.is_valid:
                        poly_geom = poly_geom.buffer(0)
                    
                    # Get raster indices bounding the polygon
                    minx, miny, maxx, maxy = poly_geom.bounds
                    row_min, col_min = src.index(minx, maxy)
                    row_max, col_max = src.index(maxx, miny)
                    row_min, row_max = max(0, min(row_min, src.height-1)), max(0, min(row_max, src.height-1))
                    col_min, col_max = max(0, min(col_min, src.width-1)), max(0, min(col_max, src.width-1))
                    
                    # Skip if polygon outside raster
                    if row_max < row_min or col_max < col_min:
                        print(f"   Polygon is outside raster {os.path.basename(tif_path)}")
                        continue
                    
                    # Generate more random points than needed, then filter inside polygon
                    n_try = pixels_needed * 10
                    rr = np.random.randint(row_min, row_max + 1, size=n_try)
                    cc = np.random.randint(col_min, col_max + 1, size=n_try)
                    points = [Point(src.xy(r, c)) for r, c in zip(rr, cc)]
                    mask_inside = [poly_geom.contains(pt) for pt in points]

                    if sum(mask_inside) == 0:
                        continue
                    
                    # Keep only points inside polygon and limit to required number
                    rr_in, cc_in = np.array(rr)[mask_inside], np.array(cc)[mask_inside]
                    n_take = min(pixels_needed - pixels_collected, len(rr_in))
                    rr_in, cc_in = rr_in[:n_take], cc_in[:n_take]
                    
                    # Read raster values for selected pixels
                    data = src.read()[:, rr_in, cc_in].T
                    mask_valid = np.all(np.isfinite(data), axis=1)
                    data = data[mask_valid]
                    rr_in, cc_in = rr_in[mask_valid], cc_in[mask_valid]

                    if len(data) == 0:
                        print(f"   No valid pixels in {os.path.basename(tif_path)} after filtering.")
                        continue
                    
                    # Store sampled pixels in DataFrame
                    df = pd.DataFrame(data, columns=[f"band_{i}" for i in range(data.shape[1])])
                    xs, ys = src.xy(rr_in, cc_in)
                    df["x"] = xs
                    df["y"] = ys
                    df["label"] = label_value
                    df["polygon_id"] = poly_id
                    df["year"] = year
                    
                    if "tipo_1" in gdf.columns:
                        df["tipo"] = row["tipo_1"]
                    else:
                        df["tipo"] = None
                    

                    pixel_data.append(df)
                    pixels_collected += len(df)
                    total_collected += len(df)
                    n_pixels_total -= len(df)

                    print(f"   Raster {os.path.basename(tif_path)} ➜ {len(df)} pixels collected | Polygon subtotal: {pixels_collected} | Global total: {total_collected}")
                    
                    # Stop if polygon quota reached
                    if pixels_collected >= pixels_needed:
                        break
            
            # Handle deficit: if polygon did not provide all its pixels, keep for next attempt
            deficit = pixels_needed - pixels_collected
            if deficit > 0:
                print(f"   Polygon ID={poly_id} provided {pixels_collected} pixels; {deficit} missing. Will be retried.")
                row["pixels_missing"] = max(1, deficit)
                new_remaining.append(row)
            else:
                if n_pixels_total > 0:
                    print(f"   Polygon ID={poly_id} reached its quota, but global target not met. It will remain available.")
                    row["pixels_missing"] = max(1, pixels_needed)
                    new_remaining.append(row)
                else:
                    print(f"   Polygon ID={poly_id} reached its quota and no more pixels are needed.")
        
        # Update remaining polygons and increment attempt counter
        remaining_polygons = gpd.GeoDataFrame(new_remaining, columns=gdf.columns)
        attempt += 1

    print(f"\nTotal pixels collected: {total_collected}")
    # Combine all sampled pixels into a single DataFrame
    return pd.concat(pixel_data, ignore_index=True) if pixel_data else pd.DataFrame()
#%%
def save_outputs(result_df, base_dir, tile_name, year, tif_paths, gdf_change, mode=None):
    """
    Persist results to CSV and Shapefile.

    Inputs:
        result_df (pandas.DataFrame):
            Sampled pixels and attributes (bands, coordinates, labels).
        base_dir (str):
            Base directory for writing outputs.
        tile_name (str):
            Tile identifier (e.g., "T29TNE").
        year (int):
            Processed year; used in output filenames (skipped if mode="nochange").
        tif_paths (list[str]):
            List of rasters; used to derive CRS if available.
        gdf_change (geopandas.GeoDataFrame):
            Fallback source of CRS if no raster is available.
        mode (str, default="both"):
            "nochange", "change", or "both" – affects filename.

    Outputs:
        - Writes CSV and Shapefile with adjusted naming.
    """
    if mode == "nochange":
        out_csv = os.path.join(base_dir, f"embedding_{tile_name}_sample_nochange.csv")
        out_shp = os.path.join(base_dir, f"embedding_{tile_name}_sample_points_nochange.shp")
    else:
        out_csv = os.path.join(base_dir, f"embedding_{tile_name}_{year}_sample.csv")
        out_shp = os.path.join(base_dir, f"embedding_{tile_name}_{year}_sample_points.shp")

    result_df.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"\nCSV saved to: {out_csv}")

    output_crs = None
    if tif_paths:
        with rasterio.open(tif_paths[0]) as src:
            output_crs = src.crs
    if output_crs is None:
        output_crs = gdf_change.crs

    gdf_points = gpd.GeoDataFrame(
        result_df,
        geometry=gpd.points_from_xy(result_df.x, result_df.y),
        crs=output_crs
    )
    gdf_points.to_file(out_shp, driver="ESRI Shapefile", encoding="utf-8")
    print(f"Shapefile saved to: {out_shp}")
#%%
def process_tile(year, tile_name, base_dir, path_shp_change, path_shp_nochange, random_state, pixels_total, mode=None):
    """
    Execute the full processing workflow for a given year and tile.

    Inputs:
        year (int):
            Year to process.
        tile_name (str):
            Tile identifier (e.g., "T29TNE").
        base_dir (str):
            Base directory containing the yearly raster folders.
        path_shp_change (str):
            Path to polygons with change.
        path_shp_nochange (str):
            Path to polygons without change.
        random_state (int):
            Seed for reproducible sampling.
        pixels_total (int):
            Global target number of pixels to sample.
        
        mode (str, default="both"):
            Defines which type of polygons to sample:
                - "nochange": only from no-change polygons (label=0)
                - "change":   only from change polygons (label=1)
                - "both":     balanced between no-change and change

    Outputs:
        - pandas.DataFrame:
            Final concatenated DataFrame (possibly downsampled to pixels_total),
            with columns band_*, x, y, label, polygon_id, year.

        - Saves CSV and Shapefile to disk.
    """
    # 1) Load polygons
    gdf_change, gdf_nochange = load_shapefiles(path_shp_change, path_shp_nochange, year)

    # 2) Discover TIFF rasters for the given tile and year
    tif_dir = os.path.join(base_dir, tile_name, str(year))
    tif_paths = [os.path.join(tif_dir, f) for f in os.listdir(tif_dir) if f.lower().endswith(".tif")]

    # 3) Sampling logic
    if mode == "both":
        target_nochange = pixels_total // 2
        target_change = pixels_total - target_nochange
    
        print(f"\n--- Sampling {target_nochange} pixels NO CHANGE ---")
        df_nochange = sample_pixels(gdf_nochange, 0, target_nochange, tif_paths, random_state, year)
    
        print(f"\n--- Sampling {target_change} pixels CHANGE ---")
        df_change = sample_pixels(gdf_change, 1, target_change, tif_paths, random_state, year)
    
        result_df = pd.concat([df_nochange, df_change], ignore_index=True)
    
    elif mode == "nochange":
        print(f"\n--- Sampling {pixels_total} pixels NO CHANGE ---")
        result_df = sample_pixels(gdf_nochange, 0, pixels_total, tif_paths, random_state, year)
    
    elif mode == "change":
        print(f"\n--- Sampling {pixels_total} pixels CHANGE ---")
        result_df = sample_pixels(gdf_change, 1, pixels_total, tif_paths, random_state, year)
    
    else:
        raise ValueError("Invalid mode. Use 'nochange', 'change', or 'both'.")

    # 5) Concatenate and adjust to the exact global target if needed
    if len(result_df) > pixels_total:
        result_df = result_df.sample(n=pixels_total, random_state=random_state).reset_index(drop=True)

    # 6) Persist outputs
    save_outputs(result_df, base_dir, tile_name, year, tif_paths, gdf_change, mode=mode)
    return result_df
#%%
def main():
    print(f"\n=== Processing year {year} ===")
    process_tile(
        year=year,
        tile_name=tile_name,
        base_dir=base_dir,
        path_shp_change=path_shp_change,
        path_shp_nochange=path_shp_nochange,
        random_state=random_state,
        pixels_total=pixels_total,
        mode=mode
        )

if __name__ == "__main__":
    main()
