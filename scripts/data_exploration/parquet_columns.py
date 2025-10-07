import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import pandas as pd
from scripts.ccd_results_utils.segment_identification import combine_parquet_files

# Show all rows and columns
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# Parquet file path
file_path = "/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/s2_images-NDVI_XX999YM1NOBS6LDA2ITER1000_START20170408_END20241229_ROIDGT_rank_0.parquet"

# Constants for Specific Coordinate Rows
directory_path = "/Users/domwelsh/green_ds/Thesis/BDR_300_artigo/"
x_coord = 512555
y_coord = 4394445


def show_parquet_columns(file_path):
    """Display column headers of a parquet file."""
    df = pd.read_parquet(file_path)
    print(f"Column headers in {file_path}:")
    print("-" * 50)
    for i, col in enumerate(df.columns, 1):
        print(f"{i}. {col}")
    print("-" * 50)
    print(f"Total columns: {len(df.columns)}")
    print("\nExample rows:")
    print("-" * 50)
    print(df.head())

def last_row_changeProb_100(file_path):
    """
    Return all rows for x-y coordinate pairs where the row with the greatest tEnd value has changeProb = 100.
    """
    df = pd.read_parquet(file_path)

    # Find the row with the maximum tEnd value for each x-y pair
    idx = df.groupby(['x_coord', 'y_coord'])['tEnd'].idxmax()

    # Get those rows and filter where changeProb == 100
    max_tEnd_rows = df.loc[idx]
    xy_pairs_with_changeProb_100 = max_tEnd_rows[max_tEnd_rows['changeProb'] == 100][['x_coord', 'y_coord']]

    # Get all rows for these x-y coordinate pairs
    result = df.merge(xy_pairs_with_changeProb_100, on=['x_coord', 'y_coord'], how='inner')

    return result

def all_rows_specific_coord(directory_path, x_coord, y_coord):
    """
    Identify a specific pixel coordinate in a tile and return all segments
    """

    df = combine_parquet_files(directory_path)
    pixel_df = df[(df['x_coord'] == x_coord) & (df['y_coord'] == y_coord)]

    return pixel_df


if __name__ == "__main__":
    # show_parquet_columns(file_path)

    # print("\n\nRows for x-y pairs where tEnd has changeProb = 100:")
    # print("=" * 50)
    # filtered_df = last_row_changeProb_100(file_path)
    # print(filtered_df)

    print(f"\n\nRows for x-y pair {x_coord}, {y_coord}\n")
    single_pixel_df = all_rows_specific_coord(directory_path, x_coord, y_coord)
    print(single_pixel_df)