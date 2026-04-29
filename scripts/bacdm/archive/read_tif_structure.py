import rasterio

# Update this path to your specific .tif file
file_path = r"C:\Users\mlc\OneDrive - Universidade de Lisboa\Documents\investigacao-projectos-reviews-alunos-juris\projetos\DGT-S2CHANGE_2023\repos\vegetation_loss\scripts\bacdm\data\after\2019_10000032_2.tif"

try:
    with rasterio.open(file_path) as dataset:
        # Get dimensions
        width = dataset.width
        height = dataset.height
        
        # Get number of bands
        bands = dataset.count
        
        # Get data type (e.g., uint8, float32)
        dtype = dataset.dtypes[0]

        print(f"File: {file_path.split('\\')[-1]}")
        print(f"{'-'*30}")
        print(f"Width:         {width} px")
        print(f"Height:        {height} px")
        print(f"Total Bands:   {bands}")
        print(f"Data Type:     {dtype}")
        print(f"{'-'*30}")

except Exception as e:
    print(f"Error opening file: {e}")