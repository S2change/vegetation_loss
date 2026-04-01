import torch

print(torch.__version__)
print("gpu", torch.cuda.is_available())

# Check if CUDA is available
print(f"Is CUDA available? {torch.cuda.is_available()}")

# Check the GPU name
if torch.cuda.is_available():
    print(f"GPU Name: {torch.cuda.get_device_name(0)}")
    print(f"CUDA Version: {torch.version.cuda}")



# Check if rasterio can be imported and its version
try:    
    import rasterio
    print(f"Rasterio version: {rasterio.__version__}")  
except ImportError as e:
    print("Rasterio is not installed or cannot be imported.")
    print(str(e))   

try:    
    import pyproj
    print(f"PROJ Data Path: {pyproj.datadir.get_data_dir()}")           
except ImportError as e:
    print("Pyproj is not installed or cannot be imported.")
    print(str(e))   

print(f"CUDA status: {torch.cuda.is_available()}")
print(f"Rasterio version: {rasterio.__version__}")
print(f"PROJ Data Path: {pyproj.datadir.get_data_dir()}")

# Simple GPU test
x = torch.rand(5, 5).cuda()
print("Successfully moved tensor to RTX 2000 Ada!")