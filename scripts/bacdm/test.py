import sys
import torch
import os
import shutil
from torch.utils.data import DataLoader
import torchvision #Always import torch before rasterio in your scripts.
import time
import rasterio
import numpy as np
import AAA_Configs
from bacdm.data.dataset_swin_GZ import MyTestData
from bacdm.swin_ynet import Encoder

# current time
current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print("Current Time:", current_time)

#model = Encoder().cuda()
#model.load_state_dict(torch.load(AAA_Configs.Test_weight_path))

if AAA_Configs.USE_CUDA:
    model = Encoder(num_classes=AAA_Configs.NUM_CLASSES).cuda()
    model.load_state_dict(torch.load(AAA_Configs.Test_weight_path, map_location='cuda'))
else:
    model = Encoder(num_classes=AAA_Configs.NUM_CLASSES)
    model.load_state_dict(torch.load(AAA_Configs.Test_weight_path, map_location='cpu'))


im_path1 = AAA_Configs.Test_im_pathA
im_path2 = AAA_Configs.Test_im_pathB
outPath = AAA_Configs.Test_det_path

if os.path.exists(outPath):
    shutil.rmtree(outPath)
os.mkdir(outPath)

test_loader = DataLoader(MyTestData(im_path1, im_path2), shuffle=False, batch_size=1)
with torch.no_grad():
    model = model.eval()
    for i, (im1, im2, label_name,orig_path) in enumerate(test_loader):
        if AAA_Configs.USE_CUDA: 
            im1 = im1.cuda()    
            im2 = im2.cuda()
        label_name = label_name[0]
        ''' adapt to more than 2 classes (see below)
        outputs = model(im1, im2)
        outputs = outputs[0][0]
        a = outputs[0].unsqueeze(0)
        # saves to png format, with names corresponding to the input tifs
        # torchvision.utils.save_image(a, outPath + '/%s' % label_name)
        # saves to geotiff format, with names corresponding to the input tifs
        image_np = a.detach().cpu().numpy()
        '''

        # 1. Get model prediction
        outputs = model(im1, im2)
        
        # 2. Extract the high-resolution output (the first item in the output list)
        # Shape is [Batch, Classes, H, W] -> [1, 5, 256, 256]
        output_high_res = outputs[0]

        # 3. Find the winning class for each pixel (0, 1, 2, 3, or 4)
        # argmax along the 'Classes' dimension (dim=1)
        # result shape: [1, 256, 256]
        prediction = torch.argmax(output_high_res, dim=1)

        # 4. Remove the batch dimension to get a 2D image [256, 256]
        # and move to CPU/Numpy
        image_np = prediction.squeeze(0).detach().cpu().numpy()
        
        # try one or the other of these approaches to handle nodata values, to see which gives better results (the resampling/smoothing in split_tifs.py should help make the 10m bands more similar to the original BACDM data and reduce noise, but it may still be worth trying both approaches to see if one gives better results than the other)
        image_np = np.nan_to_num(image_np, nan=255)
        #with rasterio.open(orig_path) as src:
        #    image_np[image_np == src.nodata] = 255 # scr.nodata==255.0

        # 5. Convert to uint8 (important for GeoTIFF and storage)
        image_np = image_np.astype('uint8')

        # 6. Save as GeoTIFF with geospatial metadata from the original input TIFF (assumption: read the geospatial metadata from the original input TIFF and apply it to the output GeoTIFF, so that the output GeoTIFF is properly georeferenced and can be used in GIS software or for further geospatial analysis; this also ensures that the output GeoTIFF has the same spatial resolution, coordinate reference system, and geotransform as the original input TIFF, which is important for accurate spatial alignment and analysis)
        orig_path = orig_path[0] 

        # 1. Use rasterio to grab the geospatial profile (CRS + Transform)
        with rasterio.open(orig_path) as src:
            profile = src.profile.copy()

        # 2. Update profile for your specific output
        profile.update(
            dtype='uint8',
            count=1,
            compress='lzw',
            nodata=None  # This removes the NoData definition, so all pixels will be valid data values (0-255) and there will be no special NoData value. This is important to avoid issues with some models that can arise when there are large uniform areas of a specific value (e.g., 255 or 0) defined as NoData, which can cause the model to struggle with those areas. By not defining any NoData value and ensuring all pixels are valid data values, we can help the model learn from the full range of pixel values without being confused by large uniform NoData areas.
        )

        # 3. Save as a true GeoTIFF
        out_tif_name = label_name.replace('.png', '.tif')
        out_tif_path = os.path.join(outPath, out_tif_name)
        
        with rasterio.open(out_tif_path, 'w', **profile) as dst:
            # Rasterio expects (Bands, Height, Width)
            dst.write(image_np.astype('uint8'),1) # Write to the first band

# time elapsed
elapsed_time = time.time() - time.mktime(time.strptime(current_time, "%Y-%m-%d %H:%M:%S"))
print("Elapsed Time:", elapsed_time, "seconds") 