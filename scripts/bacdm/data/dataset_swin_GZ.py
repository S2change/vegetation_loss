import torch
from torch.utils.data import Dataset
from torchvision import transforms
from PIL import Image
import os
import imageio.v2 as imageio
import AAA_Configs
import tifffile
import numpy as np

class MyData(Dataset):
    def __init__(self, im_path1, im_path2, lb_path):
        super(MyData, self).__init__()
        self.train_im_path1 = im_path1
        self.train_im_path2 = im_path2
        self.train_lb_path = lb_path

        # Filter for .tif files only
        self.train_imgs1 = sorted([f for f in os.listdir(im_path1) if f.lower().endswith('.tif')])
        self.train_imgs2 = sorted([f for f in os.listdir(im_path2) if f.lower().endswith('.tif')])
        
        # Labels might be .png or .tif, adjust extension check if necessary
        self.train_labels = sorted([f for f in os.listdir(lb_path) if f.lower().endswith(('.tif', '.png'))])

        # Set the number of items based on the filtered list
        self.train_im_num = len(self.train_imgs1)

        # Basic sanity check
        if len(self.train_imgs1) != len(self.train_labels):
            print(f"Warning: Found {len(self.train_imgs1)} images but {len(self.train_labels)} labels!")
        
    def __len__(self):
        return self.train_im_num

    def __getitem__(self, index):
        img_file1 = os.path.join(self.train_im_path1, self.train_imgs1[index])
        _img1 = imageio.imread(img_file1)
        img1 = _img1[:, :, AAA_Configs.selected_nums]
        #
        # img1 = Image.open(img_file1)

        img_file2 = os.path.join(self.train_im_path2, self.train_imgs2[index])
        _img2 = imageio.imread(img_file2)
        img2 = _img2[:, :, AAA_Configs.selected_nums]
        #
        # img2 = Image.open(img_file2)

        label_file = os.path.join(self.train_lb_path, self.train_labels[index])
        
        labels = Image.open(label_file)

        im1, im2, lb0, lb1, lb2, lb3 = self.transform(img1, img2, labels)

        # lb0 etc are already [1, H, W] from the new transform logic
        # Just extract the channel and clamp
        lb0 = torch.clamp(lb0[0], 0, AAA_Configs.NUM_CLASSES - 1)
        lb1 = torch.clamp(lb1[0], 0, AAA_Configs.NUM_CLASSES - 1)
        lb2 = torch.clamp(lb2[0], 0, AAA_Configs.NUM_CLASSES - 1)
        lb3 = torch.clamp(lb3[0], 0, AAA_Configs.NUM_CLASSES - 1)

        return im1, im2, lb0, lb1, lb2, lb3

    def transform(self, img1, img2, label):
        # Image transforms (keep these as they are)
        transform_img_2 = transforms.Compose([
            transforms.ToTensor(), 
            transforms.Normalize(AAA_Configs.normalization_mean, AAA_Configs.normalization_std)
        ])
        
        im1 = transform_img_2(img1)
        im2 = transform_img_2(img2)

        # MASK TRANSFORMS: Convert PIL to NumPy to preserve raw integers (0, 1, 2, 3, 4)
        label_np = np.array(label).astype(np.uint8)
        
        # Create different scales manually for Deep Supervision
        # We use NEAREST interpolation to ensure class 4 doesn't blend into class 2.5
        label_0 = torch.from_numpy(label_np).long().unsqueeze(0) # [1, 256, 256]
        
        # Function to resize masks while preserving integer values
        def resize_mask(mask, size):
            return transforms.functional.resize(
                mask.unsqueeze(0), # Add batch dim
                [size, size], 
                interpolation=transforms.InterpolationMode.NEAREST
            ).squeeze(0)

        label_4 = resize_mask(label_0, 64)
        label_8 = resize_mask(label_0, 32)
        label_16 = resize_mask(label_0, 16)

        return im1, im2, label_0, label_4, label_8, label_16

class MyTestData(Dataset):
    def __init__(self, im_path1, im_path2):
        super(MyTestData, self).__init__()
        self.train_im_path1 = im_path1
        self.train_im_path2 = im_path2
        
        # Filter for .tif files only
        self.train_imgs1 = sorted([f for f in os.listdir(im_path1) if f.lower().endswith('.tif')])
        self.train_imgs2 = sorted([f for f in os.listdir(im_path2) if f.lower().endswith('.tif')])

        # Set the number of items based on the filtered list
        self.train_im_num = len(self.train_imgs1)

        
    def __len__(self):
        return self.train_im_num
    def __getitem__(self, index):
        img_file1 = os.path.join(self.train_im_path1, self.train_imgs1[index])
        _img1 = imageio.imread(img_file1)
        img1 = _img1[:, :, AAA_Configs.selected_nums]
        #
        # img1 = Image.open(img_file1)

        img_file2 = os.path.join(self.train_im_path2, self.train_imgs2[index])
        _img2 = imageio.imread(img_file2)
        img2 = _img2[:, :, AAA_Configs.selected_nums]
        #
        # img2 = Image.open(img_file2)

        label_file = str(self.train_imgs1[index][:-4]) + '.png'
        
        # mlc: to return path of the input geotiff, to be used for extracting georeferencing info for the output geotiff
        path1 = os.path.join(self.train_im_path1, self.train_imgs1[index])

        im1, im2 = self.transform(img1, img2)
        #return im1, im2, label_file
        return im1, im2, label_file, path1
    
    def transform(self, img1, img2):
        transform_img_2 = transforms.Compose([transforms.ToTensor(), transforms.Normalize(AAA_Configs.normalization_mean, AAA_Configs.normalization_std)])
        im1 = transform_img_2(img1)
        im2 = transform_img_2(img2)
        return im1, im2