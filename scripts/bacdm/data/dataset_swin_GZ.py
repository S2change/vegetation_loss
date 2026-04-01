import torch
from torch.utils.data import Dataset
from torchvision import transforms
from PIL import Image
import os
import imageio.v2 as imageio
import AAA_Configs
import tifffile

class MyData(Dataset):
    def __init__(self, im_path1, im_path2, lb_path):
        super(MyData, self).__init__()
        self.train_im_path1 = im_path1
        self.train_im_path2 = im_path2
        self.train_lb_path = lb_path

        self.train_im_num = len(os.listdir(im_path1))
        self.train_imgs1 = sorted((os.listdir(self.train_im_path1)))
        self.train_imgs2 = sorted((os.listdir(self.train_im_path2)))
        self.train_labels = sorted((os.listdir(self.train_lb_path)))

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
        lb0 = 1. - lb0[0]
        lb1 = 1. - lb1[0]
        lb2 = 1. - lb2[0]
        lb3 = 1. - lb3[0]

        return im1, im2, lb0, lb1, lb2, lb3

    def transform(self, img1, img2, label):
        transform_img = transforms.Compose([transforms.ToTensor()])
        transform_img_4 = transforms.Compose([transforms.Resize((64, 64), Image.NEAREST), transforms.ToTensor()])
        transform_img_8 = transforms.Compose([transforms.Resize((32, 32), Image.NEAREST), transforms.ToTensor()])
        transform_img_16 = transforms.Compose([transforms.Resize((16, 16), Image.NEAREST), transforms.ToTensor()])
        transform_img_2 = transforms.Compose([transforms.ToTensor(), transforms.Normalize(AAA_Configs.normalization_mean, AAA_Configs.normalization_std)])
        im1 = transform_img_2(img1)
        im2 = transform_img_2(img2)
        label0 = transform_img(label)
        label_4 = transform_img_4(label)
        label_8 = transform_img_8(label)
        label_16 = transform_img_16(label)
        return im1, im2, label0, label_4, label_8, label_16

class MyTestData(Dataset):
    def __init__(self, im_path1, im_path2):
        super(MyTestData, self).__init__()
        self.train_im_path1 = im_path1
        self.train_im_path2 = im_path2
        self.train_im_num = len(os.listdir(im_path1))
        self.train_imgs1 = sorted(os.listdir(self.train_im_path1))
        self.train_imgs2 = sorted(os.listdir(self.train_im_path2))
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