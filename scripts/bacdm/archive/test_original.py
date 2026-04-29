import torch
import os
import shutil
from torch.utils.data import DataLoader
from data.dataset_swin_GZ import MyTestData
from swin_ynet import Encoder
import torchvision
import time
import AAA_Configs

model = Encoder().cuda()
model.load_state_dict(torch.load(AAA_Configs.Test_weight_path))

im_path1 = AAA_Configs.Test_im_pathA
im_path2 = AAA_Configs.Test_im_pathB
outPath = AAA_Configs.Test_det_path

if os.path.exists(outPath):
    shutil.rmtree(outPath)
os.mkdir(outPath)

test_loader = DataLoader(MyTestData(im_path1, im_path2), shuffle=False, batch_size=1)
with torch.no_grad():
    model = model.eval()
    for i, (im1, im2, label_name) in enumerate(test_loader):
        im1 = im1.cuda()
        im2 = im2.cuda()
        label_name = label_name[0]
        outputs = model(im1, im2)
        outputs = outputs[0][0]
        a = outputs[0].unsqueeze(0)
        torchvision.utils.save_image(a, outPath + '/%s' % label_name)

