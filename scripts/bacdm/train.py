import torch
import torch.nn as nn
import tqdm
from my_scheduler import LR_Scheduler
from bacdm.swin_ynet import Encoder
from data.dataset_swin_GZ import MyData
import torch.optim as optim
from torch.utils.data import DataLoader
import warnings
import os
import datetime
import AAA_Configs
import random
import numpy as np

if __name__ == '__main__':

    # 固定随机种子
    seed = AAA_Configs.seednumber
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False

    # 以下的参数是需要修改的
    # 训练集的路径 im_path1：这里暂定燃烧前；im_path2：这里暂定燃烧后
    im_path1 = AAA_Configs.Train_im_pathA
    im_path2 = AAA_Configs.Train_im_pathB
    # 标签的路径
    lb_path = AAA_Configs.Train_lb_path
    # 权重保存的路径
    weight_path = AAA_Configs.Train_weight_path

    warnings.filterwarnings("ignore")
    model = Encoder().cuda()

    import pytorch_iou


    deal = nn.Softmax(dim=1)



    model = model.train()
    ce_loss = nn.CrossEntropyLoss()
    iou_loss = pytorch_iou.IOU().cuda()

    LR = AAA_Configs.LearningRate
    EPOCH = AAA_Configs.EPOCH

    _num = len(os.listdir(im_path1))
    scheduler = LR_Scheduler('cos', LR, EPOCH, _num // 10 + 1)
    optimizer = optim.SGD(model.parameters(), lr=LR, momentum=0.9, weight_decay=0.0005, nesterov=False)


    def make_optimizer(LR, model):
        params = []
        for key, value in model.named_parameters():
            if not value.requires_grad:
                continue
            if "encoder1" in key:
                lr = LR * 0.1
            else:
                lr = LR
            params += [{"params": [value], "lr": lr}]
        optimizer = getattr(torch.optim, "SGD")(params, momentum=0.9, weight_decay=0.0005, nesterov=False)
        return optimizer


    train_loader = DataLoader(MyData(im_path1, im_path2, lb_path), shuffle=True, batch_size=AAA_Configs.batch_size, pin_memory=True, num_workers=AAA_Configs.num_workers)

    losses0 = 0
    losses1 = 0
    losses2 = 0
    losses3 = 0
    losses8 = 0
    losses9 = 0
    losses10 = 0
    losses11 = 0

    print(len(train_loader))


    def adjust_learning_rate(optimizer, epoch, start_lr):
        if epoch % 20 == 0:  # epoch != 0 and
            for param_group in optimizer.param_groups:
                param_group["lr"] = param_group["lr"] * 0.1
            print(param_group["lr"])


    loss_least = 100000
    for epoch_num in range(EPOCH):
        print(epoch_num)
        adjust_learning_rate(optimizer, epoch_num, LR)
        print('LR is:', optimizer.state_dict()['param_groups'][0]['lr'])
        show_dict = {'epoch': epoch_num}

        loss_all = 0
        for i_batch, (im1, im2, label0, label1, label2, label3) in enumerate(
                tqdm.tqdm(train_loader, ncols=60, postfix=show_dict)):  # ,edge0,edge1,edge2,edge3
            im1 = im1.cuda()
            im2 = im2.cuda()
            label0 = label0.cuda()
            label1 = label1.cuda()
            label2 = label2.cuda()
            label3 = label3.cuda()

            outputs = model(im1, im2)
            loss0 = ce_loss(outputs[0], label0.long())
            loss1 = ce_loss(outputs[1], label1.long())
            loss2 = ce_loss(outputs[2], label2.long())
            loss3 = ce_loss(outputs[3], label3.long())
            loss8 = iou_loss(deal(outputs[0]), label0)
            loss9 = iou_loss(deal(outputs[1]), label1)
            loss10 = iou_loss(deal(outputs[2]), label2)
            loss11 = iou_loss(deal(outputs[3]), label3)
            loss = loss0 + loss1 + loss2 + loss3  + loss8 + loss9 + loss10 + loss11
            loss_all = loss_all + loss.item()
            losses0 += loss0.item()
            losses1 += loss1.item()
            losses2 += loss2.item()
            losses3 += loss3.item()
            losses8 += loss8.item()
            losses9 += loss9.item()
            losses10 += loss10.item()
            losses11 += loss11.item()
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            if i_batch % 100 == 0:
                print(i_batch, '|', 'losses0: {:.3f}'.format(losses0), '|', 'losses1: {:.3f}'.format(losses1),
                      '|', 'losses2: {:.3f}'.format(losses2), '|', 'losses3: {:.3f}'.format(losses3), '|',
                      'losses8: {:.3f}'.format(losses8), '|', 'losses9: {:.3f}'.format(losses9), '|',
                      'losses10: {:.3f}'.format(losses10), '|', 'losses11: {:.3f}'.format(losses11))
                losses0 = 0
                losses1 = 0
                losses2 = 0
                losses3 = 0
                losses8 = 0
                losses9 = 0
                losses10 = 0
                losses11 = 0
        f12 = open(weight_path + 'epoch_' + str(epoch_num) + '_loss_' + str(round(loss_all, 3)) + '.txt', 'wb')
        f12.close()
        nowdate_time_str = (datetime.datetime.now()).strftime("%Y%m%d%H%M%S")
        torch.save(model.state_dict(), weight_path + nowdate_time_str + '_' + str(epoch_num) + '.pth')

