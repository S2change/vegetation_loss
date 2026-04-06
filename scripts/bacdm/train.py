import sys

import torch
import torch.nn as nn
import tqdm
import torch.optim as optim
from torch.utils.data import DataLoader
import warnings
import os
import datetime
import random
import numpy as np

import AAA_Configs
from bacdm.swin_ynet import Encoder
from bacdm.data.dataset_swin_GZ import MyData
from bacdm.my_scheduler import LR_Scheduler
from bacdm import pytorch_iou

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
    # mc: added argument num_classes to Encoder and encoder1, and set it to 5 for our 5 classes (0-4) in the training data;
    model = Encoder(num_classes=AAA_Configs.NUM_CLASSES).cuda()
    deal = nn.Softmax(dim=1)
    model = model.train()

    # # mc: Define weights for [Class 0, 1, 2, 3, 4] 
    # Adjust these based on your earlier distribution (90%, 0.23%, etc.)
    # Higher numbers for rarer classes.
    class_weights = torch.tensor(AAA_Configs.CLASS_WEIGHTS, dtype=torch.float32).cuda() # [1.0, 50.0, 100.0, 100.0, 5.0] too much for 1, 2, 3; [1.0, 20.0, 50.0, 50.0, 5.0] might be a good starting point; you can experiment with these weights to see how they affect training performance and model learning from the imbalanced classes in your dataset
    ce_loss = nn.CrossEntropyLoss(weight=class_weights) # mc: added class weights to the loss function to help the model learn better from the imbalanced classes in our dataset, especially since we have a very high imbalance with Class 0 being much more prevalent than the other classes; these weights can be adjusted based on experimentation and the specific distribution of classes in your training data for potentially improved performance
    
    # mc: not used for multi-class problems, but you can experiment with adding it back in if you want to try a combined loss function approach; just keep in mind that IOU loss is typically more suited for binary segmentation problems, and using it for multi-class problems can be tricky and may require modifications to the loss function or the way the outputs are processed before calculating IOU
    # iou_loss = pytorch_iou.IOU().cuda()

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
        # Initialize global counters for this epoch
        global_preds = np.zeros(AAA_Configs.NUM_CLASSES, dtype=np.int64)
        global_labels = np.zeros(AAA_Configs.NUM_CLASSES, dtype=np.int64)

        for i_batch, (im1, im2, label0, label1, label2, label3) in enumerate(
                tqdm.tqdm(train_loader, ncols=60, postfix=show_dict)):
            
            # print number of pixels in label0 that are different than 0:
            print(f"Batch {i_batch} - Non-background pixels in label0: {(label0 != 0).sum().item()}") # this can help you see how many pixels in this batch belong to the rarer classes (1-4) vs the background class (0), which can give you insight into the imbalance in the training data and how the model is learning from it

            if im1.shape[-2:] != (256, 256) or im2.shape[-2:] != (256, 256):
                sys.exit(f"ERROR: Found odd shape {im1.shape} or {im2.shape} in batch {i_batch}")
            
            im1, im2 = im1.cuda(), im2.cuda()
            label0 = label0.cuda()
            label1 = label1.cuda()
            label2 = label2.cuda()
            label3 = label3.cuda()

            # --- TRAINING STEP ---
            outputs = model(im1, im2)
            loss0 = ce_loss(outputs[0], label0.long())
            loss1 = ce_loss(outputs[1], label1.long())
            loss2 = ce_loss(outputs[2], label2.long())
            loss3 = ce_loss(outputs[3], label3.long())
            # remove iou_loss to prevent multiclass problems (MC 5abr2016)
            #loss8 = iou_loss(deal(outputs[0]), label0)
            #loss9 = iou_loss(deal(outputs[1]), label1)
            #loss10 = iou_loss(deal(outputs[2]), label2)
            #loss11 = iou_loss(deal(outputs[3]), label3)
            loss = loss0 + loss1 + loss2 + loss3  # + loss8 + loss9 + loss10 + loss11 # objective function for training; you can modify this to give different weights to the different loss components if you want to prioritize certain aspects of the training, or if you want to experiment with different combinations of loss functions for potentially improved performance
            loss_all = loss_all + loss.item()
            losses0 += loss0.item()
            losses1 += loss1.item()
            losses2 += loss2.item()
            losses3 += loss3.item()
            #losses8 += loss8.item()
            #losses9 += loss9.item()
            #losses10 += loss10.item()
            #losses11 += loss11.item()
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            # scheduler.step(epoch_num * len(train_loader) + i_batch) # if you want to use the learning rate scheduler that updates every 
            # iteration instead of every epoch, you can uncomment this line and comment out the adjust_learning_rate function 
            # and its call in the training loop; this will update the learning rate based on the current epoch and batch number, 
            # which can sometimes lead to smoother learning rate transitions and potentially improved training performance

            # --- ACCUMULATE GLOBAL STATISTICS ---
            # We do this during training to get a "live" look at what the model is thinking
            with torch.no_grad():
                # Get the predictions for the main output (outputs[0])
                pred_batch = torch.argmax(outputs[0], dim=1).cpu().numpy()
                label_batch = label0.cpu().numpy()

                # Count occurrences in this batch and add to global tally
                for c in range(AAA_Configs.NUM_CLASSES):
                    global_preds[c] += np.sum(pred_batch == c)
                    global_labels[c] += np.sum(label_batch == c)
            
            # reset losses every 100 batches to monitor training progress more granularly and to prevent the loss values from becoming too large to interpret meaningfully over the course of an entire
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
        
        # --- END OF EPOCH SUMMARY ---
        print(f'\n=== Epoch {epoch_num} Global Summary ===')
        classes = ['Background', 'Cuts', 'Other1', 'Other2', 'Fire']
        
        print("Predictions:", {classes[i]: global_preds[i] for i in range(5)})
        print("Labels:     ", {classes[i]: global_labels[i] for i in range(5)})
        
        # Calculate percentages to see the "Bias"
        pred_pct = (global_preds / global_preds.sum()) * 100
        label_pct = (global_labels / global_labels.sum()) * 100
        print(f"Pred %: {np.round(pred_pct, 2)}")
        print(f"Labl %: {np.round(label_pct, 2)}")

        # --- SAVE TO FILE ---
        # Update your f12.write logic to use these global tallies
        with open(weight_path + f'epoch_{epoch_num}_stats.txt', 'w') as f_stats:
            f_stats.write(f"Global Preds: {global_preds.tolist()}\n")
            f_stats.write(f"Global Labels: {global_labels.tolist()}\n")

        nowdate_time_str = (datetime.datetime.now()).strftime("%Y%m%d%H%M%S")
        # 1. Clear GPU cache to make room
        torch.cuda.empty_cache() 
        try:
            # 2. Move weights to CPU one-by-one (Crucial for stability)
            state_dict = model.state_dict()
            cpu_state_dict = {k: v.cpu() for k, v in state_dict.items()}
            
            # 3. Save the CPU-based dictionary
            save_path = weight_path + nowdate_time_str + '_' + str(epoch_num) + '.pth'
            torch.save(cpu_state_dict, save_path)
            print(f"Successfully saved checkpoint: {save_path}")
            
        except Exception as e:
            print(f"Warning: Save failed at epoch {epoch_num} with error: {e}")
            # This prevents the whole training run from dying just because of a save error
