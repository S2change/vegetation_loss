import sys

import torch
import torch.nn as nn
import tqdm
import torch.optim as optim
from torch.utils.data import DataLoader
import warnings
import datetime
import random
import numpy as np

import AAA_Configs
from bacdm.swin_ynet import Encoder
from bacdm.data.dataset_swin_GZ import MyData
#from bacdm.my_scheduler import LR_Scheduler
from bacdm import pytorch_dice

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

    alpha=AAA_Configs.ALPHA # weight of the cross-entropy loss vs the dice loss in the overall objective function for training; you can experiment with different values for alpha to see how it affects training performance and model learning, especially in terms of how well the model learns from the imbalanced classes in your dataset; for example, if you find that the model is struggling to learn from the rarer classes, you might try increasing the weight of the dice loss (which can help with imbalanced data) by using a lower value for alpha, such as 0.5 or 0.3, to give more emphasis to the dice loss during training; conversely, if you find that the model is learning well from all classes and you want to prioritize overall accuracy, you might try a higher value for alpha, such as 0.9, to give more emphasis to the cross-entropy loss during training

    warnings.filterwarnings("ignore")
    # mc: added argument num_classes to Encoder and encoder1, and set it to 5 for our 5 classes (0-4) in the training data;
    model = Encoder(num_classes=AAA_Configs.NUM_CLASSES).cuda()
    deal = nn.Softmax(dim=1)

    resume_path = getattr(AAA_Configs, 'Resume_checkpoint_path', None)
    if resume_path:
        device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        model.load_state_dict(torch.load(resume_path, map_location=device))
        print(f"Resumed from checkpoint: {resume_path}")
        # Derive save stem and epoch offset from the resume filename so that
        # new checkpoints continue the numbering (e.g. _13 -> _14, _15, ...).
        _stem = resume_path[:-4]                          # strip ".pth"
        _last = _stem.rfind('_')
        save_stem    = _stem[:_last]                      # "...A4510_20260423150715"
        epoch_offset = int(_stem[_last + 1:]) + 1        # 13 + 1 = 14
    else:
        save_stem    = weight_path + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        epoch_offset = 0

    model = model.train()

    # # mc: Define weights for [Class 0, 1, 2, 3, 4] 
    # Adjust these based on your earlier distribution (90%, 0.23%, etc.)
    # Higher numbers for rarer classes.
    class_weights = torch.tensor(AAA_Configs.CLASS_WEIGHTS, dtype=torch.float32).cuda() # [1.0, 50.0, 100.0, 100.0, 5.0] too much for 1, 2, 3; [1.0, 20.0, 50.0, 50.0, 5.0] might be a good starting point; you can experiment with these weights to see how they affect training performance and model learning from the imbalanced classes in your dataset
    ce_loss = nn.CrossEntropyLoss(weight=class_weights, ignore_index=-100) # mc: added class weights to the loss function to help the model learn better from the imbalanced classes in our dataset, especially since we have a very high imbalance with Class 0 being much more prevalent than the other classes; these weights can be adjusted based on experimentation and the specific distribution of classes in your training data for potentially improved performance
    boundary_loss_fn = nn.BCEWithLogitsLoss()
    BOUNDARY_LOSS_WEIGHT = getattr(AAA_Configs, 'BOUNDARY_LOSS_WEIGHT', 0.1)
    ALPHA = getattr(AAA_Configs, 'ALPHA', 0.1)
    #alpha=AAA_Configs.ALPHA # weight of the cross-entropy loss vs the dice loss in the overall objective function for training; you can experiment with different values for alpha to see how it affects training performance and model learning, especially in terms of how well the model learns from the imbalanced classes in your dataset; for example, if you find that the model is struggling to learn from the rarer classes, you might try increasing the weight of the dice loss (which can help with imbalanced data) by using a lower value for alpha, such as 0.5 or 0.3, to give more emphasis to the dice loss during training; conversely, if you find that the model is learning well from all classes and you want to prioritize overall accuracy, you might try a higher value for alpha, such as 0.9, to give more emphasis to the cross-entropy loss during training

    
    # mc: not used for multi-class problems, but you can experiment with adding it back in if you want to try a combined loss function approach; just keep in mind that IOU loss is typically more suited for binary segmentation problems, and using it for multi-class problems can be tricky and may require modifications to the loss function or the way the outputs are processed before calculating IOU
    # iou_loss = pytorch_iou.IOU().cuda()
    dice_loss = pytorch_dice.MultiClassDiceLoss(num_classes=AAA_Configs.NUM_CLASSES).cuda()

    LR = AAA_Configs.LearningRate
    EPOCH = AAA_Configs.EPOCH

    # scheduler = LR_Scheduler('cos', LR, EPOCH, ...)
    
    #original
    optimizer = optim.SGD(model.parameters(), lr=LR, momentum=0.9, weight_decay=0.0005, nesterov=False)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=EPOCH, eta_min=LR * 0.01)
    for _ in range(epoch_offset):
        scheduler.step()


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
    
    # sugestão gemini para usar otimizador com diferentes learning rates para encoder1 vs o resto do modelo, para potencialmente melhorar a estabilidade do treinamento e o aprendizado a partir dos pesos pré-treinados, especialmente se você estiver usando um LR relativamente alto como 0.01; isso pode ajudar a evitar que os pesos pré-treinados sejam "bagunçados" muito rapidamente no início do treinamento, permitindo que o modelo aprenda de forma mais estável e eficaz a partir dos pesos pré-treinados, enquanto ainda permite que as outras partes do modelo aprendam com um LR mais alto para potencialmente melhorar a velocidade de convergência e o desempenho geral do modelo; você pode experimentar com diferentes multiplicadores de LR para o encoder1 (como 0.1 ou 0.01) para ver como isso afeta o treinamento e o desempenho do modelo
    #optimizer = make_optimizer(LR, model)


    train_loader = DataLoader(MyData(im_path1, im_path2, lb_path), shuffle=True, batch_size=AAA_Configs.batch_size, pin_memory=True, num_workers=AAA_Configs.num_workers)
    val_loader   = DataLoader(MyData(AAA_Configs.Test_im_pathA, AAA_Configs.Test_im_pathB, AAA_Configs.Val_lb_path), shuffle=False, batch_size=AAA_Configs.batch_size, pin_memory=True, num_workers=AAA_Configs.num_workers)

    losses0 = 0
    losses1 = 0
    losses2 = 0
    losses3 = 0
    losses8 = 0
    losses9 = 0
    losses10 = 0
    losses11 = 0
    losses_boundary = 0
            

    print(len(train_loader))

    # --- EMA model (smooths oscillations; saved alongside the regular checkpoint) ---
    ema_decay = 0.999
    ema_state = {k: v.clone().float().cpu() for k, v in model.state_dict().items()}

    def update_ema(model, ema_state, decay):
        with torch.no_grad():
            for k, v in model.state_dict().items():
                ema_state[k].mul_(decay).add_(v.float().cpu(), alpha=1.0 - decay)

    # --- best-checkpoint tracking (based on training Cuts F1 as a proxy) ---
    cuts_class_id = next((k for k, v in AAA_Configs.CLASS_NAMES.items() if v == 'Cuts'), 1)
    best_cuts_f1  = 0.0

    loss_least = 100000
    for epoch_num in range(EPOCH):
        print('---------- EPOCH ',epoch_num, '----------')
        print('LR is:', optimizer.state_dict()['param_groups'][0]['lr'])
        show_dict = {'epoch': epoch_num}

        loss_all = 0
        # Initialize global counters for this epoch
        global_preds  = np.zeros(AAA_Configs.NUM_CLASSES, dtype=np.int64)
        global_labels = np.zeros(AAA_Configs.NUM_CLASSES, dtype=np.int64)
        global_tp     = np.zeros(AAA_Configs.NUM_CLASSES, dtype=np.int64)

        for i_batch, (im1, im2, label0, label1, label2, label3, valid_mask) in enumerate(
                tqdm.tqdm(train_loader, ncols=60, postfix=show_dict)):

            if im1.shape[-2:] != (256, 256) or im2.shape[-2:] != (256, 256):
                sys.exit(f"ERROR: Found odd shape {im1.shape} or {im2.shape} in batch {i_batch}")

            im1, im2 = im1.cuda(), im2.cuda()
            label0 = label0.cuda()
            label1 = label1.cuda()
            label2 = label2.cuda()
            label3 = label3.cuda()
            valid_mask = valid_mask.cuda()  # [B, H, W] bool

            # Downsample valid_mask for auxiliary decoder scales.
            # A block is invalid if ANY source pixel is NoData (min-pool).
            def _down_mask(m, size):
                k = m.shape[-1] // size
                return (-torch.nn.functional.max_pool2d(
                    -m.float().unsqueeze(1), kernel_size=k, stride=k
                ).squeeze(1)) > 0.5

            vm64 = _down_mask(valid_mask, 64)
            vm32 = _down_mask(valid_mask, 32)
            vm16 = _down_mask(valid_mask, 16)

            # Mask labels for CrossEntropyLoss: set NoData pixels to ignore_index.
            def _mask_label(lbl, vm):
                out = lbl.clone()
                out[~vm] = -100
                return out

            # --- TRAINING STEP ---
            outputs = model(im1, im2)
            # outputs: (x_p, boundary_map, x_pre2, x_pre3, x_pre4)

            loss0 = ce_loss(outputs[0], _mask_label(label0, valid_mask).long())
            loss1 = ce_loss(outputs[2], _mask_label(label1, vm64).long())
            loss2 = ce_loss(outputs[3], _mask_label(label2, vm32).long())
            loss3 = ce_loss(outputs[4], _mask_label(label3, vm16).long())
            # mc: multiclass Dice loss (mask zeros out NoData pixels in numerator and denominator)
            loss8  = dice_loss(outputs[0], label0.long(), mask=valid_mask)
            loss9  = dice_loss(outputs[2], label1.long(), mask=vm64)
            loss10 = dice_loss(outputs[3], label2.long(), mask=vm32)
            loss11 = dice_loss(outputs[4], label3.long(), mask=vm16)
            # boundary loss on the main output (outputs[1] = boundary_map, shape B,1,H,W)
            # Wrapping the boundary label computation in torch.no_grad() prevents PyTorch from allocating a computation graph for those intermediate tensors, freeing that memory before the backward pass.
            with torch.no_grad():
                lf = label0.float().unsqueeze(1)
                # kernel=7 gives ~3-pixel wide boundary, better matched to Swin's 4x4 patch grid
                boundary_label = (torch.nn.functional.max_pool2d(lf, 7, stride=1, padding=3) !=
                                  -torch.nn.functional.max_pool2d(-lf, 7, stride=1, padding=3)
                                  ).float().squeeze(1)
            loss_boundary = boundary_loss_fn(outputs[1].squeeze(1), boundary_label)
            # remove iou_loss to prevent multiclass problems (MC 5abr2016)
            #loss8 = iou_loss(deal(outputs[0]), label0)
            #loss9 = iou_loss(deal(outputs[1]), label1)
            #loss10 = iou_loss(deal(outputs[2]), label2)
            #loss11 = iou_loss(deal(outputs[3]), label3)
            loss = ALPHA * (loss0 + loss1 + loss2 + loss3) + (1 - ALPHA - BOUNDARY_LOSS_WEIGHT) * (loss8 + loss9 + loss10 + loss11) + BOUNDARY_LOSS_WEIGHT * loss_boundary
            
            loss_all = loss_all + loss.item()
            losses0 += loss0.item()
            losses1 += loss1.item()
            losses2 += loss2.item()
            losses3 += loss3.item()
            losses8 += loss8.item()
            losses9 += loss9.item()
            losses10 += loss10.item()
            losses11 += loss11.item()
            losses_boundary += loss_boundary.item()
            
            optimizer.zero_grad()
            if not torch.isfinite(loss):
                print(f"WARNING: non-finite loss {loss.item()} at batch {i_batch}, skipping")
                del outputs, loss
                continue
            loss.backward()

            # Limits the magnitude of updates to prevent distribution collapse / gradient explosion
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)

            optimizer.step()
            update_ema(model, ema_state, ema_decay)

            # --- ACCUMULATE GLOBAL STATISTICS ---
            with torch.no_grad():
                pred_batch = torch.argmax(outputs[0], dim=1).cpu().numpy()
                label_batch = label0.cpu().numpy()
                vm_np = valid_mask.cpu().numpy()
                label_batch_masked = label_batch[vm_np]
                pred_batch_masked  = pred_batch[vm_np]
                for c in range(AAA_Configs.NUM_CLASSES):
                    global_preds[c]  += np.sum(pred_batch_masked == c)
                    global_labels[c] += np.sum(label_batch_masked == c)
                    global_tp[c]     += np.sum((pred_batch_masked == c) & (label_batch_masked == c))

            # Free GPU tensors from this batch before next iteration
            del outputs, loss, loss0, loss1, loss2, loss3, loss8, loss9, loss10, loss11, loss_boundary, boundary_label
            if i_batch % 10 == 0:
                torch.cuda.empty_cache()
            
            # print number of pixels in label0 that are different than 0:
            print(f"Batch {i_batch} - Global Preds: {global_preds.tolist()}; Labels: {global_labels.tolist()}") # this can help you see how many pixels in this batch belong to the rarer classes (1-4) vs the background class (0), which can give you insight into the imbalance in the training data and how the model is learning from it

            # reset losses every 100 batches to monitor training progress more granularly and to prevent the loss values from becoming too large to interpret meaningfully over the course of an entire epoch
            if i_batch % 100 == 0:
                print(i_batch, '|', 'losses0: {:.3f}'.format(losses0), '|', 'losses1: {:.3f}'.format(losses1),
                      '|', 'losses2: {:.3f}'.format(losses2), '|', 'losses3: {:.3f}'.format(losses3), '|',
                      'losses8: {:.3f}'.format(losses8), '|', 'losses9: {:.3f}'.format(losses9), '|',
                      'losses10: {:.3f}'.format(losses10), '|', 'losses11: {:.3f}'.format(losses11), '|',
                      'losses_boundary: {:.3f}'.format(losses_boundary))
                losses0 = 0
                losses1 = 0
                losses2 = 0
                losses3 = 0
                losses8 = 0
                losses9 = 0
                losses10 = 0
                losses11 = 0
                losses_boundary = 0
            
            # 
        
        # --- END OF EPOCH SUMMARY ---
        precision = global_tp / (global_preds  + 1e-8)
        recall    = global_tp / (global_labels + 1e-8)
        f1        = 2 * precision * recall / (precision + recall + 1e-8)
        cuts_f1   = f1[cuts_class_id]

        print(f'\n=== Epoch {epoch_num} Global Summary ===')
        print("Predictions:", {AAA_Configs.CLASS_NAMES[i]: global_preds[i] for i in range(AAA_Configs.NUM_CLASSES)})
        print("Labels:     ", {AAA_Configs.CLASS_NAMES[i]: global_labels[i] for i in range(AAA_Configs.NUM_CLASSES)})
        pred_pct  = (global_preds  / global_preds.sum())  * 100
        label_pct = (global_labels / global_labels.sum()) * 100
        print(f"Pred %:  {np.round(pred_pct,  2)}")
        print(f"Labl %:  {np.round(label_pct, 2)}")
        print(f"F1:      { {AAA_Configs.CLASS_NAMES[i]: round(float(f1[i]), 3) for i in range(AAA_Configs.NUM_CLASSES)} }")
        print(f"Train Cuts F1: {cuts_f1:.4f}")

        # --- VALIDATION on held-out test set ---
        model.eval()
        val_preds  = np.zeros(AAA_Configs.NUM_CLASSES, dtype=np.int64)
        val_labels = np.zeros(AAA_Configs.NUM_CLASSES, dtype=np.int64)
        val_tp     = np.zeros(AAA_Configs.NUM_CLASSES, dtype=np.int64)
        with torch.no_grad():
            for im1_v, im2_v, lbl_v, _, _, _, vm_v in val_loader:
                im1_v, im2_v = im1_v.cuda(), im2_v.cuda()
                vm_v = vm_v.cuda()
                out_v = model(im1_v, im2_v)
                pred_v  = torch.argmax(out_v[0], dim=1).cpu().numpy()
                lbl_v   = lbl_v.cpu().numpy()
                vm_np_v = vm_v.cpu().numpy()
                lbl_m  = lbl_v[vm_np_v]
                pred_m = pred_v[vm_np_v]
                for c in range(AAA_Configs.NUM_CLASSES):
                    val_preds[c]  += np.sum(pred_m == c)
                    val_labels[c] += np.sum(lbl_m  == c)
                    val_tp[c]     += np.sum((pred_m == c) & (lbl_m == c))
        model.train()

        val_precision = val_tp / (val_preds  + 1e-8)
        val_recall    = val_tp / (val_labels + 1e-8)
        val_f1        = 2 * val_precision * val_recall / (val_precision + val_recall + 1e-8)
        val_cuts_f1   = val_f1[cuts_class_id]
        print(f"Val   Cuts F1: {val_cuts_f1:.4f}  (best so far: {best_cuts_f1:.4f})")

        with open(save_stem + f'_epoch_{epoch_offset + epoch_num}_stats.txt', 'w') as f_stats:
            f_stats.write(f"Train Preds:   {global_preds.tolist()}\n")
            f_stats.write(f"Train Labels:  {global_labels.tolist()}\n")
            f_stats.write(f"Train F1:      {np.round(f1,            4).tolist()}\n")
            f_stats.write(f"Val   Preds:   {val_preds.tolist()}\n")
            f_stats.write(f"Val   Labels:  {val_labels.tolist()}\n")
            f_stats.write(f"Val   Prec:    {np.round(val_precision, 4).tolist()}\n")
            f_stats.write(f"Val   Recall:  {np.round(val_recall,    4).tolist()}\n")
            f_stats.write(f"Val   F1:      {np.round(val_f1,        4).tolist()}\n")

        torch.cuda.empty_cache()
        try:
            cpu_state = {k: v.cpu() for k, v in model.state_dict().items()}
            save_path = f"{save_stem}_{epoch_offset + epoch_num}.pth"
            torch.save(cpu_state, save_path)
            print(f"Saved checkpoint: {save_path}")

            # Save EMA weights every epoch (overwrites; keeps only the latest)
            ema_path = f"{save_stem}_ema.pth"
            torch.save(ema_state, ema_path)

            # Best checkpoint selected by validation Cuts F1 (not training F1)
            if val_cuts_f1 > best_cuts_f1:
                best_cuts_f1 = val_cuts_f1
                best_path = f"{save_stem}_best.pth"
                torch.save(cpu_state, best_path)
                print(f"  *** New best val Cuts F1 {best_cuts_f1:.4f} — saved to {best_path}")

        except Exception as e:
            print(f"Warning: Save failed at epoch {epoch_num}: {e}")

        scheduler.step()
