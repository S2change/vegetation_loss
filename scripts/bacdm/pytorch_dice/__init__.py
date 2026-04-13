import torch
import torch.nn as nn
import torch.nn.functional as F

# dice loss that was used for 100-epock pretty good model, but had some issues with "bloating" of predicted change areas; the stabilized version below with Tversky parameters should help mitigate this issue by penalizing false positives more heavily, which is likely contributing to the bloating you are seeing in the predictions; you can experiment with the alpha and beta parameters in the stabilized version to find the best balance for your specific dataset and model behavior
def _dice_loss(pred, target, num_classes=5, smooth=1e-6):
    """
    Multi-class Dice Loss (Macro-averaged)
    pred: [B, K, H, W] - Raw logits from BACDM
    target: [B, H, W] - Class indices (0 to K-1)
    """
    # 1. Convert logits to probabilities
    probs = F.softmax(pred, dim=1)
    
    # 2. Convert target to One-Hot: [B, H, W] -> [B, K, H, W]
    target_one_hot = F.one_hot(target.long(), num_classes).permute(0, 3, 1, 2).float()
    
    dice_per_class = []
    
    # Skip background (class 0) if you only want to optimize change detection classes (1-4); adjust this if you want to include background in the loss
    for c in range(1, num_classes):
        p = probs[:, c, :, :]
        t = target_one_hot[:, c, :, :]
        
        intersection = torch.sum(p * t, dim=(1, 2))
        # sum_total = torch.sum(p, dim=(1, 2)) + torch.sum(t, dim=(1, 2))
        # Using squared terms in the denominator can help with sharpening
        sum_total = torch.sum(p**2, dim=(1, 2)) + torch.sum(t**2, dim=(1, 2))
        
        # Dice = (2 * I) / (Area1 + Area2)
        dice = (2. * intersection + smooth) / (sum_total + smooth)
        
        # We want to minimize (1 - Dice)
        dice_per_class.append(1.0 - dice)
    
    # Average across all batches and all classes
    return torch.stack(dice_per_class).mean()

# new loss function with Tversky parameters to help mitigate bloating by penalizing false positives more heavily; you can experiment with the alpha and beta parameters to find the best balance for your specific dataset and model behavior
def _stabilized_dice_loss(pred, target, num_classes=5, smooth=1e-6):
    probs = F.softmax(pred, dim=1)
    target_one_hot = F.one_hot(target.long(), num_classes).permute(0, 3, 1, 2).float()
    
    # Tversky parameters: alpha=0.3 (False Negatives), beta=0.7 (False Positives)
    # High beta penalizes the "bloating" you are seeing.
    alpha = 0.3
    beta = 0.7
    
    loss_per_class = []
    # ONLY loop through change classes (1-4)
    for c in range(1, num_classes):
        p = probs[:, c, :, :]
        t = target_one_hot[:, c, :, :]
        
        tp = torch.sum(p * t, dim=(1, 2))
        fp = torch.sum(p * (1 - t), dim=(1, 2))
        fn = torch.sum((1 - p) * t, dim=(1, 2))
        
        tversky_index = (tp + smooth) / (tp + alpha * fn + beta * fp + smooth)
        loss_per_class.append(1.0 - tversky_index)
        
    return torch.stack(loss_per_class).mean()


class MultiClassDiceLoss(nn.Module):
    def __init__(self, num_classes=5):
        super(MultiClassDiceLoss, self).__init__()
        self.num_classes = num_classes

    def forward(self, pred, target):
        #return _dice_loss(pred, target, self.num_classes)

        # 1. Get the raw stabilized dice/tversky value
        # (Using your range(1, num_classes) logic)
        score = _stabilized_dice_loss(pred, target, self.num_classes)
        # 2. Wrap the final score in Log-Cosh
        return torch.log(torch.cosh(score))