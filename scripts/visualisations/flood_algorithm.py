# https://scikit-image.org/docs/0.25.x/api/skimage.segmentation.html#skimage.segmentation.flood
# Mask corresponding to a flood fill. Starting at a specific seed_point, connected points equal or within tolerance of the seed value are found.

import numpy as np
from skimage.segmentation import flood

def threshold_watershed(image,T):
    labels = np.full(image.shape, -1, dtype=int)
    label_id = 0
    for idx, val in np.ndenumerate(image):
        if labels[idx] == -1:
            mask = flood(image, idx, tolerance=T,connectivity=1.5)
            labels[mask] = label_id
            label_id += 1
    return labels

# example
import matplotlib.pyplot as plt
# Example with T=30
dummy_image = np.array([
    [8, 12, 14, 40, 42, 45, 80, 82, 85],
    [13, 15, 16, 41, 44, 46, 81, 83, 86],
    [20, 22, 25, 48, 50, 52, 88, 90, 92],
    [21, 23, 26, 49, 51, 53, 89, 12, 14],
    [30, 32, 35, 60, 62, 65, 70, 10, 8] # Transition/boundary values
], dtype=np.uint8)
T=30
labeled_spatial_clusters = threshold_watershed(dummy_image, T) #
if labeled_spatial_clusters is not None:
    print("Labeled spatial clusters:\n", labeled_spatial_clusters)
    # Visualize the clusters
    plt.imshow(labeled_spatial_clusters, cmap='viridis')
    plt.title(f'Spatially Connected Pixel Clusters (Watershed) - Threshold={T}')
    plt.show()
