import numpy as np
from skimage import io
from skimage.segmentation import watershed
from skimage.filters import sobel
from skimage.measure import label
from scipy import ndimage as ndi
import matplotlib.pyplot as plt

def find_spatially_connected_clusters(image_path, threshold_T):
    """
    Reads an image, finds spatially connected pixel clusters based on a
    watershed algorithm with a threshold, and returns a labeled image.

    Args:
        image_path (str): The path to the input image file (TIFF format recommended).
        threshold_T (int or float): The threshold for creating markers for the watershed algorithm.
                                    This threshold is applied to the image gradient.

    Returns:
        numpy.ndarray: A labeled image of the same shape as the input,
                       with each pixel's value representing its cluster ID.
                       Returns None if the image file is not found.
    """
    try:
        img = io.imread(image_path)
        if img.ndim > 2:
            # Convert to grayscale if it's a color image
            img = img[:, :, 0] # Assuming the first channel is sufficient, or calculate luminance
            print("Warning: Input image is not grayscale. Using the first channel.")
    except FileNotFoundError:
        print(f"Error: The file '{image_path}' was not found.")
        return None
    except Exception as e:
        print(f"Error reading image: {e}")
        return None

    # Apply Sobel filter to find gradients
    elevation_map = sobel(img)

    # Create markers for watershed. Pixels below the threshold will be potential seeds.
    # Adjusting this threshold might be necessary depending on the image data.
    markers = np.zeros_like(img, dtype=int)
    markers[img < threshold_T] = 1
    markers[img >= threshold_T] = 2

    # Perform watershed segmentation
    # The watershed algorithm segments the image based on the elevation map (gradient)
    # and the markers (initial seeds).
    labeled_image = watershed(elevation_map, markers)

    return labeled_image

# Example usage with the dummy image
# Create a dummy image for demonstration
dummy_image = np.array([
    [10, 12, 14, 80],
    [13, 15, 78, 82],
    [20, 22, 90, 92]
], dtype=np.uint8)
io.imsave('dummy_image.png', dummy_image)


labeled_spatial_clusters = find_spatially_connected_clusters('dummy_image.png', threshold_T=25)

if labeled_spatial_clusters is not None:
    print("Labeled spatial clusters:\n", labeled_spatial_clusters)

    # Visualize the clusters
    plt.imshow(labeled_spatial_clusters, cmap='viridis')
    plt.title('Spatially Connected Pixel Clusters (Watershed)')
    plt.show()
