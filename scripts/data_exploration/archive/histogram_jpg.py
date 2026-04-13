import cv2
import matplotlib.pyplot as plt
import os

# --- Configuration ---
image_path = r"I:\mascaras_jpg\9\Direito\009_DIR_3_I_SR.JPG" # Replace with a real filename" 

def plot_rgb_histogram(img_path):
    if not os.path.exists(img_path):
        print(f"Error: {img_path} not found.")
        return

    # 1. Load the image
    # OpenCV loads as BGR by default
    img = cv2.imread(img_path)
    if img is None:
        print("Error: Could not decode image.")
        return

    # 2. Setup the plot
    plt.figure(figsize=(10, 6))
    plt.title(f"Color Histogram: {os.path.basename(img_path)}")
    plt.xlabel("Pixel Intensity (0-255)")
    plt.ylabel("Pixel Count")
    
    colors = ('b', 'g', 'r') # OpenCV order
    labels = ('Blue Channel', 'Green Channel', 'Red Channel')

    # 3. Calculate and plot histogram for each channel
    for i, col in enumerate(colors):
        # cv2.calcHist([images], [channels], mask, [histSize], [ranges])
        hist = cv2.calcHist([img], [i], None, [256], [0, 256])
        plt.plot(hist, color=col, label=labels[i], linewidth=2)
        plt.xlim([0, 256])

    plt.legend()
    plt.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    
    # Save a copy to your temp folder
    save_path = img_path.replace(".jpg", "_histogram.png")
    # If I: is read-only, change this to a local path:
    # save_path = r"C:\Users\mlc\Downloads\temp\histogram.png"
    
    plt.savefig(save_path)
    print(f"Histogram saved to: {save_path}")
    plt.show()

if __name__ == "__main__":
    plot_rgb_histogram(image_path)