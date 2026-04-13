import os
from PIL import Image

# --- Configuration ---
input_image_path = r"I:\Máscaras\9\Direito\009_DIR_3_I_SR.jpg" # Replace with a real filename
output_folder = r"C:\Users\mlc\Downloads\temp\resize_test"
os.makedirs(output_folder, exist_ok=True)

def test_resize(image_path, size=(256, 256)):
    if not os.path.exists(image_path):
        print(f"Error: File {image_path} not found.")
        return

    # 1. Open the original high-res image
    with Image.open(image_path) as img:
        original_size = img.size
        print(f"Original Size: {original_size}")

        # 2. Resize to 256x256
        # We use Resampling.LANCZOS for the highest possible quality during downscaling
        resized_img = img.resize(size, Image.Resampling.LANCZOS)
        
        # 3. Save the result
        base_name = os.path.basename(image_path)
        save_path = os.path.join(output_folder, f"resized_{base_name}")
        resized_img.save(save_path)
        
        print(f"Resized image saved to: {save_path}")
        print(f"Scale reduction: {original_size[0]/size[0]:.1f}x smaller")

if __name__ == "__main__":
    test_resize(input_image_path)