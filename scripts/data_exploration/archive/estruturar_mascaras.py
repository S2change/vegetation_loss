import os
import shutil

# --- Path Configuration ---
# Source directory on your I: drive
source_root = r"I:\Máscaras"
# Destination directory for the jpg-only version
target_root = r"I:\mascaras_jpg"

def mirror_jpg_files(src, dst):
    """
    Walks through src, recreating the folder structure in dst, 
    but only copying .jpg files.
    """
    # 1. Ensure the root target folder exists
    if not os.path.exists(dst):
        os.makedirs(dst)
        print(f"Created root directory: {dst}")

    files_copied = 0
    folders_created = 0

    print(f"Scanning {src} for JPG files...")

    # 2. Walk through all subfolders
    for root, dirs, files in os.walk(src):
        # Filter for .jpg files (case-insensitive)
        jpg_files = [f for f in files if f.lower().endswith(".jpg")]

        if jpg_files:
            # Determine the relative path from the source root
            # e.g., if root is I:\Máscaras\9\Direito, rel_path is 9\Direito
            rel_path = os.path.relpath(root, src)
            
            # Create the corresponding path in the target folder
            dest_path = os.path.join(dst, rel_path)
            
            if not os.path.exists(dest_path):
                os.makedirs(dest_path)
                folders_created += 1

            # 3. Copy each JPG file
            for filename in jpg_files:
                source_file = os.path.join(root, filename)
                target_file = os.path.join(dest_path, filename)
                
                # shutil.copy2 preserves original file metadata (timestamps)
                shutil.copy2(source_file, target_file)
                files_copied += 1
                
            # Optional: Print progress for each folder processed
            print(f"  Processed: {rel_path} ({len(jpg_files)} files)")

    print("-" * 30)
    print(f"Task Complete!")
    print(f"Total Folders Created: {folders_created}")
    print(f"Total JPGs Copied:     {files_copied}")

if __name__ == "__main__":
    # Execute the function
    try:
        mirror_jpg_files(source_root, target_root)
    except Exception as e:
        print(f"An error occurred: {e}")