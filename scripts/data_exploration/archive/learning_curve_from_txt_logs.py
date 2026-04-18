# I want to create a learning curve from the information iin files like 
# "C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\training_data\bacdm_weights\chips_346_FS_0103101002_LR01_alpha50_epoch_70_stats.txt"
'''
Global Preds: [20184140, 93416, 16, 89159, 2308725]
Global Labels: [20232930, 87478, 991, 77656, 2276401]
'''
# The number in the file name (e.g. 70 in the example above) is the epoch number, and the numbers in the content of the file are the global predictions and global labels for each class (0-4) for that epoch; I want to extract this information from all the files in a given directory, and then create a learning curve that shows how the global predictions and global labels for each class change over the epochs; I can use matplotlib to create this learning curve, with the x-axis representing the epoch number and the y-axis representing the global predictions and global labels for each class; I can create separate lines for each class to show how they change over time, and I can also add a legend to differentiate between the classes; this will allow me to visualize how the model's predictions and labels evolve during training, which can provide insights into the learning process and help identify any issues such as overfitting or underfitting. 
import os
import re       
import matplotlib.pyplot as plt

def extract_epoch_and_stats(file_path):
    # Extract epoch number from the file name using regex
    epoch_match = re.search(r'epoch_(\d+)', file_path)
    if epoch_match:
        epoch = int(epoch_match.group(1))
    else:
        raise ValueError(f"Epoch number not found in file name: {file_path}")
    
    # Read the content of the file and extract global predictions and labels
    with open(file_path, 'r') as f:
        content = f.read()
    
    preds_match = re.search(r'Global Preds:\s*\[([0-9,\s]+)\]', content)
    labels_match = re.search(r'Global Labels:\s*\[([0-9,\s]+)\]', content)
    
    if preds_match and labels_match:
        global_preds = list(map(int, preds_match.group(1).split(',')))
        global_labels = list(map(int, labels_match.group(1).split(',')))
        return epoch, global_preds, global_labels
    else:
        raise ValueError(f"Global Preds or Global Labels not found in file content: {file_path}")

def main():   
    directory = r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\bacdm\bacdm_weights"
    epoch_stats = []
    
    # Iterate through all files in the directory
    for filename in os.listdir(directory):
        if filename.endswith("_stats.txt") and filename.startswith("341_"):  # Ensure we only process the relevant stats files
            file_path = os.path.join(directory, filename)
            try:
                epoch, global_preds, global_labels = extract_epoch_and_stats(file_path)
                epoch_stats.append((epoch, global_preds, global_labels))
            except ValueError as e:
                print(e)
    
    # Sort the stats by epoch number
    epoch_stats.sort(key=lambda x: x[0])
    
    
    # Prepare data for plotting
    epochs = [stat[0] for stat in epoch_stats]
    preds_by_class = list(zip(*[stat[1] for stat in epoch_stats]))  # Transpose to get preds by class
    labels_by_class = list(zip(*[stat[2] for stat in epoch_stats]))  # Transpose to get labels by class 

    
    # Plotting
    # Since class size vary a lot, I want a separate plot for each class to better visualize the trends, and I will use a logarithmic scale for the y-axis to handle the large differences in values between classes; this way, we can see the trends 
    # for all classes more clearly without one class dominating the plot due to its larger values.
    for i in range(len(preds_by_class)):
        plt.figure(figsize=(12, 6))     
        plt.plot(epochs, preds_by_class[i], label=f'Class {i} Preds')
        plt.plot(epochs, labels_by_class[i], label=f'Class {i} Labels', linestyle='--')
        plt.xlabel('Epoch')     
        plt.xlabel('Epoch')
        plt.ylabel('Count') 
        plt.title('Global Predictions and Labels by Class over Epochs')
        plt.legend()    
        plt.grid()
        plt.show()  

if __name__ == "__main__":    main()