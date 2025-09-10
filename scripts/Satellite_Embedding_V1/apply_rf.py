import joblib
import pandas as pd

# Load the trained model from the file
loaded_rf_model = joblib.load('random_forest_model.joblib')

# Load your large CSV file into a pandas DataFrame
# Replace 'your_large_dataset.csv' with the actual path to your file
large_df = pd.read_csv('your_large_dataset.csv')

# Assuming the large dataset has the same feature columns as the training data (X)
# Select the feature columns from the large dataset
# You might need to adjust 'feature_columns' based on your local data
feature_columns = [col for col in large_df.columns if col not in ['label', 'x', 'y', 'ano', 'tipo']] # Adjust if necessary
X_large = large_df[feature_columns]


# Apply the loaded model to predict labels on the large dataset
predictions_large = loaded_rf_model.predict(X_large)

# Add the predictions as a new column to your large DataFrame
large_df['predicted_label'] = predictions_large

# Display the DataFrame with predictions (optional)
print("DataFrame with predicted labels:")
display(large_df.head())

# You can save the results to a new CSV file if needed
# large_df.to_csv('large_dataset_with_predictions.csv', index=False)
# print("\nLarge dataset with predictions saved as 'large_dataset_with_predictions.csv'")
