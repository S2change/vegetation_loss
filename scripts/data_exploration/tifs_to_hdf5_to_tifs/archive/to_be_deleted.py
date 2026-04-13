import os
h5_filename = os.path.join(os.getcwd(), 'satellite_data_intersected.h5')
print(f"Your file is here: {h5_filename}")
print(f"Does it exist? {os.path.exists(h5_filename)}")