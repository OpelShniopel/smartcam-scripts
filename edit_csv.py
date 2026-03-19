import numpy as np

# Load the data (assuming 2 columns: Zoom and Focus)
# data[:, 0] is column 1 (Zoom), data[:, 1] is column 2 (Focus)
data = np.loadtxt('focus_data.txt')

constant = 150

# Add constant to the second column (Index 1)
# Formula: X_{i,1} = X_{i,1} + 150
data[:, 1] += constant

# Save back to file
np.savetxt('focus_data_offset.txt', data, fmt='%d')