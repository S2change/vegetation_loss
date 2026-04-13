import numpy as np
from scipy import stats
from datetime import date

def detect_breaks_welch(ordinal_dates, y, window_size=16):
    """
    Applies a sliding window Welch's t-test to detect drops in mean.
    
    Args:
        ordinal_dates (list): Sorted list of dates.
        y (list): Variable values (between -1 and 1).
        window_size (int): Total size of the window (default 16).
        
    Returns:
        list: p-values for the hypothesis that mean(after) < mean(before).
    """
    y = np.array(y)
    half = window_size // 2
    p_values = []

    # Slide window: we need at least 'window_size' elements to start
    for i in range(window_size, len(y) + 1):
        window = y[i - window_size : i]
        
        before = window[:half]
        after = window[half:]
        
        # Welch's t-test: equal_var=False
        # alternative='less' tests if mean(before) < mean(after)
        # To test if after < before, we swap the groups or use 'greater'
        # Here we use: H0: mean(before) <= mean(after) vs H1: mean(before) > mean(after)
        # which is equivalent to testing if 'after' dropped.
        t_stat, p_val = stats.ttest_ind(before, after, equal_var=False, alternative='greater')
        
        p_values.append(p_val)
        
    return p_values

# Example usage:
# p_vals = detect_breaks_welch(ordinal_dates, y_values)

def generate_break_data():
    # 1. Generate the date range
    start_date = date(2020, 1, 1).toordinal()
    end_date = date(2020, 6, 30).toordinal()
    ordinal_dates = list(range(start_date, end_date + 1))
    
    # Identify the indices for the breaks
    break1_ord = date(2020, 2, 15).toordinal()
    break2_ord = date(2020, 6, 15).toordinal()
    
    # 2. Generate the y values with random noise
    # We start high, drop at Feb 15, then drop again at June 15
    y = []
    np.random.seed(42) # For reproducibility
    
    for d in ordinal_dates:
        noise = np.random.normal(0, 0.4) # Small random fluctuations
        
        if d < break1_ord:
            val = 0.8 + noise           # High initial state
        elif d < break2_ord:
            val = 0.2 + noise           # First drop (Feb 15)
        else:
            val = -0.6 + noise          # Second drop (June 15)
            
        # Clip values to stay within [-1, 1] as per your requirement
        y.append(np.clip(val, -1, 1))
        
    return ordinal_dates, y

# Generate the lists
ordinal_dates, y_values = generate_break_data()

break1_ord = date(2020, 2, 15).toordinal()
break2_ord = date(2020, 6, 15).toordinal()
window_size = 16


# Verification
print(f"Total days: {len(ordinal_dates)}")
print(f"First 5 y-values: {y_values[:5]}")

p_vals = detect_breaks_welch(ordinal_dates, y_values)
print(f"Total p-values: {len(p_vals)}")
print(f"First 5 p-values: {p_vals[:5]}")

# plotting the results
import matplotlib.pyplot as plt
plt.figure(figsize=(12, 6))
plt.subplot(2, 1, 1)    
plt.plot(ordinal_dates, y_values, label='y values')
plt.axvline(x=break1_ord, color='r', linestyle='--', label='Break 1 (Feb 15, 2020)')
plt.axvline(x=break2_ord, color='g', linestyle='--', label='Break 2 (June 15, 2020)')
plt.title('Simulated y values with breaks') 
plt.xlabel('Ordinal Date')
plt.ylabel('y value')
plt.legend()
plt.subplot(2, 1, 2)

# use the dates in the center of the window for plotting p-values but keep the range of dates consistent with the original ordinal_dates
p_val_dates = ordinal_dates[window_size - 1:window_size - 1 + len(p_vals)]  # Corresponding dates for p-values
# set the range to be the same as the original ordinal_dates for better visualization
plt.plot(p_val_dates, p_vals, label='p-values (Welch\'s test)')
plt.axhline(y=0.01, color='r', linestyle='--', label='Significance Threshold (0.01)')
plt.title('P-values for Break Detection')   
plt.xlabel('Ordinal Date')
plt.ylabel('p-value')   
plt.legend()
plt.tight_layout()
plt.show()
