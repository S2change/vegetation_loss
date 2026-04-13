from datetime import datetime, timezone

# 1. Your input data
ts_ms = 1667647823345
new_tile = "T29TNE"
static_part = "N0511_R137" # Remains the same

# 2. Convert Milliseconds to a UTC Datetime object
dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

# 3. Format the date strings
# Acquisition format: 20221105-113023
acq_time = dt.strftime("%Y%m%d-%H%M%S")

# Processing format: 20221105T113023 (Usually slightly later, but we'll use the same for now)
proc_time = dt.strftime("%Y%m%dT%H%M%S")

# 4. Construct the final string
filename = f"S2C_MSIL2A_{acq_time}_{static_part}_{new_tile}_{proc_time}"

print(f"Generated Filename: {filename}")
# Result: S2C_MSIL2A_20221105-113023_N0511_R137_T29TNE_20221105T113023