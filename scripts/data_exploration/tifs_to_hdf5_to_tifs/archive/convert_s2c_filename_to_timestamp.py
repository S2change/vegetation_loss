from datetime import datetime, timezone

# CNCA ex : S2C_MSIL2A_20251007-110951_N0511_R137_T29TPE_20251007T145121.tif 
filename = "S2C_MSIL2A_20221105-113023_N0511_R137_T29TNE_20221105T113023"

# 1. Extract the specific date-time substring
time_str = filename.split('_')[2] 

# 2. Parse into a datetime object
# We include timezone.utc because Sentinel-2 filenames are always UTC
dt_obj = datetime.strptime(time_str, "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)

# 3. Convert to total seconds (float)
seconds = dt_obj.timestamp()

# 4. Convert to milliseconds (int)
milliseconds = int(seconds * 1000)

print(f"Datetime: {dt_obj}")
print(f"Timestamp in milliseconds: {milliseconds}")
# Result: 1759835391000