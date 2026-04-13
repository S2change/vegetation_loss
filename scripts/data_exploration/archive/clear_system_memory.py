import gc
import sys
import psutil
import pandas as pd

def clear_system_memory():
    """
    Forcefully clears Python's garbage collector and 
    attempts to release memory back to the OS.
    """
    print(f"RAM before clearing: {psutil.virtual_memory().percent}%")

    # 1. Delete all large DataFrames currently in the global scope (if any)
    # This looks for any pandas objects that might be lingering
    for var in list(globals().keys()):
        if isinstance(globals()[var], pd.DataFrame):
            del globals()[var]

    # 2. Clear IPython/Jupyter 'Out' history (The hidden cache)
    # If you are in a script, this part will simply be ignored.
    if 'In' in globals():
        globals()['In'] = []
        globals()['Out'] = {}
        print("Cleared IPython history cache.")

    # 3. Force Garbage Collection
    # Running it 3 times is a common trick to clear cyclic references
    for _ in range(3):
        gc.collect()

    # 4. Final status
    print(f"RAM after clearing: {psutil.virtual_memory().percent}%")
    print("Memory cleanup complete.")

# To run it:
clear_system_memory()