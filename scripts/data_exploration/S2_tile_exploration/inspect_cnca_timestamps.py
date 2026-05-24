import argparse
import csv
import os
from datetime import datetime, timezone

import h5py
import numpy as np

TILES = [
    #"T29SMC", "T29TQF", "T29SMD", 
    "T29TQG", "T29SNB", "T29TME",
    "T29SNC", "T29SND", "T29SPB", "T29SPC", "T29TNE", "T29SPD",
    "T29TNF", "T29TNG", "T29TPE", "T29TPF", "T29TPG",
]
DS_NAME    = "original_timestamps"
N_TS       = 600
NODATA_VAL = 65535
OUTPUT_DIR = "/users1/cpca070342024/mlc/outputs/s2_timestamps"

parser = argparse.ArgumentParser(description="Inspect timestamps per tile in HDF5 files.")
group = parser.add_mutually_exclusive_group()
group.add_argument("--n", type=int, default=None, metavar="N",
                   help=f"Most recent N timestamps (default: {N_TS})")
group.add_argument("--year", type=int, default=None, metavar="YEAR",
                   help="All timestamps for a given year (e.g. 2024)")
args = parser.parse_args()

os.makedirs(OUTPUT_DIR, exist_ok=True)


def find_data_dataset(f):
    result = []
    def visitor(name, obj):
        if isinstance(obj, h5py.Dataset) and obj.ndim == 3 and obj.shape[1] == 10:
            result.append(name)
    f.visititems(visitor)
    return result[0] if result else None


def ts_ms_to_date(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")


for tile in TILES:
    fn_input = f"/users1/dgt/hdf5/{tile}.h5"

    if not os.path.isfile(fn_input):
        print(f"[{tile}] SKIP — file not found: {fn_input}")
        continue

    if args.year is not None:
        label    = str(args.year)
        out_name = f"cnca_{args.year}_{tile}.csv"
    else:
        n        = args.n if args.n is not None else N_TS
        label    = str(n)
        out_name = f"cnca_{n}_{tile}.csv"

    out_path = os.path.join(OUTPUT_DIR, out_name)

    print(f"[{tile}] processing → {out_name}")

    with h5py.File(fn_input, "r") as f:
        if DS_NAME not in f:
            print(f"[{tile}] SKIP — dataset '{DS_NAME}' not found")
            continue

        timestamps = np.asarray(f[DS_NAME])

        if args.year is not None:
            years = np.array([
                datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).year
                for ts in timestamps
            ])
            sel_idx = np.where(years == args.year)[0]
            sel_idx = sel_idx[np.argsort(timestamps[sel_idx])[::-1]]
        else:
            n = args.n if args.n is not None else N_TS
            sel_idx = np.argsort(timestamps)[-n:][::-1]

        print(f"[{tile}]   {len(sel_idx)} timestamps selected")

        ds_data_name = find_data_dataset(f)
        if ds_data_name is None:
            print(f"[{tile}] WARNING — no 3-D/10-band dataset found; band stats will be -1")

        with open(out_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["rank", "index", "timestamp_ms", "date",
                             "total_values", "nodata_count", "nodata_pct"])

            for rank, idx in enumerate(sel_idx, start=1):
                ts_ms    = int(timestamps[idx])
                date_str = ts_ms_to_date(ts_ms)

                if ds_data_name is not None:
                    band0  = f[ds_data_name][idx, 0, :]
                    total  = band0.size
                    nodata = int(np.sum(band0 == NODATA_VAL))
                else:
                    total = nodata = -1

                nodata_pct = round(nodata / total * 100, 4) if total > 0 else float("nan")
                writer.writerow([rank, int(idx), ts_ms, date_str,
                                 total, nodata, nodata_pct])

    print(f"[{tile}]   saved → {out_path}")

print("\nDone.")
