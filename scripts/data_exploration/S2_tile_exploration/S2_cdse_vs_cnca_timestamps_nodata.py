"""
S2_cdse_vs_cnca_timestamps_nodata.py

Compares CDSE catalog timestamps against CNCA HDF5 nodata statistics
for matching (year, tile) pairs.

File naming convention in DATA_DIR:
  cdse_{year}_{tile}.csv   — CDSE catalog query results
  cnca_{year}_{tile}.csv   — HDF5 nodata statistics

CDSE columns  : start_date, cloud_cover, sensing_ms, name
CNCA columns  : rank, index, timestamp_ms, date, total_values, nodata_count, nodata_pct
"""

import csv
from pathlib import Path

DATA_DIR = Path(r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\S2_tiles_timestamps")

# ── Discover files and find matching pairs ─────────────────────────────────────
def parse_stem(stem: str):
    """Return (source, year, tile) from a filename stem like 'cdse_2025_T29SMC'."""
    parts = stem.split("_", 2)   # split on first 2 underscores only
    return parts[0], parts[1], parts[2]

cdse_keys = {}
cnca_keys = {}
for f in sorted(DATA_DIR.glob("*.csv")):
    source, year, tile = parse_stem(f.stem)
    key = (year, tile)
    if source == "cdse":
        cdse_keys[key] = f
    elif source == "cnca":
        cnca_keys[key] = f

pairs        = {k: (cdse_keys[k], cnca_keys[k]) for k in cdse_keys if k in cnca_keys}
cdse_only    = sorted(k for k in cdse_keys if k not in cnca_keys)
cnca_only    = sorted(k for k in cnca_keys if k not in cdse_keys)

print(f"Matching pairs ({len(pairs)}):")
for (year, tile) in sorted(pairs):
    print(f"  {year}  {tile}")

if cdse_only:
    print(f"\nCDSE only — no matching CNCA ({len(cdse_only)}):")
    for year, tile in cdse_only:
        print(f"  {year}  {tile}")

if cnca_only:
    print(f"\nCNCA only — no matching CDSE ({len(cnca_only)}):")
    for year, tile in cnca_only:
        print(f"  {year}  {tile}")

# ── Describe the first pair ────────────────────────────────────────────────────
first_key   = sorted(pairs)[0]
year, tile  = first_key
cdse_path, cnca_path = pairs[first_key]

CLOUD_THRESHOLD = 60.0   # CDSE scenes below this are expected in CNCA

def read_csv(path):
    with open(path, newline="") as fh:
        rows = list(csv.DictReader(fh))
    return rows

cdse_rows = read_csv(cdse_path)
cnca_rows = read_csv(cnca_path)

print(f"\n── First pair: year={year}  tile={tile} ──────────────────────────────")
print(f"\nCDSE file : {cdse_path.name}  ({len(cdse_rows)} rows)")
print(f"  Columns : {list(cdse_rows[0].keys()) if cdse_rows else '(empty)'}")
if cdse_rows:
    print(f"  First row: {dict(cdse_rows[0])}")
    print(f"  Last row : {dict(cdse_rows[-1])}")

print(f"\nCNCA file : {cnca_path.name}  ({len(cnca_rows)} rows)")
print(f"  Columns : {list(cnca_rows[0].keys()) if cnca_rows else '(empty)'}")
if cnca_rows:
    print(f"  First row: {dict(cnca_rows[0])}")
    print(f"  Last row : {dict(cnca_rows[-1])}")

# ── Query 1: CDSE scenes with cloud_cover < threshold missing from CNCA ────────
print(f"\n── Query 1: CDSE cloud_cover < {CLOUD_THRESHOLD}% not present in CNCA ──────────")

all_missing = []   # list of dicts for summary across all pairs

for (yr, tl) in sorted(pairs):
    cdse_r = read_csv(pairs[(yr, tl)][0])
    cnca_r = read_csv(pairs[(yr, tl)][1])

    cnca_ts = {row["timestamp_ms"] for row in cnca_r}

    missing = [
        row for row in cdse_r
        if float(row["cloud_cover"]) < CLOUD_THRESHOLD
        and row["sensing_ms"] not in cnca_ts
    ]

    n_low_cloud = sum(1 for r in cdse_r if float(r["cloud_cover"]) < CLOUD_THRESHOLD)
    flag = "  *** MISSING" if missing else ""
    print(f"\n  {yr}  {tl}: {n_low_cloud} CDSE scenes < {CLOUD_THRESHOLD}%  |  "
          f"{len(missing)} missing from CNCA{flag}")

    for row in missing:
        date = row["start_date"][:10]
        print(f"    {date}  cloud={float(row['cloud_cover']):.1f}%  {row['name']}")
        all_missing.append({"year": yr, "tile": tl, "date": date,
                            "cloud_cover": row["cloud_cover"],
                            "sensing_ms": row["sensing_ms"],
                            "name": row["name"]})

print(f"\nSummary: {len(all_missing)} CDSE scenes (cloud < {CLOUD_THRESHOLD}%) "
      f"missing from CNCA across {len(pairs)} pairs")
