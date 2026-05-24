"""
Search Sentinel-2 L2A scenes via the Copernicus Data Space Ecosystem (CDSE)
OData catalog for multiple tiles and years.

Outputs one CSV per (year, tile) combination to OUT_DIR.
Authentication uses requests_oauthlib (sentinelhub library not used here).
"""

import csv
import requests
from datetime import datetime, timezone
from pathlib import Path

# ── Configuration ──────────────────────────────────────────────────────────────
YEARS = [2025]
TILES = ['T29SMC', 'T29TQF', 'T29SMD', 'T29TQG', 'T29SNB', 'T29TME', 'T29SNC', 'T29SND', 'T29SPB', 'T29SPC', 'T29TNE', 'T29SPD', 'T29TNF', 'T29TNG', 'T29TPE', 'T29TPF', 'T29TPG']

OUT_DIR = Path(r"C:\Users\mlc\Downloads\temp\test_tif_to_hdf5\S2_tiles_timestamps")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Helpers ────────────────────────────────────────────────────────────────────
ODATA_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
TOP = 100

def sensing_ms(name: str) -> int:
    """Sensing timestamp in milliseconds from the product filename."""
    sensing_str = name.split("_")[2]   # e.g. '20250306T110829'
    dt = datetime.strptime(sensing_str, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def fetch_scenes(tile: str, year: int) -> list:
    """Fetch all S2 L2A scenes for a tile and year from the CDSE OData catalog."""
    odata_filter = (
        f"Collection/Name eq 'SENTINEL-2'"
        f" and contains(Name,'{tile}')"
        f" and Attributes/OData.CSC.StringAttribute/any("
            f"att:att/Name eq 'productType'"
            f" and att/OData.CSC.StringAttribute/Value eq 'S2MSI2A')"
        f" and ContentDate/Start gt {year}-01-01T00:00:00.000Z"
        f" and ContentDate/Start lt {year}-12-31T23:59:59.000Z"
    )
    results, skip = [], 0
    while True:
        resp = requests.get(ODATA_URL, params={
            "$filter":  odata_filter,
            "$orderby": "ContentDate/Start asc",
            "$top":     TOP,
            "$skip":    skip,
            "$expand":  "Attributes",
        })
        resp.raise_for_status()
        items = resp.json().get("value", [])
        results.extend(items)
        print(f"    page {skip//TOP + 1}: {len(items)} scenes  (total: {len(results)})")
        if len(items) < TOP:
            break
        skip += TOP
    return results

def write_csv(scenes: list, path: Path) -> None:
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["start_date", "cloud_cover", "sensing_ms", "name"])
        for r in scenes:
            cloud = next(
                (a["Value"] for a in r.get("Attributes", []) if a.get("Name") == "cloudCover"),
                "",
            )
            name = r.get("Name", "")
            ms   = sensing_ms(name) if name else ""
            writer.writerow([r.get("ContentDate", {}).get("Start", ""), cloud, ms, name])

# ── Main loop ──────────────────────────────────────────────────────────────────
for year in YEARS:
    for tile in TILES:
        print(f"\n{tile}  {year}")
        scenes = fetch_scenes(tile, year)

        # Deduplicate by product name
        seen, unique = set(), []
        for r in scenes:
            key = r.get("Name") or r.get("Id")
            if key not in seen:
                seen.add(key)
                unique.append(r)
        if len(unique) < len(scenes):
            print(f"  ({len(scenes) - len(unique)} duplicate(s) removed)")

        out_path = OUT_DIR / f"cdse_{year}_{tile}.csv"
        write_csv(unique, out_path)
        print(f"  {len(unique)} scenes → {out_path}")
