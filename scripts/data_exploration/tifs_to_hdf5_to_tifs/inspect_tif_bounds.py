import os
import numpy as np
import rasterio

folder_path = r"D:\s2_images\T29TNE"

# ── Collect files ──────────────────────────────────────────────────────────────
files = sorted(f for f in os.listdir(folder_path) if f.endswith('.tif'))
if not files:
    print("No .tif files found in the directory.")
    exit()

# ── Read bounds ────────────────────────────────────────────────────────────────
print(f"Reading bounds for {len(files)} files...\n")
records = []
for f in files:
    with rasterio.open(os.path.join(folder_path, f)) as src:
        b = src.bounds
        records.append({
            'filename': f,
            'left':   b.left,
            'right':  b.right,
            'bottom': b.bottom,
            'top':    b.top,
            'width':  b.right - b.left,
            'height': b.top   - b.bottom,
            'px_width':  src.width,
            'px_height': src.height,
        })

lefts   = np.array([r['left']   for r in records])
rights  = np.array([r['right']  for r in records])
bottoms = np.array([r['bottom'] for r in records])
tops    = np.array([r['top']    for r in records])
widths  = np.array([r['width']  for r in records])
heights = np.array([r['height'] for r in records])

# ── Per-file table ─────────────────────────────────────────────────────────────
col_w = max(len(r['filename']) for r in records)
header = f"{'FILE':<{col_w}}  {'LEFT':>12}  {'RIGHT':>12}  {'BOTTOM':>12}  {'TOP':>12}  {'WIDTH(m)':>10}  {'HEIGHT(m)':>10}  {'PX W':>7}  {'PX H':>7}"
print(header)
print("-" * len(header))
for r in records:
    print(f"{r['filename']:<{col_w}}  {r['left']:>12.1f}  {r['right']:>12.1f}  {r['bottom']:>12.1f}  {r['top']:>12.1f}  {r['width']:>10.1f}  {r['height']:>10.1f}  {r['px_width']:>7}  {r['px_height']:>7}")

# ── Summary statistics ─────────────────────────────────────────────────────────
print("\n── Summary (all values in CRS units / metres) ──────────────────────────────")
for label, arr in [("LEFT", lefts), ("RIGHT", rights), ("BOTTOM", bottoms), ("TOP", tops), ("WIDTH", widths), ("HEIGHT", heights)]:
    print(f"  {label:<8}  min={arr.min():.1f}  max={arr.max():.1f}  median={np.median(arr):.1f}  range={arr.max()-arr.min():.1f}  std={arr.std():.1f}")

# ── Identify outliers relative to median ──────────────────────────────────────
print("\n── Deviation from median extent ────────────────────────────────────────────")
med_left, med_right = np.median(lefts), np.median(rights)
med_bottom, med_top = np.median(bottoms), np.median(tops)

print(f"  Median extent:  left={med_left:.1f}  right={med_right:.1f}  bottom={med_bottom:.1f}  top={med_top:.1f}\n")

print(f"  {'FILE':<{col_w}}  {'dLEFT':>8}  {'dRIGHT':>8}  {'dBOTTOM':>8}  {'dTOP':>8}  {'MAX_DEV':>8}")
print(f"  {'-'*col_w}  {'--------':>8}  {'--------':>8}  {'--------':>8}  {'--------':>8}  {'--------':>8}")
for r in records:
    dl = r['left']   - med_left
    dr = r['right']  - med_right
    db = r['bottom'] - med_bottom
    dt = r['top']    - med_top
    max_dev = max(abs(dl), abs(dr), abs(db), abs(dt))
    print(f"  {r['filename']:<{col_w}}  {dl:>+8.1f}  {dr:>+8.1f}  {db:>+8.1f}  {dt:>+8.1f}  {max_dev:>8.1f}")
