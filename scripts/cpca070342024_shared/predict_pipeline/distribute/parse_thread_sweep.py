"""Summarize a thread_sweep_slurm.sh .out file.

Parses the per-THREADS sections and tabulates inference ms/chip, total
infer time, Step-1 read time, and model-load time per thread count, plus
the parallel efficiency vs. the 1-thread baseline.

Usage:
    python parse_thread_sweep.py /path/to/thread_sweep_<jobid>.out
"""
import re
import sys


def parse(path: str) -> list[dict]:
    rows: list[dict] = []
    cur: dict | None = None
    with open(path) as f:
        for line in f:
            m = re.search(r"### THREADS=(\d+)", line)
            if m:
                if cur is not None:
                    rows.append(cur)
                cur = {"threads": int(m.group(1))}
                continue
            if cur is None:
                continue
            m = re.search(r"torch threads:\s*(\d+)", line)
            if m:
                cur["torch_threads"] = int(m.group(1))
            m = re.search(r"Step 1 time:\s*([\d.]+)\s*s", line)
            if m:
                cur["step1_s"] = float(m.group(1))
            m = re.search(r"Loaded in\s*([\d.]+)\s*s", line)
            if m:
                cur["load_s"] = float(m.group(1))
            m = re.search(r"Total infer time:\s*([\d.]+)\s*s\s*\(([\d.]+)\s*ms/chip\)", line)
            if m:
                cur["infer_s"] = float(m.group(1))
                cur["ms_per_chip"] = float(m.group(2))
            if "FAILED" in line:
                cur["failed"] = True
    if cur is not None:
        rows.append(cur)
    return rows


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python parse_thread_sweep.py <thread_sweep_*.out>",
              file=sys.stderr)
        sys.exit(1)
    rows = parse(sys.argv[1])
    if not rows:
        print("No THREADS sections found — is this a thread_sweep .out?",
              file=sys.stderr)
        sys.exit(1)

    baseline = next((r["ms_per_chip"] for r in rows
                     if r["threads"] == 1 and "ms_per_chip" in r), None)

    hdr = f"{'threads':>7} {'torch':>5} {'ms/chip':>9} {'infer_s':>8} " \
          f"{'step1_s':>8} {'load_s':>7} {'speedup':>8} {'efficiency':>10}"
    print(hdr)
    print("-" * len(hdr))
    for r in sorted(rows, key=lambda x: x["threads"]):
        t = r["threads"]
        if r.get("failed") or "ms_per_chip" not in r:
            print(f"{t:>7} {'—':>5} {'FAILED':>9}")
            continue
        ms = r["ms_per_chip"]
        speedup = (baseline / ms) if baseline else float("nan")
        # Parallel efficiency: speedup / threads. 1.0 = perfect scaling;
        # near 1/threads = no benefit (bandwidth-bound).
        eff = speedup / t if baseline else float("nan")
        print(f"{t:>7} {r.get('torch_threads','?'):>5} {ms:>9.1f} "
              f"{r.get('infer_s',float('nan')):>8.1f} "
              f"{r.get('step1_s',float('nan')):>8.2f} "
              f"{r.get('load_s',float('nan')):>7.2f} "
              f"{speedup:>7.2f}x {eff:>9.0%}")

    print()
    print("Interpretation:")
    print("  efficiency near 100%  -> compute-bound; more threads help, want")
    print("                           threads x concurrent ~= node cores.")
    print("  efficiency drops fast -> bandwidth-bound; extra threads wasted,")
    print("                           keep threads=1 and tune MAX_CONCURRENT.")


if __name__ == "__main__":
    main()
