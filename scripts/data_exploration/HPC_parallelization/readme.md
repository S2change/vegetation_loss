## HPC parallelisation strategy for chip inference with 50 % overlap

### How the 4 sub-grids look on the chip grid

```
Original grid             H-shifted (+128 px right)
┌───┬───┬───┬───┐         ┌─╫─┬─╫─┬─╫─┐
│ A │ B │ C │ D │         │A:B│B:C│C:D│   each cell = right half of
├───┼───┼───┼───┤         ├─╫─┼─╫─┼─╫─┤   left chip + left half of
│ E │ F │ G │ H │         │E:F│F:G│G:H│   right chip
└───┴───┴───┴───┘         └─╫─┴─╫─┴─╫─┘

V-shifted (+128 px down)  Diagonal (+128, +128)
┌───┬───┬───┬───┐         ┌─╫─┬─╫─┬─╫─┐
├A:E┼B:F┼C:G┼D:H┤         ├╬──┼╬──┼╬──┤  each cell uses
├───┼───┼───┼───┤         ├─╫─┼─╫─┼─╫─┤  quadrants from 4 chips
└───┴───┴───┴───┘         └─╫─┴─╫─┴─╫─┘
```

For an N_x × N_y grid this gives roughly:

- Original: N_x × N_y chips
- H-shifted: (N_x − 1) × N_y
- V-shifted: N_x × (N_y − 1)
- Diagonal: (N_x − 1) × (N_y − 1)
- **Total: ~4 × N_x × N_y** (≈ 6 400 for 1 600 chips)

---

### Recommended strategy: 2D block decomposition with a 1-chip ghost border

Assign each task a **B × B block of original chips** (B = 4 recommended).
The task independently forms all shifted chips whose pixels fall within that
block by loading an extra 1-chip-wide ghost border.

```
Task block (B=4, shown as ■)     Ghost border (shown as □)

□ □ □ □ □ □        ← top ghost row
□ ■ ■ ■ ■ □
□ ■ ■ ■ ■ □
□ ■ ■ ■ ■ □
□ ■ ■ ■ ■ □
□ □ □ □ □ □        ← bottom ghost row
```

Chips to **load** per task: `(B+1)² = 25` original chip chunks

Chips to **predict** per task:

| Sub-grid | Count in B=4 block |
|---|---|
| Original  | 4 × 4 = 16 |
| H-shifted | 3 × 4 = 12 |
| V-shifted | 4 × 3 = 12 |
| Diagonal  | 3 × 3 = 9  |
| **Total** | **49**     |

---

### RAM budget per CPU (5 GB)

| Item | Size |
|---|---|
| BACDM model weights + activations | ~1.5 GB |
| 25 original chip chunks (ghost included) | 25 × 60 MB = 1.5 GB |
| Before/after float32 composites for 25 chips | 25 × 5 MB = 125 MB |
| Working buffers | ~0.5 GB |
| **Total** | **~3.6 GB ✓** |

---

### Task distribution across 480 CPUs

For a 40 × 40 chip grid (1 600 chips) with B = 4:

```
Number of blocks = ceil(40/4) × ceil(40/4) = 10 × 10 = 100 blocks
Chips predicted  = 100 × 49 = 4 900  (~6 400 total; boundary blocks predict fewer)
CPUs available   = 5 nodes × 96 cores = 480
```

100 blocks across 480 CPUs means each CPU handles roughly 1 block.
Use a **job queue** (e.g. SLURM task array or Python `multiprocessing.Queue`)
rather than a static assignment so faster CPUs pick up slack automatically.

```
CPU 0 → block (0,0) → done → picks up block (1,5) → ...
CPU 1 → block (0,1) → done → picks up block (2,3) → ...
...
```

For the largest tiles (1 600 chips, 40 × 40 grid, 100 blocks), most CPUs
will each process exactly 1 block. For smaller tiles fewer CPUs are needed.

---

### Ghost cells: what "shared rows/columns" means in practice

Ghost chips are **read by both neighbouring tasks independently** — no message
passing, no shared memory. The HDF5 file is opened read-only by all tasks
simultaneously, which is safe. The slight redundancy (each ghost chip is loaded
twice) is negligible compared to synchronisation overhead.

```
Task (row 0, col 0) loads ghost column 4  ─┐
Task (row 0, col 1) also loads ghost col 4 ─┘  both read from HDF5, no conflict
```

---

### Post-processing: merging overlapping predictions

Each pixel in the final output map may have been predicted by 1, 2, or 4
chips (original + up to 3 shifted). After all tasks complete, merge by:

1. **Majority vote** (best for discrete class maps)
2. **Softmax score average** (if tasks also save raw logits)

This merge step is embarrassingly parallel by output row and can be a second
SLURM task array.

---

### End-to-end pipeline

```
Phase 1 — pre-processing  (1 CPU, ~17 min on HPC)
    extract_recent_chip_chunked.py  →  T29TPG_48ts_….h5

Phase 2 — inference  (≤ 100 CPUs, parallel)
    each task: load 25 chip chunks → form 49 chips → predict → write

Phase 3 — merge  (≤ N_y CPUs, parallel)
    combine overlapping predictions row by row → final map
```
