"""Generate evenly-spaced target dates for a prediction run.

The pipeline's `TARGET_DATES` is a comma-separated list of break dates to
predict on. Typing each one is tedious for a long span, so this module
turns a (start, end) pair into a regular cadence — one date every
`step_days` (default 45), starting `step_days` after the start date.

Used by submit_tile.sh: when START_DATE / END_DATE are given instead of an
explicit TARGET_DATES, it calls this module to build the list, then exports
it so every array task + the aggregator inherit the same dates.

CLI:
    python target_dates_creation.py 2025-01-01 2025-06-01
    python target_dates_creation.py 2025-01-01 2025-06-01 --step-days 30
"""
from __future__ import annotations

from datetime import date, timedelta


def make_target_dates(start: str, end: str, step_days: int = 45) -> list[str]:
    """Return YYYY-MM-DD dates from `start` to `end`, every `step_days`.

    The cadence starts at offset 0 — i.e. the first returned date is
    `step_days` after `start` (the start date itself is never included).
    Dates strictly after `end` are excluded; `end` is included only if it
    lands exactly on the cadence.

    Parameters
    ----------
    start, end : str
        Inclusive date bounds, "YYYY-MM-DD". `end` must not precede `start`.
    step_days : int
        Spacing in days between consecutive dates (default 45). Must be > 0.

    Returns
    -------
    list[str]
        Dates in "YYYY-MM-DD" order. Empty if the span is shorter than one
        step (no date falls strictly after `start` and on/before `end`).
    """
    if step_days <= 0:
        raise ValueError(f"step_days must be positive, got {step_days}")

    start_d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    if end_d < start_d:
        raise ValueError(f"end ({end}) precedes start ({start})")

    dates: list[str] = []
    current = start_d + timedelta(days=step_days)
    while current <= end_d:
        dates.append(current.isoformat())
        current += timedelta(days=step_days)
    return dates


def _main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Print comma-separated target dates every N days.",
    )
    parser.add_argument("start", help="start date, YYYY-MM-DD")
    parser.add_argument("end", help="end date, YYYY-MM-DD")
    parser.add_argument(
        "--step-days", type=int, default=45,
        help="spacing in days between dates (default 45)",
    )
    args = parser.parse_args(argv)

    dates = make_target_dates(args.start, args.end, args.step_days)
    # Print as a single comma-separated line — the exact TARGET_DATES form
    # submit_tile.sh expects.
    print(",".join(dates))
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
