#!/usr/bin/env python3
"""
mock_punch_generator.py - write fake ZKTeco punches to a CSV "device" file.

zk_erp_sync.py reads these files when a device section has driver = mock,
so you can test the full ERP side on a laptop without real hardware.

Examples:
  # normal IN (~09:00) and OUT (~18:00) for 3 users over the last 3 days
  python mock_punch_generator.py --file mock_data/main_gate.csv --users 1001,1002,1003 --days 3

  # one punch right now for user 1001 (simulates someone at the device)
  python mock_punch_generator.py --file mock_data/main_gate.csv --users 1001 --now

  # double press: two punches 5 seconds apart (should be skipped by the sync)
  python mock_punch_generator.py --file mock_data/main_gate.csv --users 1002 --now --double

  # punch from an ID that has no Employee in ERP (should become "unmapped")
  python mock_punch_generator.py --file mock_data/back_gate.csv --users 9999 --now
"""
import argparse
import csv
import random
from datetime import date, datetime, time, timedelta
from pathlib import Path

FIELDS = ["user_id", "timestamp", "punch", "status"]
FMT = "%Y-%m-%d %H:%M:%S"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--file", required=True, help="CSV file acting as the device memory")
    ap.add_argument("--users", required=True, help="comma separated device user IDs, e.g. 1001,1002")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--days", type=int, help="IN/OUT pair per user per day for the last N days (incl. today)")
    mode.add_argument("--now", action="store_true", help="one punch per user at the current time")
    ap.add_argument("--punch", type=int, default=0, help="punch code for --now: 0=IN (default), 1=OUT")
    ap.add_argument("--double", action="store_true", help="with --now: add a second punch 5s later")
    ap.add_argument("--seed", type=int, help="random seed for repeatable data")
    args = ap.parse_args()

    rnd = random.Random(args.seed)
    users = [u.strip() for u in args.users.split(",") if u.strip()]
    now = datetime.now().replace(microsecond=0)
    rows = []

    if args.days:
        today = date.today()
        for back in range(args.days - 1, -1, -1):
            day = today - timedelta(days=back)
            for u in users:
                t_in = datetime.combine(day, time(9, 0)) + timedelta(seconds=rnd.randint(-1200, 1200))
                t_out = datetime.combine(day, time(18, 0)) + timedelta(seconds=rnd.randint(-1800, 1800))
                rows += [(u, t_in, 0), (u, t_out, 1)]
        rows = [r for r in rows if r[1] <= now]  # a device can't hold future punches
    else:
        for u in users:
            rows.append((u, now, args.punch))
            if args.double:
                rows.append((u, now + timedelta(seconds=5), args.punch))

    rows.sort(key=lambda r: r[1])
    path = Path(args.file)
    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="") as fh:
        w = csv.writer(fh)
        if new_file:
            w.writerow(FIELDS)
        for u, t, p in rows:
            w.writerow([u, t.strftime(FMT), p, 1])

    print(f"Added {len(rows)} punches to {path}")
    for u, t, p in rows[-6:]:
        print(f"  {u:>6}  {t.strftime(FMT)}  {'IN' if p == 0 else 'OUT'}")


if __name__ == "__main__":
    main()
