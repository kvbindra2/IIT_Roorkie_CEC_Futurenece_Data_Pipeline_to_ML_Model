"""
Part F experiment helper - sets up each observability experiment and restores the originals.

Never edit include/data/nammamart/*.csv by hand: every experiment works on a COPY of the
pristine export in assignment/nammamart_dataset/, and `restore` puts the originals back.

Usage (project root, any Python 3.9+, standard library only):
    python -m include.experiments_karamvir list
    python -m include.experiments_karamvir setup 5      # prepare experiment 5
    python -m include.experiments_karamvir restore      # ALWAYS run after each experiment
    python -m include.experiments_karamvir status       # what is currently changed?
"""
from __future__ import annotations

import csv
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ORIGINALS = ROOT / "assignment" / "nammamart_dataset"
DATA = ROOT / "include" / "data" / "nammamart"
INCOMING = DATA / "incoming"
FILES = ["orders.csv", "payments.csv", "customers.csv", "products.csv", "stores.csv"]
INCREMENT = "orders_2026-10-01_increment.csv"


def _read(name):
    with open(DATA / name, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    return rows[0], rows[1:]


def _write(name, header, rows):
    with open(DATA / name, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def restore():
    for name in FILES:
        shutil.copyfile(ORIGINALS / name, DATA / name)
        bak = DATA / f"{name}.bak"
        if bak.exists():
            bak.unlink()
    for f in INCOMING.glob("*.csv"):
        f.unlink()
    print("Restored the 5 original files from assignment/nammamart_dataset/ and emptied incoming/.")
    print("Note: experiment 3 (increment) is meant to be permanent - re-run `setup 3` if you want it back.")


def status():
    changed = [n for n in FILES if not (DATA / n).exists()
               or (DATA / n).read_bytes() != (ORIGINALS / n).read_bytes()]
    inc = [f.name for f in INCOMING.glob("*.csv")]
    print("Changed/missing source files:", changed or "none")
    print("Files in incoming/:", inc or "none")


# ---------------------------------------------------------------------------------------
def exp3():
    """Incremental load: drop the next-day file into incoming/."""
    INCOMING.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ORIGINALS / INCREMENT, INCOMING / INCREMENT)
    print(f"Copied {INCREMENT} into include/data/nammamart/incoming/")


def exp4():
    """Missing source file: rename products.csv."""
    (DATA / "products.csv").rename(DATA / "products.csv.bak")
    print("Renamed products.csv -> products.csv.bak (extract/load_raw_products will fail and retry)")


def exp5():
    """New bad data: 5 rows appended to orders.csv."""
    header, rows = _read("orders.csv")
    good = dict(zip(header, rows[1]))                     # a known-good row to copy from
    def row(oid, **over):
        r = {**good, "order_id": oid, **over}
        return [r[c] for c in header]
    bad = [
        row("ORD900001", customer_id=""),                                    # null customer
        row("ORD900002", quantity="-2", order_amount="-720.0"),             # negative quantity
        row("ORD900003", store_id="S42"),                                    # unknown store
        row("ORD900004", order_status="SHIPPED"),                            # invalid status
        row("ORD900005", order_ts="2027-03-15 10:00:00"),                    # 2027 timestamp
    ]
    _write("orders.csv", header, rows + bad)
    print("Appended ORD900001..ORD900005 to orders.csv: null customer, qty -2, store S42, SHIPPED, 2027 ts")


def exp6():
    """Schema change: rename order_amount -> total."""
    header, rows = _read("orders.csv")
    header = ["total" if c == "order_amount" else c for c in header]
    _write("orders.csv", header, rows)
    print("Renamed column order_amount -> total in orders.csv")


def exp7():
    """Volume anomaly: only 50 orders. Run the DAG normally once BEFORE this so there is a baseline."""
    header, rows = _read("orders.csv")
    _write("orders.csv", header, rows[:50])
    print(f"orders.csv cut from {len(rows)} to 50 rows")


def _scale_store(scale_amount: bool, store="S05", factor=100):
    header, rows = _read("orders.csv")
    i_store, i_price, i_amt = header.index("store_id"), header.index("unit_price"), header.index("order_amount")
    n = 0
    for r in rows:
        if r[i_store] == store:
            try:
                r[i_price] = str(round(float(r[i_price]) * factor, 2))
                if scale_amount:
                    r[i_amt] = str(round(float(r[i_amt]) * factor, 2))
                n += 1
            except ValueError:
                pass
    _write("orders.csv", header, rows)
    return n


def exp8():
    """Distribution drift: unit_price x100 for store S05 (order_amount untouched)."""
    n = _scale_store(scale_amount=False)
    print(f"unit_price x100 for {n} S05 orders (order_amount unchanged)")


def exp8b():
    """Distribution drift, harder variant: unit_price AND order_amount x100 for S05 (internally consistent)."""
    n = _scale_store(scale_amount=True)
    print(f"unit_price and order_amount x100 for {n} S05 orders - row-level checks cannot see this")


def exp9():
    """Freshness: every order_ts moved back 30 days."""
    header, rows = _read("orders.csv")
    i = header.index("order_ts")
    for r in rows:
        try:
            r[i] = (datetime.strptime(r[i], "%Y-%m-%d %H:%M:%S") - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass                                          # leave the already-broken timestamps as they are
    _write("orders.csv", header, rows)
    print("All valid order_ts moved back 30 days (latest order now ~31 Aug 2026)")


EXPERIMENTS = {"3": exp3, "4": exp4, "5": exp5, "6": exp6, "7": exp7, "8": exp8, "8b": exp8b, "9": exp9}


def main(argv):
    if len(argv) < 2 or argv[1] == "list":
        for k, f in EXPERIMENTS.items():
            print(f"  {k:>3}  {f.__doc__.strip()}")
        print("  1, 2, 10 need no file changes (see PART_F_RUNBOOK.md)")
    elif argv[1] == "restore":
        restore()
    elif argv[1] == "status":
        status()
    elif argv[1] == "setup" and len(argv) == 3 and argv[2] in EXPERIMENTS:
        restore() if argv[2] != "3" else None
        EXPERIMENTS[argv[2]]()
    else:
        print(__doc__)


if __name__ == "__main__":
    main(sys.argv)
