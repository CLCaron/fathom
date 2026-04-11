"""One-shot script to clean up polluted sector values in existing data.

Idempotent: safe to run multiple times. Updates rows where the sector
value matches a known yfinance alias to our canonical vocabulary.
"""

import sqlite3
import sys

from fathom.engine.pipeline import _SECTOR_ALIASES

DB_PATH = "data/signals.db"
TABLES = ["insider_trades", "congressional_trades", "sector_cache"]


def main():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    total = 0

    for table in TABLES:
        for alias, canonical in _SECTOR_ALIASES.items():
            cur.execute(
                f"UPDATE {table} SET sector = ? WHERE sector = ?",
                (canonical, alias),
            )
            count = cur.rowcount
            if count:
                print(f"  {table}: {alias!r} -> {canonical!r}: {count} rows")
                total += count

    con.commit()
    con.close()

    if total:
        print(f"\nUpdated {total} rows total.")
    else:
        print("No rows needed updating.")


if __name__ == "__main__":
    main()
