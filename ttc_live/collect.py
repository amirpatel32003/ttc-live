"""Poll the feed for a while and write what was seen to one Parquet file.

    python -m ttc_live.collect --minutes 50 --every 30 --out data/raw

Each poll is a snapshot. A vehicle that has not reported since the last poll would repeat its old position,
so rows are kept once per (vehicle, report time). The bunching flag is worked out per snapshot, before
that de-duplication, because it depends on where the other vehicles were at the same moment.
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .feed import Position, bunched_ids, fetch_raw, parse

COLUMNS = ("snapshot_at", "reported_at", "vehicle_id", "route", "direction", "lat", "lon", "heading", "speed_kmh",
           "local_date", "local_hour", "is_bunched")


def snapshot_rows(body: dict, fetched_at: datetime) -> list[tuple]:
    rows: list[Position] = parse(body, fetched_at)
    bunched = bunched_ids(rows)
    return [
        (r.fetched_at, r.reported_at, r.vehicle_id, r.route, r.direction, r.lat, r.lon, r.heading, r.speed_kmh,
         r.local_date, r.local_hour, r.vehicle_id in bunched)
        for r in rows
    ]


def write_parquet(rows: list[tuple], path: Path) -> int:
    """De-duplicate on (vehicle, report time) and write. Returns the number of rows kept."""
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(
        "create table p (snapshot_at timestamptz, reported_at timestamptz, vehicle_id varchar, route varchar,"
        " direction varchar, lat double, lon double, heading int, speed_kmh double, local_date varchar,"
        " local_hour int, is_bunched boolean)"
    )
    if rows:
        con.executemany("insert into p values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    con.execute(
        f"""
        copy (
            select * exclude (rn) from (
                select *, row_number() over (partition by vehicle_id, reported_at order by snapshot_at) as rn from p
            ) where rn = 1 order by reported_at, vehicle_id
        ) to '{path.as_posix()}' (format parquet, compression zstd)
        """
    )
    kept = con.execute(f"select count(*) from read_parquet('{path.as_posix()}')").fetchone()[0]
    con.close()
    return kept


def collect(minutes: float, every: float, out_dir: Path, fetch=fetch_raw, sleep=time.sleep, now=None) -> dict:
    now = now or (lambda: datetime.now(timezone.utc))
    started = now()
    deadline = started.timestamp() + minutes * 60
    rows: list[tuple] = []
    polls = failures = 0
    while True:
        tick = now()
        try:
            rows.extend(snapshot_rows(fetch(), tick))
            polls += 1
        except Exception as exc:  # one bad poll must not lose the hour
            failures += 1
            print(f"poll failed: {exc}")
        if now().timestamp() + every > deadline:
            break
        sleep(every)
    path = out_dir / f"positions_{started.strftime('%Y%m%dT%H%M%SZ')}.parquet"
    kept = write_parquet(rows, path)
    return {"path": str(path), "polls": polls, "failed_polls": failures, "rows_seen": len(rows), "rows_kept": kept}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--minutes", type=float, default=50)
    ap.add_argument("--every", type=float, default=30, help="seconds between polls")
    ap.add_argument("--out", type=Path, default=Path("data/raw"))
    args = ap.parse_args(argv)
    result = collect(args.minutes, args.every, args.out)
    print(result)
    return 0 if result["polls"] > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
