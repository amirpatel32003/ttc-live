"""Roll raw positions up to one row per route, Toronto date and hour, and keep that table current.

    python -m ttc_live.aggregate --raw data/raw --agg data/route_hour.parquet --site site/data

The aggregate stores sums and counts, never averages, so hours can be merged and re-merged safely:
loading the same raw file twice replaces its hours instead of doubling them.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

AGG_SCHEMA = """
    local_date varchar, local_hour int, route varchar,
    reports bigint, speed_sum double, moving_reports bigint, moving_speed_sum double,
    stopped_reports bigint, bunched_reports bigint, vehicles bigint, first_report timestamptz, last_report timestamptz
"""

ROLLUP_SQL = """
    select
        local_date, local_hour, route,
        count(*)                                             as reports,
        sum(speed_kmh)                                       as speed_sum,
        count(*) filter (where speed_kmh > 0)                as moving_reports,
        coalesce(sum(speed_kmh) filter (where speed_kmh > 0), 0) as moving_speed_sum,
        count(*) filter (where speed_kmh = 0)                as stopped_reports,
        count(*) filter (where is_bunched)                   as bunched_reports,
        count(distinct vehicle_id)                           as vehicles,
        min(reported_at)                                     as first_report,
        max(reported_at)                                     as last_report
    from raw
    where direction <> ''
    group by 1, 2, 3
"""


def update_aggregate(raw_dir: Path, agg_path: Path) -> int:
    """Merge every raw file in raw_dir into the aggregate. Returns the row count of the aggregate."""
    con = duckdb.connect()
    con.execute(f"create table agg ({AGG_SCHEMA})")
    if agg_path.exists():
        con.execute(f"insert into agg select * from read_parquet('{agg_path.as_posix()}')")
    files = sorted(raw_dir.glob("positions_*.parquet")) if raw_dir.exists() else []
    if files:
        listing = ", ".join(f"'{f.as_posix()}'" for f in files)
        con.execute(f"create table raw as select * from read_parquet([{listing}], union_by_name = true)")
        con.execute(f"create table fresh as {ROLLUP_SQL}")
        # An hour collected in two runs (a late start, a re-run) is recomputed from all its raw rows we hold,
        # then replaces what was there. Sums from different raw files for the same hour are added together.
        con.execute(
            """
            create table merged as
            select local_date, local_hour, route,
                   sum(reports) as reports, sum(speed_sum) as speed_sum, sum(moving_reports) as moving_reports,
                   sum(moving_speed_sum) as moving_speed_sum, sum(stopped_reports) as stopped_reports,
                   sum(bunched_reports) as bunched_reports, max(vehicles) as vehicles,
                   min(first_report) as first_report, max(last_report) as last_report
            from (
                select * from fresh
                union all
                select a.* from agg a
                where not exists (
                    select 1 from fresh f
                    where f.local_date = a.local_date and f.local_hour = a.local_hour and f.route = a.route
                      and f.first_report <= a.last_report and f.last_report >= a.first_report
                )
            ) group by 1, 2, 3
            """
        )
    else:
        con.execute("create table merged as select * from agg")
    agg_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"copy (select * from merged order by 1, 2, 3) to '{agg_path.as_posix()}' (format parquet, compression zstd)")
    n = con.execute("select count(*) from merged").fetchone()[0]
    con.close()
    return n


def _iso(epoch_seconds) -> str | None:
    """Timestamps leave DuckDB as epoch seconds so nothing depends on an optional time zone package."""
    if epoch_seconds is None:
        return None
    return datetime.fromtimestamp(float(epoch_seconds), tz=timezone.utc).isoformat(timespec="seconds")


def build_history(agg_path: Path) -> dict:
    """What the page needs: typical speed and bunching by hour of day, per route and citywide."""
    con = duckdb.connect()
    con.execute(f"create table a as select *, dayofweek(cast(local_date as date)) in (0, 6) as weekend from read_parquet('{agg_path.as_posix()}')")
    cover = con.execute(
        "select epoch(min(first_report)), epoch(max(last_report)), count(distinct local_date), sum(reports), count(distinct local_date || '-' || local_hour) from a"
    ).fetchone()

    def by_hour(where: str, params: list) -> list:
        rows = con.execute(
            f"""
            select local_hour, sum(moving_speed_sum) / nullif(sum(moving_reports), 0), sum(bunched_reports) * 1.0 / nullif(sum(moving_reports), 0),
                   sum(stopped_reports) * 1.0 / nullif(sum(reports), 0), sum(reports), count(distinct local_date)
            from a where {where} group by 1 order by 1
            """, params).fetchall()
        return [{"h": h, "speed": None if s is None else round(s, 1), "bunched": None if b is None else round(b, 4),
                 "stopped": None if st is None else round(st, 4), "reports": int(n), "days": int(d)} for h, s, b, st, n, d in rows]

    routes = {}
    for route, speed, bunched, reports, veh in con.execute(
        """
        select route, sum(moving_speed_sum) / nullif(sum(moving_reports), 0), sum(bunched_reports) * 1.0 / nullif(sum(moving_reports), 0),
               sum(reports), max(vehicles)
        from a group by 1 having sum(reports) >= 200 order by 1
        """).fetchall():
        routes[route] = {"speed": None if speed is None else round(speed, 1), "bunched": None if bunched is None else round(bunched, 4),
                         "reports": int(reports), "peak_vehicles": int(veh), "hours": by_hour("route = ?", [route])}
    latest = con.execute("select max(local_date) from a").fetchone()[0]
    if latest:
        for route, speed, bunched, reports, days in con.execute(
            """
            select route, sum(moving_speed_sum) / nullif(sum(moving_reports), 0), sum(bunched_reports) * 1.0 / nullif(sum(moving_reports), 0),
                   sum(reports), count(distinct local_date)
            from a where cast(local_date as date) > cast(? as date) - 7 group by 1
            """, [latest]).fetchall():
            if route in routes:
                routes[route]["week"] = {"speed": None if speed is None else round(speed, 1), "bunched": None if bunched is None else round(bunched, 4),
                                         "reports": int(reports), "days": int(days)}
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "coverage": {
            "first_report": _iso(cover[0]), "last_report": _iso(cover[1]),
            "days": int(cover[2] or 0), "reports": int(cover[3] or 0), "hours": int(cover[4] or 0),
        },
        "city": {"weekday": by_hour("not weekend", []), "weekend": by_hour("weekend", [])},
        "routes": routes,
    }
    con.close()
    return payload


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw", type=Path, default=Path("data/raw"))
    ap.add_argument("--agg", type=Path, default=Path("data/route_hour.parquet"))
    ap.add_argument("--site", type=Path, default=Path("site/data"))
    args = ap.parse_args(argv)
    n = update_aggregate(args.raw, args.agg)
    history = build_history(args.agg)
    args.site.mkdir(parents=True, exist_ok=True)
    (args.site / "history.json").write_text(json.dumps(history, separators=(",", ":")), encoding="utf-8")
    con = duckdb.connect()
    con.execute(f"copy (select * from read_parquet('{args.agg.as_posix()}') order by 1, 2, 3) to '{(args.site / 'route_hour.csv').as_posix()}' (header)")
    print({"aggregate_rows": n, **history["coverage"], "routes": len(history["routes"])})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
