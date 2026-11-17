"""Collector and aggregate, run end to end on a fake feed."""
import json
from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from ttc_live.aggregate import build_history, update_aggregate
from ttc_live.collect import collect

START = datetime(2026, 11, 16, 21, 58, 0, tzinfo=timezone.utc)   # 16:58 in Toronto, so the run crosses an hour


class FakeClock:
    def __init__(self, start):
        self.t = start

    def now(self):
        return self.t

    def sleep(self, seconds):
        self.t += timedelta(seconds=seconds)


def fake_feed(clock, fail_on=()):
    calls = {"n": 0}

    def fetch():
        calls["n"] += 1
        if calls["n"] in fail_on:
            raise RuntimeError("feed down")
        step = calls["n"]
        vehicles = []
        for i in range(6):
            vehicles.append({"routeTag": "504", "id": f"44{i:02d}", "dirTag": "504_1_504A", "heading": "90",
                             "lat": str(43.6480 + 0.004 * i), "lon": str(-79.39 + 0.0002 * step), "speedKmHr": "18", "secsSinceReport": "5"})
        # two vehicles running nose to tail on the 29
        for i in range(2):
            vehicles.append({"routeTag": "29", "id": f"80{i:02d}", "dirTag": "29_0_29A", "heading": "0",
                             "lat": str(43.66 + 0.0005 * i), "lon": "-79.43", "speedKmHr": "12", "secsSinceReport": "5"})
        # a vehicle that never reports again: same position, growing age
        vehicles.append({"routeTag": "510", "id": "4500", "dirTag": "510_1_510", "heading": "0", "lat": "43.65", "lon": "-79.39",
                         "speedKmHr": "0", "secsSinceReport": str(5 + 30 * (step - 1))})
        return {"vehicle": vehicles}

    return fetch


@pytest.fixture()
def run(tmp_path):
    clock = FakeClock(START)
    result = collect(4, 30, tmp_path / "raw", fetch=fake_feed(clock, fail_on={3}), sleep=clock.sleep, now=clock.now)
    return tmp_path, result


def test_collector_survives_a_failed_poll_and_dedupes_repeats(run):
    tmp, result = run
    assert result["failed_polls"] == 1 and result["polls"] == 8
    con = duckdb.connect()
    path = result["path"]
    assert con.execute(f"select count(*) from read_parquet('{path}') where vehicle_id = '4500'").fetchone()[0] == 1
    assert con.execute(f"select count(*) from (select vehicle_id, reported_at from read_parquet('{path}') group by 1, 2 having count(*) > 1)").fetchone()[0] == 0
    assert result["rows_kept"] == 8 * 8 + 1


def test_bunching_flag_is_stored_per_snapshot(run):
    _, result = run
    con = duckdb.connect()
    got = dict(con.execute(f"select route, bool_or(is_bunched) from read_parquet('{result['path']}') group by 1").fetchall())
    assert got == {"504": False, "29": True, "510": False}


def test_aggregate_splits_the_run_across_toronto_hours(run):
    tmp, _ = run
    agg = tmp / "route_hour.parquet"
    update_aggregate(tmp / "raw", agg)
    con = duckdb.connect()
    hours = con.execute(f"select local_hour, reports from read_parquet('{agg}') where route = '504' order by 1").fetchall()
    assert [h for h, _ in hours] == [16, 17]
    assert sum(n for _, n in hours) == 48


def test_loading_the_same_file_twice_does_not_double_count(run):
    tmp, _ = run
    agg = tmp / "route_hour.parquet"
    update_aggregate(tmp / "raw", agg)
    con = duckdb.connect()
    before = con.execute(f"select sum(reports), sum(speed_sum) from read_parquet('{agg}')").fetchone()
    update_aggregate(tmp / "raw", agg)
    assert con.execute(f"select sum(reports), sum(speed_sum) from read_parquet('{agg}')").fetchone() == before


def test_history_reports_speed_of_moving_vehicles_and_bunching_share(run):
    tmp, _ = run
    agg = tmp / "route_hour.parquet"
    update_aggregate(tmp / "raw", agg)
    # lower the threshold for the test by checking the hourly city series, which has no minimum
    hist = build_history(agg)
    weekday = {h["h"]: h for h in hist["city"]["weekday"]}
    assert weekday[17]["speed"] == pytest.approx((6 * 18 + 2 * 12) / 8, abs=0.1)
    assert weekday[17]["bunched"] == pytest.approx(2 / 8, abs=0.001)
    assert hist["coverage"]["days"] == 1
    assert hist["coverage"]["first_report"].startswith("2026-11-16T21:5")
    json.dumps(hist)


def test_history_route_week_covers_only_the_last_seven_days(tmp_path):
    agg = tmp_path / "agg.parquet"
    con = duckdb.connect()
    con.execute("""
        create table t (local_date varchar, local_hour int, route varchar, reports bigint, speed_sum double, moving_reports bigint,
                        moving_speed_sum double, stopped_reports bigint, bunched_reports bigint, vehicles bigint, first_report timestamptz, last_report timestamptz)
    """)
    for d, speed in (("2026-11-01", 40.0), ("2026-11-14", 10.0), ("2026-11-16", 20.0)):
        con.execute("insert into t values (?, 8, '504', 300, ?, 300, ?, 0, 30, 10, now(), now())", [d, speed * 300, speed * 300])
    con.execute(f"copy t to '{agg.as_posix()}' (format parquet)")
    wk = build_history(agg)["routes"]["504"]["week"]
    assert wk["days"] == 2 and wk["reports"] == 600
    assert wk["speed"] == pytest.approx(15.0, abs=0.1)
    assert wk["bunched"] == pytest.approx(0.1, abs=0.001)
