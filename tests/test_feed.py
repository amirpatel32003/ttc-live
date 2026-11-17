from datetime import datetime, timezone

from ttc_live.feed import bunched_ids, haversine_m, parse

T0 = datetime(2026, 11, 16, 22, 0, 30, tzinfo=timezone.utc)   # 17:00:30 in Toronto


def veh(**kw):
    base = {"routeTag": "504", "predictable": "true", "heading": "90", "speedKmHr": "20", "lon": "-79.3900",
            "id": "4400", "dirTag": "504_1_504A", "lat": "43.6480", "secsSinceReport": "10"}
    base.update({k: str(v) for k, v in kw.items()})
    return base


def test_report_time_is_fetch_time_minus_age_and_hour_is_toronto_time():
    row = parse({"vehicle": [veh(secsSinceReport=45)]}, T0)[0]
    assert row.reported_at == datetime(2026, 11, 16, 21, 59, 45, tzinfo=timezone.utc)
    assert (row.local_date, row.local_hour) == ("2026-11-16", 16)


def test_single_vehicle_arrives_as_a_dict_not_a_list():
    assert len(parse({"vehicle": veh()}, T0)) == 1
    assert parse({}, T0) == []


def test_bad_rows_are_dropped():
    body = {"vehicle": [
        veh(id="1"),
        veh(id="2", lat="0", lon="0"),                 # GPS fault
        veh(id="3", secsSinceReport=900),              # stale
        veh(id="4", lat="not-a-number"),
        veh(id="5", routeTag=""),
    ]}
    assert [r.vehicle_id for r in parse(body, T0)] == ["1"]


def test_speed_is_clamped_to_a_sane_range():
    assert parse({"vehicle": [veh(speedKmHr=400)]}, T0)[0].speed_kmh == 120
    assert parse({"vehicle": [veh(speedKmHr=-3)]}, T0)[0].speed_kmh == 0


def test_haversine_matches_a_known_distance():
    # Union Station to Bloor-Yonge is about 2.6 km
    assert 2400 < haversine_m(43.6453, -79.3806, 43.6709, -79.3857) < 2950


def test_bunching_needs_same_run_close_together_and_moving():
    near = dict(lat="43.6481", lon="-79.3901")        # about 14 m from the base vehicle
    rows = parse({"vehicle": [
        veh(id="A"),
        veh(id="B", **near),                                           # bunched with A
        veh(id="C", **near, dirTag="504_0_504A"),                      # other direction
        veh(id="D", **near, routeTag="501", dirTag="501_1_501"),       # other route
        veh(id="E", **near, speedKmHr=0),                              # parked, e.g. at a terminal
        veh(id="F", lat="43.6600", lon="-79.3500"),                    # same run, far away
    ]}, T0)
    assert bunched_ids(rows) == {"A", "B"}
