"""Read the TTC's public real-time vehicle feed (the UmoIQ / NextBus JSON API) and turn it into clean rows.

One call returns every bus and streetcar in service, about 950 vehicles at midday. Each vehicle says how many
seconds ago it last reported, so the true report time is the fetch time minus that.
"""
from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

FEED_URL = "https://retro.umoiq.com/service/publicJSONFeed"
TORONTO = ZoneInfo("America/Toronto")
# A box around the TTC service area. Anything outside is a GPS fault or a vehicle at a far-off garage.
LAT_MIN, LAT_MAX, LON_MIN, LON_MAX = 43.55, 43.92, -79.70, -79.05
MAX_REPORT_AGE_S = 180


@dataclass(frozen=True)
class Position:
    fetched_at: datetime        # UTC, when we asked
    reported_at: datetime       # UTC, when the vehicle last reported
    vehicle_id: str
    route: str
    direction: str              # the feed's dirTag, e.g. "504_1_504A"; empty when not on a run
    lat: float
    lon: float
    heading: int
    speed_kmh: float
    local_date: str             # Toronto calendar date of the report
    local_hour: int


def fetch_raw(session=None, timeout: int = 20, retries: int = 3) -> dict:
    import requests

    session = session or requests.Session()
    last: Exception | None = None
    for attempt in range(retries):
        try:
            resp = session.get(FEED_URL, params={"command": "vehicleLocations", "a": "ttc", "t": "0"}, timeout=timeout)
            resp.raise_for_status()
            body = resp.json()
            if "Error" in body:
                raise RuntimeError(str(body["Error"]))
            return body
        except Exception as exc:
            last = exc
            time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"feed unavailable after {retries} tries: {last}")


def _as_list(value) -> list:
    """The feed returns a dict instead of a list when only one vehicle is in service."""
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def parse(body: dict, fetched_at: datetime) -> list[Position]:
    """Keep vehicles that are on a route, inside the service area and reported recently."""
    rows: list[Position] = []
    for v in _as_list(body.get("vehicle")):
        try:
            lat, lon = float(v["lat"]), float(v["lon"])
            age = int(v.get("secsSinceReport", 0))
            speed = float(v.get("speedKmHr", 0) or 0)
            heading = int(float(v.get("heading", -1) or -1))
        except (KeyError, TypeError, ValueError):
            continue
        if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
            continue
        if age < 0 or age > MAX_REPORT_AGE_S or not v.get("routeTag") or not v.get("id"):
            continue
        reported = datetime.fromtimestamp(fetched_at.timestamp() - age, tz=timezone.utc)
        local = reported.astimezone(TORONTO)
        rows.append(
            Position(fetched_at, reported, str(v["id"]), str(v["routeTag"]), str(v.get("dirTag") or ""), lat, lon,
                     heading, max(0.0, min(speed, 120.0)), local.strftime("%Y-%m-%d"), local.hour)
        )
    return rows


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


BUNCH_DISTANCE_M = 300.0


def bunched_ids(rows: list[Position], distance_m: float = BUNCH_DISTANCE_M) -> set[str]:
    """Vehicles running bunched in one snapshot.

    Two vehicles count as bunched when they are on the same route and direction, within 300 m of each
    other, and both moving. Requiring movement leaves out vehicles parked together at a terminal.
    """
    by_run: dict[tuple[str, str], list[Position]] = {}
    for r in rows:
        if r.direction and r.speed_kmh > 0:
            by_run.setdefault((r.route, r.direction), []).append(r)
    out: set[str] = set()
    for group in by_run.values():
        for i, a in enumerate(group):
            for b in group[i + 1:]:
                if haversine_m(a.lat, a.lon, b.lat, b.lon) <= distance_m:
                    out.add(a.vehicle_id)
                    out.add(b.vehicle_id)
    return out
