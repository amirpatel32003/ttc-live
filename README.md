# TTC buses and streetcars, live

**Live map: https://amirpatel32003.github.io/ttc-live/**

Every TTC bus and streetcar on a map, refreshed every 15 seconds. Pick a route to see how fast it is moving, which vehicles are running in bunches, and how that compares with the route's usual day.

For the route you pick, it also shows where the longest gap between vehicles is in each direction and roughly how long that wait is. A Near me button lists the vehicles within a kilometre of you and whether each is heading your way.

This is the real-time half of a two-part look at TTC reliability. The other half is the [subway delay pipeline and dashboard](https://github.com/amirpatel32003/ttc-delay-pipeline), which works from the City's published history.

[![CI](https://github.com/amirpatel32003/ttc-live/actions/workflows/ci.yml/badge.svg)](https://github.com/amirpatel32003/ttc-live/actions/workflows/ci.yml)
[![Collect and publish](https://github.com/amirpatel32003/ttc-live/actions/workflows/collect.yml/badge.svg)](https://github.com/amirpatel32003/ttc-live/actions/workflows/collect.yml)

## The question

The TTC publishes where its vehicles are, but not how a route is doing. A rider at a stop wants to know if the 29 is crawling or if three buses are about to arrive together after a long gap. Answering that takes the live positions, a rule for what counts as bunching, and a record of what is normal for that route at that hour.

## How it works

| Part | What it does |
|---|---|
| Live map | The page calls the TTC's public vehicle feed from the browser every 15 seconds, so the map is current without any server of mine in between. |
| Collector | A scheduled job polls the same feed every 30 seconds for 50 minutes of each hour and writes one Parquet file per run. |
| Storage | Raw hours are kept for 7 days as assets on a GitHub release. A route-hour table of sums and counts is kept for good. |
| Aggregate | DuckDB rolls raw reports up to route, date and hour. The page reads a small JSON profile built from that table. |
| BigQuery (optional) | When a project and key are configured, each raw hour is also loaded into a partitioned, clustered BigQuery table. |

```
TTC vehicle feed (JSON, about 950 vehicles)
      |                                  |
  browser, every 15 s             collector, every 30 s
      |                                  |
  live map, bunching            positions_<run>.parquet  --> BigQuery (optional)
  and speed now                          |
      |                        route_hour.parquet (sums and counts)
      |                                  |
      +------- compares now with ----- history.json
```

## Decisions worth explaining

**The report time is not the fetch time.** Each vehicle says how many seconds ago it last reported. The collector subtracts that, then keeps one row per vehicle and report time. Without this a vehicle that has gone quiet would be counted again on every poll with the same old position.

**Bunching has a written rule.** Two vehicles are bunched when they are on the same route and direction, within 300 metres, and both moving. The moving condition keeps vehicles parked together at a terminal out of the count. The page and the collector use the same rule, and a test pins it down.

**The flag is set per snapshot, before de-duplication.** Whether a vehicle is bunched depends on where the others were at that same moment, so it has to be worked out while the snapshot is intact.

**The aggregate stores sums and counts, not averages.** An hour usually arrives in two pieces because runs do not line up with the clock. Sums can be added; averages cannot. Loading the same file twice replaces its hours instead of doubling them, and there is a test for that.

**Gaps are measured along the route, not as the crow flies.** Each vehicle is projected onto the line through the route's stops, which gives its distance from the start. The longest stretch between neighbours is the gap. Vehicle direction tags in the feed do not always match the route's own list, so both are reduced to the 0 or 1 in the middle of the tag. Vehicles more than 400 metres off the line, such as replacement buses on a detour, are left out and counted separately. The wait is that distance over the route's current pace, and the page calls it an estimate.

**Speed means moving speed.** Averages leave out stopped vehicles, and the stopped share is reported next to it. Mixing the two would make a route with long terminal layovers look slow.

**Hours are Toronto hours.** The local date and hour are fixed when the row is written, so nothing downstream has to know about time zones.

**One bad poll does not lose the hour.** A failed request is counted and skipped.

## Run it yourself

```bash
pip install -r requirements.txt
python -m ttc_live.collect --minutes 2 --every 30 --out data/raw
python -m ttc_live.aggregate --raw data/raw --agg data/route_hour.parquet --site site/data
python -m http.server -d site 8000        # then open http://localhost:8000
python -m pytest -q tests
```

## Limits

- The schedule runs on GitHub Actions, which starts jobs late when it is busy and sometimes skips one. Coverage is most of each hour, not all of it, and the page states how many hours it has.
- Speed is what each vehicle reported at one instant. It is not a trip time.
- The 300 metre rule is a simple distance check. It does not know about headways or the timetable.
- The record started in November 2026. Until several weeks are in, the typical-day chart is provisional and the page says so.
- The gap estimate assumes vehicles keep the route's current pace. It is not an arrival prediction.
- Near me judges direction from each vehicle's compass heading, which is unreliable when a vehicle is turning or stopped.
- Subway trains are not in this feed.

## Data

Vehicle positions come from the TTC's public real-time feed (the UmoIQ, formerly NextBus, API). Data copyright Toronto Transit Commission. Map tiles from OpenStreetMap contributors. The map library is Leaflet. This project is not affiliated with the TTC.

## Author

Amir Patel, Toronto. [Portfolio](https://amirpatel32003.github.io/) · [LinkedIn](https://www.linkedin.com/in/connect-amir)
