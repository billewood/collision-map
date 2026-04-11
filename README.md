# Collision Map

Interactive web map comparing bicycle/pedestrian collisions from EMS dispatch radio transcripts against official SWITRS records. Built to visualize the underreporting gap in West Contra Costa County, CA — and reusable for any collision dataset with lat/lon or geocodable addresses.

**Live demo:** (deploy to Render to activate)

## What it does

- Plots incidents from two sources on an OpenStreetMap base layer:
  - **Dispatch** (red) — extracted from EMS scanner transcripts via the [collision-underreporting](https://github.com/billewood/collision-underreporting) pipeline
  - **SWITRS** (blue) — official California collision records from [TIMS](https://tims.berkeley.edu)
- Color-codes markers by type (bicycle = green, pedestrian = yellow, vehicle = red/blue)
- Sizes markers by injury severity
- Shows gap ratio in the header (dispatch count ÷ SWITRS count)
- Sidebar filters by source, type (bike/ped), and date range
- Click any incident for full detail panel

## Architecture

```
collision-underreporting pipeline
  └─ data/incidents/{city}/*.jsonl
       └─ python ingest.py --source dispatch → SQLite

tims.berkeley.edu CSV export
  └─ python ingest.py --source switrs → SQLite

SQLite → FastAPI → GeoJSON API → Leaflet.js map
```

## Stack

- **Backend:** FastAPI + SQLAlchemy + SQLite (WAL mode)
- **Frontend:** Vanilla JS + Leaflet.js + OpenStreetMap tiles
- **Geocoding:** Nominatim (free, no API key required)
- **Hosting:** Render.com (1 GB persistent disk for SQLite)

## Setup

```bash
pip install -r requirements.txt
```

### Ingest dispatch incidents

```bash
# Single date file
python ingest.py --source dispatch --file path/to/20251015.jsonl --city el_cerrito

# Entire incidents directory
python ingest.py --source dispatch --dir path/to/data/incidents/el_cerrito/ --city el_cerrito
```

### Ingest SWITRS data

Export a CSV from [tims.berkeley.edu](https://tims.berkeley.edu) (Data → Collisions, filter by jurisdiction and date range), then:

```bash
python ingest.py --source switrs --file path/to/switrs_export.csv --city el_cerrito
```

### Geocode unresolved incidents

Incidents from dispatch transcripts have text locations ("3500 block San Pablo Ave") rather than coordinates. After ingesting, run the geocoder to resolve them via Nominatim:

```bash
python geocode.py  # geocodes all unresolved incidents in the DB
```

Nominatim is rate-limited to 1 request/second — expect ~1 hour per 3,000 incidents.

### Run locally

```bash
uvicorn app:app --reload
# → http://localhost:8000
```

## Deploy to Render

1. Push this repo to GitHub
2. New Web Service on [render.com](https://render.com), connect the repo
3. Render auto-detects `render.yaml` — no manual config needed
4. After deploy, run ingest from your local machine pointing at the Render DB, or add a `/ingest` admin endpoint

## API

| Endpoint | Description |
|---|---|
| `GET /incidents` | GeoJSON FeatureCollection, filterable by source/type/date |
| `GET /incidents/summary` | Monthly gap ratio table (dispatch vs SWITRS counts) |
| `GET /incidents/{id}` | Single incident detail |

### Filter params for `/incidents`

| Param | Values |
|---|---|
| `source` | `dispatch` \| `switrs` \| `all` |
| `involves_bicycle` | `true` \| `false` |
| `involves_pedestrian` | `true` \| `false` |
| `date_start` | `YYYY-MM-DD` |
| `date_end` | `YYYY-MM-DD` |
| `geocoded_only` | `true` \| `false` |
| `min_confidence` | `0.0`–`1.0` (dispatch only) |

## Related

- [collision-underreporting](https://github.com/billewood/collision-underreporting) — pipeline that produces the dispatch incident data (Broadcastify download → Whisper transcription → Claude Haiku extraction)
- [el_cerrito_infra](https://github.com/billewood/el_cerrito_infra) — infrastructure tracker this map's architecture is based on
