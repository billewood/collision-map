# Collision Map

Interactive web map comparing bicycle/pedestrian collisions from EMS dispatch radio transcripts against official SWITRS records. Built to visualize the underreporting gap in West Contra Costa County, CA — and reusable for any collision dataset with lat/lon or geocodable addresses.

**Live demo:** (deploy to Render to activate)

## What it does

Currently maps **3,189 bicycle and pedestrian crashes** from official SWITRS records across four West Contra Costa jurisdictions — El Cerrito, Richmond, Albany, and Berkeley — spanning 2014–2025. This represents a roughly 20× expansion from the initial El Cerrito-only dataset (158 crashes).

- Plots incidents on a clean CartoDB basemap
- **SWITRS** (official CA collision records) — color-coded by type: green = bicycle, amber = pedestrian, gray = vehicle only
- **Dispatch** (EMS scanner transcripts) — coming soon, from the [collision-underreporting](https://github.com/billewood/collision-underreporting) pipeline; once loaded, the gap ratio header will show how many collisions go unreported
- Sizes markers by injury severity; click any marker or list item to highlight and inspect
- Sidebar filters by source, type (bike/ped), and date range

## Architecture

```
tims.berkeley.edu CSV export
  └─ data/switrs/{city}/Crashes.csv + Parties.csv
       └─ python prepare_switrs.py --all --ingest → SQLite

Claude Haiku (address normalization) + Google Maps API (geocoding)
  └─ python geocode.py → fills lat/lon in SQLite

collision-underreporting pipeline
  └─ data/incidents/{city}/*.jsonl
       └─ python ingest.py --source dispatch → SQLite (future)

SQLite → FastAPI → GeoJSON API → Leaflet.js map
```

## Stack

- **Backend:** FastAPI + SQLAlchemy + SQLite (WAL mode)
- **Frontend:** Vanilla JS + Leaflet.js + CartoDB Positron tiles
- **Geocoding:** Claude Haiku (address normalization) + Google Maps Geocoding API
- **Bike network:** OpenStreetMap via Overpass API (loaded client-side)
- **Hosting:** Render.com (1 GB persistent disk for SQLite)

## Setup

```bash
pip install -r requirements.txt
```

Create a `.env` file in this directory:
```
ANTHROPIC_API_KEY=your_key
GOOGLE_MAPS_API_KEY=your_key
```

### Run locally

```bash
uvicorn app:app --reload
# → http://localhost:8000
```

---

## Adding new SWITRS data (repeat every few months)

SWITRS data is updated periodically on [tims.berkeley.edu](https://tims.berkeley.edu). Here's the full process to refresh it.

### 1. Download from TIMS

1. Go to [tims.berkeley.edu](https://tims.berkeley.edu) → Data → Collisions
2. Filter by:
   - **Jurisdiction:** El Cerrito, Richmond, Albany, Berkeley (run separately or together)
   - **Date range:** extend to cover new months since last update
   - **Collision type:** check "Bicycle" and "Pedestrian" only
3. Export → download `Crashes.csv` and `Parties.csv`
4. Place the files into the appropriate subdirectory:

```
data/switrs/
  el cerrito/
    Crashes.csv
    Parties.csv
  richmond/
    Crashes.csv
    Parties.csv
  albany and berkeley/
    Crashes.csv
    Parties.csv
```

> The directory names must match exactly — they map to city keys in the DB.

### 2. Ingest all cities

```bash
DB_PATH=/path/to/collision_map.db python3 prepare_switrs.py --all --ingest
```

Already-imported crashes are skipped by `switrs_case_id`, so re-running is safe.

To process a single city only:
```bash
python3 prepare_switrs.py --city "el cerrito" --ingest
python3 prepare_switrs.py --city "richmond" --ingest
python3 prepare_switrs.py --city "albany and berkeley" --ingest
```

### 3. Geocode new unresolved addresses

About half of SWITRS records have lat/lon already. The rest have intersection strings
(e.g. `"Ashbury Av & Lynn Av"`) that need geocoding.

The geocoder uses **Claude Haiku** to normalize SWITRS abbreviations and local names
(e.g. `Bart Path` → `Ohlone Greenway`, `Av` → `Avenue`), then **Google Maps** to resolve
coordinates. Results are validated against the West Contra Costa bounding box to reject
out-of-area matches.

```bash
python3 geocode.py
```

To preview what Claude will normalize without making any Google Maps calls:
```bash
python3 geocode.py --dry-run
```

To re-geocode rows that were previously resolved by an older geocoding method:
```bash
python3 geocode.py --reset-nominatim
```

**API keys required:** `GOOGLE_MAPS_API_KEY` and `ANTHROPIC_API_KEY` in `.env`.
Cost: ~$0.001 Claude + ~$0.005 Google per 1,000 addresses.

### 4. Verify on the map

```bash
uvicorn app:app --reload
```

Newly geocoded incidents appear in **red** on the map for QA purposes (they have
`source_file = 'google_maps'`). Once you're satisfied with placement, remove the
temporary override from `markerColor()` in `static/index.html`:

```js
// Remove this line when done with QA:
if (props.source_file === 'nominatim' || props.source_file === 'google_maps') return '#e53e3e';
```

---

## API

| Endpoint | Description |
|---|---|
| `GET /incidents` | GeoJSON FeatureCollection, filterable |
| `GET /incidents/summary` | Monthly gap ratio (dispatch vs SWITRS) |
| `GET /incidents/{id}` | Single incident detail |
| `GET /meta` | Date range and source coverage for current DB |

### Filter params for `/incidents`

| Param | Values |
|---|---|
| `source` | `dispatch` \| `switrs` |
| `involves_bicycle` | `true` \| `false` |
| `involves_pedestrian` | `true` \| `false` |
| `date_start` | `YYYY-MM-DD` |
| `date_end` | `YYYY-MM-DD` |
| `geocoded_only` | `true` \| `false` |
| `min_confidence` | `0.0`–`1.0` (dispatch only) |

## Deploy to Render

1. Push this repo to GitHub
2. New Web Service on [render.com](https://render.com), connect the repo
3. Render auto-detects `render.yaml` — no manual config needed
4. Set `GOOGLE_MAPS_API_KEY` and `ANTHROPIC_API_KEY` as environment variables in Render dashboard
5. After deploy, run ingest from your local machine pointing at the Render DB path (`/data/collision_map.db`)

## Related

- [collision-underreporting](https://github.com/billewood/collision-underreporting) — pipeline that produces the dispatch incident data (Broadcastify download → Whisper transcription → Claude extraction)
- [el_cerrito_infra](https://github.com/billewood/el_cerrito_infra) — infrastructure tracker this map's architecture is based on
