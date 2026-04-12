"""
Geocoding: resolve SWITRS intersection strings to lat/lon.

Strategy:
  1. Claude normalizes all raw SWITRS location strings in one batch call
     (expands abbreviations, fixes formatting, adds correct city context)
  2. Google Maps Geocoding API resolves each normalized string to coordinates
  3. Results validated against per-city bbox before writing to DB

Usage:
    python geocode.py                        # geocode all unresolved incidents
    python geocode.py --dry-run             # preview normalized strings, no writes
    python geocode.py --reset-nominatim     # also re-geocode previously Nominatim-resolved rows
    python geocode.py --reset-google        # re-geocode rows previously resolved by Google Maps

Requires:
    GOOGLE_MAPS_API_KEY in environment or .env file
    ANTHROPIC_API_KEY in environment or .env file
"""
import asyncio
import json
import os
import time
from pathlib import Path
from typing import Optional

import googlemaps
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# ── Config ────────────────────────────────────────────────────────────────────

# Broad bounds passed to Google Maps as a soft bias
GMAPS_BOUNDS = {
    "southwest": {"lat": 37.7, "lng": -122.5},
    "northeast": {"lat": 38.2, "lng": -122.1},
}

# Per-city hard bboxes for validating results — reject anything outside
CITY_BBOX = {
    "el_cerrito": {"lat": (37.887, 37.952), "lon": (-122.345, -122.278)},
    "richmond":   {"lat": (37.888, 38.000), "lon": (-122.425, -122.290)},
    "albany":     {"lat": (37.878, 37.902), "lon": (-122.310, -122.275)},
    "berkeley":   {"lat": (37.836, 37.906), "lon": (-122.325, -122.228)},
}
# Fallback bbox covering the whole area
FALLBACK_BBOX = {"lat": (37.7, 38.2), "lon": (-122.6, -122.1)}

CITY_DISPLAY = {
    "el_cerrito": "El Cerrito, CA",
    "richmond":   "Richmond, CA",
    "albany":     "Albany, CA",
    "berkeley":   "Berkeley, CA",
}


# ── Step 1: Claude normalization ──────────────────────────────────────────────

SYSTEM_PROMPT = """\
You normalize raw SWITRS traffic collision location strings for geocoding.
SWITRS uses abbreviated street types and inconsistent formatting.

Rules:
- Expand abbreviations: Av→Avenue, Bl/Blvd→Boulevard, St→Street, Dr→Drive,
  Rd→Road, Ct→Court, Pl→Place, Ln→Lane, Ter→Terrace, Fwy→Freeway, Hwy→Highway
- Format intersections as "Street A at Street B, City, CA"
- If no cross street, use "Street A, City, CA"
- Use the city provided with each record — do NOT substitute a different city
- Strip noise like block numbers after the street name (e.g. "Colusa Av 540" → "Colusa Avenue")
- Local name mappings:
    "Bart Path" or "BART Path" → "Ohlone Greenway"
    "Spr Canyn Rd" → "Spruce Canyon Road"
    "San Pablo Dam Rd" → "San Pablo Dam Road"
- Return ONLY a JSON array of normalized strings, one per input, same order
- No explanation, no markdown, just the JSON array
"""


def _normalize_batch(items: list[tuple[str, str]], client: Anthropic) -> list[str]:
    """Normalize one batch via Claude.
    items: list of (location_text, city_display) tuples.
    Returns list of normalized strings, same length.
    """
    import re
    locations_with_city = [f"{loc} [{city}]" for loc, city in items]
    user_msg = (
        "Normalize these SWITRS location strings. "
        "Each entry includes the city in brackets — use that city in your output:\n"
        + json.dumps(locations_with_city, indent=2)
    )
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    text = response.content[0].text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()
    try:
        normalized = json.loads(text)
    except json.JSONDecodeError:
        print("Claude raw response:\n", text[:800])
        raise
    assert len(normalized) == len(items), \
        f"Claude returned {len(normalized)} results for {len(items)} inputs"
    return normalized


def normalize_with_claude(items: list[tuple[str, str]], batch_size: int = 20) -> list[str]:
    """Normalize all (location, city) pairs in batches of 20."""
    client = Anthropic()
    results = []
    total = (len(items) - 1) // batch_size + 1
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        print(f"  Claude batch {i // batch_size + 1}/{total} ({len(batch)} strings)...")
        results.extend(_normalize_batch(batch, client))
    return results


# ── Step 2: Google Maps geocoding ─────────────────────────────────────────────

def geocode_with_google(
    normalized: str,
    city_key: str,
    gmaps_client: googlemaps.Client,
) -> Optional[tuple[float, float]]:
    """Geocode a normalized address string. Returns (lat, lon) or None."""
    results = gmaps_client.geocode(normalized, bounds=GMAPS_BOUNDS)
    if not results:
        return None

    loc = results[0]["geometry"]["location"]
    lat, lon = loc["lat"], loc["lng"]

    # Validate within per-city bbox
    bbox = CITY_BBOX.get(city_key, FALLBACK_BBOX)
    if not (bbox["lat"][0] <= lat <= bbox["lat"][1] and
            bbox["lon"][0] <= lon <= bbox["lon"][1]):
        return None

    return lat, lon


# ── Main runner ───────────────────────────────────────────────────────────────

def run(db_path: str, dry_run: bool = False, reset_nominatim: bool = False,
        reset_google: bool = False) -> None:
    os.environ["DB_PATH"] = db_path

    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    import database as db
    db.create_tables()
    session = db.SessionLocal()

    try:
        from database import Incident
        from sqlalchemy import or_

        q = session.query(Incident).filter(Incident.location_text.isnot(None))

        if reset_google and reset_nominatim:
            q = q.filter(or_(
                Incident.latitude.is_(None),
                Incident.source_file == "nominatim",
                Incident.source_file == "google_maps",
            ))
        elif reset_google:
            q = q.filter(or_(
                Incident.latitude.is_(None),
                Incident.source_file == "google_maps",
            ))
        elif reset_nominatim:
            q = q.filter(or_(
                Incident.latitude.is_(None),
                Incident.source_file == "nominatim",
            ))
        else:
            q = q.filter(Incident.latitude.is_(None))

        unresolved = q.all()
        print(f"{len(unresolved)} incidents to geocode")
        if not unresolved:
            return

        # ── Step 1: normalize all strings with Claude ──
        items = [
            (r.location_text, CITY_DISPLAY.get(r.city, "El Cerrito, CA"))
            for r in unresolved
        ]
        print(f"Normalizing {len(items)} strings with Claude Haiku...")
        normalized = normalize_with_claude(items)

        # Show preview
        print("\nNormalized strings:")
        for (raw, city_disp), norm in zip(items, normalized):
            marker = "" if raw == norm else "  (changed)"
            print(f"  {raw!r:45} → {norm!r}{marker}")

        if dry_run:
            print("\n[dry run — no geocoding performed]")
            return

        # ── Step 2: geocode each with Google Maps ──
        api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
        if not api_key:
            raise ValueError("GOOGLE_MAPS_API_KEY not set in environment or .env")
        gmaps = googlemaps.Client(key=api_key)

        print(f"\nGeocoding with Google Maps...")
        resolved = skipped = 0
        for row, norm in zip(unresolved, normalized):
            coords = geocode_with_google(norm, row.city or "", gmaps)
            if coords:
                lat, lon = coords
                row.latitude = lat
                row.longitude = lon
                row.geocoded = True
                row.source_file = "google_maps"
                session.commit()
                resolved += 1
                print(f"  [ok] {norm!r:55} → {lat:.5f}, {lon:.5f}")
            else:
                # Clear bad coordinates if resetting
                if (reset_google or reset_nominatim) and row.latitude is not None:
                    row.latitude = None
                    row.longitude = None
                    row.geocoded = False
                    row.source_file = None
                    session.commit()
                print(f"  [skip] {norm!r} — outside {row.city} bbox")
                skipped += 1
            time.sleep(0.05)  # Google allows 50 req/sec; be polite

        print(f"\nDone. {resolved} resolved, {skipped} skipped.")

    finally:
        session.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Geocode unresolved incidents via Claude + Google Maps")
    parser.add_argument("--db", default=os.environ.get("DB_PATH", "./collision_map.db"))
    parser.add_argument("--dry-run", action="store_true", help="Preview normalized strings, no geocoding")
    parser.add_argument("--reset-nominatim", action="store_true",
                        help="Re-geocode rows previously resolved by Nominatim")
    parser.add_argument("--reset-google", action="store_true",
                        help="Re-geocode rows previously resolved by Google Maps (fixes wrong-city results)")
    args = parser.parse_args()
    run(args.db, dry_run=args.dry_run, reset_nominatim=args.reset_nominatim,
        reset_google=args.reset_google)
