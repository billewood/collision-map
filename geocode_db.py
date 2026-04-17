"""
geocode_db.py — Geocode missing coordinates in the collision_map DB.

Pulls ungeoocoded records for specified cities, normalizes addresses with
Claude Haiku, geocodes with Google Maps, and updates the DB in place.

Self-intersection addresses (e.g. "Colusa Av & Colusa Av 540") are detected
and reformatted as block addresses (e.g. "540 Colusa Avenue, City") before
geocoding.

Usage:
    python geocode_db.py --cities el_cerrito richmond albany berkeley
    python geocode_db.py --cities el_cerrito --dry-run
    python geocode_db.py --cities el_cerrito --bbox "37.895,37.935,-122.325,-122.280"

Requires .env with ANTHROPIC_API_KEY and GOOGLE_MAPS_API_KEY.
"""

import argparse
import os
import re
import sys
import time
from pathlib import Path
from typing import Optional

import googlemaps
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# Reuse geocoder logic from switrs-geocoder
sys.path.insert(0, str(Path(__file__).parent.parent / "switrs-geocoder"))
from geocode_switrs import (
    normalize_with_claude, geocode_one,
)

# ── Per-city bounding boxes (lat_min, lat_max, lon_min, lon_max) ─────────────
# These should generously cover the city limits including hills and edges.
CITY_BBOX = {
    "el_cerrito":    (37.895, 37.935, -122.330, -122.280),
    "richmond":      (37.880, 38.000, -122.430, -122.240),
    "albany":        (37.883, 37.900, -122.315, -122.280),
    "berkeley":      (37.840, 37.910, -122.325, -122.230),
    "oakland":       (37.700, 37.860, -122.355, -122.115),
    "san_francisco": (37.700, 37.820, -122.520, -122.350),
    "emeryville":    (37.828, 37.848, -122.310, -122.280),
    "san_pablo":     (37.950, 37.975, -122.360, -122.325),
    "piedmont":      (37.810, 37.835, -122.250, -122.215),
    "alameda":       (37.710, 37.800, -122.320, -122.225),
    "fremont":       (37.470, 37.600, -122.110, -121.870),
    "hayward":       (37.590, 37.680, -122.130, -122.020),
    "san_leandro":   (37.680, 37.740, -122.200, -122.120),
    "concord":       (37.940, 38.010, -122.070, -121.920),
    "walnut_creek":  (37.870, 37.930, -122.090, -122.010),
    "antioch":       (37.950, 38.020, -121.830, -121.740),
    "pleasanton":    (37.630, 37.690, -121.920, -121.840),
    "livermore":     (37.650, 37.720, -121.810, -121.700),
    "san_ramon":     (37.730, 37.800, -121.990, -121.910),
    "pittsburg":     (38.000, 38.040, -121.920, -121.850),
    "brentwood":     (37.900, 37.960, -121.740, -121.660),
    "pleasant_hill": (37.930, 37.970, -122.090, -122.040),
    "martinez":      (37.990, 38.030, -122.160, -122.090),
    "union_city":    (37.570, 37.610, -122.100, -122.010),
    "dublin":        (37.690, 37.730, -121.960, -121.850),
    "newark":        (37.500, 37.560, -122.070, -121.990),
}


def _fix_self_intersections(location_text: str) -> str:
    """
    Detect self-intersection addresses like 'Colusa Av & Colusa Av 540'
    and convert to block address '540 Colusa Av'.
    """
    if "&" not in location_text:
        return location_text
    parts = [p.strip() for p in location_text.split("&")]
    if len(parts) != 2:
        return location_text
    # Extract base street name (without trailing block numbers)
    base_a = re.sub(r"\s+\d+$", "", parts[0]).strip()
    base_b = re.sub(r"\s+\d+$", "", parts[1]).strip()
    if base_a.lower() == base_b.lower():
        # Same street — look for a block number in either part
        num_match = re.search(r"\d+$", parts[0]) or re.search(r"\d+$", parts[1])
        if num_match:
            return f"{num_match.group()} {base_a}"
        return base_a  # no block number, just return the street
    return location_text


def main():
    parser = argparse.ArgumentParser(description="Geocode missing coords in collision_map.db")
    parser.add_argument("--cities", nargs="+", required=True,
                        help="City keys to geocode (e.g. el_cerrito richmond)")
    parser.add_argument("--bbox", default=None,
                        help="Override bbox: 'lat_min,lat_max,lon_min,lon_max'")
    parser.add_argument("--batch-size", type=int, default=20,
                        help="Claude normalization batch size (default 20)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Normalize addresses only, skip Google Maps geocoding")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max records to process per city")
    args = parser.parse_args()

    from database import SessionLocal, Incident, create_tables
    create_tables()
    session = SessionLocal()

    total_resolved = total_skipped = 0

    for city in args.cities:
        print(f"\n{'='*60}")
        print(f"Geocoding: {city}")

        q = session.query(Incident).filter(
            Incident.source == "switrs",
            Incident.city == city,
            Incident.latitude.is_(None),
        )
        if args.limit:
            q = q.limit(args.limit)
        rows = q.all()
        print(f"  {len(rows)} records missing coordinates")

        if not rows:
            continue

        # Pre-process: fix self-intersection addresses
        fixed = 0
        for r in rows:
            original = r.location_text or ""
            cleaned = _fix_self_intersections(original)
            if cleaned != original:
                fixed += 1
        if fixed:
            print(f"  Fixed {fixed} self-intersection addresses")

        # Build location items for Claude normalization
        items = [
            (_fix_self_intersections(r.location_text or ""), r.jurisdiction or city)
            for r in rows
        ]

        # Normalize with Claude Haiku
        print(f"\n  Normalizing {len(items)} addresses with Claude Haiku...")
        normalized = normalize_with_claude(items, batch_size=args.batch_size)

        # Preview
        print(f"\n  Normalization preview (first 10):")
        for i in range(min(10, len(items))):
            flag = " *" if items[i][0] != (rows[i].location_text or "") else ""
            print(f"    {items[i][0]!r:45} → {normalized[i]!r}{flag}")
        if len(items) > 10:
            print(f"    ... and {len(items) - 10} more")

        if args.dry_run:
            print(f"\n  [dry run — skipping Google Maps]")
            continue

        # Geocode with Google Maps
        api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
        if not api_key:
            raise SystemExit("GOOGLE_MAPS_API_KEY not set. Add to .env file.")
        gmaps = googlemaps.Client(key=api_key)

        bbox: Optional[tuple] = None
        if args.bbox:
            bbox = tuple(float(x) for x in args.bbox.split(","))
        elif city in CITY_BBOX:
            bbox = CITY_BBOX[city]

        resolved = skipped = 0
        failed_addrs = []
        print(f"\n  Geocoding with Google Maps"
              f"{' (bbox: ' + city + ')' if bbox else ''}...")
        for row, norm in zip(rows, normalized):
            coords = geocode_one(norm, gmaps, bbox)
            if coords:
                lat, lon = coords
                row.latitude = lat
                row.longitude = lon
                row.geocoded = True
                resolved += 1
            else:
                skipped += 1
                failed_addrs.append((row.id, row.location_text, norm))
            time.sleep(0.05)

        session.commit()
        total_resolved += resolved
        total_skipped += skipped

        print(f"\n  {city}: {resolved} resolved, {skipped} failed/outside bbox")
        if failed_addrs:
            print(f"  Failed addresses:")
            for rid, orig, norm in failed_addrs:
                print(f"    [{rid}] {orig!r} → {norm!r}")

    session.close()
    print(f"\n{'='*60}")
    print(f"TOTAL: {total_resolved} resolved, {total_skipped} failed")
    print(f"Done.")


if __name__ == "__main__":
    main()
