"""
Geocoding: resolve text location strings to lat/lon using Nominatim (OSM).

Nominatim is free, no API key required, but rate-limited to 1 req/sec.
Results are cached in the SQLite database to avoid re-querying.

Usage:
    python geocode.py --db /data/collision_map.db
"""
import asyncio
import time
from typing import Optional

import httpx

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
# Required by Nominatim ToS — identify your app
USER_AGENT = "collision-map/1.0 (github.com/billewood/collision-map)"
# Bias results toward the Bay Area
VIEWBOX = "-122.5,37.7,-122.1,38.1"  # west,south,east,north


async def geocode_text(
    location: str,
    city_hint: str = "El Cerrito, CA",
    client: Optional[httpx.AsyncClient] = None,
) -> Optional[tuple[float, float]]:
    """
    Resolve a text location to (lat, lon).

    Tries two strategies:
      1. location + city_hint (e.g. "San Pablo Ave at Potrero Ave, Richmond, CA")
      2. location alone with viewbox bias

    Returns None if no result found.
    """
    queries = [
        f"{location}, {city_hint}",
        location,
    ]

    should_close = client is None
    if client is None:
        client = httpx.AsyncClient(
            headers={"User-Agent": USER_AGENT},
            timeout=10.0,
        )

    try:
        for query in queries:
            params = {
                "q": query,
                "format": "json",
                "limit": 1,
                "viewbox": VIEWBOX,
                "bounded": 0,  # don't restrict to viewbox, just bias toward it
                "countrycodes": "us",
            }
            resp = await client.get(NOMINATIM_URL, params=params)
            resp.raise_for_status()
            results = resp.json()
            if results:
                r = results[0]
                return float(r["lat"]), float(r["lon"])
            await asyncio.sleep(1.1)  # Nominatim rate limit: 1 req/sec
    finally:
        if should_close:
            await client.aclose()

    return None


async def geocode_batch(
    locations: list[tuple[int, str, str]],
    delay: float = 1.1,
) -> dict[int, tuple[float, float]]:
    """
    Geocode a batch of (id, location_text, city_hint) tuples.
    Returns dict mapping id → (lat, lon).
    Respects Nominatim rate limit with delay between requests.
    """
    results: dict[int, tuple[float, float]] = {}

    async with httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT},
        timeout=10.0,
    ) as client:
        for incident_id, location, city_hint in locations:
            coords = await geocode_text(location, city_hint, client=client)
            if coords:
                results[incident_id] = coords
            await asyncio.sleep(delay)

    return results
