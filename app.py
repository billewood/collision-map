"""
Collision Map API
-----------------
FastAPI backend serving collision incident data for the interactive map.
Accepts incidents from EMS dispatch transcripts and SWITRS official records.
"""
from __future__ import annotations

import os
from datetime import date
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session

import database as db
from database import Incident

app = FastAPI(
    title="Collision Map",
    description="Interactive map of bicycle/pedestrian collisions from dispatch transcripts vs SWITRS",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")


# ── Startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
def startup():
    db.create_tables()


# ── UI ────────────────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
def serve_ui():
    return FileResponse("static/index.html")


# ── Incidents ─────────────────────────────────────────────────────────────────

@app.get("/incidents")
def list_incidents(
    source: Optional[str] = Query(None, description="dispatch | switrs | all"),
    city: Optional[str] = Query(None),
    jurisdiction: Optional[str] = Query(None),
    involves_bicycle: Optional[bool] = Query(None),
    involves_pedestrian: Optional[bool] = Query(None),
    date_start: Optional[str] = Query(None, description="YYYY-MM-DD"),
    date_end: Optional[str] = Query(None, description="YYYY-MM-DD"),
    geocoded_only: bool = Query(False, description="Only return incidents with lat/lon"),
    min_confidence: float = Query(0.0, description="Minimum confidence (dispatch only)"),
    limit: int = Query(2000, le=5000),
    session: Session = Depends(db.get_db),
):
    """
    Return incidents as GeoJSON FeatureCollection.
    Incidents without lat/lon are included as features with null geometry
    unless geocoded_only=true.
    """
    q = session.query(Incident)

    if source and source != "all":
        q = q.filter(Incident.source == source)
    if city:
        q = q.filter(Incident.city == city)
    if jurisdiction:
        q = q.filter(Incident.jurisdiction.ilike(f"%{jurisdiction}%"))
    if involves_bicycle is not None:
        q = q.filter(Incident.involves_bicycle == involves_bicycle)
    if involves_pedestrian is not None:
        q = q.filter(Incident.involves_pedestrian == involves_pedestrian)
    if date_start:
        q = q.filter(Incident.collision_date >= date_start)
    if date_end:
        q = q.filter(Incident.collision_date <= date_end)
    if geocoded_only:
        q = q.filter(Incident.latitude.isnot(None))
    if min_confidence > 0:
        q = q.filter(
            (Incident.source != "dispatch") | (Incident.confidence >= min_confidence)
        )

    rows = q.order_by(Incident.collision_date.desc()).limit(limit).all()

    features = []
    for r in rows:
        geometry = None
        if r.latitude is not None and r.longitude is not None:
            geometry = {"type": "Point", "coordinates": [r.longitude, r.latitude]}

        features.append({
            "type": "Feature",
            "geometry": geometry,
            "properties": {
                "id": r.id,
                "source": r.source,
                "city": r.city,
                "jurisdiction": r.jurisdiction,
                "incident_type": r.incident_type,
                "involves_bicycle": r.involves_bicycle,
                "involves_pedestrian": r.involves_pedestrian,
                "injuries_mentioned": r.injuries_mentioned,
                "severity": r.severity,
                "collision_date": r.collision_date,
                "block_start_utc": r.block_start_utc,
                "location_text": r.location_text,
                "confidence": r.confidence,
                "cut_off": r.cut_off,
            },
        })

    return {
        "type": "FeatureCollection",
        "features": features,
        "meta": {
            "total": len(features),
            "geocoded": sum(1 for f in features if f["geometry"] is not None),
            "dispatch": sum(1 for f in features if f["properties"]["source"] == "dispatch"),
            "switrs": sum(1 for f in features if f["properties"]["source"] == "switrs"),
        },
    }


@app.get("/incidents/summary")
def summary(
    city: Optional[str] = Query(None),
    date_start: Optional[str] = Query(None),
    date_end: Optional[str] = Query(None),
    session: Session = Depends(db.get_db),
):
    """Gap ratio summary: dispatch vs SWITRS counts by month."""
    q = session.query(Incident)
    if city:
        q = q.filter(Incident.city == city)
    if date_start:
        q = q.filter(Incident.collision_date >= date_start)
    if date_end:
        q = q.filter(Incident.collision_date <= date_end)

    rows = q.all()

    by_month: dict[str, dict] = {}
    for r in rows:
        if not r.collision_date:
            continue
        month = r.collision_date[:7]  # YYYY-MM
        if month not in by_month:
            by_month[month] = {"dispatch": 0, "switrs": 0}
        by_month[month][r.source] = by_month[month].get(r.source, 0) + 1

    table = []
    for month in sorted(by_month):
        d = by_month[month].get("dispatch", 0)
        s = by_month[month].get("switrs", 0)
        table.append({
            "month": month,
            "dispatch": d,
            "switrs": s,
            "gap_ratio": round(d / s, 2) if s > 0 else None,
        })

    return {"months": table, "total_dispatch": sum(r["dispatch"] for r in table),
            "total_switrs": sum(r["switrs"] for r in table)}


@app.get("/incidents/{incident_id}")
def get_incident(incident_id: int, session: Session = Depends(db.get_db)):
    row = session.get(Incident, incident_id)
    if not row:
        raise HTTPException(404, "Incident not found")
    return row


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
