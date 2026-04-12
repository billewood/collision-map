"""
Ingest collision data into the collision-map database.

Supports two sources:
  dispatch  — JSONL files produced by collision-underreporting pipeline
  switrs    — normalized CSV/JSONL from switrs/pull.py

Usage:
    python ingest.py --source dispatch --file path/to/incidents.jsonl --city el_cerrito
    python ingest.py --source switrs   --file path/to/switrs_normalized.csv --city el_cerrito
    python ingest.py --source dispatch --dir  path/to/incidents/el_cerrito/
"""
import json
import sys
from datetime import date
from pathlib import Path

import click
import pandas as pd

import database as db
from database import Incident, ImportRun, SessionLocal, create_tables


def ingest_switrs_df_upsert(df, city: str, session) -> tuple[int, int, int]:
    """
    Insert new SWITRS rows or update existing ones without touching geocoded coordinates.
    Returns (imported, updated, skipped).
    """
    imported = updated = skipped = 0
    for _, inc in df.iterrows():
        case_id = str(inc.get("switrs_case_id", "")) or None
        if not case_id:
            skipped += 1
            continue

        existing = session.query(Incident).filter_by(switrs_case_id=case_id).first()

        def _int(val):
            try: return int(val) if pd.notna(val) else None
            except (TypeError, ValueError): return None

        if existing:
            # Only update non-geocode fields — preserve lat/lon/geocoded/source_file
            existing.number_killed  = _int(inc.get("number_killed"))
            existing.number_injured = _int(inc.get("number_injured"))
            existing.party_ages     = inc.get("party_ages") or None
            existing.involves_bicycle    = bool(inc.get("involves_bicycle"))
            existing.involves_pedestrian = bool(inc.get("involves_pedestrian"))
            existing.injuries_mentioned  = inc.get("injuries_mentioned")
            existing.severity       = inc.get("severity")
            existing.incident_type  = inc.get("incident_type")
            updated += 1
        else:
            lat = inc.get("latitude") if pd.notna(inc.get("latitude", float("nan"))) else None
            lon = inc.get("longitude") if pd.notna(inc.get("longitude", float("nan"))) else None
            row = Incident(
                source="switrs", city=city,
                jurisdiction=inc.get("jurisdiction") or inc.get("city"),
                location_text=inc.get("location"),
                latitude=lat, longitude=lon,
                geocoded=lat is not None and lon is not None,
                incident_type=inc.get("incident_type"),
                involves_bicycle=bool(inc.get("involves_bicycle")),
                involves_pedestrian=bool(inc.get("involves_pedestrian")),
                injuries_mentioned=inc.get("injuries_mentioned"),
                severity=inc.get("severity"),
                collision_date=str(inc["collision_date"])[:10] if pd.notna(inc.get("collision_date")) else None,
                switrs_case_id=case_id,
                number_killed=_int(inc.get("number_killed")),
                number_injured=_int(inc.get("number_injured")),
                party_ages=inc.get("party_ages") or None,
            )
            session.add(row)
            imported += 1

    session.commit()
    return imported, updated, skipped


def _severity_from_switrs(code) -> str | None:
    mapping = {1: "fatal", 2: "severe", 3: "other", 4: "complaint"}
    try:
        return mapping.get(int(code))
    except (TypeError, ValueError):
        return None


def ingest_dispatch_jsonl(path: Path, city: str, session, overwrite: bool = False) -> tuple[int, int]:
    """Load a dispatch incidents JSONL file into the database."""
    imported = skipped = 0
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        inc = json.loads(line)

        if inc.get("incident_type") == "parse_error":
            skipped += 1
            continue
        if inc.get("confidence", 1.0) < 0.3:
            skipped += 1
            continue

        # Dedup by source_file + incident_type + location
        source_file = inc.get("source_file") or path.name
        existing = (
            session.query(Incident)
            .filter_by(
                source="dispatch",
                source_file=source_file,
                incident_type=inc.get("incident_type"),
                location_text=inc.get("location"),
            )
            .first()
        )
        if existing and not overwrite:
            skipped += 1
            continue
        if existing and overwrite:
            session.delete(existing)

        row = Incident(
            source="dispatch",
            city=city,
            jurisdiction=inc.get("jurisdiction"),
            location_text=inc.get("location"),
            incident_type=inc.get("incident_type"),
            involves_bicycle=bool(inc.get("involves_bicycle")),
            involves_pedestrian=bool(inc.get("involves_pedestrian")),
            injuries_mentioned=inc.get("injuries_mentioned"),
            cut_off=bool(inc.get("cut_off")),
            confidence=inc.get("confidence"),
            block_start_utc=inc.get("block_start_utc"),
            raw_text=inc.get("raw_text"),
            source_file=source_file,
        )
        session.add(row)
        imported += 1

    session.commit()
    return imported, skipped


def ingest_switrs_df(df: pd.DataFrame, city: str, session, overwrite: bool = False) -> tuple[int, int]:
    """Load a normalized SWITRS DataFrame into the database."""
    imported = skipped = 0
    for _, inc in df.iterrows():
        case_id = str(inc.get("switrs_case_id", "")) or None

        if case_id:
            existing = session.query(Incident).filter_by(switrs_case_id=case_id).first()
            if existing and not overwrite:
                skipped += 1
                continue
            if existing and overwrite:
                session.delete(existing)

        lat = inc.get("latitude") if pd.notna(inc.get("latitude", float("nan"))) else None
        lon = inc.get("longitude") if pd.notna(inc.get("longitude", float("nan"))) else None

        def _int(val):
            try: return int(val) if pd.notna(val) else None
            except (TypeError, ValueError): return None

        row = Incident(
            source="switrs",
            city=city,
            jurisdiction=inc.get("jurisdiction") or inc.get("city"),
            location_text=inc.get("location"),
            latitude=lat,
            longitude=lon,
            geocoded=lat is not None and lon is not None,
            incident_type=inc.get("incident_type"),
            involves_bicycle=bool(inc.get("involves_bicycle")),
            involves_pedestrian=bool(inc.get("involves_pedestrian")),
            injuries_mentioned=inc.get("injuries_mentioned"),
            severity=_severity_from_switrs(inc.get("collision_severity")),
            collision_date=str(inc["collision_date"])[:10] if pd.notna(inc.get("collision_date")) else None,
            switrs_case_id=case_id,
            number_killed=_int(inc.get("number_killed")),
            number_injured=_int(inc.get("number_injured")),
            party_ages=inc.get("party_ages") or None,
        )
        session.add(row)
        imported += 1

    session.commit()
    return imported, skipped


@click.command()
@click.option("--source", required=True, type=click.Choice(["dispatch", "switrs"]))
@click.option("--city", required=True, help="City key (e.g. el_cerrito)")
@click.option("--file", "file_path", default=None, help="Single file to ingest")
@click.option("--dir", "dir_path", default=None, help="Directory of JSONL files to ingest")
@click.option("--overwrite", is_flag=True, default=False, help="Replace existing records")
def main(source, city, file_path, dir_path, overwrite):
    """Ingest collision incidents into the collision-map database."""
    create_tables()
    session = SessionLocal()

    paths = []
    if file_path:
        paths = [Path(file_path)]
    elif dir_path:
        d = Path(dir_path)
        paths = sorted(d.rglob("*.jsonl" if source == "dispatch" else "*.csv"))
        # Exclude call_log files
        paths = [p for p in paths if "_call_log" not in p.name]
    else:
        click.echo("Provide --file or --dir", err=True)
        sys.exit(1)

    total_imported = total_skipped = 0
    for path in paths:
        click.echo(f"  {path.name}", nl=False)
        if source == "dispatch":
            imp, skp = ingest_dispatch_jsonl(path, city, session, overwrite)
        else:
            try:
                import pandas as pd
                sys.path.insert(0, str(Path(__file__).parent.parent / "collision-underreporting"))
                from switrs.pull import load_switrs_csv, filter_bike_ped, normalize
                df = normalize(filter_bike_ped(load_switrs_csv(path)), city)
            except ImportError:
                click.echo(" [skip — collision-underreporting not on path]", err=True)
                continue
            imp, skp = ingest_switrs_df(df, city, session, overwrite)

        click.echo(f" → {imp} imported, {skp} skipped")
        total_imported += imp
        total_skipped += skp

    session.add(ImportRun(
        source=source,
        city=city,
        records_imported=total_imported,
        records_skipped=total_skipped,
    ))
    session.commit()
    session.close()
    click.echo(f"\nDone. {total_imported} imported, {total_skipped} skipped.")


if __name__ == "__main__":
    main()
