"""
SQLAlchemy models for collision-map.

Two tables:
  incidents   — one row per collision (from dispatch transcripts or SWITRS)
  import_runs — log of each data import (for deduplication and auditing)
"""
import os
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Boolean, Column, DateTime, Float, Integer, String, Text,
    create_engine, event,
)
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

DB_PATH = os.environ.get("DB_PATH", "./collision_map.db")
engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})

# Enable WAL mode for better concurrent read performance
@event.listens_for(engine, "connect")
def set_wal(dbapi_conn, _):
    dbapi_conn.execute("PRAGMA journal_mode=WAL")


SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


class Base(DeclarativeBase):
    pass


class Incident(Base):
    __tablename__ = "incidents"

    id = Column(Integer, primary_key=True, index=True)

    # Source: "dispatch" (from our pipeline) or "switrs" (official records)
    source = Column(String, nullable=False, index=True)

    # Geography
    city = Column(String, index=True)          # pipeline city key, e.g. "el_cerrito"
    jurisdiction = Column(String, index=True)  # actual city name, e.g. "Richmond"
    location_text = Column(Text)               # raw text from transcript or SWITRS
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    geocoded = Column(Boolean, default=False)

    # Incident classification
    incident_type = Column(String, index=True)   # traffic_collision, bicycle_collision, etc.
    involves_bicycle = Column(Boolean, default=False)
    involves_pedestrian = Column(Boolean, default=False)
    injuries_mentioned = Column(Boolean, nullable=True)
    severity = Column(String, nullable=True)     # null | fatal | severe | other | pdo

    # Temporal
    collision_date = Column(String, nullable=True, index=True)  # YYYY-MM-DD
    block_start_utc = Column(String, nullable=True)             # ISO timestamp of audio block

    # Counts & party detail
    number_killed = Column(Integer, nullable=True)
    number_injured = Column(Integer, nullable=True)
    party_ages = Column(String, nullable=True)   # comma-separated, e.g. "28, 45, 33"

    # Quality / provenance
    confidence = Column(Float, nullable=True)     # 0.0–1.0, dispatch source only
    cut_off = Column(Boolean, default=False)       # transcript cut off mid-dispatch
    source_file = Column(String, nullable=True)    # originating file name
    switrs_case_id = Column(String, nullable=True, index=True)

    # Raw content
    raw_text = Column(Text, nullable=True)

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class ImportRun(Base):
    __tablename__ = "import_runs"

    id = Column(Integer, primary_key=True)
    source = Column(String, nullable=False)       # "dispatch" or "switrs"
    city = Column(String, nullable=False)
    date_range_start = Column(String, nullable=True)
    date_range_end = Column(String, nullable=True)
    records_imported = Column(Integer, default=0)
    records_skipped = Column(Integer, default=0)
    run_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    notes = Column(Text, nullable=True)


def create_tables():
    Base.metadata.create_all(bind=engine)
    # Add new columns to existing DB without dropping the table
    _migrate_add_columns([
        ("number_killed",  "INTEGER"),
        ("number_injured", "INTEGER"),
        ("party_ages",     "TEXT"),
    ])


def _migrate_add_columns(cols: list[tuple[str, str]]) -> None:
    """Add columns to incidents table if they don't already exist (SQLite safe)."""
    with engine.connect() as conn:
        existing = {row[1] for row in conn.execute(
            __import__("sqlalchemy").text("PRAGMA table_info(incidents)")
        )}
        for col_name, col_type in cols:
            if col_name not in existing:
                conn.execute(__import__("sqlalchemy").text(
                    f"ALTER TABLE incidents ADD COLUMN {col_name} {col_type}"
                ))
                conn.commit()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
