"""
prepare_switrs.py — Load and merge SWITRS Crashes + Parties CSVs.

Produces a single DataFrame (one row per party) with location, people,
and crash-characteristic columns. Coded fields are decoded into human-
readable labels alongside the raw codes.

Usage:
    python prepare_switrs.py                    # prints summary, saves merged.csv
    python prepare_switrs.py --ingest           # also loads into collision_map.db

    from prepare_switrs import load_merged
    df = load_merged()
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

SWITRS_ROOT = Path(__file__).parent / "data" / "switrs"
DATA_DIR = SWITRS_ROOT  # legacy alias

# Each subdirectory name → city key used in the DB
CITY_DIRS = {
    "el cerrito":          "el_cerrito",
    "richmond":            "richmond",
    "albany and berkeley": "albany_berkeley",
}

# ── SWITRS code → label mappings ──────────────────────────────────────────────

SEVERITY = {1: "fatal", 2: "severe_injury", 3: "other_visible_injury", 4: "complaint_of_pain", 0: "no_injury"}

COLLISION_TYPE = {
    "A": "head_on", "B": "sideswipe", "C": "rear_end", "D": "broadside",
    "E": "hit_object", "F": "overturned", "G": "vehicle_pedestrian", "H": "other",
}

MVIW_MAP = {
    "A": "non_collision", "B": "pedestrian", "C": "other_motor_vehicle",
    "D": "motor_vehicle_on_other_roadway", "E": "parked_motor_vehicle",
    "F": "train", "G": "bicycle", "H": "animal", "I": "fixed_object", "J": "other_object",
}

LIGHTING_MAP = {
    "A": "daylight", "B": "dusk_dawn", "C": "dark_street_lights",
    "D": "dark_no_lights", "E": "dark_not_reported",
}

WEATHER_MAP = {
    "A": "clear", "B": "cloudy", "C": "raining", "D": "snowing",
    "E": "fog", "F": "other", "G": "wind",
}

ROAD_SURFACE_MAP = {"A": "dry", "B": "wet", "C": "snowy_icy", "D": "slippery"}

HIT_AND_RUN_MAP = {"M": "misdemeanor", "F": "felony", "N": "no", "-": "no"}

PED_ACTION_MAP = {
    "A": "no_pedestrian", "B": "crossing_in_crosswalk_at_intersection",
    "C": "crossing_in_crosswalk_not_at_intersection", "D": "crossing_not_in_crosswalk",
    "E": "in_road_includes_shoulder", "F": "not_in_road", "G": "approaching_waiting_to_cross",
    "H": "other",
}

PARTY_TYPE_MAP = {1: "driver", 2: "pedestrian", 3: "parked_vehicle", 4: "bicyclist", 5: "other"}

SOBRIETY_MAP = {
    "A": "not_drinking", "B": "drinking_not_under_influence",
    "C": "under_influence", "D": "impairment_unknown", "G": "not_applicable",
    "H": "impairment_physical", "-": "unknown",
}

SAFETY_EQUIP_MAP = {
    "A": "none", "B": "unknown", "C": "lap_belt", "D": "lap_shoulder_belt",
    "E": "shoulder_harness", "F": "child_restraint", "G": "bicycle_helmet",
    "H": "other", "J": "not_required", "M": "airbag", "N": "not_applicable",
    "-": "not_applicable",
}

RACE_MAP = {
    "A": "white", "B": "hispanic", "C": "black", "D": "asian",
    "E": "filipino", "F": "pacific_islander", "G": "american_indian", "H": "other",
    "-": "unknown",
}

MOVE_PRE_ACC_MAP = {
    "A": "stopped", "B": "proceeding_straight", "C": "ran_off_road_right",
    "D": "ran_off_road_left", "E": "making_right_turn", "F": "making_left_turn",
    "G": "making_U_turn", "H": "backing", "I": "slowing_stopping",
    "J": "passing_other_vehicle", "K": "changing_lanes", "L": "parking_maneuver",
    "M": "entering_traffic", "N": "other_unsafe_turning", "O": "crossed_into_opposing_lane",
    "P": "parked", "Q": "merging", "R": "traveling_wrong_way", "S": "other",
    "-": "unknown",
}

VEHICLE_TYPE_MAP = {
    "A": "passenger_car", "B": "passenger_car_with_trailer", "C": "motorcycle",
    "D": "pickup_truck", "E": "pickup_truck_with_trailer", "F": "panel_truck",
    "G": "panel_truck_with_trailer", "H": "truck_tractor", "I": "truck_tractor_with_trailer",
    "J": "school_bus", "K": "other_bus", "L": "emergency_vehicle", "M": "highway_construction",
    "N": "city_bus", "O": "other_truck", "P": "bicycle", "Q": "moped",
    "R": "pedestrian", "S": "other",
}

DAY_OF_WEEK_MAP = {1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday",
                   5: "Friday", 6: "Saturday", 7: "Sunday"}

PCF_CATEGORY_MAP = {
    "00": "unknown", "01": "dui", "02": "impeding_traffic", "03": "unsafe_speed",
    "04": "following_too_closely", "05": "wrong_side_of_road", "06": "improper_passing",
    "07": "unsafe_lane_change", "08": "improper_turning", "09": "automobile_right_of_way",
    "10": "pedestrian_right_of_way", "11": "pedestrian_violation", "12": "traffic_signals",
    "13": "hazardous_parking", "14": "lights", "15": "brakes", "16": "other_equipment",
    "17": "other_hazardous_mv", "18": "other_than_driver",
    "21": "unsafe_speed", "22": "wrong_side_of_road", "23": "improper_passing",
    "24": "unsafe_lane_change", "25": "improper_turning", "26": "automobile_right_of_way",
    "27": "pedestrian_right_of_way", "28": "pedestrian_violation", "29": "traffic_signals",
    "-": "unknown",
}


def _decode(series: pd.Series, mapping: dict) -> pd.Series:
    """Map coded values to labels. Tries raw value, int coercion, and str coercion."""
    def _lookup(v):
        if pd.isna(v):
            return None
        if v in mapping:
            return mapping[v]
        try:
            if int(v) in mapping:
                return mapping[int(v)]
        except (ValueError, TypeError):
            pass
        return mapping.get(str(v), None)
    return series.map(_lookup)


# ── Column selections ─────────────────────────────────────────────────────────

CRASH_COLS = [
    "CASE_ID", "COLLISION_DATE", "COLLISION_TIME", "ACCIDENT_YEAR", "DAY_OF_WEEK",
    # Location
    "PRIMARY_RD", "SECONDARY_RD", "INTERSECTION", "DISTANCE", "DIRECTION",
    "LATITUDE", "LONGITUDE",
    "CITY", "COUNTY",
    # Severity & counts
    "COLLISION_SEVERITY", "NUMBER_KILLED", "NUMBER_INJURED", "PARTY_COUNT",
    "COUNT_SEVERE_INJ", "COUNT_VISIBLE_INJ", "COUNT_COMPLAINT_PAIN",
    "COUNT_PED_KILLED", "COUNT_PED_INJURED",
    "COUNT_BICYCLIST_KILLED", "COUNT_BICYCLIST_INJURED",
    # Crash characteristics
    "TYPE_OF_COLLISION", "MVIW", "PED_ACTION", "HIT_AND_RUN",
    "ALCOHOL_INVOLVED", "PEDESTRIAN_ACCIDENT", "BICYCLE_ACCIDENT",
    "STWD_VEHTYPE_AT_FAULT",
    # Cause
    "PRIMARY_COLL_FACTOR", "PCF_VIOL_CATEGORY",
    # Environment
    "WEATHER_1", "ROAD_SURFACE", "ROAD_COND_1", "LIGHTING",
]

PARTY_COLS = [
    "CASE_ID", "PARTY_NUMBER", "PARTY_TYPE",
    "AT_FAULT", "PARTY_SEX", "PARTY_AGE", "RACE",
    "PARTY_SOBRIETY", "PARTY_DRUG_PHYSICAL",
    "PARTY_SAFETY_EQUIP_1",
    "MOVE_PRE_ACC",
    "VEHICLE_YEAR", "VEHICLE_MAKE", "STWD_VEHICLE_TYPE",
    "INATTENTION",
    "OAF_1", "OAF_2",
    "PARTY_NUMBER_KILLED", "PARTY_NUMBER_INJURED",
]


# ── Main loader ───────────────────────────────────────────────────────────────

def load_merged(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    """
    Load Crashes.csv + Parties.csv, merge on CASE_ID, return cleaned DataFrame.
    One row per party (multiple rows per crash for multi-party incidents).
    Coded fields are decoded into human-readable label columns (suffix _label).
    """
    crashes = pd.read_csv(data_dir / "Crashes.csv", usecols=CRASH_COLS, low_memory=False)
    parties = pd.read_csv(data_dir / "Parties.csv", usecols=PARTY_COLS, low_memory=False)

    # Parse date
    crashes["COLLISION_DATE"] = pd.to_datetime(crashes["COLLISION_DATE"], errors="coerce")

    # Zero-pad COLLISION_TIME to 4 digits, then format as HH:MM
    crashes["COLLISION_TIME"] = (
        crashes["COLLISION_TIME"]
        .fillna(0)
        .astype(int)
        .astype(str)
        .str.zfill(4)
        .apply(lambda t: f"{t[:2]}:{t[2:]}" if len(t) == 4 else None)
    )

    # Clean location text
    for col in ("PRIMARY_RD", "SECONDARY_RD"):
        crashes[col] = crashes[col].str.strip().str.title()

    # Decode crash fields
    crashes["severity_label"]        = _decode(crashes["COLLISION_SEVERITY"], SEVERITY)
    crashes["collision_type_label"]   = _decode(crashes["TYPE_OF_COLLISION"],  COLLISION_TYPE)
    crashes["mviw_label"]             = _decode(crashes["MVIW"],               MVIW_MAP)
    crashes["lighting_label"]         = _decode(crashes["LIGHTING"],           LIGHTING_MAP)
    crashes["weather_label"]          = _decode(crashes["WEATHER_1"],          WEATHER_MAP)
    crashes["road_surface_label"]     = _decode(crashes["ROAD_SURFACE"],       ROAD_SURFACE_MAP)
    crashes["hit_and_run_label"]      = _decode(crashes["HIT_AND_RUN"],        HIT_AND_RUN_MAP)
    crashes["ped_action_label"]       = _decode(crashes["PED_ACTION"],         PED_ACTION_MAP)
    crashes["day_of_week_label"]      = _decode(crashes["DAY_OF_WEEK"],        DAY_OF_WEEK_MAP)
    crashes["pcf_category_label"]     = _decode(crashes["PCF_VIOL_CATEGORY"],  PCF_CATEGORY_MAP)

    # Decode party fields
    parties["party_type_label"]       = _decode(parties["PARTY_TYPE"],         PARTY_TYPE_MAP)
    parties["sobriety_label"]         = _decode(parties["PARTY_SOBRIETY"],     SOBRIETY_MAP)
    parties["safety_equip_label"]     = _decode(parties["PARTY_SAFETY_EQUIP_1"], SAFETY_EQUIP_MAP)
    parties["race_label"]             = _decode(parties["RACE"],               RACE_MAP)
    parties["move_pre_acc_label"]     = _decode(parties["MOVE_PRE_ACC"],       MOVE_PRE_ACC_MAP)
    parties["vehicle_type_label"]     = _decode(parties["STWD_VEHICLE_TYPE"],  VEHICLE_TYPE_MAP)

    # Clean sentinel ages (998/999 = unknown in SWITRS)
    parties["PARTY_AGE"] = pd.to_numeric(parties["PARTY_AGE"], errors="coerce")
    parties.loc[parties["PARTY_AGE"] >= 120, "PARTY_AGE"] = None

    # Merge: left join keeps all parties, attaches crash info
    df = parties.merge(crashes, on="CASE_ID", how="left")

    # Tidy column names to lowercase
    df.columns = [c.lower() for c in df.columns]

    return df


def print_summary(df: pd.DataFrame, label: str = "") -> None:
    n_crashes = df["case_id"].nunique()
    n_parties = len(df)
    print(f"\n{'='*50}")
    print(f"  SWITRS {label}  ({n_crashes} crashes, {n_parties} parties)")
    print(f"{'='*50}")

    print("\nSeverity breakdown:")
    print(df.groupby("case_id").first()["severity_label"].value_counts().to_string())

    print("\nCrash type:")
    print(df.groupby("case_id").first()["collision_type_label"].value_counts().to_string())

    print("\nInvolves bicycle / pedestrian:")
    crash_df = df.groupby("case_id").first()
    print(f"  Bicycle:    {(crash_df['bicycle_accident'] == 'Y').sum()}")
    print(f"  Pedestrian: {(crash_df['pedestrian_accident'] == 'Y').sum()}")

    print("\nParty types:")
    print(df["party_type_label"].value_counts().to_string())

    print("\nAge distribution (non-null, excl. sentinel 998/999):")
    ages = pd.to_numeric(df["party_age"], errors="coerce")
    ages = ages[(ages >= 0) & (ages < 120)]  # filter sentinels
    print(f"  median {ages.median():.0f}, mean {ages.mean():.1f}, min {ages.min():.0f}, max {ages.max():.0f}")

    print("\nMissing lat/lon:", crash_df[["latitude", "longitude"]].isna().any(axis=1).sum(), "crashes")
    print()


# ── Ingest helper ─────────────────────────────────────────────────────────────

def ingest_city(data_dir: Path, city_key: str) -> None:
    """Load, merge, and ingest one city's Crashes.csv + Parties.csv into the DB."""
    from ingest import ingest_switrs_df, ingest_switrs_df_upsert
    from database import create_tables, SessionLocal

    label = data_dir.name.title()
    print(f"\n── {label} ──────────────────────────────────────")
    df = load_merged(data_dir)
    print_summary(df, label=label)

    out_path = data_dir / "merged.csv"
    df.to_csv(out_path, index=False)
    print(f"Saved merged CSV → {out_path}")

    # Aggregate party ages per crash before collapsing to one-row-per-crash
    ages_by_case = (
        df.groupby("case_id")["party_age"]
        .apply(lambda s: ", ".join(str(int(v)) for v in s.dropna()))
    )

    # Build one-row-per-crash ingest dataframe
    ingest_df = df.groupby("case_id").first().reset_index().copy()
    ingest_df["involves_bicycle"]    = ingest_df["bicycle_accident"] == "Y"
    ingest_df["involves_pedestrian"] = ingest_df["pedestrian_accident"] == "Y"
    ingest_df["injuries_mentioned"]  = ingest_df["number_injured"].fillna(0) > 0
    ingest_df["party_ages"]          = ingest_df["case_id"].map(ages_by_case)
    ingest_df = ingest_df.rename(columns={
        "case_id":              "switrs_case_id",
        "severity_label":       "severity",
        "collision_type_label": "incident_type",
        "city":                 "jurisdiction",
        "number_killed":        "number_killed",
        "number_injured":       "number_injured",
    })
    ingest_df["location"] = (
        ingest_df["primary_rd"].fillna("") + " & " + ingest_df["secondary_rd"].fillna("")
    ).str.strip(" &")
    ingest_df["collision_date"] = ingest_df["collision_date"].astype(str).str[:10]

    create_tables()
    session = SessionLocal()
    imported, updated, skipped = ingest_switrs_df_upsert(ingest_df, city=city_key, session=session)
    session.close()
    print(f"Ingested: {imported} new, {updated} updated, {skipped} skipped")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Prepare and ingest SWITRS data for collision-map")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--all", action="store_true",
                       help="Process all known city subdirectories under data/switrs/")
    group.add_argument("--city", choices=list(CITY_DIRS.keys()),
                       help="Process a single city directory")
    group.add_argument("--data-dir",
                       help="Path to a directory containing Crashes.csv + Parties.csv (legacy)")
    parser.add_argument("--ingest", action="store_true",
                        help="Load into collision_map.db (required for --all / --city)")
    args = parser.parse_args()

    if args.all or args.city:
        dirs = {args.city: CITY_DIRS[args.city]} if args.city else CITY_DIRS
        for dir_name, city_key in dirs.items():
            path = SWITRS_ROOT / dir_name
            if not (path / "Crashes.csv").exists():
                print(f"[skip] {path} — no Crashes.csv found")
                continue
            if args.ingest:
                ingest_city(path, city_key)
            else:
                df = load_merged(path)
                print_summary(df, label=dir_name.title())
    else:
        # Legacy single-directory mode
        data_dir = Path(args.data_dir) if args.data_dir else DATA_DIR
        df = load_merged(data_dir)
        print_summary(df, label=data_dir.name.title())
        if args.ingest:
            city_key = next(
                (v for k, v in CITY_DIRS.items() if k in str(data_dir).lower()),
                "unknown"
            )
            ingest_city(data_dir, city_key)


if __name__ == "__main__":
    main()
