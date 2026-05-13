from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Sequence

from .legacy_dump import TABLE_COPY_RE, parse_copy_line

DEFAULT_SITE_ROOT = "https://truepb.net"
DEFAULT_SYNTHETIC_ID_OFFSET = 9_000_000_000
DEFAULT_COLUMNS = ["Event", "Perf", "Pos", "Venue", "Meeting", "Date"]


@dataclass
class AthleteExport:
    athlete_id: int
    runner_id: int
    display_name: str
    runner_name: str | None
    club: str | None
    gender: str | None
    age: int | None
    age_group: str | None
    source_url: str
    results: list[dict[str, Any]] = field(default_factory=list)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export TruePB race results into an importable SQL file for the Po10 live DB.",
    )
    parser.add_argument(
        "--source-dsn",
        default=os.environ.get("SOURCE_DATABASE_URL"),
        help="PostgreSQL DSN for the source TruePB database. Defaults to SOURCE_DATABASE_URL.",
    )
    parser.add_argument(
        "--sql",
        default=None,
        help="Path to a PostgreSQL dump file such as 16-04-2026.sql. Use this instead of --source-dsn.",
    )
    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Calendar year to export, for example 2026.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Only export races on or after this YYYY-MM-DD date. Defaults to Jan 1 of --year.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path for the generated SQL file.",
    )
    parser.add_argument(
        "--site-root",
        default=DEFAULT_SITE_ROOT,
        help="Base URL used to build links back to the TruePB source site.",
    )
    parser.add_argument(
        "--synthetic-id-offset",
        type=int,
        default=DEFAULT_SYNTHETIC_ID_OFFSET,
        help="Offset used to generate stable athlete ids for runners without a Po10 athlete id.",
    )
    parser.add_argument(
        "--skip-dnf",
        action="store_true",
        help="Skip DNF rows instead of importing them as results with perf='DNF'.",
    )
    return parser.parse_args()


def source_dsn_from_args(args: argparse.Namespace) -> str:
    dsn = (args.source_dsn or "").strip()
    if not dsn:
        raise SystemExit("Missing source DSN. Pass --source-dsn or set SOURCE_DATABASE_URL.")
    return dsn


def sql_path_from_args(args: argparse.Namespace) -> Path:
    if not args.sql:
        raise SystemExit("Pass either --source-dsn or --sql.")
    return Path(args.sql)


def parse_start_date_arg(value: str | None, year: int) -> date:
    if not value:
        return date(year, 1, 1)
    try:
        start_date = date.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit("--start-date must use YYYY-MM-DD format.") from exc
    if start_date.year != year:
        raise SystemExit("--start-date must fall within the requested --year.")
    return start_date


def normalize_gender(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = value.strip().upper()
    if cleaned == "M":
        return "Male"
    if cleaned == "F":
        return "Female"
    return value.strip().title()


def infer_age_group(age: int | None, rows: Sequence[dict[str, Any]]) -> str | None:
    if any(bool(row.get("is_u13")) for row in rows):
        return "U13"
    if any(bool(row.get("is_u15")) for row in rows):
        return "U15"
    if any(bool(row.get("is_u17")) for row in rows):
        return "U17"
    if any(bool(row.get("is_u20")) for row in rows):
        return "U20"

    if age is None:
        return None
    if age < 13:
        return "U13"
    if age < 15:
        return "U15"
    if age < 17:
        return "U17"
    if age < 20:
        return "U20"
    if age < 23:
        return "U23"
    if age >= 35:
        return "V35"
    return "Senior"


def safe_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def choose_distance_km(row: dict[str, Any]) -> Decimal | None:
    actual = safe_decimal(row.get("actual_distance_km"))
    if actual and actual > 0:
        return actual
    claimed = safe_decimal(row.get("claimed_distance_km"))
    if claimed and claimed > 0:
        return claimed
    return None


def is_near(value: Decimal, target: str, tolerance: str) -> bool:
    return abs(value - Decimal(target)) <= Decimal(tolerance)


def format_decimal_compact(value: Decimal) -> str:
    normalized = value.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def format_track_distance_label(distance_km: Decimal) -> str:
    meters = distance_km * Decimal("1000")
    rounded_meters = int(meters.to_integral_value())
    if abs(meters - Decimal(rounded_meters)) <= Decimal("0.01"):
        return f"{rounded_meters}m"
    return f"{format_decimal_compact(distance_km)}K"


def base_event_label(distance_km: Decimal | None, row: dict[str, Any]) -> str | None:
    if distance_km is None:
        return None

    if is_near(distance_km, "42.195", "0.75"):
        return "Marathon"
    if is_near(distance_km, "21.0975", "0.35"):
        return "Half Marathon"
    if is_near(distance_km, "10", "0.2"):
        return "10K"
    if is_near(distance_km, "5", "0.15"):
        return "5K"
    if is_near(distance_km, "3", "0.05"):
        return "3000m" if row.get("is_track") else "3K"
    if is_near(distance_km, "1.5", "0.02"):
        return "1500m" if row.get("is_track") else "1.5K"
    if is_near(distance_km, "1.609344", "0.03") or is_near(distance_km, "1.6", "0.03"):
        return "Mile"
    if is_near(distance_km, "0.8", "0.02"):
        return "800m"
    if is_near(distance_km, "0.4", "0.02"):
        return "400m"

    if row.get("is_track"):
        return format_track_distance_label(distance_km)
    return f"{format_decimal_compact(distance_km)}K"


def build_event_label(row: dict[str, Any]) -> str:
    base = base_event_label(choose_distance_km(row), row)
    if not base:
        base = (row.get("race_group_item_name") or row.get("race_group_title") or row.get("race_name") or "Result").strip()

    if row.get("is_xc") and "XC" not in base.upper():
        base = f"{base} XC"

    if row.get("is_relay"):
        if row.get("leg_number"):
            base = f"{base} Relay Leg {row['leg_number']}"
        else:
            base = f"{base} Relay Leg"
    elif row.get("is_shortleg"):
        base = f"{base} Short Leg"
    elif row.get("is_longleg"):
        base = f"{base} Long Leg"

    return base


def format_race_time(seconds: Any) -> str | None:
    if seconds is None:
        return None
    total = round(float(seconds))
    if total <= 0:
        return None
    hrs = int(total // 3600)
    mins = int((total % 3600) // 60)
    secs = int(total % 60)
    if hrs > 0:
        return f"{hrs}:{mins:02d}:{secs:02d}"
    return f"{mins}:{secs:02d}"


def build_perf(row: dict[str, Any]) -> str | None:
    if row.get("did_not_finish"):
        return "DNF"
    finish = row.get("finish_time_seconds")
    if finish is not None:
        return format_race_time(finish)
    watch_time = row.get("watch_time")
    if watch_time is not None:
        return format_race_time(watch_time)
    return None


def build_position(row: dict[str, Any]) -> str | None:
    if row.get("did_not_finish"):
        return None
    xc_position = row.get("xc_finish_position")
    if xc_position:
        return str(xc_position)
    finish_position = row.get("finish_position")
    if finish_position:
        return str(finish_position)
    return None


def target_athlete_id(row: dict[str, Any], synthetic_id_offset: int) -> int:
    explicit = row.get("powerof10_athlete_id")
    if explicit is not None and int(explicit) > 0:
        return int(explicit)
    return synthetic_id_offset + int(row["runner_id"])


def build_source_url(row: dict[str, Any], site_root: str) -> str:
    powerof10_url = (row.get("powerof10_url") or "").strip()
    if powerof10_url:
        return powerof10_url
    return f"{site_root.rstrip('/')}/results"


def parse_pg_int(value: str | None) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def parse_pg_bool(value: str | None) -> bool:
    return value == "t"


def parse_pg_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def parse_pg_decimal(value: str | None) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(value)


def parse_copy_columns(line: str) -> tuple[str, list[str]] | None:
    match = TABLE_COPY_RE.match(line.rstrip("\n"))
    if not match:
        return None
    table_name = match.group(1)
    columns = [column.strip() for column in match.group(2).split(",")]
    return table_name, columns


def load_year_races_from_dump(
    sql_path: Path,
    year: int,
    start_date: date,
) -> dict[int, dict[str, Any]]:
    end_date = date(year + 1, 1, 1)
    races: dict[int, dict[str, Any]] = {}
    current_table: str | None = None
    current_columns: list[str] = []

    bool_fields = {
        "is_boxing_day",
        "is_deserters",
        "is_6stage",
        "is_12stage",
        "is_historical",
        "is_national_xc",
        "is_truepb_course",
        "is_truepb_eligible",
        "is_northern_xc",
        "is_relay",
        "is_short_race",
        "is_xc",
        "is_road",
        "is_track",
        "is_southern_xc",
        "is_midland_xc",
        "is_southern_12stage",
        "is_northern_12stage",
        "is_midland_12stage",
        "is_southern_6stage",
        "is_midland_6stage",
        "is_northern_6stage",
        "is_leeds_only",
        "is_u13",
        "is_u15",
        "is_u17",
        "is_u20",
        "is_southern_4stage",
        "is_northern_4stage",
        "is_midland_4stage",
        "is_southern_6stagew",
        "is_midland_6stagew",
        "is_northern_6stagew",
        "is_national_12stage",
        "is_national_6stage",
        "is_national_4stage",
        "is_national_6stagew",
        "is_xc_relay",
    }
    int_fields = {"id", "truepb_adjustment_seconds", "race_group_sort_order"}
    decimal_fields = {"claimed_distance_km", "actual_distance_km", "handicap_factor"}

    with sql_path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if current_table is not None:
                if line.startswith("\\."):
                    current_table = None
                    current_columns = []
                    continue
                if current_table != "races":
                    continue

                fields = parse_copy_line(line, len(current_columns))
                raw_row = dict(zip(current_columns, fields))
                race_date = parse_pg_date(raw_row.get("date"))
                if race_date is None or race_date < start_date or race_date >= end_date:
                    continue

                race: dict[str, Any] = {}
                for key, value in raw_row.items():
                    if key in bool_fields:
                        race[key] = parse_pg_bool(value)
                    elif key in int_fields:
                        race[key] = parse_pg_int(value)
                    elif key in decimal_fields:
                        race[key] = parse_pg_decimal(value)
                    elif key == "date":
                        race["race_date"] = race_date
                    elif key == "name":
                        race["race_name"] = value
                    elif key == "location":
                        race["race_location"] = value
                    else:
                        race[key] = value
                race["race_id"] = race.pop("id")
                races[race["race_id"]] = race
                continue

            copy_header = parse_copy_columns(line)
            if copy_header is None:
                continue
            current_table, current_columns = copy_header

    return races


def load_rows_from_dump(
    sql_path: Path,
    year: int,
    synthetic_id_offset: int,
    start_date: date,
) -> list[dict[str, Any]]:
    selected_races = load_year_races_from_dump(sql_path, year, start_date)
    if not selected_races:
        return []

    current_table: str | None = None
    current_columns: list[str] = []
    needed_runner_ids: set[int] = set()
    runners_by_id: dict[int, dict[str, Any]] = {}
    results_by_race: dict[int, list[dict[str, Any]]] = defaultdict(list)

    result_bool_fields = {
        "did_not_finish",
        "is_shortleg",
        "is_longleg",
        "is_b_team",
        "is_a_team",
        "is_soft_deleted",
        "is_u13",
        "is_u15",
        "is_u17",
        "is_u20",
    }
    result_int_fields = {
        "id",
        "runner_id",
        "race_id",
        "finish_time_seconds",
        "watch_time",
        "xc_finish_position",
        "leg_number",
    }
    result_decimal_fields = {"race_score"}
    runner_int_fields = {"id", "powerof10_athlete_id", "age"}

    with sql_path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if current_table is not None:
                if line.startswith("\\."):
                    current_table = None
                    current_columns = []
                    continue

                if current_table == "race_results":
                    fields = parse_copy_line(line, len(current_columns))
                    raw_row = dict(zip(current_columns, fields))
                    race_id = parse_pg_int(raw_row.get("race_id"))
                    if race_id is None or race_id not in selected_races:
                        continue
                    if parse_pg_bool(raw_row.get("is_soft_deleted")):
                        continue

                    row: dict[str, Any] = {}
                    for key, value in raw_row.items():
                        if key in result_bool_fields:
                            row[key] = parse_pg_bool(value)
                        elif key in result_int_fields:
                            row[key] = parse_pg_int(value)
                        elif key in result_decimal_fields:
                            row[key] = parse_pg_decimal(value)
                        else:
                            row[key] = value
                    row["result_id"] = row.pop("id")
                    needed_runner_ids.add(int(row["runner_id"]))
                    results_by_race[int(row["race_id"])].append(row)
                    continue

                if current_table == "runners":
                    fields = parse_copy_line(line, len(current_columns))
                    raw_row = dict(zip(current_columns, fields))
                    runner_id = parse_pg_int(raw_row.get("id"))
                    if runner_id is None or runner_id not in needed_runner_ids:
                        continue

                    runner: dict[str, Any] = {}
                    for key, value in raw_row.items():
                        if key in runner_int_fields:
                            runner[key] = parse_pg_int(value)
                        elif key == "name":
                            runner["runner_name"] = value
                        elif key == "club":
                            runner["runner_club"] = value
                        elif key == "gender":
                            runner["runner_gender"] = value
                        else:
                            runner[key] = value
                    runner["runner_id"] = runner.pop("id")
                    runners_by_id[runner["runner_id"]] = runner
                    continue

                continue

            copy_header = parse_copy_columns(line)
            if copy_header is None:
                continue
            table_name, columns = copy_header
            if table_name not in {"race_results", "runners"}:
                current_table = None
                current_columns = []
                continue
            current_table = table_name
            current_columns = columns

    rows: list[dict[str, Any]] = []
    for race_id, race_results in results_by_race.items():
        race_results.sort(
            key=lambda row: (
                bool(row.get("did_not_finish")),
                row.get("xc_finish_position") if row.get("xc_finish_position") is not None else 10**9,
                row.get("finish_time_seconds") if row.get("finish_time_seconds") is not None else 10**9,
                row.get("result_id") if row.get("result_id") is not None else 10**9,
            )
        )
        for index, row in enumerate(race_results, start=1):
            row["finish_position"] = index
            merged = dict(row)
            merged.update(selected_races[race_id])
            runner = runners_by_id.get(int(row["runner_id"]), {})
            merged.update(runner)
            merged.setdefault("runner_id", row["runner_id"])
            merged.setdefault("runner_name", f"Runner {row['runner_id']}")
            rows.append(merged)

    rows.sort(
        key=lambda row: (
            target_athlete_id(row, synthetic_id_offset),
            -(row["race_date"].toordinal() if row.get("race_date") else 0),
            row.get("race_name") or "",
            -(row.get("result_id") or 0),
        )
    )
    return rows


def load_rows(
    source_dsn: str,
    year: int,
    synthetic_id_offset: int,
    start_date: date,
) -> list[dict[str, Any]]:
    import psycopg
    from psycopg.rows import dict_row

    end_date = date(year + 1, 1, 1)
    query = """
        SELECT
            rr.id AS result_id,
            rr.runner_id,
            rr.race_id,
            rr.finish_time_seconds,
            rr.race_score,
            rr.did_not_finish,
            rr.watch_time,
            rr.notes,
            rr.is_shortleg,
            rr.is_longleg,
            rr.is_b_team,
            rr.is_a_team,
            rr.is_u13,
            rr.is_u15,
            rr.is_u17,
            rr.is_u20,
            rr.leg_number,
            rr.xc_finish_position,
            rr.created_at AS result_created_at,
            rr.updated_at AS result_updated_at,
            ra.name AS race_name,
            ra.location AS race_location,
            ra.date AS race_date,
            ra.claimed_distance_km,
            ra.actual_distance_km,
            ra.handicap_factor,
            ra.is_truepb_course,
            ra.truepb_adjustment_seconds,
            ra.is_truepb_eligible,
            ra.is_boxing_day,
            ra.is_deserters,
            ra.is_6stage,
            ra.is_12stage,
            ra.is_historical,
            ra.is_national_xc,
            ra.is_northern_xc,
            ra.is_southern_xc,
            ra.is_midland_xc,
            ra.is_relay,
            ra.is_short_race,
            ra.is_xc,
            ra.is_road,
            ra.is_track,
            ra.is_southern_12stage,
            ra.is_northern_12stage,
            ra.is_midland_12stage,
            ra.is_southern_6stage,
            ra.is_midland_6stage,
            ra.is_northern_6stage,
            ra.is_southern_4stage,
            ra.is_northern_4stage,
            ra.is_midland_4stage,
            ra.is_southern_6stagew,
            ra.is_midland_6stagew,
            ra.is_northern_6stagew,
            ra.is_national_12stage,
            ra.is_national_6stage,
            ra.is_national_4stage,
            ra.is_national_6stagew,
            ra.race_group_key,
            ra.race_group_title,
            ra.race_group_item_name,
            ra.race_group_sort_order,
            r.name AS runner_name,
            r.club AS runner_club,
            r.gender AS runner_gender,
            r.powerof10_url,
            r.powerof10_athlete_id,
            r.age AS runner_age,
            ROW_NUMBER() OVER (
                PARTITION BY rr.race_id
                ORDER BY
                    rr.did_not_finish ASC,
                    rr.xc_finish_position NULLS LAST,
                    rr.finish_time_seconds NULLS LAST,
                    rr.id ASC
            ) AS finish_position
        FROM race_results rr
        JOIN races ra ON ra.id = rr.race_id
        JOIN runners r ON r.id = rr.runner_id
        WHERE ra.date >= %(start_date)s
          AND ra.date < %(end_date)s
          AND COALESCE(rr.is_soft_deleted, FALSE) = FALSE
        ORDER BY
            COALESCE(r.powerof10_athlete_id, %(synthetic_offset)s + rr.runner_id),
            ra.date DESC,
            ra.name ASC,
            rr.id DESC
    """

    with psycopg.connect(source_dsn, row_factory=dict_row) as conn:
        return conn.execute(
            query,
            {
                "start_date": start_date,
                "end_date": end_date,
                "synthetic_offset": synthetic_id_offset,
            },
        ).fetchall()


def collect_athletes(
    rows: Sequence[dict[str, Any]],
    *,
    year: int,
    site_root: str,
    synthetic_id_offset: int,
    skip_dnf: bool,
) -> list[AthleteExport]:
    grouped_rows: dict[int, list[dict[str, Any]]] = defaultdict(list)
    athlete_meta: dict[int, dict[str, Any]] = {}

    for row in rows:
        athlete_id = target_athlete_id(row, synthetic_id_offset)
        if skip_dnf and row.get("did_not_finish"):
            continue
        grouped_rows[athlete_id].append(row)

        existing = athlete_meta.get(athlete_id)
        if existing is None:
            athlete_meta[athlete_id] = row
            continue

        # Prefer the row with a Po10 source URL or more complete metadata.
        if not existing.get("powerof10_url") and row.get("powerof10_url"):
            athlete_meta[athlete_id] = row
            continue
        if not existing.get("runner_club") and row.get("runner_club"):
            athlete_meta[athlete_id] = row
            continue
        if existing.get("runner_age") is None and row.get("runner_age") is not None:
            athlete_meta[athlete_id] = row

    athletes: list[AthleteExport] = []
    for athlete_id in sorted(grouped_rows):
        athlete_rows = grouped_rows[athlete_id]
        meta = athlete_meta[athlete_id]
        age = int(meta["runner_age"]) if meta.get("runner_age") is not None else None
        athlete = AthleteExport(
            athlete_id=athlete_id,
            runner_id=int(meta["runner_id"]),
            display_name=(meta.get("runner_name") or f"Runner {meta['runner_id']}").strip(),
            runner_name=(meta.get("runner_name") or None),
            club=(meta.get("runner_club") or None),
            gender=normalize_gender(meta.get("runner_gender")),
            age=age,
            age_group=infer_age_group(age, athlete_rows),
            source_url=build_source_url(meta, site_root),
        )

        for row_order, row in enumerate(athlete_rows):
            race_date = row.get("race_date")
            date_text = race_date.isoformat() if race_date else None
            perf = build_perf(row)
            result = {
                "row_order": row_order,
                "event": build_event_label(row),
                "perf": perf,
                "pos": build_position(row),
                "venue": row.get("race_location"),
                "venue_url": f"{site_root.rstrip('/')}/results",
                "meeting": row.get("race_name"),
                "date_text": date_text,
                "result_date": race_date,
                "extra": {
                    "source": "truepb_results_db",
                    "year": year,
                    "runner_id": int(row["runner_id"]),
                    "race_id": int(row["race_id"]),
                    "result_id": int(row["result_id"]),
                    "powerof10_athlete_id": int(row["powerof10_athlete_id"]) if row.get("powerof10_athlete_id") is not None else None,
                    "used_synthetic_athlete_id": not (
                        row.get("powerof10_athlete_id") is not None
                        and int(row["powerof10_athlete_id"]) > 0
                    ),
                    "finish_time_seconds": row.get("finish_time_seconds"),
                    "watch_time": row.get("watch_time"),
                    "race_score": float(row["race_score"]) if row.get("race_score") is not None else None,
                    "did_not_finish": bool(row.get("did_not_finish")),
                    "notes": row.get("notes"),
                    "is_shortleg": bool(row.get("is_shortleg")),
                    "is_longleg": bool(row.get("is_longleg")),
                    "is_a_team": bool(row.get("is_a_team")),
                    "is_b_team": bool(row.get("is_b_team")),
                    "leg_number": row.get("leg_number"),
                    "xc_finish_position": row.get("xc_finish_position"),
                    "claimed_distance_km": float(row["claimed_distance_km"]) if row.get("claimed_distance_km") is not None else None,
                    "actual_distance_km": float(row["actual_distance_km"]) if row.get("actual_distance_km") is not None else None,
                    "handicap_factor": float(row["handicap_factor"]) if row.get("handicap_factor") is not None else None,
                    "is_truepb_course": bool(row.get("is_truepb_course")),
                    "truepb_adjustment_seconds": row.get("truepb_adjustment_seconds"),
                    "is_truepb_eligible": bool(row.get("is_truepb_eligible")),
                    "is_boxing_day": bool(row.get("is_boxing_day")),
                    "is_deserters": bool(row.get("is_deserters")),
                    "is_historical": bool(row.get("is_historical")),
                    "is_6stage": bool(row.get("is_6stage")),
                    "is_12stage": bool(row.get("is_12stage")),
                    "is_national_xc": bool(row.get("is_national_xc")),
                    "is_northern_xc": bool(row.get("is_northern_xc")),
                    "is_southern_xc": bool(row.get("is_southern_xc")),
                    "is_midland_xc": bool(row.get("is_midland_xc")),
                    "is_relay": bool(row.get("is_relay")),
                    "is_short_race": bool(row.get("is_short_race")),
                    "is_xc": bool(row.get("is_xc")),
                    "is_road": bool(row.get("is_road")),
                    "is_track": bool(row.get("is_track")),
                    "race_group_key": row.get("race_group_key"),
                    "race_group_title": row.get("race_group_title"),
                    "race_group_item_name": row.get("race_group_item_name"),
                    "race_group_sort_order": row.get("race_group_sort_order"),
                },
            }
            athlete.results.append(result)

        athletes.append(athlete)

    return athletes


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (date, datetime)):
        return f"'{value.isoformat()}'"
    if isinstance(value, (dict, list)):
        return sql_literal(json.dumps(value, separators=(",", ":"), sort_keys=True))
    text = str(value).replace("'", "''")
    return f"'{text}'"


def write_insert_statement(
    fh,
    table_name: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    *,
    batch_size: int = 500,
) -> None:
    if not rows:
        return

    header = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES\n"
    for batch_start in range(0, len(rows), batch_size):
        batch = rows[batch_start : batch_start + batch_size]
        fh.write(header)
        value_lines = []
        for row in batch:
            literals = ", ".join(sql_literal(value) for value in row)
            value_lines.append(f"  ({literals})")
        fh.write(",\n".join(value_lines))
        fh.write(";\n\n")


def generate_sql(
    athletes: Sequence[AthleteExport],
    *,
    year: int,
    start_date: date,
    site_root: str,
) -> str:
    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    title = f"{year} TRUEPB Results"
    athlete_rows: list[list[Any]] = []
    result_rows: list[list[Any]] = []

    for athlete in athletes:
        athlete_rows.append(
            [
                athlete.athlete_id,
                athlete.runner_id,
                athlete.display_name,
                athlete.runner_name,
                athlete.club,
                athlete.gender,
                athlete.age,
                athlete.age_group,
                athlete.source_url,
            ]
        )
        for result in athlete.results:
            result_rows.append(
                [
                    athlete.athlete_id,
                    result["row_order"],
                    result["event"],
                    result["perf"],
                    result["pos"],
                    result["venue"],
                    result["venue_url"],
                    result["meeting"],
                    result["date_text"],
                    result["result_date"],
                    result["extra"],
                ]
            )

    lines: list[str] = []
    lines.append("-- Generated by app.export_truepb_results_sql")
    lines.append(f"-- Generated at: {generated_at}")
    lines.append(f"-- Source year: {year}")
    lines.append(f"-- Start date: {start_date.isoformat()}")
    lines.append(f"-- Site root: {site_root}")
    lines.append("")
    lines.append("BEGIN;")
    lines.append("")
    lines.append("CREATE TEMP TABLE tmp_truepb_athletes (")
    lines.append("  athlete_id BIGINT PRIMARY KEY,")
    lines.append("  runner_id INTEGER NOT NULL,")
    lines.append("  display_name TEXT NOT NULL,")
    lines.append("  runner_name TEXT,")
    lines.append("  club TEXT,")
    lines.append("  gender TEXT,")
    lines.append("  age INTEGER,")
    lines.append("  age_group TEXT,")
    lines.append("  source_url TEXT NOT NULL")
    lines.append(") ON COMMIT DROP;")
    lines.append("")
    lines.append("CREATE TEMP TABLE tmp_truepb_results (")
    lines.append("  athlete_id BIGINT NOT NULL,")
    lines.append("  row_order INTEGER NOT NULL,")
    lines.append("  event TEXT,")
    lines.append("  perf TEXT,")
    lines.append("  pos TEXT,")
    lines.append("  venue TEXT,")
    lines.append("  venue_url TEXT,")
    lines.append("  meeting TEXT,")
    lines.append("  date_text TEXT,")
    lines.append("  result_date DATE,")
    lines.append("  extra JSONB NOT NULL")
    lines.append(") ON COMMIT DROP;")
    lines.append("")

    sql_prefix = "\n".join(lines) + "\n"
    output = [sql_prefix]

    from io import StringIO

    buffer = StringIO()
    write_insert_statement(
        buffer,
        "tmp_truepb_athletes",
        [
            "athlete_id",
            "runner_id",
            "display_name",
            "runner_name",
            "club",
            "gender",
            "age",
            "age_group",
            "source_url",
        ],
        athlete_rows,
    )
    write_insert_statement(
        buffer,
        "tmp_truepb_results",
        [
            "athlete_id",
            "row_order",
            "event",
            "perf",
            "pos",
            "venue",
            "venue_url",
            "meeting",
            "date_text",
            "result_date",
            "extra",
        ],
        result_rows,
    )
    output.append(buffer.getvalue())

    output.append(
        f"""\
INSERT INTO athletes (
  athlete_id,
  display_name,
  profile_name,
  runner_name,
  club,
  gender,
  age,
  age_group,
  source_url,
  fetched_at,
  best_headers,
  performance_count,
  section_count,
  first_year,
  last_year
)
SELECT
  athlete_id,
  display_name,
  NULL,
  runner_name,
  club,
  gender,
  age,
  age_group,
  source_url,
  NULL,
  '[]'::jsonb,
  0,
  0,
  NULL,
  NULL
FROM tmp_truepb_athletes
ON CONFLICT (athlete_id) DO UPDATE
SET
  display_name = COALESCE(NULLIF(EXCLUDED.display_name, ''), athletes.display_name),
  runner_name = COALESCE(NULLIF(EXCLUDED.runner_name, ''), athletes.runner_name),
  club = COALESCE(NULLIF(EXCLUDED.club, ''), athletes.club),
  gender = COALESCE(NULLIF(EXCLUDED.gender, ''), athletes.gender),
  age = COALESCE(EXCLUDED.age, athletes.age),
  age_group = COALESCE(NULLIF(EXCLUDED.age_group, ''), athletes.age_group),
  source_url = CASE
    WHEN athletes.source_url IS NULL OR athletes.source_url = '' OR athletes.source_url = '#'
    THEN EXCLUDED.source_url
    ELSE athletes.source_url
  END,
  updated_at = NOW();

DELETE FROM athlete_performance_sections s
USING tmp_truepb_athletes a
WHERE s.athlete_id = a.athlete_id
  AND s.source_kind = 'truepb_results'
  AND s.year = {year}
  AND NOT EXISTS (
    SELECT 1
    FROM athlete_performances p
    WHERE p.section_id = s.id
      AND p.result_date < {sql_literal(start_date)}
  );

DELETE FROM athlete_performances p
USING athlete_performance_sections s, tmp_truepb_athletes a
WHERE p.section_id = s.id
  AND s.athlete_id = a.athlete_id
  AND s.source_kind = 'truepb_results'
  AND s.year = {year}
  AND p.result_date >= {sql_literal(start_date)};

INSERT INTO athlete_performance_sections (
  athlete_id,
  source_kind,
  section_order,
  title,
  year,
  columns_json,
  metadata
)
SELECT
  a.athlete_id,
  'truepb_results',
  COALESCE((
    SELECT MAX(existing.section_order) + 1
    FROM athlete_performance_sections existing
    WHERE existing.athlete_id = a.athlete_id
  ), 0),
  {sql_literal(title)},
  {year},
  {sql_literal(DEFAULT_COLUMNS)}::jsonb,
  jsonb_build_object(
    'source', 'truepb_results_sql',
    'year', {year},
    'start_date', {sql_literal(start_date.isoformat())},
    'runner_id', a.runner_id,
    'generated_at', {sql_literal(generated_at)}
  )
FROM tmp_truepb_athletes a
WHERE NOT EXISTS (
  SELECT 1
  FROM athlete_performance_sections existing
  WHERE existing.athlete_id = a.athlete_id
    AND existing.source_kind = 'truepb_results'
    AND existing.year = {year}
);

WITH incoming_counts AS (
  SELECT
    athlete_id,
    COUNT(*)::integer AS incoming_count
  FROM tmp_truepb_results
  GROUP BY athlete_id
),
preserved_rows AS (
  SELECT
    p.id,
    incoming_counts.incoming_count + ROW_NUMBER() OVER (
      PARTITION BY p.athlete_id
      ORDER BY p.result_date DESC NULLS LAST, p.row_order ASC, p.id ASC
    ) - 1 AS next_row_order
  FROM athlete_performances p
  JOIN athlete_performance_sections s ON s.id = p.section_id
  JOIN incoming_counts ON incoming_counts.athlete_id = p.athlete_id
  WHERE s.source_kind = 'truepb_results'
    AND s.year = {year}
    AND p.result_date < {sql_literal(start_date)}
)
UPDATE athlete_performances p
SET row_order = preserved_rows.next_row_order
FROM preserved_rows
WHERE p.id = preserved_rows.id;

UPDATE athlete_performance_sections s
SET
  title = {sql_literal(title)},
  columns_json = {sql_literal(DEFAULT_COLUMNS)}::jsonb,
  metadata = s.metadata || jsonb_build_object(
    'source', 'truepb_results_sql',
    'year', {year},
    'start_date', {sql_literal(start_date.isoformat())},
    'generated_at', {sql_literal(generated_at)}
  )
FROM tmp_truepb_athletes a
WHERE s.athlete_id = a.athlete_id
  AND s.source_kind = 'truepb_results'
  AND s.year = {year};

WITH target_sections AS (
  SELECT DISTINCT ON (s.athlete_id)
    s.athlete_id,
    s.id
  FROM athlete_performance_sections s
  JOIN tmp_truepb_athletes a ON a.athlete_id = s.athlete_id
  WHERE s.source_kind = 'truepb_results'
    AND s.year = {year}
  ORDER BY s.athlete_id, s.section_order ASC, s.id ASC
)
INSERT INTO athlete_performances (
  athlete_id,
  section_id,
  source_kind,
  row_order,
  event,
  perf,
  pos,
  venue,
  venue_url,
  meeting,
  date_text,
  result_date,
  extra
)
SELECT
  r.athlete_id,
  s.id,
  'truepb_results',
  r.row_order,
  r.event,
  r.perf,
  r.pos,
  r.venue,
  r.venue_url,
  r.meeting,
  r.date_text,
  r.result_date,
  r.extra
FROM tmp_truepb_results r
JOIN target_sections s ON s.athlete_id = r.athlete_id;

WITH agg AS (
  SELECT
    s.athlete_id,
    COUNT(p.id) AS performance_count,
    COUNT(DISTINCT s.id) AS section_count,
    MIN(s.year) AS first_year,
    MAX(s.year) AS last_year
  FROM athlete_performance_sections s
  LEFT JOIN athlete_performances p ON p.section_id = s.id
  WHERE s.athlete_id IN (SELECT athlete_id FROM tmp_truepb_athletes)
  GROUP BY s.athlete_id
)
UPDATE athletes a
SET
  performance_count = COALESCE(agg.performance_count, 0),
  section_count = COALESCE(agg.section_count, 0),
  first_year = agg.first_year,
  last_year = agg.last_year,
  updated_at = NOW()
FROM agg
WHERE a.athlete_id = agg.athlete_id;

COMMIT;
"""
    )
    return "".join(output)


def main() -> None:
    args = parse_args()
    start_date = parse_start_date_arg(args.start_date, args.year)
    if args.sql:
        rows = load_rows_from_dump(
            sql_path_from_args(args),
            args.year,
            args.synthetic_id_offset,
            start_date,
        )
    else:
        rows = load_rows(
            source_dsn_from_args(args),
            args.year,
            args.synthetic_id_offset,
            start_date,
        )
    athletes = collect_athletes(
        rows,
        year=args.year,
        site_root=args.site_root,
        synthetic_id_offset=args.synthetic_id_offset,
        skip_dnf=args.skip_dnf,
    )

    sql_text = generate_sql(
        athletes,
        year=args.year,
        start_date=start_date,
        site_root=args.site_root,
    )
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(sql_text, encoding="utf-8")

    result_count = sum(len(athlete.results) for athlete in athletes)
    synthetic_count = sum(1 for athlete in athletes if athlete.athlete_id >= args.synthetic_id_offset)
    print(f"Wrote {output_path}")
    print(f"Athletes: {len(athletes)}")
    print(f"Results: {result_count}")
    print(f"Start date: {start_date.isoformat()}")
    print(f"Synthetic athlete ids: {synthetic_count}")


if __name__ == "__main__":
    main()
