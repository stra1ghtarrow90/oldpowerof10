from __future__ import annotations

import argparse
import csv
import os
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .legacy_dump import extract_athlete_id_from_url, parse_result_date
from .wayback_html import normalize_identity


DEFAULT_SOURCE_DSN = os.environ.get("SOURCE_DATABASE_URL")
DEFAULT_TARGET_DSN = os.environ.get("TARGET_DATABASE_URL")
DEFAULT_REPORT = "imports/generated/truepb_profile_sync_report.csv"
DEFAULT_SOURCE_URL_PREFIX = "https://www.thepowerof10.info/athletes/profile.aspx?athleteid="
KNOWN_RESULT_KEYS = {"event", "perf", "pos", "venue", "venue_url", "meeting", "date"}


@dataclass
class SourcePerformanceRow:
    event: str | None
    perf: str | None
    pos: str | None
    venue: str | None
    venue_url: str | None
    meeting: str | None
    date_text: str | None
    result_date: date | None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class SourceSection:
    title: str
    year: int | None
    columns: list[str]
    rows: list[SourcePerformanceRow] = field(default_factory=list)


@dataclass
class SourceAthlete:
    athlete_id: int
    display_name: str
    profile_name: str | None
    runner_name: str | None
    club: str | None
    gender: str | None
    age: int | None
    age_group: str | None
    source_url: str
    fetched_at: datetime | None
    best_headers: list[str]
    best_rows: list[list[str]] = field(default_factory=list)
    sections: list[SourceSection] = field(default_factory=list)

    @property
    def preferred_name(self) -> str:
        for value in (self.runner_name, self.display_name, self.profile_name):
            text = (value or "").strip()
            if text:
                return text
        return f"Athlete {self.athlete_id}"

    @property
    def cache_payload(self) -> dict[str, Any]:
        sections: list[dict[str, Any]] = []
        for section in self.sections:
            section_rows: list[dict[str, Any]] = []
            for row in section.rows:
                payload = dict(row.extra or {})
                payload.update(
                    {
                        "event": row.event,
                        "perf": row.perf,
                        "pos": row.pos,
                        "venue": row.venue,
                        "venue_url": row.venue_url,
                        "meeting": row.meeting,
                        "date": row.date_text or (row.result_date.isoformat() if row.result_date else None),
                    }
                )
                section_rows.append(payload)
            sections.append(
                {
                    "title": section.title,
                    "columns": section.columns,
                    "rows": section_rows,
                }
            )
        return {
            "best_performances": {
                "headers": self.best_headers,
                "rows": self.best_rows,
            },
            "performances": sections,
        }


@dataclass
class TargetRunner:
    runner_id: int
    name: str | None
    club: str | None
    gender: str | None
    powerof10_url: str | None
    powerof10_athlete_id: int | None
    age: int | None

    @property
    def available_for_match(self) -> bool:
        return self.powerof10_athlete_id is None


@dataclass
class MatchDecision:
    action: str
    reason: str
    runner_id: int | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sync Po10-backed athletes from truepb_live into the original TruePB DB, "
            "skipping existing target Po10 profiles and merging onto matching runners."
        )
    )
    parser.add_argument(
        "--source-dsn",
        default=DEFAULT_SOURCE_DSN,
        help="PostgreSQL DSN for the source truepb_live database. Defaults to SOURCE_DATABASE_URL.",
    )
    parser.add_argument(
        "--target-dsn",
        default=DEFAULT_TARGET_DSN,
        help="PostgreSQL DSN for the target truepb database. Defaults to TARGET_DATABASE_URL.",
    )
    parser.add_argument(
        "--report",
        default=DEFAULT_REPORT,
        help=f"CSV report output path. Defaults to {DEFAULT_REPORT}.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional source athlete limit for testing.",
    )
    parser.add_argument(
        "--athlete-id",
        type=int,
        default=None,
        help="Optional single athlete id to sync.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve matches and build the report without writing to the target DB.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-athlete progress as the sync runs.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="When not using --verbose, print a progress line every N athletes. Use 0 to disable.",
    )
    parser.add_argument(
        "--skip-insert-runners",
        action="store_true",
        help="Do not create new runners in the target DB when no safe merge target exists.",
    )
    return parser.parse_args()


def require_dsn(value: str | None, flag: str) -> str:
    text = (value or "").strip()
    if not text:
        raise SystemExit(f"Missing {flag}. Pass {flag} or set the corresponding environment variable.")
    return text


def normalize_gender_code(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = value.strip().lower()
    if cleaned in {"m", "male"}:
        return "M"
    if cleaned in {"f", "female"}:
        return "F"
    return value.strip()


def normalize_event_key(label: str | None) -> str | None:
    if not label:
        return None
    text = "".join(ch if ch.isalnum() else "_" for ch in label.strip().lower())
    while "__" in text:
        text = text.replace("__", "_")
    text = text.strip("_")
    return text or None


def parse_perf_seconds(value: str | None) -> int | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw or raw == "-" or "dnf" in raw.lower():
        return None
    cleaned = "".join(ch for ch in raw if ch.isdigit() or ch in {":", "."})
    if not cleaned:
        return None
    parts = cleaned.split(":")
    try:
        if len(parts) == 1:
            return round(float(parts[0]))
        if len(parts) == 2:
            return round(int(parts[0]) * 60 + float(parts[1]))
        if len(parts) == 3:
            return round(int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2]))
    except ValueError:
        return None
    return None


def choose_result_year(row: SourcePerformanceRow, section: SourceSection) -> int | None:
    if row.result_date is not None:
        return row.result_date.year
    parsed = parse_result_date(row.date_text)
    if parsed is not None:
        return parsed.year
    return section.year


def load_source_athletes(source_dsn: str, *, athlete_id: int | None, limit: int | None) -> list[SourceAthlete]:
    athlete_where: list[str] = [
        """
        (
          COALESCE(NULLIF(TRIM(a.profile_name), ''), NULL) IS NOT NULL
          OR EXISTS (
            SELECT 1
            FROM athlete_best_performance_rows b
            WHERE b.athlete_id = a.athlete_id
          )
          OR EXISTS (
            SELECT 1
            FROM athlete_performance_sections s
            WHERE s.athlete_id = a.athlete_id
              AND s.source_kind <> 'truepb_results'
          )
        )
        """
    ]
    params: dict[str, Any] = {}
    if athlete_id is not None:
        athlete_where.append("a.athlete_id = %(athlete_id)s")
        params["athlete_id"] = athlete_id

    query = f"""
        SELECT
            a.athlete_id,
            a.display_name,
            a.profile_name,
            a.runner_name,
            a.club,
            a.gender,
            a.age,
            a.age_group,
            a.source_url,
            a.fetched_at,
            a.best_headers
        FROM athletes a
        WHERE {' AND '.join(athlete_where)}
        ORDER BY a.athlete_id
    """
    if limit is not None:
        query += " LIMIT %(limit)s"
        params["limit"] = limit

    with psycopg.connect(source_dsn, row_factory=dict_row) as conn:
        base_rows = conn.execute(query, params).fetchall()
        athlete_ids = [int(row["athlete_id"]) for row in base_rows]
        if not athlete_ids:
            return []

        athletes = {
            int(row["athlete_id"]): SourceAthlete(
                athlete_id=int(row["athlete_id"]),
                display_name=(row["display_name"] or row["profile_name"] or f"Athlete {row['athlete_id']}").strip(),
                profile_name=row["profile_name"],
                runner_name=row["runner_name"],
                club=row["club"],
                gender=row["gender"],
                age=int(row["age"]) if row["age"] is not None else None,
                age_group=row["age_group"],
                source_url=(row["source_url"] or f"{DEFAULT_SOURCE_URL_PREFIX}{row['athlete_id']}").strip(),
                fetched_at=row["fetched_at"],
                best_headers=list(row["best_headers"] or []),
            )
            for row in base_rows
        }

        best_rows = conn.execute(
            """
            SELECT athlete_id, row_order, cells
            FROM athlete_best_performance_rows
            WHERE athlete_id = ANY(%(athlete_ids)s)
            ORDER BY athlete_id, row_order
            """,
            {"athlete_ids": athlete_ids},
        ).fetchall()
        for row in best_rows:
            athlete = athletes[int(row["athlete_id"])]
            athlete.best_rows.append(
                ["" if cell is None else str(cell) for cell in (row["cells"] or [])]
            )

        section_rows = conn.execute(
            """
            SELECT
                s.id AS section_id,
                s.athlete_id,
                s.section_order,
                s.title,
                s.year,
                s.columns_json,
                p.row_order,
                p.event,
                p.perf,
                p.pos,
                p.venue,
                p.venue_url,
                p.meeting,
                p.date_text,
                p.result_date,
                p.extra
            FROM athlete_performance_sections s
            LEFT JOIN athlete_performances p ON p.section_id = s.id
            WHERE s.athlete_id = ANY(%(athlete_ids)s)
              AND s.source_kind <> 'truepb_results'
            ORDER BY s.athlete_id, s.section_order, p.row_order
            """,
            {"athlete_ids": athlete_ids},
        ).fetchall()

        section_map: dict[tuple[int, int], SourceSection] = {}
        section_lists: dict[int, list[SourceSection]] = defaultdict(list)
        for row in section_rows:
            athlete_key = int(row["athlete_id"])
            section_key = (athlete_key, int(row["section_id"]))
            section = section_map.get(section_key)
            if section is None:
                section = SourceSection(
                    title=(row["title"] or "").strip() or f"Section {row['section_order']}",
                    year=int(row["year"]) if row["year"] is not None else None,
                    columns=list(row["columns_json"] or []),
                )
                section_map[section_key] = section
                section_lists[athlete_key].append(section)

            if row["row_order"] is None:
                continue

            section.rows.append(
                SourcePerformanceRow(
                    event=row["event"],
                    perf=row["perf"],
                    pos=row["pos"],
                    venue=row["venue"],
                    venue_url=row["venue_url"],
                    meeting=row["meeting"],
                    date_text=row["date_text"],
                    result_date=row["result_date"],
                    extra=dict(row["extra"] or {}),
                )
            )

        for athlete_key, sections in section_lists.items():
            athletes[athlete_key].sections = sections

    return [athletes[key] for key in sorted(athletes)]


def load_target_state(target_dsn: str) -> tuple[set[int], set[int], list[TargetRunner]]:
    with psycopg.connect(target_dsn, row_factory=dict_row) as conn:
        existing_profiles = {
            int(row["athlete_id"])
            for row in conn.execute("SELECT athlete_id FROM powerof10_profiles").fetchall()
        }
        existing_cache = {
            int(row["athlete_id"])
            for row in conn.execute("SELECT athlete_id FROM powerof10_cache").fetchall()
        }
        runners = [
            TargetRunner(
                runner_id=int(row["id"]),
                name=row["name"],
                club=row["club"],
                gender=row["gender"],
                powerof10_url=row["powerof10_url"],
                powerof10_athlete_id=int(row["powerof10_athlete_id"]) if row["powerof10_athlete_id"] is not None else None,
                age=int(row["age"]) if row["age"] is not None else None,
            )
            for row in conn.execute(
                """
                SELECT id, name, club, gender, powerof10_url, powerof10_athlete_id, age
                FROM runners
                """
            ).fetchall()
        ]
    return existing_profiles, existing_cache, runners


def build_runner_indexes(runners: list[TargetRunner]) -> dict[str, Any]:
    by_athlete_id: dict[int, list[TargetRunner]] = defaultdict(list)
    by_name_club: dict[tuple[str, str], list[TargetRunner]] = defaultdict(list)
    by_name: dict[str, list[TargetRunner]] = defaultdict(list)

    for runner in runners:
        if runner.powerof10_athlete_id is not None:
            by_athlete_id[runner.powerof10_athlete_id].append(runner)
        athlete_from_url = extract_athlete_id_from_url(runner.powerof10_url)
        if athlete_from_url is not None:
            by_athlete_id[athlete_from_url].append(runner)

        name_key = normalize_identity(runner.name)
        club_key = normalize_identity(runner.club)
        if name_key:
            by_name[name_key].append(runner)
            if club_key:
                by_name_club[(name_key, club_key)].append(runner)

    return {
        "by_athlete_id": by_athlete_id,
        "by_name_club": by_name_club,
        "by_name": by_name,
    }


def candidate_names(athlete: SourceAthlete) -> list[str]:
    seen: set[str] = set()
    values: list[str] = []
    for raw in (athlete.runner_name, athlete.display_name, athlete.profile_name):
        key = normalize_identity(raw)
        if key and key not in seen:
            seen.add(key)
            values.append(key)
    return values


def dedupe_runners(runners: list[TargetRunner]) -> list[TargetRunner]:
    seen: set[int] = set()
    result: list[TargetRunner] = []
    for runner in runners:
        if runner.runner_id in seen:
            continue
        seen.add(runner.runner_id)
        result.append(runner)
    return result


def choose_runner(
    athlete: SourceAthlete,
    indexes: dict[str, Any],
) -> MatchDecision:
    athlete_id_matches = dedupe_runners(indexes["by_athlete_id"].get(athlete.athlete_id, []))
    if athlete_id_matches:
        if len(athlete_id_matches) == 1:
            return MatchDecision("match_existing_runner", "matched by athlete id", athlete_id_matches[0].runner_id)

        name_keys = set(candidate_names(athlete))
        club_key = normalize_identity(athlete.club)
        narrowed = [
            runner
            for runner in athlete_id_matches
            if normalize_identity(runner.name) in name_keys
            and (not club_key or normalize_identity(runner.club) == club_key)
        ]
        narrowed = dedupe_runners(narrowed)
        if len(narrowed) == 1:
            return MatchDecision("match_existing_runner", "matched by athlete id and name", narrowed[0].runner_id)
        return MatchDecision("skip_ambiguous", "multiple target runners already linked to athlete id")

    club_key = normalize_identity(athlete.club)
    name_keys = candidate_names(athlete)

    for name_key in name_keys:
        if not club_key:
            continue
        matches = [
            runner
            for runner in indexes["by_name_club"].get((name_key, club_key), [])
            if runner.available_for_match
        ]
        matches = dedupe_runners(matches)
        if len(matches) == 1:
            return MatchDecision("match_existing_runner", "matched by exact name and club", matches[0].runner_id)
        if len(matches) > 1:
            return MatchDecision("skip_ambiguous", "multiple exact name+club matches in target runners")

    for name_key in name_keys:
        matches = [
            runner
            for runner in indexes["by_name"].get(name_key, [])
            if runner.available_for_match
        ]
        matches = dedupe_runners(matches)
        if len(matches) == 1:
            return MatchDecision("match_existing_runner", "matched by exact name", matches[0].runner_id)
        if len(matches) > 1:
            return MatchDecision("skip_ambiguous", "multiple exact name matches in target runners")

    return MatchDecision("insert_runner", "no safe existing runner match")


def build_event_pbs(athlete: SourceAthlete, runner_id: int) -> list[tuple[Any, ...]]:
    headers = [str(header or "").strip().lower() for header in athlete.best_headers]
    if not headers:
        return []
    try:
        event_idx = headers.index("event")
        pb_idx = headers.index("pb")
    except ValueError:
        return []

    rows: list[tuple[Any, ...]] = []
    for raw_row in athlete.best_rows:
        if event_idx >= len(raw_row) or pb_idx >= len(raw_row):
            continue
        event_label = str(raw_row[event_idx] or "").strip()
        pb_time = str(raw_row[pb_idx] or "").strip()
        event_key = normalize_event_key(event_label)
        if not event_label or not pb_time or not event_key:
            continue
        rows.append(
            (
                runner_id,
                athlete.athlete_id,
                event_key,
                pb_time,
                parse_perf_seconds(pb_time),
                athlete.source_url,
                event_label,
            )
        )
    return rows


def build_event_years(athlete: SourceAthlete, runner_id: int) -> list[tuple[Any, ...]]:
    best_by_key: dict[tuple[str, int], tuple[str, int, str]] = {}
    for section in athlete.sections:
        for row in section.rows:
            event_label = (row.event or "").strip()
            event_key = normalize_event_key(event_label)
            perf_seconds = parse_perf_seconds(row.perf)
            year = choose_result_year(row, section)
            if not event_key or perf_seconds is None or year is None:
                continue
            key = (event_key, year)
            current = best_by_key.get(key)
            candidate = (row.perf or "", perf_seconds, event_label)
            if current is None or perf_seconds < current[1]:
                best_by_key[key] = candidate

    rows: list[tuple[Any, ...]] = []
    for (event_key, year), (sb_time, sb_seconds, event_label) in sorted(best_by_key.items()):
        rows.append(
            (
                runner_id,
                athlete.athlete_id,
                event_key,
                year,
                sb_time,
                sb_seconds,
                athlete.source_url,
                event_label,
            )
        )
    return rows


def build_performance_rows(athlete: SourceAthlete, runner_id: int) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for section in athlete.sections:
        for row in section.rows:
            event_label = (row.event or "").strip()
            event_key = normalize_event_key(event_label)
            if not event_key or not event_label:
                continue
            rows.append(
                (
                    runner_id,
                    athlete.athlete_id,
                    event_label,
                    event_key,
                    row.perf,
                    parse_perf_seconds(row.perf),
                    row.pos,
                    row.venue,
                    row.meeting,
                    row.date_text or (row.result_date.isoformat() if row.result_date else None),
                    row.result_date or parse_result_date(row.date_text),
                    section.title,
                    athlete.source_url,
                )
            )
    return rows


def write_report(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "athlete_id",
                "display_name",
                "club",
                "action",
                "reason",
                "runner_id",
                "source_url",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def progress_line(index: int, total: int, athlete: SourceAthlete, action: str, reason: str, runner_id: int | None) -> str:
    runner_text = f" runner_id={runner_id}" if runner_id is not None else ""
    club_text = f" club={athlete.club}" if athlete.club else ""
    return (
        f"[{index}/{total}] athlete_id={athlete.athlete_id} "
        f"name={athlete.preferred_name}{club_text} "
        f"action={action} reason={reason}{runner_text}"
    )


def maybe_print_progress(
    args: argparse.Namespace,
    *,
    index: int,
    total: int,
    athlete: SourceAthlete,
    action: str,
    reason: str,
    runner_id: int | None,
) -> None:
    if args.verbose:
        print(progress_line(index, total, athlete, action, reason, runner_id), flush=True)
        return

    if args.progress_every and index % args.progress_every == 0:
        print(progress_line(index, total, athlete, action, reason, runner_id), flush=True)


def update_runner_state(
    runner: TargetRunner,
    athlete: SourceAthlete,
    indexes: dict[str, Any],
) -> None:
    runner.powerof10_athlete_id = athlete.athlete_id
    if not (runner.powerof10_url or "").strip():
        runner.powerof10_url = athlete.source_url
    if not (runner.club or "").strip() and (athlete.club or "").strip():
        runner.club = athlete.club
    if not (runner.gender or "").strip() and normalize_gender_code(athlete.gender):
        runner.gender = normalize_gender_code(athlete.gender)

    indexes["by_athlete_id"][athlete.athlete_id].append(runner)
    name_key = normalize_identity(runner.name)
    club_key = normalize_identity(runner.club)
    if name_key:
        indexes["by_name"][name_key].append(runner)
        if club_key:
            indexes["by_name_club"][(name_key, club_key)].append(runner)


def insert_runner(
    conn,
    athlete: SourceAthlete,
) -> int:
    row = conn.execute(
        """
        INSERT INTO runners (
            name,
            club,
            created_at,
            gender,
            powerof10_url,
            updated_at,
            powerof10_athlete_id,
            age
        )
        VALUES (%s, %s, NOW(), %s, %s, NOW(), %s, %s)
        RETURNING id
        """,
        (
            athlete.preferred_name,
            athlete.club,
            normalize_gender_code(athlete.gender),
            athlete.source_url,
            athlete.athlete_id,
            athlete.age,
        ),
    ).fetchone()
    return int(row["id"])


def upsert_target_po10(conn, athlete: SourceAthlete, runner_id: int) -> None:
    cache_payload = athlete.cache_payload
    fetched_at = athlete.fetched_at or datetime.now(UTC)
    athlete_name = (athlete.profile_name or athlete.display_name or athlete.preferred_name).strip()

    conn.execute(
        """
        INSERT INTO powerof10_profiles (
            athlete_id,
            athlete_name,
            source_url,
            status,
            http_status,
            last_error,
            last_attempt_at,
            last_success_at,
            created_at
        )
        VALUES (%s, %s, %s, 'ok', 200, NULL, %s, %s, NOW())
        """,
        (
            athlete.athlete_id,
            athlete_name,
            athlete.source_url,
            fetched_at,
            fetched_at,
        ),
    )

    conn.execute(
        """
        INSERT INTO powerof10_cache (
            athlete_id,
            source_url,
            best_performances,
            performances,
            fetched_at
        )
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            athlete.athlete_id,
            athlete.source_url,
            Jsonb(cache_payload["best_performances"]),
            Jsonb(cache_payload["performances"]),
            fetched_at,
        ),
    )

    conn.execute("DELETE FROM powerof10_event_pbs WHERE runner_id = %s", (runner_id,))
    conn.execute("DELETE FROM powerof10_event_years WHERE runner_id = %s", (runner_id,))
    conn.execute("DELETE FROM powerof10_performances WHERE runner_id = %s", (runner_id,))

    pb_rows = build_event_pbs(athlete, runner_id)
    if pb_rows:
        conn.executemany(
            """
            INSERT INTO powerof10_event_pbs (
                runner_id,
                athlete_id,
                event_key,
                pb_time,
                pb_seconds,
                source_url,
                event_label
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            pb_rows,
        )

    sb_rows = build_event_years(athlete, runner_id)
    if sb_rows:
        conn.executemany(
            """
            INSERT INTO powerof10_event_years (
                runner_id,
                athlete_id,
                event_key,
                year,
                sb_time,
                sb_seconds,
                source_url,
                event_label
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            sb_rows,
        )

    performance_rows = build_performance_rows(athlete, runner_id)
    if performance_rows:
        conn.executemany(
            """
            INSERT INTO powerof10_performances (
                runner_id,
                athlete_id,
                event_label,
                event_key,
                perf_time,
                perf_seconds,
                pos,
                venue,
                meeting,
                date_text,
                date_date,
                section_title,
                source_url
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            performance_rows,
        )


def process_sync(args: argparse.Namespace) -> None:
    source_dsn = require_dsn(args.source_dsn, "--source-dsn")
    target_dsn = require_dsn(args.target_dsn, "--target-dsn")

    source_athletes = load_source_athletes(
        source_dsn,
        athlete_id=args.athlete_id,
        limit=args.limit,
    )
    existing_profiles, existing_cache, target_runners = load_target_state(target_dsn)
    indexes = build_runner_indexes(target_runners)

    print(
        f"Loaded {len(source_athletes)} source athletes, "
        f"{len(existing_profiles)} target powerof10_profiles, "
        f"{len(existing_cache)} target powerof10_cache rows, "
        f"{len(target_runners)} target runners",
        flush=True,
    )

    report_rows: list[dict[str, Any]] = []
    summary: dict[str, int] = defaultdict(int)
    total = len(source_athletes)

    with psycopg.connect(target_dsn, row_factory=dict_row) as target_conn:
        for index, athlete in enumerate(source_athletes, start=1):
            if athlete.athlete_id in existing_profiles:
                summary["skip_existing_profile"] += 1
                report_rows.append(
                    {
                        "athlete_id": athlete.athlete_id,
                        "display_name": athlete.display_name,
                        "club": athlete.club or "",
                        "action": "skip_existing_profile",
                        "reason": "athlete already exists in target powerof10_profiles",
                        "runner_id": "",
                        "source_url": athlete.source_url,
                    }
                )
                maybe_print_progress(
                    args,
                    index=index,
                    total=total,
                    athlete=athlete,
                    action="skip_existing_profile",
                    reason="athlete already exists in target powerof10_profiles",
                    runner_id=None,
                )
                continue

            if athlete.athlete_id in existing_cache:
                summary["skip_existing_cache"] += 1
                report_rows.append(
                    {
                        "athlete_id": athlete.athlete_id,
                        "display_name": athlete.display_name,
                        "club": athlete.club or "",
                        "action": "skip_existing_cache",
                        "reason": "athlete already exists in target powerof10_cache",
                        "runner_id": "",
                        "source_url": athlete.source_url,
                    }
                )
                maybe_print_progress(
                    args,
                    index=index,
                    total=total,
                    athlete=athlete,
                    action="skip_existing_cache",
                    reason="athlete already exists in target powerof10_cache",
                    runner_id=None,
                )
                continue

            decision = choose_runner(athlete, indexes)
            if decision.action == "skip_ambiguous":
                summary["skip_ambiguous"] += 1
                report_rows.append(
                    {
                        "athlete_id": athlete.athlete_id,
                        "display_name": athlete.display_name,
                        "club": athlete.club or "",
                        "action": "skip_ambiguous",
                        "reason": decision.reason,
                        "runner_id": "",
                        "source_url": athlete.source_url,
                    }
                )
                maybe_print_progress(
                    args,
                    index=index,
                    total=total,
                    athlete=athlete,
                    action="skip_ambiguous",
                    reason=decision.reason,
                    runner_id=None,
                )
                continue

            if decision.action == "insert_runner" and args.skip_insert_runners:
                summary["skip_insert_runner_disabled"] += 1
                report_rows.append(
                    {
                        "athlete_id": athlete.athlete_id,
                        "display_name": athlete.display_name,
                        "club": athlete.club or "",
                        "action": "skip_insert_runner_disabled",
                        "reason": "no safe runner match and --skip-insert-runners was set",
                        "runner_id": "",
                        "source_url": athlete.source_url,
                    }
                )
                maybe_print_progress(
                    args,
                    index=index,
                    total=total,
                    athlete=athlete,
                    action="skip_insert_runner_disabled",
                    reason="no safe runner match and --skip-insert-runners was set",
                    runner_id=None,
                )
                continue

            runner_id = decision.runner_id
            action = decision.action
            reason = decision.reason

            if args.dry_run:
                if action == "insert_runner":
                    summary["dry_run_insert_runner"] += 1
                else:
                    summary["dry_run_match_existing_runner"] += 1
                report_rows.append(
                    {
                        "athlete_id": athlete.athlete_id,
                        "display_name": athlete.display_name,
                        "club": athlete.club or "",
                        "action": action,
                        "reason": reason,
                        "runner_id": runner_id or "",
                        "source_url": athlete.source_url,
                    }
                )
                maybe_print_progress(
                    args,
                    index=index,
                    total=total,
                    athlete=athlete,
                    action=action,
                    reason=reason,
                    runner_id=runner_id,
                )
                continue

            try:
                with target_conn.transaction():
                    if action == "insert_runner":
                        runner_id = insert_runner(target_conn, athlete)
                        new_runner = TargetRunner(
                            runner_id=runner_id,
                            name=athlete.preferred_name,
                            club=athlete.club,
                            gender=normalize_gender_code(athlete.gender),
                            powerof10_url=athlete.source_url,
                            powerof10_athlete_id=athlete.athlete_id,
                            age=athlete.age,
                        )
                        target_runners.append(new_runner)
                        update_runner_state(new_runner, athlete, indexes)
                    else:
                        target_conn.execute(
                            """
                            UPDATE runners
                            SET
                                name = COALESCE(NULLIF(name, ''), %s),
                                club = COALESCE(NULLIF(club, ''), %s),
                                gender = COALESCE(NULLIF(gender, ''), %s),
                                age = COALESCE(age, %s),
                                powerof10_url = COALESCE(NULLIF(powerof10_url, ''), %s),
                                powerof10_athlete_id = COALESCE(powerof10_athlete_id, %s),
                                updated_at = NOW()
                            WHERE id = %s
                            """,
                            (
                                athlete.preferred_name,
                                athlete.club,
                                normalize_gender_code(athlete.gender),
                                athlete.age,
                                athlete.source_url,
                                athlete.athlete_id,
                                runner_id,
                            ),
                        )
                        runner = next(
                            item for item in target_runners if item.runner_id == runner_id
                        )
                        update_runner_state(runner, athlete, indexes)

                    upsert_target_po10(target_conn, athlete, int(runner_id))

                existing_profiles.add(athlete.athlete_id)
                existing_cache.add(athlete.athlete_id)
                summary[action] += 1
                report_rows.append(
                    {
                        "athlete_id": athlete.athlete_id,
                        "display_name": athlete.display_name,
                        "club": athlete.club or "",
                        "action": action,
                        "reason": reason,
                        "runner_id": runner_id or "",
                        "source_url": athlete.source_url,
                    }
                )
                maybe_print_progress(
                    args,
                    index=index,
                    total=total,
                    athlete=athlete,
                    action=action,
                    reason=reason,
                    runner_id=runner_id,
                )
            except Exception as exc:
                summary["error"] += 1
                report_rows.append(
                    {
                        "athlete_id": athlete.athlete_id,
                        "display_name": athlete.display_name,
                        "club": athlete.club or "",
                        "action": "error",
                        "reason": str(exc),
                        "runner_id": runner_id or "",
                        "source_url": athlete.source_url,
                    }
                )
                maybe_print_progress(
                    args,
                    index=index,
                    total=total,
                    athlete=athlete,
                    action="error",
                    reason=str(exc),
                    runner_id=runner_id,
                )

    report_path = Path(args.report)
    write_report(report_path, report_rows)

    print(f"Processed {len(source_athletes)} source athletes")
    for key in sorted(summary):
        print(f"{key}: {summary[key]}")
    print(f"Report: {report_path}")


def main() -> None:
    process_sync(parse_args())


if __name__ == "__main__":
    main()
