from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any

from .export_truepb_results_sql import sql_literal, write_insert_statement
from .legacy_dump import TABLE_COPY_RE, parse_copy_line, parse_timestamp
from .sync_profiles_to_truepb import (
    MatchDecision,
    SourceAthlete,
    SourcePerformanceRow,
    SourceSection,
    TargetRunner,
    build_event_pbs,
    build_event_years,
    build_performance_rows,
    build_runner_indexes,
    choose_runner,
    load_target_state,
    normalize_gender_code,
    update_runner_state,
    write_report,
)


DEFAULT_TARGET_DSN = os.environ.get("TARGET_DATABASE_URL")
DEFAULT_REPORT = "imports/generated/truepb_profile_sync_report.csv"


@dataclass
class PlannedExport:
    athlete: SourceAthlete
    action: str
    reason: str
    matched_runner_id: int | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read a plain SQL dump of truepb_live and generate an import SQL file "
            "for the original TruePB database."
        )
    )
    parser.add_argument(
        "--sql",
        required=True,
        help="Path to a plain PostgreSQL SQL dump of truepb_live, for example pg_dump truepb_live > truepb_live.sql.",
    )
    parser.add_argument(
        "--target-dsn",
        default=DEFAULT_TARGET_DSN,
        help="PostgreSQL DSN for the target truepb database. Defaults to TARGET_DATABASE_URL.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path for the generated import SQL file.",
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
        help="Optional single athlete id to export.",
    )
    parser.add_argument(
        "--skip-insert-runners",
        action="store_true",
        help="Do not generate rows that would require inserting a new target runner.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-athlete progress as the export runs.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=100,
        help="When not using --verbose, print a progress line every N athletes. Use 0 to disable.",
    )
    return parser.parse_args()


def require_dsn(value: str | None, flag: str) -> str:
    text = (value or "").strip()
    if not text:
        raise SystemExit(f"Missing {flag}. Pass {flag} or set the corresponding environment variable.")
    return text


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


def parse_copy_header(line: str) -> tuple[str, list[str]] | None:
    match = TABLE_COPY_RE.match(line.rstrip("\n"))
    if not match:
        return None
    table_name = match.group(1)
    columns = [column.strip() for column in match.group(2).split(",")]
    return table_name, columns


def json_value(value: str | None, default: Any) -> Any:
    if value in (None, ""):
        return default
    return json.loads(value)


def load_source_athletes_from_dump(sql_path: Path, *, athlete_id: int | None, limit: int | None) -> list[SourceAthlete]:
    athletes_meta: dict[int, dict[str, Any]] = {}
    best_rows_by_athlete: dict[int, list[tuple[int, list[str]]]] = defaultdict(list)
    sections_by_id: dict[int, dict[str, Any]] = {}
    section_ids_by_athlete: dict[int, list[int]] = defaultdict(list)
    performance_rows_by_section: dict[int, list[tuple[int, SourcePerformanceRow]]] = defaultdict(list)

    current_table: str | None = None
    current_columns: list[str] = []

    with sql_path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            if current_table is None:
                copy_header = parse_copy_header(raw_line)
                if copy_header is None:
                    continue
                table_name, columns = copy_header
                if table_name in {
                    "athletes",
                    "athlete_best_performance_rows",
                    "athlete_performance_sections",
                    "athlete_performances",
                }:
                    current_table = table_name
                    current_columns = columns
                continue

            if raw_line.startswith("\\."):
                current_table = None
                current_columns = []
                continue

            fields = parse_copy_line(raw_line, len(current_columns))
            row = dict(zip(current_columns, fields))

            if current_table == "athletes":
                current_athlete_id = int(row["athlete_id"])
                if athlete_id is not None and current_athlete_id != athlete_id:
                    continue
                athletes_meta[current_athlete_id] = {
                    "athlete_id": current_athlete_id,
                    "display_name": row.get("display_name") or row.get("profile_name") or f"Athlete {current_athlete_id}",
                    "profile_name": row.get("profile_name"),
                    "runner_name": row.get("runner_name"),
                    "club": row.get("club"),
                    "gender": row.get("gender"),
                    "age": int(row["age"]) if row.get("age") else None,
                    "age_group": row.get("age_group"),
                    "source_url": row.get("source_url") or f"https://www.thepowerof10.info/athletes/profile.aspx?athleteid={current_athlete_id}",
                    "fetched_at": parse_timestamp(row.get("fetched_at")),
                    "best_headers": list(json_value(row.get("best_headers"), [])),
                }
                continue

            if current_table == "athlete_best_performance_rows":
                current_athlete_id = int(row["athlete_id"])
                if athlete_id is not None and current_athlete_id != athlete_id:
                    continue
                cells = ["" if value is None else str(value) for value in json_value(row.get("cells"), [])]
                best_rows_by_athlete[current_athlete_id].append((int(row["row_order"]), cells))
                continue

            if current_table == "athlete_performance_sections":
                current_athlete_id = int(row["athlete_id"])
                if athlete_id is not None and current_athlete_id != athlete_id:
                    continue
                if row.get("source_kind") == "truepb_results":
                    continue
                section_id = int(row["id"])
                sections_by_id[section_id] = {
                    "athlete_id": current_athlete_id,
                    "section_order": int(row["section_order"]),
                    "title": row.get("title") or f"Section {row['section_order']}",
                    "year": int(row["year"]) if row.get("year") else None,
                    "columns": list(json_value(row.get("columns_json"), [])),
                }
                section_ids_by_athlete[current_athlete_id].append(section_id)
                continue

            if current_table == "athlete_performances":
                section_id_text = row.get("section_id")
                if not section_id_text:
                    continue
                section_id = int(section_id_text)
                if section_id not in sections_by_id:
                    continue
                source_kind = row.get("source_kind")
                if source_kind == "truepb_results":
                    continue
                performance_rows_by_section[section_id].append(
                    (
                        int(row["row_order"]),
                        SourcePerformanceRow(
                            event=row.get("event"),
                            perf=row.get("perf"),
                            pos=row.get("pos"),
                            venue=row.get("venue"),
                            venue_url=row.get("venue_url"),
                            meeting=row.get("meeting"),
                            date_text=row.get("date_text"),
                            result_date=datetime.fromisoformat(row["result_date"]).date() if row.get("result_date") else None,
                            extra=dict(json_value(row.get("extra"), {})),
                        ),
                    )
                )

    athletes: list[SourceAthlete] = []
    for current_athlete_id in sorted(athletes_meta):
        meta = athletes_meta[current_athlete_id]
        best_rows = [
            cells
            for _, cells in sorted(best_rows_by_athlete.get(current_athlete_id, []), key=lambda item: item[0])
        ]

        sections: list[SourceSection] = []
        for section_id in sorted(
            section_ids_by_athlete.get(current_athlete_id, []),
            key=lambda item: (
                sections_by_id[item]["section_order"],
                item,
            ),
        ):
            section_meta = sections_by_id[section_id]
            section = SourceSection(
                title=section_meta["title"],
                year=section_meta["year"],
                columns=section_meta["columns"],
                rows=[
                    row
                    for _, row in sorted(
                        performance_rows_by_section.get(section_id, []),
                        key=lambda item: item[0],
                    )
                ],
            )
            sections.append(section)

        profile_name = (meta["profile_name"] or "").strip()
        if not profile_name and not best_rows and not sections:
            continue

        athletes.append(
            SourceAthlete(
                athlete_id=current_athlete_id,
                display_name=str(meta["display_name"]).strip(),
                profile_name=meta["profile_name"],
                runner_name=meta["runner_name"],
                club=meta["club"],
                gender=meta["gender"],
                age=meta["age"],
                age_group=meta["age_group"],
                source_url=meta["source_url"],
                fetched_at=meta["fetched_at"],
                best_headers=[str(value) for value in meta["best_headers"]],
                best_rows=best_rows,
                sections=sections,
            )
        )

    if limit is not None:
        athletes = athletes[:limit]
    return athletes


def plan_exports(
    athletes: list[SourceAthlete],
    *,
    target_dsn: str,
    skip_insert_runners: bool,
    args: argparse.Namespace,
) -> tuple[list[PlannedExport], list[dict[str, Any]], dict[str, int]]:
    existing_profiles, existing_cache, target_runners = load_target_state(target_dsn)
    indexes = build_runner_indexes(target_runners)

    print(
        f"Loaded {len(athletes)} source athletes from dump, "
        f"{len(existing_profiles)} target powerof10_profiles, "
        f"{len(existing_cache)} target powerof10_cache rows, "
        f"{len(target_runners)} target runners",
        flush=True,
    )

    planned: list[PlannedExport] = []
    report_rows: list[dict[str, Any]] = []
    summary: dict[str, int] = defaultdict(int)
    total = len(athletes)

    for index, athlete in enumerate(athletes, start=1):
        if athlete.athlete_id in existing_profiles:
            action = "skip_existing_profile"
            reason = "athlete already exists in target powerof10_profiles"
            summary[action] += 1
            report_rows.append(
                {
                    "athlete_id": athlete.athlete_id,
                    "display_name": athlete.display_name,
                    "club": athlete.club or "",
                    "action": action,
                    "reason": reason,
                    "runner_id": "",
                    "source_url": athlete.source_url,
                }
            )
            maybe_print_progress(args, index=index, total=total, athlete=athlete, action=action, reason=reason, runner_id=None)
            continue

        if athlete.athlete_id in existing_cache:
            action = "skip_existing_cache"
            reason = "athlete already exists in target powerof10_cache"
            summary[action] += 1
            report_rows.append(
                {
                    "athlete_id": athlete.athlete_id,
                    "display_name": athlete.display_name,
                    "club": athlete.club or "",
                    "action": action,
                    "reason": reason,
                    "runner_id": "",
                    "source_url": athlete.source_url,
                }
            )
            maybe_print_progress(args, index=index, total=total, athlete=athlete, action=action, reason=reason, runner_id=None)
            continue

        decision: MatchDecision = choose_runner(athlete, indexes)
        if decision.action == "skip_ambiguous":
            action = "skip_ambiguous"
            reason = decision.reason
            summary[action] += 1
            report_rows.append(
                {
                    "athlete_id": athlete.athlete_id,
                    "display_name": athlete.display_name,
                    "club": athlete.club or "",
                    "action": action,
                    "reason": reason,
                    "runner_id": "",
                    "source_url": athlete.source_url,
                }
            )
            maybe_print_progress(args, index=index, total=total, athlete=athlete, action=action, reason=reason, runner_id=None)
            continue

        if decision.action == "insert_runner" and skip_insert_runners:
            action = "skip_insert_runner_disabled"
            reason = "no safe runner match and --skip-insert-runners was set"
            summary[action] += 1
            report_rows.append(
                {
                    "athlete_id": athlete.athlete_id,
                    "display_name": athlete.display_name,
                    "club": athlete.club or "",
                    "action": action,
                    "reason": reason,
                    "runner_id": "",
                    "source_url": athlete.source_url,
                }
            )
            maybe_print_progress(args, index=index, total=total, athlete=athlete, action=action, reason=reason, runner_id=None)
            continue

        matched_runner_id = decision.runner_id
        action = "export_match_existing_runner" if decision.action == "match_existing_runner" else "export_insert_runner"
        reason = decision.reason
        summary[action] += 1
        planned.append(
            PlannedExport(
                athlete=athlete,
                action=action,
                reason=reason,
                matched_runner_id=matched_runner_id,
            )
        )
        report_rows.append(
            {
                "athlete_id": athlete.athlete_id,
                "display_name": athlete.display_name,
                "club": athlete.club or "",
                "action": action,
                "reason": reason,
                "runner_id": matched_runner_id or "",
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
            runner_id=matched_runner_id,
        )

        if decision.action == "insert_runner":
            runner = TargetRunner(
                runner_id=-(athlete.athlete_id),
                name=athlete.preferred_name,
                club=athlete.club,
                gender=normalize_gender_code(athlete.gender),
                powerof10_url=athlete.source_url,
                powerof10_athlete_id=athlete.athlete_id,
                age=athlete.age,
            )
            target_runners.append(runner)
            update_runner_state(runner, athlete, indexes)
        elif matched_runner_id is not None:
            runner = next((item for item in target_runners if item.runner_id == matched_runner_id), None)
            if runner is not None:
                update_runner_state(runner, athlete, indexes)

    return planned, report_rows, summary


def generate_sql(planned: list[PlannedExport], *, source_sql_path: Path) -> str:
    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat()

    athlete_rows: list[list[Any]] = []
    pb_rows: list[list[Any]] = []
    sb_rows: list[list[Any]] = []
    performance_rows: list[list[Any]] = []

    for item in planned:
        athlete = item.athlete
        athlete_name = (athlete.profile_name or athlete.display_name or athlete.preferred_name).strip()
        cache_payload = athlete.cache_payload
        athlete_rows.append(
            [
                athlete.athlete_id,
                item.matched_runner_id,
                athlete.preferred_name,
                athlete_name,
                athlete.club,
                normalize_gender_code(athlete.gender),
                athlete.age,
                athlete.source_url,
                athlete.fetched_at,
                cache_payload["best_performances"],
                cache_payload["performances"],
            ]
        )

        for row in build_event_pbs(athlete, 0):
            pb_rows.append(
                [
                    athlete.athlete_id,
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                ]
            )

        for row in build_event_years(athlete, 0):
            sb_rows.append(
                [
                    athlete.athlete_id,
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                ]
            )

        for row in build_performance_rows(athlete, 0):
            performance_rows.append(
                [
                    athlete.athlete_id,
                    row[2],
                    row[3],
                    row[4],
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                    row[10],
                    row[11],
                    row[12],
                ]
            )

    output = StringIO()
    output.write("-- Generated by app.export_profiles_to_truepb_sql\n")
    output.write(f"-- Generated at: {generated_at}\n")
    output.write(f"-- Source dump: {source_sql_path}\n\n")
    output.write("BEGIN;\n\n")
    output.write(
        """CREATE TEMP TABLE tmp_po10_sync_athletes (
  athlete_id BIGINT PRIMARY KEY,
  matched_runner_id INTEGER,
  runner_name TEXT NOT NULL,
  athlete_name TEXT NOT NULL,
  club TEXT,
  gender TEXT,
  age INTEGER,
  source_url TEXT NOT NULL,
  fetched_at TIMESTAMPTZ,
  best_performances JSONB NOT NULL,
  performances JSONB NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE tmp_po10_sync_event_pbs (
  athlete_id BIGINT NOT NULL,
  event_key TEXT NOT NULL,
  pb_time TEXT,
  pb_seconds INTEGER,
  source_url TEXT,
  event_label TEXT NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE tmp_po10_sync_event_years (
  athlete_id BIGINT NOT NULL,
  event_key TEXT NOT NULL,
  year INTEGER NOT NULL,
  sb_time TEXT,
  sb_seconds INTEGER,
  source_url TEXT,
  event_label TEXT NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE tmp_po10_sync_performances (
  athlete_id BIGINT NOT NULL,
  event_label TEXT NOT NULL,
  event_key TEXT NOT NULL,
  perf_time TEXT,
  perf_seconds INTEGER,
  pos TEXT,
  venue TEXT,
  meeting TEXT,
  date_text TEXT,
  date_date DATE,
  section_title TEXT,
  source_url TEXT
) ON COMMIT DROP;

CREATE TEMP TABLE tmp_runner_map (
  athlete_id BIGINT PRIMARY KEY,
  runner_id INTEGER NOT NULL
) ON COMMIT DROP;

"""
    )

    write_insert_statement(
        output,
        "tmp_po10_sync_athletes",
        [
            "athlete_id",
            "matched_runner_id",
            "runner_name",
            "athlete_name",
            "club",
            "gender",
            "age",
            "source_url",
            "fetched_at",
            "best_performances",
            "performances",
        ],
        athlete_rows,
    )
    write_insert_statement(
        output,
        "tmp_po10_sync_event_pbs",
        [
            "athlete_id",
            "event_key",
            "pb_time",
            "pb_seconds",
            "source_url",
            "event_label",
        ],
        pb_rows,
    )
    write_insert_statement(
        output,
        "tmp_po10_sync_event_years",
        [
            "athlete_id",
            "event_key",
            "year",
            "sb_time",
            "sb_seconds",
            "source_url",
            "event_label",
        ],
        sb_rows,
    )
    write_insert_statement(
        output,
        "tmp_po10_sync_performances",
        [
            "athlete_id",
            "event_label",
            "event_key",
            "perf_time",
            "perf_seconds",
            "pos",
            "venue",
            "meeting",
            "date_text",
            "date_date",
            "section_title",
            "source_url",
        ],
        performance_rows,
    )

    output.write(
        """CREATE TEMP TABLE tmp_po10_filtered AS
SELECT *
FROM tmp_po10_sync_athletes a
WHERE NOT EXISTS (
        SELECT 1
        FROM powerof10_profiles p
        WHERE p.athlete_id = a.athlete_id
      )
  AND NOT EXISTS (
        SELECT 1
        FROM powerof10_cache c
        WHERE c.athlete_id = a.athlete_id
      );

CREATE UNIQUE INDEX tmp_po10_filtered_athlete_id_idx ON tmp_po10_filtered (athlete_id);

INSERT INTO tmp_runner_map (athlete_id, runner_id)
SELECT f.athlete_id, r.id
FROM tmp_po10_filtered f
JOIN runners r ON r.powerof10_athlete_id = f.athlete_id
ON CONFLICT (athlete_id) DO NOTHING;

INSERT INTO tmp_runner_map (athlete_id, runner_id)
SELECT f.athlete_id, f.matched_runner_id
FROM tmp_po10_filtered f
JOIN runners r ON r.id = f.matched_runner_id
LEFT JOIN tmp_runner_map m ON m.athlete_id = f.athlete_id
WHERE f.matched_runner_id IS NOT NULL
  AND m.athlete_id IS NULL
ON CONFLICT (athlete_id) DO NOTHING;

WITH inserted AS (
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
  SELECT
    f.runner_name,
    NULLIF(f.club, ''),
    NOW(),
    NULLIF(f.gender, ''),
    f.source_url,
    NOW(),
    f.athlete_id,
    f.age
  FROM tmp_po10_filtered f
  LEFT JOIN tmp_runner_map m ON m.athlete_id = f.athlete_id
  WHERE m.athlete_id IS NULL
  RETURNING id, powerof10_athlete_id
)
INSERT INTO tmp_runner_map (athlete_id, runner_id)
SELECT powerof10_athlete_id::bigint, id
FROM inserted
ON CONFLICT (athlete_id) DO NOTHING;

UPDATE runners r
SET
  name = COALESCE(NULLIF(r.name, ''), f.runner_name),
  club = COALESCE(NULLIF(r.club, ''), NULLIF(f.club, '')),
  gender = COALESCE(NULLIF(r.gender, ''), NULLIF(f.gender, '')),
  age = COALESCE(r.age, f.age),
  powerof10_url = COALESCE(NULLIF(r.powerof10_url, ''), f.source_url),
  powerof10_athlete_id = COALESCE(r.powerof10_athlete_id, f.athlete_id),
  updated_at = NOW()
FROM tmp_po10_filtered f
JOIN tmp_runner_map m ON m.athlete_id = f.athlete_id
WHERE r.id = m.runner_id;

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
SELECT
  f.athlete_id,
  f.athlete_name,
  f.source_url,
  'ok',
  200,
  NULL,
  COALESCE(f.fetched_at, NOW()),
  COALESCE(f.fetched_at, NOW()),
  NOW()
FROM tmp_po10_filtered f
ON CONFLICT (athlete_id) DO NOTHING;

INSERT INTO powerof10_cache (
  athlete_id,
  source_url,
  best_performances,
  performances,
  fetched_at
)
SELECT
  f.athlete_id,
  f.source_url,
  f.best_performances,
  f.performances,
  COALESCE(f.fetched_at, NOW())
FROM tmp_po10_filtered f
ON CONFLICT (athlete_id) DO NOTHING;

DELETE FROM powerof10_event_pbs
WHERE runner_id IN (SELECT runner_id FROM tmp_runner_map);

DELETE FROM powerof10_event_years
WHERE runner_id IN (SELECT runner_id FROM tmp_runner_map);

DELETE FROM powerof10_performances
WHERE runner_id IN (SELECT runner_id FROM tmp_runner_map);

INSERT INTO powerof10_event_pbs (
  runner_id,
  athlete_id,
  event_key,
  pb_time,
  pb_seconds,
  source_url,
  event_label
)
SELECT
  m.runner_id,
  p.athlete_id,
  p.event_key,
  p.pb_time,
  p.pb_seconds,
  p.source_url,
  p.event_label
FROM tmp_po10_sync_event_pbs p
JOIN tmp_runner_map m ON m.athlete_id = p.athlete_id
JOIN tmp_po10_filtered f ON f.athlete_id = p.athlete_id;

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
SELECT
  m.runner_id,
  y.athlete_id,
  y.event_key,
  y.year,
  y.sb_time,
  y.sb_seconds,
  y.source_url,
  y.event_label
FROM tmp_po10_sync_event_years y
JOIN tmp_runner_map m ON m.athlete_id = y.athlete_id
JOIN tmp_po10_filtered f ON f.athlete_id = y.athlete_id;

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
SELECT
  m.runner_id,
  p.athlete_id,
  p.event_label,
  p.event_key,
  p.perf_time,
  p.perf_seconds,
  p.pos,
  p.venue,
  p.meeting,
  p.date_text,
  p.date_date,
  p.section_title,
  p.source_url
FROM tmp_po10_sync_performances p
JOIN tmp_runner_map m ON m.athlete_id = p.athlete_id
JOIN tmp_po10_filtered f ON f.athlete_id = p.athlete_id;

COMMIT;
"""
    )
    return output.getvalue()


def main() -> None:
    args = parse_args()
    sql_path = Path(args.sql)
    target_dsn = require_dsn(args.target_dsn, "--target-dsn")

    source_athletes = load_source_athletes_from_dump(
        sql_path,
        athlete_id=args.athlete_id,
        limit=args.limit,
    )
    planned, report_rows, summary = plan_exports(
        source_athletes,
        target_dsn=target_dsn,
        skip_insert_runners=args.skip_insert_runners,
        args=args,
    )

    report_path = Path(args.report)
    write_report(report_path, report_rows)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(generate_sql(planned, source_sql_path=sql_path), encoding="utf-8")

    print(f"Processed {len(source_athletes)} source athletes from dump")
    for key in sorted(summary):
        print(f"{key}: {summary[key]}")
    print(f"Exported athletes: {len(planned)}")
    print(f"SQL: {output_path}")
    print(f"Report: {report_path}")


if __name__ == "__main__":
    main()
