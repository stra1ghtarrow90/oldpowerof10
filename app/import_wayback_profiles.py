from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from psycopg.types.json import Jsonb

from .db import ensure_schema, get_conn
from .legacy_dump import parse_result_date
from .wayback_html import (
    WaybackAthlete,
    athlete_id_from_path,
    discover_manifest_path,
    iter_html_files,
    load_manifest,
    normalize_identity,
    parse_wayback_profile,
)


DEFAULT_SYNTHETIC_ID_OFFSET = 9_000_000_000
KNOWN_RESULT_KEYS = {"event", "perf", "pos", "venue", "venue_url", "meeting", "date"}


@dataclass
class AthleteState:
    athlete_id: int
    display_name: str | None
    profile_name: str | None
    runner_name: str | None
    club: str | None
    gender: str | None
    age_group: str | None
    source_url: str | None
    has_best_rows: bool
    truepb_sections: int
    non_truepb_sections: int

    @property
    def has_powerof10_profile(self) -> bool:
        return bool(self.profile_name) or self.has_best_rows or self.non_truepb_sections > 0

    @property
    def is_truepb_only(self) -> bool:
        return not self.has_powerof10_profile and self.truepb_sections > 0

    @property
    def candidate_names(self) -> set[str]:
        return {
            value
            for value in (
                normalize_identity(self.display_name),
                normalize_identity(self.profile_name),
                normalize_identity(self.runner_name),
            )
            if value
        }


@dataclass
class MatchDecision:
    athlete_id: int | None
    status: str
    reason: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import saved Wayback Power of 10 athlete profile HTML into the live PostgreSQL site.",
    )
    parser.add_argument(
        "--html-dir",
        required=True,
        help="Directory containing downloaded Wayback athlete HTML files.",
    )
    parser.add_argument(
        "--manifest",
        default=None,
        help="Optional latest_profile_captures.csv produced by app.wayback_profiles.",
    )
    parser.add_argument(
        "--report",
        default=None,
        help="Optional CSV report path. Defaults to import_report.csv beside the HTML directory.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit for testing a smaller batch.",
    )
    parser.add_argument(
        "--synthetic-id-offset",
        type=int,
        default=DEFAULT_SYNTHETIC_ID_OFFSET,
        help="Synthetic athlete id offset used by the TruePB SQL exporter.",
    )
    return parser.parse_args()


def load_athlete_states(conn) -> dict[int, AthleteState]:
    rows = conn.execute(
        """
        SELECT
            a.athlete_id,
            a.display_name,
            a.profile_name,
            a.runner_name,
            a.club,
            a.gender,
            a.age_group,
            a.source_url,
            EXISTS(
                SELECT 1
                FROM athlete_best_performance_rows b
                WHERE b.athlete_id = a.athlete_id
            ) AS has_best_rows,
            COUNT(DISTINCT CASE WHEN s.source_kind = 'truepb_results' THEN s.id END) AS truepb_sections,
            COUNT(DISTINCT CASE WHEN s.source_kind <> 'truepb_results' THEN s.id END) AS non_truepb_sections
        FROM athletes a
        LEFT JOIN athlete_performance_sections s ON s.athlete_id = a.athlete_id
        GROUP BY
            a.athlete_id,
            a.display_name,
            a.profile_name,
            a.runner_name,
            a.club,
            a.gender,
            a.age_group,
            a.source_url
        """
    ).fetchall()

    states: dict[int, AthleteState] = {}
    for row in rows:
        states[row["athlete_id"]] = AthleteState(
            athlete_id=row["athlete_id"],
            display_name=row["display_name"],
            profile_name=row["profile_name"],
            runner_name=row["runner_name"],
            club=row["club"],
            gender=row["gender"],
            age_group=row["age_group"],
            source_url=row["source_url"],
            has_best_rows=bool(row["has_best_rows"]),
            truepb_sections=int(row["truepb_sections"] or 0),
            non_truepb_sections=int(row["non_truepb_sections"] or 0),
        )
    return states


def build_truepb_candidate_index(
    states_by_id: dict[int, AthleteState],
    synthetic_id_offset: int,
) -> dict[str, set[int]]:
    index: dict[str, set[int]] = defaultdict(set)
    for state in states_by_id.values():
        if state.athlete_id < synthetic_id_offset:
            continue
        if not state.is_truepb_only:
            continue
        for name in state.candidate_names:
            index[name].add(state.athlete_id)
    return index


def remove_candidate_from_index(index: dict[str, set[int]], state: AthleteState | None) -> None:
    if state is None:
        return
    for name in state.candidate_names:
        ids = index.get(name)
        if not ids:
            continue
        ids.discard(state.athlete_id)
        if not ids:
            index.pop(name, None)


def find_synthetic_match(
    athlete: WaybackAthlete,
    states_by_id: dict[int, AthleteState],
    candidate_index: dict[str, set[int]],
) -> MatchDecision:
    names = {
        value
        for value in (
            normalize_identity(athlete.display_name),
            normalize_identity(athlete.profile_name),
            normalize_identity(athlete.runner_name),
        )
        if value
    }
    if not names:
        return MatchDecision(None, "none", "no_name")

    candidate_ids: set[int] = set()
    for name in names:
        candidate_ids.update(candidate_index.get(name, set()))

    if not candidate_ids:
        return MatchDecision(None, "none", "no_exact_name_match")

    scored: list[tuple[int, int, str]] = []
    athlete_club = normalize_identity(athlete.club)
    athlete_gender = (athlete.gender or "").strip().lower()
    athlete_age_group = normalize_identity(athlete.age_group)

    for candidate_id in sorted(candidate_ids):
        state = states_by_id.get(candidate_id)
        if state is None:
            continue

        score = 100
        reasons = ["name"]

        candidate_club = normalize_identity(state.club)
        if athlete_club and candidate_club:
            if athlete_club != candidate_club:
                continue
            score += 30
            reasons.append("club")

        candidate_gender = (state.gender or "").strip().lower()
        if athlete_gender and candidate_gender:
            if athlete_gender != candidate_gender:
                continue
            score += 10
            reasons.append("gender")

        candidate_age_group = normalize_identity(state.age_group)
        if athlete_age_group and candidate_age_group:
            if athlete_age_group != candidate_age_group:
                continue
            score += 10
            reasons.append("age_group")

        scored.append((score, candidate_id, "+".join(reasons)))

    if not scored:
        return MatchDecision(None, "none", "all_name_matches_conflicted")

    scored.sort(key=lambda item: (-item[0], item[1]))
    top_score, top_id, top_reason = scored[0]
    top_ties = [item for item in scored if item[0] == top_score]
    if len(top_ties) > 1:
        tied = ",".join(str(item[1]) for item in top_ties)
        return MatchDecision(None, "ambiguous", f"top_score_tie:{tied}")

    if top_score >= 130:
        return MatchDecision(top_id, "matched", top_reason)

    if top_score >= 110 and len(scored) == 1:
        return MatchDecision(top_id, "matched", top_reason)

    return MatchDecision(None, "ambiguous", f"weak_match:{top_reason}")


def ensure_target_athlete(conn, athlete_id: int, athlete: WaybackAthlete, existing_state: AthleteState | None) -> None:
    if existing_state is None:
        conn.execute(
            """
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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 0, NULL, NULL)
            """,
            (
                athlete_id,
                athlete.display_name,
                athlete.profile_name,
                athlete.runner_name,
                athlete.club,
                athlete.gender,
                None,
                athlete.age_group,
                athlete.source_url,
                athlete.fetched_at,
                Jsonb(athlete.best_headers),
            ),
        )
        return

    conn.execute(
        """
        UPDATE athletes
        SET
            display_name = %s,
            profile_name = %s,
            runner_name = COALESCE(runner_name, %s),
            club = COALESCE(%s, club),
            gender = COALESCE(%s, gender),
            age_group = COALESCE(%s, age_group),
            source_url = %s,
            fetched_at = COALESCE(%s, fetched_at),
            best_headers = %s,
            updated_at = NOW()
        WHERE athlete_id = %s
        """,
        (
            athlete.display_name,
            athlete.profile_name,
            athlete.runner_name,
            athlete.club,
            athlete.gender,
            athlete.age_group,
            athlete.source_url,
            athlete.fetched_at,
            Jsonb(athlete.best_headers),
            athlete_id,
        ),
    )


def move_synthetic_athlete(conn, source_athlete_id: int, target_athlete_id: int) -> None:
    conn.execute(
        "UPDATE athlete_best_performance_rows SET athlete_id = %s WHERE athlete_id = %s",
        (target_athlete_id, source_athlete_id),
    )
    conn.execute(
        "UPDATE athlete_performance_sections SET athlete_id = %s WHERE athlete_id = %s",
        (target_athlete_id, source_athlete_id),
    )
    conn.execute(
        "UPDATE athlete_performances SET athlete_id = %s WHERE athlete_id = %s",
        (target_athlete_id, source_athlete_id),
    )
    conn.execute(
        "DELETE FROM athletes WHERE athlete_id = %s",
        (source_athlete_id,),
    )


def next_section_order(conn, athlete_id: int) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(MAX(section_order), -1) + 1 AS next_order
        FROM athlete_performance_sections
        WHERE athlete_id = %s
        """,
        (athlete_id,),
    ).fetchone()
    return int(row["next_order"] or 0)


def insert_best_rows(conn, athlete_id: int, athlete: WaybackAthlete) -> None:
    conn.execute(
        "DELETE FROM athlete_best_performance_rows WHERE athlete_id = %s",
        (athlete_id,),
    )
    for row_order, cells in enumerate(athlete.best_rows):
        conn.execute(
            """
            INSERT INTO athlete_best_performance_rows (athlete_id, row_order, cells)
            VALUES (%s, %s, %s)
            """,
            (athlete_id, row_order, Jsonb(cells)),
        )


def insert_wayback_sections(conn, athlete_id: int, athlete: WaybackAthlete) -> None:
    section_order = next_section_order(conn, athlete_id)
    for section in athlete.sections:
        metadata = {
            **section.metadata,
            "profile_source_url": athlete.source_url,
            "profile_wayback_url": athlete.wayback_url,
        }
        section_row = conn.execute(
            """
            INSERT INTO athlete_performance_sections (
                athlete_id,
                source_kind,
                section_order,
                title,
                year,
                columns_json,
                metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                athlete_id,
                "powerof10_wayback",
                section_order,
                section.title,
                section.year,
                Jsonb(section.columns),
                Jsonb(metadata),
            ),
        ).fetchone()
        section_id = section_row["id"]

        for row_order, result in enumerate(section.rows):
            extra = {key: value for key, value in result.items() if key not in KNOWN_RESULT_KEYS}
            conn.execute(
                """
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
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    athlete_id,
                    section_id,
                    "powerof10_wayback",
                    row_order,
                    result.get("event"),
                    result.get("perf"),
                    result.get("pos"),
                    result.get("venue"),
                    result.get("venue_url"),
                    result.get("meeting"),
                    result.get("date"),
                    parse_result_date(result.get("date")),
                    Jsonb(extra),
                ),
            )

        section_order += 1


def refresh_athlete_aggregates(conn, athlete_id: int) -> None:
    summary = conn.execute(
        """
        SELECT
            COUNT(p.id) AS performance_count,
            COUNT(DISTINCT s.id) AS section_count,
            MIN(s.year) AS first_year,
            MAX(s.year) AS last_year
        FROM athlete_performance_sections s
        LEFT JOIN athlete_performances p ON p.section_id = s.id
        WHERE s.athlete_id = %s
        """,
        (athlete_id,),
    ).fetchone()

    conn.execute(
        """
        UPDATE athletes
        SET
            performance_count = %s,
            section_count = %s,
            first_year = %s,
            last_year = %s,
            updated_at = NOW()
        WHERE athlete_id = %s
        """,
        (
            summary["performance_count"] or 0,
            summary["section_count"] or 0,
            summary["first_year"],
            summary["last_year"],
            athlete_id,
        ),
    )


def write_report(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "athlete_id",
                "target_athlete_id",
                "html_file",
                "display_name",
                "club",
                "action",
                "reason",
                "matched_synthetic_id",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    html_dir = Path(args.html_dir)
    if not html_dir.exists():
        raise SystemExit(f"HTML directory does not exist: {html_dir}")

    manifest_path = discover_manifest_path(html_dir, args.manifest)
    manifest = load_manifest(manifest_path)
    html_files = iter_html_files(html_dir, args.limit)
    if not html_files:
        raise SystemExit(f"No HTML files found under {html_dir}")

    ensure_schema()

    report_rows: list[dict] = []
    summary: dict[str, int] = defaultdict(int)

    with get_conn() as conn:
        states_by_id = load_athlete_states(conn)
        candidate_index = build_truepb_candidate_index(states_by_id, args.synthetic_id_offset)

        for html_path in html_files:
            athlete_id_hint = athlete_id_from_path(html_path)
            manifest_row = manifest.get(athlete_id_hint or -1)

            try:
                athlete = parse_wayback_profile(html_path, manifest_row)
            except Exception as exc:
                summary["error"] += 1
                report_rows.append(
                    {
                        "athlete_id": athlete_id_hint or "",
                        "target_athlete_id": "",
                        "html_file": html_path.name,
                        "display_name": "",
                        "club": "",
                        "action": "error",
                        "reason": str(exc),
                        "matched_synthetic_id": "",
                    }
                )
                continue

            existing_state = states_by_id.get(athlete.athlete_id)
            if existing_state and existing_state.has_powerof10_profile:
                summary["skipped_existing_powerof10"] += 1
                report_rows.append(
                    {
                        "athlete_id": athlete.athlete_id,
                        "target_athlete_id": athlete.athlete_id,
                        "html_file": html_path.name,
                        "display_name": athlete.display_name,
                        "club": athlete.club or "",
                        "action": "skipped_existing_powerof10",
                        "reason": "athlete already has imported powerof10 profile data",
                        "matched_synthetic_id": "",
                    }
                )
                continue

            match = MatchDecision(None, "none", "not_checked")
            if existing_state is None:
                match = find_synthetic_match(athlete, states_by_id, candidate_index)
                if match.status == "ambiguous":
                    summary["skipped_ambiguous_match"] += 1
                    report_rows.append(
                        {
                            "athlete_id": athlete.athlete_id,
                            "target_athlete_id": "",
                            "html_file": html_path.name,
                            "display_name": athlete.display_name,
                            "club": athlete.club or "",
                            "action": "skipped_ambiguous_match",
                            "reason": match.reason,
                            "matched_synthetic_id": "",
                        }
                    )
                    continue

            try:
                ensure_target_athlete(conn, athlete.athlete_id, athlete, existing_state)
                if match.athlete_id is not None:
                    move_synthetic_athlete(conn, match.athlete_id, athlete.athlete_id)
                insert_best_rows(conn, athlete.athlete_id, athlete)
                insert_wayback_sections(conn, athlete.athlete_id, athlete)
                refresh_athlete_aggregates(conn, athlete.athlete_id)
                conn.commit()
            except Exception as exc:
                conn.rollback()
                summary["error"] += 1
                report_rows.append(
                    {
                        "athlete_id": athlete.athlete_id,
                        "target_athlete_id": athlete.athlete_id,
                        "html_file": html_path.name,
                        "display_name": athlete.display_name,
                        "club": athlete.club or "",
                        "action": "error",
                        "reason": str(exc),
                        "matched_synthetic_id": match.athlete_id or "",
                    }
                )
                continue

            if match.athlete_id is not None:
                summary["merged_synthetic"] += 1
                remove_candidate_from_index(candidate_index, states_by_id.get(match.athlete_id))
                states_by_id.pop(match.athlete_id, None)
                action = "merged_synthetic"
                reason = match.reason
            elif existing_state is not None:
                summary["enriched_existing_truepb"] += 1
                action = "enriched_existing_truepb"
                reason = "existing athlete had only truepb_results"
            else:
                summary["inserted_new_wayback"] += 1
                action = "inserted_new_wayback"
                reason = "new athlete from wayback profile"

            remove_candidate_from_index(candidate_index, existing_state)
            states_by_id[athlete.athlete_id] = AthleteState(
                athlete_id=athlete.athlete_id,
                display_name=athlete.display_name,
                profile_name=athlete.profile_name,
                runner_name=(existing_state.runner_name if existing_state and existing_state.runner_name else athlete.runner_name),
                club=athlete.club or (existing_state.club if existing_state else None),
                gender=athlete.gender or (existing_state.gender if existing_state else None),
                age_group=athlete.age_group or (existing_state.age_group if existing_state else None),
                source_url=athlete.source_url,
                has_best_rows=bool(athlete.best_rows),
                truepb_sections=existing_state.truepb_sections if existing_state else 0,
                non_truepb_sections=len(athlete.sections),
            )

            report_rows.append(
                {
                    "athlete_id": athlete.athlete_id,
                    "target_athlete_id": athlete.athlete_id,
                    "html_file": html_path.name,
                    "display_name": athlete.display_name,
                    "club": athlete.club or "",
                    "action": action,
                    "reason": reason,
                    "matched_synthetic_id": match.athlete_id or "",
                }
            )

    report_path = Path(args.report) if args.report else (html_dir.parent / "import_report.csv")
    write_report(report_path, report_rows)

    print(f"Processed {len(report_rows)} Wayback HTML files")
    for key in sorted(summary):
        print(f"{key}: {summary[key]}")
    if manifest_path is not None:
        print(f"Manifest: {manifest_path}")
    print(f"Report: {report_path}")


if __name__ == "__main__":
    main()
