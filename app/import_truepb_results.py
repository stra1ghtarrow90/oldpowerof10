from __future__ import annotations

import argparse
import json
from pathlib import Path

from psycopg.types.json import Jsonb

from .db import ensure_schema, get_conn
from .legacy_dump import parse_result_date


KNOWN_RESULT_KEYS = {"event", "perf", "pos", "venue", "venue_url", "meeting", "date"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import additional TruePB result sections for athletes that already exist."
    )
    parser.add_argument(
        "--json",
        required=True,
        help="Path to a JSON file containing TruePB result sections.",
    )
    return parser.parse_args()


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


def next_section_order(conn, athlete_id: int) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(MAX(section_order), -1) + 1 AS next_order
        FROM athlete_performance_sections
        WHERE athlete_id = %s
        """,
        (athlete_id,),
    ).fetchone()
    return int(row["next_order"])


def main() -> None:
    args = parse_args()
    payload = json.loads(Path(args.json).read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise SystemExit("Expected the JSON file to contain a list of section objects")

    ensure_schema()

    imported_sections = 0
    imported_results = 0

    with get_conn() as conn:
        for item in payload:
            athlete_id = int(item["athlete_id"])
            athlete = conn.execute(
                "SELECT athlete_id FROM athletes WHERE athlete_id = %s",
                (athlete_id,),
            ).fetchone()
            if not athlete:
                raise SystemExit(
                    f"Athlete {athlete_id} does not exist yet. New athlete creation is not implemented in this step."
                )

            section_order = next_section_order(conn, athlete_id)
            title = item["title"]
            year = item.get("year")
            columns = item.get("columns") or ["Event", "Perf", "Pos", "Venue", "Meeting", "Date"]
            metadata = {key: value for key, value in item.items() if key not in {"athlete_id", "title", "year", "columns", "results"}}
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
                    "truepb_results",
                    section_order,
                    title,
                    year,
                    Jsonb(columns),
                    Jsonb(metadata),
                ),
            ).fetchone()
            section_id = section_row["id"]
            imported_sections += 1

            for row_order, result in enumerate(item.get("results") or []):
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
                        "truepb_results",
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
                imported_results += 1

            refresh_athlete_aggregates(conn, athlete_id)

        conn.commit()

    print(f"Imported {imported_sections} TruePB sections")
    print(f"Imported {imported_results} TruePB performances")


if __name__ == "__main__":
    main()
