from __future__ import annotations

import argparse
from pathlib import Path

from psycopg.types.json import Jsonb

from .db import ensure_schema, get_conn
from .legacy_dump import parse_result_date, load_dump, resolve_athletes


KNOWN_RESULT_KEYS = {"event", "perf", "pos", "venue", "venue_url", "meeting", "date"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import the legacy Power of 10 cache dump.")
    parser.add_argument(
        "--sql",
        default="/imports/legacy/16-04-2026.sql",
        help="Path to the PostgreSQL dump to import.",
    )
    return parser.parse_args()


def reset_tables(conn) -> None:
    conn.execute(
        """
        TRUNCATE TABLE
          athlete_performances,
          athlete_performance_sections,
          athlete_best_performance_rows,
          athletes
        RESTART IDENTITY CASCADE
        """
    )


def import_athletes(conn, athletes) -> tuple[int, int, int]:
    athlete_count = 0
    section_count = 0
    performance_count = 0

    for athlete in athletes:
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
            VALUES (
                %(athlete_id)s,
                %(display_name)s,
                %(profile_name)s,
                %(runner_name)s,
                %(club)s,
                %(gender)s,
                %(age)s,
                %(age_group)s,
                %(source_url)s,
                %(fetched_at)s,
                %(best_headers)s,
                %(performance_count)s,
                %(section_count)s,
                %(first_year)s,
                %(last_year)s
            )
            """,
            {
                "athlete_id": athlete.athlete_id,
                "display_name": athlete.display_name,
                "profile_name": athlete.profile_name,
                "runner_name": athlete.runner_name,
                "club": athlete.club,
                "gender": athlete.gender,
                "age": athlete.age,
                "age_group": athlete.age_group,
                "source_url": athlete.source_url,
                "fetched_at": athlete.fetched_at,
                "best_headers": Jsonb(athlete.best_headers),
                "performance_count": athlete.performance_count,
                "section_count": athlete.section_count,
                "first_year": athlete.first_year,
                "last_year": athlete.last_year,
            },
        )
        athlete_count += 1

        for row_order, cells in enumerate(athlete.best_rows):
            conn.execute(
                """
                INSERT INTO athlete_best_performance_rows (athlete_id, row_order, cells)
                VALUES (%s, %s, %s)
                """,
                (athlete.athlete_id, row_order, Jsonb(cells)),
            )

        for section_order, section in enumerate(athlete.performances):
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
                    athlete.athlete_id,
                    "powerof10_cache",
                    section_order,
                    section.get("title") or f"Section {section_order + 1}",
                    athlete.last_year if section_order == 0 else None,
                    Jsonb(section.get("columns") or []),
                    Jsonb({}),
                ),
            ).fetchone()
            section_id = section_row["id"]
            section_count += 1

            year = None
            title = section.get("title") or ""
            if title[:4].isdigit():
                year = int(title[:4])
                conn.execute(
                    "UPDATE athlete_performance_sections SET year = %s WHERE id = %s",
                    (year, section_id),
                )

            for row_order, result in enumerate(section.get("rows", [])):
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
                        athlete.athlete_id,
                        section_id,
                        "powerof10_cache",
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
                performance_count += 1

    return athlete_count, section_count, performance_count


def main() -> None:
    args = parse_args()
    sql_path = Path(args.sql)

    ensure_schema()
    profiles, runners, cache_rows = load_dump(sql_path)
    athletes = resolve_athletes(profiles, runners, cache_rows)

    with get_conn() as conn:
        reset_tables(conn)
        athlete_count, section_count, performance_count = import_athletes(conn, athletes)
        conn.commit()

    print(f"Imported {athlete_count} athletes")
    print(f"Imported {section_count} sections")
    print(f"Imported {performance_count} performances")


if __name__ == "__main__":
    main()
