from __future__ import annotations

import os
import re
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

from flask import Flask, abort, redirect, render_template, request, send_from_directory, url_for

from .db import get_conn
from .rankings_support import (
    age_group_label,
    area_label,
    event_aliases,
    event_label,
    normalize_key,
    parse_mark,
    ranking_direction,
    section_age_group,
    sex_label,
)


ROOT = Path(__file__).resolve().parent.parent
ASSET_ROOT = ROOT / "thepowerof10.info"
TOOLBAR_MIN_YEAR = 2006
TOOLBAR_MAX_YEAR = 2026
TOOLBAR_AREA_IDS = {"0", "61", "62", "63", "64", "65", "66", "67", "68", "69", "91", "92", "93", "94"}
TOOLBAR_SEXES = {"M", "W", "X"}
TOOLBAR_AGE_GROUPS = {"ALL", "U20", "U17", "U15", "U13", "DIS"}
RESULTS_PAGE_SIZE = 250
ALL_TIME_RANKING_LIMIT = 500
TRUEPB_SB_OVERLAY_YEAR = 2026
RESULTS_DATE_INPUT_FORMATS = ("%Y-%m-%d", "%d %b %Y", "%d %b %y", "%d-%b-%Y", "%d-%b-%y")
RESULTS_DATE_SQL_TEMPLATE = """
COALESCE(
    {alias}.result_date,
    CASE
        WHEN {alias}.date_text ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$' THEN TO_DATE({alias}.date_text, 'YYYY-MM-DD')
        WHEN {alias}.date_text ~ '^\\d{{1,2}} [A-Za-z]{{3}} \\d{{4}}$' THEN TO_DATE({alias}.date_text, 'DD Mon YYYY')
        WHEN {alias}.date_text ~ '^\\d{{1,2}} [A-Za-z]{{3}} \\d{{2}}$' THEN TO_DATE({alias}.date_text, 'DD Mon YY')
        ELSE NULL
    END
)
"""

app = Flask(__name__, template_folder=str(Path(__file__).resolve().parent / "templates"))


@app.route("/healthz")
def healthz():
    return {"status": "ok"}


@app.route("/favicon.ico")
def favicon():
    return send_from_directory(ROOT, "favicon.ico")


@app.route("/thepowerof10.info/<path:asset_path>")
def legacy_assets(asset_path: str):
    return send_from_directory(ASSET_ROOT, asset_path)


def summary_counts(conn):
    return conn.execute(
        """
        SELECT
            COUNT(*) AS athlete_count,
            COALESCE(SUM(performance_count), 0) AS performance_count
        FROM athletes
        """
    ).fetchone()


def load_athlete_rows(
    conn,
    *,
    q: str = "",
    surname: str = "",
    first_name: str = "",
    club: str = "",
):
    sql = """
        SELECT
            athlete_id,
            profile_name,
            runner_name,
            display_name,
            club,
            gender,
            age_group,
            performance_count,
            first_year,
            last_year
        FROM athletes
    """
    params: list[object] = []
    conditions: list[str] = []

    def add_name_filter(value: str) -> None:
        like = f"%{value}%"
        conditions.append(
            """
            (
                COALESCE(display_name, '') ILIKE %s OR
                COALESCE(profile_name, '') ILIKE %s OR
                COALESCE(runner_name, '') ILIKE %s
            )
            """
        )
        params.extend([like, like, like])

    if q:
        like = f"%{q}%"
        conditions.append(
            """
            (
                COALESCE(display_name, '') ILIKE %s OR
                COALESCE(profile_name, '') ILIKE %s OR
                COALESCE(runner_name, '') ILIKE %s OR
                COALESCE(club, '') ILIKE %s OR
                CAST(athlete_id AS TEXT) ILIKE %s
            )
            """
        )
        params.extend([like, like, like, like, like])

    if surname:
        add_name_filter(surname)

    if first_name:
        add_name_filter(first_name)

    if club:
        conditions.append("COALESCE(club, '') ILIKE %s")
        params.append(f"%{club}%")

    if conditions:
        sql += " WHERE " + " AND ".join(conditions)

    sql += " ORDER BY LOWER(display_name), athlete_id"
    return conn.execute(sql, params).fetchall()


def ranking_years(conn) -> list[int]:
    row = conn.execute(
        """
        SELECT
            MIN(year) AS min_year,
            MAX(year) AS max_year
        FROM athlete_performance_sections
        WHERE year IS NOT NULL
        """
    ).fetchone()
    min_year = row["min_year"] or TOOLBAR_MIN_YEAR
    max_year = row["max_year"] or date.today().year
    return list(range(max_year, min_year - 1, -1))


def toolbar_year(year: int | None) -> int:
    if year is None:
        return 0
    return max(TOOLBAR_MIN_YEAR, min(TOOLBAR_MAX_YEAR, year))


def effective_result_date_sql(alias: str = "p", section_alias: str | None = None) -> str:
    base = RESULTS_DATE_SQL_TEMPLATE.format(alias=alias).strip()
    if section_alias is None:
        return base
    return f"""
COALESCE(
    {base},
    CASE
        WHEN {section_alias}.year IS NOT NULL THEN MAKE_DATE({section_alias}.year, 1, 1)
        ELSE NULL
    END
)
""".strip()


def parse_results_date(value: str | None) -> date | None:
    text = (value or "").strip()
    if not text:
        return None
    for fmt in RESULTS_DATE_INPUT_FORMATS:
        try:
            if fmt == "%Y-%m-%d":
                return date.fromisoformat(text)
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def results_date_label(day: date | None) -> str:
    if day is None:
        return ""
    return f"{day.strftime('%a')} {day.day} {day.strftime('%b %Y')}"


def results_detail_date_label(day: date | None) -> str:
    if day is None:
        return ""
    return f"{day.day} {day.strftime('%b %Y')}"


def results_search_pattern(value: str | None) -> str | None:
    text = (value or "").strip()
    if not text:
        return None
    pattern = text.replace("*", "%")
    if "%" not in pattern and "_" not in pattern:
        pattern = f"%{pattern}%"
    return pattern


def normalize_results_key(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def local_results_event_url(
    *,
    meeting: str | None,
    venue: str | None,
    result_date: date | None,
    date_text: str | None,
    event: str | None,
) -> str | None:
    meeting_name = (meeting or "").strip()
    venue_name = (venue or "").strip()
    event_name = (event or "").strip()
    meeting_day = result_date or parse_results_date(date_text)

    if meeting_day is None or not (meeting_name or venue_name):
        return None

    params: dict[str, str] = {
        "date": meeting_day.isoformat(),
    }
    if meeting_name:
        params["meeting"] = meeting_name
    if venue_name:
        params["venue"] = venue_name
    if event_name:
        params["event"] = event_name
    return url_for("results_detail", **params)


def result_position_value(pos: str | None) -> tuple[int, str]:
    text = (pos or "").strip()
    if not text:
        return (10**9, "")
    match = re.search(r"\d+", text)
    if not match:
        return (10**9, text.lower())
    return (int(match.group(0)), text.lower())


def result_sex_label(gender: str | None) -> str:
    normalized = (gender or "").strip().lower()
    if normalized == "male":
        return "M"
    if normalized == "female":
        return "W"
    return ""


def meeting_row_sort_key(row: dict, direction: str) -> tuple[tuple[int, str], float, str]:
    parsed_mark = parse_mark(row["perf"])
    if parsed_mark is None:
        mark_value = 10**9 if direction == "lower" else 10**9
    elif direction == "lower":
        mark_value = parsed_mark
    else:
        mark_value = -parsed_mark
    return (
        result_position_value(row["pos"]),
        mark_value,
        (row["display_name"] or "").lower(),
    )


def is_better_mark(candidate: dict, existing: dict | None, direction: str) -> bool:
    if existing is None:
        return True
    if direction == "higher":
        if candidate["sort_value"] > existing["sort_value"]:
            return True
        if candidate["sort_value"] < existing["sort_value"]:
            return False
    else:
        if candidate["sort_value"] < existing["sort_value"]:
            return True
        if candidate["sort_value"] > existing["sort_value"]:
            return False

    candidate_date = candidate["result_date"] or date.min
    existing_date = existing["result_date"] or date.min
    return candidate_date >= existing_date


def load_results_years(conn) -> list[int]:
    effective_date = effective_result_date_sql("p", "s")
    rows = conn.execute(
        f"""
        SELECT DISTINCT EXTRACT(YEAR FROM {effective_date})::int AS year
        FROM athlete_performances p
        JOIN athlete_performance_sections s ON s.id = p.section_id
        WHERE {effective_date} IS NOT NULL
        ORDER BY year DESC
        """
    ).fetchall()
    return [row["year"] for row in rows if row["year"] is not None]


def normalize_best_table_event(label: str | None) -> str:
    key = normalize_key(label)
    if not key:
        return ""

    key = key.replace("ROAD", "").replace("TRACK", "")
    alias_map = {
        "MARATHON": "MAR",
        "MAR": "MAR",
        "HALFMARATHON": "HM",
        "HM": "HM",
        "PARKRUN": "PARKRUN",
        "MILE": "MILE",
        "5K": "5K",
        "10K": "10K",
    }
    if key in alias_map:
        return alias_map[key]

    if key.endswith("M") and key[:-1].isdigit():
        return key[:-1]

    return key


def load_truepb_year_bests(conn, athlete_id: int, year: int) -> dict[str, dict]:
    effective_date = effective_result_date_sql("p", "s")
    rows = conn.execute(
        f"""
        SELECT
            p.event,
            p.perf,
            {effective_date} AS result_date
        FROM athlete_performances p
        JOIN athlete_performance_sections s ON s.id = p.section_id
        WHERE
            p.athlete_id = %s AND
            p.source_kind = 'truepb_results' AND
            COALESCE(BTRIM(p.event), '') <> '' AND
            COALESCE(BTRIM(p.perf), '') <> '' AND
            EXTRACT(YEAR FROM {effective_date}) = %s
        """,
        (athlete_id, year),
    ).fetchall()

    bests: dict[str, dict] = {}
    for row in rows:
        event_key = normalize_best_table_event(row["event"])
        sort_value = parse_mark(row["perf"])
        if not event_key or sort_value is None:
            continue

        candidate = {
            "perf": row["perf"],
            "sort_value": sort_value,
            "result_date": row["result_date"],
        }
        if is_better_mark(candidate, bests.get(event_key), ranking_direction(row["event"] or "")):
            bests[event_key] = candidate

    return bests


def overlay_best_rows_for_year(
    headers: list[str] | None,
    best_rows: list[dict],
    year_bests: dict[str, dict],
    *,
    year: int,
) -> tuple[list[str], list[dict]]:
    source_headers = [str(header) for header in (headers or [])]
    if not source_headers:
        return source_headers, best_rows

    overlay_label = str(year)
    try:
        event_idx = source_headers.index("Event")
        pb_idx = source_headers.index("PB")
    except ValueError:
        return source_headers, best_rows

    target_headers = list(source_headers)
    if overlay_label in target_headers:
        overlay_idx = target_headers.index(overlay_label)
    else:
        overlay_idx = pb_idx + 1
        target_headers.insert(overlay_idx, overlay_label)

    updated_rows: list[dict] = []
    for row in best_rows:
        cells = list(row["cells"] or [])
        event_label = cells[event_idx] if event_idx < len(cells) else ""
        event_key = normalize_best_table_event(event_label)
        year_best = year_bests.get(event_key)
        overlay_value = year_best["perf"] if year_best else ""

        if overlay_idx < len(cells):
            cells[overlay_idx] = overlay_value
        else:
            while len(cells) < overlay_idx:
                cells.append("")
            cells.insert(overlay_idx, overlay_value)

        if year_best and pb_idx < len(cells):
            existing_pb_value = cells[pb_idx]
            existing_sort_value = parse_mark(existing_pb_value)
            existing_pb = (
                {
                    "perf": existing_pb_value,
                    "sort_value": existing_sort_value,
                    "result_date": None,
                }
                if existing_sort_value is not None
                else None
            )
            if is_better_mark(year_best, existing_pb, ranking_direction(event_label or "")):
                cells[pb_idx] = year_best["perf"]

        updated_rows.append(
            {
                **row,
                "cells": cells,
            }
        )

    return target_headers, updated_rows


def load_generated_best_table(conn, athlete_id: int) -> tuple[list[str], list[dict]]:
    effective_date = effective_result_date_sql("p", "s")
    rows = conn.execute(
        f"""
        SELECT
            p.source_kind,
            p.event,
            p.perf,
            p.extra,
            {effective_date} AS result_date,
            EXTRACT(YEAR FROM {effective_date})::int AS result_year
        FROM athlete_performances p
        JOIN athlete_performance_sections s ON s.id = p.section_id
        WHERE
            p.athlete_id = %s AND
            COALESCE(BTRIM(p.event), '') <> '' AND
            COALESCE(BTRIM(p.perf), '') <> ''
        ORDER BY result_year DESC NULLS LAST, p.row_order ASC
        """,
        (athlete_id,),
    ).fetchall()

    event_bests: dict[str, dict] = {}
    years: set[int] = set()
    for row in rows:
        extra = row["extra"] if isinstance(row["extra"], dict) else {}
        event_text = (row["event"] or "").strip()
        normalized_event = normalize_key(event_text)
        raw_is_relay = bool(extra.get("is_relay")) or "RELAY" in normalized_event or "SHORTLEG" in normalized_event or "LONGLEG" in normalized_event
        raw_is_xc = bool(extra.get("is_xc")) or "XC" in normalized_event
        raw_is_road = bool(extra.get("is_road"))
        raw_is_track = bool(extra.get("is_track"))
        has_surface_flags = any(key in extra for key in ("is_road", "is_track", "is_xc", "is_relay"))

        if row["source_kind"] == "truepb_results":
            if raw_is_relay or raw_is_xc:
                continue
            if has_surface_flags and not (raw_is_road or raw_is_track):
                continue

        if re.fullmatch(r"\d+(?:\.\d+)?(?:K|M)L", normalized_event):
            continue

        event_key = normalize_best_table_event(row["event"])
        sort_value = parse_mark(row["perf"])
        if not event_key or sort_value is None:
            continue

        result_year = row["result_year"]
        if result_year is not None:
            years.add(result_year)

        direction = ranking_direction(row["event"] or "")
        candidate = {
            "perf": row["perf"],
            "sort_value": sort_value,
            "result_date": row["result_date"],
        }
        event_best = event_bests.setdefault(
            event_key,
            {
                "label": row["event"],
                "direction": direction,
                "pb": None,
                "years": {},
            },
        )

        if is_better_mark(candidate, event_best["pb"], event_best["direction"]):
            event_best["pb"] = candidate
            event_best["label"] = row["event"]

        if result_year is not None and is_better_mark(
            candidate,
            event_best["years"].get(result_year),
            event_best["direction"],
        ):
            event_best["years"][result_year] = candidate

    if not event_bests:
        return [], []

    ordered_years = sorted(years, reverse=True)
    headers = ["Event", "PB", *[str(year) for year in ordered_years]]
    best_rows = []
    for row_order, event_key in enumerate(
        sorted(event_bests, key=lambda item: normalize_best_table_event(event_bests[item]["label"]))
    ):
        event_best = event_bests[event_key]
        cells = [
            event_best["label"],
            event_best["pb"]["perf"] if event_best["pb"] else "",
            *[
                event_best["years"].get(year, {}).get("perf", "")
                for year in ordered_years
            ],
        ]
        best_rows.append({"row_order": row_order, "cells": cells})

    return headers, best_rows


def load_results_events(conn) -> list[str]:
    rows = conn.execute(
        """
        SELECT event
        FROM (
            SELECT DISTINCT event
            FROM athlete_performances
            WHERE COALESCE(BTRIM(event), '') <> ''
        ) event_options
        ORDER BY LOWER(event)
        """
    ).fetchall()
    return [row["event"] for row in rows]


def load_result_meetings(
    conn,
    *,
    event: str = "",
    meeting: str = "",
    venue: str = "",
    date_from: date | None = None,
    date_to: date | None = None,
    year: int | None = None,
    limit: int = RESULTS_PAGE_SIZE,
    offset: int = 0,
):
    effective_date = effective_result_date_sql("p", "s")
    meeting_key_expr = "LOWER(BTRIM(COALESCE(p.meeting, '')))"
    venue_key_expr = "LOWER(BTRIM(COALESCE(p.venue, '')))"
    sql = f"""
        SELECT
            {meeting_key_expr} AS meeting_key,
            {venue_key_expr} AS venue_key,
            {effective_date} AS meeting_date,
            MIN(NULLIF(BTRIM(COALESCE(p.meeting, '')), '')) AS meeting_display,
            MIN(NULLIF(BTRIM(COALESCE(p.venue, '')), '')) AS venue_display,
            COUNT(*) AS performance_count,
            COUNT(DISTINCT p.athlete_id) AS athlete_count,
            COUNT(DISTINCT NULLIF(BTRIM(COALESCE(p.event, '')), '')) AS event_count
        FROM athlete_performances p
        JOIN athlete_performance_sections s ON s.id = p.section_id
        WHERE
            (
                COALESCE(BTRIM(p.meeting), '') <> '' OR
                COALESCE(BTRIM(p.venue), '') <> ''
            ) AND
            {effective_date} IS NOT NULL
    """
    params: list[object] = []

    if event:
        sql += " AND p.event = %s"
        params.append(event)

    meeting_pattern = results_search_pattern(meeting)
    if meeting_pattern:
        sql += " AND COALESCE(p.meeting, '') ILIKE %s"
        params.append(meeting_pattern)

    venue_pattern = results_search_pattern(venue)
    if venue_pattern:
        sql += " AND COALESCE(p.venue, '') ILIKE %s"
        params.append(venue_pattern)

    if date_from is not None:
        sql += f" AND {effective_date} >= %s"
        params.append(date_from)

    if date_to is not None:
        sql += f" AND {effective_date} <= %s"
        params.append(date_to)

    if year is not None:
        sql += f" AND EXTRACT(YEAR FROM {effective_date}) = %s"
        params.append(year)

    sql += """
        GROUP BY
    """
    sql += f"""
            {meeting_key_expr},
            {venue_key_expr},
            {effective_date}
    """
    sql += """
        ORDER BY meeting_date DESC, meeting_key, venue_key
        LIMIT %s
        OFFSET %s
    """
    params.append(limit)
    params.append(offset)
    return conn.execute(sql, params).fetchall()


def count_result_meetings(
    conn,
    *,
    event: str = "",
    meeting: str = "",
    venue: str = "",
    date_from: date | None = None,
    date_to: date | None = None,
    year: int | None = None,
) -> int:
    effective_date = effective_result_date_sql("p", "s")
    meeting_key_expr = "LOWER(BTRIM(COALESCE(p.meeting, '')))"
    venue_key_expr = "LOWER(BTRIM(COALESCE(p.venue, '')))"
    sql = f"""
        SELECT COUNT(*) AS total
        FROM (
            SELECT
                {meeting_key_expr},
                {venue_key_expr},
                {effective_date}
            FROM athlete_performances p
            JOIN athlete_performance_sections s ON s.id = p.section_id
            WHERE
                (
                    COALESCE(BTRIM(p.meeting), '') <> '' OR
                    COALESCE(BTRIM(p.venue), '') <> ''
                ) AND
                {effective_date} IS NOT NULL
    """
    params: list[object] = []

    if event:
        sql += " AND p.event = %s"
        params.append(event)

    meeting_pattern = results_search_pattern(meeting)
    if meeting_pattern:
        sql += " AND COALESCE(p.meeting, '') ILIKE %s"
        params.append(meeting_pattern)

    venue_pattern = results_search_pattern(venue)
    if venue_pattern:
        sql += " AND COALESCE(p.venue, '') ILIKE %s"
        params.append(venue_pattern)

    if date_from is not None:
        sql += f" AND {effective_date} >= %s"
        params.append(date_from)

    if date_to is not None:
        sql += f" AND {effective_date} <= %s"
        params.append(date_to)

    if year is not None:
        sql += f" AND EXTRACT(YEAR FROM {effective_date}) = %s"
        params.append(year)

    sql += """
            GROUP BY 1, 2, 3
        ) grouped_meetings
    """
    row = conn.execute(sql, params).fetchone()
    return int(row["total"] or 0)


def load_meeting_rows(
    conn,
    *,
    meeting_key: str,
    venue_key: str,
    meeting_date: date,
    selected_event: str = "",
):
    effective_date = effective_result_date_sql("p", "s")
    sql = f"""
        SELECT
            p.id,
            p.athlete_id,
            p.source_kind,
            p.event,
            p.perf,
            p.pos,
            p.venue,
            p.venue_url,
            p.meeting,
            p.date_text,
            {effective_date} AS result_date,
            s.year AS section_year,
            a.display_name,
            a.club,
            a.gender,
            a.age,
            a.age_group
        FROM athlete_performances p
        JOIN athlete_performance_sections s ON s.id = p.section_id
        JOIN athletes a ON a.athlete_id = p.athlete_id
        WHERE
            LOWER(BTRIM(COALESCE(p.meeting, ''))) = %s AND
            LOWER(BTRIM(COALESCE(p.venue, ''))) = %s AND
            (
                COALESCE(BTRIM(p.meeting), '') <> '' OR
                COALESCE(BTRIM(p.venue), '') <> ''
            ) AND
            {effective_date} = %s
    """
    params: list[object] = [meeting_key, venue_key, meeting_date]

    if selected_event:
        sql += " AND p.event = %s"
        params.append(selected_event)

    sql += """
        ORDER BY
            LOWER(COALESCE(p.event, '')),
            CASE
                WHEN COALESCE(p.pos, '') ~ '^[0-9]+' THEN CAST(SUBSTRING(p.pos FROM '^[0-9]+') AS INTEGER)
                ELSE 2147483647
            END,
            p.row_order,
            LOWER(a.display_name)
    """
    return conn.execute(sql, params).fetchall()


def load_event_bests(conn, meeting_rows: list[dict]) -> tuple[dict[tuple[int, str], dict], dict[tuple[int, str, int], dict]]:
    athlete_ids = sorted({row["athlete_id"] for row in meeting_rows})
    events = sorted({row["event"] for row in meeting_rows if row["event"]})
    if not athlete_ids or not events:
        return {}, {}

    effective_date = effective_result_date_sql("p")
    history_rows = conn.execute(
        f"""
        SELECT
            p.athlete_id,
            p.event,
            p.perf,
            {effective_date} AS result_date,
            s.year AS section_year
        FROM athlete_performances p
        JOIN athlete_performance_sections s ON s.id = p.section_id
        WHERE
            p.athlete_id = ANY(%s) AND
            p.event = ANY(%s) AND
            COALESCE(BTRIM(p.perf), '') <> ''
        """,
        (athlete_ids, events),
    ).fetchall()

    best_overall: dict[tuple[int, str], dict] = {}
    best_by_year: dict[tuple[int, str, int], dict] = {}

    for row in history_rows:
        sort_value = parse_mark(row["perf"])
        if sort_value is None:
            continue
        event_name = row["event"] or ""
        direction = ranking_direction(event_name)
        candidate = {
            "perf": row["perf"],
            "sort_value": sort_value,
            "result_date": row["result_date"],
        }
        overall_key = (row["athlete_id"], event_name)
        if is_better_mark(candidate, best_overall.get(overall_key), direction):
            best_overall[overall_key] = candidate

        result_year = row["result_date"].year if row["result_date"] else row["section_year"]
        if result_year is None:
            continue
        year_key = (row["athlete_id"], event_name, result_year)
        if is_better_mark(candidate, best_by_year.get(year_key), direction):
            best_by_year[year_key] = candidate

    return best_overall, best_by_year


def build_meeting_view(conn, *, meeting_key: str, venue_key: str, meeting_date: date, selected_event: str = ""):
    rows = load_meeting_rows(
        conn,
        meeting_key=meeting_key,
        venue_key=venue_key,
        meeting_date=meeting_date,
        selected_event=selected_event,
    )
    if not rows:
        return None

    best_overall, best_by_year = load_event_bests(conn, rows)
    rows_by_event: dict[str, list[dict]] = defaultdict(list)

    for row in rows:
        event_name = row["event"] or "Other"
        row_year = row["result_date"].year if row["result_date"] else row["section_year"]
        row_mark = parse_mark(row["perf"])
        overall = best_overall.get((row["athlete_id"], row["event"] or ""))
        seasonal = best_by_year.get((row["athlete_id"], row["event"] or "", row_year)) if row_year is not None else None

        flag = ""
        if row_mark is not None:
            is_pb = overall is not None and abs(row_mark - overall["sort_value"]) <= 1e-9
            is_sb = seasonal is not None and abs(row_mark - seasonal["sort_value"]) <= 1e-9
            if is_pb and is_sb:
                flag = "SB/PB"
            elif is_pb:
                flag = "PB"
            elif is_sb:
                flag = "SB"

        rows_by_event[event_name].append(
            {
                **row,
                "sex_label": result_sex_label(row["gender"]),
                "flag": flag,
                "sb": seasonal["perf"] if seasonal else "",
                "pb": overall["perf"] if overall else "",
            }
        )

    event_groups: list[dict] = []
    for event_name in sorted(rows_by_event, key=lambda value: normalize_key(value or "")):
        direction = ranking_direction(event_name)
        sorted_rows = sorted(rows_by_event[event_name], key=lambda row: meeting_row_sort_key(row, direction))
        event_groups.append(
            {
                "event": event_name,
                "anchor": re.sub(r"[^a-z0-9]+", "-", event_name.lower()).strip("-") or "other",
                "rows": sorted_rows,
            }
        )

    first_row = rows[0]
    meeting_name = (first_row["meeting"] or "").strip()
    venue_name = (first_row["venue"] or "").strip()
    return {
        "meeting_name": meeting_name or venue_name,
        "venue_name": venue_name if meeting_name else "",
        "meeting_date": meeting_date,
        "date_label": results_detail_date_label(meeting_date),
        "event_groups": event_groups,
        "row_count": len(rows),
        "athlete_count": len({row["athlete_id"] for row in rows}),
    }


def load_ranking_candidates(
    conn,
    *,
    sex: str,
    year: int | None,
    event_codes: set[str] | None = None,
    age_group: str = "ALL",
):
    sql = """
        SELECT
            a.athlete_id,
            a.display_name,
            a.club,
            a.gender,
            s.title AS section_title,
            s.year AS section_year,
            p.source_kind,
            p.event,
            p.perf,
            p.pos,
            p.venue,
            p.venue_url,
            p.meeting,
            p.date_text,
            p.result_date
        FROM athlete_performances p
        JOIN athlete_performance_sections s ON s.id = p.section_id
        JOIN athletes a ON a.athlete_id = p.athlete_id
        WHERE
            COALESCE(p.event, '') <> '' AND
            COALESCE(p.perf, '') <> ''
    """
    params: list[object] = []

    if event_codes:
        normalized_events = sorted({code for code in event_codes if code})
        placeholders = ", ".join(["%s"] * len(normalized_events))
        sql += f"""
            AND UPPER(REGEXP_REPLACE(COALESCE(p.event, ''), '[^A-Z0-9]+', '', 'g')) IN ({placeholders})
        """
        params.extend(normalized_events)

    if sex == "M":
        sql += " AND COALESCE(a.gender, '') = 'Male'"
    elif sex == "W":
        sql += " AND COALESCE(a.gender, '') = 'Female'"
    elif sex == "X":
        return []

    if year is not None:
        sql += " AND s.year = %s"
        params.append(year)

    if age_group != "ALL":
        sql += " AND s.title ~ %s"
        params.append(rf"^\s*\d{{4}}\s+{re.escape(age_group)}\b")

    sql += " ORDER BY a.athlete_id, p.result_date DESC NULLS LAST, p.id DESC"
    return conn.execute(sql, params).fetchall()


def ranking_sort_key(row: dict, direction: str):
    result_date = row["result_date"] or date.min
    name = (row["display_name"] or "").lower()
    if direction == "higher":
        return (-row["sort_value"], -result_date.toordinal(), name, row["athlete_id"])
    return (row["sort_value"], -result_date.toordinal(), name, row["athlete_id"])


def ranking_row_is_better(candidate: dict, existing: dict, direction: str) -> bool:
    if direction == "higher":
        if candidate["sort_value"] > existing["sort_value"]:
            return True
        if candidate["sort_value"] < existing["sort_value"]:
            return False
    else:
        if candidate["sort_value"] < existing["sort_value"]:
            return True
        if candidate["sort_value"] > existing["sort_value"]:
            return False

    candidate_date = candidate["result_date"] or date.min
    existing_date = existing["result_date"] or date.min
    return candidate_date > existing_date


def load_rankings(
    conn,
    *,
    event_code: str,
    age_group: str,
    sex: str,
    selected_year: int | None,
    all_time: bool,
    area_id: str,
    class_code: str,
    indoor_year: int | None,
):
    notes: list[str] = []
    request_active = bool(event_code)
    selected_year = None if all_time else selected_year
    event_name = event_label(event_code) if event_code else ""

    if not event_code:
        return {
            "request_active": False,
            "results": None,
            "notes": notes,
        }

    if area_id != "0":
        notes.append(
            f"Region/Nation filtering for {area_label(area_id)} is not in the imported data yet, so these rankings use the full database."
        )

    if indoor_year is not None:
        notes.append(
            "Indoor-only ranking splits are not stored separately in this import, so the requested year is shown as a combined view."
        )

    if class_code:
        notes.append(
            "Disability class filtering is not stored in the imported athlete results yet."
        )

    if sex == "X":
        notes.append(
            "Mixed rankings are not derivable from the current athlete-level result data."
        )
        return {
            "request_active": request_active,
            "results": {
                "rows": [],
                "event_code": event_code,
                "event_label": event_name,
                "age_group": age_group,
                "age_group_label": age_group_label(age_group),
                "sex": sex,
                "sex_label": sex_label(sex),
                "scope_label": "All Time" if all_time else str(selected_year or ""),
                "area_label": area_label(area_id),
                "athlete_count": 0,
                "direction": ranking_direction(event_code),
            },
            "notes": notes,
        }

    if age_group == "DIS":
        notes.append(
            "Disability rankings need classification fields that are not part of the imported cache yet."
        )
        return {
            "request_active": request_active,
            "results": {
                "rows": [],
                "event_code": event_code,
                "event_label": event_name,
                "age_group": age_group,
                "age_group_label": age_group_label(age_group),
                "sex": sex,
                "sex_label": sex_label(sex),
                "scope_label": "All Time" if all_time else str(selected_year or ""),
                "area_label": area_label(area_id),
                "athlete_count": 0,
                "direction": ranking_direction(event_code),
            },
            "notes": notes,
        }

    aliases = event_aliases(event_code)
    direction = ranking_direction(event_code)
    best_by_athlete: dict[int, dict] = {}

    for row in load_ranking_candidates(
        conn,
        sex=sex,
        year=selected_year,
        event_codes=aliases,
        age_group=age_group,
    ):
        sort_value = parse_mark(row["perf"])
        if sort_value is None:
            continue

        candidate = {
            **row,
            "sort_value": sort_value,
        }
        existing = best_by_athlete.get(row["athlete_id"])
        if existing is None or ranking_row_is_better(candidate, existing, direction):
            best_by_athlete[row["athlete_id"]] = candidate

    ranked_rows = sorted(best_by_athlete.values(), key=lambda row: ranking_sort_key(row, direction))
    total_ranked_rows = len(ranked_rows)
    if all_time and total_ranked_rows > ALL_TIME_RANKING_LIMIT:
        ranked_rows = ranked_rows[:ALL_TIME_RANKING_LIMIT]
        notes.append(
            f"All Time rankings are limited to the top {ALL_TIME_RANKING_LIMIT} rows for performance."
        )
    last_value: float | None = None
    current_rank = 0
    rendered_rows: list[dict] = []
    for index, row in enumerate(ranked_rows, start=1):
        if last_value is None or abs(row["sort_value"] - last_value) > 1e-9:
            current_rank = index
            last_value = row["sort_value"]
        rendered_rows.append(
            {
                **row,
                "rank": current_rank,
            }
        )

    if not rendered_rows:
        notes.append("No results matched this combination of event, sex, age group, and year.")

    return {
        "request_active": request_active,
        "results": {
            "rows": rendered_rows,
            "event_code": event_code,
            "event_label": event_name,
            "age_group": age_group,
            "age_group_label": age_group_label(age_group),
            "sex": sex,
            "sex_label": sex_label(sex),
            "scope_label": "All Time" if all_time else str(selected_year or ""),
            "area_label": area_label(area_id),
            "athlete_count": len(rendered_rows),
            "total_athlete_count": total_ranked_rows,
            "direction": direction,
        },
        "notes": notes,
    }


@app.route("/")
def home():
    surname = (request.args.get("surname") or "").strip()
    first_name = (request.args.get("first_name") or "").strip()
    club = (request.args.get("club") or "").strip()
    has_search = any([surname, first_name, club])

    with get_conn() as conn:
        summary = summary_counts(conn)
        athletes = load_athlete_rows(
            conn,
            surname=surname,
            first_name=first_name,
            club=club,
        ) if has_search else []

    return render_template(
        "home.html",
        athletes=athletes,
        surname=surname,
        first_name=first_name,
        club=club,
        has_search=has_search,
        athlete_count=summary["athlete_count"],
        performance_count=summary["performance_count"],
    )


@app.route("/about")
def about():
    return render_template("about.html")


@app.route("/rankings")
@app.route("/rankings/")
def rankings():
    with get_conn() as conn:
        years = ranking_years(conn)

    latest_year = years[0] if years else TOOLBAR_MAX_YEAR
    return render_template(
        "rankings.html",
        ranking_current_year=toolbar_year(latest_year),
        ranking_current_area="0",
        ranking_current_sex="M",
        ranking_current_agegroup="ALL",
        ranking_current_event_code="",
        ranking_notes=[],
        ranking_results=None,
        ranking_request_active=False,
    )


@app.route("/rankings/rankinglist.aspx")
def rankings_list():
    event_code = (request.args.get("event") or "").strip()
    age_group = (request.args.get("agegroup") or "ALL").strip().upper()
    sex = (request.args.get("sex") or "M").strip().upper()
    area_id = (request.args.get("areaid") or "0").strip()
    class_code = (request.args.get("class") or "").strip()
    indoor_year = request.args.get("iyear", type=int)
    all_time = (request.args.get("alltime") or "").strip().lower() == "y"
    selected_year = None if all_time else (
        request.args.get("year", type=int) or indoor_year
    )

    with get_conn() as conn:
        years = ranking_years(conn)
        latest_year = years[0] if years else TOOLBAR_MAX_YEAR
        if selected_year is None and not all_time:
            selected_year = latest_year
        ranking_view = load_rankings(
            conn,
            event_code=event_code,
            age_group=age_group,
            sex=sex,
            selected_year=selected_year,
            all_time=all_time,
            area_id=area_id,
            class_code=class_code,
            indoor_year=indoor_year,
        )

    toolbar_area = area_id if area_id in TOOLBAR_AREA_IDS else "0"
    toolbar_sex = sex if sex in TOOLBAR_SEXES else "M"
    toolbar_age_group = age_group if age_group in TOOLBAR_AGE_GROUPS else "ALL"

    return render_template(
        "rankings.html",
        ranking_current_year=0 if all_time else toolbar_year(selected_year),
        ranking_current_area=toolbar_area,
        ranking_current_sex=toolbar_sex,
        ranking_current_agegroup=toolbar_age_group,
        ranking_current_event_code=event_code,
        ranking_notes=ranking_view["notes"],
        ranking_results=ranking_view["results"],
        ranking_request_active=ranking_view["request_active"],
    )


@app.route("/rankings/disabilityrankinglistrequest.aspx")
def disability_rankings_redirect():
    return redirect(
        "https://thepowerof10.info/rankings/disabilityrankinglistrequest.aspx",
        code=302,
    )


@app.route("/results")
@app.route("/results/")
@app.route("/results/resultslookup.aspx")
def results_lookup():
    selected_event = (request.args.get("event") or "").strip()
    meeting = (request.args.get("meeting") or "").strip()
    venue = (request.args.get("venue") or "").strip()
    date_from_text = (request.args.get("date_from") or "").strip()
    date_to_text = (request.args.get("date_to") or "").strip()
    selected_year = request.args.get("year", type=int) or None
    page = max(request.args.get("page", type=int) or 1, 1)
    has_filters = any([selected_event, meeting, venue, date_from_text, date_to_text, selected_year])

    date_from = parse_results_date(date_from_text)
    date_to = parse_results_date(date_to_text)

    with get_conn() as conn:
        event_options = load_results_events(conn)
        year_options = load_results_years(conn)
        total_meetings = count_result_meetings(
            conn,
            event=selected_event,
            meeting=meeting,
            venue=venue,
            date_from=date_from,
            date_to=date_to,
            year=selected_year,
        )
        total_pages = max((total_meetings + RESULTS_PAGE_SIZE - 1) // RESULTS_PAGE_SIZE, 1)
        page = min(page, total_pages)
        meetings = load_result_meetings(
            conn,
            event=selected_event,
            meeting=meeting,
            venue=venue,
            date_from=date_from,
            date_to=date_to,
            year=selected_year,
            limit=RESULTS_PAGE_SIZE,
            offset=(page - 1) * RESULTS_PAGE_SIZE,
        )

    return render_template(
        "results_lookup.html",
        results_event_options=event_options,
        results_year_options=year_options,
        results_meetings=meetings,
        results_selected_event=selected_event,
        results_meeting=meeting,
        results_venue=venue,
        results_date_from=date_from_text,
        results_date_to=date_to_text,
        results_selected_year=selected_year or 0,
        results_has_filters=has_filters,
        results_date_label=results_date_label,
        results_recent_years=year_options[:4],
        results_page=page,
        results_page_size=RESULTS_PAGE_SIZE,
        results_total_meetings=total_meetings,
        results_total_pages=total_pages,
    )


@app.route("/results/results.aspx")
def results_detail():
    meeting_key = normalize_results_key(request.args.get("meeting"))
    venue_key = normalize_results_key(request.args.get("venue"))
    meeting_date = parse_results_date(request.args.get("date"))
    selected_event = (request.args.get("event") or "").strip()

    if meeting_date is None or (not meeting_key and not venue_key):
        return redirect("/results", code=302)

    with get_conn() as conn:
        meeting_view = build_meeting_view(
            conn,
            meeting_key=meeting_key,
            venue_key=venue_key,
            meeting_date=meeting_date,
            selected_event=selected_event,
        )

    if meeting_view is None:
        abort(404)

    return render_template(
        "results_detail.html",
        meeting_view=meeting_view,
    )


@app.route("/athletes")
def athlete_index():
    query = (request.args.get("q") or "").strip()
    surname = (request.args.get("surname") or "").strip()
    first_name = (request.args.get("first_name") or "").strip()
    club = (request.args.get("club") or "").strip()
    has_search = any([query, surname, first_name, club])
    with get_conn() as conn:
        athletes = (
            load_athlete_rows(
                conn,
                q=query,
                surname=surname,
                first_name=first_name,
                club=club,
            )
            if has_search
            else []
        )
        summary = summary_counts(conn)

    return render_template(
        "index.html",
        athletes=athletes,
        query=query,
        surname=surname,
        first_name=first_name,
        club=club,
        has_search=has_search,
        athlete_count=summary["athlete_count"],
        performance_count=summary["performance_count"],
    )


def load_sections(conn, athlete_id: int):
    sections = conn.execute(
        """
        SELECT
            id,
            source_kind,
            section_order,
            title,
            year,
            columns_json,
            metadata
        FROM athlete_performance_sections
        WHERE athlete_id = %s
        ORDER BY
            year DESC NULLS LAST,
            CASE WHEN source_kind = 'truepb_results' THEN 0 ELSE 1 END,
            section_order ASC
        """,
        (athlete_id,),
    ).fetchall()
    performances = conn.execute(
        """
        SELECT
            section_id,
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
        FROM athlete_performances
        WHERE athlete_id = %s
        ORDER BY section_id, row_order
        """,
        (athlete_id,),
    ).fetchall()

    rows_by_section: dict[int, list[dict]] = {}
    for result in performances:
        row = dict(result)
        row["local_results_url"] = local_results_event_url(
            meeting=row.get("meeting"),
            venue=row.get("venue"),
            result_date=row.get("result_date"),
            date_text=row.get("date_text"),
            event=row.get("event"),
        )
        rows_by_section.setdefault(result["section_id"], []).append(row)

    enriched_sections = []
    for section in sections:
        enriched_sections.append(
            {
                **section,
                "rows": rows_by_section.get(section["id"], []),
            }
        )
    return enriched_sections


@app.route("/athletes/<int:athlete_id>")
@app.route("/athletes/profile.aspx")
def athlete_profile(athlete_id: int | None = None):
    athlete_id = athlete_id or request.args.get("athleteid", type=int)
    if not athlete_id:
        abort(404)

    with get_conn() as conn:
        athlete = conn.execute(
            """
            SELECT
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
            FROM athletes
            WHERE athlete_id = %s
            """,
            (athlete_id,),
        ).fetchone()
        if not athlete:
            abort(404)

        best_rows = conn.execute(
            """
            SELECT row_order, cells
            FROM athlete_best_performance_rows
            WHERE athlete_id = %s
            ORDER BY row_order
            """,
            (athlete_id,),
        ).fetchall()
        best_headers = list(athlete["best_headers"] or [])
        if not best_headers or not best_rows:
            best_headers, best_rows = load_generated_best_table(conn, athlete_id)
        best_headers, best_rows = overlay_best_rows_for_year(
            best_headers,
            best_rows,
            load_truepb_year_bests(conn, athlete_id, TRUEPB_SB_OVERLAY_YEAR),
            year=TRUEPB_SB_OVERLAY_YEAR,
        )
        sections = load_sections(conn, athlete_id)

    section_nav = sorted(
        [
            {
                "anchor": f"section-{section['id']}",
                "label": str(section["year"]) if section["year"] is not None else section["title"],
                "year": section["year"],
            }
            for section in sections
        ],
        key=lambda item: (item["year"] if item["year"] is not None else 99999, item["label"]),
    )

    return render_template(
        "athlete.html",
        athlete=athlete,
        best_headers=best_headers,
        best_rows=best_rows,
        sections=sections,
        section_nav=section_nav,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), debug=True)
