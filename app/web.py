from __future__ import annotations

import os
from datetime import date
from pathlib import Path

from flask import Flask, abort, redirect, render_template, request, send_from_directory

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


def load_ranking_candidates(conn, *, sex: str, year: int | None):
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

    if sex == "M":
        sql += " AND COALESCE(a.gender, '') = 'Male'"
    elif sex == "W":
        sql += " AND COALESCE(a.gender, '') = 'Female'"
    elif sex == "X":
        return []

    if year is not None:
        sql += " AND s.year = %s"
        params.append(year)

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

    for row in load_ranking_candidates(conn, sex=sex, year=selected_year):
        if normalize_key(row["event"]) not in aliases:
            continue
        if age_group != "ALL" and section_age_group(row["section_title"]) != age_group:
            continue

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
        rows_by_section.setdefault(result["section_id"], []).append(result)

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
        best_rows=best_rows,
        sections=sections,
        section_nav=section_nav,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), debug=True)
