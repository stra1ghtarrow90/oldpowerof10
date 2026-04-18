from __future__ import annotations

import os
from pathlib import Path

from flask import Flask, abort, redirect, render_template, request, send_from_directory

from .db import get_conn


ROOT = Path(__file__).resolve().parent.parent
ASSET_ROOT = ROOT / "thepowerof10.info"

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
    return render_template("rankings.html")


@app.route("/rankings/rankinglist.aspx")
def rankings_list_redirect():
    query = request.query_string.decode("utf-8")
    target = "https://thepowerof10.info/rankings/rankinglist.aspx"
    if query:
        target = f"{target}?{query}"
    return redirect(target, code=302)


@app.route("/rankings/disabilityrankinglistrequest.aspx")
def disability_rankings_redirect():
    return redirect(
        "https://thepowerof10.info/rankings/disabilityrankinglistrequest.aspx",
        code=302,
    )


@app.route("/athletes")
def athlete_index():
    query = (request.args.get("q") or "").strip()
    with get_conn() as conn:
        athletes = load_athlete_rows(conn, q=query)
        summary = summary_counts(conn)

    return render_template(
        "index.html",
        athletes=athletes,
        query=query,
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
