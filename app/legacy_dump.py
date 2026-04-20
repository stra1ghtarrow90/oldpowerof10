from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, urlparse


TABLE_COPY_RE = re.compile(r"^COPY public\.([^( ]+) \((.*)\) FROM stdin;$")


@dataclass
class RunnerMeta:
    runner_id: int | None
    name: str | None
    club: str | None
    gender: str | None
    age: int | None
    powerof10_url: str | None
    explicit_athlete_id: bool


@dataclass
class ProfileMeta:
    athlete_id: int
    athlete_name: str | None
    source_url: str | None


@dataclass
class CacheRow:
    athlete_id: int
    source_url: str
    best_performances: dict
    performances: list[dict]
    fetched_at: datetime | None


@dataclass
class ResolvedAthlete:
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
    best_rows: list[list[str]]
    performances: list[dict]
    performance_count: int
    section_count: int
    first_year: int | None
    last_year: int | None


def pg_unescape(text: str) -> str | None:
    if text == r"\N":
        return None

    out: list[str] = []
    i = 0
    while i < len(text):
        char = text[i]
        if char != "\\":
            out.append(char)
            i += 1
            continue

        i += 1
        if i >= len(text):
            out.append("\\")
            break

        char = text[i]
        mapped = {
            "b": "\b",
            "f": "\f",
            "n": "\n",
            "r": "\r",
            "t": "\t",
            "v": "\v",
            "\\": "\\",
        }.get(char)
        if mapped is not None:
            out.append(mapped)
            i += 1
            continue

        if char == "x" and i + 2 < len(text):
            out.append(chr(int(text[i + 1 : i + 3], 16)))
            i += 3
            continue

        if char in "01234567":
            j = i
            while j < len(text) and j < i + 3 and text[j] in "01234567":
                j += 1
            out.append(chr(int(text[i:j], 8)))
            i = j
            continue

        out.append(char)
        i += 1

    return "".join(out)


def parse_copy_line(line: str, field_count: int) -> list[str | None]:
    parts = line.rstrip("\n").split("\t", field_count - 1)
    if len(parts) != field_count:
        raise ValueError(f"Expected {field_count} fields, found {len(parts)}")
    return [pg_unescape(part) for part in parts]


def extract_athlete_id_from_url(url: str | None) -> int | None:
    if not url:
        return None
    try:
        parsed = urlparse(url)
        values = parse_qs(parsed.query).get("athleteid")
        if values:
            return int(values[0])
    except (TypeError, ValueError):
        return None
    return None


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def parse_result_date(value: str | None) -> date | None:
    if not value:
        return None
    for fmt in ("%d %b %y", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(value, fmt).date()
            # Historic Po10 rows often use two-digit years. Python maps 00-68
            # into 2000-2068, which creates impossible future results like 2066.
            if fmt == "%d %b %y" and parsed > (date.today() + timedelta(days=366)):
                parsed = parsed.replace(year=parsed.year - 100)
            return parsed
        except ValueError:
            continue
    return None


def normalize_gender(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = value.strip().upper()
    if cleaned == "M":
        return "Male"
    if cleaned == "F":
        return "Female"
    return value.strip().title()


def normalize_name(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def runner_score(meta: RunnerMeta) -> int:
    score = 0
    if meta.explicit_athlete_id:
        score += 100
    if meta.name:
        score += 10
    if meta.club:
        score += 10
    if meta.gender:
        score += 10
    if meta.age is not None:
        score += 10
    if meta.powerof10_url:
        score += 10
    return score


def merge_runner(existing: RunnerMeta | None, candidate: RunnerMeta) -> RunnerMeta:
    if existing is None:
        return candidate

    existing_score = runner_score(existing)
    candidate_score = runner_score(candidate)
    primary = candidate if candidate_score >= existing_score else existing
    secondary = existing if primary is candidate else candidate

    return RunnerMeta(
        runner_id=primary.runner_id or secondary.runner_id,
        name=primary.name or secondary.name,
        club=primary.club or secondary.club,
        gender=primary.gender or secondary.gender,
        age=primary.age if primary.age is not None else secondary.age,
        powerof10_url=primary.powerof10_url or secondary.powerof10_url,
        explicit_athlete_id=primary.explicit_athlete_id or secondary.explicit_athlete_id,
    )


def runner_is_trusted(runner: RunnerMeta | None, profile: ProfileMeta | None) -> bool:
    if runner is None:
        return False
    if runner.explicit_athlete_id:
        return True
    if profile and normalize_name(runner.name) == normalize_name(profile.athlete_name):
        return True
    return False


def extract_year(title: str | None) -> int | None:
    if not title:
        return None
    match = re.match(r"^\s*(\d{4})\b", title)
    if match:
        return int(match.group(1))
    return None


def infer_age_group(performances: list[dict]) -> str | None:
    for section in performances:
        title = (section.get("title") or "").strip()
        match = re.match(r"^\s*\d{4}\s+([A-Z0-9+]+)\b", title)
        if match:
            return match.group(1)
    return None


def infer_club(performances: list[dict]) -> str | None:
    best_year = -1
    best_club = None
    for section in performances:
        title = (section.get("title") or "").strip()
        match = re.match(r"^\s*(\d{4})\s+[A-Z0-9+]+\s+(.*)$", title)
        if not match:
            continue
        year = int(match.group(1))
        club = match.group(2).strip() or None
        if club and year > best_year:
            best_year = year
            best_club = club
    return best_club


def load_dump(sql_path: Path) -> tuple[dict[int, ProfileMeta], dict[int, RunnerMeta], list[CacheRow]]:
    profiles: dict[int, ProfileMeta] = {}
    runners: dict[int, RunnerMeta] = {}
    cache_rows: list[CacheRow] = []

    current_table: str | None = None
    with sql_path.open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            if current_table is None:
                match = TABLE_COPY_RE.match(raw_line)
                if match:
                    table_name = match.group(1)
                    if table_name in {"powerof10_profiles", "runners", "powerof10_cache"}:
                        current_table = table_name
                continue

            if raw_line == "\\.\n":
                current_table = None
                continue

            if current_table == "powerof10_profiles":
                fields = parse_copy_line(raw_line, 9)
                athlete_id = int(fields[0] or "0")
                profiles[athlete_id] = ProfileMeta(
                    athlete_id=athlete_id,
                    athlete_name=fields[1],
                    source_url=fields[2],
                )
                continue

            if current_table == "runners":
                fields = parse_copy_line(raw_line, 16)
                athlete_id = int(fields[11]) if fields[11] else extract_athlete_id_from_url(fields[9])
                if athlete_id is None:
                    continue
                candidate = RunnerMeta(
                    runner_id=int(fields[0] or "0"),
                    name=fields[1],
                    club=fields[2],
                    gender=normalize_gender(fields[5]),
                    age=int(fields[12]) if fields[12] else None,
                    powerof10_url=fields[9],
                    explicit_athlete_id=fields[11] is not None,
                )
                runners[athlete_id] = merge_runner(runners.get(athlete_id), candidate)
                continue

            if current_table == "powerof10_cache":
                fields = parse_copy_line(raw_line, 5)
                cache_rows.append(
                    CacheRow(
                        athlete_id=int(fields[0] or "0"),
                        source_url=fields[1] or "",
                        best_performances=json.loads(fields[2] or '{"headers":[],"rows":[]}'),
                        performances=json.loads(fields[3] or "[]"),
                        fetched_at=parse_timestamp(fields[4]),
                    )
                )

    return profiles, runners, cache_rows


def resolve_athletes(
    profiles: dict[int, ProfileMeta],
    runners: dict[int, RunnerMeta],
    cache_rows: Iterable[CacheRow],
) -> list[ResolvedAthlete]:
    resolved: list[ResolvedAthlete] = []

    for row in sorted(cache_rows, key=lambda item: item.athlete_id):
        profile = profiles.get(row.athlete_id)
        runner = runners.get(row.athlete_id)
        trusted_runner = runner if runner_is_trusted(runner, profile) else None
        best_headers = [str(header) for header in (row.best_performances.get("headers") or [])]
        best_rows = [
            ["" if value is None else str(value) for value in raw_row]
            for raw_row in (row.best_performances.get("rows") or [])
        ]
        years = sorted(
            year
            for year in (extract_year(section.get("title")) for section in row.performances)
            if year is not None
        )

        display_name = (
            (profile.athlete_name if profile else None)
            or (trusted_runner.name if trusted_runner else None)
            or (runner.name if runner else None)
            or f"Athlete {row.athlete_id}"
        )
        club = (trusted_runner.club if trusted_runner else None) or infer_club(row.performances)
        resolved.append(
            ResolvedAthlete(
                athlete_id=row.athlete_id,
                display_name=display_name,
                profile_name=profile.athlete_name if profile else None,
                runner_name=trusted_runner.name if trusted_runner else (runner.name if runner else None),
                club=club,
                gender=trusted_runner.gender if trusted_runner else None,
                age=trusted_runner.age if trusted_runner else None,
                age_group=infer_age_group(row.performances),
                source_url=row.source_url or (profile.source_url if profile else "") or "#",
                fetched_at=row.fetched_at,
                best_headers=best_headers,
                best_rows=best_rows,
                performances=row.performances,
                performance_count=sum(len(section.get("rows", [])) for section in row.performances),
                section_count=len(row.performances),
                first_year=years[0] if years else None,
                last_year=years[-1] if years else None,
            )
        )

    return resolved
