from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

from bs4 import BeautifulSoup

from .legacy_dump import extract_year


PO10_PROFILE_BASE = "https://thepowerof10.info/athletes/"
RESULTS_BASE = "https://thepowerof10.info/results/results.aspx"
DEFAULT_RESULT_COLUMNS = ["Event", "Perf", "", "", "", "Pos", "", "", "", "Venue", "Meeting", "Date"]


@dataclass
class WaybackManifestRow:
    athlete_id: int
    timestamp: str | None
    original: str | None
    wayback_url: str | None

    @property
    def fetched_at(self) -> datetime | None:
        if not self.timestamp:
            return None
        try:
            return datetime.strptime(self.timestamp, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
        except ValueError:
            return None


@dataclass
class WaybackSection:
    title: str
    year: int | None
    columns: list[str]
    rows: list[dict]
    metadata: dict = field(default_factory=dict)


@dataclass
class WaybackAthlete:
    athlete_id: int
    display_name: str
    profile_name: str | None
    runner_name: str | None
    club: str | None
    gender: str | None
    age_group: str | None
    source_url: str
    fetched_at: datetime | None
    best_headers: list[str]
    best_rows: list[list[str]]
    sections: list[WaybackSection]
    html_path: Path
    wayback_url: str | None


def normalize_space(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").replace("\xa0", " ")).strip()


def normalize_identity(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", normalize_space(value).lower())


def athlete_id_from_path(path: Path) -> int | None:
    match = re.search(r"(\d+)", path.stem)
    if match:
        return int(match.group(1))
    return None


def discover_manifest_path(html_dir: Path, explicit_manifest: str | None = None) -> Path | None:
    if explicit_manifest:
        path = Path(explicit_manifest)
        if not path.exists():
            raise FileNotFoundError(f"Manifest does not exist: {path}")
        return path

    parent = html_dir.parent
    candidate = parent / "latest_profile_captures.csv"
    if candidate.exists():
        return candidate
    return None


def load_manifest(path: Path | None) -> dict[int, WaybackManifestRow]:
    if path is None or not path.exists():
        return {}

    manifest: dict[int, WaybackManifestRow] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            athlete_id_text = normalize_space(row.get("athleteid") or row.get("athlete_id"))
            if not athlete_id_text.isdigit():
                continue
            athlete_id = int(athlete_id_text)
            manifest[athlete_id] = WaybackManifestRow(
                athlete_id=athlete_id,
                timestamp=normalize_space(row.get("timestamp")) or None,
                original=normalize_space(row.get("original")) or None,
                wayback_url=normalize_space(row.get("wayback_url")) or None,
            )
    return manifest


def iter_html_files(html_dir: Path, limit: int | None = None) -> list[Path]:
    files = sorted(
        (path for path in html_dir.rglob("*.html") if path.is_file()),
        key=lambda path: (
            athlete_id_from_path(path) is None,
            athlete_id_from_path(path) or 0,
            str(path),
        ),
    )
    if limit is not None:
        return files[:limit]
    return files


def clean_node_text(node) -> str:
    return normalize_space(node.get_text(" ", strip=True))


def absolutize_profile_url(url: str | None, athlete_id: int) -> str:
    text = normalize_space(url)
    if not text:
        return f"{PO10_PROFILE_BASE}profile.aspx?athleteid={athlete_id}"
    if text.startswith("http://www.thepowerof10.info/"):
        return "https://" + text[len("http://") :]
    if text.startswith("http://thepowerof10.info/"):
        return "https://" + text[len("http://") :]
    if text.startswith("/"):
        return urljoin("https://thepowerof10.info", text)
    return text


def absolutize_result_url(url: str | None) -> str | None:
    text = normalize_space(url)
    if not text:
        return None
    if text.startswith("http://www.thepowerof10.info/"):
        return "https://" + text[len("http://") :]
    if text.startswith("http://thepowerof10.info/"):
        return "https://" + text[len("http://") :]
    if text.startswith("../"):
        return urljoin(PO10_PROFILE_BASE, text)
    if text.startswith("/"):
        return urljoin("https://thepowerof10.info", text)
    return text


def parse_details(soup: BeautifulSoup) -> dict[str, str]:
    panel = soup.select_one("#cphBody_pnlAthleteDetails")
    if panel is None:
        return {}

    details: dict[str, str] = {}
    for row in panel.find_all("tr"):
        cells = row.find_all("td", recursive=False)
        if len(cells) < 2:
            continue
        texts = [clean_node_text(cell) for cell in cells]
        for index in range(0, len(texts) - 1, 2):
            label = texts[index].rstrip(":")
            value = texts[index + 1]
            if label and value:
                details[label] = value
    return details


def parse_best_performances(soup: BeautifulSoup) -> tuple[list[str], list[list[str]]]:
    table = soup.select_one("#cphBody_divBestPerformances table")
    if table is None:
        return [], []

    headers: list[str] = []
    rows: list[list[str]] = []
    for row in table.find_all("tr"):
        cells = row.find_all(["td", "th"])
        if not cells:
            continue
        values = [clean_node_text(cell) for cell in cells]
        if not any(values):
            continue
        if not headers:
            headers = values
            continue
        if values == headers:
            continue
        if len(values) < len(headers):
            values = values + [""] * (len(headers) - len(values))
        rows.append(values[: len(headers)])
    return headers, rows


def build_result_url_from_query(values: dict[str, str]) -> str | None:
    if not values:
        return None
    return f"{RESULTS_BASE}?{urlencode(values)}"


def parse_result_row(values: list[str], cells, columns: list[str]) -> dict | None:
    if len(values) < 12:
        return None
    if not any(values[index] for index in (0, 1, 5, 9, 10, 11)):
        return None

    venue_anchor = cells[9].find("a")
    venue_url = absolutize_result_url(venue_anchor.get("href")) if venue_anchor else None

    extra: dict[str, str] = {}
    for index, value in enumerate(values):
        if index in {0, 1, 5, 9, 10, 11} or not value:
            continue
        label = columns[index] if index < len(columns) else ""
        key = normalize_identity(label) or f"col_{index}"
        if key in extra:
            key = f"{key}_{index}"
        extra[key] = value

    if venue_url:
        parsed = urlparse(venue_url)
        for key, values_list in parse_qs(parsed.query).items():
            if not values_list:
                continue
            extra[f"result_{key}"] = values_list[0]

    result = {
        "event": values[0],
        "perf": values[1],
        "pos": values[5],
        "venue": values[9],
        "venue_url": venue_url or build_result_url_from_query(
            {
                "event": values[0],
                "venue": values[9],
                "date": values[11].replace(" ", "-"),
            }
        ),
        "meeting": values[10],
        "date": values[11],
    }
    result.update(extra)
    return result


def parse_performance_sections(soup: BeautifulSoup, html_path: Path, manifest_row: WaybackManifestRow | None) -> list[WaybackSection]:
    table = soup.select_one("#cphBody_pnlPerformances table.alternatingrowspanel")
    if table is None:
        return []

    sections: list[WaybackSection] = []
    current_section: WaybackSection | None = None

    for row in table.find_all("tr"):
        cells = row.find_all("td", recursive=False)
        if not cells:
            continue

        values = [clean_node_text(cell) for cell in cells]
        joined = " ".join(value for value in values if value)
        style = normalize_space(row.get("style", "")).lower()

        if len(cells) == 1 and cells[0].get("colspan") == "12" and joined:
            current_section = WaybackSection(
                title=joined,
                year=extract_year(joined),
                columns=DEFAULT_RESULT_COLUMNS.copy(),
                rows=[],
                metadata={
                    "source": "powerof10_wayback_html",
                    "html_file": html_path.name,
                    "wayback_url": manifest_row.wayback_url if manifest_row else None,
                    "capture_timestamp": manifest_row.timestamp if manifest_row else None,
                },
            )
            sections.append(current_section)
            continue

        if current_section is None:
            continue

        if values[:2] == ["Event", "Perf"] and "Venue" in values and "Meeting" in values and "Date" in values:
            current_section.columns = values
            continue

        result = parse_result_row(values, cells, current_section.columns)
        if result is not None:
            current_section.rows.append(result)

    return [section for section in sections if section.rows]


def parse_wayback_profile(path: Path, manifest_row: WaybackManifestRow | None = None) -> WaybackAthlete:
    athlete_id = manifest_row.athlete_id if manifest_row else athlete_id_from_path(path)
    if athlete_id is None:
        raise ValueError(f"Could not infer athlete id from {path}")

    html = path.read_text(encoding="utf-8", errors="replace")
    soup = BeautifulSoup(html, "html.parser")

    title_node = soup.select_one("#cphBody_pnlMain h2") or soup.find("h2")
    display_name = clean_node_text(title_node) if title_node else f"Athlete {athlete_id}"
    if not display_name:
        display_name = f"Athlete {athlete_id}"

    details = parse_details(soup)
    best_headers, best_rows = parse_best_performances(soup)
    sections = parse_performance_sections(soup, path, manifest_row)

    return WaybackAthlete(
        athlete_id=athlete_id,
        display_name=display_name,
        profile_name=display_name,
        runner_name=display_name,
        club=details.get("Club"),
        gender=details.get("Gender"),
        age_group=details.get("Age Group"),
        source_url=absolutize_profile_url(manifest_row.original if manifest_row else None, athlete_id),
        fetched_at=manifest_row.fetched_at if manifest_row else None,
        best_headers=best_headers,
        best_rows=best_rows,
        sections=sections,
        html_path=path,
        wayback_url=manifest_row.wayback_url if manifest_row else None,
    )
