from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parent
TOOLBAR_TEMPLATE = ROOT / "templates" / "partials" / "rankings_toolbar.html"

EVENT_LINK_RE = re.compile(r'<a id="e([^"]+)"[^>]*>([^<]+)</a>')
SECTION_AGE_RE = re.compile(r"^\s*\d{4}\s+([A-Z0-9+]+)\b")
MARK_RE = re.compile(r"\d+(?::\d+){0,2}(?:\.\d+)?")

AREA_LABELS = {
    "0": "UK",
    "61": "North East",
    "62": "Yorkshire",
    "63": "North West",
    "64": "West Midlands",
    "65": "East Midlands",
    "66": "East",
    "67": "London",
    "68": "South East",
    "69": "South West",
    "91": "England",
    "92": "Scotland",
    "93": "Wales",
    "94": "N Ireland",
}

SEX_LABELS = {
    "M": "Men",
    "W": "Women",
    "X": "Mixed",
}

AGE_GROUP_LABELS = {
    "ALL": "Overall",
    "U20": "Under 20",
    "U17": "Under 17",
    "U15": "Under 15",
    "U13": "Under 13",
    "DIS": "Disability",
}

FIELD_PREFIXES = ("HJ", "PV", "LJ", "TJ", "SP", "DT", "HT", "JT")
POINTS_PREFIXES = ("DEC", "HEP", "HEPI", "PEN", "PENI", "OCT")
FIELD_LABELS = {
    "HIGHJUMP",
    "POLEVAULT",
    "LONGJUMP",
    "TRIPLEJUMP",
    "SHOT",
    "DISCUS",
    "HAMMER",
    "JAVELIN",
}
POINTS_LABELS = {
    "DECATHLON",
    "HEPTATHLON",
    "INDOORHEP",
    "PENTATHLON",
    "INDOORPEN",
    "OCTATHLON",
}


def normalize_key(value: str | None) -> str:
    return re.sub(r"[^A-Z0-9]+", "", (value or "").upper())


def load_event_labels() -> dict[str, str]:
    text = TOOLBAR_TEMPLATE.read_text(encoding="utf-8")
    labels: dict[str, str] = {}
    for raw_id, label in EVENT_LINK_RE.findall(text):
        if len(raw_id) <= 4:
            continue
        event_code = raw_id[4:]
        labels.setdefault(event_code, label.strip())
    return labels


EVENT_LABELS = load_event_labels()


def event_label(event_code: str) -> str:
    return EVENT_LABELS.get(event_code, event_code)


def area_label(area_id: str) -> str:
    return AREA_LABELS.get(area_id, area_id)


def sex_label(sex: str) -> str:
    return SEX_LABELS.get(sex, sex)


def age_group_label(age_group: str) -> str:
    return AGE_GROUP_LABELS.get(age_group, age_group)


def section_age_group(title: str | None) -> str | None:
    if not title:
        return None
    match = SECTION_AGE_RE.match(title)
    if not match:
        return None
    return match.group(1)


def event_aliases(event_code: str) -> set[str]:
    code = event_code.strip()
    label = event_label(code)
    normalized_code = normalize_key(code)
    aliases = {normalized_code, normalize_key(label)}

    if label.lower() == "half marathon":
        aliases.add("HM")
    if label.lower() == "marathon":
        aliases.update({"MAR", "MARATHON"})

    hurdles_match = re.match(r"^(\d+)", normalized_code)
    if "HURDLES" in normalize_key(label) and hurdles_match:
        aliases.add(f"{hurdles_match.group(1)}H")

    steeple_match = re.match(r"^(\d+SC)", normalized_code)
    if steeple_match:
        aliases.add(steeple_match.group(1))

    for prefix in FIELD_PREFIXES + POINTS_PREFIXES:
        if normalized_code.startswith(prefix):
            aliases.add(prefix)

    return {alias for alias in aliases if alias}


def ranking_direction(event_code: str) -> str:
    normalized_code = normalize_key(event_code)
    normalized_label = normalize_key(event_label(event_code))
    if normalized_code.startswith(FIELD_PREFIXES) or normalized_label in FIELD_LABELS:
        return "higher"
    if normalized_code.startswith(POINTS_PREFIXES) or normalized_label in POINTS_LABELS:
        return "higher"
    return "lower"


def parse_mark(perf: str | None) -> float | None:
    if not perf:
        return None
    match = MARK_RE.search(perf.replace(",", ""))
    if not match:
        return None

    token = match.group(0)
    parts = token.split(":")
    if len(parts) == 1:
        try:
            return float(parts[0])
        except ValueError:
            return None

    total = 0.0
    for part in parts:
        try:
            total = (total * 60) + float(part)
        except ValueError:
            return None
    return total
