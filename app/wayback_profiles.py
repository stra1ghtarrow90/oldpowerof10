from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen


CDX_API = "https://web.archive.org/cdx/search/cdx"
PROFILE_PREFIX = "http://www.thepowerof10.info/athletes/profile.aspx?athleteid="
DEFAULT_OUTPUT_ROOT = Path("imports") / "wayback_profiles"
DEFAULT_MANIFEST = DEFAULT_OUTPUT_ROOT / "latest_profile_captures.csv"
DEFAULT_STATE = DEFAULT_OUTPUT_ROOT / "latest_profile_captures.state.json"
DEFAULT_HTML_DIR = DEFAULT_OUTPUT_ROOT / "html"
DEFAULT_FAILURE_LOG = DEFAULT_OUTPUT_ROOT / "download_failures.csv"
DEFAULT_PAGE_LIMIT = 5000
REQUEST_TIMEOUT = 120
USER_AGENT = "oldpowerof10-wayback-export/1.0"
MAX_RETRIES = 5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export the latest Wayback snapshot for each Power of 10 athlete profile, and optionally download the HTML."
    )
    parser.add_argument(
        "--prefix",
        default=PROFILE_PREFIX,
        help="CDX prefix to query.",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST,
        help="CSV manifest output path.",
    )
    parser.add_argument(
        "--state",
        type=Path,
        default=DEFAULT_STATE,
        help="Resume state JSON path.",
    )
    parser.add_argument(
        "--html-dir",
        type=Path,
        default=DEFAULT_HTML_DIR,
        help="Directory where downloaded HTML files will be written.",
    )
    parser.add_argument(
        "--failure-log",
        type=Path,
        default=DEFAULT_FAILURE_LOG,
        help="CSV path for download failures.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_PAGE_LIMIT,
        help="Number of CDX rows to request per page.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.25,
        help="Delay between archive requests in seconds.",
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download the latest HTML files after building the manifest.",
    )
    parser.add_argument(
        "--download-only",
        action="store_true",
        help="Skip the CDX scan and only download from the existing manifest.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing HTML files when downloading.",
    )
    parser.add_argument(
        "--max-downloads",
        type=int,
        default=0,
        help="Optional cap on HTML downloads for testing. 0 means no cap.",
    )
    return parser.parse_args()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def atomic_write_text(path: Path, content: str) -> None:
    ensure_parent(path)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(content, encoding="utf-8")
    tmp_path.replace(path)


def archive_get_bytes(url: str) -> bytes:
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        request = Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                return response.read()
        except (HTTPError, URLError, TimeoutError) as exc:
            last_error = exc
            if attempt == MAX_RETRIES:
                break
            sleep_for = min(30.0, 2 ** (attempt - 1))
            print(
                f"Request failed on attempt {attempt}/{MAX_RETRIES}: {exc}. Retrying in {sleep_for:.1f}s",
                file=sys.stderr,
            )
            time.sleep(sleep_for)
    assert last_error is not None
    raise last_error


def archive_get_json(url: str) -> Any:
    return json.loads(archive_get_bytes(url).decode("utf-8", "replace"))


def athlete_id_from_original(original: str) -> str | None:
    athlete_id = parse_qs(urlparse(original).query).get("athleteid", [""])[0]
    return athlete_id if athlete_id.isdigit() else None


def latest_wayback_url(timestamp: str, original: str) -> str:
    return f"https://web.archive.org/web/{timestamp}id_/{original}"


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"resume_key": None, "latest": {}, "pages": 0, "rows": 0}
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(path: Path, state: dict[str, Any]) -> None:
    atomic_write_text(path, json.dumps(state, indent=2, sort_keys=True))


def cdx_page(prefix: str, limit: int, resume_key: str | None) -> tuple[list[list[str]], str | None]:
    params: list[tuple[str, str]] = [
        ("url", prefix),
        ("matchType", "prefix"),
        ("output", "json"),
        ("fl", "original,timestamp,statuscode"),
        ("filter", "statuscode:200"),
        ("showResumeKey", "true"),
        ("limit", str(limit)),
        ("gzip", "false"),
    ]
    if resume_key:
        params.append(("resumeKey", resume_key))
    url = f"{CDX_API}?{urlencode(params)}"
    payload = archive_get_json(url)

    if not isinstance(payload, list) or not payload:
        return [], None

    rows = payload[1:]
    next_resume_key = None

    if rows and rows[-1] == []:
        rows.pop()
    if rows and isinstance(rows[-1], list) and len(rows[-1]) == 1:
        next_resume_key = rows.pop()[0]
        if rows and rows[-1] == []:
            rows.pop()

    filtered_rows = [row for row in rows if isinstance(row, list) and len(row) >= 2]
    return filtered_rows, next_resume_key


def build_latest_manifest(args: argparse.Namespace) -> dict[str, dict[str, str]]:
    state = load_state(args.state)
    latest: dict[str, dict[str, str]] = state.get("latest") or {}
    resume_key = state.get("resume_key")
    processed_pages = int(state.get("pages") or 0)
    processed_rows = int(state.get("rows") or 0)

    while True:
        rows, next_resume_key = cdx_page(args.prefix, args.limit, resume_key)
        if not rows and not next_resume_key:
            break

        for row in rows:
            original, timestamp = row[0], row[1]
            athlete_id = athlete_id_from_original(original)
            if athlete_id is None:
                continue
            existing = latest.get(athlete_id)
            if existing is None or timestamp > existing["timestamp"]:
                latest[athlete_id] = {
                    "timestamp": timestamp,
                    "original": original,
                }

        processed_pages += 1
        processed_rows += len(rows)
        resume_key = next_resume_key
        save_state(
            args.state,
            {
                "resume_key": resume_key,
                "latest": latest,
                "pages": processed_pages,
                "rows": processed_rows,
            },
        )
        print(
            f"CDX pages={processed_pages} rows={processed_rows} athlete_profiles={len(latest)}",
            file=sys.stderr,
        )
        if not resume_key:
            break
        time.sleep(args.sleep)

    return latest


def write_manifest(path: Path, latest: dict[str, dict[str, str]]) -> None:
    ensure_parent(path)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["athleteid", "timestamp", "original", "wayback_url"])
        for athlete_id in sorted(latest, key=int):
            row = latest[athlete_id]
            writer.writerow(
                [
                    athlete_id,
                    row["timestamp"],
                    row["original"],
                    latest_wayback_url(row["timestamp"], row["original"]),
                ]
            )


def read_manifest(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_failure_log(path: Path, failures: list[dict[str, str]]) -> None:
    ensure_parent(path)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["athleteid", "timestamp", "original", "wayback_url", "error"])
        for row in failures:
            writer.writerow(
                [
                    row["athleteid"],
                    row["timestamp"],
                    row["original"],
                    row["wayback_url"],
                    row["error"],
                ]
            )


def download_html_from_manifest(args: argparse.Namespace) -> None:
    rows = read_manifest(args.manifest)
    args.html_dir.mkdir(parents=True, exist_ok=True)
    downloaded = 0
    skipped = 0
    failed = 0
    failures: list[dict[str, str]] = []

    for index, row in enumerate(rows, start=1):
        athlete_id = row["athleteid"]
        timestamp = row["timestamp"]
        original = row["original"]
        target = args.html_dir / f"{athlete_id}.html"
        wayback_url = latest_wayback_url(timestamp, original)

        if target.exists() and not args.force:
            skipped += 1
            continue

        try:
            html = archive_get_bytes(wayback_url)
        except Exception as exc:
            failed += 1
            failures.append(
                {
                    "athleteid": athlete_id,
                    "timestamp": timestamp,
                    "original": original,
                    "wayback_url": wayback_url,
                    "error": str(exc),
                }
            )
            print(
                f"Failed download for athlete {athlete_id} ({index}/{len(rows)}): {exc}",
                file=sys.stderr,
            )
            if args.max_downloads and downloaded >= args.max_downloads:
                break
            time.sleep(args.sleep)
            continue

        target.write_bytes(html)
        downloaded += 1

        if downloaded % 100 == 0:
            print(
                f"Downloaded {downloaded} HTML files ({index}/{len(rows)} manifest rows, skipped {skipped}, failed {failed})",
                file=sys.stderr,
            )
        if args.max_downloads and downloaded >= args.max_downloads:
            break
        time.sleep(args.sleep)

    write_failure_log(args.failure_log, failures)
    print(
        f"HTML download complete: downloaded={downloaded} skipped={skipped} failed={failed} output_dir={args.html_dir} failure_log={args.failure_log}",
        file=sys.stderr,
    )


def main() -> None:
    args = parse_args()

    if args.download_only:
        if not args.manifest.exists():
            raise SystemExit(f"Manifest does not exist: {args.manifest}")
        download_html_from_manifest(args)
        return

    latest = build_latest_manifest(args)
    write_manifest(args.manifest, latest)
    print(f"Wrote manifest with {len(latest)} athlete profiles to {args.manifest}", file=sys.stderr)

    if args.download:
        download_html_from_manifest(args)


if __name__ == "__main__":
    main()
