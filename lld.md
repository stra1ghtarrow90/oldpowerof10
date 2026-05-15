# Low Level Design: TruePB Po10 Live Archive

Generated from the repository contents on 2026-05-15.

## 1. Purpose

This project rebuilds a usable Power of 10 style athlete/results site from several data sources:

- an existing TruePB/Power of 10 PostgreSQL dump;
- archived Power of 10 athlete profile pages from the Wayback Machine;
- new TruePB race results exported from a live TruePB database or a plain PostgreSQL dump;
- static Power of 10 assets saved under `thepowerof10.info/`.

The end product is a Dockerized Flask application backed by PostgreSQL. It can show athlete profile pages, best performance tables, historical performance sections, rankings, meeting/result lookup pages, and Power of 10 styled static assets.

The core design idea is provenance. Every performance row is stored in one common schema, but its source is preserved with `source_kind`:

- `powerof10_cache`: historical data imported from the legacy cache dump.
- `powerof10_wayback`: profile and performance data parsed from downloaded Wayback HTML.
- `truepb_results`: newer TruePB result sections appended or generated from TruePB data.

That lets the app merge old Power of 10 history with newer TruePB results without losing where each row came from.

## 2. Repository Inventory

```text
.
|-- README.md
|-- wayback_instructions.txt
|-- schema.sql
|-- docker-compose.yml
|-- Dockerfile
|-- requirements.txt
|-- favicon.ico
|-- app/
|   |-- __init__.py
|   |-- db.py
|   |-- web.py
|   |-- rankings_support.py
|   |-- legacy_dump.py
|   |-- import_legacy_dump.py
|   |-- wayback_profiles.py
|   |-- wayback_html.py
|   |-- import_wayback_profiles.py
|   |-- import_truepb_results.py
|   |-- export_truepb_results_sql.py
|   |-- sync_profiles_to_truepb.py
|   `-- export_profiles_to_truepb_sql.py
|-- app/templates/
|   |-- base.html
|   |-- home.html
|   |-- index.html
|   |-- athlete.html
|   |-- rankings.html
|   |-- results_lookup.html
|   |-- results_detail.html
|   |-- about.html
|   `-- partials/rankings_toolbar.html
|-- imports/
|   `-- examples/truepb-results.sample.json
`-- thepowerof10.info/
    |-- athletes/
    |-- images/
    |-- js/
    |-- logos/
    `-- stylesheets/
```

Important generated paths described by the repo:

```text
imports/legacy/16-04-2026.sql
imports/wayback_profiles/latest_profile_captures.csv
imports/wayback_profiles/latest_profile_captures.state.json
imports/wayback_profiles/html/<athleteid>.html
imports/wayback_profiles/download_failures.csv
imports/wayback_profiles/import_report.csv
imports/generated/truepb-results-<year>-from-<date>.sql
imports/generated/truepb_profile_sync_report.csv
```

Only `imports/examples/truepb-results.sample.json` exists in this checkout. The larger import folders are expected runtime/generated inputs.

## 3. Runtime Stack

- Python 3.12 slim container.
- Flask 3.0.3 web application.
- Gunicorn 22.0.0 production HTTP server.
- PostgreSQL 16 database.
- `psycopg[binary]` 3.2.13 for database access.
- `beautifulsoup4` 4.12.3 for archived HTML parsing.
- Docker Compose for local database and web service orchestration.

Docker Compose services:

| Service | Image/build | Purpose | Ports |
| --- | --- | --- | --- |
| `db` | `postgres:16` | Stores normalized athlete, section, performance, and best-row data | host `5432` -> container `5432` |
| `web` | local `Dockerfile` | Runs Gunicorn serving `app:app` | host `8001` -> container `8000` |

Default application DSN:

```text
postgresql://truepb:truepb@localhost:5432/truepb_live
```

Container DSN:

```text
postgresql://truepb:truepb@db:5432/truepb_live
```

## 4. Architecture Diagram

```mermaid
flowchart LR
    LegacyDump["Legacy TruePB / Po10 SQL dump"]
    WaybackCDX["Wayback CDX API"]
    WaybackHTML["Downloaded profile HTML"]
    TruePBDB["TruePB DB or SQL dump"]
    JsonImport["Manual TruePB JSON"]

    LegacyParser["app.legacy_dump + import_legacy_dump"]
    Manifest["app.wayback_profiles manifest + downloader"]
    HtmlParser["app.wayback_html parser"]
    WaybackImporter["app.import_wayback_profiles merge importer"]
    TruePBExporter["app.export_truepb_results_sql"]
    TruePBJsonImporter["app.import_truepb_results"]

    DB[("PostgreSQL truepb_live")]
    Web["Flask app.web"]
    Pages["Home / Athlete / Results / Rankings"]
    Static["Archived Power of 10 assets"]
    Sync["sync/export profiles back to TruePB"]

    LegacyDump --> LegacyParser --> DB
    WaybackCDX --> Manifest --> WaybackHTML --> HtmlParser --> WaybackImporter --> DB
    TruePBDB --> TruePBExporter --> DB
    JsonImport --> TruePBJsonImporter --> DB
    DB --> Web --> Pages
    Static --> Web
    DB --> Sync
```

## 5. Database Design

The schema is intentionally small and normalized around athletes, performance sections, and performance rows.

```mermaid
erDiagram
    athletes ||--o{ athlete_best_performance_rows : has
    athletes ||--o{ athlete_performance_sections : has
    athlete_performance_sections ||--o{ athlete_performances : contains

    athletes {
        bigint athlete_id PK
        text display_name
        text profile_name
        text runner_name
        text club
        text gender
        integer age
        text age_group
        text source_url
        timestamptz fetched_at
        jsonb best_headers
        integer performance_count
        integer section_count
        integer first_year
        integer last_year
    }

    athlete_best_performance_rows {
        bigint athlete_id PK,FK
        integer row_order PK
        jsonb cells
    }

    athlete_performance_sections {
        bigserial id PK
        bigint athlete_id FK
        text source_kind
        integer section_order
        text title
        integer year
        jsonb columns_json
        jsonb metadata
    }

    athlete_performances {
        bigserial id PK
        bigint athlete_id FK
        bigint section_id FK
        text source_kind
        integer row_order
        text event
        text perf
        text pos
        text venue
        text venue_url
        text meeting
        text date_text
        date result_date
        jsonb extra
    }
```

### 5.1 `athletes`

The `athletes` table is the profile-level record. It stores identity, display metadata, source URL, fetch timestamp, best table headers, and aggregate counts.

Important fields:

- `athlete_id`: real Power of 10 athlete id where known. TruePB-only runners without a Po10 id can be represented by a synthetic id.
- `display_name`: preferred name shown in listings.
- `profile_name`: name parsed from a Power of 10 profile.
- `runner_name`: name from TruePB runner metadata.
- `club`, `gender`, `age`, `age_group`: profile/race metadata used in pages, matching, and rankings.
- `source_url`: original profile/result source.
- `fetched_at`: archive capture or cache fetch timestamp.
- `best_headers`: JSON array of best-performance table header labels.
- `performance_count`, `section_count`, `first_year`, `last_year`: denormalized counts refreshed by import scripts.

### 5.2 `athlete_best_performance_rows`

Stores each row of the profile best-performance table as JSON cells. The headers live on `athletes.best_headers`, and row order is preserved with `(athlete_id, row_order)`.

This mirrors how Power of 10 profile pages present a compact PB/SB table.

### 5.3 `athlete_performance_sections`

Groups performance rows into profile sections such as:

- `2026 TRUEPB Results`
- a historical year/age-group/club section from Power of 10
- an imported Wayback section

Important fields:

- `source_kind`: provenance label.
- `section_order`: order within the athlete profile.
- `title`: original section title.
- `year`: parsed year used for sorting, filtering, rankings, and date fallback.
- `columns_json`: source table columns.
- `metadata`: source details, generated-at timestamps, capture timestamp, Wayback URL, runner id, etc.

### 5.4 `athlete_performances`

Stores normalized result rows.

Core display fields:

- `event`
- `perf`
- `pos`
- `venue`
- `venue_url`
- `meeting`
- `date_text`
- `result_date`

`extra` captures source-specific data that does not fit the shared columns. Examples include TruePB race flags, race ids, runner ids, result ids, distance, relay flags, and query parameters parsed from Power of 10 venue links.

Indexes:

- `idx_athletes_display_name` for lower-case name search.
- `idx_athlete_sections_lookup` for profile section lookup and sorting.
- `idx_athlete_performances_lookup` for section-row lookup.
- `idx_athlete_performances_result_date` for recent/result pages.

## 6. Source Provenance Model

All sources become the same database shape. Provenance is preserved by `source_kind` and `metadata`.

| Source kind | Created by | Meaning |
| --- | --- | --- |
| `powerof10_cache` | `app.import_legacy_dump` | Existing cached Power of 10 data from the legacy SQL dump |
| `powerof10_wayback` | `app.import_wayback_profiles` | HTML parsed from Wayback profile snapshots |
| `truepb_results` | `app.import_truepb_results` or `app.export_truepb_results_sql` | Newer TruePB race result data |

This design matters because athlete pages can show all known history, while import scripts can decide whether an athlete already has real Power of 10 profile data or only TruePB result data.

## 7. Legacy Dump Import

Files:

- `app/legacy_dump.py`
- `app/import_legacy_dump.py`

Input:

```text
imports/legacy/16-04-2026.sql
```

Expected source tables inside the plain PostgreSQL dump:

- `powerof10_profiles`
- `runners`
- `powerof10_cache`

### 7.1 Dump Parsing

`legacy_dump.py` reads PostgreSQL `COPY public.<table> (...) FROM stdin;` sections using:

- `TABLE_COPY_RE`
- `parse_copy_line`
- `pg_unescape`

It does not restore the dump into a temporary database. It parses the plain SQL file directly.

### 7.2 Source Metadata Extraction

`load_dump()` extracts:

- `ProfileMeta` from `powerof10_profiles`;
- `RunnerMeta` from `runners`;
- `CacheRow` from `powerof10_cache`.

`RunnerMeta` can identify athlete id from:

- explicit `powerof10_athlete_id`;
- or `athleteid` query parameter inside `powerof10_url`.

If multiple runner rows point to the same athlete id, `merge_runner()` prefers the richer row using `runner_score()`. Score is based on explicit athlete id, name, club, gender, age, and URL.

### 7.3 Trust Rules

`runner_is_trusted()` returns true when:

- runner has an explicit Po10 athlete id; or
- runner name normalizes to the same identity as the profile athlete name.

Untrusted runner fields are not allowed to override the profile identity.

### 7.4 Resolved Athlete Creation

`resolve_athletes()` builds `ResolvedAthlete` objects:

- `display_name`: profile name, trusted runner name, runner name, or fallback `Athlete <id>`.
- `club`: trusted runner club or inferred from newest performance section title.
- `gender`, `age`: trusted runner metadata only.
- `age_group`: inferred from performance section title.
- `best_headers` and `best_rows`: from cache JSON.
- `performances`: cache JSON performance sections.
- aggregate counts and first/last year: computed from section data.

### 7.5 Database Write

`import_legacy_dump.py`:

1. Calls `ensure_schema()`.
2. Parses and resolves dump athletes.
3. Truncates all normalized app tables with `RESTART IDENTITY CASCADE`.
4. Inserts `athletes`.
5. Inserts `athlete_best_performance_rows`.
6. Inserts `athlete_performance_sections` with `source_kind='powerof10_cache'`.
7. Inserts `athlete_performances` with `source_kind='powerof10_cache'`.
8. Parses `result_date` from `date` using `parse_result_date()`.

`parse_result_date()` accepts:

- `%d %b %y`, for example `14 May 26`;
- `%Y-%m-%d`, for example `2026-05-14`.

For old two-digit dates, it corrects impossible future dates by subtracting 100 years if Python's default interpretation lands more than 366 days in the future.

## 8. Wayback Machine Profile Pipeline

Files:

- `app/wayback_profiles.py`
- `app/wayback_html.py`
- `app/import_wayback_profiles.py`
- `wayback_instructions.txt`

The Wayback pipeline has three separate stages:

1. Discover latest profile captures using the Wayback CDX API.
2. Download one archived profile HTML file per athlete.
3. Parse and import those HTML files into PostgreSQL.

### 8.1 Why Wayback Profiles Are Important

TruePB race results alone are event rows. They are not a full historical Power of 10 profile.

Archived Power of 10 athlete profiles provide:

- the real Power of 10 athlete id;
- canonical profile display name;
- club, gender, and age-group metadata;
- best-known-performance headers and rows;
- historical year sections;
- original meeting, venue, performance, position, and date text;
- links back to original Power of 10 result pages where available;
- enough identity data to merge TruePB-only synthetic athletes into real Po10 athlete ids.

Without the Wayback profile import, TruePB-only runners can exist as synthetic ids and the site can show recent result sections, but it cannot reliably reconstruct historical Power of 10 profile pages.

### 8.2 CDX Manifest Builder

`app.wayback_profiles` queries:

```text
https://web.archive.org/cdx/search/cdx
```

Default query prefix:

```text
http://www.thepowerof10.info/athletes/profile.aspx?athleteid=
```

CDX parameters:

- `matchType=prefix`
- `output=json`
- `fl=original,timestamp,statuscode`
- `filter=statuscode:200`
- `showResumeKey=true`
- `limit=5000`
- `gzip=false`

For each CDX row, `athlete_id_from_original()` extracts the `athleteid` query parameter.

The manifest keeps only the newest timestamp for each athlete id:

```csv
athleteid,timestamp,original,wayback_url
13341,YYYYMMDDhhmmss,http://www.thepowerof10.info/athletes/profile.aspx?athleteid=13341,https://web.archive.org/web/YYYYMMDDhhmmssid_/...
```

### 8.3 Resume State

State path:

```text
imports/wayback_profiles/latest_profile_captures.state.json
```

State contains:

- `resume_key`
- `latest`
- `pages`
- `rows`

The state is written atomically via a temporary file and replace. This allows long CDX scans to resume without starting over.

### 8.4 HTML Download

Manifest output:

```text
imports/wayback_profiles/latest_profile_captures.csv
```

HTML output:

```text
imports/wayback_profiles/html/<athleteid>.html
```

Failure log:

```text
imports/wayback_profiles/download_failures.csv
```

Downloader behavior:

- skips existing HTML unless `--force` is passed;
- retries archive requests up to 5 times;
- uses exponential backoff up to 30 seconds;
- supports `--download-only` to reuse an existing manifest;
- supports `--max-downloads` for test batches;
- sleeps between requests, default `0.25` seconds.

### 8.5 Wayback HTML Parsing

`app.wayback_html` parses saved HTML files with BeautifulSoup.

Key dataclasses:

- `WaybackManifestRow`
- `WaybackSection`
- `WaybackAthlete`

Important selectors:

| Data | Selector / logic |
| --- | --- |
| Profile title/name | `#cphBody_pnlMain h2` or fallback first `h2` |
| Athlete details panel | `#cphBody_pnlAthleteDetails` |
| Best performance table | `#cphBody_divBestPerformances table` |
| Historical performances table | `#cphBody_pnlPerformances table.alternatingrowspanel` |

Parsed athlete fields:

- `athlete_id`
- `display_name`
- `profile_name`
- `runner_name`
- `club`
- `gender`
- `age_group`
- `source_url`
- `fetched_at`
- `best_headers`
- `best_rows`
- `sections`
- `html_path`
- `wayback_url`

### 8.6 Details Parsing

`parse_details()` reads two-column label/value pairs from `#cphBody_pnlAthleteDetails`.

Recognized values used by the importer:

- `Club`
- `Gender`
- `Age Group`

### 8.7 Best Performances Parsing

`parse_best_performances()`:

- reads the first non-empty row as headers;
- ignores repeated header rows;
- pads short rows to header length;
- stores each row as a list of strings.

These become:

- `athletes.best_headers`
- `athlete_best_performance_rows.cells`

### 8.8 Historical Section Parsing

`parse_performance_sections()` walks the Power of 10 performance table.

Section header rule:

- a row with one `td`;
- `colspan="12"`;
- non-empty joined text.

For each section:

- `title`: row text;
- `year`: parsed with `extract_year()`;
- `columns`: default Power of 10 result columns until a header row is found;
- `metadata`: source marker, HTML filename, Wayback URL, and capture timestamp.

Default columns:

```text
["Event", "Perf", "", "", "", "Pos", "", "", "", "Venue", "Meeting", "Date"]
```

Result row parsing requires at least 12 cells and maps:

| Cell index | Field |
| --- | --- |
| `0` | `event` |
| `1` | `perf` |
| `5` | `pos` |
| `9` | `venue` |
| `10` | `meeting` |
| `11` | `date` |

The venue link, if present, becomes `venue_url`. If there is no anchor URL, a fallback local-style Power of 10 result query URL is built from event, venue, and date.

Extra non-empty columns are stored in `extra` using a normalized column label or fallback `col_<index>`.

If a parsed venue URL has query parameters, they are added to `extra` with `result_<key>`.

### 8.9 URL Normalization

Profile and result URLs are normalized to HTTPS where possible.

Examples:

- `http://www.thepowerof10.info/...` -> `https://www.thepowerof10.info/...`
- `http://thepowerof10.info/...` -> `https://thepowerof10.info/...`
- `/path` -> `https://thepowerof10.info/path`
- `../results/...` -> resolved relative to `https://thepowerof10.info/athletes/`

## 9. Wayback Import And Merge Logic

File:

- `app/import_wayback_profiles.py`

Input:

```text
--html-dir /imports/wayback_profiles/html
--manifest /imports/wayback_profiles/latest_profile_captures.csv
--limit 25
--synthetic-id-offset 9000000000
```

Default synthetic id offset:

```text
9,000,000,000
```

### 9.1 Existing Athlete State

`load_athlete_states()` loads each athlete with:

- identity fields;
- source URL;
- whether best rows exist;
- count of `truepb_results` sections;
- count of non-TruePB sections.

`AthleteState.has_powerof10_profile` is true if any of these exist:

- `profile_name`;
- best-performance rows;
- non-TruePB sections.

`AthleteState.is_truepb_only` is true when:

- the athlete does not have a Power of 10 profile;
- and has one or more `truepb_results` sections.

### 9.2 Synthetic TruePB Candidate Index

`build_truepb_candidate_index()` only indexes athletes where:

- `athlete_id >= synthetic_id_offset`;
- `state.is_truepb_only` is true.

It indexes normalized candidate names from:

- `display_name`;
- `profile_name`;
- `runner_name`.

This is how Wayback profiles can later replace synthetic TruePB-only athlete ids with real Po10 athlete ids.

### 9.3 Match Scoring

`find_synthetic_match()` compares a parsed Wayback athlete against synthetic TruePB-only candidates.

Base score:

- exact normalized name match: `100`

Additional score:

- club matches: `+30`
- gender matches: `+10`
- age group matches: `+10`

Conflict rules:

- if both athlete and candidate have club values and they differ, candidate is rejected;
- same for gender and age group;
- if top score ties, import skips as ambiguous.

Acceptance rules:

- score `>= 130` accepts;
- score `>= 110` accepts only if there is exactly one scored candidate;
- otherwise match is ambiguous.

This gives strong preference to exact name + club and avoids unsafe merges.

### 9.4 Import Decision Matrix

| Existing condition | Action |
| --- | --- |
| Real athlete id exists and already has Po10 profile data | skip as `skipped_existing_powerof10` |
| Real athlete id exists but only has `truepb_results` | enrich same row as `enriched_existing_truepb` |
| Real athlete id missing, one safe synthetic match found | create real athlete id and move synthetic rows as `merged_synthetic` |
| Real athlete id missing, no synthetic match | insert new Wayback athlete as `inserted_new_wayback` |
| Real athlete id missing, synthetic match ambiguous | skip as `skipped_ambiguous_match` |
| Parse or database error | report as `error` |

### 9.5 Database Writes

For each successfully imported Wayback athlete:

1. `ensure_target_athlete()` inserts or updates the `athletes` row.
2. `move_synthetic_athlete()` moves best rows, sections, and performances from synthetic id to real Po10 id, then deletes the synthetic athlete.
3. `insert_best_rows()` replaces best rows for the real athlete.
4. `insert_wayback_sections()` appends sections with `source_kind='powerof10_wayback'`.
5. Each Wayback result row becomes an `athlete_performances` record with parsed `result_date`.
6. `refresh_athlete_aggregates()` updates counts and first/last year.
7. The transaction commits per athlete.

Import report fields:

```text
athlete_id,target_athlete_id,html_file,display_name,club,action,reason,matched_synthetic_id
```

Default report path:

```text
imports/wayback_profiles/import_report.csv
```

## 10. TruePB Result Import And Export

There are two ways to bring TruePB race results into the live app:

1. Direct JSON append with `app.import_truepb_results`.
2. Generated SQL from a TruePB DB or SQL dump with `app.export_truepb_results_sql`.

### 10.1 Manual JSON Import

File:

- `app/import_truepb_results.py`

Sample input:

```json
[
  {
    "athlete_id": 13341,
    "title": "2026 TRUEPB Results",
    "year": 2026,
    "columns": ["Event", "Perf", "Pos", "Venue", "Meeting", "Date"],
    "results": [
      {
        "event": "5K",
        "perf": "15:02",
        "pos": "3",
        "venue": "Leeds",
        "venue_url": "https://example.com/results/123",
        "meeting": "TruePB Summer 5K",
        "date": "2026-05-14"
      }
    ]
  }
]
```

Rules:

- JSON root must be a list.
- Athlete must already exist.
- A new `athlete_performance_sections` row is appended.
- Section `source_kind` is `truepb_results`.
- Unknown item-level fields go into section `metadata`.
- Unknown result-level fields go into performance `extra`.
- Aggregates are refreshed for each athlete.

This importer intentionally does not create new athletes. It is a simple append path for known athletes.

### 10.2 TruePB SQL Exporter

File:

- `app/export_truepb_results_sql.py`

Inputs:

- live TruePB database via `--source-dsn` / `SOURCE_DATABASE_URL`; or
- plain PostgreSQL dump via `--sql`.

Required:

- `--year`
- `--output`

Optional:

- `--start-date`, default January 1 of `--year`;
- `--site-root`, default `https://truepb.net`;
- `--synthetic-id-offset`, default `9000000000`;
- `--skip-dnf`.

### 10.3 Source Tables Used

From live DB:

- `race_results`
- `races`
- `runners`

From a plain SQL dump:

- `races`
- `race_results`
- `runners`

The dump parser again reads PostgreSQL `COPY` sections directly.

### 10.4 Race Filtering

Only races are selected where:

- `race.date >= start_date`;
- `race.date < Jan 1 of next year`;
- race date is inside requested `--year`;
- race results are not soft deleted.

### 10.5 Athlete Id Resolution

`target_athlete_id()`:

- if `runner.powerof10_athlete_id` is present and positive, use it;
- otherwise use `synthetic_id_offset + runner_id`.

This means TruePB-only runners can be imported before their archived Power of 10 profile is known. Later, `import_wayback_profiles.py` can safely merge a synthetic id into the real athlete id.

### 10.6 Event Label Creation

`build_event_label()` derives a Power of 10 style event label from distance and race flags.

Distance examples:

| Distance | Label |
| --- | --- |
| near `42.195 km` | `Marathon` |
| near `21.0975 km` | `Half Marathon` |
| near `10 km` | `10K` |
| near `5 km` | `5K` |
| near `3 km`, track | `3000m` |
| near `3 km`, non-track | `3K` |
| near mile | `Mile` |
| near `0.8 km` | `800m` |
| near `0.4 km` | `400m` |

Suffix rules:

- cross country adds `XC` if not already present;
- relay adds `Relay Leg` or `Relay Leg <number>`;
- short leg adds `Short Leg`;
- long leg adds `Long Leg`.

### 10.7 Performance Formatting

`build_perf()`:

- returns `DNF` when `did_not_finish` is true;
- otherwise formats `finish_time_seconds`;
- otherwise formats `watch_time`;
- otherwise returns `None`.

`format_race_time()`:

- `h:mm:ss` for hour-plus performances;
- `m:ss` otherwise.

`build_position()`:

- returns no position for DNF;
- prefers `xc_finish_position`;
- otherwise uses computed `finish_position`.

### 10.8 Age Group Inference

TruePB export infers age group from:

- result flags: `is_u13`, `is_u15`, `is_u17`, `is_u20`;
- runner age:
  - `< 13`: `U13`
  - `< 15`: `U15`
  - `< 17`: `U17`
  - `< 20`: `U20`
  - `< 23`: `U23`
  - `>= 35`: `V35`
  - otherwise `Senior`

### 10.9 Extra Metadata

Each generated TruePB result stores `extra` JSON including:

- `source: truepb_results_db`
- `year`
- `runner_id`
- `race_id`
- `result_id`
- `powerof10_athlete_id`
- `used_synthetic_athlete_id`
- timing fields;
- race score;
- DNF flag;
- notes;
- relay/leg flags;
- age flags;
- distance fields;
- course and eligibility flags;
- cross country, road, track, relay, regional/national flags;
- race group metadata.

This is deliberately verbose. The display pages only need a subset today, but keeping the source facts allows later blog posts, filters, audits, or richer pages.

### 10.10 Generated SQL Behavior

The exporter writes an import SQL file that:

1. Opens a transaction.
2. Creates temp tables:
   - `tmp_truepb_athletes`
   - `tmp_truepb_results`
3. Inserts exported athlete metadata and result rows into temp tables.
4. Upserts `athletes`.
5. Deletes replaceable `truepb_results` sections for the requested year.
6. Deletes TruePB performance rows on or after `start_date`.
7. Preserves earlier rows in the same year before `start_date`.
8. Creates a `truepb_results` section for that year if absent.
9. Updates section metadata with generated source details.
10. Reorders preserved rows after incoming rows.
11. Inserts incoming rows into the year section.
12. Refreshes athlete aggregate counts.
13. Commits.

The key incremental behavior is:

- rows before `--start-date` are preserved;
- rows on or after `--start-date` are refreshed;
- generated SQL can safely update recent results without wiping older same-year rows.

## 11. Historical Results Model

Historical results come from two places:

1. `powerof10_cache` sections in the legacy dump.
2. `powerof10_wayback` sections parsed from archived profile HTML.

Both are represented as:

```text
athlete -> sections -> performances
```

Section titles often encode useful context:

```text
<year> <age group> <club>
```

The system uses the section title to infer:

- `year`, via the leading four digits;
- age group;
- club, using newest matching section.

Historical rows preserve original Power of 10 display fields:

- event;
- performance;
- position;
- venue;
- meeting;
- date.

Dates are stored twice:

- `date_text`: original source text;
- `result_date`: parsed date if parseable.

When a row date is missing or not parseable, some result pages can fall back to the section year.

## 12. Web Application Design

File:

- `app/web.py`

The Flask app is created as:

```python
app = Flask(__name__, template_folder="app/templates")
```

Database access uses `get_conn()` from `app/db.py`, returning dict-like rows.

### 12.1 Static Assets

Routes:

- `/favicon.ico`
- `/thepowerof10.info/<path:asset_path>`

The app serves archived Power of 10 CSS, JS, images, logos, and sample profile assets from `thepowerof10.info/`.

This lets the rebuilt site look and behave closer to the original archive rather than a new unrelated UI.

### 12.2 Health Route

```text
GET /healthz
```

Returns:

```json
{"status": "ok"}
```

### 12.3 Home And Athlete Search

Routes:

- `/`
- `/athletes`

The search UI can filter by:

- free query `q`;
- surname;
- first name;
- club.

Search checks:

- `display_name`
- `profile_name`
- `runner_name`
- `club`
- `athlete_id` text, for free query.

Summary counts come from `summary_counts()`:

- total athletes;
- total performances.

### 12.4 Athlete Profile

Routes:

- `/athletes/<athlete_id>`
- `/athletes/profile.aspx?athleteid=<id>`

The second route preserves the old Power of 10 URL style.

Profile load sequence:

1. Load athlete record.
2. Load best-performance rows.
3. If best table is missing, generate one from all stored performances.
4. Overlay TruePB year bests for `TRUEPB_SB_OVERLAY_YEAR = 2026`.
5. Load all sections and rows.
6. Add `local_results_url` for rows that can point to a local meeting results page.
7. Render `athlete.html`.

Section sorting:

- year descending;
- TruePB result sections before non-TruePB for the same year;
- then section order.

### 12.5 Generated Best Table

If a profile has no imported best rows, `load_generated_best_table()` builds a best table from performance rows.

It:

- ignores empty event/perf rows;
- normalizes event labels;
- parses marks into comparable numeric values;
- chooses lower values for running times;
- chooses higher values for field/points events;
- filters unsuitable TruePB rows such as relay and XC for best table generation;
- produces headers:

```text
Event, PB, <year1>, <year2>, ...
```

### 12.6 TruePB Best Overlay

`overlay_best_rows_for_year()` overlays year bests from TruePB result rows onto an imported Power of 10 best table.

Default year:

```text
2026
```

It inserts a `2026` column after `PB` if missing, fills the event's year best, and updates PB if the TruePB result beats the existing PB.

### 12.7 Results Lookup

Routes:

- `/results`
- `/results/`
- `/results/resultslookup.aspx`

Filters:

- event;
- meeting;
- venue;
- date from;
- date to;
- year;
- page.

Results are grouped by:

- normalized meeting name;
- normalized venue name;
- effective meeting date.

Page size:

```text
250 meetings
```

`effective_result_date_sql()` uses:

1. `athlete_performances.result_date`;
2. parsed `date_text`;
3. section year fallback where needed.

### 12.8 Results Detail

Route:

```text
/results/results.aspx
```

Parameters:

- `meeting`
- `venue`
- `date`
- optional `event`

The detail page:

- finds rows for the normalized meeting, venue, and date;
- groups rows by event;
- computes PB/SB flags by comparing against the athlete's historical rows;
- sorts event rows by position and performance;
- renders `results_detail.html`.

### 12.9 Rankings

Routes:

- `/rankings`
- `/rankings/`
- `/rankings/rankinglist.aspx`
- `/rankings/disabilityrankinglistrequest.aspx`

`rankings_support.py` provides:

- event label extraction from `partials/rankings_toolbar.html`;
- area labels;
- sex labels;
- age-group labels;
- event alias normalization;
- mark parsing;
- ranking direction.

Ranking behavior:

- one best mark per athlete is selected;
- lower is better for running times;
- higher is better for field and points events;
- all-time rankings are capped at 500 rows for performance;
- unsupported filters produce notes instead of pretending precision.

Known unsupported ranking filters in current data:

- region/nation filtering;
- indoor-only splits;
- disability class filtering;
- mixed rankings.

## 13. Routing Summary

| Route | Handler | Purpose |
| --- | --- | --- |
| `/healthz` | `healthz` | Health check |
| `/favicon.ico` | `favicon` | Favicon |
| `/thepowerof10.info/<path>` | `legacy_assets` | Archived static assets |
| `/` | `home` | Landing/search page |
| `/about` | `about` | Static about page |
| `/athletes` | `athlete_index` | Athlete search page |
| `/athletes/<id>` | `athlete_profile` | Rebuilt profile page |
| `/athletes/profile.aspx?athleteid=<id>` | `athlete_profile` | Old Po10-style profile URL |
| `/rankings` | `rankings` | Ranking form/default page |
| `/rankings/rankinglist.aspx` | `rankings_list` | Ranking results |
| `/rankings/disabilityrankinglistrequest.aspx` | `disability_rankings_redirect` | Redirect to original Po10 URL |
| `/results` | `results_lookup` | Meeting/result search |
| `/results/resultslookup.aspx` | `results_lookup` | Old Po10-style lookup URL |
| `/results/results.aspx` | `results_detail` | Meeting detail/results page |

## 14. Syncing Profiles Back Into TruePB

Files:

- `app/sync_profiles_to_truepb.py`
- `app/export_profiles_to_truepb_sql.py`

These scripts move Po10-backed profiles from `truepb_live` back into the original TruePB database shape.

### 14.1 Direct Sync

`sync_profiles_to_truepb.py` connects to:

- source `truepb_live`, via `SOURCE_DATABASE_URL`;
- target TruePB DB, via `TARGET_DATABASE_URL`.

It loads source athletes that have real Po10 profile evidence:

- `profile_name`;
- best-performance rows;
- or any non-`truepb_results` section.

It skips target athletes that already exist in:

- `powerof10_profiles`;
- `powerof10_cache`.

Runner matching order:

1. Existing target runner by `powerof10_athlete_id` or athlete id in URL.
2. Exact normalized name + club.
3. Exact normalized name.
4. Insert new runner if no safe match and insertion is allowed.

It then writes:

- `powerof10_profiles`;
- `powerof10_cache`;
- `powerof10_event_pbs`;
- `powerof10_event_years`;
- `powerof10_performances`.

Dry-run and verbose modes are supported.

### 14.2 SQL Export Sync

`export_profiles_to_truepb_sql.py` reads a plain SQL dump of `truepb_live` and generates SQL that can be applied to the target TruePB database.

It exists for environments where direct DB-to-DB sync is not preferred.

It:

- parses `athletes`, `athlete_best_performance_rows`, `athlete_performance_sections`, and `athlete_performances` from a dump;
- ignores `truepb_results` sections for profile export;
- plans runner matches against the target DB;
- writes a report;
- writes SQL with temp tables and target inserts/updates.

## 15. Operational Workflows

### 15.1 Start Database

```bash
docker compose up -d db
```

### 15.2 Import Legacy Cache Dump

```bash
docker compose run --rm web python -m app.import_legacy_dump --sql /imports/legacy/16-04-2026.sql
```

### 15.3 Start Web App

```bash
docker compose up -d web
```

Open:

```text
http://localhost:8001
```

The README currently says `http://localhost:8080`, but `docker-compose.yml` maps the web container as `8001:8000`.

### 15.4 Build Wayback Manifest

```bash
python3 -m app.wayback_profiles
```

### 15.5 Download Wayback HTML

```bash
python3 -m app.wayback_profiles --download
```

Resume missing downloads only:

```bash
python3 -m app.wayback_profiles --download-only
```

Small test:

```bash
python3 -m app.wayback_profiles --download --max-downloads 25
```

### 15.6 Import Wayback Profiles

```bash
docker compose build web
docker compose run --rm web python -m app.import_wayback_profiles \
  --html-dir /imports/wayback_profiles/html
```

Small test:

```bash
docker compose run --rm web python -m app.import_wayback_profiles \
  --html-dir /imports/wayback_profiles/html \
  --limit 25
```

### 15.7 Export TruePB Results From Dump

```bash
docker compose run --rm web python -m app.export_truepb_results_sql \
  --sql /imports/legacy/16-04-2026.sql \
  --year 2026 \
  --start-date 2026-05-01 \
  --output /imports/generated/truepb-results-2026-from-2026-05-01.sql
```

Apply generated SQL:

```bash
docker compose exec -T db psql -U truepb -d truepb_live < imports/generated/truepb-results-2026-from-2026-05-01.sql
```

### 15.8 Import Manual TruePB JSON

```bash
docker compose run --rm web python -m app.import_truepb_results \
  --json /imports/examples/truepb-results.sample.json
```

### 15.9 Sync Profiles Back To TruePB

Dry run:

```bash
SOURCE_DATABASE_URL='postgresql://truepb:truepb@localhost:5432/truepb_live' \
TARGET_DATABASE_URL='postgresql://truepb:password@target-host:5432/truepb' \
python3 -m app.sync_profiles_to_truepb --dry-run
```

Limited dry run:

```bash
SOURCE_DATABASE_URL='postgresql://truepb:truepb@localhost:5432/truepb_live' \
TARGET_DATABASE_URL='postgresql://truepb:password@target-host:5432/truepb' \
python3 -m app.sync_profiles_to_truepb --dry-run --limit 25
```

Verbose dry run:

```bash
SOURCE_DATABASE_URL='postgresql://truepb:truepb@localhost:5432/truepb_live' \
TARGET_DATABASE_URL='postgresql://truepb:password@target-host:5432/truepb' \
python3 -m app.sync_profiles_to_truepb --dry-run --verbose
```

## 16. Key Implementation Details For A Build Blog

### 16.1 The Problem

Power of 10 athlete data exists in several states:

- legacy cache data already captured in a TruePB-related database;
- old profile pages still available through Wayback;
- new TruePB race results;
- runners that do not yet have a known Po10 athlete id.

The project turns those disconnected sources into one coherent local site.

### 16.2 The Data Unification Choice

Rather than creating separate tables per source, the project normalizes every source into:

```text
athlete -> best rows
athlete -> performance sections -> performance rows
```

This gives every page one query model while preserving source-specific facts in `source_kind`, section `metadata`, and row `extra`.

### 16.3 The Wayback Automation Story

The Wayback process is automatic because the CDX API can enumerate all archived profile URLs by prefix. The script does not need a manually prepared list of athlete ids. It scans the archive index, extracts `athleteid`, keeps the latest timestamp per athlete, writes a resumable state file, and can then download each HTML file.

This turns the Wayback Machine into a bulk profile source:

```text
CDX prefix scan -> latest capture per athlete -> local HTML file -> BeautifulSoup parser -> normalized DB rows
```

### 16.4 The Synthetic Athlete Story

TruePB can have results for runners before the real Power of 10 athlete id is known. The exporter handles this with:

```text
synthetic_athlete_id = 9000000000 + runner_id
```

That keeps TruePB result imports stable and non-conflicting.

Later, when a Wayback profile appears for the real athlete id, the importer can:

1. parse canonical profile data;
2. exact-match name against synthetic TruePB-only candidates;
3. require supporting club/gender/age-group evidence where available;
4. move synthetic rows to the real athlete id;
5. delete the synthetic athlete.

This is the bridge between modern TruePB results and historical Power of 10 identity.

### 16.5 The Incremental Results Story

The SQL exporter is built for repeated updates. With `--start-date`, it refreshes only newer results and preserves earlier rows in the same year.

That matters because race data changes during a season. The import SQL can update recent TruePB data without rebuilding the whole database or losing earlier imported rows.

### 16.6 The Historical Results Story

Historical results are not just a flat list. They are grouped by year/age/club sections because that is how Power of 10 profile pages were structured.

The app keeps section titles and row order so the rebuilt profile page can remain close to the source. It also parses dates and years so the same data can power newer pages like result lookup and rankings.

### 16.7 The UI Compatibility Story

The app keeps old URL shapes:

```text
/athletes/profile.aspx?athleteid=13341
/results/resultslookup.aspx
/results/results.aspx
/rankings/rankinglist.aspx
```

It also serves archived CSS/JS/images from `thepowerof10.info/`.

That makes the rebuilt site usable both as a modern Flask app and as a Power of 10 style archive.

## 17. Known Limitations And Design Tradeoffs

- Wayback parsing depends on legacy Power of 10 HTML selectors. If archived pages use different markup, parsing may fail or skip sections.
- The Wayback importer is safe to rerun, but it scans all HTML files each time rather than maintaining imported-file state.
- Manual JSON TruePB import only supports existing athletes.
- Region/nation rankings are not fully available because imported athlete/result data does not include all necessary region fields.
- Indoor-only ranking splits are not separately modeled.
- Disability ranking class fields are not imported.
- Mixed rankings are not derived from current athlete-level data.
- TruePB-to-Po10 merge intentionally skips ambiguous synthetic matches, which avoids bad merges but may require manual follow-up.
- README says the app opens on port `8080`, but current Docker Compose maps it to `8001`.

## 18. File-by-File Design Notes

### `app/db.py`

Owns database URL resolution, connection context manager, and schema bootstrapping.

### `schema.sql`

Defines the normalized live-site schema.

### `app/legacy_dump.py`

Parses plain PostgreSQL dump files, extracts legacy profile/cache/runner data, resolves trusted athlete metadata, and provides shared helpers for date/year parsing.

### `app/import_legacy_dump.py`

Resets live tables and imports the legacy Power of 10 cache as `powerof10_cache` rows.

### `app/wayback_profiles.py`

Scans Wayback CDX, writes latest-capture manifest, stores resumable scan state, downloads HTML, and logs failures.

### `app/wayback_html.py`

Parses saved athlete profile HTML into structured profile, best table, section, and result dataclasses.

### `app/import_wayback_profiles.py`

Imports parsed Wayback profiles into the live DB, enriches TruePB-only athletes, and merges safe synthetic athletes into real Po10 athlete ids.

### `app/import_truepb_results.py`

Simple JSON importer for appending TruePB result sections to existing athletes.

### `app/export_truepb_results_sql.py`

Reads TruePB race data from a DB or SQL dump, converts it to Po10-like result rows, and generates incremental import SQL for `truepb_live`.

### `app/web.py`

Runs the Flask web app: home/search, athlete profiles, result lookup/detail, rankings, old-style URL compatibility, and static asset serving.

### `app/rankings_support.py`

Provides ranking labels, aliases, mark parsing, and ranking direction logic.

### `app/sync_profiles_to_truepb.py`

Syncs Po10-backed profiles from `truepb_live` back into the original TruePB database using safe runner matching.

### `app/export_profiles_to_truepb_sql.py`

Offline SQL-dump version of the profile sync path. Generates target TruePB import SQL and a report.

### `app/templates/*`

HTML rendering layer for home, athlete profiles, results lookup/detail, rankings, and shared layout.

### `thepowerof10.info/*`

Archived static assets and sample original profile resources used to preserve the Power of 10 look and URLs.

## 19. Suggested Blog Outline

1. Rebuilding an athletics archive from mixed sources.
2. Why a normalized athlete/section/performance schema was chosen.
3. Importing the legacy Power of 10 cache dump.
4. Using the Wayback CDX API to discover profile pages automatically.
5. Parsing archived profile HTML into structured data.
6. Why profile identity matters more than raw results.
7. Handling TruePB-only runners with synthetic ids.
8. Safely merging synthetic athletes into real Po10 athlete ids.
9. Exporting modern TruePB results into historical profile sections.
10. Preserving old Power of 10 URLs and static assets in Flask.
11. Building rankings and result pages from the same normalized rows.
12. Syncing reconstructed profiles back into TruePB.
13. What remains hard: ambiguous matches, incomplete ranking metadata, and fragile legacy HTML.
