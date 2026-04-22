# TruePB Po10 Live Site

This repo turns the reconstructed Power of 10 archive into a live Dockerized site backed by PostgreSQL.

## What It Includes

- `docker-compose.yml`: PostgreSQL + web app
- `app/import_legacy_dump.py`: imports the current `powerof10_cache` data from `imports/legacy/16-04-2026.sql`
- `app/import_truepb_results.py`: appends new sections/results for existing athletes from a JSON file
- `app/import_wayback_profiles.py`: imports saved Wayback athlete profile HTML into the live DB and merges qualifying TruePB-only synthetic athletes into the real Po10 athlete id
- `app/web.py`: serves the athlete index and athlete profile pages from PostgreSQL
- `thepowerof10.info/`: the archived CSS, JS, and image assets used by the site
- `imports/legacy/16-04-2026.sql`: the existing dump used to seed the database locally

## Repo Layout

```text
truepb-po10-live/
  app/
  imports/
    examples/
      truepb-results.sample.json
    legacy/
      16-04-2026.sql
  thepowerof10.info/
  docker-compose.yml
  Dockerfile
  schema.sql
```

## Start It

1. Start PostgreSQL:

```bash
docker compose up -d db
```

2. Import the legacy Po10 cache dump:

```bash
docker compose run --rm web python -m app.import_legacy_dump --sql /imports/legacy/16-04-2026.sql
```

3. Start the web app:

```bash
docker compose up -d web
```

4. Open:

```text
http://localhost:8080
```

## Import New TruePB Results

The TruePB importer intentionally only supports **existing athletes** for now.
Adding brand-new athletes can come later.

Expected JSON shape:

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

Import it like this:

```bash
docker compose run --rm web python -m app.import_truepb_results --json /imports/examples/truepb-results.sample.json
```

## Export Wayback Athlete Profiles

To build a CSV of the latest archived Power of 10 athlete profile snapshot for each athlete:

```bash
python3 -m app.wayback_profiles
```

That writes:

- `imports/wayback_profiles/latest_profile_captures.csv`
- `imports/wayback_profiles/latest_profile_captures.state.json`

The state file lets the CDX scan resume if it gets interrupted.

To also download the latest archived HTML for each athlete profile:

```bash
python3 -m app.wayback_profiles --download
```

That writes one file per athlete to:

- `imports/wayback_profiles/html/<athleteid>.html`

Any failed HTML downloads are written to:

- `imports/wayback_profiles/download_failures.csv`

If the manifest already exists and you only want the HTML download pass:

```bash
python3 -m app.wayback_profiles --download-only
```

## Import Saved Wayback Profiles

After you have downloaded the archived athlete HTML, rebuild the web image once so the new HTML parser dependency is installed:

```bash
docker compose build web
```

If your downloaded HTML lives under `imports/wayback_profiles/html` and the manifest CSV is in `imports/wayback_profiles/latest_profile_captures.csv`, import it like this:

```bash
docker compose run --rm web python -m app.import_wayback_profiles \
  --html-dir /imports/wayback_profiles/html
```

You can test a smaller batch first:

```bash
docker compose run --rm web python -m app.import_wayback_profiles \
  --html-dir /imports/wayback_profiles/html \
  --limit 25
```

If the manifest CSV is somewhere else, pass it explicitly:

```bash
docker compose run --rm web python -m app.import_wayback_profiles \
  --html-dir /imports/wayback_profiles/html \
  --manifest /imports/wayback_profiles/latest_profile_captures.csv
```

What the importer does:

- skips any athlete id that already has imported Power of 10 profile data
- enriches an existing athlete row when that athlete id only has `truepb_results`
- auto-merges a synthetic TruePB athlete into the real Po10 athlete id when there is one exact-name match with supporting metadata and no ambiguity
- inserts a brand-new athlete when there is no existing real or synthetic match
- imports both best-known-performances and historical performance sections so the profile page and `/results` use the Wayback data

The importer writes a CSV report by default to:

- `imports/wayback_profiles/import_report.csv`

That default comes from the HTML directory, so if you import from some other folder it writes `import_report.csv` beside that folder unless you override `--report`.

That report tells you whether each athlete was:

- imported as a new Wayback athlete
- enriched onto an existing TruePB-only athlete
- merged from a synthetic TruePB athlete id
- skipped because a Po10 profile already exists
- skipped because the synthetic match was ambiguous

## Sync Imported Po10 Profiles Back Into TruePB

After you have imported legacy Po10 data and Wayback athlete profiles into `truepb_live`, you can sync those Po10-backed athletes into the original `truepb` database.

What this sync does:

- reads Po10-backed athletes from `truepb_live`
- skips any athlete id that already exists in target `powerof10_profiles`
- also skips any athlete id already present in target `powerof10_cache`
- tries to merge onto an existing target `runner` by:
  - existing `powerof10_athlete_id`
  - exact name + club
  - exact name
- inserts a new target `runner` only when there is no safe existing match
- writes target `powerof10_profiles`
- writes target `powerof10_cache`
- rebuilds target `powerof10_event_pbs`, `powerof10_event_years`, and `powerof10_performances` for the matched runner

Dry-run first:

```bash
SOURCE_DATABASE_URL='postgresql://truepb:truepb@localhost:5432/truepb_live' \
TARGET_DATABASE_URL='postgresql://truepb:password@target-host:5432/truepb' \
python3 -m app.sync_profiles_to_truepb --dry-run
```

Limit the first test batch if you want:

```bash
SOURCE_DATABASE_URL='postgresql://truepb:truepb@localhost:5432/truepb_live' \
TARGET_DATABASE_URL='postgresql://truepb:password@target-host:5432/truepb' \
python3 -m app.sync_profiles_to_truepb --dry-run --limit 25
```

Run with per-athlete output:

```bash
SOURCE_DATABASE_URL='postgresql://truepb:truepb@localhost:5432/truepb_live' \
TARGET_DATABASE_URL='postgresql://truepb:password@target-host:5432/truepb' \
python3 -m app.sync_profiles_to_truepb --dry-run --verbose
```

If you do not want full verbose output, the script prints a progress line every `100` athletes by default. Change that with:

```bash
--progress-every 25
```

Run the real sync:

```bash
SOURCE_DATABASE_URL='postgresql://truepb:truepb@localhost:5432/truepb_live' \
TARGET_DATABASE_URL='postgresql://truepb:password@target-host:5432/truepb' \
python3 -m app.sync_profiles_to_truepb
```

Sync just one athlete:

```bash
SOURCE_DATABASE_URL='postgresql://truepb:truepb@localhost:5432/truepb_live' \
TARGET_DATABASE_URL='postgresql://truepb:password@target-host:5432/truepb' \
python3 -m app.sync_profiles_to_truepb --athlete-id 13341
```

If you want to avoid creating any new target runners while testing matching, add:

```bash
--skip-insert-runners
```

The sync writes a CSV report by default to:

- `imports/generated/truepb_profile_sync_report.csv`

That report tells you whether each athlete was:

- skipped because the athlete already exists in target `powerof10_profiles`
- skipped because the athlete already exists in target `powerof10_cache`
- merged onto an existing target runner
- inserted as a new target runner
- skipped because the target runner match was ambiguous
- failed with an error

## Export Po10 Profiles From A truepb_live Dump Into TruePB

If you prefer not to connect directly from the live source DB into the target `truepb` DB, you can dump `truepb_live` and generate an import SQL file from that dump.

This exporter:

- reads a plain SQL dump of `truepb_live`
- extracts Po10-backed athletes only
- checks the live target `truepb` DB so it can:
  - skip athlete ids already in `powerof10_profiles`
  - skip athlete ids already in `powerof10_cache`
  - merge onto an existing `runner` when there is a safe match
  - create a new `runner` only when needed
- writes an import SQL file for `truepb`
- writes a CSV report showing what will happen

Important:

- use a plain SQL dump, not `pg_dump -Fc`
- this still needs live access to the target `truepb` DB so it can decide what to skip or merge

Create the dump:

```bash
docker compose exec -T db pg_dump -U truepb -d truepb_live > imports/generated/truepb_live.sql
```

Build the web image once so the exporter is available:

```bash
docker compose build web
```

Generate the import SQL and report:

```bash
docker compose run --rm \
  -e TARGET_DATABASE_URL='postgresql://truepb:password@target-host:5432/truepb' \
  web \
  python -m app.export_profiles_to_truepb_sql \
    --sql /imports/generated/truepb_live.sql \
    --output /imports/generated/po10-sync-into-truepb.sql \
    --report /imports/generated/po10-sync-report.csv \
    --verbose
```

Test a smaller batch first if you want:

```bash
docker compose run --rm \
  -e TARGET_DATABASE_URL='postgresql://truepb:password@target-host:5432/truepb' \
  web \
  python -m app.export_profiles_to_truepb_sql \
    --sql /imports/generated/truepb_live.sql \
    --output /imports/generated/po10-sync-into-truepb.sql \
    --report /imports/generated/po10-sync-report.csv \
    --limit 25 \
    --verbose
```

If you want to avoid creating new target runners while testing matching:

```bash
--skip-insert-runners
```

Then import the generated SQL into the target `truepb` DB:

```bash
psql 'postgresql://truepb:password@target-host:5432/truepb' < imports/generated/po10-sync-into-truepb.sql
```

Files written:

- `imports/generated/po10-sync-into-truepb.sql`
- `imports/generated/po10-sync-report.csv`

## Notes

- The importer trusts `powerof10_profiles` for athlete naming and uses `runners` only as supporting metadata.
- The schema stores both section structure and flattened performances so you can append TruePB results later without re-importing the raw dump.
- Static assets are served from the archived `thepowerof10.info` folder already in this directory.
- The legacy SQL dump is about `739MB`, so `.gitignore` excludes it by default even though it is present in this folder for local bootstrap.
