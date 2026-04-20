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

## Notes

- The importer trusts `powerof10_profiles` for athlete naming and uses `runners` only as supporting metadata.
- The schema stores both section structure and flattened performances so you can append TruePB results later without re-importing the raw dump.
- Static assets are served from the archived `thepowerof10.info` folder already in this directory.
- The legacy SQL dump is about `739MB`, so `.gitignore` excludes it by default even though it is present in this folder for local bootstrap.
