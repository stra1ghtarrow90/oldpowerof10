# TruePB Po10 Live Site

This repo turns the reconstructed Power of 10 archive into a live Dockerized site backed by PostgreSQL.

## What It Includes

- `docker-compose.yml`: PostgreSQL + web app
- `app/import_legacy_dump.py`: imports the current `powerof10_cache` data from `imports/legacy/16-04-2026.sql`
- `app/import_truepb_results.py`: appends new sections/results for existing athletes from a JSON file
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

If the manifest already exists and you only want the HTML download pass:

```bash
python3 -m app.wayback_profiles --download-only
```

## Notes

- The importer trusts `powerof10_profiles` for athlete naming and uses `runners` only as supporting metadata.
- The schema stores both section structure and flattened performances so you can append TruePB results later without re-importing the raw dump.
- Static assets are served from the archived `thepowerof10.info` folder already in this directory.
- The legacy SQL dump is about `739MB`, so `.gitignore` excludes it by default even though it is present in this folder for local bootstrap.
# oldpowerof10
# oldpowerof10
