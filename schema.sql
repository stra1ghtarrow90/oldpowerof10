CREATE TABLE IF NOT EXISTS athletes (
    athlete_id BIGINT PRIMARY KEY,
    display_name TEXT NOT NULL,
    profile_name TEXT,
    runner_name TEXT,
    club TEXT,
    gender TEXT,
    age INTEGER,
    age_group TEXT,
    source_url TEXT NOT NULL,
    fetched_at TIMESTAMPTZ,
    best_headers JSONB NOT NULL DEFAULT '[]'::jsonb,
    performance_count INTEGER NOT NULL DEFAULT 0,
    section_count INTEGER NOT NULL DEFAULT 0,
    first_year INTEGER,
    last_year INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS athlete_best_performance_rows (
    athlete_id BIGINT NOT NULL REFERENCES athletes (athlete_id) ON DELETE CASCADE,
    row_order INTEGER NOT NULL,
    cells JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (athlete_id, row_order)
);

CREATE TABLE IF NOT EXISTS athlete_performance_sections (
    id BIGSERIAL PRIMARY KEY,
    athlete_id BIGINT NOT NULL REFERENCES athletes (athlete_id) ON DELETE CASCADE,
    source_kind TEXT NOT NULL,
    section_order INTEGER NOT NULL,
    title TEXT NOT NULL,
    year INTEGER,
    columns_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (athlete_id, source_kind, section_order)
);

CREATE TABLE IF NOT EXISTS athlete_performances (
    id BIGSERIAL PRIMARY KEY,
    athlete_id BIGINT NOT NULL REFERENCES athletes (athlete_id) ON DELETE CASCADE,
    section_id BIGINT NOT NULL REFERENCES athlete_performance_sections (id) ON DELETE CASCADE,
    source_kind TEXT NOT NULL,
    row_order INTEGER NOT NULL,
    event TEXT,
    perf TEXT,
    pos TEXT,
    venue TEXT,
    venue_url TEXT,
    meeting TEXT,
    date_text TEXT,
    result_date DATE,
    extra JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_athletes_display_name ON athletes (LOWER(display_name));
CREATE INDEX IF NOT EXISTS idx_athlete_sections_lookup ON athlete_performance_sections (athlete_id, year DESC, source_kind, section_order);
CREATE INDEX IF NOT EXISTS idx_athlete_performances_lookup ON athlete_performances (athlete_id, section_id, row_order);
CREATE INDEX IF NOT EXISTS idx_athlete_performances_result_date ON athlete_performances (result_date DESC NULLS LAST);
