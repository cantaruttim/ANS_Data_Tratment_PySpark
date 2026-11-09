
BEGIN;

-- =========================================================
-- 1. DATASETS
-- Catalog of datasets available in the platform.
-- =========================================================

CREATE TABLE datasets (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    code VARCHAR(100) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    source_url TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_datasets PRIMARY KEY (id),
    CONSTRAINT uq_datasets_code UNIQUE (code),

    CONSTRAINT ck_datasets_code_not_blank
        CHECK (BTRIM(code) <> ''),

    CONSTRAINT ck_datasets_name_not_blank
        CHECK (BTRIM(name) <> ''),

    CONSTRAINT ck_datasets_source_url_not_blank
        CHECK (BTRIM(source_url) <> '')
);


-- =========================================================
-- 2. INGESTION RUNS
-- Execution history for each dataset ingestion.
-- =========================================================

CREATE TABLE ingestion_runs (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    dataset_id BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    trigger_type VARCHAR(20) NOT NULL DEFAULT 'MANUAL',
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    parameters JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_ingestion_runs PRIMARY KEY (id),

    CONSTRAINT fk_ingestion_runs_dataset
        FOREIGN KEY (dataset_id)
        REFERENCES datasets (id)
        ON DELETE RESTRICT,

    CONSTRAINT ck_ingestion_runs_status
        CHECK (
            status IN (
                'PENDING',
                'RUNNING',
                'SUCCEEDED',
                'PARTIAL',
                'FAILED',
                'CANCELLED'
            )
        ),

    CONSTRAINT ck_ingestion_runs_trigger_type
        CHECK (
            trigger_type IN (
                'MANUAL',
                'SCHEDULED',
                'API'
            )
        ),

    CONSTRAINT ck_ingestion_runs_timestamps
        CHECK (
            finished_at IS NULL
            OR started_at IS NULL
            OR finished_at >= started_at
        ),

    CONSTRAINT ck_ingestion_runs_pending
        CHECK (
            status <> 'PENDING'
            OR (
                started_at IS NULL
                AND finished_at IS NULL
            )
        ),

    CONSTRAINT ck_ingestion_runs_running
        CHECK (
            status <> 'RUNNING'
            OR (
                started_at IS NOT NULL
                AND finished_at IS NULL
            )
        ),

    CONSTRAINT ck_ingestion_runs_terminal_finish
        CHECK (
            status NOT IN (
                'SUCCEEDED',
                'PARTIAL',
                'FAILED',
                'CANCELLED'
            )
            OR finished_at IS NOT NULL
        ),

    CONSTRAINT ck_ingestion_runs_terminal_start
        CHECK (
            status NOT IN (
                'SUCCEEDED',
                'PARTIAL',
                'FAILED'
            )
            OR started_at IS NOT NULL
        )
);


-- =========================================================
-- 3. INGESTED FILES
-- Individual files discovered, downloaded and stored.
-- =========================================================

CREATE TABLE ingested_files (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    ingestion_run_id UUID NOT NULL,
    source_file_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    object_key TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'DISCOVERED',
    checksum_sha256 VARCHAR(64),
    file_size_bytes BIGINT,
    source_modified_at TIMESTAMPTZ,
    downloaded_at TIMESTAMPTZ,
    stored_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_ingested_files PRIMARY KEY (id),

    CONSTRAINT fk_ingested_files_run
        FOREIGN KEY (ingestion_run_id)
        REFERENCES ingestion_runs (id)
        ON DELETE RESTRICT,

    CONSTRAINT uq_ingested_files_run_source
        UNIQUE (ingestion_run_id, source_url),

    CONSTRAINT ck_ingested_files_status
        CHECK (
            status IN (
                'DISCOVERED',
                'DOWNLOADED',
                'STORED',
                'FAILED',
                'SKIPPED'
            )
        ),

    CONSTRAINT ck_ingested_files_file_size
        CHECK (
            file_size_bytes IS NULL
            OR file_size_bytes >= 0
        ),

    CONSTRAINT ck_ingested_files_checksum
        CHECK (
            checksum_sha256 IS NULL
            OR checksum_sha256 ~ '^[0-9a-fA-F]{64}$'
        ),

    CONSTRAINT ck_ingested_files_downloaded
        CHECK (
            status NOT IN ('DOWNLOADED', 'STORED')
            OR downloaded_at IS NOT NULL
        ),

    CONSTRAINT ck_ingested_files_stored
        CHECK (
            status <> 'STORED'
            OR (
                object_key IS NOT NULL
                AND stored_at IS NOT NULL
                AND checksum_sha256 IS NOT NULL
                AND file_size_bytes IS NOT NULL
            )
        ),

    CONSTRAINT ck_ingested_files_stored_after_download
        CHECK (
            stored_at IS NULL
            OR downloaded_at IS NOT NULL
        ),

    CONSTRAINT ck_ingested_files_object_key
        CHECK (
            object_key IS NULL
            OR BTRIM(object_key) <> ''
        )
);


-- =========================================================
-- 4. INDEXES
-- Support common operational queries.
-- =========================================================

CREATE INDEX idx_ingestion_runs_dataset_created
    ON ingestion_runs (dataset_id, created_at DESC);

CREATE INDEX idx_ingestion_runs_status_created
    ON ingestion_runs (status, created_at DESC);

CREATE INDEX idx_ingested_files_run
    ON ingested_files (ingestion_run_id);

CREATE INDEX idx_ingested_files_status_created
    ON ingested_files (status, created_at DESC);

CREATE UNIQUE INDEX uq_ingested_files_object_key
    ON ingested_files (object_key)
    WHERE object_key IS NOT NULL;

COMMIT;
