CREATE TABLE jobs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key text NOT NULL UNIQUE
        CHECK (octet_length(idempotency_key) BETWEEN 1 AND 128),
    request jsonb NOT NULL CHECK (jsonb_typeof(request) = 'object'),
    status text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    worker_id text,
    lease_expires_at timestamptz,
    claim_version bigint NOT NULL DEFAULT 0 CHECK (claim_version >= 0),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts integer NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    failure_code text,
    failure_message text,
    CONSTRAINT attempts_bounded CHECK (attempt_count <= max_attempts),
    CONSTRAINT ownership_matches_state CHECK (
        (status = 'running' AND worker_id IS NOT NULL AND worker_id <> ''
            AND lease_expires_at IS NOT NULL AND claim_version > 0 AND attempt_count > 0)
        OR (status <> 'running' AND worker_id IS NULL AND lease_expires_at IS NULL)
    ),
    CONSTRAINT completion_matches_state CHECK (
        (status IN ('succeeded', 'failed', 'cancelled')) = (completed_at IS NOT NULL)
    )
);

CREATE INDEX jobs_queued_order ON jobs (created_at, id) WHERE status = 'queued';
CREATE INDEX jobs_expired_leases ON jobs (lease_expires_at, id) WHERE status = 'running';
CREATE INDEX jobs_list_order ON jobs (created_at, id);

CREATE TABLE job_effects (
    job_id uuid PRIMARY KEY REFERENCES jobs(id),
    claim_version bigint NOT NULL CHECK (claim_version > 0),
    result jsonb NOT NULL CHECK (jsonb_typeof(result) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Cross-table success/effect consistency is enforced by the finalization
-- transaction in Task 3 and independently checked by the chaos audit.
