CREATE TABLE batches (
    id         uuid PRIMARY KEY,
    total      integer     NOT NULL CHECK (total BETWEEN 0 AND 1000),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE notifications (
    id                  uuid PRIMARY KEY,
    batch_id            uuid REFERENCES batches (id),
    idempotency_key     text UNIQUE,
    channel             text        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    recipient           text        NOT NULL,
    content             text        NOT NULL,
    priority            text        NOT NULL CHECK (priority IN ('high', 'normal', 'low')),
    -- Numeric form of priority so the relay can sort without a CASE expression.
    priority_rank       smallint    NOT NULL GENERATED ALWAYS AS (
                            CASE priority WHEN 'high' THEN 3 WHEN 'normal' THEN 2 ELSE 1 END
                        ) STORED,
    status              text        NOT NULL CHECK (status IN
                            ('pending', 'queued', 'processing', 'sent', 'failed', 'cancelled')),
    attempts            integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts        integer     NOT NULL DEFAULT 8 CHECK (max_attempts > 0),
    next_attempt_at     timestamptz NOT NULL,
    scheduled_at        timestamptz,
    provider_message_id text,
    last_error          text,
    correlation_id      text        NOT NULL DEFAULT '',
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    sent_at             timestamptz
);

-- Relay: due pending rows.
CREATE INDEX notifications_due_idx ON notifications (next_attempt_at) WHERE status = 'pending';
-- Sweeper: rows stuck in processing.
CREATE INDEX notifications_processing_idx ON notifications (updated_at) WHERE status = 'processing';
-- Listing with keyset pagination, newest first.
CREATE INDEX notifications_created_idx ON notifications (created_at DESC, id DESC);
-- Filtered listing.
CREATE INDEX notifications_filter_idx ON notifications (channel, status, created_at DESC);
CREATE INDEX notifications_batch_idx ON notifications (batch_id, created_at DESC) WHERE batch_id IS NOT NULL;

CREATE TABLE delivery_attempts (
    id              bigserial PRIMARY KEY,
    notification_id uuid        NOT NULL REFERENCES notifications (id) ON DELETE CASCADE,
    attempt         integer     NOT NULL,
    http_status     integer,
    error           text,
    latency_ms      bigint      NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX delivery_attempts_notification_idx ON delivery_attempts (notification_id);

-- Wake the relay once per INSERT statement (a 1000-item batch sends one signal).
CREATE FUNCTION notify_new_notification() RETURNS trigger
    LANGUAGE plpgsql AS
$$
BEGIN
    PERFORM pg_notify('new_notification', '');
    RETURN NULL;
END;
$$;

CREATE TRIGGER notifications_notify
    AFTER INSERT ON notifications
    FOR EACH STATEMENT EXECUTE FUNCTION notify_new_notification();
