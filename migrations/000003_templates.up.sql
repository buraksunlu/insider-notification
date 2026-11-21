-- Reusable message bodies with {{variable}} placeholders. Templates are
-- immutable, so a notification's template_id always names the body it was
-- rendered from; the rendered text itself is stored on the notification.
CREATE TABLE templates (
    id         uuid PRIMARY KEY,
    name       text        NOT NULL UNIQUE,
    channel    text        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    body       text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE notifications ADD COLUMN template_id uuid REFERENCES templates (id);
