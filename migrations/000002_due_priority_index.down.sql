DROP INDEX IF EXISTS notifications_queued_idx;
DROP INDEX IF EXISTS notifications_due_idx;
CREATE INDEX notifications_due_idx ON notifications (next_attempt_at) WHERE status = 'pending';
