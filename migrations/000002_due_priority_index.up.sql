-- The relay claims due rows highest priority first; index that order so a
-- large backlog does not sort every due row on each claim.
DROP INDEX IF EXISTS notifications_due_idx;
CREATE INDEX notifications_due_idx ON notifications (priority_rank DESC, next_attempt_at) WHERE status = 'pending';
-- Sweeper: rows queued for too long (their message never reached a queue).
CREATE INDEX notifications_queued_idx ON notifications (updated_at) WHERE status = 'queued';
