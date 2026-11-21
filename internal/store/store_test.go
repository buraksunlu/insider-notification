package store_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buraksunlu/insider-notification/internal/domain"
	"github.com/buraksunlu/insider-notification/internal/store"
	"github.com/buraksunlu/insider-notification/internal/testutil"
)

var ctx = context.Background()

func strp(s string) *string { return &s }

// past is slightly behind wall clock so rows are due by the database's now().
func past() time.Time { return time.Now().Add(-2 * time.Second) }

func newSMS(content string) domain.Notification {
	return domain.NewNotification(domain.CreateInput{
		Channel: "sms", Recipient: "+905551234567", Content: content,
	}, "corr-test", past())
}

func setup(t *testing.T) (*store.Store, *pgxpool.Pool) {
	t.Helper()
	pool := testutil.Postgres(t)
	return store.New(pool), pool
}

func mustCreate(t *testing.T, s *store.Store, n domain.Notification) domain.Notification {
	t.Helper()
	got, created, err := s.Create(ctx, n)
	if err != nil || !created {
		t.Fatalf("create: created=%v err=%v", created, err)
	}
	return got
}

func TestCreate_RoundTrip(t *testing.T) {
	s, _ := setup(t)
	n := newSMS("hello")
	mustCreate(t, s, n)

	got, err := s.Get(ctx, n.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Content != "hello" || got.Status != domain.StatusPending || got.Priority != domain.PriorityNormal ||
		got.MaxAttempts != 8 || got.CorrelationID != "corr-test" {
		t.Fatalf("unexpected row: %+v", got)
	}
}

func TestGet_NotFound(t *testing.T) {
	s, _ := setup(t)
	if _, err := s.Get(ctx, uuid.New()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestCreate_IdempotentReplay(t *testing.T) {
	s, _ := setup(t)
	first := newSMS("hello")
	first.IdempotencyKey = strp("key-1")
	mustCreate(t, s, first)

	second := newSMS("hello")
	second.IdempotencyKey = strp("key-1")
	got, created, err := s.Create(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	if created || got.ID != first.ID {
		t.Fatalf("replay: created=%v id=%v, want existing %v", created, got.ID, first.ID)
	}
}

func TestCreate_IdempotencyConflict(t *testing.T) {
	s, _ := setup(t)
	first := newSMS("hello")
	first.IdempotencyKey = strp("key-1")
	mustCreate(t, s, first)

	other := newSMS("goodbye")
	other.IdempotencyKey = strp("key-1")
	got, _, err := s.Create(ctx, other)
	if !errors.Is(err, store.ErrIdempotencyConflict) {
		t.Fatalf("want ErrIdempotencyConflict, got %v", err)
	}
	if got.ID != first.ID {
		t.Fatalf("conflict should return the existing row, got %v", got.ID)
	}
}

func TestCreateBatch_ReusesExistingKey(t *testing.T) {
	s, _ := setup(t)
	existing := newSMS("a")
	existing.IdempotencyKey = strp("dup")
	mustCreate(t, s, existing)

	again := newSMS("a")
	again.IdempotencyKey = strp("dup")
	fresh := newSMS("b")

	batchID, ids, err := s.CreateBatch(ctx, []domain.Notification{again, fresh})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != existing.ID || ids[1] != fresh.ID {
		t.Fatalf("ids = %v, want [%v %v]", ids, existing.ID, fresh.ID)
	}
	got, _ := s.Get(ctx, fresh.ID)
	if got.BatchID == nil || *got.BatchID != batchID {
		t.Fatalf("fresh row not linked to batch: %v", got.BatchID)
	}
}

func TestCreateBatch_DuplicateKeyInBatch(t *testing.T) {
	s, pool := setup(t)
	a, b := newSMS("a"), newSMS("a")
	a.IdempotencyKey, b.IdempotencyKey = strp("same"), strp("same")

	_, _, err := s.CreateBatch(ctx, []domain.Notification{a, newSMS("x"), b})
	var itemErr *store.ItemError
	if !errors.Is(err, store.ErrDuplicateKeyInBatch) || !errors.As(err, &itemErr) || itemErr.Index != 2 {
		t.Fatalf("want ErrDuplicateKeyInBatch at index 2, got %v", err)
	}
	assertCount(t, pool, "notifications", 0)
}

func TestCreateBatch_ConflictRollsBack(t *testing.T) {
	s, pool := setup(t)
	existing := newSMS("a")
	existing.IdempotencyKey = strp("k")
	mustCreate(t, s, existing)

	changed := newSMS("different")
	changed.IdempotencyKey = strp("k")
	_, _, err := s.CreateBatch(ctx, []domain.Notification{newSMS("x"), changed})
	var itemErr *store.ItemError
	if !errors.Is(err, store.ErrIdempotencyConflict) || !errors.As(err, &itemErr) || itemErr.Index != 1 {
		t.Fatalf("want ErrIdempotencyConflict at index 1, got %v", err)
	}
	assertCount(t, pool, "notifications", 1)
	assertCount(t, pool, "batches", 0)
}

func TestGetBatch_CountsAndItems(t *testing.T) {
	s, _ := setup(t)
	items := []domain.Notification{newSMS("1"), newSMS("2"), newSMS("3")}
	batchID, ids, err := s.CreateBatch(ctx, items)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Cancel(ctx, ids[0]); err != nil {
		t.Fatal(err)
	}

	sum, page, next, err := s.GetBatch(ctx, batchID, store.Page{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if sum.Total != 3 || sum.Counts[domain.StatusPending] != 2 || sum.Counts[domain.StatusCancelled] != 1 {
		t.Fatalf("summary = %+v", sum)
	}
	if len(page) != 2 || next == "" {
		t.Fatalf("page len=%d next=%q", len(page), next)
	}
	if _, _, _, err := s.GetBatch(ctx, uuid.New(), store.Page{}); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown batch: want ErrNotFound, got %v", err)
	}
}

func TestCancel_OnlyPendingOrQueued(t *testing.T) {
	s, pool := setup(t)
	for _, st := range []domain.Status{domain.StatusPending, domain.StatusQueued} {
		n := mustCreate(t, s, newSMS("x"))
		setStatus(t, pool, n.ID, st)
		got, err := s.Cancel(ctx, n.ID)
		if err != nil || got.Status != domain.StatusCancelled {
			t.Fatalf("cancel from %s: status=%s err=%v", st, got.Status, err)
		}
	}
	for _, st := range []domain.Status{domain.StatusProcessing, domain.StatusSent, domain.StatusFailed, domain.StatusCancelled} {
		n := mustCreate(t, s, newSMS("x"))
		setStatus(t, pool, n.ID, st)
		if _, err := s.Cancel(ctx, n.ID); !errors.Is(err, store.ErrNotCancellable) {
			t.Fatalf("cancel from %s: want ErrNotCancellable, got %v", st, err)
		}
	}
	if _, err := s.Cancel(ctx, uuid.New()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cancel unknown: want ErrNotFound, got %v", err)
	}
}

func TestStartProcessing_SkipsCancelled(t *testing.T) {
	s, pool := setup(t)
	n := mustCreate(t, s, newSMS("x"))
	setStatus(t, pool, n.ID, domain.StatusQueued)
	if _, err := s.Cancel(ctx, n.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.StartProcessing(ctx, n.ID); err != nil || ok {
		t.Fatalf("cancelled row must not start: ok=%v err=%v", ok, err)
	}
}

func TestStartProcessing_Once(t *testing.T) {
	s, pool := setup(t)
	n := mustCreate(t, s, newSMS("x"))
	setStatus(t, pool, n.ID, domain.StatusQueued)

	got, ok, err := s.StartProcessing(ctx, n.ID)
	if err != nil || !ok || got.Status != domain.StatusProcessing || got.Attempts != 1 {
		t.Fatalf("first start: ok=%v err=%v row=%+v", ok, err, got)
	}
	if _, ok, err := s.StartProcessing(ctx, n.ID); err != nil || ok {
		t.Fatalf("second start must be rejected: ok=%v err=%v", ok, err)
	}
}

func TestClaimDue_RespectsScheduleAndPriority(t *testing.T) {
	s, _ := setup(t)
	low := domain.NewNotification(domain.CreateInput{Channel: "sms", Recipient: "+905551234567", Content: "low", Priority: "low"}, "c", past().Add(-time.Minute))
	high := domain.NewNotification(domain.CreateInput{Channel: "sms", Recipient: "+905551234567", Content: "high", Priority: "high"}, "c", past())
	future := time.Now().Add(time.Hour)
	later := domain.NewNotification(domain.CreateInput{Channel: "sms", Recipient: "+905551234567", Content: "later", ScheduledAt: &future}, "c", past())
	for _, n := range []domain.Notification{low, high, later} {
		mustCreate(t, s, n)
	}

	var published []domain.Notification
	count, err := s.ClaimDue(ctx, 10, func(n domain.Notification) error {
		published = append(published, n)
		return nil
	})
	if err != nil || count != 2 {
		t.Fatalf("claim: count=%d err=%v", count, err)
	}
	if published[0].ID != high.ID || published[1].ID != low.ID {
		t.Fatalf("order = [%s %s], want [high low]", published[0].Content, published[1].Content)
	}
	for _, id := range []uuid.UUID{high.ID, low.ID} {
		if got, _ := s.Get(ctx, id); got.Status != domain.StatusQueued {
			t.Fatalf("claimed row status = %s, want queued", got.Status)
		}
	}
	if got, _ := s.Get(ctx, later.ID); got.Status != domain.StatusPending {
		t.Fatalf("scheduled row status = %s, want pending", got.Status)
	}
}

func TestClaimDue_RollsBackOnPublishError(t *testing.T) {
	s, _ := setup(t)
	n := mustCreate(t, s, newSMS("x"))

	_, err := s.ClaimDue(ctx, 10, func(domain.Notification) error { return errors.New("broker down") })
	if err == nil {
		t.Fatal("expected publish error")
	}
	if got, _ := s.Get(ctx, n.ID); got.Status != domain.StatusPending {
		t.Fatalf("status = %s, want pending after failed publish", got.Status)
	}
}

// TestClaimDue_CommitsBeforePublish is the regression test for the
// relay/consumer race: a consumer may receive the message the moment it is
// published, so the row must already be committed as queued by then.
func TestClaimDue_CommitsBeforePublish(t *testing.T) {
	s, _ := setup(t)
	n := mustCreate(t, s, newSMS("x"))

	var started bool
	_, err := s.ClaimDue(ctx, 10, func(m domain.Notification) error {
		// What a consumer does on receipt, on another connection.
		_, ok, err := s.StartProcessing(ctx, m.ID)
		started = ok
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if !started {
		t.Fatal("consumer could not claim a published row: it was not committed as queued before publish")
	}
	if got, _ := s.Get(ctx, n.ID); got.Status != domain.StatusProcessing {
		t.Fatalf("status = %s, want processing", got.Status)
	}
}

func TestClaimDue_PublishErrorReleasesOnlyUnpublished(t *testing.T) {
	s, _ := setup(t)
	var ids []uuid.UUID
	for i := range 3 {
		ids = append(ids, mustCreate(t, s, newSMS(fmt.Sprint(i))).ID)
	}

	calls := 0
	count, err := s.ClaimDue(ctx, 10, func(domain.Notification) error {
		calls++
		if calls == 2 {
			return errors.New("broker down")
		}
		return nil
	})
	if err == nil || count != 1 {
		t.Fatalf("claim: count=%d err=%v, want 1 published and an error", count, err)
	}
	statuses := map[domain.Status]int{}
	for _, id := range ids {
		got, _ := s.Get(ctx, id)
		statuses[got.Status]++
	}
	if statuses[domain.StatusQueued] != 1 || statuses[domain.StatusPending] != 2 {
		t.Fatalf("statuses = %v, want 1 queued (published) and 2 pending", statuses)
	}
}

func TestMarkSent_OverridesCancelled(t *testing.T) {
	s, pool := setup(t)
	n := mustCreate(t, s, newSMS("x"))
	// The sweeper reset a stalled delivery, then the user cancelled it; the
	// stalled worker finally gets a 2xx. The message was delivered.
	setStatus(t, pool, n.ID, domain.StatusCancelled)
	if err := s.MarkSent(ctx, n.ID, strp("prov-late"), time.Now()); err != nil {
		t.Fatalf("MarkSent over cancelled: %v", err)
	}
	if got, _ := s.Get(ctx, n.ID); got.Status != domain.StatusSent {
		t.Fatalf("status = %s, want sent", got.Status)
	}
}

func TestDeliveryTransitions(t *testing.T) {
	s, pool := setup(t)
	start := func() domain.Notification {
		n := mustCreate(t, s, newSMS("x"))
		setStatus(t, pool, n.ID, domain.StatusQueued)
		got, ok, err := s.StartProcessing(ctx, n.ID)
		if err != nil || !ok {
			t.Fatalf("start: ok=%v err=%v", ok, err)
		}
		return got
	}

	sent := start()
	if err := s.MarkSent(ctx, sent.ID, strp("prov-1"), time.Now()); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, sent.ID)
	if got.Status != domain.StatusSent || got.ProviderMessageID == nil || *got.ProviderMessageID != "prov-1" || got.SentAt == nil {
		t.Fatalf("after MarkSent: %+v", got)
	}

	retry := start()
	next := time.Now().Add(time.Minute).Truncate(time.Microsecond)
	if err := s.ScheduleRetry(ctx, retry.ID, next, "503"); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Get(ctx, retry.ID)
	if got.Status != domain.StatusPending || !got.NextAttemptAt.Equal(next) || got.LastError == nil || *got.LastError != "503" {
		t.Fatalf("after ScheduleRetry: %+v", got)
	}

	failed := start()
	if err := s.MarkFailed(ctx, failed.ID, "400 bad request"); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Get(ctx, failed.ID); got.Status != domain.StatusFailed {
		t.Fatalf("after MarkFailed: %s", got.Status)
	}
	if err := s.ScheduleRetry(ctx, failed.ID, next, "x"); !errors.Is(err, store.ErrStateConflict) {
		t.Fatalf("retry of failed row: want ErrStateConflict, got %v", err)
	}

	if err := s.RecordAttempt(ctx, domain.Attempt{NotificationID: sent.ID, Attempt: 1, HTTPStatus: 202, LatencyMS: 12}); err != nil {
		t.Fatal(err)
	}
	assertCount(t, pool, "delivery_attempts", 1)
}

func TestList_FiltersAndCursor(t *testing.T) {
	s, pool := setup(t)
	for i := 0; i < 25; i++ {
		mustCreate(t, s, newSMS(fmt.Sprint(i)))
	}
	email := domain.NewNotification(domain.CreateInput{Channel: "email", Recipient: "a@b.co", Content: "e"}, "c", past())
	mustCreate(t, s, email)
	setStatus(t, pool, email.ID, domain.StatusSent)

	sms := domain.ChannelSMS
	seen := map[uuid.UUID]bool{}
	cursor, pages := "", 0
	for {
		items, next, err := s.List(ctx, store.ListFilter{Channel: &sms, Page: store.Page{Limit: 10, Cursor: cursor}})
		if err != nil {
			t.Fatal(err)
		}
		pages++
		for _, n := range items {
			if seen[n.ID] || n.Channel != domain.ChannelSMS {
				t.Fatalf("duplicate or unfiltered item %v", n.ID)
			}
			seen[n.ID] = true
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if pages != 3 || len(seen) != 25 {
		t.Fatalf("pages=%d items=%d, want 3 and 25", pages, len(seen))
	}

	sent := domain.StatusSent
	items, _, err := s.List(ctx, store.ListFilter{Status: &sent})
	if err != nil || len(items) != 1 || items[0].ID != email.ID {
		t.Fatalf("status filter: %v %v", items, err)
	}

	from, to := time.Now().Add(time.Hour), time.Now().Add(2*time.Hour)
	items, _, err = s.List(ctx, store.ListFilter{From: &from, To: &to})
	if err != nil || len(items) != 0 {
		t.Fatalf("date filter: %d items, err %v", len(items), err)
	}
}

func TestList_BadCursor(t *testing.T) {
	s, _ := setup(t)
	for _, c := range []string{"%%%", "bm90LWEtY3Vyc29y"} {
		if _, _, err := s.List(ctx, store.ListFilter{Page: store.Page{Cursor: c}}); !errors.Is(err, store.ErrBadCursor) {
			t.Fatalf("cursor %q: want ErrBadCursor, got %v", c, err)
		}
	}
}

func TestResetStaleQueued(t *testing.T) {
	s, pool := setup(t)
	stale := mustCreate(t, s, newSMS("stale"))
	fresh := mustCreate(t, s, newSMS("fresh"))
	if _, err := pool.Exec(ctx, `UPDATE notifications SET status='queued', updated_at = now() - interval '15 minutes' WHERE id=$1`, stale.ID); err != nil {
		t.Fatal(err)
	}
	setStatus(t, pool, fresh.ID, domain.StatusQueued)

	n, err := s.ResetStaleQueued(ctx, 10*time.Minute)
	if err != nil || n != 1 {
		t.Fatalf("reset: n=%d err=%v", n, err)
	}
	if got, _ := s.Get(ctx, stale.ID); got.Status != domain.StatusPending {
		t.Fatalf("stale row status = %s, want pending", got.Status)
	}
	if got, _ := s.Get(ctx, fresh.ID); got.Status != domain.StatusQueued {
		t.Fatalf("fresh row status = %s, want queued", got.Status)
	}
}

func TestClaimDue_UsesPriorityIndex(t *testing.T) {
	_, pool := setup(t)
	var def string
	err := pool.QueryRow(ctx, `SELECT indexdef FROM pg_indexes WHERE indexname = 'notifications_due_idx'`).Scan(&def)
	if err != nil {
		t.Fatal(err)
	}
	if want := "(priority_rank DESC, next_attempt_at)"; !strings.Contains(def, want) {
		t.Fatalf("notifications_due_idx = %q, want it on %s", def, want)
	}
}

func TestResetStuck(t *testing.T) {
	s, pool := setup(t)
	stuck := mustCreate(t, s, newSMS("stuck"))
	fresh := mustCreate(t, s, newSMS("fresh"))
	if _, err := pool.Exec(ctx, `UPDATE notifications SET status='processing', updated_at = now() - interval '5 minutes' WHERE id=$1`, stuck.ID); err != nil {
		t.Fatal(err)
	}
	setStatus(t, pool, fresh.ID, domain.StatusProcessing)

	n, err := s.ResetStuck(ctx, 2*time.Minute)
	if err != nil || n != 1 {
		t.Fatalf("reset: n=%d err=%v", n, err)
	}
	if got, _ := s.Get(ctx, stuck.ID); got.Status != domain.StatusPending {
		t.Fatalf("stuck row status = %s", got.Status)
	}
	if got, _ := s.Get(ctx, fresh.ID); got.Status != domain.StatusProcessing {
		t.Fatalf("fresh row status = %s", got.Status)
	}
}

func TestCountByStatusAndPing(t *testing.T) {
	s, pool := setup(t)
	a := mustCreate(t, s, newSMS("a"))
	mustCreate(t, s, newSMS("b"))
	setStatus(t, pool, a.ID, domain.StatusSent)

	counts, err := s.CountByStatus(ctx)
	if err != nil || counts[domain.StatusSent] != 1 || counts[domain.StatusPending] != 1 {
		t.Fatalf("counts=%v err=%v", counts, err)
	}
	if err := s.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func setStatus(t *testing.T, pool *pgxpool.Pool, id uuid.UUID, st domain.Status) {
	t.Helper()
	if _, err := pool.Exec(ctx, `UPDATE notifications SET status=$2 WHERE id=$1`, id, string(st)); err != nil {
		t.Fatal(err)
	}
}

func assertCount(t *testing.T, pool *pgxpool.Pool, table string, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s rows = %d, want %d", table, got, want)
	}
}

func TestCancelBatch_CancelsOnlyPendingAndQueued(t *testing.T) {
	s, pool := setup(t)
	batchID, ids, err := s.CreateBatch(ctx, []domain.Notification{newSMS("1"), newSMS("2"), newSMS("3")})
	if err != nil {
		t.Fatal(err)
	}
	setStatus(t, pool, ids[1], domain.StatusQueued)
	setStatus(t, pool, ids[2], domain.StatusSent)

	n, err := s.CancelBatch(ctx, batchID)
	if err != nil || n != 2 {
		t.Fatalf("cancel batch: n=%d err=%v, want 2", n, err)
	}
	if got, _ := s.Get(ctx, ids[2]); got.Status != domain.StatusSent {
		t.Fatalf("sent item status = %s, want sent", got.Status)
	}
	if _, err := s.CancelBatch(ctx, uuid.New()); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown batch: want ErrNotFound, got %v", err)
	}
}

func TestListenSignalsOnInsert(t *testing.T) {
	s, _ := setup(t)
	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wake := s.Listen(lctx, "new_notification")

	// The first signal fires once LISTEN is active, covering anything
	// inserted before the subscription existed.
	select {
	case <-wake:
	case <-time.After(5 * time.Second):
		t.Fatal("no signal after LISTEN became active")
	}

	mustCreate(t, s, newSMS("listen"))
	select {
	case <-wake:
	case <-time.After(5 * time.Second):
		t.Fatal("no signal after insert")
	}

	cancel()
	select {
	case _, ok := <-wake:
		if ok {
			// A buffered signal may still be pending; the next read must close.
			if _, ok := <-wake; ok {
				t.Fatal("channel not closed after cancel")
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("channel not closed after cancel")
	}
}

func TestHasIdempotencyKey(t *testing.T) {
	s, _ := setup(t)
	n := newSMS("x")
	n.IdempotencyKey = strp("known")
	mustCreate(t, s, n)

	for key, want := range map[string]bool{"known": true, "unknown": false} {
		got, err := s.HasIdempotencyKey(ctx, key)
		if err != nil || got != want {
			t.Errorf("HasIdempotencyKey(%q) = %v, %v; want %v", key, got, err, want)
		}
	}
}
