package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/buraksunlu/insider-notification/internal/domain"
	"github.com/buraksunlu/insider-notification/internal/provider"
	"github.com/buraksunlu/insider-notification/internal/queue"
	"github.com/buraksunlu/insider-notification/internal/store"
)

var fixedNow = time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)

// fakeStore models the worker-side state machine of one notification row.
type fakeStore struct {
	mu sync.Mutex
	n  domain.Notification

	startErr      error
	markSentErrs  []error // returned by successive MarkSent calls, then nil
	recordErr     error
	sentID        *string
	sentCalls     int
	retryAt       time.Time
	retryReason   string
	failedReason  string
	attempts      []domain.Attempt
	transitionLog []string
}

func newFakeStore(n domain.Notification) *fakeStore {
	n.Status = domain.StatusQueued
	return &fakeStore{n: n}
}

func (f *fakeStore) StartProcessing(_ context.Context, id uuid.UUID) (domain.Notification, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.startErr != nil {
		return domain.Notification{}, false, f.startErr
	}
	if id != f.n.ID || f.n.Status != domain.StatusQueued {
		return domain.Notification{}, false, nil
	}
	f.n.Status = domain.StatusProcessing
	f.n.Attempts++
	return f.n, true, nil
}

func (f *fakeStore) MarkSent(_ context.Context, _ uuid.UUID, id *string, _ time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sentCalls++
	if len(f.markSentErrs) > 0 {
		err := f.markSentErrs[0]
		f.markSentErrs = f.markSentErrs[1:]
		if err != nil {
			return err
		}
	}
	f.n.Status = domain.StatusSent
	f.sentID = id
	f.transitionLog = append(f.transitionLog, "sent")
	return nil
}

func (f *fakeStore) ScheduleRetry(_ context.Context, _ uuid.UUID, next time.Time, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n.Status = domain.StatusPending
	f.retryAt, f.retryReason = next, reason
	f.transitionLog = append(f.transitionLog, "retry")
	return nil
}

func (f *fakeStore) MarkFailed(_ context.Context, _ uuid.UUID, reason string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n.Status = domain.StatusFailed
	f.failedReason = reason
	f.transitionLog = append(f.transitionLog, "failed")
	return nil
}

func (f *fakeStore) RecordAttempt(_ context.Context, a domain.Attempt) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts = append(f.attempts, a)
	return f.recordErr
}

// requeue simulates the relay publishing the row again after a retry.
func (f *fakeStore) requeue() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n.Status = domain.StatusQueued
}

type fakeSender struct {
	mu    sync.Mutex
	res   provider.Result
	err   error
	calls []provider.Request
	keys  []string
	corrs []string
}

func (s *fakeSender) Send(_ context.Context, key, corr string, r provider.Request) (provider.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, r)
	s.keys = append(s.keys, key)
	s.corrs = append(s.corrs, corr)
	return s.res, s.err
}

type fakeDLQ struct {
	mu      sync.Mutex
	err     error
	got     []domain.Notification
	reasons []string
	// failedFirst records whether the row was already failed at publish time.
	store       *fakeStore
	failedFirst bool
}

func (d *fakeDLQ) PublishDLQ(_ context.Context, n domain.Notification, reason string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.got = append(d.got, n)
	d.reasons = append(d.reasons, reason)
	if d.store != nil {
		d.store.mu.Lock()
		d.failedFirst = d.store.n.Status == domain.StatusFailed
		d.store.mu.Unlock()
	}
	return d.err
}

func sms() domain.Notification {
	return domain.NewNotification(domain.CreateInput{
		Channel: "sms", Recipient: "+905551234567", Content: "Merhaba",
	}, "corr-row", fixedNow)
}

func accepted(id string) provider.Result {
	return provider.Result{StatusCode: http.StatusAccepted, MessageID: &id, Latency: 42 * time.Millisecond}
}

type rig struct {
	h     *Handler
	store *fakeStore
	send  *fakeSender
	dlq   *fakeDLQ
	n     domain.Notification
}

func newRig(t *testing.T) *rig {
	t.Helper()
	n := sms()
	st := newFakeStore(n)
	snd := &fakeSender{res: accepted("pm-1")}
	dlq := &fakeDLQ{store: st}
	h := &Handler{
		Store:          st,
		Sender:         snd,
		DLQ:            dlq,
		Limiters:       NewLimiters(100),
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:            func() time.Time { return fixedNow },
		Rand:           func() float64 { return 1 },
		RetryBase:      2 * time.Second,
		StoreRetryBase: time.Millisecond,
	}
	return &rig{h: h, store: st, send: snd, dlq: dlq, n: n}
}

func (r *rig) msg() queue.Message {
	return queue.Message{ID: r.n.ID, Channel: r.n.Channel, CorrelationID: "corr-msg"}
}

func TestHandleSent(t *testing.T) {
	r := newRig(t)
	if err := r.h.Handle(context.Background(), r.msg()); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if r.store.n.Status != domain.StatusSent || r.store.sentID == nil || *r.store.sentID != "pm-1" {
		t.Fatalf("status %s id %v, want sent with pm-1", r.store.n.Status, r.store.sentID)
	}
	if len(r.send.calls) != 1 {
		t.Fatalf("Send calls = %d, want 1", len(r.send.calls))
	}
	want := provider.Request{To: r.n.Recipient, Channel: "sms", Content: r.n.Content}
	if r.send.calls[0] != want {
		t.Errorf("request = %+v, want %+v", r.send.calls[0], want)
	}
	if r.send.keys[0] != r.n.ID.String() {
		t.Errorf("Idempotency-Key = %q, want notification id", r.send.keys[0])
	}
	if r.send.corrs[0] != "corr-msg" {
		t.Errorf("correlation id = %q, want corr-msg", r.send.corrs[0])
	}
	if len(r.store.attempts) != 1 {
		t.Fatalf("attempts recorded = %d, want 1", len(r.store.attempts))
	}
	a := r.store.attempts[0]
	if a.Attempt != 1 || a.HTTPStatus != 202 || a.Error != nil || a.LatencyMS != 42 {
		t.Errorf("attempt = %+v", a)
	}
	if len(r.dlq.got) != 0 {
		t.Error("DLQ published on success")
	}
}

func TestHandleSentWithoutMessageID(t *testing.T) {
	r := newRig(t)
	r.send.res = provider.Result{StatusCode: http.StatusOK}
	if err := r.h.Handle(context.Background(), r.msg()); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if r.store.n.Status != domain.StatusSent || r.store.sentID != nil {
		t.Fatalf("status %s id %v, want sent with nil id", r.store.n.Status, r.store.sentID)
	}
}

func TestHandleRetryableSchedulesRetry(t *testing.T) {
	r := newRig(t)
	r.send.res = provider.Result{StatusCode: http.StatusServiceUnavailable}
	if err := r.h.Handle(context.Background(), r.msg()); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if r.store.n.Status != domain.StatusPending {
		t.Fatalf("status = %s, want pending", r.store.n.Status)
	}
	// attempt 1, base 2s, rand 1 → full 2s.
	if want := fixedNow.Add(2 * time.Second); !r.store.retryAt.Equal(want) {
		t.Errorf("retry at %v, want %v", r.store.retryAt, want)
	}
	if r.store.retryReason == "" {
		t.Error("retry reason empty")
	}
	if len(r.dlq.got) != 0 {
		t.Error("DLQ published on retry")
	}
	if len(r.store.attempts) != 1 || r.store.attempts[0].HTTPStatus != 503 {
		t.Errorf("attempts = %+v", r.store.attempts)
	}
}

func TestHandleRetryAfterIsHonoured(t *testing.T) {
	r := newRig(t)
	r.send.res = provider.Result{StatusCode: http.StatusTooManyRequests, RetryAfter: 7 * time.Second}
	if err := r.h.Handle(context.Background(), r.msg()); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if want := fixedNow.Add(7 * time.Second); !r.store.retryAt.Equal(want) {
		t.Errorf("retry at %v, want %v", r.store.retryAt, want)
	}
}

func TestHandleTransportErrorRetries(t *testing.T) {
	r := newRig(t)
	r.send.res, r.send.err = provider.Result{}, context.DeadlineExceeded
	if err := r.h.Handle(context.Background(), r.msg()); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if r.store.n.Status != domain.StatusPending {
		t.Fatalf("status = %s, want pending", r.store.n.Status)
	}
	a := r.store.attempts[0]
	if a.HTTPStatus != 0 || a.Error == nil {
		t.Errorf("attempt = %+v, want no status and an error", a)
	}
}

func TestHandleExhaustedRetriesGoToDLQ(t *testing.T) {
	r := newRig(t)
	r.send.res = provider.Result{StatusCode: http.StatusServiceUnavailable}
	ctx := context.Background()
	for i := 1; i <= domain.DefaultMaxAttempts; i++ {
		if err := r.h.Handle(ctx, r.msg()); err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
		if i < domain.DefaultMaxAttempts {
			if r.store.n.Status != domain.StatusPending {
				t.Fatalf("attempt %d: status %s, want pending", i, r.store.n.Status)
			}
			r.store.requeue()
		}
	}
	if r.store.n.Status != domain.StatusFailed {
		t.Fatalf("status = %s, want failed", r.store.n.Status)
	}
	if len(r.dlq.got) != 1 || r.dlq.got[0].Attempts != domain.DefaultMaxAttempts {
		t.Fatalf("DLQ = %+v, want one record with %d attempts", r.dlq.got, domain.DefaultMaxAttempts)
	}
	if len(r.store.attempts) != domain.DefaultMaxAttempts {
		t.Errorf("attempts recorded = %d, want %d", len(r.store.attempts), domain.DefaultMaxAttempts)
	}
}

func TestHandlePermanentGoesToDLQFirstAttempt(t *testing.T) {
	r := newRig(t)
	r.send.res = provider.Result{StatusCode: http.StatusBadRequest}
	if err := r.h.Handle(context.Background(), r.msg()); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if r.store.n.Status != domain.StatusFailed || r.store.n.Attempts != 1 {
		t.Fatalf("status %s attempts %d, want failed after 1", r.store.n.Status, r.store.n.Attempts)
	}
	if len(r.dlq.got) != 1 {
		t.Fatalf("DLQ records = %d, want 1", len(r.dlq.got))
	}
	if !r.dlq.failedFirst {
		t.Error("DLQ published before the row was marked failed")
	}
	got := r.dlq.got[0]
	if got.Status != domain.StatusFailed || got.LastError == nil || *got.LastError != r.store.failedReason {
		t.Errorf("DLQ record status %s last_error %v, want failed with %q", got.Status, got.LastError, r.store.failedReason)
	}
}

func TestHandleDLQFailureIsNotFatal(t *testing.T) {
	r := newRig(t)
	r.send.res = provider.Result{StatusCode: http.StatusBadRequest}
	r.dlq.err = errors.New("broker down")
	if err := r.h.Handle(context.Background(), r.msg()); err != nil {
		t.Fatalf("Handle: %v, want nil (DB already says failed)", err)
	}
	if r.store.n.Status != domain.StatusFailed {
		t.Fatalf("status = %s, want failed", r.store.n.Status)
	}
}

func TestHandleSkipsRowThatIsNotQueued(t *testing.T) {
	r := newRig(t)
	r.store.n.Status = domain.StatusCancelled
	if err := r.h.Handle(context.Background(), r.msg()); err != nil {
		t.Fatalf("Handle: %v, want nil (ack)", err)
	}
	if len(r.send.calls) != 0 || len(r.store.attempts) != 0 {
		t.Fatalf("cancelled row was sent (%d calls, %d attempts)", len(r.send.calls), len(r.store.attempts))
	}
}

func TestHandleStartProcessingErrorRequeues(t *testing.T) {
	r := newRig(t)
	r.store.startErr = errors.New("db down")
	if err := r.h.Handle(context.Background(), r.msg()); err == nil {
		t.Fatal("Handle: want error so the message is requeued")
	}
	if len(r.send.calls) != 0 {
		t.Fatal("sent although the row could not be claimed")
	}
}

func TestHandleRetriesStateWrite(t *testing.T) {
	r := newRig(t)
	r.store.markSentErrs = []error{errors.New("blip"), errors.New("blip")}
	if err := r.h.Handle(context.Background(), r.msg()); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if r.store.sentCalls != 3 || r.store.n.Status != domain.StatusSent {
		t.Fatalf("MarkSent calls %d status %s, want 3 and sent", r.store.sentCalls, r.store.n.Status)
	}
}

func TestHandleStateWriteGivesUp(t *testing.T) {
	r := newRig(t)
	boom := errors.New("db down")
	r.store.markSentErrs = []error{boom, boom, boom, boom}
	if err := r.h.Handle(context.Background(), r.msg()); !errors.Is(err, boom) {
		t.Fatalf("Handle: %v, want %v", err, boom)
	}
	if r.store.sentCalls != 3 {
		t.Errorf("MarkSent calls = %d, want 3", r.store.sentCalls)
	}
}

func TestHandleStateConflictIsNotRetried(t *testing.T) {
	r := newRig(t)
	r.store.markSentErrs = []error{store.ErrStateConflict}
	if err := r.h.Handle(context.Background(), r.msg()); err != nil {
		t.Fatalf("Handle: %v, want nil (row moved on, nothing to redo)", err)
	}
	if r.store.sentCalls != 1 {
		t.Errorf("MarkSent calls = %d, want 1", r.store.sentCalls)
	}
}

func TestHandleRecordAttemptFailureIsNotFatal(t *testing.T) {
	r := newRig(t)
	r.store.recordErr = errors.New("audit table locked")
	if err := r.h.Handle(context.Background(), r.msg()); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if r.store.n.Status != domain.StatusSent {
		t.Fatalf("status = %s, want sent", r.store.n.Status)
	}
}

func TestHandleFallsBackToRowCorrelationID(t *testing.T) {
	r := newRig(t)
	m := r.msg()
	m.CorrelationID = ""
	if err := r.h.Handle(context.Background(), m); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if r.send.corrs[0] != "corr-row" {
		t.Errorf("correlation id = %q, want corr-row", r.send.corrs[0])
	}
}

// openStore claims every id as a fresh row, so many messages can be pushed
// through the real limiter.
type openStore struct {
	fakeStore
	channels sync.Map // uuid.UUID → domain.Channel
}

func (o *openStore) StartProcessing(_ context.Context, id uuid.UUID) (domain.Notification, bool, error) {
	n := sms()
	c, _ := o.channels.Load(id)
	n.ID, n.Channel, n.Status, n.Attempts = id, c.(domain.Channel), domain.StatusProcessing, 1
	return n, true, nil
}

func (o *openStore) MarkSent(context.Context, uuid.UUID, *string, time.Time) error { return nil }
func (o *openStore) RecordAttempt(context.Context, domain.Attempt) error           { return nil }

func limiterHandler(t *testing.T) *Handler {
	h := newRig(t).h
	h.Store = &openStore{}
	return h
}

func pump(t *testing.T, h *Handler, c domain.Channel, count int) {
	t.Helper()
	st := h.Store.(*openStore)
	for range count {
		id := uuid.New()
		st.channels.Store(id, c)
		if err := h.Handle(context.Background(), queue.Message{ID: id, Channel: c}); err != nil {
			t.Errorf("Handle: %v", err)
			return
		}
	}
}

func TestRateLimitPerChannel(t *testing.T) {
	h := limiterHandler(t)
	start := time.Now()
	pump(t, h, domain.ChannelSMS, 150) // burst 100, then 50 more at 100/s
	if elapsed := time.Since(start); elapsed < 450*time.Millisecond {
		t.Errorf("150 sms took %v, want ≥ ~0.5s", elapsed)
	}
}

func TestRateLimitChannelsAreIndependent(t *testing.T) {
	h := limiterHandler(t)
	start := time.Now()
	var wg sync.WaitGroup
	for _, c := range []domain.Channel{domain.ChannelSMS, domain.ChannelEmail} {
		wg.Add(1)
		go func() { defer wg.Done(); pump(t, h, c, 100) }()
	}
	wg.Wait()
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Errorf("100 sms + 100 email took %v, channels block each other", elapsed)
	}
}
