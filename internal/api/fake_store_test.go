package api_test

import (
	"context"
	"sync"

	"github.com/google/uuid"

	"github.com/buraksunlu/insider-notification/internal/domain"
	"github.com/buraksunlu/insider-notification/internal/store"
)

// fakeStore is an in-memory NotificationStore. Fields ending in Err force
// the matching method to fail.
type fakeStore struct {
	mu        sync.Mutex
	byID      map[uuid.UUID]domain.Notification
	byKey     map[string]uuid.UUID
	batches   map[uuid.UUID]int
	createErr error
	cancelErr error
	listErr   error

	lastFilter store.ListFilter
}

func newFakeStore() *fakeStore {
	return &fakeStore{byID: map[uuid.UUID]domain.Notification{}, byKey: map[string]uuid.UUID{}, batches: map[uuid.UUID]int{}}
}

func (f *fakeStore) Create(_ context.Context, n domain.Notification) (domain.Notification, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return domain.Notification{}, false, f.createErr
	}
	if n.IdempotencyKey != nil {
		if id, ok := f.byKey[*n.IdempotencyKey]; ok {
			existing := f.byID[id]
			if existing.Content != n.Content {
				return existing, false, store.ErrIdempotencyConflict
			}
			return existing, false, nil
		}
		f.byKey[*n.IdempotencyKey] = n.ID
	}
	f.byID[n.ID] = n
	return n, true, nil
}

func (f *fakeStore) CreateBatch(_ context.Context, ns []domain.Notification) (uuid.UUID, []uuid.UUID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return uuid.Nil, nil, f.createErr
	}
	id := uuid.New()
	ids := make([]uuid.UUID, len(ns))
	for i, n := range ns {
		// Like the real store, an existing idempotency key reuses that row.
		if n.IdempotencyKey != nil {
			if existing, ok := f.byKey[*n.IdempotencyKey]; ok {
				ids[i] = existing
				continue
			}
			f.byKey[*n.IdempotencyKey] = n.ID
		}
		n.BatchID = &id
		f.byID[n.ID] = n
		ids[i] = n.ID
	}
	f.batches[id] = len(ns)
	return id, ids, nil
}

func (f *fakeStore) Get(_ context.Context, id uuid.UUID) (domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, ok := f.byID[id]
	if !ok {
		return n, store.ErrNotFound
	}
	return n, nil
}

func (f *fakeStore) GetBatch(_ context.Context, id uuid.UUID, _ store.Page) (store.BatchSummary, []domain.Notification, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	total, ok := f.batches[id]
	if !ok {
		return store.BatchSummary{}, nil, "", store.ErrNotFound
	}
	sum := store.BatchSummary{ID: id, Total: total, Counts: map[domain.Status]int{}}
	var items []domain.Notification
	for _, n := range f.byID {
		if n.BatchID != nil && *n.BatchID == id {
			sum.Counts[n.Status]++
			items = append(items, n)
		}
	}
	return sum, items, "", nil
}

func (f *fakeStore) Cancel(_ context.Context, id uuid.UUID) (domain.Notification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cancelErr != nil {
		return domain.Notification{}, f.cancelErr
	}
	n, ok := f.byID[id]
	if !ok {
		return n, store.ErrNotFound
	}
	if n.Status != domain.StatusPending && n.Status != domain.StatusQueued {
		return domain.Notification{}, store.ErrNotCancellable
	}
	n.Status = domain.StatusCancelled
	f.byID[id] = n
	return n, nil
}

func (f *fakeStore) CancelBatch(_ context.Context, id uuid.UUID) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.batches[id]; !ok {
		return 0, store.ErrNotFound
	}
	var n int64
	for k, v := range f.byID {
		if v.BatchID != nil && *v.BatchID == id && (v.Status == domain.StatusPending || v.Status == domain.StatusQueued) {
			v.Status = domain.StatusCancelled
			f.byID[k] = v
			n++
		}
	}
	return n, nil
}

func (f *fakeStore) List(_ context.Context, filter store.ListFilter) ([]domain.Notification, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastFilter = filter
	if f.listErr != nil {
		return nil, "", f.listErr
	}
	var out []domain.Notification
	for _, n := range f.byID {
		out = append(out, n)
	}
	return out, "next-cursor", nil
}

func (f *fakeStore) put(n domain.Notification) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byID[n.ID] = n
}
