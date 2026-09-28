package projector_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/integrations/indexcore"
	"github.com/nathanxiangang-web/panta/internal/projector"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

type bindingReader struct {
	binding storage.Binding
	err     error
}

func (reader bindingReader) GetBinding(context.Context, storage.BindingID) (storage.Binding, error) {
	return reader.binding, reader.err
}

type journalReader struct {
	events []indexcore.JournalEvent
	err    error
	calls  []indexcore.JournalRequest
}

func (reader *journalReader) ReadJournal(_ context.Context, request indexcore.JournalRequest) ([]indexcore.JournalEvent, error) {
	reader.calls = append(reader.calls, request)
	return reader.events, reader.err
}

type projectionStore struct {
	cursor  int64
	err     error
	batches []projector.Batch
}

func (store *projectionStore) Cursor(context.Context, storage.BindingID) (int64, error) {
	return store.cursor, store.err
}

func (store *projectionStore) ApplyBatch(_ context.Context, batch projector.Batch) error {
	store.batches = append(store.batches, batch)
	return store.err
}

func TestProjectOnceUsesExactStoredCursorAndBindingRoot(t *testing.T) {
	for _, cursor := range []int64{0, 42} {
		t.Run(string(rune('0'+cursor/42)), func(t *testing.T) {
			journal := &journalReader{}
			store := &projectionStore{cursor: cursor}
			service := newService(t, activeBinding(), journal, store)
			result, err := service.ProjectOnce(context.Background(), "binding-1", 25)
			if err != nil {
				t.Fatalf("ProjectOnce() error = %v", err)
			}
			if len(journal.calls) != 1 || journal.calls[0].AfterSeq != cursor ||
				journal.calls[0].RootID != "root-1" || journal.calls[0].Limit != 25 {
				t.Fatalf("Journal calls = %#v", journal.calls)
			}
			if result.PreviousCursor != cursor || result.CurrentCursor != cursor || len(store.batches) != 0 {
				t.Fatalf("result/store = %#v/%#v", result, store.batches)
			}
		})
	}
}

func TestProjectOnceRejectsDisabledBindingBeforeJournal(t *testing.T) {
	binding := activeBinding()
	binding.Status = storage.BindingStatusDisabled
	journal := &journalReader{}
	store := &projectionStore{}
	service := newService(t, binding, journal, store)
	_, err := service.ProjectOnce(context.Background(), binding.ID, 10)
	if !errors.Is(err, projector.ErrDisabledBinding) || len(journal.calls) != 0 || len(store.batches) != 0 {
		t.Fatalf("ProjectOnce() = %v, journal=%#v batches=%#v", err, journal.calls, store.batches)
	}
}

func TestProjectOnceMapsResourceEventsInOrderAndIgnoresPayload(t *testing.T) {
	resourceID := "resource-1"
	types := []indexcore.JournalEventType{
		indexcore.EventResourceAdded, indexcore.EventResourceUpdated,
		indexcore.EventResourceRenamed, indexcore.EventResourceMoved,
		indexcore.EventResourceRemoved,
	}
	events := make([]indexcore.JournalEvent, 0, len(types))
	for i, eventType := range types {
		events = append(events, indexcore.JournalEvent{
			EventSeq: int64(43 + i), EventType: eventType, ResourceID: &resourceID,
			Payload: json.RawMessage(`{"availability":"invented","resource_id":"wrong"}`),
		})
	}
	journal := &journalReader{events: events}
	store := &projectionStore{cursor: 42}
	service := newService(t, activeBinding(), journal, store)
	result, err := service.ProjectOnce(context.Background(), "binding-1", 10)
	if err != nil {
		t.Fatalf("ProjectOnce() error = %v", err)
	}
	if result.CurrentCursor != 47 || result.Mutations != 5 || len(store.batches) != 1 {
		t.Fatalf("result/batches = %#v/%#v", result, store.batches)
	}
	batch := store.batches[0]
	for i, mutation := range batch.Mutations {
		want := projector.AvailabilityPresent
		if i == len(batch.Mutations)-1 {
			want = projector.AvailabilityRemoved
		}
		if mutation.Availability != want || mutation.IndexCoreRootID != "root-1" ||
			mutation.IndexCoreResourceID != resourceID || mutation.StorageBindingID != "binding-1" {
			t.Fatalf("mutation[%d] = %#v", i, mutation)
		}
		wantCopyID := catalog.CopyID(fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1))
		if mutation.CopyID != wantCopyID {
			t.Fatalf("mutation[%d].CopyID = %q, want %q", i, mutation.CopyID, wantCopyID)
		}
	}
}

func TestProjectOnceRootLifecycleEventsAdvanceCursorOnly(t *testing.T) {
	journal := &journalReader{events: []indexcore.JournalEvent{
		{EventSeq: 8, EventType: indexcore.EventRootDeprecated},
		{EventSeq: 9, EventType: indexcore.EventRootDeleted},
	}}
	store := &projectionStore{cursor: 7}
	service := newService(t, activeBinding(), journal, store)
	result, err := service.ProjectOnce(context.Background(), "binding-1", 2)
	if err != nil || result.CurrentCursor != 9 || result.Mutations != 0 ||
		len(store.batches) != 1 || len(store.batches[0].Mutations) != 0 {
		t.Fatalf("ProjectOnce() = %#v, %v; batches=%#v", result, err, store.batches)
	}
}

func TestProjectOnceRejectsInvalidJournalBeforeMutation(t *testing.T) {
	resourceID := "resource-1"
	tests := []struct {
		name   string
		events []indexcore.JournalEvent
	}{
		{name: "missing resource", events: []indexcore.JournalEvent{{EventSeq: 1, EventType: indexcore.EventResourceAdded}}},
		{name: "not above cursor", events: []indexcore.JournalEvent{{EventSeq: 42, EventType: indexcore.EventResourceAdded, ResourceID: &resourceID}}},
		{name: "not increasing", events: []indexcore.JournalEvent{
			{EventSeq: 44, EventType: indexcore.EventResourceAdded, ResourceID: &resourceID},
			{EventSeq: 43, EventType: indexcore.EventResourceUpdated, ResourceID: &resourceID},
		}},
		{name: "unknown type", events: []indexcore.JournalEvent{{EventSeq: 43, EventType: "future-event"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			journal := &journalReader{events: test.events}
			store := &projectionStore{cursor: 42}
			service := newService(t, activeBinding(), journal, store)
			_, err := service.ProjectOnce(context.Background(), "binding-1", 10)
			if !errors.Is(err, projector.ErrInvalidJournal) || len(store.batches) != 0 {
				t.Fatalf("ProjectOnce() = %v, batches=%#v", err, store.batches)
			}
		})
	}
}

func activeBinding() storage.Binding {
	return storage.Binding{ID: "binding-1", IndexCoreRootID: "root-1", Status: storage.BindingStatusActive}
}

func newService(t *testing.T, binding storage.Binding, journal *journalReader, store *projectionStore) *projector.Service {
	t.Helper()
	sequence := 0
	service, err := projector.NewService(bindingReader{binding: binding}, journal, store,
		projector.WithCopyIDFactory(func() (catalog.CopyID, error) {
			sequence++
			return catalog.CopyID(fmt.Sprintf("00000000-0000-4000-8000-%012d", sequence)), nil
		}),
	)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	return service
}
