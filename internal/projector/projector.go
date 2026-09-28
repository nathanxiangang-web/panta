// Package projector maps bounded IndexCore Journal pages into Panta's derived
// unresolved Copy state. It owns orchestration and validation, not SQL.
package projector

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/nathanxiangang-web/panta/internal/integrations/indexcore"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

var (
	ErrInvalidArgument = errors.New("invalid projection argument")
	ErrDisabledBinding = errors.New("storage binding is disabled")
	ErrInvalidJournal  = errors.New("invalid IndexCore Journal page")
	ErrCursorConflict  = errors.New("projection cursor conflict")
	ErrBindingConflict = errors.New("physical resource belongs to another storage binding")
)

type Availability string

const (
	AvailabilityPresent Availability = "PRESENT"
	AvailabilityRemoved Availability = "REMOVED"
)

// Mutation is one ordered unresolved Copy projection operation.
type Mutation struct {
	IndexCoreRootID     string
	IndexCoreResourceID string
	StorageBindingID    storage.BindingID
	Availability        Availability
}

// Batch is committed atomically by ProjectionStore. ExpectedCursor fences a
// concurrent projector; LastEventSeq is the last sequence actually observed.
type Batch struct {
	StorageBindingID storage.BindingID
	ExpectedCursor   int64
	LastEventSeq     int64
	Mutations        []Mutation
}

// ProjectionStore owns the atomic cursor and Copy persistence boundary.
type ProjectionStore interface {
	Cursor(context.Context, storage.BindingID) (int64, error)
	ApplyBatch(context.Context, Batch) error
}

type BindingReader interface {
	GetBinding(context.Context, storage.BindingID) (storage.Binding, error)
}

// Service performs exactly one bounded Journal projection attempt.
type Service struct {
	bindings BindingReader
	journal  indexcore.JournalReadPort
	store    ProjectionStore
}

func NewService(bindings BindingReader, journal indexcore.JournalReadPort, store ProjectionStore) (*Service, error) {
	if bindings == nil || journal == nil || store == nil {
		return nil, ErrInvalidArgument
	}
	return &Service{bindings: bindings, journal: journal, store: store}, nil
}

type Result struct {
	PreviousCursor int64
	CurrentCursor  int64
	EventsRead     int
	Mutations      int
}

// ProjectOnce reads and applies at most one Q8 page. It never loops or holds a
// database transaction while the Journal request is in flight.
func (service *Service) ProjectOnce(ctx context.Context, bindingID storage.BindingID, limit int) (Result, error) {
	if bindingID == "" || limit <= 0 {
		return Result{}, ErrInvalidArgument
	}
	binding, err := service.bindings.GetBinding(ctx, bindingID)
	if err != nil {
		return Result{}, fmt.Errorf("load storage binding: %w", err)
	}
	if binding.Status != storage.BindingStatusActive {
		return Result{}, ErrDisabledBinding
	}

	cursor, err := service.store.Cursor(ctx, binding.ID)
	if err != nil {
		return Result{}, fmt.Errorf("read projection cursor: %w", err)
	}
	events, err := service.journal.ReadJournal(ctx, indexcore.JournalRequest{
		RootID: binding.IndexCoreRootID, AfterSeq: cursor, Limit: limit,
	})
	if err != nil {
		return Result{}, fmt.Errorf("read IndexCore Journal: %w", err)
	}
	result := Result{PreviousCursor: cursor, CurrentCursor: cursor, EventsRead: len(events)}
	if len(events) == 0 {
		return result, nil
	}

	batch, err := mapBatch(binding, cursor, events)
	if err != nil {
		return Result{}, err
	}
	if err := service.store.ApplyBatch(ctx, batch); err != nil {
		return Result{}, fmt.Errorf("apply projection batch: %w", err)
	}
	result.CurrentCursor = batch.LastEventSeq
	result.Mutations = len(batch.Mutations)
	return result, nil
}

func mapBatch(binding storage.Binding, cursor int64, events []indexcore.JournalEvent) (Batch, error) {
	batch := Batch{StorageBindingID: binding.ID, ExpectedCursor: cursor, LastEventSeq: cursor}
	previous := cursor
	for position, event := range events {
		if event.EventSeq <= cursor || event.EventSeq <= previous {
			return Batch{}, fmt.Errorf("%w: event %d has non-increasing sequence %d after %d", ErrInvalidJournal, position, event.EventSeq, previous)
		}
		previous = event.EventSeq
		batch.LastEventSeq = event.EventSeq

		availability, resourceEvent, valid := eventAvailability(event.EventType)
		if !valid {
			return Batch{}, fmt.Errorf("%w: event %d has unknown event type %q", ErrInvalidJournal, position, event.EventType)
		}
		if !resourceEvent {
			continue
		}
		if event.ResourceID == nil || strings.TrimSpace(*event.ResourceID) == "" {
			return Batch{}, fmt.Errorf("%w: event %d requires resource_id", ErrInvalidJournal, position)
		}
		batch.Mutations = append(batch.Mutations, Mutation{
			IndexCoreRootID: binding.IndexCoreRootID, IndexCoreResourceID: *event.ResourceID,
			StorageBindingID: binding.ID, Availability: availability,
		})
	}
	return batch, nil
}

func eventAvailability(eventType indexcore.JournalEventType) (Availability, bool, bool) {
	switch eventType {
	case indexcore.EventResourceAdded, indexcore.EventResourceUpdated,
		indexcore.EventResourceRenamed, indexcore.EventResourceMoved:
		return AvailabilityPresent, true, true
	case indexcore.EventResourceRemoved:
		return AvailabilityRemoved, true, true
	case indexcore.EventRootDeprecated, indexcore.EventRootDeleted:
		return "", false, true
	default:
		return "", false, false
	}
}
