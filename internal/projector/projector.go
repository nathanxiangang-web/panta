// Package projector maps bounded IndexCore Journal pages into Panta's derived
// unresolved Copy state. It owns orchestration and validation, not SQL.
package projector

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/integrations/indexcore"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

var (
	ErrInvalidArgument  = errors.New("invalid projection argument")
	ErrDisabledBinding  = errors.New("storage binding is disabled")
	ErrInvalidJournal   = errors.New("invalid IndexCore Journal page")
	ErrCopyIDGeneration = errors.New("Copy ID generation failed")
	ErrCursorConflict   = errors.New("projection cursor conflict")
	ErrBindingConflict  = errors.New("physical resource belongs to another storage binding")
)

type Availability string

const (
	AvailabilityPresent Availability = "PRESENT"
	AvailabilityRemoved Availability = "REMOVED"
)

// Mutation is one ordered unresolved Copy projection operation.
type Mutation struct {
	CopyID              catalog.CopyID
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
	copyIDs  CopyIDFactory
}

// CopyIDFactory creates Panta-owned candidate Copy identities before the
// persistence adapter is called.
type CopyIDFactory func() (catalog.CopyID, error)

type Option func(*Service) error

func WithCopyIDFactory(factory CopyIDFactory) Option {
	return func(service *Service) error {
		if factory == nil {
			return ErrInvalidArgument
		}
		service.copyIDs = factory
		return nil
	}
}

func NewService(bindings BindingReader, journal indexcore.JournalReadPort, store ProjectionStore, options ...Option) (*Service, error) {
	if bindings == nil || journal == nil || store == nil {
		return nil, ErrInvalidArgument
	}
	service := &Service{bindings: bindings, journal: journal, store: store, copyIDs: newCopyID}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(service); err != nil {
			return nil, err
		}
	}
	return service, nil
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

	batch, err := service.mapBatch(binding, cursor, events)
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

func (service *Service) mapBatch(binding storage.Binding, cursor int64, events []indexcore.JournalEvent) (Batch, error) {
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
		copyID, err := service.copyIDs()
		if err != nil {
			return Batch{}, fmt.Errorf("%w: %v", ErrCopyIDGeneration, err)
		}
		if copyID == "" {
			return Batch{}, ErrCopyIDGeneration
		}
		batch.Mutations = append(batch.Mutations, Mutation{
			CopyID:          copyID,
			IndexCoreRootID: binding.IndexCoreRootID, IndexCoreResourceID: *event.ResourceID,
			StorageBindingID: binding.ID, Availability: availability,
		})
	}
	return batch, nil
}

func newCopyID() (catalog.CopyID, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate UUID entropy: %w", err)
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	encoded := make([]byte, 36)
	hex.Encode(encoded[0:8], value[0:4])
	encoded[8] = '-'
	hex.Encode(encoded[9:13], value[4:6])
	encoded[13] = '-'
	hex.Encode(encoded[14:18], value[6:8])
	encoded[18] = '-'
	hex.Encode(encoded[19:23], value[8:10])
	encoded[23] = '-'
	hex.Encode(encoded[24:36], value[10:16])
	return catalog.CopyID(encoded), nil
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
