// Package storage owns Panta product metadata that maps provider-neutral
// storage connections and OpenList mount paths to canonical IndexCore root IDs.
// It contains no provider, OpenList, IndexCore client, or persistence details.
package storage

import (
	"context"
	"errors"
	"path"
	"strings"
	"time"
)

var (
	ErrNotFound         = errors.New("storage mapping not found")
	ErrInvalidArgument  = errors.New("invalid storage mapping argument")
	ErrInvalidMountPath = errors.New("invalid OpenList mount path")
	ErrConflict         = errors.New("storage mapping conflicts with an existing record")
)

type ConnectionID string
type BindingID string
type ConnectionStatus string
type BindingStatus string

const (
	ConnectionStatusActive   ConnectionStatus = "ACTIVE"
	ConnectionStatusDisabled ConnectionStatus = "DISABLED"

	BindingStatusActive   BindingStatus = "ACTIVE"
	BindingStatusDisabled BindingStatus = "DISABLED"
)

func (status ConnectionStatus) Valid() bool {
	return status == ConnectionStatusActive || status == ConnectionStatusDisabled
}

func (status BindingStatus) Valid() bool {
	return status == BindingStatusActive || status == BindingStatusDisabled
}

type Connection struct {
	ID            ConnectionID
	ProviderType  string
	CredentialRef *string
	Status        ConnectionStatus
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type Binding struct {
	ID                BindingID
	ConnectionID      ConnectionID
	OpenListMountPath string
	IndexCoreRootID   string
	Status            BindingStatus
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// NormalizeMountPath returns the canonical slash form persisted by Panta.
// Repeated and trailing slashes are normalized, while ambiguous dot components
// and backslash-separated paths are rejected.
func NormalizeMountPath(value string) (string, error) {
	if value == "" || !strings.HasPrefix(value, "/") || strings.Contains(value, "\\") {
		return "", ErrInvalidMountPath
	}
	for _, component := range strings.Split(value, "/") {
		if component == "." || component == ".." {
			return "", ErrInvalidMountPath
		}
	}
	normalized := path.Clean(value)
	if normalized == "." || !strings.HasPrefix(normalized, "/") {
		return "", ErrInvalidMountPath
	}
	return normalized, nil
}

type Repository interface {
	CreateConnection(context.Context, Connection) error
	GetConnection(context.Context, ConnectionID) (Connection, error)
	CreateBinding(context.Context, Binding) error
	GetBinding(context.Context, BindingID) (Binding, error)
	GetBindingByIndexCoreRootID(context.Context, string) (Binding, error)
	GetBindingByConnectionMount(context.Context, ConnectionID, string) (Binding, error)
}
