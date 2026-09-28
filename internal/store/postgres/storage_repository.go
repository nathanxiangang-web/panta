package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/nathanxiangang-web/panta/internal/storage"
)

type StorageRepository struct {
	db storageDB
}

type storageDB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

var _ storage.Repository = (*StorageRepository)(nil)

func NewStorageRepository(db storageDB) (*StorageRepository, error) {
	if db == nil {
		return nil, errors.New("storage mapping database is required")
	}
	return &StorageRepository{db: db}, nil
}

func (repository *StorageRepository) CreateConnection(ctx context.Context, connection storage.Connection) error {
	if connection.ID == "" || strings.TrimSpace(connection.ProviderType) == "" || !connection.Status.Valid() || connection.CreatedAt.IsZero() || connection.UpdatedAt.IsZero() {
		return storage.ErrInvalidArgument
	}
	_, err := repository.db.Exec(ctx, `
INSERT INTO storage_connections (
    storage_connection_id, provider_type, credential_ref, status, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6)`,
		string(connection.ID), connection.ProviderType, connection.CredentialRef,
		string(connection.Status), connection.CreatedAt, connection.UpdatedAt,
	)
	return wrapStorageWriteError("create storage connection", err)
}

func (repository *StorageRepository) GetConnection(ctx context.Context, id storage.ConnectionID) (storage.Connection, error) {
	if id == "" {
		return storage.Connection{}, storage.ErrInvalidArgument
	}
	var connection storage.Connection
	var connectionID string
	err := repository.db.QueryRow(ctx, `
SELECT storage_connection_id::text, provider_type, credential_ref, status, created_at, updated_at
FROM storage_connections
WHERE storage_connection_id = $1`, string(id)).Scan(
		&connectionID, &connection.ProviderType, &connection.CredentialRef,
		&connection.Status, &connection.CreatedAt, &connection.UpdatedAt,
	)
	if err != nil {
		return storage.Connection{}, wrapStorageReadError("connection", string(id), err)
	}
	connection.ID = storage.ConnectionID(connectionID)
	return connection, nil
}

func (repository *StorageRepository) CreateBinding(ctx context.Context, binding storage.Binding) error {
	normalizedMount, err := storage.NormalizeMountPath(binding.OpenListMountPath)
	if err != nil {
		return err
	}
	if binding.ID == "" || binding.ConnectionID == "" || strings.TrimSpace(binding.IndexCoreRootID) == "" || !binding.Status.Valid() || binding.CreatedAt.IsZero() || binding.UpdatedAt.IsZero() {
		return storage.ErrInvalidArgument
	}
	_, err = repository.db.Exec(ctx, `
INSERT INTO storage_bindings (
    storage_binding_id, storage_connection_id, openlist_mount_path,
    indexcore_root_id, status, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		string(binding.ID), string(binding.ConnectionID), normalizedMount,
		binding.IndexCoreRootID, string(binding.Status), binding.CreatedAt, binding.UpdatedAt,
	)
	return wrapStorageWriteError("create storage binding", err)
}

func (repository *StorageRepository) GetBinding(ctx context.Context, id storage.BindingID) (storage.Binding, error) {
	if id == "" {
		return storage.Binding{}, storage.ErrInvalidArgument
	}
	return repository.getBinding(ctx, `
SELECT storage_binding_id::text, storage_connection_id::text, openlist_mount_path,
       indexcore_root_id, status, created_at, updated_at
FROM storage_bindings
WHERE storage_binding_id = $1`, string(id))
}

func (repository *StorageRepository) GetBindingByIndexCoreRootID(ctx context.Context, rootID string) (storage.Binding, error) {
	if strings.TrimSpace(rootID) == "" {
		return storage.Binding{}, storage.ErrInvalidArgument
	}
	return repository.getBinding(ctx, `
SELECT storage_binding_id::text, storage_connection_id::text, openlist_mount_path,
       indexcore_root_id, status, created_at, updated_at
FROM storage_bindings
WHERE indexcore_root_id = $1`, rootID)
}

func (repository *StorageRepository) GetBindingByConnectionMount(ctx context.Context, connectionID storage.ConnectionID, mountPath string) (storage.Binding, error) {
	if connectionID == "" {
		return storage.Binding{}, storage.ErrInvalidArgument
	}
	normalizedMount, err := storage.NormalizeMountPath(mountPath)
	if err != nil {
		return storage.Binding{}, err
	}
	return repository.getBinding(ctx, `
SELECT storage_binding_id::text, storage_connection_id::text, openlist_mount_path,
       indexcore_root_id, status, created_at, updated_at
FROM storage_bindings
WHERE storage_connection_id = $1 AND openlist_mount_path = $2`, string(connectionID), normalizedMount)
}

func (repository *StorageRepository) getBinding(ctx context.Context, query string, arguments ...any) (storage.Binding, error) {
	var binding storage.Binding
	var bindingID, connectionID string
	err := repository.db.QueryRow(ctx, query, arguments...).Scan(
		&bindingID, &connectionID, &binding.OpenListMountPath,
		&binding.IndexCoreRootID, &binding.Status, &binding.CreatedAt, &binding.UpdatedAt,
	)
	if err != nil {
		return storage.Binding{}, wrapStorageReadError("binding", fmt.Sprint(arguments...), err)
	}
	binding.ID = storage.BindingID(bindingID)
	binding.ConnectionID = storage.ConnectionID(connectionID)
	return binding, nil
}

func wrapStorageReadError(entity, identity string, err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: %s %s", storage.ErrNotFound, entity, identity)
	}
	return fmt.Errorf("get storage %s %s: %w", entity, identity, err)
}

func wrapStorageWriteError(operation string, err error) error {
	if err == nil {
		return nil
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		switch postgresError.Code {
		case "23505":
			return fmt.Errorf("%w: %s", storage.ErrConflict, postgresError.ConstraintName)
		case "23503", "23514":
			return fmt.Errorf("%w: %s", storage.ErrInvalidArgument, postgresError.ConstraintName)
		}
	}
	return fmt.Errorf("%s: %w", operation, err)
}
