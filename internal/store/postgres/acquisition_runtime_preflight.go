package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ActiveAcquisitionSession is a read-only startup inventory. Missing binding or
// connection rows are returned as invalid data, not silently filtered by a JOIN.
type ActiveAcquisitionSession struct {
	ConnectionID     string
	ProviderType     string
	CredentialRef    string
	BindingStatus    string
	ConnectionStatus string
	Complete         bool
}

// ListActiveAcquisitionSessions inventories every provider-stage Manifest before
// a worker can claim it. It never reads credential material or IndexCore data.
func ListActiveAcquisitionSessions(ctx context.Context, pool *pgxpool.Pool) ([]ActiveAcquisitionSession, error) {
	if pool == nil {
		return nil, fmt.Errorf("acquisition runtime database is required")
	}
	rows, err := pool.Query(ctx, `SELECT DISTINCT sc.storage_connection_id::text, sc.provider_type,
       sc.credential_ref, sb.status, sc.status
FROM acquisition_manifests am
LEFT JOIN storage_bindings sb ON sb.storage_binding_id=am.target_storage_binding_id
LEFT JOIN storage_connections sc ON sc.storage_connection_id=sb.storage_connection_id
WHERE am.state='ACTIVE'`)
	if err != nil {
		return nil, fmt.Errorf("inventory active acquisition sessions: %w", err)
	}
	defer rows.Close()
	var result []ActiveAcquisitionSession
	for rows.Next() {
		var id, provider, ref, bindingStatus, connectionStatus sql.NullString
		if err := rows.Scan(&id, &provider, &ref, &bindingStatus, &connectionStatus); err != nil {
			return nil, fmt.Errorf("scan active acquisition session: %w", err)
		}
		result = append(result, ActiveAcquisitionSession{ConnectionID: id.String, ProviderType: provider.String,
			CredentialRef: ref.String, BindingStatus: bindingStatus.String, ConnectionStatus: connectionStatus.String,
			Complete: id.Valid && provider.Valid && ref.Valid && bindingStatus.Valid && connectionStatus.Valid})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active acquisition sessions: %w", err)
	}
	return result, nil
}
