// Package jobs owns Panta's provider-neutral durable control-plane state model.
// It defines persistence contracts only; provider execution and scheduling live
// outside this package.
package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound                = errors.New("job not found")
	ErrNoClaimableJob          = errors.New("no claimable job")
	ErrLeaseConflict           = errors.New("job lease conflict")
	ErrInvalidTransition       = errors.New("invalid job state transition")
	ErrDuplicateIdempotencyKey = errors.New("duplicate job idempotency key")
	ErrInvalidArgument         = errors.New("invalid job argument")
)

type JobID string
type State string

const (
	StateQueued           State = "QUEUED"
	StateRunning          State = "RUNNING"
	StateRetryWait        State = "RETRY_WAIT"
	StateRecoveryRequired State = "RECOVERY_REQUIRED"
	StateSucceeded        State = "SUCCEEDED"
	StateFailed           State = "FAILED"
	StateCanceled         State = "CANCELED"
)

func (state State) Terminal() bool {
	return state == StateSucceeded || state == StateFailed || state == StateCanceled
}

type Job struct {
	ID             JobID
	Type           string
	Payload        json.RawMessage
	State          State
	IdempotencyKey *string
	AttemptCount   int
	MaxAttempts    int
	NextAttemptAt  *time.Time
	LeaseOwner     *string
	LeaseExpiresAt *time.Time
	LastError      *string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
}

type CreateRequest struct {
	ID             JobID
	Type           string
	Payload        json.RawMessage
	IdempotencyKey *string
	MaxAttempts    int
}

type ClaimRequest struct {
	Owner         string
	Now           time.Time
	LeaseDuration time.Duration
}

type LeaseRequest struct {
	ID              JobID
	Owner           string
	ExpectedAttempt int
	Now             time.Time
}

type RenewLeaseRequest struct {
	LeaseRequest
	LeaseDuration time.Duration
}

type FailureRequest struct {
	LeaseRequest
	Error string
}

type RetryRequest struct {
	FailureRequest
	RetryAt time.Time
}

type CancelRequest struct {
	ID  JobID
	Now time.Time
}

type RecoveryRequest struct {
	Now   time.Time
	Limit int
}

// Repository persists the minimum durable job state machine. Implementations
// must make ClaimNext atomic and enforce owner plus claim-generation fencing on
// every active-lease mutation. Lease expiry authorization uses database time.
type Repository interface {
	Create(context.Context, CreateRequest) (Job, error)
	Get(context.Context, JobID) (Job, error)
	GetByIdempotencyKey(context.Context, string) (Job, error)
	ClaimNext(context.Context, ClaimRequest) (Job, error)
	RenewLease(context.Context, RenewLeaseRequest) (Job, error)
	Succeed(context.Context, LeaseRequest) (Job, error)
	Fail(context.Context, FailureRequest) (Job, error)
	RetryAt(context.Context, RetryRequest) (Job, error)
	Cancel(context.Context, CancelRequest) (Job, error)
	MarkExpiredRunningRecoveryRequired(context.Context, RecoveryRequest) (int64, error)
}
