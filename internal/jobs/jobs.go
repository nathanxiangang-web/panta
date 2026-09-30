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

	// ClaimAttempts is the monotonically increasing claim generation. Every
	// successful claim increments it, it is never bounded, and it is the fencing
	// token that makes an earlier claimant's lease stale. Stale workers are
	// rejected by comparing this value, so a Job may be claimed any number of
	// times as it moves through provider, visibility, and later stages.
	ClaimAttempts int

	// FailureCount is the consumed failure/retry budget. Only a real failure
	// retry increments it, and MaxAttempts bounds it. Stage transitions such as
	// provider polling or a provider success must never consume it.
	FailureCount int
	MaxAttempts  int

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

// LeaseRequest fences one active-lease mutation.
//
// ExpectedClaim is the claim generation the caller believes it holds. It must be
// the value returned by the claim that granted the lease, never a failure count.
type LeaseRequest struct {
	ID            JobID
	Owner         string
	ExpectedClaim int
	Now           time.Time
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
//
// ClaimNext must increment the claim generation on every claim and must never
// refuse a claim because the failure budget is exhausted: RETRY_WAIT rows are
// always schedulable. Only RetryAt consumes the failure budget.
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
