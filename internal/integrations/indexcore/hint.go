package indexcore

import (
	"context"
	"errors"
	"strconv"
)

// HintReason is the closed set of trusted Mutation Hint reasons. Gate 3.8 uses
// only HintReasonPossibleChange.
type HintReason string

const (
	HintReasonPossibleChange HintReason = "POSSIBLE_CHANGE"
)

func (reason HintReason) valid() bool { return reason == HintReasonPossibleChange }

// HintRequest is one trusted Mutation Hint for an exact root and directory scope.
//
// RootID is IndexCore's canonical root identity and ScopeKey is a root-absolute
// canonical directory scope. Neither may be derived from a provider scope, an
// OpenList mount, or a provider task reference.
type HintRequest struct {
	RootID   string
	ScopeKey string
	Reason   HintReason
}

// HintReceipt is IndexCore's accepted-ingest receipt.
//
// A receipt proves only that IndexCore durably ingested verification work. It is
// never Canonical truth, never a resource identity, and never a reason to mark a
// Manifest READY. SignalSeq is observation-work metadata for the IndexCore
// pipeline and must not be treated as a canonical generation.
type HintReceipt struct {
	Status    string
	RootID    string
	ScopeKey  string
	WorkState string
	SignalSeq int64
}

// HintPort submits one trusted Mutation Hint. It grants no read, query, journal,
// or database capability.
type HintPort interface {
	SubmitHint(context.Context, HintRequest) (HintReceipt, error)
}

const (
	// HintPath is IndexCore's accepted P9 trusted Hint route.
	HintPath = "/internal/v1/mutation-hints"

	// MaxHintBodyBytes is IndexCore's hard request body limit. The client refuses a
	// larger encoded request locally rather than relying on the remote 413.
	MaxHintBodyBytes = 4096

	// hintReceiptFieldBytes bounds the receipt strings the client will retain.
	hintReceiptFieldBytes = 4096
)

var (
	ErrHintInvalidRequest  = errors.New("indexcore hint invalid request")
	ErrHintUnauthorized    = errors.New("indexcore hint unauthorized")
	ErrHintRequestTooLarge = errors.New("indexcore hint request too large")
	ErrHintUnsupportedType = errors.New("indexcore hint unsupported media type")
	ErrHintBusy            = errors.New("indexcore hint backpressure")
	ErrHintUnavailable     = errors.New("indexcore hint ingest unavailable")
	ErrHintUnexpectedState = errors.New("indexcore hint unexpected status")
	ErrHintMalformed       = errors.New("indexcore hint malformed response")
	ErrHintTransport       = errors.New("indexcore hint transport error")
)

// HintError is a typed trusted-Hint failure. It never carries the bearer token.
type HintError struct {
	Kind error
	// StatusCode is the HTTP status the remote returned, or 0 for a local failure.
	StatusCode int
	// RemoteCode is IndexCore's stable error code such as "invalid_request".
	RemoteCode string
	// RetryAfter is the remote Retry-After guidance preserved from a 429, when the
	// remote supplied a usable value. The client never sleeps.
	RetryAfter *RetryAfter
	Cause      error
}

func (err *HintError) Error() string {
	message := "indexcore hint: " + err.Kind.Error()
	if err.StatusCode != 0 {
		message += " (status " + strconv.Itoa(err.StatusCode) + ")"
	}
	if err.RemoteCode != "" {
		message += " code=" + err.RemoteCode
	}
	if err.RetryAfter != nil {
		message += " retry_after=" + err.RetryAfter.String()
	}
	if err.Cause != nil {
		message += ": " + err.Cause.Error()
	}
	return message
}

func (err *HintError) Unwrap() []error {
	if err.Cause == nil {
		return []error{err.Kind}
	}
	return []error{err.Kind, err.Cause}
}
