package app

import (
	"context"
	"fmt"
	"net/http"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/integrations/indexcore"
)

// Package app is the composition boundary, so this is where the concrete IndexCore
// trusted Hint transport is adapted to the acquisition-owned port. Without this
// adapter the two capabilities would satisfy their own tests while the real process
// could never be wired: acquisition would ask for a MutationHintPort that no
// production type provides.
//
// The adapter is deliberately the only place that knows both capabilities, and it
// carries no policy of its own beyond the closed reason translation. Loopback
// enforcement, the token, the bounded timeout, and the no-redirect rule all stay in
// indexcore.NewHintClient, so there is one place to audit them.

// Compile-time proof of the two capabilities the composition has to bridge. If
// either assertion stops holding, the build fails here instead of at runtime
// wiring.
var (
	_ acquisition.MutationHintPort = (*HintPort)(nil)
	_ indexcore.HintPort           = (*indexcore.HintClient)(nil)
)

// HintConfig is the composition-level configuration for the trusted Hint client.
// It mirrors the indexcore hint configuration rather than reinterpreting it, so the
// boundary keeps exactly one meaning for each value.
type HintConfig struct {
	// BaseURL is the address of IndexCore's loopback-only trusted Hint listener.
	// indexcore.NewHintClient enforces that the host is exactly 127.0.0.1 or ::1.
	BaseURL string
	// Token is the trusted Hint bearer token. It is passed through unchanged and is
	// never logged.
	Token string
	// HTTPClient overrides the transport for tests. The client re-arms its timeout
	// and no-redirect rule, so an injected client is never trusted directly. Nil
	// uses a bounded default.
	HTTPClient *http.Client
}

// HintPort adapts the concrete IndexCore trusted Hint client to the
// acquisition-owned MutationHintPort.
type HintPort struct {
	client *indexcore.HintClient
}

// NewHintPort builds the production adapter. It fails closed on an invalid
// configuration, so a misconfigured Hint endpoint can never be wired into the
// acquisition flow.
func NewHintPort(config HintConfig) (*HintPort, error) {
	client, err := indexcore.NewHintClient(indexcore.HintConfig{
		BaseURL:    config.BaseURL,
		Token:      config.Token,
		HTTPClient: config.HTTPClient,
	})
	if err != nil {
		return nil, fmt.Errorf("compose IndexCore trusted Hint client: %w", err)
	}
	return &HintPort{client: client}, nil
}

// NewHintPortWithClient composes an already-built client. It exists so a caller can
// share one client, and so tests can prove the adapter wraps the real client.
func NewHintPortWithClient(client *indexcore.HintClient) (*HintPort, error) {
	if client == nil {
		return nil, fmt.Errorf("compose IndexCore trusted Hint client: client is required")
	}
	return &HintPort{client: client}, nil
}

// SubmitMutationHint implements acquisition.MutationHintPort by translating the
// acquisition-owned request and receipt to and from the IndexCore capability.
func (port *HintPort) SubmitMutationHint(
	ctx context.Context,
	hint acquisition.MutationHint,
) (acquisition.MutationHintReceipt, error) {
	if port == nil || port.client == nil {
		return acquisition.MutationHintReceipt{}, fmt.Errorf(
			"compose IndexCore trusted Hint client: adapter is not configured")
	}
	reason, err := hintReasonToIndexCore(hint.Reason)
	if err != nil {
		return acquisition.MutationHintReceipt{}, err
	}
	receipt, err := port.client.SubmitHint(ctx, indexcore.HintRequest{
		RootID:   hint.RootID,
		ScopeKey: hint.ScopeKey,
		Reason:   reason,
	})
	if err != nil {
		// The typed IndexCore failure crosses the boundary unchanged so callers keep
		// the distinct backpressure/authorization/protocol classification.
		return acquisition.MutationHintReceipt{}, err
	}
	return acquisition.MutationHintReceipt{
		Status:    receipt.Status,
		RootID:    receipt.RootID,
		ScopeKey:  receipt.ScopeKey,
		WorkState: receipt.WorkState,
		SignalSeq: receipt.SignalSeq,
	}, nil
}

// hintReasonToIndexCore is the closed translation between the two reason
// vocabularies. An unsupported reason fails closed rather than being forwarded.
func hintReasonToIndexCore(reason acquisition.MutationHintReason) (indexcore.HintReason, error) {
	switch reason {
	case acquisition.MutationHintPossibleChange:
		return indexcore.HintReasonPossibleChange, nil
	default:
		return "", fmt.Errorf("%w: unsupported mutation hint reason %q",
			acquisition.ErrInvalidRefreshRequest, reason)
	}
}
