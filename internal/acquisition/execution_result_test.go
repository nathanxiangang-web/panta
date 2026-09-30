package acquisition_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

// --- Issue #41 tests 9 and 10: the execution step must not discard or invent
// --- provider result identity -------------------------------------------------

// TestExecutionStepSucceededCarriesExactResultName proves PROVIDER_SUCCEEDED returns
// the provider-reported direct-child name, preserved byte for byte.
func TestExecutionStepSucceededCarriesExactResultName(t *testing.T) {
	names := []string{
		"item.bin",
		"影片.mkv",
		"my file.bin",
		" leading-space.bin",
		"trailing-space.bin ",
		"MiXeD-Case.BIN",
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			fixture := newExecutionFixture(t)
			// First execution durably links a task reference.
			if _, err := fixture.service.Execute(context.Background(), fixture.request); err != nil {
				t.Fatalf("first Execute() error = %v", err)
			}
			setProviderStatus(fixture, contracts.TaskStatus{
				State:  contracts.TaskStateSucceeded,
				Result: &contracts.DownloadResult{Name: name},
			})

			result, err := fixture.service.Execute(context.Background(), fixture.request)
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if result.Outcome != acquisition.OutcomeProviderSucceeded {
				t.Fatalf("outcome = %q, want PROVIDER_SUCCEEDED", result.Outcome)
			}
			if result.ResultName == nil {
				t.Fatal("PROVIDER_SUCCEEDED discarded the provider result name")
			}
			// Byte-for-byte: no trim, no clean, no case folding.
			if *result.ResultName != name {
				t.Fatalf("ResultName = %q, want %q unchanged", *result.ResultName, name)
			}
			if len(*result.ResultName) != len(name) {
				t.Fatalf("ResultName length = %d, want %d", len(*result.ResultName), len(name))
			}
		})
	}
}

// TestExecutionStepNonSuccessDoesNotInventResultIdentity proves no outcome other
// than a provider success reports a result name, even when the provider happens to
// attach one.
func TestExecutionStepNonSuccessDoesNotInventResultIdentity(t *testing.T) {
	states := []struct {
		state contracts.TaskState
		want  acquisition.StepOutcome
	}{
		{state: contracts.TaskStatePending, want: acquisition.OutcomeProviderInProgress},
		{state: contracts.TaskStateRunning, want: acquisition.OutcomeProviderInProgress},
		{state: contracts.TaskStateFailed, want: acquisition.OutcomeProviderFailed},
		{state: contracts.TaskStateCanceled, want: acquisition.OutcomeProviderCanceled},
	}
	for _, test := range states {
		t.Run(string(test.state), func(t *testing.T) {
			fixture := newExecutionFixture(t)
			if _, err := fixture.service.Execute(context.Background(), fixture.request); err != nil {
				t.Fatalf("first Execute() error = %v", err)
			}
			// A misbehaving provider attaches a result to a non-success state.
			setProviderStatus(fixture, contracts.TaskStatus{
				State:  test.state,
				Result: &contracts.DownloadResult{Name: "must-not-escape.bin"},
			})

			result, err := fixture.service.Execute(context.Background(), fixture.request)
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if result.Outcome != test.want {
				t.Fatalf("outcome = %q, want %q", result.Outcome, test.want)
			}
			if result.ResultName != nil {
				t.Fatalf("outcome %q invented result identity %q", result.Outcome, *result.ResultName)
			}
			if result.Succeeded() {
				t.Fatal("a non-success state reported Succeeded()")
			}
		})
	}
}

// TestExecutionStepSucceededWithoutResultNameIsAllowedHere proves an omitted
// provider name is representable at this layer. Whether it is usable is the
// provider-handoff decision, not the execution step's.
func TestExecutionStepSucceededWithoutResultNameIsAllowedHere(t *testing.T) {
	fixture := newExecutionFixture(t)
	if _, err := fixture.service.Execute(context.Background(), fixture.request); err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	setProviderStatus(fixture, contracts.TaskStatus{State: contracts.TaskStateSucceeded})

	result, err := fixture.service.Execute(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result.Outcome != acquisition.OutcomeProviderSucceeded {
		t.Fatalf("outcome = %q, want PROVIDER_SUCCEEDED", result.Outcome)
	}
	if result.ResultName != nil {
		t.Fatalf("ResultName = %q, want nil when the provider reported none", *result.ResultName)
	}
	if !result.Succeeded() {
		t.Fatal("Succeeded() = false for PROVIDER_SUCCEEDED")
	}
}

// TestExecutionStepRejectsInvalidProviderResultName proves an unusable name fails
// closed here rather than travelling onward as identity.
func TestExecutionStepRejectsInvalidProviderResultName(t *testing.T) {
	for _, name := range []string{"", "   ", ".", "..", "a/b", `a\b`} {
		t.Run(name, func(t *testing.T) {
			fixture := newExecutionFixture(t)
			if _, err := fixture.service.Execute(context.Background(), fixture.request); err != nil {
				t.Fatalf("first Execute() error = %v", err)
			}
			setProviderStatus(fixture, contracts.TaskStatus{
				State:  contracts.TaskStateSucceeded,
				Result: &contracts.DownloadResult{Name: name},
			})

			if _, err := fixture.service.Execute(context.Background(), fixture.request); !errors.Is(err, acquisition.ErrProviderResultName) {
				t.Fatalf("Execute() error = %v, want ErrProviderResultName", err)
			}
		})
	}
}

// setProviderStatus installs a full scripted status, including an optional result
// identity, for the task reference the fixture durably linked.
func setProviderStatus(fixture *executionFixture, status contracts.TaskStatus) {
	fixture.provider.mu.Lock()
	defer fixture.provider.mu.Unlock()
	fixture.provider.statuses["provider-task-0001"] = status
}
