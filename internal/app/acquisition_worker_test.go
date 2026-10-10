package app

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/platform/config"
)

type workerRunnerDouble struct {
	mu       sync.Mutex
	calls    int
	requests []RunOnceRequest
	run      func(context.Context, RunOnceRequest, int) (RunOnceResult, error)
}

type testStartDiagnostic struct{ category string }

func (diagnostic testStartDiagnostic) Error() string              { return "private upstream response" }
func (diagnostic testStartDiagnostic) DiagnosticCategory() string { return diagnostic.category }
func (diagnostic testStartDiagnostic) DiagnosticElapsed() time.Duration {
	return 123 * time.Millisecond
}

func (runner *workerRunnerDouble) RunOnce(ctx context.Context, request RunOnceRequest) (RunOnceResult, error) {
	runner.mu.Lock()
	runner.calls++
	number := runner.calls
	runner.requests = append(runner.requests, request)
	runner.mu.Unlock()
	return runner.run(ctx, request, number)
}

func (runner *workerRunnerDouble) snapshot() (int, []RunOnceRequest) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.calls, append([]RunOnceRequest(nil), runner.requests...)
}

type workerWaitDouble struct {
	called  chan time.Duration
	release chan struct{}
}

func newWorkerWaitDouble() *workerWaitDouble {
	return &workerWaitDouble{called: make(chan time.Duration, 10), release: make(chan struct{}, 10)}
}

func (waiter *workerWaitDouble) Wait(ctx context.Context, delay time.Duration) error {
	waiter.called <- delay
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waiter.release:
		return nil
	}
}

func testWorkerPolicy() config.AcquisitionWorker {
	return config.AcquisitionWorker{Enabled: true, OwnerPrefix: "test-worker", Interval: 100 * time.Millisecond,
		TickTimeout: time.Second, LeaseDuration: 10 * time.Second, RetryDelay: 100 * time.Millisecond,
		RecoveryLimit: 5, ProjectorPageLimit: 25}
}

func newTestWorker(t *testing.T, runner RunOnceExecutor, options ...WorkerOption) *AcquisitionWorker {
	t.Helper()
	worker, err := NewAcquisitionWorker(runner, testWorkerPolicy(), options...)
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func waitWorkerSignal[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("worker signal timed out")
		var zero T
		return zero
	}
}

func TestAcquisitionWorkerSerialTicksAndStableUniqueOwner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan int, 3)
	releaseFirst := make(chan struct{})
	runner := &workerRunnerDouble{run: func(ctx context.Context, request RunOnceRequest, number int) (RunOnceResult, error) {
		entered <- number
		if number == 1 {
			<-releaseFirst
		}
		if number == 2 {
			cancel()
		}
		return RunOnceResult{State: RunOnceIdle}, nil
	}}
	waiter := newWorkerWaitDouble()
	worker := newTestWorker(t, runner, WithWorkerWaiter(waiter))
	other := newTestWorker(t, runner)
	if worker.Owner() == other.Owner() || worker.Owner() == "test-worker" || worker.Owner() == "" {
		t.Fatalf("worker owners not unique: %q/%q", worker.Owner(), other.Owner())
	}
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	if number := waitWorkerSignal(t, entered); number != 1 {
		t.Fatalf("first tick = %d", number)
	}
	if err := worker.Run(context.Background()); !errors.Is(err, ErrAcquisitionWorkerRunning) {
		t.Fatalf("second loop = %v", err)
	}
	if calls, _ := runner.snapshot(); calls != 1 {
		t.Fatalf("overlapping calls = %d", calls)
	}
	close(releaseFirst)
	if delay := waitWorkerSignal(t, waiter.called); delay != testWorkerPolicy().Interval {
		t.Fatalf("wait delay = %s", delay)
	}
	if calls, _ := runner.snapshot(); calls != 1 {
		t.Fatalf("tick before interval release: %d", calls)
	}
	waiter.release <- struct{}{}
	if number := waitWorkerSignal(t, entered); number != 2 {
		t.Fatalf("second tick = %d", number)
	}
	if err := waitWorkerSignal(t, done); err != nil {
		t.Fatalf("worker exit = %v", err)
	}
	if calls, requests := runner.snapshot(); calls != 2 || requests[0].Owner != worker.Owner() || requests[1].Owner != worker.Owner() {
		t.Fatalf("owner/calls = %d, %#v", calls, requests)
	}
}

func TestAcquisitionWorkerNormalEventsAndCadence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiter := newWorkerWaitDouble()
	events := make(chan WorkerEvent, 10)
	runner := &workerRunnerDouble{run: func(_ context.Context, _ RunOnceRequest, number int) (RunOnceResult, error) {
		switch number {
		case 1:
			return RunOnceResult{State: RunOnceIdle}, nil
		case 2:
			return RunOnceResult{State: RunOnceRecoveryOnly, Recovered: 2}, nil
		default:
			return RunOnceResult{State: RunOnceStageCompleted,
				Stage: &acquisition.StageDispatchResult{Stage: acquisition.StageProvider,
					DiagnosticCategory: "SOURCE_REJECTED", DiagnosticElapsedMS: 321,
					DiagnosticStage: "OFFLINE_POST_OUTER_JSON", DiagnosticResponseShape: "HTML"}}, nil
		}
	}}
	worker := newTestWorker(t, runner, WithWorkerWaiter(waiter), WithWorkerEventSink(func(event WorkerEvent) {
		events <- event
		if event.Kind == WorkerStage {
			cancel()
		}
	}))
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	for _, want := range []WorkerEventKind{WorkerStarted, WorkerIdle, WorkerRecoveryOnly, WorkerStage, WorkerShutdown} {
		event := waitWorkerSignal(t, events)
		if event.Kind != want || event.Owner != worker.Owner() {
			t.Fatalf("event = %#v, want %s", event, want)
		}
		if want == WorkerStage && (event.DiagnosticCategory != "SOURCE_REJECTED" || event.DiagnosticElapsedMS != 321 || event.DiagnosticStage != "OFFLINE_POST_OUTER_JSON" || event.DiagnosticResponseShape != "HTML") {
			t.Fatalf("stage diagnostic = %#v", event)
		}
		if want == WorkerIdle || want == WorkerRecoveryOnly {
			if delay := waitWorkerSignal(t, waiter.called); delay != testWorkerPolicy().Interval {
				t.Fatalf("wait delay = %s", delay)
			}
			waiter.release <- struct{}{}
		}
		if want == WorkerRecoveryOnly && event.Recovered != 2 {
			t.Fatalf("recovered count = %d", event.Recovered)
		}
	}
	if err := waitWorkerSignal(t, done); err != nil {
		t.Fatal(err)
	}
	if calls, _ := runner.snapshot(); calls != 3 {
		t.Fatalf("tick count = %d", calls)
	}
}

func TestParserMetadataIsClosed(t *testing.T) {
	if closedParserStage("PRIVATE-RESPONSE") != "UNKNOWN" || closedResponseShape("PRIVATE-RESPONSE") != "UNKNOWN" {
		t.Fatal("untrusted response metadata escaped")
	}
}

func TestAcquisitionWorkerTransientErrorIsPaced(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiter := newWorkerWaitDouble()
	events := make(chan WorkerEvent, 10)
	runner := &workerRunnerDouble{run: func(_ context.Context, _ RunOnceRequest, number int) (RunOnceResult, error) {
		if number == 1 {
			return RunOnceResult{}, &RunOnceError{Kind: RunOnceStageError, Cause: errors.New("provider unavailable")}
		}
		cancel()
		return RunOnceResult{State: RunOnceIdle}, nil
	}}
	worker := newTestWorker(t, runner, WithWorkerWaiter(waiter), WithWorkerEventSink(func(event WorkerEvent) { events <- event }))
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	if event := waitWorkerSignal(t, events); event.Kind != WorkerStarted {
		t.Fatal(event)
	}
	if event := waitWorkerSignal(t, events); event.Kind != WorkerTransientError || event.ErrorKind != RunOnceStageError {
		t.Fatal(event)
	}
	_ = waitWorkerSignal(t, waiter.called)
	if calls, _ := runner.snapshot(); calls != 1 {
		t.Fatalf("transient error caused immediate retry: %d", calls)
	}
	waiter.release <- struct{}{}
	if err := waitWorkerSignal(t, done); err != nil {
		t.Fatal(err)
	}
	if calls, _ := runner.snapshot(); calls != 2 {
		t.Fatalf("tick count = %d", calls)
	}
}

func TestAcquisitionWorkerEmitsOnlyClosedStartDiagnostic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiter := newWorkerWaitDouble()
	events := make(chan WorkerEvent, 10)
	runner := &workerRunnerDouble{run: func(_ context.Context, _ RunOnceRequest, _ int) (RunOnceResult, error) {
		return RunOnceResult{}, &RunOnceError{Kind: RunOnceStageError,
			Cause: testStartDiagnostic{category: "SOURCE_REJECTED"}}
	}}
	worker := newTestWorker(t, runner, WithWorkerWaiter(waiter), WithWorkerEventSink(func(event WorkerEvent) { events <- event }))
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	_ = waitWorkerSignal(t, events)
	event := waitWorkerSignal(t, events)
	if event.DiagnosticCategory != "SOURCE_REJECTED" || event.DiagnosticElapsedMS != 123 {
		t.Fatalf("diagnostic event = %#v", event)
	}
	cancel()
	if err := waitWorkerSignal(t, done); err != nil {
		t.Fatal(err)
	}
	if got := closedDiagnosticCategory("SECRET_SOURCE_URL"); got != "" {
		t.Fatalf("unexpected untrusted category %q", got)
	}
}

func TestAcquisitionWorkerRecoveryDebtStops(t *testing.T) {
	waiter := newWorkerWaitDouble()
	events := make(chan WorkerEvent, 10)
	runner := &workerRunnerDouble{run: func(context.Context, RunOnceRequest, int) (RunOnceResult, error) {
		return RunOnceResult{Recovered: 1}, &RunOnceError{Kind: RunOnceRecoveryDebt, Cause: acquisition.ErrLeaseRecoveryDebt}
	}}
	worker := newTestWorker(t, runner, WithWorkerWaiter(waiter), WithWorkerEventSink(func(event WorkerEvent) { events <- event }))
	err := worker.Run(context.Background())
	var failure *WorkerFailure
	if !errors.As(err, &failure) || !errors.Is(err, ErrAcquisitionWorkerRecoveryDebt) || failure.Recovered != 1 {
		t.Fatalf("debt failure = %v", err)
	}
	if calls, _ := runner.snapshot(); calls != 1 {
		t.Fatalf("debt tick count = %d", calls)
	}
	if event := waitWorkerSignal(t, events); event.Kind != WorkerStarted {
		t.Fatal(event)
	}
	if event := waitWorkerSignal(t, events); event.Kind != WorkerFatalRecoveryDebt || event.Recovered != 1 {
		t.Fatal(event)
	}
	select {
	case <-waiter.called:
		t.Fatal("debt waited for another tick")
	default:
	}
}

func TestAcquisitionWorkerCancellationBeforeAndDuringTick(t *testing.T) {
	entered := make(chan struct{}, 1)
	runner := &workerRunnerDouble{run: func(ctx context.Context, _ RunOnceRequest, _ int) (RunOnceResult, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return RunOnceResult{}, ctx.Err()
	}}
	worker := newTestWorker(t, runner)
	before, cancelBefore := context.WithCancel(context.Background())
	cancelBefore()
	if err := worker.Run(before); err != nil {
		t.Fatal(err)
	}
	if calls, _ := runner.snapshot(); calls != 0 {
		t.Fatalf("cancel-before-start called RunOnce %d times", calls)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	_ = waitWorkerSignal(t, entered)
	cancel()
	if err := waitWorkerSignal(t, done); err != nil {
		t.Fatal(err)
	}
	if calls, _ := runner.snapshot(); calls != 1 {
		t.Fatalf("cancel-during-tick called RunOnce %d times", calls)
	}
}

func TestAcquisitionWorkerCancellationWhileWaitingPreventsAnotherTick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	waiter := newWorkerWaitDouble()
	runner := &workerRunnerDouble{run: func(tickCtx context.Context, request RunOnceRequest, _ int) (RunOnceResult, error) {
		deadline, hasDeadline := tickCtx.Deadline()
		if !hasDeadline || time.Until(deadline) > testWorkerPolicy().TickTimeout ||
			request.LeaseDuration < testWorkerPolicy().TickTimeout+config.AcquisitionLeaseSafetyMargin {
			return RunOnceResult{}, errors.New("tick deadline/lease safety was not applied")
		}
		return RunOnceResult{State: RunOnceIdle}, nil
	}}
	worker := newTestWorker(t, runner, WithWorkerWaiter(waiter))
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	_ = waitWorkerSignal(t, waiter.called)
	cancel()
	if err := waitWorkerSignal(t, done); err != nil {
		t.Fatal(err)
	}
	if calls, _ := runner.snapshot(); calls != 1 {
		t.Fatalf("cancellation during wait produced %d ticks", calls)
	}
}

func TestApplicationWorkerOptInRequiresCompleteInjection(t *testing.T) {
	if _, err := New(config.Config{Environment: "test"}); err != nil {
		t.Fatalf("disabled app: %v", err)
	}
	policy := testWorkerPolicy()
	cfg := config.Config{Environment: "test", AcquisitionWorker: policy}
	if _, err := New(cfg); !errors.Is(err, ErrAcquisitionWorkerUnwired) {
		t.Fatalf("unwired app = %v", err)
	}
	if _, err := NewWithAcquisitionWorker(cfg, nil); !errors.Is(err, ErrAcquisitionWorkerUnwired) {
		t.Fatalf("nil worker = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runner := &workerRunnerDouble{run: func(context.Context, RunOnceRequest, int) (RunOnceResult, error) {
		cancel()
		return RunOnceResult{State: RunOnceIdle}, nil
	}}
	worker := newTestWorker(t, runner)
	application, err := NewWithAcquisitionWorker(cfg, worker)
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if calls, _ := runner.snapshot(); calls != 1 {
		t.Fatalf("injected worker calls = %d", calls)
	}
}
