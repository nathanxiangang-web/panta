package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/platform/config"
)

var (
	ErrInvalidAcquisitionWorker      = errors.New("invalid acquisition worker configuration or dependency")
	ErrAcquisitionWorkerRunning      = errors.New("acquisition worker is already running")
	ErrAcquisitionWorkerRecoveryDebt = errors.New("acquisition worker stopped for recovery debt")
)

type RunOnceExecutor interface {
	RunOnce(context.Context, RunOnceRequest) (RunOnceResult, error)
}

var _ RunOnceExecutor = (*AcquisitionRunOnce)(nil)

// WorkerWaiter makes cadence deterministic in tests. Production uses one
// interruptible timer only after the preceding RunOnce has returned.
type WorkerWaiter interface {
	Wait(context.Context, time.Duration) error
}

type timerWorkerWaiter struct{}

func (timerWorkerWaiter) Wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type WorkerEventKind string

const (
	WorkerStarted           WorkerEventKind = "STARTED"
	WorkerIdle              WorkerEventKind = "IDLE"
	WorkerRecoveryOnly      WorkerEventKind = "RECOVERY_ONLY"
	WorkerStage             WorkerEventKind = "STAGE"
	WorkerTransientError    WorkerEventKind = "TRANSIENT_ERROR"
	WorkerFatalRecoveryDebt WorkerEventKind = "FATAL_RECOVERY_DEBT"
	WorkerShutdown          WorkerEventKind = "SHUTDOWN"
)

// WorkerEvent is deliberately free of source links, provider references and
// credentials. Only durable IDs and closed categories are emitted.
type WorkerEvent struct {
	Kind          WorkerEventKind
	Owner         string
	Recovered     int64
	JobID         jobs.JobID
	ClaimAttempts int
	Stage         acquisition.Stage
	ErrorKind     RunOnceErrorKind
}

type WorkerEventSink func(WorkerEvent)

type WorkerOption func(*AcquisitionWorker) error

func WithWorkerWaiter(waiter WorkerWaiter) WorkerOption {
	return func(worker *AcquisitionWorker) error {
		if waiter == nil {
			return ErrInvalidAcquisitionWorker
		}
		worker.waiter = waiter
		return nil
	}
}

func WithWorkerEventSink(sink WorkerEventSink) WorkerOption {
	return func(worker *AcquisitionWorker) error {
		if sink == nil {
			return ErrInvalidAcquisitionWorker
		}
		worker.sink = sink
		return nil
	}
}

func WithWorkerClock(clock func() time.Time) WorkerOption {
	return func(worker *AcquisitionWorker) error {
		if clock == nil {
			return ErrInvalidAcquisitionWorker
		}
		worker.clock = clock
		return nil
	}
}

// WorkerFailure is an operator-visible stop reason. Its text excludes the
// underlying integration error; callers may inspect Cause through errors.Is/As.
type WorkerFailure struct {
	Kind      WorkerEventKind
	Recovered int64
	Cause     error
}

func (failure *WorkerFailure) Error() string {
	return fmt.Sprintf("acquisition worker stopped: %s", failure.Kind)
}
func (failure *WorkerFailure) Unwrap() error { return failure.Cause }

type AcquisitionWorker struct {
	runner  RunOnceExecutor
	policy  config.AcquisitionWorker
	owner   string
	waiter  WorkerWaiter
	sink    WorkerEventSink
	clock   func() time.Time
	mu      sync.Mutex
	running bool
}

// NewAcquisitionWorker is explicit opt-in composition, not process startup
// wiring. A cryptographic per-instance suffix makes the lease owner stable for
// this worker and distinct across concurrent replicas without a global ID.
func NewAcquisitionWorker(runner RunOnceExecutor, policy config.AcquisitionWorker, options ...WorkerOption) (*AcquisitionWorker, error) {
	if runner == nil || !policy.Enabled || policy.Validate() != nil {
		return nil, ErrInvalidAcquisitionWorker
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("%w: owner identity unavailable", ErrInvalidAcquisitionWorker)
	}
	worker := &AcquisitionWorker{runner: runner, policy: policy,
		owner:  policy.OwnerPrefix + "-" + hex.EncodeToString(nonce[:]),
		waiter: timerWorkerWaiter{}, sink: func(WorkerEvent) {}, clock: time.Now}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(worker); err != nil {
			return nil, err
		}
	}
	return worker, nil
}

func (worker *AcquisitionWorker) Owner() string { return worker.owner }

// Run starts the first tick immediately, then waits a full interval after each
// completed tick (including transient errors). There is no catch-up scheduling,
// overlap, detached provider goroutine or Job-only failure fallback.
func (worker *AcquisitionWorker) Run(ctx context.Context) error {
	worker.mu.Lock()
	if worker.running {
		worker.mu.Unlock()
		return ErrAcquisitionWorkerRunning
	}
	worker.running = true
	worker.mu.Unlock()
	defer func() { worker.mu.Lock(); worker.running = false; worker.mu.Unlock() }()
	if ctx.Err() != nil {
		worker.emit(WorkerEvent{Kind: WorkerShutdown})
		return nil
	}
	worker.emit(WorkerEvent{Kind: WorkerStarted})
	for {
		if ctx.Err() != nil {
			worker.emit(WorkerEvent{Kind: WorkerShutdown})
			return nil
		}
		now := worker.clock().UTC()
		tickCtx, cancel := context.WithTimeout(ctx, worker.policy.TickTimeout)
		result, err := worker.runner.RunOnce(tickCtx, RunOnceRequest{
			Owner: worker.owner, Now: now, LeaseDuration: worker.policy.LeaseDuration,
			RetryAt:            now.Add(worker.policy.RetryDelay),
			RecoveryLimit:      worker.policy.RecoveryLimit,
			ProjectorPageLimit: worker.policy.ProjectorPageLimit,
		})
		tickErr := tickCtx.Err()
		cancel()
		if errors.Is(err, acquisition.ErrLeaseRecoveryDebt) {
			worker.emit(WorkerEvent{Kind: WorkerFatalRecoveryDebt, Recovered: result.Recovered})
			return &WorkerFailure{Kind: WorkerFatalRecoveryDebt, Recovered: result.Recovered,
				Cause: errors.Join(ErrAcquisitionWorkerRecoveryDebt, err)}
		}
		if ctx.Err() != nil {
			worker.emit(WorkerEvent{Kind: WorkerShutdown})
			return nil
		}
		if err != nil || tickErr != nil {
			kind := RunOnceStageError
			var typed *RunOnceError
			if errors.As(err, &typed) {
				kind = typed.Kind
			}
			if tickErr != nil {
				kind = RunOnceCanceled
			}
			worker.emit(WorkerEvent{Kind: WorkerTransientError, Recovered: result.Recovered,
				JobID: result.ClaimedJobID, ClaimAttempts: result.ClaimAttempts, ErrorKind: kind})
		} else {
			switch result.State {
			case RunOnceIdle:
				worker.emit(WorkerEvent{Kind: WorkerIdle})
			case RunOnceRecoveryOnly:
				worker.emit(WorkerEvent{Kind: WorkerRecoveryOnly, Recovered: result.Recovered})
			case RunOnceStageCompleted:
				if result.Stage == nil {
					return &WorkerFailure{Kind: WorkerTransientError, Cause: errors.New("stage result missing")}
				}
				worker.emit(WorkerEvent{Kind: WorkerStage, Recovered: result.Recovered,
					JobID: result.ClaimedJobID, ClaimAttempts: result.ClaimAttempts, Stage: result.Stage.Stage})
			default:
				return &WorkerFailure{Kind: WorkerTransientError, Cause: errors.New("unexpected RunOnce result")}
			}
		}
		if err := worker.waiter.Wait(ctx, worker.policy.Interval); err != nil {
			if ctx.Err() != nil {
				worker.emit(WorkerEvent{Kind: WorkerShutdown})
				return nil
			}
			return &WorkerFailure{Kind: WorkerTransientError, Cause: err}
		}
	}
}

func (worker *AcquisitionWorker) emit(event WorkerEvent) {
	event.Owner = worker.owner
	worker.sink(event)
}
