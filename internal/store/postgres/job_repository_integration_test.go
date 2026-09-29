package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/jobs"
)

func TestPostgresJobCreateGetAndIdempotency(t *testing.T) {
	ctx := context.Background()
	repository := migratedJobRepository(t, ctx)
	key := "create-resource-1"
	created := mustCreateJob(t, ctx, repository, jobs.CreateRequest{
		ID: "10000000-0000-4000-8000-000000000001", Type: "RESOURCE_ACQUIRE",
		Payload: json.RawMessage(`{"source":"opaque"}`), IdempotencyKey: &key, MaxAttempts: 3,
	})
	if created.State != jobs.StateQueued || created.AttemptCount != 0 || created.IdempotencyKey == nil || *created.IdempotencyKey != key {
		t.Fatalf("created job = %#v", created)
	}

	got, err := repository.Get(ctx, created.ID)
	if err != nil || got.ID != created.ID || got.Type != created.Type || !jsonEquivalent(got.Payload, created.Payload) {
		t.Fatalf("Get() = %#v, %v", got, err)
	}
	byKey, err := repository.GetByIdempotencyKey(ctx, key)
	if err != nil || byKey.ID != created.ID {
		t.Fatalf("GetByIdempotencyKey() = %#v, %v", byKey, err)
	}

	_, err = repository.Create(ctx, jobs.CreateRequest{
		ID: "10000000-0000-4000-8000-000000000002", Type: "RESOURCE_ACQUIRE",
		Payload: json.RawMessage(`{}`), IdempotencyKey: &key, MaxAttempts: 3,
	})
	if !errors.Is(err, jobs.ErrDuplicateIdempotencyKey) {
		t.Fatalf("duplicate Create() error = %v, want ErrDuplicateIdempotencyKey", err)
	}
}

func TestPostgresJobClaimLeaseAndCompletion(t *testing.T) {
	ctx := context.Background()
	repository := migratedJobRepository(t, ctx)
	job := mustCreateJob(t, ctx, repository, jobs.CreateRequest{
		ID: "20000000-0000-4000-8000-000000000001", Type: "TEST", Payload: json.RawMessage(`{}`), MaxAttempts: 3,
	})
	now := time.Date(2026, 9, 28, 10, 0, 0, 0, time.UTC)
	claimed, err := repository.ClaimNext(ctx, jobs.ClaimRequest{Owner: "worker-a", Now: now, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatalf("ClaimNext() error = %v", err)
	}
	if claimed.ID != job.ID || claimed.State != jobs.StateRunning || claimed.AttemptCount != 1 || claimed.LeaseOwner == nil || *claimed.LeaseOwner != "worker-a" || claimed.StartedAt == nil {
		t.Fatalf("claimed job = %#v", claimed)
	}

	wrongLease := jobs.LeaseRequest{ID: job.ID, Owner: "worker-b", ExpectedAttempt: claimed.AttemptCount, Now: now.Add(10 * time.Second)}
	if _, err := repository.RenewLease(ctx, jobs.RenewLeaseRequest{LeaseRequest: wrongLease, LeaseDuration: time.Minute}); !errors.Is(err, jobs.ErrLeaseConflict) {
		t.Fatalf("wrong-owner RenewLease() error = %v", err)
	}
	if _, err := repository.Succeed(ctx, wrongLease); !errors.Is(err, jobs.ErrLeaseConflict) {
		t.Fatalf("wrong-owner Succeed() error = %v", err)
	}
	if _, err := repository.Fail(ctx, jobs.FailureRequest{LeaseRequest: wrongLease, Error: "wrong owner"}); !errors.Is(err, jobs.ErrLeaseConflict) {
		t.Fatalf("wrong-owner Fail() error = %v", err)
	}

	renewed, err := repository.RenewLease(ctx, jobs.RenewLeaseRequest{
		LeaseRequest:  jobs.LeaseRequest{ID: job.ID, Owner: "worker-a", ExpectedAttempt: claimed.AttemptCount, Now: now.Add(20 * time.Second)},
		LeaseDuration: 2 * time.Minute,
	})
	if err != nil || renewed.LeaseExpiresAt == nil || claimed.LeaseExpiresAt == nil || !renewed.LeaseExpiresAt.After(*claimed.LeaseExpiresAt) {
		t.Fatalf("RenewLease() = %#v, %v", renewed, err)
	}
	succeeded, err := repository.Succeed(ctx, jobs.LeaseRequest{ID: job.ID, Owner: "worker-a", ExpectedAttempt: claimed.AttemptCount, Now: now.Add(30 * time.Second)})
	if err != nil || succeeded.State != jobs.StateSucceeded || succeeded.LeaseOwner != nil || succeeded.FinishedAt == nil {
		t.Fatalf("Succeed() = %#v, %v", succeeded, err)
	}
	if _, err := repository.ClaimNext(ctx, jobs.ClaimRequest{Owner: "worker-c", Now: now.Add(time.Hour), LeaseDuration: time.Minute}); !errors.Is(err, jobs.ErrNoClaimableJob) {
		t.Fatalf("ClaimNext() after terminal state error = %v", err)
	}
}

func TestPostgresJobRetryTimingAndMaxAttempts(t *testing.T) {
	ctx := context.Background()
	repository := migratedJobRepository(t, ctx)
	now := time.Date(2026, 9, 28, 11, 0, 0, 0, time.UTC)
	job := mustCreateJob(t, ctx, repository, jobs.CreateRequest{
		ID: "30000000-0000-4000-8000-000000000001", Type: "TEST", Payload: json.RawMessage(`{}`), MaxAttempts: 2,
	})
	first, err := repository.ClaimNext(ctx, jobs.ClaimRequest{Owner: "worker-a", Now: now, LeaseDuration: time.Minute})
	if err != nil || first.ID != job.ID {
		t.Fatalf("first ClaimNext() = %#v, %v", first, err)
	}
	retryAt := now.Add(time.Hour)
	waiting, err := repository.RetryAt(ctx, jobs.RetryRequest{
		FailureRequest: jobs.FailureRequest{
			LeaseRequest: jobs.LeaseRequest{ID: job.ID, Owner: "worker-a", ExpectedAttempt: first.AttemptCount, Now: now.Add(10 * time.Second)},
			Error:        "temporary failure",
		},
		RetryAt: retryAt,
	})
	if err != nil || waiting.State != jobs.StateRetryWait || waiting.NextAttemptAt == nil || !waiting.NextAttemptAt.Equal(retryAt) {
		t.Fatalf("RetryAt() = %#v, %v", waiting, err)
	}
	if _, err := repository.ClaimNext(ctx, jobs.ClaimRequest{Owner: "worker-b", Now: retryAt.Add(-time.Second), LeaseDuration: time.Minute}); !errors.Is(err, jobs.ErrNoClaimableJob) {
		t.Fatalf("early ClaimNext() error = %v", err)
	}
	second, err := repository.ClaimNext(ctx, jobs.ClaimRequest{Owner: "worker-b", Now: retryAt, LeaseDuration: time.Minute})
	if err != nil || second.AttemptCount != 2 || second.State != jobs.StateRunning {
		t.Fatalf("due ClaimNext() = %#v, %v", second, err)
	}
	exhausted, err := repository.RetryAt(ctx, jobs.RetryRequest{
		FailureRequest: jobs.FailureRequest{
			LeaseRequest: jobs.LeaseRequest{ID: job.ID, Owner: "worker-b", ExpectedAttempt: second.AttemptCount, Now: retryAt.Add(10 * time.Second)},
			Error:        "still failing",
		},
		RetryAt: retryAt.Add(2 * time.Hour),
	})
	if err != nil || exhausted.State != jobs.StateFailed || exhausted.NextAttemptAt != nil || exhausted.FinishedAt == nil {
		t.Fatalf("exhausted RetryAt() = %#v, %v", exhausted, err)
	}

	terminalFailure := mustCreateJob(t, ctx, repository, jobs.CreateRequest{
		ID: "30000000-0000-4000-8000-000000000002", Type: "TEST", Payload: json.RawMessage(`{}`), MaxAttempts: 2,
	})
	claimed, err := repository.ClaimNext(ctx, jobs.ClaimRequest{Owner: "worker-c", Now: retryAt.Add(3 * time.Hour), LeaseDuration: time.Minute})
	if err != nil || claimed.ID != terminalFailure.ID {
		t.Fatalf("failure ClaimNext() = %#v, %v", claimed, err)
	}
	failed, err := repository.Fail(ctx, jobs.FailureRequest{
		LeaseRequest: jobs.LeaseRequest{ID: terminalFailure.ID, Owner: "worker-c", ExpectedAttempt: claimed.AttemptCount, Now: retryAt.Add(3*time.Hour + 10*time.Second)},
		Error:        "terminal failure",
	})
	if err != nil || failed.State != jobs.StateFailed || failed.LastError == nil {
		t.Fatalf("Fail() = %#v, %v", failed, err)
	}
}

func TestPostgresJobCancellationRules(t *testing.T) {
	ctx := context.Background()
	repository := migratedJobRepository(t, ctx)
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	queued := mustCreateJob(t, ctx, repository, jobs.CreateRequest{
		ID: "40000000-0000-4000-8000-000000000001", Type: "TEST", Payload: json.RawMessage(`{}`), MaxAttempts: 2,
	})
	canceled, err := repository.Cancel(ctx, jobs.CancelRequest{ID: queued.ID, Now: now})
	if err != nil || canceled.State != jobs.StateCanceled {
		t.Fatalf("Cancel(QUEUED) = %#v, %v", canceled, err)
	}

	retryJob := mustCreateJob(t, ctx, repository, jobs.CreateRequest{
		ID: "40000000-0000-4000-8000-000000000002", Type: "TEST", Payload: json.RawMessage(`{}`), MaxAttempts: 2,
	})
	claimed, err := repository.ClaimNext(ctx, jobs.ClaimRequest{Owner: "worker-a", Now: now, LeaseDuration: time.Minute})
	if err != nil || claimed.ID != retryJob.ID {
		t.Fatalf("ClaimNext(retry job) = %#v, %v", claimed, err)
	}
	waiting, err := repository.RetryAt(ctx, jobs.RetryRequest{
		FailureRequest: jobs.FailureRequest{
			LeaseRequest: jobs.LeaseRequest{ID: retryJob.ID, Owner: "worker-a", ExpectedAttempt: claimed.AttemptCount, Now: now.Add(10 * time.Second)},
			Error:        "retry later",
		},
		RetryAt: now.Add(time.Hour),
	})
	if err != nil || waiting.State != jobs.StateRetryWait {
		t.Fatalf("RetryAt() = %#v, %v", waiting, err)
	}
	canceled, err = repository.Cancel(ctx, jobs.CancelRequest{ID: retryJob.ID, Now: now.Add(20 * time.Second)})
	if err != nil || canceled.State != jobs.StateCanceled {
		t.Fatalf("Cancel(RETRY_WAIT) = %#v, %v", canceled, err)
	}

	running := mustCreateJob(t, ctx, repository, jobs.CreateRequest{
		ID: "40000000-0000-4000-8000-000000000003", Type: "TEST", Payload: json.RawMessage(`{}`), MaxAttempts: 2,
	})
	claimed, err = repository.ClaimNext(ctx, jobs.ClaimRequest{Owner: "worker-b", Now: now.Add(2 * time.Hour), LeaseDuration: time.Minute})
	if err != nil || claimed.ID != running.ID {
		t.Fatalf("ClaimNext(running job) = %#v, %v", claimed, err)
	}
	if _, err := repository.Cancel(ctx, jobs.CancelRequest{ID: running.ID, Now: now.Add(2*time.Hour + 10*time.Second)}); !errors.Is(err, jobs.ErrInvalidTransition) {
		t.Fatalf("Cancel(RUNNING) error = %v", err)
	}
}

func TestPostgresJobLeaseGenerationFencesSameOwner(t *testing.T) {
	ctx := context.Background()
	repository := migratedJobRepository(t, ctx)
	now := time.Date(2026, 9, 28, 12, 30, 0, 0, time.UTC)
	job := mustCreateJob(t, ctx, repository, jobs.CreateRequest{
		ID: "45000000-0000-4000-8000-000000000001", Type: "TEST", Payload: json.RawMessage(`{}`), MaxAttempts: 3,
	})
	first, err := repository.ClaimNext(ctx, jobs.ClaimRequest{Owner: "worker-a", Now: now, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatalf("first ClaimNext() error = %v", err)
	}
	retryAt := now.Add(time.Minute)
	if _, err := repository.RetryAt(ctx, jobs.RetryRequest{
		FailureRequest: jobs.FailureRequest{
			LeaseRequest: jobs.LeaseRequest{ID: job.ID, Owner: "worker-a", ExpectedAttempt: first.AttemptCount, Now: now.Add(time.Second)},
			Error:        "retry",
		},
		RetryAt: retryAt,
	}); err != nil {
		t.Fatalf("RetryAt(attempt 1) error = %v", err)
	}
	second, err := repository.ClaimNext(ctx, jobs.ClaimRequest{Owner: "worker-a", Now: retryAt, LeaseDuration: time.Minute})
	if err != nil || second.AttemptCount != first.AttemptCount+1 {
		t.Fatalf("second ClaimNext() = %#v, %v", second, err)
	}

	stale := jobs.LeaseRequest{ID: job.ID, Owner: "worker-a", ExpectedAttempt: first.AttemptCount, Now: retryAt.Add(time.Second)}
	if _, err := repository.RenewLease(ctx, jobs.RenewLeaseRequest{LeaseRequest: stale, LeaseDuration: time.Minute}); !errors.Is(err, jobs.ErrLeaseConflict) {
		t.Fatalf("stale-generation RenewLease() error = %v", err)
	}
	if _, err := repository.Succeed(ctx, stale); !errors.Is(err, jobs.ErrLeaseConflict) {
		t.Fatalf("stale-generation Succeed() error = %v", err)
	}
	if _, err := repository.Fail(ctx, jobs.FailureRequest{LeaseRequest: stale, Error: "stale failure"}); !errors.Is(err, jobs.ErrLeaseConflict) {
		t.Fatalf("stale-generation Fail() error = %v", err)
	}
	if _, err := repository.RetryAt(ctx, jobs.RetryRequest{
		FailureRequest: jobs.FailureRequest{LeaseRequest: stale, Error: "stale retry"},
		RetryAt:        retryAt.Add(time.Hour),
	}); !errors.Is(err, jobs.ErrLeaseConflict) {
		t.Fatalf("stale-generation RetryAt() error = %v", err)
	}

	unchanged, err := repository.Get(ctx, job.ID)
	if err != nil || unchanged.State != jobs.StateRunning || unchanged.AttemptCount != second.AttemptCount || unchanged.LeaseOwner == nil || *unchanged.LeaseOwner != "worker-a" || unchanged.LeaseExpiresAt == nil || second.LeaseExpiresAt == nil || !unchanged.LeaseExpiresAt.Equal(*second.LeaseExpiresAt) {
		t.Fatalf("attempt 2 after stale mutations = %#v, %v", unchanged, err)
	}
	completed, err := repository.Succeed(ctx, jobs.LeaseRequest{
		ID: job.ID, Owner: "worker-a", ExpectedAttempt: second.AttemptCount, Now: retryAt.Add(2 * time.Second),
	})
	if err != nil || completed.State != jobs.StateSucceeded {
		t.Fatalf("Succeed(attempt 2) = %#v, %v", completed, err)
	}
}

func TestPostgresJobExpiredLeaseRecoveryAndRestart(t *testing.T) {
	ctx := context.Background()
	repository := migratedJobRepository(t, ctx)
	now := time.Date(2026, 9, 28, 13, 0, 0, 0, time.UTC)
	job := mustCreateJob(t, ctx, repository, jobs.CreateRequest{
		ID: "50000000-0000-4000-8000-000000000001", Type: "TEST", Payload: json.RawMessage(`{}`), MaxAttempts: 3,
	})
	claimed, err := repository.ClaimNext(ctx, jobs.ClaimRequest{Owner: "worker-a", Now: now, LeaseDuration: 10 * time.Second})
	if err != nil {
		t.Fatalf("ClaimNext() error = %v", err)
	}
	callerFuture := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	if count, err := repository.MarkExpiredRunningRecoveryRequired(ctx, jobs.RecoveryRequest{Now: callerFuture, Limit: 10}); err != nil || count != 0 {
		t.Fatalf("early recovery = %d, %v", count, err)
	}
	if _, err := repository.pool.Exec(ctx, `UPDATE jobs SET lease_expires_at = CURRENT_TIMESTAMP - interval '1 second' WHERE job_id = $1`, string(job.ID)); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	backdatedNow := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	staleLease := jobs.LeaseRequest{ID: job.ID, Owner: "worker-a", ExpectedAttempt: claimed.AttemptCount, Now: backdatedNow}
	if _, err := repository.RenewLease(ctx, jobs.RenewLeaseRequest{LeaseRequest: staleLease, LeaseDuration: time.Minute}); !errors.Is(err, jobs.ErrLeaseConflict) {
		t.Fatalf("stale RenewLease() error = %v", err)
	}
	if _, err := repository.Succeed(ctx, staleLease); !errors.Is(err, jobs.ErrLeaseConflict) {
		t.Fatalf("stale Succeed() error = %v", err)
	}
	if _, err := repository.Fail(ctx, jobs.FailureRequest{LeaseRequest: staleLease, Error: "stale owner"}); !errors.Is(err, jobs.ErrLeaseConflict) {
		t.Fatalf("stale Fail() error = %v", err)
	}
	if count, err := repository.MarkExpiredRunningRecoveryRequired(ctx, jobs.RecoveryRequest{Now: backdatedNow, Limit: 10}); err != nil || count != 1 {
		t.Fatalf("expired recovery = %d, %v", count, err)
	}
	recovered, err := repository.Get(ctx, job.ID)
	if err != nil || recovered.State != jobs.StateRecoveryRequired || recovered.LeaseOwner != nil || recovered.LeaseExpiresAt != nil || recovered.LastError == nil {
		t.Fatalf("recovered job = %#v, %v", recovered, err)
	}
	if _, err := repository.ClaimNext(ctx, jobs.ClaimRequest{Owner: "worker-b", Now: now.Add(time.Hour), LeaseDuration: time.Minute}); !errors.Is(err, jobs.ErrNoClaimableJob) {
		t.Fatalf("ClaimNext(RECOVERY_REQUIRED) error = %v", err)
	}
	if _, err := repository.Succeed(ctx, jobs.LeaseRequest{ID: job.ID, Owner: "worker-a", ExpectedAttempt: claimed.AttemptCount, Now: now.Add(11 * time.Second)}); !errors.Is(err, jobs.ErrInvalidTransition) {
		t.Fatalf("stale Succeed() error = %v", err)
	}

	reopenedPool, err := Open(ctx, os.Getenv("PANTA_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("reopen database: %v", err)
	}
	defer reopenedPool.Close()
	reopenedRepository, err := NewJobRepository(reopenedPool)
	if err != nil {
		t.Fatalf("NewJobRepository(reopened) error = %v", err)
	}
	durable, err := reopenedRepository.Get(ctx, job.ID)
	if err != nil || durable.State != jobs.StateRecoveryRequired || durable.AttemptCount != 1 {
		t.Fatalf("durable job after reopen = %#v, %v", durable, err)
	}
}

func TestPostgresJobConcurrentClaimIsExclusive(t *testing.T) {
	ctx := context.Background()
	repository := migratedJobRepository(t, ctx)
	mustCreateJob(t, ctx, repository, jobs.CreateRequest{
		ID: "60000000-0000-4000-8000-000000000001", Type: "TEST", Payload: json.RawMessage(`{}`), MaxAttempts: 2,
	})
	now := time.Date(2026, 9, 28, 14, 0, 0, 0, time.UTC)
	start := make(chan struct{})
	results := make(chan error, 2)
	var waitGroup sync.WaitGroup
	for _, owner := range []string{"worker-a", "worker-b"} {
		waitGroup.Add(1)
		go func(owner string) {
			defer waitGroup.Done()
			<-start
			_, err := repository.ClaimNext(ctx, jobs.ClaimRequest{Owner: owner, Now: now, LeaseDuration: time.Minute})
			results <- err
		}(owner)
	}
	close(start)
	waitGroup.Wait()
	close(results)

	successes, unavailable := 0, 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, jobs.ErrNoClaimableJob):
			unavailable++
		default:
			t.Fatalf("unexpected ClaimNext() error = %v", err)
		}
	}
	if successes != 1 || unavailable != 1 {
		t.Fatalf("concurrent claims: successes=%d unavailable=%d", successes, unavailable)
	}
}

func migratedJobRepository(t *testing.T, ctx context.Context) *JobRepository {
	t.Helper()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)
	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	status, err := migrator.Apply(ctx)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if !status.Compatible || status.CurrentVersion != 7 || status.LatestVersion != 7 {
		t.Fatalf("migration status = %#v", status)
	}
	repository, err := NewJobRepository(pool)
	if err != nil {
		t.Fatalf("NewJobRepository() error = %v", err)
	}
	return repository
}

func mustCreateJob(t *testing.T, ctx context.Context, repository *JobRepository, request jobs.CreateRequest) jobs.Job {
	t.Helper()
	job, err := repository.Create(ctx, request)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	return job
}
