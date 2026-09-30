// panta-staging is a read-only, bounded evidence collector for Gate 3.18.
// It never starts the worker, submits a source, or handles provider secrets.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/integrations/indexcore"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/platform/bootstrap"
	"github.com/nathanxiangang-web/panta/internal/store/postgres"
)

var errUnsafeInput = errors.New("invalid staging evidence configuration")
var errObservation = errors.New("staging evidence observation failed")
var errStopped = errors.New("staging acceptance stopped; preserve durable state")
var errDeadline = errors.New("staging acceptance deadline reached; preserve durable state")

const journalPageLimit = 100
const journalMaxPages = 200

type baseline struct {
	Kind                    string `json:"kind"`
	RootID                  string `json:"root_id"`
	Generation              int64  `json:"generation"`
	JournalLastSeq          int64  `json:"journal_last_seq"`
	JobsTotal               int64  `json:"jobs_total"`
	TotalClaimAttempts      int64  `json:"total_claim_attempts"`
	ProviderTasksTotal      int64  `json:"provider_tasks_total"`
	LastAppliedAdmissionSeq *int64 `json:"last_applied_admission_seq,omitempty"`
	RecordedAt              string `json:"recorded_at"`
}

type observation struct {
	Kind                string `json:"kind"`
	Outcome             string `json:"outcome"`
	Reason              string `json:"reason,omitempty"`
	RecordedAt          string `json:"recorded_at"`
	ManifestID          string `json:"manifest_id"`
	JobID               string `json:"job_id"`
	ManifestState       string `json:"manifest_state"`
	JobState            string `json:"job_state"`
	ClaimAttempts       int    `json:"claim_attempts"`
	FailureCount        int    `json:"failure_count"`
	SourceSHA256        string `json:"source_sha256"`
	ResultNameSHA256    string `json:"result_name_sha256,omitempty"`
	ResultCopyID        string `json:"result_copy_id,omitempty"`
	ProviderTaskState   string `json:"provider_task_state,omitempty"`
	ProviderRefPresent  bool   `json:"provider_ref_present"`
	RootID              string `json:"root_id"`
	RootGeneration      int64  `json:"root_generation"`
	Q5Matches           int    `json:"q5_matches"`
	Q5Ambiguous         bool   `json:"q5_ambiguous"`
	CanonicalResourceID string `json:"canonical_resource_id,omitempty"`
	Q8LastSeq           int64  `json:"q8_last_seq"`
	Q8MatchingEventSeq  int64  `json:"q8_matching_event_seq"`
	ProjectionCursor    int64  `json:"projection_cursor"`
	CopyAvailability    string `json:"copy_availability,omitempty"`
}

type settings struct {
	databaseURL string
	readURL     string
	rootID      string
	manifestID  string
	jobID       string
	baseline    string
	deadline    time.Duration
	interval    time.Duration
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		// No underlying transport/SQL error is printed: URLs and database DSNs
		// may carry sensitive material. The report contains no source_ref.
		log.Printf("staging evidence stopped: %v", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	if len(args) != 1 || args[0] != "baseline" && args[0] != "inspect" && args[0] != "watch" {
		return errUnsafeInput
	}
	cfg, err := loadSettings(args[0])
	if err != nil {
		return errUnsafeInput
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	pool, err := postgres.Open(ctx, cfg.databaseURL)
	if err != nil {
		return errObservation
	}
	defer pool.Close()
	migrator, err := postgres.NewMigrator(pool)
	if err != nil {
		return errObservation
	}
	status, err := migrator.Status(ctx)
	if err != nil || !status.Compatible || status.CurrentVersion != 12 || status.LatestVersion != 12 {
		return errObservation
	}
	client, err := indexcore.NewClient(cfg.readURL, indexcore.WithTimeout(10*time.Second))
	if err != nil {
		return errUnsafeInput
	}
	if args[0] == "baseline" {
		result, err := collectBaseline(ctx, pool, client, cfg.rootID)
		if err != nil {
			return errObservation
		}
		return writeJSON(output, result)
	}
	base, err := readBaseline(cfg.baseline, cfg.rootID)
	if err != nil {
		return errUnsafeInput
	}
	if args[0] == "inspect" {
		result, err := collectObservation(ctx, pool, client, cfg, base)
		if err != nil {
			return errObservation
		}
		if result.Outcome == "STOP" || result.ManifestState != "ACTIVE" || result.JobState != "QUEUED" ||
			result.ClaimAttempts != 0 || result.ProviderTaskState != "" || result.ResultCopyID != "" {
			result.Outcome, result.Reason = "STOP", "submission is not an unclaimed acquisition"
		} else {
			result.Outcome = "READY_TO_START"
		}
		if err := writeJSON(output, result); err != nil {
			return errObservation
		}
		if result.Outcome == "STOP" {
			return errStopped
		}
		return nil
	}
	deadlineCtx, cancel := context.WithTimeout(ctx, cfg.deadline)
	defer cancel()
	for {
		result, err := collectObservation(deadlineCtx, pool, client, cfg, base)
		if err != nil {
			if errors.Is(deadlineCtx.Err(), context.DeadlineExceeded) {
				return reportTimeout(output, cfg)
			}
			return errObservation
		}
		if errors.Is(deadlineCtx.Err(), context.DeadlineExceeded) {
			return reportTimeout(output, cfg)
		}
		if err := writeJSON(output, result); err != nil {
			return errObservation
		}
		switch result.Outcome {
		case "PASS":
			return nil
		case "STOP":
			return errStopped
		}
		timer := time.NewTimer(cfg.interval)
		select {
		case <-deadlineCtx.Done():
			timer.Stop()
			if errors.Is(deadlineCtx.Err(), context.DeadlineExceeded) {
				return reportTimeout(output, cfg)
			}
			return deadlineCtx.Err()
		case <-timer.C:
		}
	}
}

func reportTimeout(output io.Writer, cfg settings) error {
	if err := writeJSON(output, observation{Kind: "observation", Outcome: "TIMEOUT", Reason: "bounded deadline",
		RecordedAt: time.Now().UTC().Format(time.RFC3339), ManifestID: cfg.manifestID, JobID: cfg.jobID, RootID: cfg.rootID}); err != nil {
		return errObservation
	}
	return errDeadline
}

func loadSettings(mode string) (settings, error) {
	cfg := settings{databaseURL: os.Getenv("PANTA_DATABASE_URL"), readURL: os.Getenv("PANTA_INDEXCORE_BASE_URL"),
		rootID: os.Getenv("PANTA_STAGING_ROOT_ID"), manifestID: os.Getenv("PANTA_STAGING_MANIFEST_ID"),
		jobID: os.Getenv("PANTA_STAGING_JOB_ID"), baseline: os.Getenv("PANTA_STAGING_BASELINE_FILE")}
	if cfg.databaseURL == "" || cfg.readURL == "" || !safeEvidenceID(cfg.rootID) || strings.Contains(cfg.readURL, "@") {
		return settings{}, errUnsafeInput
	}
	if mode != "baseline" && (!validUUID(cfg.manifestID) || !validUUID(cfg.jobID) || cfg.baseline == "") {
		return settings{}, errUnsafeInput
	}
	if mode == "watch" {
		seconds, err := strconv.Atoi(os.Getenv("PANTA_STAGING_DEADLINE_SECONDS"))
		if err != nil || seconds < 60 || seconds > 7200 {
			return settings{}, errUnsafeInput
		}
		cfg.deadline = time.Duration(seconds) * time.Second
		cfg.interval = 5 * time.Second
	}
	return cfg, nil
}

func validUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for index, char := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if char != '-' {
				return false
			}
		} else if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F')) {
			return false
		}
	}
	return true
}

func safeEvidenceID(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') || char == '-' || char == '_' || char == '.') {
			return false
		}
	}
	return true
}

func readBaseline(path, root string) (baseline, error) {
	data, err := bootstrap.ReadProtectedFile(path, 4096)
	if err != nil {
		return baseline{}, errUnsafeInput
	}
	var parsed baseline
	if json.Unmarshal(data, &parsed) != nil || parsed.Kind != "baseline" || parsed.RootID != root ||
		parsed.Generation < 0 || parsed.JournalLastSeq < 0 {
		return baseline{}, errUnsafeInput
	}
	return parsed, nil
}

func writeJSON(output io.Writer, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return errObservation
	}
	_, err = output.Write(append(encoded, '\n'))
	return err
}

func fingerprint(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func collectBaseline(ctx context.Context, pool *pgxpool.Pool, client *indexcore.Client, root string) (baseline, error) {
	status, err := client.RootStatus(ctx, indexcore.RootStatusRequest{RootID: root})
	if err != nil || status.LifecycleState != indexcore.RootActive || status.RootID != root {
		return baseline{}, errObservation
	}
	last, _, err := journalTail(ctx, client, root, 0, "")
	if err != nil {
		return baseline{}, err
	}
	var jobsTotal, claimsTotal, tasksTotal int64
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM jobs),
       (SELECT coalesce(sum(claim_attempts), 0) FROM jobs),
       (SELECT count(*) FROM acquisition_provider_tasks)`).Scan(&jobsTotal, &claimsTotal, &tasksTotal); err != nil {
		return baseline{}, err
	}
	return baseline{Kind: "baseline", RootID: root, Generation: status.CurrentGeneration,
		JournalLastSeq: last, JobsTotal: jobsTotal, TotalClaimAttempts: claimsTotal,
		ProviderTasksTotal: tasksTotal, LastAppliedAdmissionSeq: status.LastAppliedAdmissionSeq,
		RecordedAt: time.Now().UTC().Format(time.RFC3339)}, nil
}

func journalTail(ctx context.Context, client *indexcore.Client, root string, after int64, resourceID string) (int64, int64, error) {
	last, matched := after, int64(0)
	for page := 0; page < journalMaxPages; page++ {
		events, err := client.ReadJournal(ctx, indexcore.JournalRequest{RootID: root, AfterSeq: last, Limit: journalPageLimit})
		if err != nil {
			return 0, 0, err
		}
		for _, event := range events {
			if event.EventSeq <= last {
				return 0, 0, errObservation
			}
			last = event.EventSeq
			if resourceID != "" && event.ResourceID != nil && *event.ResourceID == resourceID {
				matched = event.EventSeq
			}
		}
		if len(events) < journalPageLimit {
			return last, matched, nil
		}
	}
	return 0, 0, errObservation
}

type durableState struct {
	manifestState, jobState, targetPath, bindingID, rootID, sourceRef, providerType string
	resultName, resultCopyID                                                        *string
	claimAttempts, failureCount                                                     int
	providerTaskState                                                               string
	providerRefPresent                                                              bool
	projectionCursor                                                                int64
	copyBindingID, copyRootID, copyResourceID, copyAvailability                     string
}

func readDurable(ctx context.Context, pool *pgxpool.Pool, cfg settings) (durableState, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return durableState{}, err
	}
	defer tx.Rollback(context.Background())
	var state durableState
	var jobType string
	var jobPayload []byte
	var jobKey *string
	err = tx.QueryRow(ctx, `SELECT m.state, j.state, m.target_path, m.target_storage_binding_id::text,
       b.indexcore_root_id, m.result_name, m.result_copy_id::text, j.claim_attempts, j.attempt_count,
       m.source_ref, j.job_type, j.payload, j.idempotency_key, c.provider_type
FROM acquisition_manifests m
JOIN jobs j ON j.job_id = m.job_id
JOIN storage_bindings b ON b.storage_binding_id = m.target_storage_binding_id
JOIN storage_connections c ON c.storage_connection_id = b.storage_connection_id
WHERE m.manifest_id = $1 AND j.job_id = $2`, cfg.manifestID, cfg.jobID).Scan(
		&state.manifestState, &state.jobState, &state.targetPath, &state.bindingID, &state.rootID,
		&state.resultName, &state.resultCopyID, &state.claimAttempts, &state.failureCount, &state.sourceRef,
		&jobType, &jobPayload, &jobKey, &state.providerType)
	if err != nil || state.rootID != cfg.rootID {
		return durableState{}, errObservation
	}
	if acquisition.ValidateLinkedAcquisitionJob(acquisition.ManifestID(cfg.manifestID), jobs.Job{
		ID: jobs.JobID(cfg.jobID), Type: jobType, Payload: jobPayload, IdempotencyKey: jobKey,
	}) != nil {
		return durableState{}, errObservation
	}
	var taskJobID, taskProviderID string
	err = tx.QueryRow(ctx, `SELECT job_id::text, provider_id, state, provider_task_ref IS NOT NULL
FROM acquisition_provider_tasks WHERE manifest_id = $1`, cfg.manifestID).Scan(
		&taskJobID, &taskProviderID, &state.providerTaskState, &state.providerRefPresent)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return durableState{}, errObservation
	}
	if err == nil && (taskJobID != cfg.jobID || taskProviderID != state.providerType) {
		return durableState{}, errObservation
	}
	err = tx.QueryRow(ctx, `SELECT last_event_seq FROM indexcore_projection_cursors WHERE storage_binding_id = $1`, state.bindingID).Scan(&state.projectionCursor)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return durableState{}, errObservation
	}
	if state.resultCopyID != nil {
		err = tx.QueryRow(ctx, `SELECT storage_binding_id::text, indexcore_root_id, indexcore_resource_id, availability
FROM copies WHERE copy_id = $1`, *state.resultCopyID).Scan(&state.copyBindingID, &state.copyRootID, &state.copyResourceID, &state.copyAvailability)
		if err != nil {
			return durableState{}, errObservation
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return durableState{}, errObservation
	}
	return state, nil
}

func collectObservation(ctx context.Context, pool *pgxpool.Pool, client *indexcore.Client, cfg settings, base baseline) (observation, error) {
	state, err := readDurable(ctx, pool, cfg)
	if err != nil {
		return observation{}, err
	}
	report := observation{Kind: "observation", Outcome: "PROGRESS", RecordedAt: time.Now().UTC().Format(time.RFC3339),
		ManifestID: cfg.manifestID, JobID: cfg.jobID, ManifestState: state.manifestState, JobState: state.jobState,
		ClaimAttempts: state.claimAttempts, FailureCount: state.failureCount, ResultCopyID: deref(state.resultCopyID),
		SourceSHA256:      fingerprint(state.sourceRef),
		ProviderTaskState: state.providerTaskState, ProviderRefPresent: state.providerRefPresent,
		RootID:           cfg.rootID,
		ProjectionCursor: state.projectionCursor, CopyAvailability: state.copyAvailability}
	if state.resultName != nil {
		report.ResultNameSHA256 = fingerprint(*state.resultName)
	}
	if state.manifestState == "RECOVERY_REQUIRED" || state.jobState == "RECOVERY_REQUIRED" ||
		state.manifestState == "FAILED" || state.jobState == "FAILED" ||
		state.manifestState == "CANCELED" || state.jobState == "CANCELED" {
		report.Outcome, report.Reason = evaluate(report, state, base)
		return report, nil
	}
	rootStatus, err := client.RootStatus(ctx, indexcore.RootStatusRequest{RootID: cfg.rootID})
	if err != nil || rootStatus.RootID != cfg.rootID {
		return observation{}, errObservation
	}
	report.RootGeneration = rootStatus.CurrentGeneration
	if rootStatus.LifecycleState != indexcore.RootActive {
		report.Outcome, report.Reason = "STOP", "IndexCore root is not ACTIVE"
		return report, nil
	}
	var canonicalID string
	if state.resultName != nil {
		candidate, err := acquisition.CanonicalCandidatePath(state.targetPath, state.resultName)
		if err != nil {
			report.Outcome, report.Reason = "STOP", "invalid durable result locator"
			return report, nil
		}
		resolved, err := client.Resolve(ctx, indexcore.ResolveRequest{RootID: cfg.rootID, Path: candidate})
		if err != nil {
			return observation{}, errObservation
		}
		report.Q5Matches, report.Q5Ambiguous = len(resolved.Matches), resolved.Ambiguous
		if resolved.Ambiguous || len(resolved.Matches) > 1 {
			report.Outcome, report.Reason = "STOP", "ambiguous Q5 candidate"
			return report, nil
		}
		if len(resolved.Matches) == 1 {
			match := resolved.Matches[0]
			if !safeEvidenceID(match.ResourceID) || match.RootID != cfg.rootID || match.CanonicalPath == nil || *match.CanonicalPath != candidate ||
				match.Presence != indexcore.ResourcePresent {
				report.Outcome, report.Reason = "STOP", "Q5 identity mismatch"
				return report, nil
			}
			canonicalID, report.CanonicalResourceID = match.ResourceID, match.ResourceID
		}
	}
	last, matching, err := journalTail(ctx, client, cfg.rootID, base.JournalLastSeq, canonicalID)
	if err != nil {
		return observation{}, errObservation
	}
	report.Q8LastSeq, report.Q8MatchingEventSeq = last, matching
	report.Outcome, report.Reason = evaluate(report, state, base)
	return report, nil
}

func evaluate(report observation, state durableState, base baseline) (string, string) {
	if state.providerTaskState == "START_RESERVED" && report.ManifestState == "RECOVERY_REQUIRED" {
		return "STOP", "provider reference uncertain"
	}
	if report.ManifestState == "RECOVERY_REQUIRED" || report.JobState == "RECOVERY_REQUIRED" ||
		report.ManifestState == "FAILED" || report.JobState == "FAILED" ||
		report.ManifestState == "CANCELED" || report.JobState == "CANCELED" {
		return "STOP", "terminal failure or recovery debt"
	}
	if report.ManifestState != "READY" || report.JobState != "SUCCEEDED" {
		return "PROGRESS", ""
	}
	if report.ResultCopyID == "" || report.Q5Matches != 1 || report.Q5Ambiguous || report.CanonicalResourceID == "" ||
		report.Q8MatchingEventSeq <= base.JournalLastSeq || report.RootGeneration <= base.Generation ||
		report.ProjectionCursor < report.Q8MatchingEventSeq || state.copyAvailability != "PRESENT" ||
		state.copyBindingID != state.bindingID || state.copyRootID != report.RootID ||
		state.copyResourceID != report.CanonicalResourceID || !state.providerRefPresent {
		return "STOP", "READY lacks required canonical evidence"
	}
	return "PASS", ""
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
