package acquisitionruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/app"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/platform/bootstrap"
	"github.com/nathanxiangang-web/panta/internal/platform/config"
	"github.com/nathanxiangang-web/panta/internal/platform/operator"
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
	"github.com/nathanxiangang-web/panta/internal/runtime"
	"github.com/nathanxiangang-web/panta/internal/store/postgres"
)

func operatorProtectedConfig(t *testing.T, fixture fixtureState, hintURL string) string {
	t.Helper()
	base := t.TempDir()
	if err := os.Chmod(base, 0700); err != nil {
		t.Fatal(err)
	}
	mount := filepath.Join(base, "secrets")
	if err := os.Mkdir(mount, 0700); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{"hint.token": runtimeHintToken, "provider.cookie": runtimeTestSecret} {
		if err := os.WriteFile(filepath.Join(mount, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(base, "operator.json")
	content := fmt.Sprintf(`{"secret_dir":%q,"hint_base_url":%q,"hint_token_file":"hint.token","sessions":[{"provider_id":"115","connection_id":%q,"credential_ref":%q,"secret_file":"provider.cookie"}]}`,
		mount, hintURL, string(fixture.bindingID), runtimeTestCredential)
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestPostgresOperatorPreflightThenExplicitStart(t *testing.T) {
	fixture, manifestID, jobID := runtimeFixture(t)
	provider := &runtimeDownloader{}
	hintCalls, readCalls := 0, 0
	read := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		readCalls++
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/roots/canonical-root/resolve":
			fmt.Fprint(w, `{"matches":[{"resource_id":"operator-resource","root_id":"canonical-root","canonical_path":"/downloads/item/movie.mkv","parent_resource_id":null,"name":"movie.mkv","is_dir":false,"size":42,"mtime":"2026-09-28T01:02:03Z","resource_presence":"PRESENT","introduced_at_generation":1,"last_confirmed_generation":1}],"ambiguous":false}`)
		case "/v1/roots/canonical-root/journal":
			fmt.Fprint(w, `{"items":[{"event_seq":1,"generation_number":1,"intra_generation_seq":1,"event_type":"resource-added","resource_id":"operator-resource","payload":"{}","committed_at":"2026-09-28T01:02:03Z"}]}`)
		default:
			t.Errorf("unexpected read %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer read.Close()
	hint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hintCalls++
		if r.Header.Get("Authorization") != "Bearer "+runtimeHintToken {
			t.Error("missing trusted Hint authorization")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprint(w, `{"status":"accepted","root_id":"canonical-root","scope_key":"/downloads/item","work_state":"PENDING","signal_seq":1}`)
	}))
	defer hint.Close()
	cfg := runtimeConfig(read.URL)
	path := operatorProtectedConfig(t, fixture, hint.URL)
	constructs := 0
	build := func(ctx context.Context, c config.Config, deps runtime.RuntimeDependencies) (*runtime.Runtime, error) {
		constructs++
		deps.DownloaderFactory = func(_ context.Context, id contracts.ProviderID, secret []byte) (contracts.DownloaderProvider, error) {
			if id != "115" || string(secret) != runtimeTestSecret {
				return nil, fmt.Errorf("synthetic credential mismatch")
			}
			return provider, nil
		}
		return runtime.NewRuntime(ctx, c, deps)
	}
	if err := operator.Run(fixture.ctx, operator.Preflight, cfg, path, build); err != nil {
		t.Fatal(err)
	}
	if constructs != 1 || provider.starts != 0 || hintCalls != 0 || readCalls != 0 ||
		readJob(t, fixture.ctx, fixture.pool, jobID).ClaimAttempts != 0 ||
		readManifest(t, fixture.ctx, fixture.pool, manifestID).State != acquisition.StateActive {
		t.Fatal("read-only preflight performed work")
	}
	var tasks, copies int
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT COUNT(*) FROM acquisition_provider_tasks`).Scan(&tasks); err != nil {
		t.Fatal(err)
	}
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT COUNT(*) FROM copies`).Scan(&copies); err != nil {
		t.Fatal(err)
	}
	if tasks != 0 || copies != 0 {
		t.Fatal("preflight wrote provider task or Copy")
	}

	clock := &runtimeClock{now: time.Now().UTC()}
	ctx, stop := context.WithTimeout(fixture.ctx, 8*time.Second)
	defer stop()
	stages := 0
	startBuilder := func(buildCtx context.Context, c config.Config, deps runtime.RuntimeDependencies) (*runtime.Runtime, error) {
		deps.WorkerOptions = []app.WorkerOption{app.WithWorkerClock(clock.Now), app.WithWorkerWaiter(runtimeWaiter{clock: clock}),
			app.WithWorkerEventSink(func(event app.WorkerEvent) {
				if event.Kind == app.WorkerTransientError {
					t.Errorf("worker error: %s", event.ErrorKind)
					stop()
				}
				if event.Kind == app.WorkerStage {
					stages++
					if event.Stage == acquisition.StageCanonical {
						stop()
					}
				}
			})}
		return build(buildCtx, c, deps)
	}
	if err := operator.Run(ctx, operator.Start, cfg, path, startBuilder); err != nil {
		t.Fatal(err)
	}
	manifest := readManifest(t, fixture.ctx, fixture.pool, manifestID)
	job := readJob(t, fixture.ctx, fixture.pool, jobID)
	if constructs != 2 || stages != 3 || provider.starts != 1 || hintCalls != 1 || readCalls != 2 ||
		manifest.State != acquisition.StateReady || manifest.ResultCopyID == nil || job.State != jobs.StateSucceeded {
		t.Fatalf("operator start incomplete: constructs=%d stages=%d starts=%d Hint/read=%d/%d pair=%s/%s",
			constructs, stages, provider.starts, hintCalls, readCalls, manifest.State, job.State)
	}
	if strings.Contains(fmt.Sprint(manifest), runtimeTestSecret) {
		t.Fatal("secret persisted in Manifest")
	}
}

func TestPostgresOperatorPreflightRejectsMissingCredentialBeforeClaim(t *testing.T) {
	fixture, _, jobID := runtimeFixture(t)
	read := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("preflight made IndexCore request") }))
	defer read.Close()
	hint := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("preflight sent Hint") }))
	defer hint.Close()
	path := operatorProtectedConfig(t, fixture, hint.URL)
	if err := os.Remove(filepath.Join(filepath.Dir(path), "secrets", "provider.cookie")); err != nil {
		t.Fatal(err)
	}
	called := false
	err := operator.Run(fixture.ctx, operator.Preflight, runtimeConfig(read.URL), path,
		func(context.Context, config.Config, runtime.RuntimeDependencies) (*runtime.Runtime, error) {
			called = true
			return nil, nil
		})
	if err == nil || called || readJob(t, fixture.ctx, fixture.pool, jobID).ClaimAttempts != 0 ||
		strings.Contains(err.Error(), runtimeTestSecret) {
		t.Fatalf("unsafe preflight outcome: err=%v called=%t", err, called)
	}
	if !errors.Is(err, bootstrap.ErrUnsafeBootstrap) {
		t.Fatalf("wrong failure category: %v", err)
	}
}

func TestPostgresOperatorFatalRecoveryDebtStopsBeforeNewClaim(t *testing.T) {
	fixture, _, jobID := runtimeFixture(t)
	jobStore, err := postgres.NewJobRepository(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := jobStore.ClaimNextByType(fixture.ctx, acquisition.JobTypeAcquisition, jobs.ClaimRequest{
		Owner: "expired-operator", Now: time.Now().UTC(), LeaseDuration: 10 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(fixture.ctx, `UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second',
idempotency_key='corrupt-link' WHERE job_id=$1`, string(jobID)); err != nil {
		t.Fatal(err)
	}
	read := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("recovery debt made IndexCore request") }))
	defer read.Close()
	hint := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("recovery debt sent Hint") }))
	defer hint.Close()
	path := operatorProtectedConfig(t, fixture, hint.URL)
	provider := &runtimeDownloader{}
	build := func(ctx context.Context, c config.Config, deps runtime.RuntimeDependencies) (*runtime.Runtime, error) {
		deps.DownloaderFactory = func(context.Context, contracts.ProviderID, []byte) (contracts.DownloaderProvider, error) {
			return provider, nil
		}
		return runtime.NewRuntime(ctx, c, deps)
	}
	ctx, cancel := context.WithTimeout(fixture.ctx, 8*time.Second)
	defer cancel()
	err = operator.Run(ctx, operator.Start, runtimeConfig(read.URL), path, build)
	if !errors.Is(err, app.ErrAcquisitionWorkerRecoveryDebt) || provider.starts != 0 ||
		readJob(t, fixture.ctx, fixture.pool, jobID).ClaimAttempts != 1 {
		t.Fatalf("fatal debt did not stop worker: %v starts=%d", err, provider.starts)
	}
}

func TestPostgresOperatorCLIReadOnlyPreflight(t *testing.T) {
	if goruntime.GOOS != "linux" {
		t.Skip("protected file bootstrap is Linux-only")
	}
	fixture, manifestID, jobID := runtimeFixture(t)
	readCalls, hintCalls := 0, 0
	read := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { readCalls++ }))
	defer read.Close()
	hint := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hintCalls++ }))
	defer hint.Close()
	path := operatorProtectedConfig(t, fixture, hint.URL)
	// These bytes are synthetic and used only for local cookie parsing. No
	// provider operation or login check is performed by preflight.
	cookie := "UID=synthetic-uid;CID=synthetic-cid;SEID=synthetic-seid;KID=synthetic-kid"
	if err := os.WriteFile(filepath.Join(filepath.Dir(path), "secrets", "provider.cookie"), []byte(cookie), 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "panta")
	build := exec.Command(filepath.Join(goruntime.GOROOT(), "bin", "go"), "build", "-o", binary, "../../../cmd/panta")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build panta CLI: %v: %s", err, output)
	}
	ctx, cancel := context.WithTimeout(fixture.ctx, 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "preflight-acquisition")
	command.Env = append(os.Environ(),
		"PANTA_ACQUISITION_WORKER_ENABLED=true",
		"PANTA_DATABASE_URL="+os.Getenv("PANTA_TEST_DATABASE_URL"),
		"PANTA_INDEXCORE_BASE_URL="+read.URL,
		"PANTA_ACQUISITION_BOOTSTRAP_FILE="+path)
	output, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "acquisition preflight OK") ||
		strings.Contains(string(output), cookie) || strings.Contains(string(output), runtimeHintToken) {
		t.Fatalf("CLI preflight failed or disclosed secret: %v output=%q", err, output)
	}
	if readCalls != 0 || hintCalls != 0 || readJob(t, fixture.ctx, fixture.pool, jobID).ClaimAttempts != 0 ||
		readManifest(t, fixture.ctx, fixture.pool, manifestID).State != acquisition.StateActive {
		t.Fatal("CLI preflight performed work")
	}
}

func TestPostgresOperatorCLIExplicitIdleStartAndSIGTERM(t *testing.T) {
	if goruntime.GOOS != "linux" {
		t.Skip("signal and protected mount test is Linux-only")
	}
	fixture, manifestID, jobID := runtimeFixture(t)
	// Empty the controlled fixture so the real adapter is never driven.
	if _, err := fixture.pool.Exec(fixture.ctx, `DELETE FROM acquisition_manifests WHERE manifest_id=$1`, string(manifestID)); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(fixture.ctx, `DELETE FROM jobs WHERE job_id=$1`, string(jobID)); err != nil {
		t.Fatal(err)
	}
	readCalls, hintCalls := 0, 0
	read := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { readCalls++ }))
	defer read.Close()
	hint := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hintCalls++ }))
	defer hint.Close()
	path := operatorProtectedConfig(t, fixture, hint.URL)
	if err := os.WriteFile(filepath.Join(filepath.Dir(path), "secrets", "provider.cookie"),
		[]byte("UID=synthetic-uid;CID=synthetic-cid;SEID=synthetic-seid;KID=synthetic-kid"), 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "panta")
	build := exec.Command(filepath.Join(goruntime.GOROOT(), "bin", "go"), "build", "-o", binary, "../../../cmd/panta")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build panta CLI: %v: %s", err, output)
	}
	ctx, cancel := context.WithTimeout(fixture.ctx, 8*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "start-acquisition")
	command.Env = append(os.Environ(), "PANTA_ACQUISITION_WORKER_ENABLED=true",
		"PANTA_DATABASE_URL="+os.Getenv("PANTA_TEST_DATABASE_URL"), "PANTA_INDEXCORE_BASE_URL="+read.URL,
		"PANTA_ACQUISITION_BOOTSTRAP_FILE="+path)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond)
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("SIGTERM exit=%v output=%q", err, output.String())
	}
	if readCalls != 0 || hintCalls != 0 || strings.Contains(output.String(), runtimeHintToken) {
		t.Fatal("idle start contacted IndexCore or disclosed token")
	}
}

func TestPostgresOperatorNewRuntimeObservesRotatedSecret(t *testing.T) {
	fixture, _, jobID := runtimeFixture(t)
	read := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("preflight made read request") }))
	defer read.Close()
	hint := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("preflight sent Hint") }))
	defer hint.Close()
	path := operatorProtectedConfig(t, fixture, hint.URL)
	provider := &runtimeDownloader{}
	var observed []string
	build := func(ctx context.Context, c config.Config, deps runtime.RuntimeDependencies) (*runtime.Runtime, error) {
		deps.DownloaderFactory = func(_ context.Context, _ contracts.ProviderID, secret []byte) (contracts.DownloaderProvider, error) {
			observed = append(observed, string(secret))
			return provider, nil
		}
		return runtime.NewRuntime(ctx, c, deps)
	}
	if err := operator.Run(fixture.ctx, operator.Preflight, runtimeConfig(read.URL), path, build); err != nil {
		t.Fatal(err)
	}
	rotated := "rotated-synthetic-cookie"
	if err := os.WriteFile(filepath.Join(filepath.Dir(path), "secrets", "provider.cookie"), []byte(rotated), 0600); err != nil {
		t.Fatal(err)
	}
	if err := operator.Run(fixture.ctx, operator.Preflight, runtimeConfig(read.URL), path, build); err != nil {
		t.Fatal(err)
	}
	if len(observed) != 2 || observed[0] != runtimeTestSecret || observed[1] != rotated || provider.starts != 0 ||
		readJob(t, fixture.ctx, fixture.pool, jobID).ClaimAttempts != 0 {
		t.Fatalf("secret rotation/reconstruction failed: builds=%d starts=%d", len(observed), provider.starts)
	}
}

func TestPostgresOperatorRejectsConcurrentDoubleStart(t *testing.T) {
	fixture, manifestID, jobID := runtimeFixture(t)
	if _, err := fixture.pool.Exec(fixture.ctx, `DELETE FROM acquisition_manifests WHERE manifest_id=$1`, string(manifestID)); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(fixture.ctx, `DELETE FROM jobs WHERE job_id=$1`, string(jobID)); err != nil {
		t.Fatal(err)
	}
	read := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("idle worker made IndexCore request") }))
	defer read.Close()
	hint := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("idle worker sent Hint") }))
	defer hint.Close()
	path := operatorProtectedConfig(t, fixture, hint.URL)
	started := make(chan struct{}, 1)
	provider := &runtimeDownloader{}
	build := func(ctx context.Context, c config.Config, deps runtime.RuntimeDependencies) (*runtime.Runtime, error) {
		deps.DownloaderFactory = func(context.Context, contracts.ProviderID, []byte) (contracts.DownloaderProvider, error) {
			return provider, nil
		}
		deps.WorkerOptions = []app.WorkerOption{app.WithWorkerEventSink(func(event app.WorkerEvent) {
			if event.Kind == app.WorkerStarted {
				select {
				case started <- struct{}{}:
				default:
				}
			}
		})}
		return runtime.NewRuntime(ctx, c, deps)
	}
	ctx, cancel := context.WithCancel(fixture.ctx)
	defer cancel()
	firstDone := make(chan error, 1)
	go func() { firstDone <- operator.Run(ctx, operator.Start, runtimeConfig(read.URL), path, build) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first worker did not start")
	}
	if err := operator.Run(fixture.ctx, operator.Start, runtimeConfig(read.URL), path, build); !errors.Is(err, operator.ErrAlreadyStarting) {
		t.Fatalf("second start = %v", err)
	}
	cancel()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first worker did not stop")
	}
	if provider.starts != 0 {
		t.Fatal("idle double-start test invoked provider")
	}
}
