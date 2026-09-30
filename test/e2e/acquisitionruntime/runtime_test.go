package acquisitionruntime

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/app"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/platform/config"
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
	"github.com/nathanxiangang-web/panta/internal/runtime"
	"github.com/nathanxiangang-web/panta/internal/storage"
	"github.com/nathanxiangang-web/panta/internal/store/postgres"
)

const runtimeTestCredential = "runtime-secret-ref"
const runtimeTestSecret = "synthetic-cookie-not-a-real-account"
const runtimeHintToken = "0123456789abcdef0123456789abcdef"

type runtimeSecretResolver struct{ missing bool }

func (resolver runtimeSecretResolver) ResolveSecret(_ context.Context, ref contracts.CredentialRef) ([]byte, error) {
	if resolver.missing || ref != runtimeTestCredential {
		return nil, contracts.ErrSecretUnavailable
	}
	return []byte(runtimeTestSecret), nil
}

type runtimeDownloader struct {
	starts, polls int
	cancel        context.CancelFunc
}

type runtimeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *runtimeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}
func (clock *runtimeClock) Advance(delay time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(delay)
	clock.mu.Unlock()
}

type runtimeWaiter struct{ clock *runtimeClock }

func (waiter runtimeWaiter) Wait(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	waiter.clock.Advance(2 * delay)
	return nil
}
func (provider *runtimeDownloader) Descriptor() contracts.Descriptor {
	return contracts.Descriptor{ID: "115", DisplayName: "controlled authenticated 115", Capabilities: contracts.CapabilitySet{Downloader: true}}
}
func (provider *runtimeDownloader) StartDownload(_ context.Context, request contracts.DownloadRequest) (contracts.TaskReference, error) {
	provider.starts++
	if provider.cancel != nil {
		provider.cancel()
		return contracts.TaskReference{}, context.Canceled
	}
	if request.Target.Scope != "runtime-save-dir" {
		return contracts.TaskReference{}, errors.New("wrong provider scope")
	}
	return contracts.TaskReference{Value: "controlled-task-ref"}, nil
}
func (provider *runtimeDownloader) DownloadStatus(_ context.Context, ref contracts.TaskReference) (contracts.TaskStatus, error) {
	provider.polls++
	if ref.Value != "controlled-task-ref" {
		return contracts.TaskStatus{}, errors.New("wrong task reference")
	}
	return contracts.TaskStatus{Reference: ref, State: contracts.TaskStateSucceeded,
		Result: &contracts.DownloadResult{Name: "movie.mkv"}}, nil
}
func (*runtimeDownloader) CancelDownload(context.Context, contracts.TaskReference) error { return nil }

func runtimePolicy() config.AcquisitionWorker {
	return config.AcquisitionWorker{Enabled: true, OwnerPrefix: "runtime-pg", Interval: 100 * time.Millisecond,
		TickTimeout: time.Second, LeaseDuration: 10 * time.Second, RetryDelay: 100 * time.Millisecond,
		RecoveryLimit: 10, ProjectorPageLimit: 25}
}

type fixtureState struct {
	ctx       context.Context
	pool      *pgxpool.Pool
	bindingID storage.BindingID
}

func runtimeFixture(t *testing.T) (fixtureState, acquisition.ManifestID, jobs.JobID) {
	t.Helper()
	databaseURL := os.Getenv("PANTA_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("PANTA_TEST_DATABASE_URL is not set")
	}
	parsed, err := pgxpool.ParseConfig(databaseURL)
	if err != nil || (parsed.ConnConfig.Database != "panta_test" && !strings.HasSuffix(parsed.ConnConfig.Database, "_test")) {
		t.Fatal("refusing destructive integration test outside a _test database")
	}
	ctx := context.Background()
	pool, err := postgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DROP SCHEMA public CASCADE; CREATE SCHEMA public`) })
	migrator, err := postgres.NewMigrator(pool)
	if err != nil {
		t.Fatal(err)
	}
	status, err := migrator.Apply(ctx)
	if err != nil || status.CurrentVersion != 12 || !status.Compatible {
		t.Fatalf("migrate test DB: %v", err)
	}
	bindingID := storage.BindingID("54000000-0000-4000-8000-000000000001")
	manifestID := acquisition.ManifestID("54000000-0000-4000-8000-000000000080")
	jobID := jobs.JobID("54000000-0000-4000-8000-000000000081")
	now := time.Now().UTC()
	if _, err := pool.Exec(ctx, `INSERT INTO storage_connections
(storage_connection_id,provider_type,credential_ref,status,created_at,updated_at)
VALUES ($1,'115',$2,'ACTIVE',$3,$3)`, string(bindingID), runtimeTestCredential, now); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO storage_bindings
(storage_binding_id,storage_connection_id,provider_scope,openlist_mount_path,indexcore_root_id,status,created_at,updated_at)
VALUES ($1,$1,'runtime-save-dir','/canonical-root','canonical-root','ACTIVE',$2,$2)`, string(bindingID), now); err != nil {
		t.Fatal(err)
	}
	jobStore, err := postgres.NewJobRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	key := acquisition.AcquisitionJobIdempotencyKey(manifestID)
	if _, err := jobStore.Create(ctx, jobs.CreateRequest{ID: jobID, Type: acquisition.JobTypeAcquisition,
		Payload:        []byte(fmt.Sprintf(`{"manifest_id":"%s","schema_version":1}`, manifestID)),
		IdempotencyKey: &key, MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO acquisition_manifests
(manifest_id,source_type,source_ref,expected_name,target_storage_binding_id,target_path,job_id,state,created_at,updated_at)
VALUES ($1,'magnet','magnet:?xt=urn:btih:synthetic','movie.mkv',$2,'/downloads/item',$3,'ACTIVE',$4,$4)`,
		string(manifestID), string(bindingID), string(jobID), now); err != nil {
		t.Fatal(err)
	}
	fixture := fixtureState{ctx: ctx, pool: pool, bindingID: bindingID}
	return fixture, manifestID, jobID
}

func runtimeDeps(fixture fixtureState, hintURL string, provider *runtimeDownloader) runtime.RuntimeDependencies {
	return runtime.RuntimeDependencies{Secrets: runtimeSecretResolver{},
		Sessions:    []runtime.RuntimeSession{{ProviderID: "115", ConnectionID: storage.ConnectionID(fixture.bindingID), CredentialRef: runtimeTestCredential}},
		HintBaseURL: hintURL, HintToken: runtimeHintToken,
		DownloaderFactory: func(_ context.Context, id contracts.ProviderID, secret []byte) (contracts.DownloaderProvider, error) {
			if id != "115" || string(secret) != runtimeTestSecret {
				return nil, errors.New("credential not supplied")
			}
			return provider, nil
		}}
}

func runtimeConfig(readURL string) config.Config {
	return config.Config{Environment: "test", DatabaseURL: os.Getenv("PANTA_TEST_DATABASE_URL"), IndexCoreBaseURL: readURL,
		AcquisitionWorker: runtimePolicy()}
}

func readManifest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id acquisition.ManifestID) acquisition.Manifest {
	t.Helper()
	repository, err := postgres.NewAcquisitionManifestRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := repository.GetManifest(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return manifest
}

func readJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id jobs.JobID) jobs.Job {
	t.Helper()
	repository, err := postgres.NewJobRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	job, err := repository.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func TestPostgresAcquisitionRuntimeFailClosedBeforeClaim(t *testing.T) {
	fixture, manifestID, jobID := runtimeFixture(t)
	read := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unexpected read request") }))
	defer read.Close()
	hint := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unexpected Hint request") }))
	defer hint.Close()
	cfg := runtimeConfig(read.URL)
	deps := runtimeDeps(fixture, hint.URL, &runtimeDownloader{})
	for name, mutate := range map[string]func(*runtime.RuntimeDependencies){
		"missing secret":   func(d *runtime.RuntimeDependencies) { d.Secrets = runtimeSecretResolver{missing: true} },
		"wrong credential": func(d *runtime.RuntimeDependencies) { d.Sessions[0].CredentialRef = "wrong-ref" },
		"wrong connection": func(d *runtime.RuntimeDependencies) {
			d.Sessions[0].ConnectionID = "54000000-0000-4000-8000-000000000099"
		},
		"unknown provider":   func(d *runtime.RuntimeDependencies) { d.Sessions[0].ProviderID = "unknown" },
		"missing Hint token": func(d *runtime.RuntimeDependencies) { d.HintToken = "" },
		"invalid Hint token": func(d *runtime.RuntimeDependencies) { d.HintToken = "short" },
		"missing read URL":   func(d *runtime.RuntimeDependencies) {},
	} {
		t.Run(name, func(t *testing.T) {
			invalid := deps
			invalid.Sessions = append([]runtime.RuntimeSession(nil), deps.Sessions...)
			localCfg := cfg
			if name == "missing read URL" {
				localCfg.IndexCoreBaseURL = ""
			}
			mutate(&invalid)
			application, err := runtime.NewRuntime(fixture.ctx, localCfg, invalid)
			if err == nil {
				application.Close()
				t.Fatal("unsafe runtime started")
			}
			if strings.Contains(err.Error(), runtimeTestSecret) || strings.Contains(err.Error(), runtimeHintToken) {
				t.Fatalf("secret leaked: %v", err)
			}
			job := readJob(t, fixture.ctx, fixture.pool, jobID)
			if job.State != jobs.StateQueued || job.ClaimAttempts != 0 {
				t.Fatalf("startup claimed Job: %#v", job)
			}
		})
	}
	// A valid configured session cannot cover a different ACTIVE Manifest's
	// provider connection, even when the ProviderID is identical.
	const otherID = "54000000-0000-4000-8000-000000000099"
	if _, err := fixture.pool.Exec(fixture.ctx, `INSERT INTO storage_connections
(storage_connection_id,provider_type,credential_ref,status,created_at,updated_at)
VALUES ($1,'115','other-ref','ACTIVE',clock_timestamp(),clock_timestamp())`, otherID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(fixture.ctx, `INSERT INTO storage_bindings
(storage_binding_id,storage_connection_id,provider_scope,openlist_mount_path,indexcore_root_id,status,created_at,updated_at)
VALUES ($1,$1,'other-scope','/other-root','other-root','ACTIVE',clock_timestamp(),clock_timestamp())`, otherID); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(fixture.ctx, `UPDATE acquisition_manifests SET target_storage_binding_id=$2 WHERE manifest_id=$1`,
		string(manifestID), otherID); err != nil {
		t.Fatal(err)
	}
	if application, err := runtime.NewRuntime(fixture.ctx, cfg, deps); !errors.Is(err, runtime.ErrInvalidAcquisitionRuntime) {
		if application != nil {
			application.Close()
		}
		t.Fatalf("unregistered active connection accepted: %v", err)
	}
	if readJob(t, fixture.ctx, fixture.pool, jobID).ClaimAttempts != 0 {
		t.Fatal("preflight claimed Job")
	}
	// Dropping one migration history row simulates a partially upgraded product DB.
	if _, err := fixture.pool.Exec(fixture.ctx, `DELETE FROM schema_migrations WHERE version=12`); err != nil {
		t.Fatal(err)
	}
	if application, err := runtime.NewRuntime(fixture.ctx, cfg, deps); !errors.Is(err, postgres.ErrIncompatibleSchema) {
		if application != nil {
			application.Close()
		}
		t.Fatalf("broken schema accepted: %v", err)
	}
}

func TestPostgresAcquisitionRuntimeRealProtocolE2E(t *testing.T) {
	fixture, manifestID, jobID := runtimeFixture(t)
	provider := &runtimeDownloader{}
	visible := false
	hintCalls, resolveCalls, journalCalls := 0, 0, 0
	read := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/roots/canonical-root/resolve":
			resolveCalls++
			if r.URL.Query().Get("path") != "/downloads/item/movie.mkv" {
				t.Errorf("Q5 path = %s", r.URL.Query().Get("path"))
			}
			if !visible {
				fmt.Fprint(w, `{"matches":[],"ambiguous":false}`)
				return
			}
			fmt.Fprint(w, `{"matches":[{"resource_id":"runtime-resource","root_id":"canonical-root","canonical_path":"/downloads/item/movie.mkv","parent_resource_id":null,"name":"movie.mkv","is_dir":false,"size":42,"mtime":"2026-09-28T01:02:03Z","resource_presence":"PRESENT","introduced_at_generation":1,"last_confirmed_generation":1}],"ambiguous":false}`)
		case "/v1/roots/canonical-root/journal":
			journalCalls++
			fmt.Fprint(w, `{"items":[{"event_seq":1,"generation_number":1,"intra_generation_seq":1,"event_type":"resource-added","resource_id":"runtime-resource","payload":"{}","committed_at":"2026-09-28T01:02:03Z"}]}`)
		default:
			t.Errorf("unexpected read path %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer read.Close()
	hint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hintCalls++
		if r.Header.Get("Authorization") != "Bearer "+runtimeHintToken || r.URL.Path != "/internal/v1/mutation-hints" {
			t.Errorf("invalid Hint request")
		}
		visible = true
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		fmt.Fprint(w, `{"status":"accepted","root_id":"canonical-root","scope_key":"/downloads/item","work_state":"PENDING","signal_seq":1}`)
	}))
	defer hint.Close()
	cfg := runtimeConfig(read.URL)
	deps := runtimeDeps(fixture, hint.URL, provider)
	clock := &runtimeClock{now: time.Now().UTC()}
	waiter := runtimeWaiter{clock: clock}
	timeoutCtx, timeoutCancel := context.WithTimeout(fixture.ctx, 8*time.Second)
	defer timeoutCancel()
	runCtx, stop := context.WithCancel(timeoutCtx)
	defer stop()
	var stages []acquisition.Stage
	deps.WorkerOptions = []app.WorkerOption{app.WithWorkerClock(clock.Now), app.WithWorkerWaiter(waiter),
		app.WithWorkerEventSink(func(event app.WorkerEvent) {
			if event.Kind == app.WorkerTransientError || event.Kind == app.WorkerFatalRecoveryDebt {
				t.Errorf("worker failed at %s: %s", event.Kind, event.ErrorKind)
				stop()
			}
			if event.Kind != app.WorkerStage {
				return
			}
			stages = append(stages, event.Stage)
			if event.Stage == acquisition.StageVisibility {
				manifest := readManifest(t, fixture.ctx, fixture.pool, manifestID)
				if manifest.State != acquisition.StateAwaitingCanonical || manifest.ResultCopyID != nil {
					t.Errorf("Hint alone caused READY: %#v", manifest)
				}
			}
			if event.Stage == acquisition.StageCanonical {
				stop()
			}
		})}
	application, err := runtime.NewRuntime(fixture.ctx, cfg, deps)
	if err != nil {
		t.Fatal(err)
	}
	defer application.Close()
	if err := application.Run(runCtx); err != nil {
		t.Fatal(err)
	}
	manifest := readManifest(t, fixture.ctx, fixture.pool, manifestID)
	job := readJob(t, fixture.ctx, fixture.pool, jobID)
	if manifest.State != acquisition.StateReady || manifest.ResultCopyID == nil || job.State != jobs.StateSucceeded ||
		provider.starts != 1 || provider.polls != 1 || hintCalls != 1 || resolveCalls != 1 || journalCalls != 1 {
		t.Fatalf("final=%s copy=%v job=%s starts=%d polls=%d hint/Q5/Q8=%d/%d/%d stages=%v",
			manifest.State, manifest.ResultCopyID, job.State, provider.starts, provider.polls, hintCalls, resolveCalls, journalCalls, stages)
	}
	if len(stages) != 3 || stages[0] != acquisition.StageProvider ||
		stages[1] != acquisition.StageVisibility || stages[2] != acquisition.StageCanonical {
		t.Fatalf("stages=%v", stages)
	}
	application.Close()
	application.Close()
	if err := fixture.pool.Ping(fixture.ctx); err != nil {
		t.Fatalf("runtime closed caller-owned pool: %v", err)
	}
	response, err := read.Client().Get(read.URL + "/v1/roots/canonical-root/resolve?path=%2Fdownloads%2Fitem%2Fmovie.mkv")
	if err != nil {
		t.Fatalf("runtime closed caller-owned HTTP server: %v", err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("caller-owned HTTP server status = %d", response.StatusCode)
	}
}

func TestPostgresAcquisitionRuntimeCanceledStartRecoversWithoutRestart(t *testing.T) {
	fixture, manifestID, jobID := runtimeFixture(t)
	read := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unexpected IndexCore read") }))
	defer read.Close()
	hint := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("unexpected Hint") }))
	defer hint.Close()
	provider := &runtimeDownloader{}
	cfg := runtimeConfig(read.URL)
	deps := runtimeDeps(fixture, hint.URL, provider)
	firstCtx, firstStop := context.WithCancel(fixture.ctx)
	defer firstStop()
	provider.cancel = firstStop
	first, err := runtime.NewRuntime(fixture.ctx, cfg, deps)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Run(firstCtx); err != nil {
		t.Fatal(err)
	}
	first.Close()
	if provider.starts != 1 || readManifest(t, fixture.ctx, fixture.pool, manifestID).State != acquisition.StateActive ||
		readJob(t, fixture.ctx, fixture.pool, jobID).State != jobs.StateRunning {
		t.Fatal("canceled first start did not preserve active durable pair")
	}
	var reservation string
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT state FROM acquisition_provider_tasks WHERE manifest_id=$1`, string(manifestID)).Scan(&reservation); err != nil {
		t.Fatal(err)
	}
	if reservation != string(acquisition.ProviderTaskStartReserved) {
		t.Fatalf("provider fence = %s", reservation)
	}
	if _, err := fixture.pool.Exec(fixture.ctx, `UPDATE jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE job_id=$1`, string(jobID)); err != nil {
		t.Fatal(err)
	}
	provider.cancel = nil
	secondCtx, secondStop := context.WithCancel(fixture.ctx)
	defer secondStop()
	deps.WorkerOptions = []app.WorkerOption{app.WithWorkerEventSink(func(event app.WorkerEvent) {
		if event.Kind == app.WorkerRecoveryOnly {
			secondStop()
		}
	})}
	second, err := runtime.NewRuntime(fixture.ctx, cfg, deps)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := second.Run(secondCtx); err != nil {
		t.Fatal(err)
	}
	if provider.starts != 1 || readManifest(t, fixture.ctx, fixture.pool, manifestID).State != acquisition.StateRecoveryRequired ||
		readJob(t, fixture.ctx, fixture.pool, jobID).State != jobs.StateRecoveryRequired {
		t.Fatalf("restart did not recover exact pair without another start: starts=%d", provider.starts)
	}
}

func TestPostgresAcquisitionRuntimeNoFalseReadyOnProtocolFailures(t *testing.T) {
	for _, scenario := range []string{"Hint rejected", "ambiguous Q5", "Q5 timeout"} {
		t.Run(scenario, func(t *testing.T) {
			fixture, manifestID, jobID := runtimeFixture(t)
			provider := &runtimeDownloader{}
			q5Calls, q8Calls, hintCalls := 0, 0, 0
			read := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/v1/roots/canonical-root/resolve":
					q5Calls++
					if scenario == "Q5 timeout" {
						<-r.Context().Done()
						return
					}
					fmt.Fprint(w, `{"matches":[],"ambiguous":true}`)
				case "/v1/roots/canonical-root/journal":
					q8Calls++
					fmt.Fprint(w, `{"items":[]}`)
				default:
					t.Errorf("unexpected read %s", r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer read.Close()
			hint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hintCalls++
				if scenario == "Hint rejected" {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusAccepted)
				fmt.Fprint(w, `{"status":"accepted","root_id":"canonical-root","scope_key":"/downloads/item","work_state":"PENDING","signal_seq":1}`)
			}))
			defer hint.Close()
			clock := &runtimeClock{now: time.Now().UTC()}
			ctx, stop := context.WithTimeout(fixture.ctx, 8*time.Second)
			defer stop()
			failureSeen := false
			deps := runtimeDeps(fixture, hint.URL, provider)
			deps.WorkerOptions = []app.WorkerOption{app.WithWorkerClock(clock.Now), app.WithWorkerWaiter(runtimeWaiter{clock: clock}),
				app.WithWorkerEventSink(func(event app.WorkerEvent) {
					if event.Kind == app.WorkerTransientError {
						failureSeen = true
						stop()
					}
				})}
			application, err := runtime.NewRuntime(fixture.ctx, runtimeConfig(read.URL), deps)
			if err != nil {
				t.Fatal(err)
			}
			defer application.Close()
			if err := application.Run(ctx); err != nil {
				t.Fatal(err)
			}
			manifest := readManifest(t, fixture.ctx, fixture.pool, manifestID)
			job := readJob(t, fixture.ctx, fixture.pool, jobID)
			if !failureSeen || manifest.State == acquisition.StateReady || job.State == jobs.StateSucceeded ||
				manifest.ResultCopyID != nil || provider.starts != 1 || hintCalls != 1 || q8Calls != 0 {
				t.Fatalf("unsafe failure state: manifest=%s job=%s copy=%v starts=%d Hint/Q5/Q8=%d/%d/%d failure=%t",
					manifest.State, job.State, manifest.ResultCopyID, provider.starts, hintCalls, q5Calls, q8Calls, failureSeen)
			}
			if scenario == "Hint rejected" && q5Calls != 0 {
				t.Fatal("Q5 ran after rejected Hint")
			}
			if scenario != "Hint rejected" && q5Calls != 1 {
				t.Fatal("Q5 failure was not observed")
			}
		})
	}
}
