package observation_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nathanxiangang-web/panta/internal/integrations/indexcore"
	"github.com/nathanxiangang-web/panta/internal/projector"
	"github.com/nathanxiangang-web/panta/internal/storage"
	"github.com/nathanxiangang-web/panta/internal/store/postgres"
)

const (
	indexCorePin = "6f0eec85c59bd8cbe55011b0d9e512e0cafd6615"
	rootID       = "15000000-0000-4000-8000-000000000001"
	connectionID = storage.ConnectionID("15000000-0000-4000-8000-000000000002")
	bindingID    = storage.BindingID("15000000-0000-4000-8000-000000000003")
	fixtureToken = "gate1-controlled-observation-token"
)

// TestControlledObservation proves the complete Gate 1 observation chain while
// keeping the controlled OpenList surface and process orchestration test-only.
// It is opt-in because it needs two fresh PostgreSQL databases and the pinned
// external IndexCore binary.
func TestControlledObservation(t *testing.T) {
	if os.Getenv("PANTA_GATE1_E2E") != "1" {
		t.Skip("set PANTA_GATE1_E2E=1 to run the controlled observation E2E")
	}

	binary := requiredEnv(t, "PANTA_GATE1_INDEXCORE_BINARY")
	indexCoreDatabaseURL := requiredE2EDatabase(t, "PANTA_GATE1_INDEXCORE_DATABASE_URL", "indexcore_e2e")
	pantaDatabaseURL := requiredE2EDatabase(t, "PANTA_GATE1_PANTA_DATABASE_URL", "panta_e2e")
	if indexCoreDatabaseURL == pantaDatabaseURL {
		t.Fatal("IndexCore and Panta must use separate databases")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	fixture := newOpenListFixture(fixtureToken)
	fixture.Reset()
	fixtureServer := httptest.NewServer(fixture)
	t.Cleanup(fixtureServer.Close)
	assertLoopbackURL(t, fixtureServer.URL)

	runner := indexCoreRunner{
		binary:      binary,
		databaseURL: indexCoreDatabaseURL,
		token:       fixtureToken,
	}
	runner.run(t, ctx, "migrate")
	runner.run(t, ctx, "root", "create", "--root-id", rootID, "--lifecycle", "ACTIVE")
	runner.run(t, ctx, "root", "config", "set", "--root-id", rootID, "--grace", "1h", "--move-horizon", "1h")
	adapterConfig, err := json.Marshal(map[string]string{
		"base_url":  fixtureServer.URL,
		"path":      "/library",
		"token_env": "PANTA_GATE1_OPENLIST_TOKEN",
	})
	if err != nil {
		t.Fatalf("encode IndexCore adapter config: %v", err)
	}
	runner.run(t, ctx, "root", "adapter", "set", "--root-id", rootID,
		"--collector", "openlist", "--config", string(adapterConfig))

	pantaPool, err := postgres.Open(ctx, pantaDatabaseURL)
	if err != nil {
		t.Fatalf("open Panta database: %v", err)
	}
	defer pantaPool.Close()
	migrator, err := postgres.NewMigrator(pantaPool)
	if err != nil {
		t.Fatalf("construct Panta migrator: %v", err)
	}
	status, err := migrator.Apply(ctx)
	if err != nil {
		t.Fatalf("apply Panta migrations: %v", err)
	}
	if !status.Compatible || status.CurrentVersion != 9 || len(status.Applied) != 9 {
		t.Fatalf("Panta migration status = %#v, want compatible migrations 0001-0004", status)
	}

	storageRepository, err := postgres.NewStorageRepository(pantaPool)
	if err != nil {
		t.Fatalf("construct StorageRepository: %v", err)
	}
	projectionStore, err := postgres.NewProjectionStore(pantaPool)
	if err != nil {
		t.Fatalf("construct ProjectionStore: %v", err)
	}
	seedPantaMapping(t, ctx, storageRepository)

	// Phase A: the real collector observes the initial nested resource.
	runner.run(t, ctx, "scan", "--root", rootID)
	client, stopIndexCore := runner.startServer(t, ctx)
	report := assertHierarchyAndResolve(t, ctx, client, "/library/docs/report.txt")
	fixture.assertRequested(t, "/library", "/library/docs")
	initialEvents := readJournal(t, ctx, client, 0)
	assertResourceEvent(t, initialEvents, report.ResourceID)
	projectorService := newProjector(t, storageRepository, client, projectionStore)
	initialProjection, err := projectorService.ProjectOnce(ctx, bindingID, 100)
	if err != nil {
		t.Fatalf("ProjectOnce(initial) error = %v", err)
	}
	initialLastSeq := lastSequence(t, initialEvents)
	if initialProjection.PreviousCursor != 0 || initialProjection.CurrentCursor != initialLastSeq {
		t.Fatalf("initial projection = %#v, want cursor 0 -> %d", initialProjection, initialLastSeq)
	}
	initialCopy := readCopy(t, ctx, pantaPool, report.ResourceID)
	assertUnresolvedPresentCopy(t, initialCopy)
	initialCount := countCopies(t, ctx, pantaPool)
	if initialCount < 1 {
		t.Fatal("initial projection created no unresolved Copy")
	}
	stopIndexCore()

	// Phase B: an identical real observation is a canonical NOOP. Panta must not
	// invent events, advance its cursor, or replace the existing Copy identity.
	runner.run(t, ctx, "scan", "--root", rootID)
	client, stopIndexCore = runner.startServer(t, ctx)
	if events := readJournal(t, ctx, client, initialLastSeq); len(events) != 0 {
		t.Fatalf("identical scan produced %d canonical resource events: %#v", len(events), events)
	}
	projectorService = newProjector(t, storageRepository, client, projectionStore)
	repeatProjection, err := projectorService.ProjectOnce(ctx, bindingID, 100)
	if err != nil {
		t.Fatalf("ProjectOnce(repeat) error = %v", err)
	}
	if repeatProjection.EventsRead != 0 || repeatProjection.Mutations != 0 ||
		repeatProjection.PreviousCursor != initialLastSeq || repeatProjection.CurrentCursor != initialLastSeq {
		t.Fatalf("repeat projection = %#v, want stable no-op at cursor %d", repeatProjection, initialLastSeq)
	}
	if got := readCopy(t, ctx, pantaPool, report.ResourceID); got.copyID != initialCopy.copyID {
		t.Fatalf("identical scan replaced CopyID %q with %q", initialCopy.copyID, got.copyID)
	}
	if got := countCopies(t, ctx, pantaPool); got != initialCount {
		t.Fatalf("identical scan changed Copy count from %d to %d", initialCount, got)
	}
	stopIndexCore()

	// Phase C: one additive fixture change becomes one real canonical event and
	// exactly one additional unresolved Copy; the original Copy stays unchanged.
	fixture.Upsert("/library/docs", fixtureItem{
		Name: "new.txt", Size: 11, Modified: "2026-09-29T01:02:03Z",
		HashInfo: map[string]string{"sha1": "2222222222222222222222222222222222222222"},
	})
	runner.run(t, ctx, "scan", "--root", rootID)
	client, stopIndexCore = runner.startServer(t, ctx)
	defer stopIndexCore()
	newResource := assertHierarchyAndResolve(t, ctx, client, "/library/docs/new.txt")
	additiveEvents := readJournal(t, ctx, client, initialLastSeq)
	assertResourceEvent(t, additiveEvents, newResource.ResourceID)
	projectorService = newProjector(t, storageRepository, client, projectionStore)
	additiveProjection, err := projectorService.ProjectOnce(ctx, bindingID, 100)
	if err != nil {
		t.Fatalf("ProjectOnce(additive) error = %v", err)
	}
	additiveLastSeq := lastSequence(t, additiveEvents)
	if additiveProjection.PreviousCursor != initialLastSeq || additiveProjection.CurrentCursor != additiveLastSeq || additiveProjection.Mutations != 1 {
		t.Fatalf("additive projection = %#v, want one mutation and cursor %d -> %d", additiveProjection, initialLastSeq, additiveLastSeq)
	}
	if got := countCopies(t, ctx, pantaPool); got != initialCount+1 {
		t.Fatalf("additive scan Copy count = %d, want %d", got, initialCount+1)
	}
	if got := readCopy(t, ctx, pantaPool, report.ResourceID); got.copyID != initialCopy.copyID {
		t.Fatalf("additive scan changed original CopyID from %q to %q", initialCopy.copyID, got.copyID)
	}
	assertUnresolvedPresentCopy(t, readCopy(t, ctx, pantaPool, newResource.ResourceID))
	cursor, err := projectionStore.Cursor(ctx, bindingID)
	if err != nil || cursor != additiveLastSeq {
		t.Fatalf("final projection cursor = %d, %v; want %d", cursor, err, additiveLastSeq)
	}

	t.Logf("controlled observation complete using pinned IndexCore %s", indexCorePin)
}

type fixtureItem struct {
	Name     string            `json:"name"`
	Size     int64             `json:"size"`
	IsDir    bool              `json:"is_dir"`
	Modified string            `json:"modified"`
	Type     int               `json:"type"`
	HashInfo map[string]string `json:"hash_info,omitempty"`
}

type openListFixture struct {
	mu          sync.RWMutex
	token       string
	directories map[string][]fixtureItem
	requests    map[string]int
}

func newOpenListFixture(token string) *openListFixture {
	return &openListFixture{token: token, directories: make(map[string][]fixtureItem), requests: make(map[string]int)}
}

func (fixture *openListFixture) Reset() {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	fixture.directories = map[string][]fixtureItem{
		"/library": {{Name: "docs", IsDir: true, Modified: "2026-09-29T01:00:00Z"}},
		"/library/docs": {{
			Name: "report.txt", Size: 17, Modified: "2026-09-29T01:01:00Z",
			HashInfo: map[string]string{"sha1": "1111111111111111111111111111111111111111"},
		}},
	}
	fixture.requests = make(map[string]int)
}

func (fixture *openListFixture) Upsert(directory string, item fixtureItem) {
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	items := fixture.directories[directory]
	replaced := false
	for index := range items {
		if items[index].Name == item.Name {
			items[index] = item
			replaced = true
			break
		}
	}
	if !replaced {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Name < items[j].Name })
	fixture.directories[directory] = items
}

func (fixture *openListFixture) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	response.Header().Set("Content-Type", "application/json")
	if request.Method != http.MethodPost || request.URL.Path != "/api/fs/list" {
		response.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(response).Encode(map[string]any{"code": 404, "message": "not found", "data": nil})
		return
	}
	if request.Header.Get("Authorization") != fixture.token {
		response.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(response).Encode(map[string]any{"code": 401, "message": "unauthorized", "data": nil})
		return
	}
	var input struct {
		Path    string `json:"path"`
		Page    int    `json:"page"`
		PerPage int    `json:"per_page"`
	}
	decoder := json.NewDecoder(io.LimitReader(request.Body, 64<<10))
	if err := decoder.Decode(&input); err != nil || input.Page < 1 || input.PerPage < 1 {
		response.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(response).Encode(map[string]any{"code": 400, "message": "invalid request", "data": nil})
		return
	}

	fixture.mu.Lock()
	fixture.requests[input.Path]++
	all := append([]fixtureItem(nil), fixture.directories[input.Path]...)
	fixture.mu.Unlock()
	total := len(all)
	start := (input.Page - 1) * input.PerPage
	if start > total {
		start = total
	}
	end := start + input.PerPage
	if end > total {
		end = total
	}
	_ = json.NewEncoder(response).Encode(map[string]any{
		"code": 200, "message": "success",
		"data": map[string]any{"content": all[start:end], "total": total},
	})
}

func (fixture *openListFixture) assertRequested(t *testing.T, paths ...string) {
	t.Helper()
	fixture.mu.RLock()
	defer fixture.mu.RUnlock()
	for _, path := range paths {
		if fixture.requests[path] == 0 {
			t.Fatalf("controlled fixture never received a list request for %s", path)
		}
	}
}

type indexCoreRunner struct {
	binary      string
	databaseURL string
	token       string
}

func (runner indexCoreRunner) environment(httpAddress string) []string {
	environment := append([]string(nil), os.Environ()...)
	environment = append(environment,
		"INDEXCORE_DATABASE_URL="+runner.databaseURL,
		"PANTA_GATE1_OPENLIST_TOKEN="+runner.token,
		"INDEXCORE_LOG_LEVEL=error",
		"INDEXCORE_SHUTDOWN_TIMEOUT=3s",
	)
	if httpAddress != "" {
		environment = append(environment, "INDEXCORE_HTTP_ADDR="+httpAddress)
	}
	return environment
}

func (runner indexCoreRunner) run(t *testing.T, ctx context.Context, arguments ...string) {
	t.Helper()
	command := exec.CommandContext(ctx, runner.binary, arguments...)
	command.Env = runner.environment("")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("IndexCore %s failed: %v\n%s", strings.Join(arguments, " "), err, strings.TrimSpace(string(output)))
	}
}

func (runner indexCoreRunner) startServer(t *testing.T, parent context.Context) (*indexcore.Client, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve IndexCore loopback address: %v", err)
	}
	address := listener.Addr().String()
	_ = listener.Close()

	ctx, cancel := context.WithCancel(parent)
	var output bytes.Buffer
	command := exec.CommandContext(ctx, runner.binary, "serve")
	command.Env = runner.environment(address)
	command.Stdout = &output
	command.Stderr = &output
	if err := command.Start(); err != nil {
		cancel()
		t.Fatalf("start IndexCore server: %v", err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = command.Wait()
		close(done)
	}()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			if command.Process != nil {
				_ = command.Process.Signal(os.Interrupt)
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				if command.Process != nil {
					_ = command.Process.Kill()
				}
				<-done
			}
			cancel()
		})
	}
	t.Cleanup(stop)

	baseURL := "http://" + address
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(15 * time.Second)
	for {
		select {
		case <-done:
			cancel()
			t.Fatalf("IndexCore server exited before readiness: %v\n%s", waitErr, strings.TrimSpace(output.String()))
		default:
		}
		request, requestErr := http.NewRequestWithContext(parent, http.MethodGet, baseURL+"/readyz", nil)
		if requestErr == nil {
			response, responseErr := client.Do(request)
			if responseErr == nil {
				_ = response.Body.Close()
				if response.StatusCode == http.StatusOK {
					break
				}
			}
		}
		if time.Now().After(deadline) {
			stop()
			t.Fatalf("IndexCore readiness timed out\n%s", strings.TrimSpace(output.String()))
		}
		time.Sleep(50 * time.Millisecond)
	}
	pantaClient, err := indexcore.NewClient(baseURL, indexcore.WithTimeout(5*time.Second))
	if err != nil {
		stop()
		t.Fatalf("construct Panta IndexCore client: %v", err)
	}
	return pantaClient, stop
}

func seedPantaMapping(t *testing.T, ctx context.Context, repository *postgres.StorageRepository) {
	t.Helper()
	now := time.Date(2026, 9, 29, 1, 0, 0, 0, time.UTC)
	connection := storage.Connection{
		ID: connectionID, ProviderType: "controlled-openlist", Status: storage.ConnectionStatusActive,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateConnection(ctx, connection); err != nil {
		t.Fatalf("create Panta StorageConnection: %v", err)
	}
	binding := storage.Binding{
		ID: bindingID, ConnectionID: connectionID, OpenListMountPath: "/library",
		IndexCoreRootID: rootID, Status: storage.BindingStatusActive,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateBinding(ctx, binding); err != nil {
		t.Fatalf("create Panta StorageBinding: %v", err)
	}
}

func newProjector(t *testing.T, bindings *postgres.StorageRepository, journal indexcore.JournalReadPort, store *postgres.ProjectionStore) *projector.Service {
	t.Helper()
	service, err := projector.NewService(bindings, journal, store)
	if err != nil {
		t.Fatalf("construct projector service: %v", err)
	}
	return service
}

func assertHierarchyAndResolve(t *testing.T, ctx context.Context, client *indexcore.Client, canonicalPath string) indexcore.ResourceContext {
	t.Helper()
	rootPage, err := client.Browse(ctx, indexcore.BrowseRequest{RootID: rootID, Limit: 100})
	if err != nil {
		t.Fatalf("Q4 root browse: %v", err)
	}
	docs := findByPath(t, rootPage.Items, "/library/docs")
	if docs.Directory == nil || !*docs.Directory {
		t.Fatalf("Q4 docs resource is not a directory: %#v", docs)
	}
	docsPage, err := client.Browse(ctx, indexcore.BrowseRequest{
		RootID: rootID, ParentResourceID: &docs.ResourceID, Limit: 100,
	})
	if err != nil {
		t.Fatalf("Q4 docs browse: %v", err)
	}
	resource := findByPath(t, docsPage.Items, canonicalPath)
	resolved, err := client.Resolve(ctx, indexcore.ResolveRequest{RootID: rootID, Path: canonicalPath})
	if err != nil {
		t.Fatalf("Q5 resolve %s: %v", canonicalPath, err)
	}
	if resolved.Ambiguous || len(resolved.Matches) != 1 || resolved.Matches[0].ResourceID != resource.ResourceID {
		t.Fatalf("Q5 resolve %s = %#v, want one Q4-identical match", canonicalPath, resolved)
	}
	if resource.Presence != indexcore.ResourcePresent {
		t.Fatalf("resource %s presence = %s, want PRESENT", canonicalPath, resource.Presence)
	}
	return resource
}

func findByPath(t *testing.T, resources []indexcore.ResourceContext, path string) indexcore.ResourceContext {
	t.Helper()
	for _, resource := range resources {
		if resource.CanonicalPath != nil && *resource.CanonicalPath == path {
			return resource
		}
	}
	t.Fatalf("resource %s not found in %#v", path, resources)
	return indexcore.ResourceContext{}
}

func readJournal(t *testing.T, ctx context.Context, client *indexcore.Client, after int64) []indexcore.JournalEvent {
	t.Helper()
	events, err := client.ReadJournal(ctx, indexcore.JournalRequest{RootID: rootID, AfterSeq: after, Limit: 100})
	if err != nil {
		t.Fatalf("Q8 read after %d: %v", after, err)
	}
	return events
}

func assertResourceEvent(t *testing.T, events []indexcore.JournalEvent, resourceID string) {
	t.Helper()
	for _, event := range events {
		if event.ResourceID != nil && *event.ResourceID == resourceID && event.EventType == indexcore.EventResourceAdded {
			return
		}
	}
	t.Fatalf("Q8 contains no resource-added event for %s: %#v", resourceID, events)
}

func lastSequence(t *testing.T, events []indexcore.JournalEvent) int64 {
	t.Helper()
	if len(events) == 0 {
		t.Fatal("expected at least one real IndexCore Journal event")
	}
	return events[len(events)-1].EventSeq
}

type projectedCopy struct {
	copyID       string
	variantID    *string
	rootID       string
	resourceID   string
	bindingID    string
	availability string
}

func readCopy(t *testing.T, ctx context.Context, pool *pgxpool.Pool, resourceID string) projectedCopy {
	t.Helper()
	var result projectedCopy
	var variantID sql.NullString
	err := pool.QueryRow(ctx, `
SELECT copy_id::text, variant_id::text, indexcore_root_id, indexcore_resource_id,
       storage_binding_id::text, availability
FROM copies
WHERE indexcore_root_id = $1 AND indexcore_resource_id = $2`, rootID, resourceID).Scan(
		&result.copyID, &variantID, &result.rootID, &result.resourceID,
		&result.bindingID, &result.availability,
	)
	if err != nil {
		t.Fatalf("read projected Copy for resource %s: %v", resourceID, err)
	}
	if variantID.Valid {
		result.variantID = &variantID.String
	}
	return result
}

func assertUnresolvedPresentCopy(t *testing.T, copy projectedCopy) {
	t.Helper()
	if copy.variantID != nil || copy.rootID != rootID || copy.bindingID != string(bindingID) || copy.availability != "PRESENT" {
		t.Fatalf("projected Copy = %#v, want unresolved PRESENT Copy for controlled binding", copy)
	}
}

func countCopies(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM copies").Scan(&count); err != nil {
		t.Fatalf("count projected Copies: %v", err)
	}
	return count
}

func requiredEnv(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("%s is required when PANTA_GATE1_E2E=1", name)
	}
	return value
}

func requiredE2EDatabase(t *testing.T, name, expectedDatabase string) string {
	t.Helper()
	raw := requiredEnv(t, name)
	config, err := pgxpool.ParseConfig(raw)
	if err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	if config.ConnConfig.Database != expectedDatabase {
		t.Fatalf("refusing E2E database %q from %s; want %q", config.ConnConfig.Database, name, expectedDatabase)
	}
	return raw
}

func assertLoopbackURL(t *testing.T, raw string) {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse fixture URL: %v", err)
	}
	host, _, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("parse fixture host: %v", err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		t.Fatalf("fixture must bind to loopback, got %q", parsed.Host)
	}
}
