package acquisitionruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
)

func TestPostgresOperatorStagingHarnessReadOnly(t *testing.T) {
	fixture, manifestID, jobID := runtimeFixture(t)
	readCalls := 0
	indexcore := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		readCalls++
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/roots/canonical-root/status":
			fmt.Fprint(w, `{"root_id":"canonical-root","lifecycle_state":"ACTIVE","current_generation":1}`)
		case "/v1/roots/canonical-root/journal":
			fmt.Fprint(w, `{"items":[]}`)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer indexcore.Close()
	binary := filepath.Join(t.TempDir(), "panta-staging")
	build := exec.Command(filepath.Join(goruntime.GOROOT(), "bin", "go"), "build", "-o", binary, "../../../cmd/panta-staging")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build staging tool: %v: %s", err, output)
	}
	baselineDirectory := t.TempDir()
	if err := os.Chmod(baselineDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	baselinePath := filepath.Join(baselineDirectory, "baseline.json")
	run := func(mode string) (string, error) {
		ctx, cancel := context.WithTimeout(fixture.ctx, 10*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, binary, mode)
		command.Env = append(os.Environ(),
			"PANTA_DATABASE_URL="+os.Getenv("PANTA_TEST_DATABASE_URL"),
			"PANTA_INDEXCORE_BASE_URL="+indexcore.URL,
			"PANTA_STAGING_ROOT_ID=canonical-root",
			"PANTA_STAGING_MANIFEST_ID="+string(manifestID),
			"PANTA_STAGING_JOB_ID="+string(jobID),
			"PANTA_STAGING_BASELINE_FILE="+baselinePath)
		output, err := command.CombinedOutput()
		return string(output), err
	}
	base, err := run("baseline")
	if err != nil || !strings.Contains(base, `"journal_last_seq":0`) || strings.Contains(base, "magnet:?xt") {
		t.Fatalf("baseline = %q, %v", base, err)
	}
	if err := os.WriteFile(baselinePath, []byte(base), 0600); err != nil {
		t.Fatal(err)
	}
	inspect, err := run("inspect")
	if err != nil || !strings.Contains(inspect, `"outcome":"READY_TO_START"`) ||
		strings.Contains(inspect, "magnet:?xt") || strings.Contains(inspect, runtimeTestSecret) {
		t.Fatalf("inspect = %q, %v", inspect, err)
	}
	var parsed map[string]any
	if json.Unmarshal([]byte(inspect), &parsed) != nil || parsed["claim_attempts"] != float64(0) {
		t.Fatalf("invalid redacted report: %q", inspect)
	}
	if readCalls < 3 || readJob(t, context.Background(), fixture.pool, jobs.JobID(jobID)).ClaimAttempts != 0 ||
		readManifest(t, context.Background(), fixture.pool, acquisition.ManifestID(manifestID)).State != acquisition.StateActive {
		t.Fatal("staging evidence tool changed durable acquisition state")
	}
}

func TestPostgresOperatorStagingHarnessTerminalEvidence(t *testing.T) {
	fixture, manifestID, jobID := runtimeFixture(t)
	const copyID = "54000000-0000-4000-8000-000000000082"
	for _, command := range []struct {
		query string
		args  []any
	}{
		{`INSERT INTO copies (copy_id,indexcore_root_id,indexcore_resource_id,storage_binding_id,availability,created_at,updated_at)
VALUES ($1,'canonical-root','staging-resource',$2,'PRESENT',clock_timestamp(),clock_timestamp())`, []any{copyID, string(fixture.bindingID)}},
		{`UPDATE acquisition_manifests SET state='READY',result_name='movie.mkv',result_copy_id=$2,updated_at=clock_timestamp() WHERE manifest_id=$1`, []any{string(manifestID), copyID}},
		{`UPDATE jobs SET state='SUCCEEDED',finished_at=clock_timestamp(),updated_at=clock_timestamp() WHERE job_id=$1`, []any{string(jobID)}},
		{`INSERT INTO acquisition_provider_tasks (manifest_id,job_id,provider_id,provider_task_ref,state,created_at,updated_at)
VALUES ($1,$2,'115','synthetic-task','REFERENCE_KNOWN',clock_timestamp(),clock_timestamp())`, []any{string(manifestID), string(jobID)}},
		{`INSERT INTO indexcore_projection_cursors (storage_binding_id,last_event_seq,updated_at)
VALUES ($1,2,clock_timestamp())`, []any{string(fixture.bindingID)}},
	} {
		if _, err := fixture.pool.Exec(fixture.ctx, command.query, command.args...); err != nil {
			t.Fatal(err)
		}
	}
	indexcore := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/roots/canonical-root/status":
			fmt.Fprint(w, `{"root_id":"canonical-root","lifecycle_state":"ACTIVE","current_generation":2}`)
		case "/v1/roots/canonical-root/journal":
			if r.URL.Query().Get("after_seq") != "1" {
				t.Errorf("unexpected journal cursor: %q", r.URL.RawQuery)
			}
			fmt.Fprint(w, `{"items":[{"event_seq":2,"generation_number":2,"intra_generation_seq":1,"event_type":"resource-added","resource_id":"staging-resource","payload":"{}","committed_at":"2026-09-28T01:02:03Z"}]}`)
		case "/v1/roots/canonical-root/resolve":
			if r.URL.Query().Get("path") != "/downloads/item/movie.mkv" {
				t.Errorf("unexpected resolve path: %q", r.URL.RawQuery)
			}
			fmt.Fprint(w, `{"matches":[{"resource_id":"staging-resource","root_id":"canonical-root","canonical_path":"/downloads/item/movie.mkv","parent_resource_id":null,"name":"movie.mkv","is_dir":false,"size":42,"mtime":"2026-09-28T01:02:03Z","resource_presence":"PRESENT","introduced_at_generation":2,"last_confirmed_generation":2}],"ambiguous":false}`)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer indexcore.Close()
	binary := filepath.Join(t.TempDir(), "panta-staging")
	build := exec.Command(filepath.Join(goruntime.GOROOT(), "bin", "go"), "build", "-o", binary, "../../../cmd/panta-staging")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build staging tool: %v: %s", err, output)
	}
	baselineDirectory := t.TempDir()
	if err := os.Chmod(baselineDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	baselinePath := filepath.Join(baselineDirectory, "baseline.json")
	if err := os.WriteFile(baselinePath, []byte(`{"kind":"baseline","root_id":"canonical-root","generation":1,"journal_last_seq":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(fixture.ctx, 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "watch")
	command.Env = append(os.Environ(),
		"PANTA_DATABASE_URL="+os.Getenv("PANTA_TEST_DATABASE_URL"),
		"PANTA_INDEXCORE_BASE_URL="+indexcore.URL,
		"PANTA_STAGING_ROOT_ID=canonical-root",
		"PANTA_STAGING_MANIFEST_ID="+string(manifestID),
		"PANTA_STAGING_JOB_ID="+string(jobID),
		"PANTA_STAGING_BASELINE_FILE="+baselinePath,
		"PANTA_STAGING_DEADLINE_SECONDS=60")
	output, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(output), `"outcome":"PASS"`) ||
		strings.Contains(string(output), "magnet:?xt") || strings.Contains(string(output), "synthetic-task") {
		t.Fatalf("terminal evidence = %q, %v", output, err)
	}
	if readJob(t, fixture.ctx, fixture.pool, jobID).State != jobs.StateSucceeded ||
		readManifest(t, fixture.ctx, fixture.pool, manifestID).State != acquisition.StateReady {
		t.Fatal("staging watch changed durable terminal state")
	}
}
