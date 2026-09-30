package acquisitionruntime

import (
	"context"
	"fmt"
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

func TestPostgresOperatorSubmissionCLIEnqueuesOnly(t *testing.T) {
	if goruntime.GOOS != "linux" {
		t.Skip("protected request file is Linux-only")
	}
	fixture, oldManifest, oldJob := runtimeFixture(t)
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "request.json")
	manifestID := acquisition.ManifestID("54000000-0000-4000-8000-000000000082")
	jobID := jobs.JobID("54000000-0000-4000-8000-000000000083")
	source := `MAGNET:?xt=urn:btih:Exact&dn=A%20B&x=untouched`
	writeRequest := func(value string) {
		t.Helper()
		content := fmt.Sprintf(`{"manifest_id":%q,"job_id":%q,"max_attempts":3,"source_ref":%q,"target_storage_binding_id":%q,"target_path":"/downloads"}`,
			string(manifestID), string(jobID), value, string(fixture.bindingID))
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	writeRequest(source)
	binary := filepath.Join(t.TempDir(), "panta")
	build := exec.Command(filepath.Join(goruntime.GOROOT(), "bin", "go"), "build", "-o", binary, "../../../cmd/panta")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v: %s", err, output)
	}
	run := func() (string, error) {
		ctx, cancel := context.WithTimeout(fixture.ctx, 10*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, binary, "submit-acquisition")
		command.Env = append(os.Environ(),
			"PANTA_DATABASE_URL="+os.Getenv("PANTA_TEST_DATABASE_URL"),
			"PANTA_ACQUISITION_WORKER_ENABLED=false",
			"PANTA_ACQUISITION_SUBMISSION_FILE="+path)
		output, err := command.CombinedOutput()
		return string(output), err
	}
	first, err := run()
	if err != nil || !strings.Contains(first, "status=CREATED") || strings.Contains(first, source) {
		t.Fatalf("first submission: err=%v output=%q", err, first)
	}
	manifest := readManifest(t, fixture.ctx, fixture.pool, manifestID)
	job := readJob(t, fixture.ctx, fixture.pool, jobID)
	if manifest.SourceRef != source || manifest.SourceType != "magnet" || manifest.State != acquisition.StateActive ||
		job.State != jobs.StateQueued || job.ClaimAttempts != 0 ||
		readJob(t, fixture.ctx, fixture.pool, oldJob).ClaimAttempts != 0 ||
		readManifest(t, fixture.ctx, fixture.pool, oldManifest).State != acquisition.StateActive {
		t.Fatal("submission changed source, started worker or altered existing Job")
	}
	replay, err := run()
	if err != nil || !strings.Contains(replay, "status=REPLAY") || strings.Contains(replay, source) {
		t.Fatalf("CLI replay: err=%v output=%q", err, replay)
	}
	writeRequest(source + "&different=1")
	conflict, err := run()
	if err == nil || strings.Contains(conflict, source) || !strings.Contains(conflict, "conflicting acquisition submission") {
		t.Fatalf("CLI conflicting replay: err=%v output=%q", err, conflict)
	}
	unsafeSource := "https://user:private@example.invalid/file"
	writeRequest(unsafeSource)
	rejected, err := run()
	if err == nil || strings.Contains(rejected, unsafeSource) || strings.Contains(rejected, "private") ||
		!strings.Contains(rejected, "invalid acquisition submission") {
		t.Fatalf("CLI unsafe source rejection: err=%v output=%q", err, rejected)
	}
}
