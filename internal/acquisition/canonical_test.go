package acquisition

// Gate 3.10 (D-033) domain unit tests for canonical confirmation.
//
// This is a pure in-package unit test file: every external boundary is a counting
// fake, so each test proves both the outcome and that no unexpected call happened.
// In particular the fences must reject before the resolver, the projector must be
// invoked at most once, the classifier must only be reached with an explicit
// Manifest Variant, and a READY replay must reach the store without touching any
// external port.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

const (
	canonicalTestManifestID = ManifestID("manifest-1")
	canonicalTestJobID      = jobs.JobID("job-1")
	canonicalTestOwner      = "owner-1"
	canonicalTestBindingID  = storage.BindingID("binding-1")
	canonicalTestRootID     = "root-1"
	canonicalTestResourceID = "resource-1"
	canonicalTestCopyID     = catalog.CopyID("copy-1")
)

// --- counting fakes -----------------------------------------------------------

type canonicalManifestReaderFake struct {
	manifest Manifest
	err      error
	calls    int
}

func (reader *canonicalManifestReaderFake) GetManifest(_ context.Context, _ ManifestID) (Manifest, error) {
	reader.calls++
	if reader.err != nil {
		return Manifest{}, reader.err
	}
	return reader.manifest, nil
}

type canonicalJobReaderFake struct {
	job   jobs.Job
	err   error
	calls int
}

func (reader *canonicalJobReaderFake) Get(_ context.Context, _ jobs.JobID) (jobs.Job, error) {
	reader.calls++
	if reader.err != nil {
		return jobs.Job{}, reader.err
	}
	return reader.job, nil
}

type canonicalBindingReaderFake struct {
	binding storage.Binding
	err     error
	calls   int
}

func (reader *canonicalBindingReaderFake) GetBinding(_ context.Context, _ storage.BindingID) (storage.Binding, error) {
	reader.calls++
	if reader.err != nil {
		return storage.Binding{}, reader.err
	}
	return reader.binding, nil
}

type canonicalResolverFake struct {
	resolution CanonicalResolution
	err        error
	calls      int
	lastRoot   string
	lastPath   string
}

func (port *canonicalResolverFake) ResolveCanonical(_ context.Context, request CanonicalResolveRequest) (CanonicalResolution, error) {
	port.calls++
	port.lastRoot = request.RootID
	port.lastPath = request.Path
	if port.err != nil {
		return CanonicalResolution{}, port.err
	}
	return port.resolution, nil
}

type canonicalProjectorFake struct {
	projection  CanonicalProjection
	err         error
	calls       int
	lastBinding storage.BindingID
	lastLimit   int
}

func (port *canonicalProjectorFake) ProjectOnce(_ context.Context, binding storage.BindingID, limit int) (CanonicalProjection, error) {
	port.calls++
	port.lastBinding = binding
	port.lastLimit = limit
	if port.err != nil {
		return CanonicalProjection{}, port.err
	}
	return port.projection, nil
}

type canonicalCopyReaderFake struct {
	copyRecord   catalog.Copy
	err          error
	calls        int
	lastRoot     string
	lastResource string
}

func (reader *canonicalCopyReaderFake) GetCopyByPhysicalIdentity(_ context.Context, rootID, resourceID string) (catalog.Copy, error) {
	reader.calls++
	reader.lastRoot = rootID
	reader.lastResource = resourceID
	if reader.err != nil {
		return catalog.Copy{}, reader.err
	}
	return reader.copyRecord, nil
}

type canonicalClassifierFake struct {
	bound         catalog.Copy
	err           error
	calls         int
	lastCopyID    catalog.CopyID
	lastVariantID catalog.VariantID
}

func (port *canonicalClassifierFake) Bind(_ context.Context, id catalog.CopyID, variant catalog.VariantID) (catalog.Copy, error) {
	port.calls++
	port.lastCopyID = id
	port.lastVariantID = variant
	if port.err != nil {
		return catalog.Copy{}, port.err
	}
	return port.bound, nil
}

type canonicalStoreFake struct {
	result CanonicalResult
	err    error
	calls  int
	plan   CanonicalPlan
}

func (store *canonicalStoreFake) CommitCanonical(_ context.Context, plan CanonicalPlan) (CanonicalResult, error) {
	store.calls++
	store.plan = plan
	if store.err != nil {
		return CanonicalResult{}, store.err
	}
	return store.result, nil
}

// --- fixture ------------------------------------------------------------------

type canonicalFixture struct {
	confirmation *CanonicalConfirmation
	manifests    *canonicalManifestReaderFake
	jobs         *canonicalJobReaderFake
	bindings     *canonicalBindingReaderFake
	resolver     *canonicalResolverFake
	projector    *canonicalProjectorFake
	copies       *canonicalCopyReaderFake
	classifier   *canonicalClassifierFake
	store        *canonicalStoreFake
	request      CanonicalRequest
	resource     CanonicalResource
	copyRecord   catalog.Copy
}

func canonicalStringPointer(value string) *string { return &value }

func canonicalVariantPointer(value catalog.VariantID) *catalog.VariantID { return &value }

func canonicalCopyIDPointer(value catalog.CopyID) *catalog.CopyID { return &value }

// canonicalTestJob builds a Job that satisfies the frozen Gate 3.2 linkage
// contract, so a test only fails the fence it is actually exercising.
func canonicalTestJob(t *testing.T, manifestID ManifestID, jobID jobs.JobID) jobs.Job {
	t.Helper()
	payload, err := json.Marshal(acquisitionJobPayload{
		SchemaVersion: AcquisitionPayloadSchemaVersion,
		ManifestID:    manifestID,
	})
	if err != nil {
		t.Fatalf("marshal acquisition Job payload: %v", err)
	}
	key := AcquisitionJobIdempotencyKey(manifestID)
	owner := canonicalTestOwner
	return jobs.Job{
		ID:             jobID,
		Type:           JobTypeAcquisition,
		Payload:        payload,
		State:          jobs.StateRunning,
		IdempotencyKey: &key,
		ClaimAttempts:  3,
		LeaseOwner:     &owner,
	}
}

func newCanonicalFixture(t *testing.T) *canonicalFixture {
	t.Helper()
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	jobID := canonicalTestJobID
	manifest := Manifest{
		ID:                     canonicalTestManifestID,
		SourceType:             "OPENLIST",
		SourceRef:              "https://example.invalid/item",
		ResultName:             canonicalStringPointer("item.mkv"),
		TargetStorageBindingID: canonicalTestBindingID,
		TargetPath:             "/downloads",
		JobID:                  &jobID,
		State:                  StateAwaitingCanonical,
		CreatedAt:              now,
		UpdatedAt:              now,
	}
	job := canonicalTestJob(t, canonicalTestManifestID, canonicalTestJobID)
	binding := storage.Binding{
		ID:              canonicalTestBindingID,
		IndexCoreRootID: canonicalTestRootID,
		Status:          storage.BindingStatusActive,
	}
	resource := CanonicalResource{
		RootID:        canonicalTestRootID,
		ResourceID:    canonicalTestResourceID,
		CanonicalPath: "/downloads/item.mkv",
		Presence:      CanonicalPresencePresent,
	}
	copyRecord := catalog.Copy{
		ID:                  canonicalTestCopyID,
		IndexCoreRootID:     canonicalTestRootID,
		IndexCoreResourceID: canonicalTestResourceID,
		StorageBindingID:    catalog.StorageBindingID(canonicalTestBindingID),
		Availability:        catalog.CopyAvailabilityPresent,
	}

	fixture := &canonicalFixture{
		manifests:  &canonicalManifestReaderFake{manifest: manifest},
		jobs:       &canonicalJobReaderFake{job: job},
		bindings:   &canonicalBindingReaderFake{binding: binding},
		resolver:   &canonicalResolverFake{resolution: CanonicalResolution{Matches: []CanonicalResource{resource}}},
		projector:  &canonicalProjectorFake{projection: CanonicalProjection{EventsRead: 1, Mutations: 1}},
		copies:     &canonicalCopyReaderFake{copyRecord: copyRecord},
		classifier: &canonicalClassifierFake{},
		store:      &canonicalStoreFake{result: CanonicalResult{Manifest: manifest, Job: job, Changed: true}},
		request: CanonicalRequest{
			ManifestID:         canonicalTestManifestID,
			JobID:              canonicalTestJobID,
			Owner:              canonicalTestOwner,
			ExpectedClaim:      3,
			ProjectorPageLimit: 50,
			Now:                now,
			RetryAt:            now.Add(time.Minute),
		},
		resource:   resource,
		copyRecord: copyRecord,
	}
	confirmation, err := NewCanonicalConfirmation(
		fixture.manifests, fixture.jobs, fixture.bindings, fixture.resolver,
		fixture.projector, fixture.copies, fixture.classifier, fixture.store,
	)
	if err != nil {
		t.Fatalf("NewCanonicalConfirmation() error = %v", err)
	}
	fixture.confirmation = confirmation
	return fixture
}

func (fixture *canonicalFixture) assertNoDownstreamCalls(t *testing.T, label string) {
	t.Helper()
	if fixture.resolver.calls != 0 || fixture.projector.calls != 0 || fixture.copies.calls != 0 ||
		fixture.classifier.calls != 0 || fixture.store.calls != 0 {
		t.Fatalf("%s: resolver=%d projector=%d copies=%d classifier=%d store=%d, want all 0",
			label, fixture.resolver.calls, fixture.projector.calls, fixture.copies.calls,
			fixture.classifier.calls, fixture.store.calls)
	}
}

func (fixture *canonicalFixture) assertNoProjectionCopyClassifier(t *testing.T, label string) {
	t.Helper()
	if fixture.projector.calls != 0 || fixture.copies.calls != 0 || fixture.classifier.calls != 0 || fixture.store.calls != 0 {
		t.Fatalf("%s: projector=%d copies=%d classifier=%d store=%d, want all 0",
			label, fixture.projector.calls, fixture.copies.calls, fixture.classifier.calls, fixture.store.calls)
	}
}

// --- 1: candidate path --------------------------------------------------------

func TestCanonicalCandidatePathTruthTable(t *testing.T) {
	accepted := []struct {
		label  string
		target string
		name   string
		want   string
	}{
		{label: "root target with plain name", target: "/", name: "item.mkv", want: "/item.mkv"},
		{label: "one directory", target: "/downloads", name: "item.mkv", want: "/downloads/item.mkv"},
		{label: "unicode name", target: "/downloads/movies", name: "影片.mkv", want: "/downloads/movies/影片.mkv"},
		{label: "interior spaces and mixed case", target: "/downloads", name: "My  File.BIN", want: "/downloads/My  File.BIN"},
		{label: "leading and trailing spaces preserved", target: "/downloads", name: " spaced.bin ", want: "/downloads/ spaced.bin "},
		{label: "deep target", target: "/a/b/c", name: "d", want: "/a/b/c/d"},
	}
	for _, test := range accepted {
		t.Run(test.label, func(t *testing.T) {
			name := test.name
			got, err := CanonicalCandidatePath(test.target, &name)
			if err != nil {
				t.Fatalf("CanonicalCandidatePath(%q, %q) error = %v, want nil", test.target, test.name, err)
			}
			if got != test.want {
				t.Fatalf("CanonicalCandidatePath(%q, %q) = %q, want %q", test.target, test.name, got, test.want)
			}
			if len(got) != len(test.want) {
				t.Fatalf("CanonicalCandidatePath(%q, %q) len = %d, want %d", test.target, test.name, len(got), len(test.want))
			}
			if !strings.HasSuffix(got, test.name) {
				t.Fatalf("CanonicalCandidatePath(%q, %q) = %q lost the exact result-name bytes", test.target, test.name, got)
			}
			if !strings.HasPrefix(got, "/") {
				t.Fatalf("CanonicalCandidatePath(%q, %q) = %q is not root absolute", test.target, test.name, got)
			}
		})
	}

	rejected := []struct {
		label  string
		target string
		name   *string
	}{
		{label: "nil result name", target: "/downloads", name: nil},
		{label: "empty result name", target: "/downloads", name: canonicalStringPointer("")},
		{label: "blank result name", target: "/downloads", name: canonicalStringPointer("   ")},
		{label: "result name with slash", target: "/downloads", name: canonicalStringPointer("a/b")},
		{label: "result name with backslash", target: "/downloads", name: canonicalStringPointer(`a\b`)},
		{label: "dot result name", target: "/downloads", name: canonicalStringPointer(".")},
		{label: "dot dot result name", target: "/downloads", name: canonicalStringPointer("..")},
		{label: "result name with nul", target: "/downloads", name: canonicalStringPointer("a\x00b")},
		{label: "relative target", target: "downloads", name: canonicalStringPointer("item.mkv")},
		{label: "relative target with directory", target: "downloads/sub", name: canonicalStringPointer("item.mkv")},
		{label: "dot dot target component", target: "/downloads/..", name: canonicalStringPointer("item.mkv")},
		{label: "leading dot dot target component", target: "/../downloads", name: canonicalStringPointer("item.mkv")},
		{label: "dot target component", target: "/downloads/./movies", name: canonicalStringPointer("item.mkv")},
		{label: "trailing slash target", target: "/downloads/", name: canonicalStringPointer("item.mkv")},
		{label: "repeated slash target", target: "/downloads//movies", name: canonicalStringPointer("item.mkv")},
		{label: "backslash target", target: `/downloads\movies`, name: canonicalStringPointer("item.mkv")},
		{label: "empty target", target: "", name: canonicalStringPointer("item.mkv")},
	}
	for _, test := range rejected {
		t.Run(test.label, func(t *testing.T) {
			got, err := CanonicalCandidatePath(test.target, test.name)
			if !errors.Is(err, ErrCanonicalResultName) {
				t.Fatalf("CanonicalCandidatePath(%q, %v) error = %v, want ErrCanonicalResultName", test.target, test.name, err)
			}
			if got != "" {
				t.Fatalf("CanonicalCandidatePath(%q, %v) = %q on error, want empty", test.target, test.name, got)
			}
		})
	}
}

// --- 2: ambiguity -------------------------------------------------------------

func TestCanonicalAmbiguityNeverGuessed(t *testing.T) {
	fixture := newCanonicalFixture(t)
	second := fixture.resource
	second.ResourceID = "resource-2"

	tests := []struct {
		label      string
		resolution CanonicalResolution
	}{
		{label: "one match flagged ambiguous", resolution: CanonicalResolution{Matches: []CanonicalResource{fixture.resource}, Ambiguous: true}},
		{label: "two matches unflagged", resolution: CanonicalResolution{Matches: []CanonicalResource{fixture.resource, second}}},
		{label: "two matches flagged ambiguous", resolution: CanonicalResolution{Matches: []CanonicalResource{fixture.resource, second}, Ambiguous: true}},
		{label: "ambiguous with zero matches", resolution: CanonicalResolution{Ambiguous: true}},
	}
	for _, test := range tests {
		t.Run(test.label, func(t *testing.T) {
			local := newCanonicalFixture(t)
			local.resolver.resolution = test.resolution
			if _, err := local.confirmation.Confirm(context.Background(), local.request); !errors.Is(err, ErrCanonicalAmbiguous) {
				t.Fatalf("Confirm() error = %v, want ErrCanonicalAmbiguous", err)
			}
			if local.resolver.calls != 1 {
				t.Fatalf("resolver calls = %d, want 1", local.resolver.calls)
			}
			if local.store.calls != 0 {
				t.Fatalf("store calls = %d, want 0 when resolution is ambiguous", local.store.calls)
			}
			if local.projector.calls != 0 || local.copies.calls != 0 || local.classifier.calls != 0 {
				t.Fatalf("projector=%d copies=%d classifier=%d, want all 0", local.projector.calls, local.copies.calls, local.classifier.calls)
			}
		})
	}
}

// --- 3: zero matches is pending ----------------------------------------------

func TestCanonicalZeroMatchesIsPendingReschedule(t *testing.T) {
	fixture := newCanonicalFixture(t)
	fixture.resolver.resolution = CanonicalResolution{}

	result, err := fixture.confirmation.Confirm(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("Confirm() error = %v, want nil", err)
	}
	if fixture.store.calls != 1 {
		t.Fatalf("store calls = %d, want 1", fixture.store.calls)
	}
	plan := fixture.store.plan
	if plan.Ready {
		t.Fatalf("plan Ready = true, want false for zero matches")
	}
	if plan.ManifestState != StateAwaitingCanonical {
		t.Fatalf("plan ManifestState = %q, want %q", plan.ManifestState, StateAwaitingCanonical)
	}
	if plan.JobState != jobs.StateRetryWait {
		t.Fatalf("plan JobState = %q, want %q", plan.JobState, jobs.StateRetryWait)
	}
	if plan.RetryAt == nil || !plan.RetryAt.Equal(fixture.request.RetryAt.UTC()) {
		t.Fatalf("plan RetryAt = %v, want %v", plan.RetryAt, fixture.request.RetryAt.UTC())
	}
	if !plan.Now.Equal(fixture.request.Now.UTC()) {
		t.Fatalf("plan Now = %v, want %v", plan.Now, fixture.request.Now.UTC())
	}
	if plan.ManifestID != fixture.request.ManifestID || plan.JobID != fixture.request.JobID ||
		plan.Owner != fixture.request.Owner || plan.ExpectedClaim != fixture.request.ExpectedClaim {
		t.Fatalf("plan identity = %#v, want the request identity", plan)
	}
	if plan.ExpectedCopyBindingID != "" {
		t.Fatalf("plan ExpectedCopyBindingID = %q, want empty for a pending plan", plan.ExpectedCopyBindingID)
	}
	if plan.ResultCopyID != "" {
		t.Fatalf("plan ResultCopyID = %q, want empty for a pending plan", plan.ResultCopyID)
	}
	if fixture.projector.calls != 0 || fixture.copies.calls != 0 || fixture.classifier.calls != 0 {
		t.Fatalf("projector=%d copies=%d classifier=%d, want all 0 for zero matches",
			fixture.projector.calls, fixture.copies.calls, fixture.classifier.calls)
	}
	if !result.Changed {
		t.Fatalf("result Changed = false, want the store result returned")
	}
}

// --- 4: identity echo ---------------------------------------------------------

func TestCanonicalIdentityEchoFailsClosed(t *testing.T) {
	tests := []struct {
		label  string
		mutate func(*CanonicalResource)
	}{
		{label: "wrong root id", mutate: func(resource *CanonicalResource) { resource.RootID = "root-other" }},
		{label: "wrong canonical path", mutate: func(resource *CanonicalResource) { resource.CanonicalPath = "/downloads/other.bin" }},
		{label: "empty canonical path", mutate: func(resource *CanonicalResource) { resource.CanonicalPath = "" }},
		{label: "non-PRESENT presence", mutate: func(resource *CanonicalResource) { resource.Presence = CanonicalPresenceRemoved }},
		{label: "blank resource id", mutate: func(resource *CanonicalResource) { resource.ResourceID = "   " }},
	}
	for _, test := range tests {
		t.Run(test.label, func(t *testing.T) {
			fixture := newCanonicalFixture(t)
			match := fixture.resource
			test.mutate(&match)
			fixture.resolver.resolution = CanonicalResolution{Matches: []CanonicalResource{match}}

			if _, err := fixture.confirmation.Confirm(context.Background(), fixture.request); !errors.Is(err, ErrCanonicalIdentity) {
				t.Fatalf("Confirm() error = %v, want ErrCanonicalIdentity", err)
			}
			if fixture.resolver.calls != 1 {
				t.Fatalf("resolver calls = %d, want 1", fixture.resolver.calls)
			}
			fixture.assertNoProjectionCopyClassifier(t, "identity echo")
		})
	}
}

// --- 5: exactly one bounded projector call ------------------------------------

func TestCanonicalProjectorIsSingleBoundedCall(t *testing.T) {
	t.Run("requested limit is forwarded exactly once", func(t *testing.T) {
		fixture := newCanonicalFixture(t)
		if _, err := fixture.confirmation.Confirm(context.Background(), fixture.request); err != nil {
			t.Fatalf("Confirm() error = %v, want nil", err)
		}
		if fixture.projector.calls != 1 {
			t.Fatalf("projector calls = %d, want exactly 1", fixture.projector.calls)
		}
		if fixture.projector.lastBinding != canonicalTestBindingID {
			t.Fatalf("projector binding = %q, want %q", fixture.projector.lastBinding, canonicalTestBindingID)
		}
		if fixture.projector.lastLimit != fixture.request.ProjectorPageLimit {
			t.Fatalf("projector limit = %d, want %d", fixture.projector.lastLimit, fixture.request.ProjectorPageLimit)
		}
		if fixture.resolver.calls != 1 || fixture.resolver.lastRoot != canonicalTestRootID ||
			fixture.resolver.lastPath != "/downloads/item.mkv" {
			t.Fatalf("resolver root=%q path=%q calls=%d, want %q %q 1",
				fixture.resolver.lastRoot, fixture.resolver.lastPath, fixture.resolver.calls,
				canonicalTestRootID, "/downloads/item.mkv")
		}
		if fixture.copies.calls != 1 || fixture.copies.lastRoot != canonicalTestRootID ||
			fixture.copies.lastResource != canonicalTestResourceID {
			t.Fatalf("copy reader root=%q resource=%q calls=%d, want %q %q 1",
				fixture.copies.lastRoot, fixture.copies.lastResource, fixture.copies.calls,
				canonicalTestRootID, canonicalTestResourceID)
		}
		if !fixture.store.plan.Ready {
			t.Fatalf("plan Ready = false, want true on the READY path")
		}
	})

	t.Run("zero limit selects the documented default", func(t *testing.T) {
		fixture := newCanonicalFixture(t)
		request := fixture.request
		request.ProjectorPageLimit = 0
		if _, err := fixture.confirmation.Confirm(context.Background(), request); err != nil {
			t.Fatalf("Confirm() error = %v, want nil", err)
		}
		if fixture.projector.calls != 1 {
			t.Fatalf("projector calls = %d, want exactly 1", fixture.projector.calls)
		}
		if fixture.projector.lastLimit != defaultCanonicalProjectorLimit {
			t.Fatalf("projector limit = %d, want the default %d", fixture.projector.lastLimit, defaultCanonicalProjectorLimit)
		}
	})

	t.Run("negative limit selects the documented default", func(t *testing.T) {
		fixture := newCanonicalFixture(t)
		request := fixture.request
		request.ProjectorPageLimit = -7
		if _, err := fixture.confirmation.Confirm(context.Background(), request); err != nil {
			t.Fatalf("Confirm() error = %v, want nil", err)
		}
		if fixture.projector.lastLimit != defaultCanonicalProjectorLimit {
			t.Fatalf("projector limit = %d, want the default %d", fixture.projector.lastLimit, defaultCanonicalProjectorLimit)
		}
	})

	t.Run("maximum limit is accepted", func(t *testing.T) {
		fixture := newCanonicalFixture(t)
		request := fixture.request
		request.ProjectorPageLimit = MaxCanonicalProjectorLimit
		if _, err := fixture.confirmation.Confirm(context.Background(), request); err != nil {
			t.Fatalf("Confirm() error = %v, want nil", err)
		}
		if fixture.projector.calls != 1 || fixture.projector.lastLimit != MaxCanonicalProjectorLimit {
			t.Fatalf("projector calls=%d limit=%d, want 1 and %d",
				fixture.projector.calls, fixture.projector.lastLimit, MaxCanonicalProjectorLimit)
		}
		if !fixture.store.plan.Ready {
			t.Fatalf("plan Ready = false, want true")
		}
	})
}

// --- 6: missing Copy is pending ----------------------------------------------

func TestCanonicalMissingCopyIsPendingReschedule(t *testing.T) {
	fixture := newCanonicalFixture(t)
	fixture.copies.err = catalog.ErrNotFound

	_, err := fixture.confirmation.Confirm(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("Confirm() error = %v, want nil for a missing Copy", err)
	}
	if fixture.projector.calls != 1 {
		t.Fatalf("projector calls = %d, want 1", fixture.projector.calls)
	}
	if fixture.copies.calls != 1 {
		t.Fatalf("copy reader calls = %d, want 1", fixture.copies.calls)
	}
	if fixture.classifier.calls != 0 {
		t.Fatalf("classifier calls = %d, want 0", fixture.classifier.calls)
	}
	if fixture.store.calls != 1 {
		t.Fatalf("store calls = %d, want 1", fixture.store.calls)
	}
	plan := fixture.store.plan
	if plan.Ready {
		t.Fatalf("plan Ready = true, want false")
	}
	if plan.ManifestState != StateAwaitingCanonical || plan.JobState != jobs.StateRetryWait {
		t.Fatalf("plan states = %q/%q, want %q/%q", plan.ManifestState, plan.JobState, StateAwaitingCanonical, jobs.StateRetryWait)
	}
	if plan.ResultCopyID != "" {
		t.Fatalf("plan ResultCopyID = %q, want empty", plan.ResultCopyID)
	}
}

// --- 7: REMOVED Copy is pending ----------------------------------------------

func TestCanonicalRemovedCopyIsPendingReschedule(t *testing.T) {
	fixture := newCanonicalFixture(t)
	fixture.copies.copyRecord.Availability = catalog.CopyAvailabilityRemoved

	_, err := fixture.confirmation.Confirm(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("Confirm() error = %v, want nil for a REMOVED Copy", err)
	}
	if fixture.store.calls != 1 {
		t.Fatalf("store calls = %d, want 1", fixture.store.calls)
	}
	plan := fixture.store.plan
	if plan.Ready {
		t.Fatalf("plan Ready = true, want false for a REMOVED Copy")
	}
	if plan.ManifestState != StateAwaitingCanonical || plan.JobState != jobs.StateRetryWait {
		t.Fatalf("plan states = %q/%q, want %q/%q", plan.ManifestState, plan.JobState, StateAwaitingCanonical, jobs.StateRetryWait)
	}
	if fixture.classifier.calls != 0 {
		t.Fatalf("classifier calls = %d, want 0", fixture.classifier.calls)
	}
}

// --- 8: Copy binding / identity mismatch -------------------------------------

func TestCanonicalCopyMismatchFailsClosed(t *testing.T) {
	tests := []struct {
		label   string
		mutate  func(*catalog.Copy)
		wantErr error
	}{
		{
			label:   "other StorageBinding",
			mutate:  func(record *catalog.Copy) { record.StorageBindingID = catalog.StorageBindingID("binding-other") },
			wantErr: ErrCanonicalCopyBinding,
		},
		{
			label:   "other root id",
			mutate:  func(record *catalog.Copy) { record.IndexCoreRootID = "root-other" },
			wantErr: ErrCanonicalIdentity,
		},
		{
			label:   "other resource id",
			mutate:  func(record *catalog.Copy) { record.IndexCoreResourceID = "resource-other" },
			wantErr: ErrCanonicalIdentity,
		},
		{
			label:   "non-PRESENT availability",
			mutate:  func(record *catalog.Copy) { record.Availability = "" },
			wantErr: ErrCanonicalIdentity,
		},
	}
	for _, test := range tests {
		t.Run(test.label, func(t *testing.T) {
			fixture := newCanonicalFixture(t)
			test.mutate(&fixture.copies.copyRecord)

			if _, err := fixture.confirmation.Confirm(context.Background(), fixture.request); !errors.Is(err, test.wantErr) {
				t.Fatalf("Confirm() error = %v, want %v", err, test.wantErr)
			}
			if fixture.projector.calls != 1 {
				t.Fatalf("projector calls = %d, want 1", fixture.projector.calls)
			}
			if fixture.copies.calls != 1 {
				t.Fatalf("copy reader calls = %d, want 1", fixture.copies.calls)
			}
			if fixture.classifier.calls != 0 {
				t.Fatalf("classifier calls = %d, want 0 when the Copy identity is rejected", fixture.classifier.calls)
			}
			if fixture.store.calls != 0 {
				t.Fatalf("store calls = %d, want 0", fixture.store.calls)
			}
		})
	}
}

// --- 9: Variant binding -------------------------------------------------------

func TestCanonicalVariantBinding(t *testing.T) {
	variantA := catalog.VariantID("variant-a")
	variantB := catalog.VariantID("variant-b")

	t.Run("absent Manifest Variant skips the classifier", func(t *testing.T) {
		fixture := newCanonicalFixture(t)
		if _, err := fixture.confirmation.Confirm(context.Background(), fixture.request); err != nil {
			t.Fatalf("Confirm() error = %v, want nil", err)
		}
		if fixture.classifier.calls != 0 {
			t.Fatalf("classifier calls = %d, want 0 when Manifest.VariantID is nil", fixture.classifier.calls)
		}
		plan := fixture.store.plan
		if !plan.Ready {
			t.Fatalf("plan Ready = false, want true for an unclassified Copy")
		}
		if plan.ResultCopyID != canonicalTestCopyID {
			t.Fatalf("plan ResultCopyID = %q, want %q", plan.ResultCopyID, canonicalTestCopyID)
		}
		if plan.ExpectedCopyVariantID != nil {
			t.Fatalf("plan ExpectedCopyVariantID = %v, want nil", plan.ExpectedCopyVariantID)
		}
	})

	t.Run("present Manifest Variant is delegated to the fenced store", func(t *testing.T) {
		fixture := newCanonicalFixture(t)
		fixture.manifests.manifest.VariantID = canonicalVariantPointer(variantA)

		if _, err := fixture.confirmation.Confirm(context.Background(), fixture.request); err != nil {
			t.Fatalf("Confirm() error = %v, want nil", err)
		}
		if fixture.classifier.calls != 0 {
			t.Fatalf("classifier calls = %d, want 0 before the fenced store transaction", fixture.classifier.calls)
		}
		plan := fixture.store.plan
		if !plan.Ready {
			t.Fatalf("plan Ready = false, want true")
		}
		if plan.ResultCopyID != canonicalTestCopyID {
			t.Fatalf("plan ResultCopyID = %q, want %q", plan.ResultCopyID, canonicalTestCopyID)
		}
		if plan.ExpectedCopyVariantID != nil {
			t.Fatalf("plan ExpectedCopyVariantID = %v, want the observed NULL binding", plan.ExpectedCopyVariantID)
		}
		if plan.ManifestVariantID == nil || *plan.ManifestVariantID != variantA {
			t.Fatalf("plan ManifestVariantID = %v, want %q", plan.ManifestVariantID, variantA)
		}
		if plan.ClassifiedVariantID == nil || *plan.ClassifiedVariantID != variantA {
			t.Fatalf("plan ClassifiedVariantID = %v, want %q", plan.ClassifiedVariantID, variantA)
		}
	})

	t.Run("Copy already bound elsewhere is never silently reclassified", func(t *testing.T) {
		fixture := newCanonicalFixture(t)
		fixture.manifests.manifest.VariantID = canonicalVariantPointer(variantA)
		fixture.copies.copyRecord.VariantID = canonicalVariantPointer(variantB)

		if _, err := fixture.confirmation.Confirm(context.Background(), fixture.request); !errors.Is(err, ErrCanonicalCopyClassified) {
			t.Fatalf("Confirm() error = %v, want ErrCanonicalCopyClassified", err)
		}
		if fixture.classifier.calls != 0 {
			t.Fatalf("classifier calls = %d, want 0 when the Copy is already bound elsewhere", fixture.classifier.calls)
		}
		if fixture.store.calls != 0 {
			t.Fatalf("store calls = %d, want 0", fixture.store.calls)
		}
	})
}

// --- 10: plan ResultCopyID ----------------------------------------------------

func TestCanonicalPlanResultCopyID(t *testing.T) {
	variantA := catalog.VariantID("variant-a")

	t.Run("pending plan carries no Copy id", func(t *testing.T) {
		fixture := newCanonicalFixture(t)
		fixture.resolver.resolution = CanonicalResolution{}
		if _, err := fixture.confirmation.Confirm(context.Background(), fixture.request); err != nil {
			t.Fatalf("Confirm() error = %v, want nil", err)
		}
		if fixture.store.plan.ResultCopyID != "" {
			t.Fatalf("plan ResultCopyID = %q, want empty", fixture.store.plan.ResultCopyID)
		}
	})

	t.Run("ready plan carries the projected Copy id", func(t *testing.T) {
		fixture := newCanonicalFixture(t)
		if _, err := fixture.confirmation.Confirm(context.Background(), fixture.request); err != nil {
			t.Fatalf("Confirm() error = %v, want nil", err)
		}
		if fixture.store.plan.ResultCopyID != canonicalTestCopyID {
			t.Fatalf("plan ResultCopyID = %q, want %q", fixture.store.plan.ResultCopyID, canonicalTestCopyID)
		}
	})

	t.Run("ready plan keeps the projected Copy id while requesting atomic binding", func(t *testing.T) {
		fixture := newCanonicalFixture(t)
		fixture.manifests.manifest.VariantID = canonicalVariantPointer(variantA)

		if _, err := fixture.confirmation.Confirm(context.Background(), fixture.request); err != nil {
			t.Fatalf("Confirm() error = %v, want nil", err)
		}
		if fixture.store.plan.ResultCopyID != canonicalTestCopyID {
			t.Fatalf("plan ResultCopyID = %q, want projected Copy %q", fixture.store.plan.ResultCopyID, canonicalTestCopyID)
		}
		if fixture.store.plan.ClassifiedVariantID == nil || *fixture.store.plan.ClassifiedVariantID != variantA {
			t.Fatalf("plan ClassifiedVariantID = %v, want %q", fixture.store.plan.ClassifiedVariantID, variantA)
		}
	})
}

// --- 11: fences before any external call --------------------------------------

func TestCanonicalFencesFailBeforeResolverCall(t *testing.T) {
	// Both directions of the link are fences: the Manifest -> Job direction
	// (Manifest.JobID must equal the requested Job, mirroring refresh's
	// isCommittedRefresh) and the Job -> Manifest direction (the durable Job payload
	// must carry this Manifest, via ValidateLinkedAcquisitionJob). Neither direction
	// may be assumed from a particular store implementation.
	tests := []struct {
		label   string
		mutate  func(t *testing.T, fixture *canonicalFixture)
		wantErr error
	}{
		{
			label:   "Job not RUNNING",
			mutate:  func(_ *testing.T, fixture *canonicalFixture) { fixture.jobs.job.State = jobs.StateRetryWait },
			wantErr: ErrCanonicalFence,
		},
		{
			label: "Job leased by another owner",
			mutate: func(_ *testing.T, fixture *canonicalFixture) {
				fixture.jobs.job.LeaseOwner = canonicalStringPointer("other-owner")
			},
			wantErr: ErrCanonicalFence,
		},
		{
			label:   "Job has no lease owner",
			mutate:  func(_ *testing.T, fixture *canonicalFixture) { fixture.jobs.job.LeaseOwner = nil },
			wantErr: ErrCanonicalFence,
		},
		{
			label:   "different claim generation",
			mutate:  func(_ *testing.T, fixture *canonicalFixture) { fixture.jobs.job.ClaimAttempts = 4 },
			wantErr: ErrCanonicalFence,
		},
		{
			label: "Job not linked to the Manifest",
			mutate: func(t *testing.T, fixture *canonicalFixture) {
				other := ManifestID("manifest-other")
				key := AcquisitionJobIdempotencyKey(other)
				payload, err := json.Marshal(acquisitionJobPayload{
					SchemaVersion: AcquisitionPayloadSchemaVersion,
					ManifestID:    other,
				})
				if err != nil {
					t.Fatalf("marshal Job payload: %v", err)
				}
				fixture.jobs.job.IdempotencyKey = &key
				fixture.jobs.job.Payload = payload
			},
			wantErr: ErrCanonicalFence,
		},
		{
			label: "Manifest linked to a different Job",
			mutate: func(_ *testing.T, fixture *canonicalFixture) {
				other := jobs.JobID("job-other")
				fixture.manifests.manifest.JobID = &other
			},
			wantErr: ErrCanonicalFence,
		},
		{
			label:   "Manifest has no Job link",
			mutate:  func(_ *testing.T, fixture *canonicalFixture) { fixture.manifests.manifest.JobID = nil },
			wantErr: ErrCanonicalFence,
		},
		{
			label:   "Job of another type",
			mutate:  func(_ *testing.T, fixture *canonicalFixture) { fixture.jobs.job.Type = "CANONICAL_CONFIRMATION" },
			wantErr: ErrCanonicalFence,
		},
		{
			label: "Manifest not AWAITING_CANONICAL",
			mutate: func(_ *testing.T, fixture *canonicalFixture) {
				fixture.manifests.manifest.State = StateAwaitingVisibility
			},
			wantErr: ErrCanonicalManifestState,
		},
		{
			label:   "Manifest FAILED",
			mutate:  func(_ *testing.T, fixture *canonicalFixture) { fixture.manifests.manifest.State = StateFailed },
			wantErr: ErrCanonicalManifestState,
		},
		{
			label:   "StorageBinding missing",
			mutate:  func(_ *testing.T, fixture *canonicalFixture) { fixture.bindings.err = storage.ErrNotFound },
			wantErr: ErrCanonicalBindingMissing,
		},
		{
			label: "StorageBinding not active",
			mutate: func(_ *testing.T, fixture *canonicalFixture) {
				fixture.bindings.binding.Status = storage.BindingStatusDisabled
			},
			wantErr: ErrCanonicalBindingState,
		},
		{
			label:   "blank IndexCore root id",
			mutate:  func(_ *testing.T, fixture *canonicalFixture) { fixture.bindings.binding.IndexCoreRootID = "  " },
			wantErr: ErrCanonicalRootID,
		},
	}
	for _, test := range tests {
		t.Run(test.label, func(t *testing.T) {
			fixture := newCanonicalFixture(t)
			test.mutate(t, fixture)
			if _, err := fixture.confirmation.Confirm(context.Background(), fixture.request); !errors.Is(err, test.wantErr) {
				t.Fatalf("Confirm() error = %v, want %v", err, test.wantErr)
			}
			fixture.assertNoDownstreamCalls(t, test.label)
		})
	}
}

// --- 12: invalid requests -----------------------------------------------------

func TestCanonicalInvalidRequestFailsClosed(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		label      string
		mutate     func(*CanonicalRequest)
		loadsState bool
	}{
		{label: "empty ManifestID", mutate: func(request *CanonicalRequest) { request.ManifestID = "" }},
		{label: "empty JobID", mutate: func(request *CanonicalRequest) { request.JobID = "" }},
		{label: "blank owner", mutate: func(request *CanonicalRequest) { request.Owner = "   " }},
		{label: "zero ExpectedClaim", mutate: func(request *CanonicalRequest) { request.ExpectedClaim = 0 }},
		{label: "negative ExpectedClaim", mutate: func(request *CanonicalRequest) { request.ExpectedClaim = -1 }},
		{label: "zero Now", mutate: func(request *CanonicalRequest) { request.Now = time.Time{} }},
		{label: "zero RetryAt", mutate: func(request *CanonicalRequest) { request.RetryAt = time.Time{} }, loadsState: true},
		{label: "RetryAt equal to Now", mutate: func(request *CanonicalRequest) { request.RetryAt = request.Now }, loadsState: true},
		{label: "RetryAt before Now", mutate: func(request *CanonicalRequest) { request.RetryAt = request.Now.Add(-time.Second) }, loadsState: true},
		{
			label:      "projector page limit above the maximum",
			mutate:     func(request *CanonicalRequest) { request.ProjectorPageLimit = MaxCanonicalProjectorLimit + 1 },
			loadsState: true,
		},
	}
	for _, test := range tests {
		t.Run(test.label, func(t *testing.T) {
			fixture := newCanonicalFixture(t)
			request := fixture.request
			test.mutate(&request)
			if _, err := fixture.confirmation.Confirm(context.Background(), request); !errors.Is(err, ErrInvalidCanonicalRequest) {
				t.Fatalf("Confirm() error = %v, want ErrInvalidCanonicalRequest", err)
			}
			fixture.assertNoDownstreamCalls(t, test.label)
			wantReads := 0
			if test.loadsState {
				wantReads = 1
			}
			if fixture.manifests.calls != wantReads || fixture.jobs.calls != wantReads {
				t.Fatalf("Manifest/Job reads = %d/%d, want %d/%d", fixture.manifests.calls, fixture.jobs.calls, wantReads, wantReads)
			}
		})
	}

	t.Run("valid zero time fields elsewhere are accepted", func(t *testing.T) {
		fixture := newCanonicalFixture(t)
		request := fixture.request
		request.Now = now
		request.RetryAt = now.Add(time.Hour)
		request.ProjectorPageLimit = 1
		if _, err := fixture.confirmation.Confirm(context.Background(), request); err != nil {
			t.Fatalf("Confirm() error = %v, want nil", err)
		}
	})
}

// --- 13: READY replay ---------------------------------------------------------

func TestCanonicalReadyReplaySkipsExternalCalls(t *testing.T) {
	fixture := newCanonicalFixture(t)
	fixture.manifests.manifest.State = StateReady
	fixture.manifests.manifest.ResultCopyID = canonicalCopyIDPointer("copy-committed")
	fixture.manifests.manifest.ResultName = nil
	fixture.jobs.job.State = jobs.StateSucceeded
	fixture.bindings.binding.Status = storage.BindingStatusDisabled
	fixture.bindings.binding.IndexCoreRootID = "changed-root"
	fixture.bindings.err = errors.New("binding must not be loaded on replay")
	fixture.request.RetryAt = time.Time{}
	fixture.store.result = CanonicalResult{
		Manifest: fixture.manifests.manifest,
		Job:      fixture.jobs.job,
		Changed:  false,
	}

	result, err := fixture.confirmation.Confirm(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("Confirm() error = %v, want nil", err)
	}
	if fixture.store.calls != 1 {
		t.Fatalf("store calls = %d, want 1", fixture.store.calls)
	}
	plan := fixture.store.plan
	if !plan.Ready {
		t.Fatalf("plan Ready = false, want true for a READY replay")
	}
	if plan.ResultCopyID != "copy-committed" {
		t.Fatalf("plan ResultCopyID = %q, want the committed %q", plan.ResultCopyID, "copy-committed")
	}
	if plan.ManifestState != StateReady || plan.JobState != jobs.StateSucceeded {
		t.Fatalf("plan states = %q/%q, want %q/%q", plan.ManifestState, plan.JobState, StateReady, jobs.StateSucceeded)
	}
	if plan.RetryAt != nil {
		t.Fatalf("plan RetryAt = %v, want nil for a replay", plan.RetryAt)
	}
	if fixture.resolver.calls != 0 || fixture.projector.calls != 0 || fixture.copies.calls != 0 || fixture.classifier.calls != 0 {
		t.Fatalf("resolver=%d projector=%d copies=%d classifier=%d, want all 0 on a READY replay",
			fixture.resolver.calls, fixture.projector.calls, fixture.copies.calls, fixture.classifier.calls)
	}
	if result.Changed {
		t.Fatalf("result Changed = true, want exact replay Changed=false")
	}
	if fixture.bindings.calls != 0 {
		t.Fatalf("binding reads = %d, want 0 on READY replay", fixture.bindings.calls)
	}
}

// --- 14: constructor ----------------------------------------------------------

type canonicalDependencySet struct {
	manifests ManifestReader
	jobs      JobReader
	bindings  RefreshBindingReader
	resolve   CanonicalResolvePort
	projector CanonicalProjectorPort
	copies    CanonicalCopyReader
	classify  CanonicalClassifier
	store     CanonicalStageStore
}

func (set *canonicalDependencySet) build() (*CanonicalConfirmation, error) {
	return NewCanonicalConfirmation(
		set.manifests, set.jobs, set.bindings, set.resolve,
		set.projector, set.copies, set.classify, set.store,
	)
}

func newCanonicalDependencySet(t *testing.T) *canonicalDependencySet {
	t.Helper()
	fixture := newCanonicalFixture(t)
	return &canonicalDependencySet{
		manifests: fixture.manifests,
		jobs:      fixture.jobs,
		bindings:  fixture.bindings,
		resolve:   fixture.resolver,
		projector: fixture.projector,
		copies:    fixture.copies,
		classify:  fixture.classifier,
		store:     fixture.store,
	}
}

func TestCanonicalConstructorRejectsNilDependencies(t *testing.T) {
	tests := []struct {
		label string
		clear func(*canonicalDependencySet)
	}{
		{label: "nil ManifestReader", clear: func(set *canonicalDependencySet) { set.manifests = nil }},
		{label: "nil JobReader", clear: func(set *canonicalDependencySet) { set.jobs = nil }},
		{label: "nil RefreshBindingReader", clear: func(set *canonicalDependencySet) { set.bindings = nil }},
		{label: "nil CanonicalResolvePort", clear: func(set *canonicalDependencySet) { set.resolve = nil }},
		{label: "nil CanonicalProjectorPort", clear: func(set *canonicalDependencySet) { set.projector = nil }},
		{label: "nil CanonicalCopyReader", clear: func(set *canonicalDependencySet) { set.copies = nil }},
		{label: "nil CanonicalClassifier", clear: func(set *canonicalDependencySet) { set.classify = nil }},
		{label: "nil CanonicalStageStore", clear: func(set *canonicalDependencySet) { set.store = nil }},
	}
	for _, test := range tests {
		t.Run(test.label, func(t *testing.T) {
			set := newCanonicalDependencySet(t)
			test.clear(set)
			confirmation, err := set.build()
			if !errors.Is(err, ErrInvalidCanonicalRequest) {
				t.Fatalf("NewCanonicalConfirmation() error = %v, want ErrInvalidCanonicalRequest", err)
			}
			if confirmation != nil {
				t.Fatalf("NewCanonicalConfirmation() = %v, want nil", confirmation)
			}
		})
	}

	t.Run("all dependencies present", func(t *testing.T) {
		set := newCanonicalDependencySet(t)
		confirmation, err := set.build()
		if err != nil {
			t.Fatalf("NewCanonicalConfirmation() error = %v, want nil", err)
		}
		if confirmation == nil {
			t.Fatalf("NewCanonicalConfirmation() = nil, want a confirmation")
		}
	})
}

// --- 15: cancelled context ----------------------------------------------------

func TestCanonicalCancelledContextReturnsEarly(t *testing.T) {
	fixture := newCanonicalFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := fixture.confirmation.Confirm(ctx, fixture.request); !errors.Is(err, context.Canceled) {
		t.Fatalf("Confirm() error = %v, want context.Canceled", err)
	}
	fixture.assertNoDownstreamCalls(t, "cancelled context")
	if fixture.manifests.calls != 0 {
		t.Fatalf("manifest reads = %d, want 0 for a cancelled context", fixture.manifests.calls)
	}
}
