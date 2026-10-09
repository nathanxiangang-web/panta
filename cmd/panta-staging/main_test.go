package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
)

func TestAcceptanceRequiresEveryIndependentEvidenceEdge(t *testing.T) {
	base := baseline{Generation: 1, JournalLastSeq: 10}
	state := durableState{bindingID: "binding", providerRefPresent: true,
		copyBindingID: "binding", copyRootID: "root", copyResourceID: "resource", copyAvailability: "PRESENT"}
	report := observation{ManifestState: "READY", JobState: "SUCCEEDED", ResultCopyID: "copy",
		RootID: "root", RootGeneration: 2, Q5Matches: 1, CanonicalResourceID: "resource",
		Q8MatchingEventSeq: 11, ProjectionCursor: 11, ProviderRefPresent: true}
	if outcome, _ := evaluate(report, state, base); outcome != "PASS" {
		t.Fatal("complete evidence did not pass")
	}
	tests := []struct {
		name   string
		change func(*observation, *durableState)
	}{
		{"Hint/provider success is not READY", func(r *observation, _ *durableState) { r.ManifestState = "AWAITING_CANONICAL" }},
		{"missing Copy", func(r *observation, _ *durableState) { r.ResultCopyID = "" }},
		{"no new generation", func(r *observation, _ *durableState) { r.RootGeneration = 1 }},
		{"no matching Q8 event", func(r *observation, _ *durableState) { r.Q8MatchingEventSeq = 0 }},
		{"projection behind", func(r *observation, _ *durableState) { r.ProjectionCursor = 10 }},
		{"Copy identity mismatch", func(_ *observation, s *durableState) { s.copyResourceID = "other" }},
		{"provider reference unknown", func(_ *observation, s *durableState) { s.providerRefPresent = false }},
		{"Q5 ambiguous", func(r *observation, _ *durableState) { r.Q5Ambiguous = true }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate, evidence := report, state
			test.change(&candidate, &evidence)
			if outcome, _ := evaluate(candidate, evidence, base); outcome == "PASS" {
				t.Fatal("incomplete evidence passed")
			}
		})
	}
	state.providerTaskState = "START_RESERVED"
	report.ManifestState = "RECOVERY_REQUIRED"
	if outcome, reason := evaluate(report, state, base); outcome != "STOP" || reason != "provider reference uncertain" {
		t.Fatalf("uncertain side effect = %s, %s", outcome, reason)
	}
}

func TestEvidenceIsRedactedAndBaselineIsRootBound(t *testing.T) {
	var output bytes.Buffer
	report := observation{Kind: "observation", Outcome: "PROGRESS", ResultNameSHA256: fingerprint("private-result-name")}
	if err := writeJSON(&output, report); err != nil || strings.Contains(output.String(), "private-result-name") {
		t.Fatalf("unsafe report: %v", err)
	}
	if goruntime.GOOS != "linux" {
		t.Skip("protected baseline file requires Linux")
	}
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(directory, "baseline.json")
	content, _ := json.Marshal(baseline{Kind: "baseline", RootID: "staging-root", Generation: 1, JournalLastSeq: 5})
	if err := os.WriteFile(file, content, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := readBaseline(file, "other-root"); err == nil {
		t.Fatal("accepted another root's baseline")
	}
	if _, err := readBaseline(file, "staging-root"); err != nil {
		t.Fatal(err)
	}
}

func TestStagingToolRejectsImplicitMutationModes(t *testing.T) {
	for _, args := range [][]string{{}, {"start"}, {"submit"}, {"watch", "--cookie=unsafe"}} {
		if err := run(t.Context(), args, &bytes.Buffer{}); err == nil {
			t.Fatalf("accepted unsafe mode %v", args)
		}
	}
}
