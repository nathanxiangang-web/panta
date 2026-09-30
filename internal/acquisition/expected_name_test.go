package acquisition

// Gate 3.9 (issue #41) domain tests for D-031 acquired-result identity.
//
// These tests pin the exact rules implemented by expected_name.go: an expected
// name is one direct path segment, its exact bytes are preserved (never trimmed,
// case-folded, or normalized), and ResolveProviderResultName fails closed on
// every identity disagreement. The file lives in the internal test package so
// the exported helpers are reached unqualified and unexported identifiers stay
// reachable if the domain grows.

import (
	"errors"
	"strings"
	"testing"
)

// gate39NameCase labels a name input so that subtests stay readable even for
// inputs (NUL, tab, empty) that cannot be printed as a subtest name.
type gate39NameCase struct {
	label string
	name  string
}

// gate39AcceptedNames is the positive matrix for tests 1-3. Leading and trailing
// spaces are deliberately included: trimming is forbidden, so they are valid and
// must round-trip unchanged.
func gate39AcceptedNames() []gate39NameCase {
	return []gate39NameCase{
		{label: "plain ascii", name: "item.bin"},
		{label: "unicode", name: "影片.mkv"},
		{label: "interior spaces", name: "my file.bin"},
		{label: "exactly max length", name: strings.Repeat("a", MaxExpectedNameLength)},
		{label: "single rune", name: "a"},
		{label: "leading space", name: " leading"},
		{label: "trailing space", name: "trailing "},
		{label: "mixed case", name: "Report.PDF"},
	}
}

// gate39RejectedNames is the negative matrix for test 2.
func gate39RejectedNames() []gate39NameCase {
	return []gate39NameCase{
		{label: "empty", name: ""},
		{label: "spaces only", name: "   "},
		{label: "tab only", name: "\t"},
		{label: "slash only", name: "/"},
		{label: "relative with slash", name: "a/b"},
		{label: "absolute", name: "/abs"},
		{label: "trailing slash", name: "a/b/"},
		{label: "backslash separator", name: `a\b`},
		{label: "backslash only", name: `\`},
		{label: "dot", name: "."},
		{label: "dot dot", name: ".."},
		{label: "embedded nul", name: "a\x00b"},
		{label: "one rune over max length", name: strings.Repeat("a", MaxExpectedNameLength+1)},
	}
}

// TestExpectedNameAcceptsDirectChildNames covers required test 1: every valid
// direct-child name is accepted and the value is left exactly as it arrived.
func TestExpectedNameAcceptsDirectChildNames(t *testing.T) {
	for _, tc := range gate39AcceptedNames() {
		t.Run(tc.label, func(t *testing.T) {
			value := tc.name
			if err := ValidateExpectedName(value); err != nil {
				t.Fatalf("ValidateExpectedName(%q) = %v, want nil", value, err)
			}
			if value != tc.name {
				t.Fatalf("ValidateExpectedName(%q) changed the value to %q", tc.name, value)
			}
			if len(value) != len(tc.name) {
				t.Fatalf("ValidateExpectedName(%q) len = %d, want %d", tc.name, len(value), len(tc.name))
			}
		})
	}
}

// TestExpectedNameRejectsNonDirectChildNames covers required test 2: every name
// that is not exactly one direct path segment is rejected with ErrInvalidArgument.
func TestExpectedNameRejectsNonDirectChildNames(t *testing.T) {
	for _, tc := range gate39RejectedNames() {
		t.Run(tc.label, func(t *testing.T) {
			if err := ValidateExpectedName(tc.name); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("ValidateExpectedName(%q) = %v, want ErrInvalidArgument", tc.name, err)
			}
		})
	}
}

// TestResolveProviderResultNamePreservesAcceptedNamesByteForByte covers required
// test 3: accepted names survive the resolution path byte for byte, in both the
// "freeze this name" and the "frozen name already matches" direction.
func TestResolveProviderResultNamePreservesAcceptedNamesByteForByte(t *testing.T) {
	for _, tc := range gate39AcceptedNames() {
		t.Run(tc.label, func(t *testing.T) {
			provider := tc.name
			got, changed, err := ResolveProviderResultName(nil, &provider)
			if err != nil {
				t.Fatalf("ResolveProviderResultName(nil, %q) error = %v, want nil", provider, err)
			}
			if !changed {
				t.Fatalf("ResolveProviderResultName(nil, %q) changed = false, want true", provider)
			}
			if got != provider {
				t.Fatalf("ResolveProviderResultName(nil, %q) name = %q, want byte-identical name", provider, got)
			}
			if len(got) != len(provider) {
				t.Fatalf("ResolveProviderResultName(nil, %q) len = %d, want %d", provider, len(got), len(provider))
			}

			frozen := tc.name
			got, changed, err = ResolveProviderResultName(&frozen, &provider)
			if err != nil {
				t.Fatalf("ResolveProviderResultName(%q, %q) error = %v, want nil", frozen, provider, err)
			}
			if changed {
				t.Fatalf("ResolveProviderResultName(%q, %q) changed = true, want false", frozen, provider)
			}
			if got != frozen {
				t.Fatalf("ResolveProviderResultName(%q, %q) name = %q, want byte-identical frozen name", frozen, provider, got)
			}
			if len(got) != len(frozen) {
				t.Fatalf("ResolveProviderResultName(%q, %q) len = %d, want %d", frozen, provider, len(got), len(frozen))
			}
		})
	}

	t.Run("trailing space is not trimmed", func(t *testing.T) {
		provider := "trailing "
		got, changed, err := ResolveProviderResultName(nil, &provider)
		if err != nil {
			t.Fatalf("ResolveProviderResultName(nil, %q) error = %v, want nil", provider, err)
		}
		if !changed || got != "trailing " || len(got) != len("trailing ") {
			t.Fatalf("ResolveProviderResultName(nil, %q) = (%q, %v), want (\"trailing \", true)", provider, got, changed)
		}
	})

	t.Run("case is not normalized against a near miss", func(t *testing.T) {
		frozen := "Report.PDF"
		provider := "report.pdf"
		got, changed, err := ResolveProviderResultName(&frozen, &provider)
		if !errors.Is(err, ErrExpectedNameMismatch) {
			t.Fatalf("ResolveProviderResultName(%q, %q) error = %v, want ErrExpectedNameMismatch", frozen, provider, err)
		}
		if got != "" || changed {
			t.Fatalf("ResolveProviderResultName(%q, %q) = (%q, %v) on error, want (\"\", false)", frozen, provider, got, changed)
		}
	})
}

// TestResolveProviderResultNameTruthTable covers required test 4: all five frozen
// rows plus the fail-closed corruption rows, including the returned `changed` bool.
func TestResolveProviderResultNameTruthTable(t *testing.T) {
	ptr := func(value string) *string { return &value }

	cases := []struct {
		label       string
		frozen      *string
		provider    *string
		wantName    string
		wantChanged bool
		wantErr     error
	}{
		{
			label:    "frozen set and provider identical",
			frozen:   ptr("item.bin"),
			provider: ptr("item.bin"),
			wantName: "item.bin",
		},
		{
			label:    "frozen set and provider nil",
			frozen:   ptr("item.bin"),
			provider: nil,
			wantName: "item.bin",
		},
		{
			label:    "frozen set and provider differs only by case",
			frozen:   ptr("Item.Bin"),
			provider: ptr("item.bin"),
			wantErr:  ErrExpectedNameMismatch,
		},
		{
			label:    "frozen set and provider differs only by trailing space",
			frozen:   ptr("item.bin"),
			provider: ptr("item.bin "),
			wantErr:  ErrExpectedNameMismatch,
		},
		{
			label:    "frozen set and provider completely different",
			frozen:   ptr("item.bin"),
			provider: ptr("other.bin"),
			wantErr:  ErrExpectedNameMismatch,
		},
		{
			label:       "frozen nil and valid provider name",
			frozen:      nil,
			provider:    ptr("item.bin"),
			wantName:    "item.bin",
			wantChanged: true,
		},
		{
			label:    "frozen nil and provider nil",
			frozen:   nil,
			provider: nil,
			wantErr:  ErrExpectedNameMissing,
		},
		{
			label:    "frozen set to an invalid name fails closed",
			frozen:   ptr("a/b"),
			provider: ptr("ok.bin"),
			wantErr:  ErrExpectedNameMismatch,
		},
		{
			label:    "frozen set to an invalid name with identical invalid provider fails closed",
			frozen:   ptr("a/b"),
			provider: ptr("a/b"),
			wantErr:  ErrExpectedNameMismatch,
		},
		{
			label:    "frozen nil and invalid provider name",
			frozen:   nil,
			provider: ptr("a/b"),
			wantErr:  ErrProviderResultName,
		},
	}

	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			got, changed, err := ResolveProviderResultName(tc.frozen, tc.provider)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("ResolveProviderResultName() error = %v, want %v", err, tc.wantErr)
				}
				if got != "" || changed {
					t.Fatalf("ResolveProviderResultName() = (%q, %v) on error, want (\"\", false)", got, changed)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveProviderResultName() error = %v, want nil", err)
			}
			if got != tc.wantName {
				t.Fatalf("ResolveProviderResultName() name = %q, want %q", got, tc.wantName)
			}
			if len(got) != len(tc.wantName) {
				t.Fatalf("ResolveProviderResultName() len = %d, want %d", len(got), len(tc.wantName))
			}
			if changed != tc.wantChanged {
				t.Fatalf("ResolveProviderResultName() changed = %v, want %v", changed, tc.wantChanged)
			}
		})
	}
}

// TestValidateExpectedNamePointer covers required test 5: nil is a valid optional
// identity, while an empty or separator-bearing pointer is rejected.
func TestValidateExpectedNamePointer(t *testing.T) {
	ptr := func(value string) *string { return &value }

	if err := ValidateExpectedNamePointer(nil); err != nil {
		t.Fatalf("ValidateExpectedNamePointer(nil) = %v, want nil", err)
	}
	if err := ValidateExpectedNamePointer(ptr("item.bin")); err != nil {
		t.Fatalf("ValidateExpectedNamePointer(%q) = %v, want nil", "item.bin", err)
	}
	if err := ValidateExpectedNamePointer(ptr("")); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ValidateExpectedNamePointer(\"\") = %v, want ErrInvalidArgument", err)
	}
	if err := ValidateExpectedNamePointer(ptr("a/b")); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ValidateExpectedNamePointer(%q) = %v, want ErrInvalidArgument", "a/b", err)
	}
}
