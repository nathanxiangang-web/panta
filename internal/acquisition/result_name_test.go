package acquisition

import (
	"errors"
	"strings"
	"testing"
)

// --- D-032: expected_name is intent, result_name is the locator ----------------

// TestValidateExpectedNameIsIntentNotPathSegment proves the request expectation is
// only bounded text. D-032 explicitly keeps expected_name as intent, so a separator
// or a dot component does not make the intent invalid.
func TestValidateExpectedNameIsIntentNotPathSegment(t *testing.T) {
	accepted := []string{
		"item.mkv", "影片.mkv", "my file.mkv", " lead.mkv", "trail.mkv ",
		"already/has/separator.mkv", `back\slash.mkv`, "..", ".", "a/../b",
	}
	for _, name := range accepted {
		if err := ValidateExpectedName(name); err != nil {
			t.Fatalf("ValidateExpectedName(%q) error = %v, want the intent accepted", name, err)
		}
	}
	rejected := []string{"", "   ", "\t", "a\x00b", strings.Repeat("x", MaxExpectedNameLength+1)}
	for _, name := range rejected {
		if err := ValidateExpectedName(name); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("ValidateExpectedName(%q) error = %v, want ErrInvalidArgument", name, err)
		}
	}
}

// TestValidateResultNameIsOneDirectChild proves the locator is a path segment.
func TestValidateResultNameIsOneDirectChild(t *testing.T) {
	accepted := []string{
		"item.mkv", "影片.mkv", "my file.mkv", " lead.mkv", "trail.mkv ",
		"MiXeD-Case.MKV", "a", strings.Repeat("界", MaxResultNameLength),
	}
	for _, name := range accepted {
		if err := ValidateResultName(name); err != nil {
			t.Fatalf("ValidateResultName(%q) error = %v, want it accepted", name, err)
		}
	}
	rejected := []string{
		"", "   ", "\t", ".", "..", "/", "a/b", "/abs", "a/b/", `a\b`, `\`,
		"a\x00b", strings.Repeat("x", MaxResultNameLength+1),
	}
	for _, name := range rejected {
		if err := ValidateResultName(name); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("ValidateResultName(%q) error = %v, want ErrInvalidArgument", name, err)
		}
	}
}

// TestValidatePointerHelpers covers the optional forms.
func TestValidatePointerHelpers(t *testing.T) {
	if err := ValidateResultNamePointer(nil); err != nil {
		t.Fatalf("nil result name error = %v", err)
	}
	if err := ValidateExpectedNamePointer(nil); err != nil {
		t.Fatalf("nil expected name error = %v", err)
	}
	if err := ValidateResultNamePointer(stringPointer("a/b")); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("separator result name error = %v, want ErrInvalidArgument", err)
	}
	// A separator is fine as intent but not as a locator.
	if err := ValidateExpectedNamePointer(stringPointer("a/b")); err != nil {
		t.Fatalf("separator expected name error = %v, want the intent accepted", err)
	}
}

// TestResolveResultNameTruthTable is the D-032 decision table. A valid provider name
// always wins, expected_name is only a fallback, and the two facts never contaminate
// each other.
func TestResolveResultNameTruthTable(t *testing.T) {
	tests := []struct {
		name        string
		existing    *string
		provider    *string
		expected    *string
		want        string
		wantChanged bool
		wantErr     error
	}{
		{
			name:     "provider name wins over a different expected name",
			provider: stringPointer("B.mkv"), expected: stringPointer("A.mkv"),
			want: "B.mkv", wantChanged: true,
		},
		{
			name:     "provider name wins even when expected is absent",
			provider: stringPointer("B.mkv"), expected: nil,
			want: "B.mkv", wantChanged: true,
		},
		{
			name:     "blank provider name falls back to expected",
			provider: stringPointer("   "), expected: stringPointer("A.mkv"),
			want: "A.mkv", wantChanged: true,
		},
		{
			name:     "absent provider name falls back to expected",
			provider: nil, expected: stringPointer("A.mkv"),
			want: "A.mkv", wantChanged: true,
		},
		{
			name:     "no provider name and no expected name is missing identity",
			provider: nil, expected: nil,
			wantErr: ErrResultNameMissing,
		},
		{
			name:     "blank provider name and blank expected name is missing identity",
			provider: stringPointer("  "), expected: stringPointer(" "),
			wantErr: ErrResultNameMissing,
		},
		{
			name:     "malformed provider name fails closed and never falls back",
			provider: stringPointer("a/b"), expected: stringPointer("A.mkv"),
			wantErr: ErrProviderResultName,
		},
		{
			name:     "malformed provider name fails closed even without a fallback",
			provider: stringPointer(".."), expected: nil,
			wantErr: ErrProviderResultName,
		},
		{
			name:     "present persisted locator is immutable on a different provider name",
			existing: stringPointer("frozen.mkv"), provider: stringPointer("other.mkv"),
			wantErr: ErrResultNameImmutable,
		},
		{
			name:     "present persisted locator is idempotent on the same provider name",
			existing: stringPointer("frozen.mkv"), provider: stringPointer("frozen.mkv"),
			want: "frozen.mkv", wantChanged: false,
		},
		{
			name:     "present persisted locator survives an absent provider name",
			existing: stringPointer("frozen.mkv"), provider: nil,
			want: "frozen.mkv", wantChanged: false,
		},
		{
			name:     "present persisted locator survives a blank provider name",
			existing: stringPointer("frozen.mkv"), provider: stringPointer("  "),
			want: "frozen.mkv", wantChanged: false,
		},
		{
			name:     "expected_name never overrides a persisted locator",
			existing: stringPointer("frozen.mkv"), provider: nil, expected: stringPointer("intent.mkv"),
			want: "frozen.mkv", wantChanged: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, changed, err := ResolveResultName(test.existing, test.provider, test.expected)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("error = %v, want %v", err, test.wantErr)
				}
				if got != "" || changed {
					t.Fatalf("got (%q, %v) alongside an error, want no value", got, changed)
				}
				return
			}
			if err != nil {
				t.Fatalf("error = %v, want nil", err)
			}
			if got != test.want {
				t.Fatalf("resolved = %q, want %q", got, test.want)
			}
			if len(got) != len(test.want) {
				t.Fatalf("resolved length = %d, want %d", len(got), len(test.want))
			}
			if changed != test.wantChanged {
				t.Fatalf("changed = %v, want %v", changed, test.wantChanged)
			}
		})
	}
}

// TestResolveResultNamePreservesBytesExactly proves no trim, clean, or case folding
// happens to the locator, including when it falls back to the intent.
func TestResolveResultNamePreservesBytesExactly(t *testing.T) {
	names := []string{
		"item.mkv", "影片.mkv", "my file.mkv", " lead.mkv", "trail.mkv ",
		"MiXeD-Case.MKV", strings.Repeat("界", MaxResultNameLength),
	}
	for _, name := range names {
		t.Run("provider "+name, func(t *testing.T) {
			got, changed, err := ResolveResultName(nil, stringPointer(name), nil)
			if err != nil || !changed {
				t.Fatalf("ResolveResultName() = (%q, %v, %v)", got, changed, err)
			}
			if got != name || len(got) != len(name) {
				t.Fatalf("resolved = %q (%d bytes), want %q (%d bytes)", got, len(got), name, len(name))
			}
		})
		t.Run("fallback "+name, func(t *testing.T) {
			got, changed, err := ResolveResultName(nil, nil, stringPointer(name))
			if err != nil || !changed {
				t.Fatalf("ResolveResultName() = (%q, %v, %v)", got, changed, err)
			}
			if got != name || len(got) != len(name) {
				t.Fatalf("resolved = %q (%d bytes), want %q (%d bytes)", got, len(got), name, len(name))
			}
		})
	}
}

// TestResolveResultNameRejectsMalformedPersistedLocator proves durable corruption
// fails closed instead of being silently rewritten.
func TestResolveResultNameRejectsMalformedPersistedLocator(t *testing.T) {
	if _, _, err := ResolveResultName(stringPointer("a/b"), stringPointer("a/b"), nil); !errors.Is(err, ErrResultNameImmutable) {
		t.Fatalf("error = %v, want ErrResultNameImmutable", err)
	}
}

// TestProviderResultNameFromStatusNormalizesBlankToAbsent proves a blank provider
// name is treated as "no name", distinct from a malformed one.
func TestProviderResultNameFromStatusNormalizesBlankToAbsent(t *testing.T) {
	if got := ProviderResultNameFromStatus(nil); got != nil {
		t.Fatalf("nil -> %v, want nil", *got)
	}
	for _, blank := range []string{"", " ", "\t", "\n", "  \t "} {
		if got := ProviderResultNameFromStatus(stringPointer(blank)); got != nil {
			t.Fatalf("%q -> %v, want nil", blank, *got)
		}
	}
	got := ProviderResultNameFromStatus(stringPointer(" name.mkv "))
	if got == nil || *got != " name.mkv " {
		t.Fatalf("got %v, want the exact bytes preserved", got)
	}
}

// stringPointer is a small local helper for the optional identity fields.
func stringPointer(value string) *string { return &value }
