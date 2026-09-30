package contracts_test

// Gate 3.9 (issue #42) provider-contract tests for the D-032 acquired-result
// identity: the provider-neutral contract carries exactly one direct-child name,
// the name validator matches the acquisition domain rule, and no provider
// addressing leaks into the contract.

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

// directChildNameCase labels a name input so that subtests stay readable even for
// inputs (NUL, tab, empty) that cannot be printed as a subtest name.
type directChildNameCase struct {
	label string
	name  string
}

// acceptedDirectChildNames mirrors the acquisition-domain positive matrix.
// Leading and trailing spaces are valid: trimming is forbidden.
func acceptedDirectChildNames() []directChildNameCase {
	return []directChildNameCase{
		{label: "plain ascii", name: "item.bin"},
		{label: "unicode", name: "影片.mkv"},
		{label: "interior spaces", name: "my file.bin"},
		{label: "exactly max length", name: strings.Repeat("a", contracts.MaxDownloadResultNameRunes)},
		{label: "single rune", name: "a"},
		{label: "leading space", name: " leading"},
		{label: "trailing space", name: "trailing "},
	}
}

// rejectedDirectChildNames is the negative matrix: not exactly one direct segment.
func rejectedDirectChildNames() []directChildNameCase {
	return []directChildNameCase{
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
		{label: "one rune over max length", name: strings.Repeat("a", contracts.MaxDownloadResultNameRunes+1)},
	}
}

// TestValidateDirectChildNameAcceptsDirectChildNames covers required test 6
// (accept side): every valid direct-child name passes and is left unmodified.
func TestValidateDirectChildNameAcceptsDirectChildNames(t *testing.T) {
	for _, tc := range acceptedDirectChildNames() {
		t.Run(tc.label, func(t *testing.T) {
			value := tc.name
			if err := contracts.ValidateDirectChildName(value); err != nil {
				t.Fatalf("ValidateDirectChildName(%q) = %v, want nil", value, err)
			}
			if value != tc.name {
				t.Fatalf("ValidateDirectChildName(%q) changed the value to %q", tc.name, value)
			}
			if len(value) != len(tc.name) {
				t.Fatalf("ValidateDirectChildName(%q) len = %d, want %d", tc.name, len(value), len(tc.name))
			}
		})
	}
}

// TestValidateDirectChildNameRejectsNonDirectChildNames covers required test 6
// (reject side): every non-direct-segment name is rejected with the contract sentinel.
func TestValidateDirectChildNameRejectsNonDirectChildNames(t *testing.T) {
	for _, tc := range rejectedDirectChildNames() {
		t.Run(tc.label, func(t *testing.T) {
			if err := contracts.ValidateDirectChildName(tc.name); !errors.Is(err, contracts.ErrInvalidDownloadResultName) {
				t.Fatalf("ValidateDirectChildName(%q) = %v, want ErrInvalidDownloadResultName", tc.name, err)
			}
		})
	}
}

// TestDownloadResultContractForbidsProviderSpecificIdentifiers covers required
// test 7: Gate 3.9 forbids provider file/dir IDs, OpenList paths, and IndexCore
// resource IDs in the provider contract, so no DownloadResult or TaskStatus field
// may be named after them. Provider-neutral identity is the direct-child name only.
func TestDownloadResultContractForbidsProviderSpecificIdentifiers(t *testing.T) {
	forbidden := []string{"fileid", "dirid", "openlist", "resource", "path", "mount"}
	types := []reflect.Type{
		reflect.TypeOf(contracts.DownloadResult{}),
		reflect.TypeOf(contracts.TaskStatus{}),
	}

	for _, typ := range types {
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i).Name
			lowerField := strings.ToLower(field)
			for _, banned := range forbidden {
				if strings.Contains(lowerField, banned) {
					t.Errorf("%s carries provider-specific field %q (matches forbidden %q)", typ.Name(), field, banned)
				}
			}
		}
	}
}

// TestDownloadResultValidate covers required test 8: a nil result is valid, a
// valid name is valid, and a separator-bearing name is rejected.
func TestDownloadResultValidate(t *testing.T) {
	var nilResult *contracts.DownloadResult
	if err := nilResult.Validate(); err != nil {
		t.Fatalf("(*DownloadResult)(nil).Validate() = %v, want nil", err)
	}
	if err := (&contracts.DownloadResult{Name: "ok"}).Validate(); err != nil {
		t.Fatalf("DownloadResult{Name: %q}.Validate() = %v, want nil", "ok", err)
	}
	if err := (&contracts.DownloadResult{Name: ""}).Validate(); !errors.Is(err, contracts.ErrInvalidDownloadResultName) {
		t.Fatalf("DownloadResult{Name: \"\"}.Validate() = %v, want ErrInvalidDownloadResultName", err)
	}
	if err := (&contracts.DownloadResult{Name: "a/b"}).Validate(); !errors.Is(err, contracts.ErrInvalidDownloadResultName) {
		t.Fatalf("DownloadResult{Name: %q}.Validate() = %v, want ErrInvalidDownloadResultName", "a/b", err)
	}
}
