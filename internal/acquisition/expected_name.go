package acquisition

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

var (
	// ErrProviderResultName reports a provider-reported name that is not one valid
	// direct child, so it cannot become durable acquisition identity.
	ErrProviderResultName = errors.New("acquisition provider result name is invalid")

	// ErrExpectedNameMismatch reports a provider result that disagrees with the
	// expected name already frozen on the Manifest.
	ErrExpectedNameMismatch = errors.New("acquisition provider result name does not match the frozen expected name")

	// ErrExpectedNameMissing reports a provider success that cannot supply the
	// identity the acquisition needs.
	ErrExpectedNameMissing = errors.New("acquisition provider success has no usable expected name")

	// ErrExpectedNameRequired reports an AWAITING_VISIBILITY Manifest that carries no
	// frozen identity, so its scope must not be advanced to canonical confirmation.
	ErrExpectedNameRequired = errors.New("acquisition Manifest awaiting visibility has no frozen expected name")
)

// ValidateExpectedName validates a direct-child acquisition identity.
//
// This is the Manifest-level rule for D-031: one direct path segment, valid UTF-8,
// non-blank, bound by MaxExpectedNameLength, no NUL, not "." or "..", and no "/" or
// "\" separator. Exact spelling, case, and interior whitespace are preserved; the
// value is never trimmed, path-cleaned, or normalized, because a provider-reported
// name must round-trip byte for byte.
func ValidateExpectedName(name string) error {
	if name == "" || !utf8.ValidString(name) || strings.ContainsRune(name, '\x00') ||
		strings.TrimSpace(name) == "" || utf8.RuneCountInString(name) > MaxExpectedNameLength {
		return ErrInvalidArgument
	}
	if name == "." || name == ".." {
		return ErrInvalidArgument
	}
	if strings.ContainsAny(name, `/\`) {
		return ErrInvalidArgument
	}
	return nil
}

// ValidateExpectedNamePointer validates an optional Manifest expected name.
func ValidateExpectedNamePointer(name *string) error {
	if name == nil {
		return nil
	}
	return ValidateExpectedName(*name)
}

// resolveProviderResultName applies the frozen D-031 provider-success identity rules
// to the durable expected name and the provider-reported result name.
//
// It returns the name that must be committed and whether the durable value changes.
//
//	frozen set   + same provider result   -> keep frozen, no change
//	frozen set   + no provider result     -> keep frozen, no change
//	frozen set   + different provider name-> ErrExpectedNameMismatch
//	frozen NULL  + valid provider name     -> store provider name
//	frozen NULL  + no provider result      -> ErrExpectedNameMissing
//
// No timing, Journal ordering, "only new item" heuristic, latest-resource guess,
// provider file ID, or OpenList path participates in this decision.
func ResolveProviderResultName(frozen *string, providerResult *string) (string, bool, error) {
	if frozen != nil {
		if err := ValidateExpectedName(*frozen); err != nil {
			// A malformed frozen identity is durable corruption: fail closed rather
			// than silently rewriting identity.
			return "", false, fmt.Errorf("%w: frozen expected name is invalid", ErrExpectedNameMismatch)
		}
		if providerResult == nil {
			return *frozen, false, nil
		}
		if err := ValidateExpectedName(*providerResult); err != nil {
			return "", false, fmt.Errorf("%w: provider result name is invalid", ErrProviderResultName)
		}
		// Exact byte comparison: case, spelling, and whitespace must agree.
		if *providerResult != *frozen {
			return "", false, fmt.Errorf("%w: provider reported a different direct child",
				ErrExpectedNameMismatch)
		}
		return *frozen, false, nil
	}

	if providerResult == nil {
		return "", false, ErrExpectedNameMissing
	}
	if err := ValidateExpectedName(*providerResult); err != nil {
		return "", false, fmt.Errorf("%w: provider result name is invalid", ErrProviderResultName)
	}
	return *providerResult, true, nil
}
