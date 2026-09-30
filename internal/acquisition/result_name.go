package acquisition

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

var (
	// ErrProviderResultName reports a provider-reported result name that is present
	// but not one valid direct child, so it cannot become a durable locator.
	ErrProviderResultName = errors.New("acquisition provider result name is invalid")

	// ErrResultNameMissing reports a provider success that cannot resolve a durable
	// result locator from either the provider or the request intent.
	ErrResultNameMissing = errors.New("acquisition provider success has no usable result name")

	// ErrResultNameImmutable reports a provider success that would change an already
	// persisted locator.
	ErrResultNameImmutable = errors.New("acquisition result name is immutable once persisted")

	// ErrResultNameConflict reports a concurrent handoff that lost the durable race.
	ErrResultNameConflict = errors.New("acquisition result name conflicts with the committed locator")
)

// ValidateResultName validates the durable acquisition result locator.
//
// This is the manifest-level rule for D-032: one direct path segment, valid UTF-8,
// non-blank, bound by MaxResultNameLength, no NUL, not "." or "..", and no "/" or
// "\" separator. Exact spelling, case, and interior whitespace are preserved; the
// value is never trimmed, path-cleaned, or normalized, because a provider-reported
// name must round-trip byte for byte to remain usable identity.
func ValidateResultName(name string) error {
	if name == "" || !utf8.ValidString(name) || strings.ContainsRune(name, '\x00') ||
		strings.TrimSpace(name) == "" || utf8.RuneCountInString(name) > MaxResultNameLength {
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

// ValidateResultNamePointer validates an optional persisted result locator.
func ValidateResultNamePointer(name *string) error {
	if name == nil {
		return nil
	}
	return ValidateResultName(*name)
}

// ValidateExpectedName validates the optional request-time expectation.
//
// D-032 keeps expected_name as intent and fallback, not as the acquired-result
// locator. It is therefore validated only as bounded required text, exactly as the
// frozen schema has always constrained it; a separator or a dot component does not
// make the intent invalid.
func ValidateExpectedName(name string) error {
	if !validRequiredText(name, MaxExpectedNameLength) {
		return ErrInvalidArgument
	}
	return nil
}

// ValidateExpectedNamePointer validates an optional request expectation.
func ValidateExpectedNamePointer(name *string) error {
	if name == nil {
		return nil
	}
	return ValidateExpectedName(*name)
}

// ResolveResultName applies the frozen D-032 provider-success locator rules and
// returns the value that must be persisted plus whether the durable value changes.
//
//	valid provider name               -> use it, even when it differs from expected_name
//	blank/absent provider name
//	    + valid expected_name         -> fall back to expected_name
//	    + absent/invalid expected_name-> ErrResultNameMissing
//	present but invalid provider name -> ErrProviderResultName, never a silent fallback
//	already persisted result_name     -> immutable: same value is idempotent,
//	                                     a different value is ErrResultNameImmutable
//
// expected_name is never rewritten, so request intent and observed result remain two
// separate facts. No timing, Journal ordering, "only new item" heuristic,
// latest-resource guess, provider file ID, or OpenList path participates here.
func ResolveResultName(
	existingResultName *string,
	providerResultName *string,
	expectedName *string,
) (string, bool, error) {
	// Immutability first: once persisted, the locator can only be replayed, never
	// changed, regardless of what the provider or intent now say.
	if existingResultName != nil {
		if err := ValidateResultName(*existingResultName); err != nil {
			return "", false, fmt.Errorf("%w: persisted result name is invalid", ErrResultNameImmutable)
		}
		if providerResultName == nil || strings.TrimSpace(*providerResultName) == "" {
			// No new evidence: the persisted locator stands.
			return *existingResultName, false, nil
		}
		if err := ValidateResultName(*providerResultName); err != nil {
			return "", false, fmt.Errorf("%w: provider result name is invalid", ErrProviderResultName)
		}
		if *providerResultName != *existingResultName {
			return "", false, fmt.Errorf("%w: provider reports %q but %q is persisted",
				ErrResultNameImmutable, *providerResultName, *existingResultName)
		}
		return *existingResultName, false, nil
	}

	// A present but unusable provider name is a contract violation, not an absent
	// name, so it fails closed instead of silently falling back to intent.
	if providerResultName != nil && strings.TrimSpace(*providerResultName) != "" {
		if err := ValidateResultName(*providerResultName); err != nil {
			return "", false, fmt.Errorf("%w: %w", ErrProviderResultName, err)
		}
		return *providerResultName, true, nil
	}

	// The provider reported no usable name, so the request intent is the fallback.
	//
	// Intent is deliberately permissive, but promoting it to the durable locator is
	// not: the fallback must satisfy the SAME direct-child rule as a provider-reported
	// name. Otherwise an intent like "a/b" would be written as a locator and only be
	// caught later by the database CHECK, turning a typed identity failure into a
	// persistence error. A permissive intent that cannot be promoted therefore fails
	// closed with the same sentinel as a missing identity, and nothing mutates.
	if expectedName == nil {
		return "", false, ErrResultNameMissing
	}
	if err := ValidateResultName(*expectedName); err != nil {
		return "", false, fmt.Errorf(
			"%w: expected name %q is valid intent but not a usable result locator",
			ErrResultNameMissing, *expectedName)
	}
	return *expectedName, true, nil
}

// ProviderResultNameFromStatus extracts the optional provider-neutral result name.
// A nil status result, an empty name, or a blank name all mean "the provider
// reported no usable name", which is distinct from a malformed name.
func ProviderResultNameFromStatus(name *string) *string {
	if name == nil {
		return nil
	}
	if strings.TrimSpace(*name) == "" {
		return nil
	}
	cloned := *name
	return &cloned
}
