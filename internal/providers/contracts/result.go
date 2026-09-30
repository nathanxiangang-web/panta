package contracts

import (
	"errors"
	"strings"
	"unicode/utf8"
)

// MaxDownloadResultNameRunes bounds a provider-neutral direct-child name. It matches
// the acquisition Manifest expected-name limit so a name accepted here can be
// persisted without a second, different bound.
const MaxDownloadResultNameRunes = 512

// ErrInvalidDownloadResultName reports a name that is not one direct path segment.
var ErrInvalidDownloadResultName = errors.New("provider download result name is not a valid direct-child name")

// ValidateDirectChildName validates a provider-reported acquired object name.
//
// A valid name is exactly one direct path segment:
//
//   - valid UTF-8;
//   - non-empty and not blank;
//   - at most MaxDownloadResultNameRunes runes;
//   - no NUL;
//   - not "." or "..";
//   - no "/" and no "\" separator.
//
// Exact spelling, case, and interior whitespace are preserved. The value is never
// trimmed, path-cleaned, or normalized, because a provider name must round-trip
// byte for byte to remain usable as canonical identity input.
func ValidateDirectChildName(name string) error {
	if name == "" || !utf8.ValidString(name) || strings.ContainsRune(name, '\x00') ||
		strings.TrimSpace(name) == "" || utf8.RuneCountInString(name) > MaxDownloadResultNameRunes {
		return ErrInvalidDownloadResultName
	}
	if name == "." || name == ".." {
		return ErrInvalidDownloadResultName
	}
	if strings.ContainsAny(name, `/\`) {
		return ErrInvalidDownloadResultName
	}
	return nil
}

// Validate checks that an optional result is well formed. A nil result is valid,
// because a pending, running, failed, or canceled task may omit identity.
func (result *DownloadResult) Validate() error {
	if result == nil {
		return nil
	}
	return ValidateDirectChildName(result.Name)
}
