package acquisition

import (
	"errors"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

var ErrInvalidSource = errors.New("invalid acquisition source")

// ResolveSource classifies an opaque provider input without changing its value.
// It does no lookup or network operation. Invalid input is rejected, never fixed.
func ResolveSource(value string) (contracts.SourceReference, error) {
	if value == "" || len(value) > MaxSourceRefLength || !utf8.ValidString(value) ||
		strings.ContainsRune(value, 0) || strings.TrimSpace(value) != value {
		return contracts.SourceReference{}, ErrInvalidSource
	}
	colon := strings.IndexByte(value, ':')
	if colon <= 0 || colon == len(value)-1 {
		return contracts.SourceReference{}, ErrInvalidSource
	}
	scheme := strings.ToLower(value[:colon])
	switch scheme {
	case "magnet", "ed2k":
		if strings.TrimSpace(value[colon+1:]) == "" || strings.ContainsAny(value, "\r\n") {
			return contracts.SourceReference{}, ErrInvalidSource
		}
	case "http", "https":
		parsed, err := url.Parse(value)
		if err != nil || parsed.Scheme == "" || strings.ToLower(parsed.Scheme) != scheme ||
			parsed.Opaque != "" || parsed.User != nil || parsed.Host == "" || parsed.Hostname() == "" ||
			!strings.HasPrefix(value[colon+1:], "//") {
			return contracts.SourceReference{}, ErrInvalidSource
		}
	default:
		return contracts.SourceReference{}, ErrInvalidSource
	}
	return contracts.SourceReference{Scheme: scheme, Value: value}, nil
}
