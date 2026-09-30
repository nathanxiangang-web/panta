package acquisition_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
)

func TestResolveSourceTruthTableAndExactValue(t *testing.T) {
	valid := []struct{ input, scheme string }{
		{`magnet:?xt=urn:btih:ABC&dn=A%20B&tr=https%3A%2F%2Ftracker.example%2Fa&x=tail`, "magnet"},
		{`MAGNET:?xt=urn:btih:ABC&dn=Exact+Case`, "magnet"},
		{`http://example.invalid/a%2Fb?x=%2f&x=Two#part`, "http"},
		{`HTTPS://example.invalid/path?q=a%20b`, "https"},
		{`ed2k://|file|A.iso|123|ABC|/`, "ed2k"},
	}
	for _, test := range valid {
		got, err := acquisition.ResolveSource(test.input)
		if err != nil || got.Scheme != test.scheme || got.Value != test.input {
			t.Fatalf("ResolveSource valid scheme=%q error=%v exact=%t", got.Scheme, err, got.Value == test.input)
		}
	}
	invalid := []string{
		"", " ", "magnet:", "ed2k:", "ftp://example.invalid/a", "http:/relative", "https://",
		"http://user:pass@example.invalid/a", "http://example.invalid/a\n", "magnet:?xt=x\x00",
		" magnet:?xt=x", "magnet:?xt=x ", "magnet:\n", "https://example.invalid/\xff",
		strings.Repeat("a", acquisition.MaxSourceRefLength+1),
	}
	for _, input := range invalid {
		if _, err := acquisition.ResolveSource(input); !errors.Is(err, acquisition.ErrInvalidSource) {
			t.Fatalf("invalid source accepted: length=%d error=%v", len(input), err)
		}
	}
}

func TestResolveSourceDoesNotContactHTTPOrigin(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	defer server.Close()
	input := server.URL + "/file%2Fpart?q=Exact%20Bytes"
	got, err := acquisition.ResolveSource(input)
	if err != nil || got.Value != input || got.Scheme != "http" || calls != 0 {
		t.Fatalf("resolve performed I/O or changed value: scheme=%q exact=%t calls=%d err=%v", got.Scheme, got.Value == input, calls, err)
	}
}
