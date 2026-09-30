package openlist

import (
	"net/http"
	"testing"
	"time"
)

// TestConfigTimeoutIsAlwaysBounded pins the internal bound that makes the public
// client safe even when a caller supplies its own HTTP client. It lives in-package
// because the effective deadline is not part of the public surface, and the public
// behaviour it protects (no unbounded request) is covered from outside.
func TestConfigTimeoutIsAlwaysBounded(t *testing.T) {
	tests := []struct {
		name       string
		configured time.Duration
		want       time.Duration
	}{
		{name: "unset uses the default bound", configured: 0, want: defaultTimeout},
		{name: "negative uses the default bound", configured: -time.Second, want: defaultTimeout},
		{name: "normal value is honoured", configured: 3 * time.Second, want: 3 * time.Second},
		{name: "excessive value is capped", configured: 24 * time.Hour, want: maximumTimeout},
		{name: "just over the cap is capped", configured: maximumTimeout + time.Second, want: maximumTimeout},
		{name: "exactly the cap is honoured", configured: maximumTimeout, want: maximumTimeout},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := effectiveTimeout(test.configured); got != test.want {
				t.Fatalf("effectiveTimeout(%v) = %v, want %v", test.configured, got, test.want)
			}
		})
	}
}

// TestInjectedClientTimeoutIsBounded pins that an injected client cannot leave the
// client unbounded, while a tighter caller-chosen value is preserved.
func TestInjectedClientTimeoutIsBounded(t *testing.T) {
	t.Run("zero injected timeout receives the bound", func(t *testing.T) {
		injected := &http.Client{Timeout: 0}
		client, err := NewHTTPClient(Config{BaseURL: "https://openlist.example", HTTPClient: injected})
		if err != nil {
			t.Fatalf("NewHTTPClient() error = %v", err)
		}
		if client.timeout <= 0 || client.timeout > maximumTimeout {
			t.Fatalf("client timeout = %v, want a bounded value", client.timeout)
		}
		if injected.Timeout <= 0 || injected.Timeout > maximumTimeout {
			t.Fatalf("injected client timeout = %v, want a bounded value, not unbounded", injected.Timeout)
		}
	})

	t.Run("loose injected timeout is tightened to the bound", func(t *testing.T) {
		injected := &http.Client{Timeout: 24 * time.Hour}
		client, err := NewHTTPClient(Config{BaseURL: "https://openlist.example", HTTPClient: injected})
		if err != nil {
			t.Fatalf("NewHTTPClient() error = %v", err)
		}
		if client.timeout > maximumTimeout {
			t.Fatalf("client timeout = %v, want at most %v", client.timeout, maximumTimeout)
		}
		if injected.Timeout > maximumTimeout {
			t.Fatalf("injected client timeout = %v, want at most %v", injected.Timeout, maximumTimeout)
		}
	})

	t.Run("tighter injected timeout is preserved", func(t *testing.T) {
		injected := &http.Client{Timeout: 50 * time.Millisecond}
		client, err := NewHTTPClient(Config{BaseURL: "https://openlist.example", HTTPClient: injected})
		if err != nil {
			t.Fatalf("NewHTTPClient() error = %v", err)
		}
		if injected.Timeout != 50*time.Millisecond {
			t.Fatalf("injected client timeout = %v, want the caller's 50ms preserved", injected.Timeout)
		}
		if client.timeout != defaultTimeout {
			t.Fatalf("client timeout = %v, want the default bound; the injected one is tighter", client.timeout)
		}
	})
}

// TestTokenSurroundingWhitespaceIsRejected pins that the token is never rewritten.
func TestTokenSurroundingWhitespaceIsRejected(t *testing.T) {
	for _, token := range []string{" leading", "trailing ", "\ttab", "\nnewline", " "} {
		if _, err := NewHTTPClient(Config{BaseURL: "https://openlist.example", Token: token}); err == nil {
			t.Fatalf("NewHTTPClient(token=%q) succeeded, want a rejection instead of silent trimming", token)
		}
	}
	// Interior whitespace is legitimate and preserved.
	const interior = "tok en"
	client, err := NewHTTPClient(Config{BaseURL: "https://openlist.example", Token: interior})
	if err != nil {
		t.Fatalf("NewHTTPClient(interior) error = %v", err)
	}
	if client.token != interior {
		t.Fatalf("client token = %q, want %q byte for byte", client.token, interior)
	}
}
