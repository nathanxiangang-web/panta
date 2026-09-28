package storage

import (
	"errors"
	"testing"
)

func TestNormalizeMountPath(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "root", input: "/", want: "/"},
		{name: "root repeated slashes", input: "////", want: "/"},
		{name: "absolute path", input: "/media/movies", want: "/media/movies"},
		{name: "trailing slash", input: "/media/movies/", want: "/media/movies"},
		{name: "repeated slashes", input: "//media///movies//", want: "/media/movies"},
		{name: "spaces remain opaque", input: "/media/My Movies", want: "/media/My Movies"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := NormalizeMountPath(test.input)
			if err != nil || got != test.want {
				t.Fatalf("NormalizeMountPath(%q) = %q, %v; want %q", test.input, got, err, test.want)
			}
		})
	}
}

func TestNormalizeMountPathRejectsInvalidForms(t *testing.T) {
	for _, input := range []string{
		"", "relative", ".", "..", `\media\movies`, `/media\movies`, "/.", "/..",
		"/media/./movies", "/media/../movies",
	} {
		t.Run(input, func(t *testing.T) {
			if got, err := NormalizeMountPath(input); got != "" || !errors.Is(err, ErrInvalidMountPath) {
				t.Fatalf("NormalizeMountPath(%q) = %q, %v; want ErrInvalidMountPath", input, got, err)
			}
		})
	}
}

func TestStorageStatusesAreExplicit(t *testing.T) {
	if !ConnectionStatusActive.Valid() || !ConnectionStatusDisabled.Valid() || ConnectionStatus("PENDING").Valid() {
		t.Fatal("unexpected StorageConnection status validation")
	}
	if !BindingStatusActive.Valid() || !BindingStatusDisabled.Valid() || BindingStatus("PENDING").Valid() {
		t.Fatal("unexpected StorageBinding status validation")
	}
}
