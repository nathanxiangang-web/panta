package architecture_test

import (
	"encoding/json"
	"os/exec"
	"strings"
	"testing"
)

type listedPackage struct {
	ImportPath string
	Imports    []string
}

func TestGateZeroDomainImportBoundaries(t *testing.T) {
	tests := []struct {
		packagePath string
		forbidden   []string
	}{
		{
			packagePath: "github.com/nathanxiangang-web/panta/internal/acquisition",
			forbidden: []string{
				"github.com/jackc/pgx", "database/sql", "/internal/providers", "/providers/115",
				"/internal/integrations/indexcore", "/internal/integrations/openlist",
				"/internal/search", "/internal/agent", "/internal/auth",
			},
		},
		{
			packagePath: "github.com/nathanxiangang-web/panta/internal/catalog",
			forbidden: []string{
				"github.com/jackc/pgx", "database/sql", "/internal/providers", "/providers/115",
				"/internal/integrations/indexcore", "/internal/integrations/openlist",
				"/internal/search", "/internal/agent", "/internal/auth", "/internal/usage", "/internal/share",
			},
		},
		{
			packagePath: "github.com/nathanxiangang-web/panta/internal/jobs",
			forbidden: []string{
				"github.com/jackc/pgx", "database/sql", "/internal/providers", "/providers/115",
				"/internal/integrations/indexcore", "/internal/integrations/openlist",
			},
		},
		{
			packagePath: "github.com/nathanxiangang-web/panta/internal/integrations/indexcore",
			forbidden:   []string{"github.com/jackc/pgx", "database/sql", "/providers/115"},
		},
		{
			// The OpenList integration is a pure HTTP read adapter: it may use the
			// standard library and its own port contracts, and nothing else in Panta.
			packagePath: "github.com/nathanxiangang-web/panta/internal/integrations/openlist",
			forbidden: []string{
				"github.com/jackc/pgx", "database/sql",
				"/internal/acquisition", "/internal/jobs", "/internal/store",
				"/internal/providers", "/providers/115",
				"/internal/integrations/indexcore",
				"/internal/search", "/internal/agent", "/internal/auth",
			},
		},
		{
			packagePath: "github.com/nathanxiangang-web/panta/internal/storage",
			forbidden: []string{
				"github.com/jackc/pgx", "database/sql", "/internal/providers", "/providers/115",
				"/internal/integrations/indexcore", "/internal/integrations/openlist",
			},
		},
		{
			packagePath: "github.com/nathanxiangang-web/panta/internal/projector",
			forbidden: []string{
				"github.com/jackc/pgx", "database/sql", "/internal/providers", "/providers/115",
				"/internal/integrations/openlist",
			},
		},
		{
			packagePath: "github.com/nathanxiangang-web/panta/internal/resourceview",
			forbidden: []string{
				"github.com/jackc/pgx", "database/sql", "github.com/nathanxiangang-web/index-core",
				"/internal/integrations/openlist",
				"/internal/providers", "/providers/115", "/internal/search",
			},
		},
		{
			// The provider-session composition boundary may implement the
			// acquisition port and use the opaque storage ConnectionID, but it must
			// not reach persistence, provider adapters, or any integration.
			packagePath: "github.com/nathanxiangang-web/panta/internal/providers/session",
			forbidden: []string{
				"github.com/jackc/pgx", "database/sql", "/providers/115",
				"/internal/store", "/internal/integrations/indexcore", "/internal/integrations/openlist",
				"/internal/search", "/internal/agent", "/internal/auth",
			},
		},
		{
			// The concrete 115 adapter is provider-isolated: it may use its pinned
			// upstream library and contracts, and nothing else in Panta. In
			// particular no 115 type may escape into the acquisition domain, and it
			// must not reach persistence or any integration.
			packagePath: "github.com/nathanxiangang-web/panta/internal/providers/115",
			forbidden: []string{
				"github.com/jackc/pgx", "database/sql",
				"/internal/acquisition", "/internal/jobs", "/internal/store",
				"/internal/integrations/indexcore", "/internal/integrations/openlist",
				"/internal/search", "/internal/agent", "/internal/auth",
				"/internal/providers/session", "/internal/providers/registry",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.packagePath, func(t *testing.T) {
			listed := goList(t, test.packagePath)
			for _, imported := range listed.Imports {
				if isAllowedDomainDependency(listed.ImportPath, imported, test.forbidden) {
					continue
				}
				for _, forbidden := range test.forbidden {
					if strings.Contains(imported, forbidden) {
						t.Fatalf("%s imports forbidden dependency %s", listed.ImportPath, imported)
					}
				}
			}
		})
	}
}

func TestGateZeroCoreDoesNotImportConcrete115Provider(t *testing.T) {
	for _, packagePath := range []string{
		"github.com/nathanxiangang-web/panta/internal/acquisition",
		"github.com/nathanxiangang-web/panta/internal/catalog",
		"github.com/nathanxiangang-web/panta/internal/jobs",
		"github.com/nathanxiangang-web/panta/internal/resourceview",
		"github.com/nathanxiangang-web/panta/internal/storage",
	} {
		listed := goList(t, packagePath)
		for _, imported := range listed.Imports {
			if strings.Contains(imported, "/internal/providers/115") {
				t.Fatalf("%s imports concrete provider %s", listed.ImportPath, imported)
			}
		}
	}
}

func TestTestUtilitiesAreAbsentFromRuntimeDependencyTree(t *testing.T) {
	command := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}", "github.com/nathanxiangang-web/panta/cmd/...")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("go list runtime dependencies: %v", err)
	}
	dependencies := string(output)
	for _, forbidden := range []string{
		"github.com/nathanxiangang-web/panta/internal/providers/testprovider",
		"github.com/nathanxiangang-web/panta/internal/providers/contracttest",
		"github.com/nathanxiangang-web/panta/test/e2e",
		"github.com/nathanxiangang-web/index-core",
	} {
		if strings.Contains(dependencies, forbidden+"\n") {
			t.Fatalf("runtime dependency tree contains test utility %s", forbidden)
		}
	}
}

// allowedDomainDependencies lists the narrow, architect-authorized exceptions to
// the blanket forbidden-substring rules. Each entry names one dependent package,
// one exact imported package path, and the specific forbidden substring that the
// exception relaxes. Anything else - including any other package under
// internal/providers - still fails the guard.
var allowedDomainDependencies = []struct {
	dependent string
	imported  string
	relaxed   string
}{
	{
		dependent: "github.com/nathanxiangang-web/panta/internal/acquisition",
		imported:  "github.com/nathanxiangang-web/panta/internal/providers/contracts",
		relaxed:   "/internal/providers",
	},
	{
		// Gate 3.8: the acquisition visibility verifier depends on the Panta-owned
		// OpenList visibility port. The integration package also holds the concrete
		// HTTP client, so this is auditable rather than airtight; the behavioral
		// guarantee that acquisition never drives list/search/walk lives in the
		// verifier's own tests and in its single Stat call.
		dependent: "github.com/nathanxiangang-web/panta/internal/acquisition",
		imported:  "github.com/nathanxiangang-web/panta/internal/integrations/openlist",
		relaxed:   "/internal/integrations/openlist",
	},
}

// isAllowedDomainDependency reports whether this exact import is an authorized
// exception. The import must still not match any other forbidden substring.
func isAllowedDomainDependency(dependent, imported string, forbidden []string) bool {
	for _, allowed := range allowedDomainDependencies {
		if allowed.dependent != dependent || allowed.imported != imported {
			continue
		}
		for _, candidate := range forbidden {
			if candidate != allowed.relaxed && strings.Contains(imported, candidate) {
				return false
			}
		}
		return true
	}
	return false
}

func goList(t *testing.T, packagePath string) listedPackage {
	t.Helper()
	command := exec.Command("go", "list", "-json", packagePath)
	output, err := command.Output()
	if err != nil {
		t.Fatalf("go list %s: %v", packagePath, err)
	}
	var listed listedPackage
	if err := json.Unmarshal(output, &listed); err != nil {
		t.Fatalf("decode go list %s: %v", packagePath, err)
	}
	return listed
}
