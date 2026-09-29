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
			packagePath: "github.com/nathanxiangang-web/panta/internal/catalog",
			forbidden: []string{
				"github.com/jackc/pgx", "database/sql", "/internal/providers", "/providers/115",
				"/internal/integrations/indexcore",
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
	}

	for _, test := range tests {
		t.Run(test.packagePath, func(t *testing.T) {
			listed := goList(t, test.packagePath)
			for _, imported := range listed.Imports {
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
