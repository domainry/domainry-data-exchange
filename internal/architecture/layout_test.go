package architecture

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRequiredDeploymentPackagesExist(t *testing.T) {
	for _, path := range []string{
		"../../module", "../../remote",
		"../application/dataexchange", "../domain/dataexchange/model", "../domain/dataexchange/repository", "../domain/dataexchange/service",
		"../assembly/module", "../assembly/saas", "../adapter/dataexchangesdk",
		"../transport/http/module", "../transport/http/saas",
		"../infrastructure/persistence/database/dataexchange", "../infrastructure/persistence/database/schema",
		"../infrastructure/persistence/sqlite", "../infrastructure/persistence/mysql", "../infrastructure/persistence/postgres",
	} {
		if info, err := os.Stat(path); err != nil || !info.IsDir() {
			t.Fatalf("required package %s is missing", path)
		}
	}
}

func TestLayerDependenciesPointInward(t *testing.T) {
	set := token.NewFileSet()
	checks := map[string][]string{
		"../domain/":         {"/internal/application/", "/internal/adapter/", "/internal/assembly/", "/internal/infrastructure/", "/internal/transport/"},
		"../application/":    {"/internal/adapter/", "/internal/assembly/", "/internal/transport/"},
		"../infrastructure/": {"/internal/application/", "/internal/adapter/", "/internal/assembly/", "/internal/transport/"},
	}
	for root, forbidden := range checks {
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			file, parseErr := parser.ParseFile(set, path, nil, parser.ImportsOnly)
			if parseErr != nil {
				return parseErr
			}
			for _, spec := range file.Imports {
				value := strings.Trim(spec.Path.Value, `"`)
				for _, segment := range forbidden {
					if strings.Contains(value, segment) {
						t.Errorf("%s imports outer layer %s", path, value)
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestModuleUsesTaggedDependencies(t *testing.T) {
	content, err := os.ReadFile("../../go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(content), "replace ") || strings.Contains(string(content), "../domainry-") {
		t.Fatal("Data Exchange must consume released module tags, not local directory replacements")
	}
}

func TestPublicFacadesStayThinAndPersistenceStaysInternal(t *testing.T) {
	for _, path := range []string{"../../module", "../../remote"} {
		entries, err := os.ReadDir(path)
		if err != nil {
			t.Fatal(err)
		}
		production := 0
		for _, entry := range entries {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") && !strings.HasSuffix(entry.Name(), "_test.go") {
				production++
			}
		}
		if production != 1 {
			t.Fatalf("public facade %s has %d production files; want exactly one", path, production)
		}
	}

	set := token.NewFileSet()
	err := filepath.WalkDir("../..", func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		file, parseErr := parser.ParseFile(set, path, nil, parser.ImportsOnly)
		if parseErr != nil {
			return parseErr
		}
		for _, spec := range file.Imports {
			if strings.Trim(spec.Path.Value, `"`) == "database/sql" && !strings.Contains(filepath.ToSlash(path), "/internal/infrastructure/persistence/") {
				t.Errorf("database/sql escaped persistence boundary: %s", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCSVCodecHasOneOwnerInSDK(t *testing.T) {
	if _, err := os.Stat("../../fileengine"); !os.IsNotExist(err) {
		t.Fatal("implementation repository must not retain a parallel file engine")
	}
}

func TestPersistenceDoesNotBranchOnConcreteDatabaseTypes(t *testing.T) {
	forbidden := []string{"sqlite", "sqlite3", "mysql", "postgres", "postgresql", "pgx", ".Name() =="}
	err := filepath.WalkDir("../../internal", func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		normalized := filepath.ToSlash(path)
		if filepath.Base(path) == "engine.go" || strings.Contains(normalized, "/persistence/sqlite/") || strings.Contains(normalized, "/persistence/mysql/") || strings.Contains(normalized, "/persistence/postgres/") {
			return nil
		}
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		text := strings.ToLower(string(content))
		for _, value := range forbidden {
			if strings.Contains(text, strings.ToLower(value)) {
				t.Errorf("Data Exchange implementation binds concrete database %q in %s", value, path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
