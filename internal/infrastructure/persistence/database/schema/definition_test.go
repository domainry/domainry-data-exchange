package schema

import (
	"slices"
	"strings"
	"testing"

	persistenceengine "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence"
	"github.com/domainry/domainry-foundation/schemaownership"
)

func TestSchemaOwnershipMatchesEveryFreshTableAndPrimaryKey(t *testing.T) {
	tables := SchemaOwnership()
	if err := schemaownership.ValidateAll(tables); err != nil {
		t.Fatal(err)
	}
	engine, err := persistenceengine.NewEngine("sqlite")
	if err != nil {
		t.Fatal(err)
	}
	migrations, err := SchemaMigrations(engine, "")
	if err != nil {
		t.Fatal(err)
	}
	created := map[string]string{}
	for _, migration := range migrations {
		const prefix = `CREATE TABLE "`
		if !strings.HasPrefix(migration.SQL, prefix) {
			t.Fatalf("Data Exchange migration %s is not canonical CREATE TABLE DDL: %s", migration.ID, migration.SQL)
		}
		name, _, found := strings.Cut(strings.TrimPrefix(migration.SQL, prefix), `"`)
		if !found || name == "" {
			t.Fatalf("invalid CREATE TABLE statement: %s", migration.SQL)
		}
		if _, duplicate := created[name]; duplicate {
			t.Fatalf("Data Exchange table %s is created more than once", name)
		}
		created[name] = migration.SQL
	}
	if len(created) != len(tables) {
		t.Fatalf("fresh Data Exchange tables=%d ownership contracts=%d: created=%v owned=%v", len(created), len(tables), sortedKeys(created), OwnedTables())
	}
	for _, table := range tables {
		statement, found := created[table.Name]
		if !found {
			t.Fatalf("Data Exchange table %s has ownership but no canonical DDL", table.Name)
		}
		quoted := make([]string, len(table.PrimaryKey))
		for index, column := range table.PrimaryKey {
			quoted[index] = `"` + column + `"`
		}
		if primaryKey := "PRIMARY KEY (" + strings.Join(quoted, ", ") + ")"; !strings.Contains(statement, primaryKey) {
			t.Fatalf("Data Exchange table %s ownership primary key %v does not match DDL: %s", table.Name, table.PrimaryKey, statement)
		}
	}
}

func TestSchemaOwnershipReturnsIndependentValues(t *testing.T) {
	first, second := SchemaOwnership(), SchemaOwnership()
	if !slices.Equal(OwnedTables(), schemaownership.Names(second)) {
		t.Fatal("Data Exchange owned table names drifted from ownership contracts")
	}
	first[0].PrimaryKey[0] = "changed"
	if second[0].PrimaryKey[0] == "changed" {
		t.Fatal("Data Exchange ownership primary keys share mutable storage")
	}
}

func TestDataExchangeSchemaExcludesSharedWorkerScopeTable(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql", "postgres"} {
		t.Run(driver, func(t *testing.T) {
			engine, err := persistenceengine.NewEngine(driver)
			if err != nil {
				t.Fatal(err)
			}
			migrations, err := SchemaMigrations(engine, "tenant")
			if err != nil {
				t.Fatal(err)
			}
			for _, migration := range migrations {
				if strings.Contains(migration.SQL, "_data_exchange_queue_scopes") {
					t.Fatalf("owner-specific queue scope table remains in %s", migration.ID)
				}
				if strings.Contains(migration.SQL, "_worker_scopes") || migration.ID == "data_exchange_worker_scopes_v1" {
					t.Fatalf("Data Exchange still owns shared Worker Scope schema in %s", migration.ID)
				}
			}
		})
	}
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
