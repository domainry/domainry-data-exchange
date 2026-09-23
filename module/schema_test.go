package module

import (
	"crypto/sha256"
	"fmt"
	"slices"
	"strings"
	"testing"

	persistenceengine "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence"
	persistence "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence/database/schema"
	"github.com/domainry/domainry-foundation/schemaownership"
)

func TestCurrentMigrationChecksumsAreDeterministic(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql", "postgres"} {
		engine, err := persistenceengine.NewEngine(driver)
		if err != nil {
			t.Fatal(err)
		}
		migrations, err := persistence.SchemaMigrations(engine, "tenant")
		if err != nil {
			t.Fatal(err)
		}
		repeated, err := persistence.SchemaMigrations(engine, "tenant")
		if err != nil {
			t.Fatal(err)
		}
		if len(migrations) != 2 || len(repeated) != len(migrations) {
			t.Fatalf("%s migration count=%d repeat=%d", driver, len(migrations), len(repeated))
		}
		for index := range migrations {
			got := fmt.Sprintf("%x", sha256.Sum256([]byte(migrations[index].SQL)))
			want := fmt.Sprintf("%x", sha256.Sum256([]byte(repeated[index].SQL)))
			if migrations[index].ID != repeated[index].ID || got != want {
				t.Errorf("%s migration %d is not deterministic", driver, index)
			}
		}
	}
}

func TestMySQLSchemaUsesIndexSafeIdentityColumns(t *testing.T) {
	engine, err := persistenceengine.NewEngine("mysql")
	if err != nil {
		t.Fatal(err)
	}
	migrations, err := persistence.SchemaMigrations(engine, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations {
		if strings.Contains(migration.SQL, "PRIMARY KEY") && !strings.Contains(migration.SQL, "VARCHAR(191)") {
			t.Fatalf("migration %s lacks index-safe keys", migration.ID)
		}
	}
	if strings.Contains(migrations[0].SQL, "UNIQUE(workspace_id") {
		t.Fatal("redundant oversized idempotency index remains")
	}
}

func TestCanonicalFreshSchemaIncludesFinalColumnsWithoutAlterMigrations(t *testing.T) {
	tests := []struct {
		driver, quotedTable, quotedColumn string
	}{
		{"sqlite", `"_data_exchange_jobs"`, `"attempt_count"`},
		{"postgres", `"_data_exchange_jobs"`, `"attempt_count"`},
		{"mysql", "`_data_exchange_jobs`", "`attempt_count`"},
	}
	for _, test := range tests {
		t.Run(test.driver, func(t *testing.T) {
			engine, err := persistenceengine.NewEngine(test.driver)
			if err != nil {
				t.Fatal(err)
			}
			migrations, err := persistence.SchemaMigrations(engine, "")
			if err != nil {
				t.Fatal(err)
			}
			var jobs string
			for _, migration := range migrations {
				if strings.Contains(strings.ToUpper(migration.SQL), "ALTER TABLE") {
					t.Fatalf("historical ALTER migration remains: %s", migration.ID)
				}
				if migration.ID == "data_exchange_jobs_v1" {
					jobs = migration.SQL
				}
			}
			if !strings.Contains(jobs, test.quotedTable) || !strings.Contains(jobs, test.quotedColumn) {
				t.Fatalf("ORM DDL=%q", jobs)
			}
			for _, column := range []string{"reference_id", "attempt_count", "next_attempt_at"} {
				if !strings.Contains(jobs, column) {
					t.Fatalf("canonical jobs DDL lacks %s: %s", column, jobs)
				}
			}
		})
	}
}

func TestModulePublishesOnlyDataExchangeOwnedSchema(t *testing.T) {
	tables := SchemaOwnership()
	if err := schemaownership.ValidateAll(tables); err != nil {
		t.Fatal(err)
	}
	if len(tables) != 2 || !slices.Equal(OwnedTables(), schemaownership.Names(tables)) {
		t.Fatalf("Data Exchange schema ownership=%d tables=%v", len(tables), OwnedTables())
	}
	for _, table := range tables {
		if table.Owner != persistence.MigrationOwner || !strings.HasPrefix(table.Name, "_data_exchange_") {
			t.Fatalf("Data Exchange Module claimed foreign table: %+v", table)
		}
	}
}

func TestSchemaOwnsNoSubjectLifecycleFenceOrReceipt(t *testing.T) {
	for _, driver := range []string{"sqlite", "mysql", "postgres"} {
		engine, err := persistenceengine.NewEngine(driver)
		if err != nil {
			t.Fatal(err)
		}
		migrations, err := persistence.SchemaMigrations(engine, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, migration := range migrations {
			for _, retired := range []string{"_data_exchange_subject_erasure_fences", "_data_exchange_subject_erasure_receipts", "_data_exchange_artifacts"} {
				if strings.Contains(migration.SQL, retired) {
					t.Fatalf("%s retained retired storage %s in %s", driver, retired, migration.ID)
				}
			}
		}
	}
}
