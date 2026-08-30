package module

import (
	"strings"
	"testing"

	"github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence"
)

func TestMySQLSchemaUsesIndexSafeIdentityColumns(t *testing.T) {
	migrations, err := persistence.SchemaMigrations("mysql", "")
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

func TestNewRecoveryMigrationsUseDialectQuotedORMDDL(t *testing.T) {
	tests := []struct {
		driver, quotedTable, quotedColumn string
	}{
		{"sqlite", `"data_exchange_jobs"`, `"attempt_count"`},
		{"postgres", `"data_exchange_jobs"`, `"attempt_count"`},
		{"mysql", "`data_exchange_jobs`", "`attempt_count`"},
	}
	for _, test := range tests {
		t.Run(test.driver, func(t *testing.T) {
			migrations, err := persistence.SchemaMigrations(test.driver, "")
			if err != nil {
				t.Fatal(err)
			}
			statement := migrations[len(migrations)-2].SQL
			if !strings.Contains(statement, test.quotedTable) || !strings.Contains(statement, test.quotedColumn) {
				t.Fatalf("ORM DDL=%q", statement)
			}
		})
	}
}
