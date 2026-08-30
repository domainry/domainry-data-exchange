package module

import (
	"strings"
	"testing"
)

func TestMySQLSchemaUsesIndexSafeIdentityColumns(t *testing.T) {
	migrations, err := schemaMigrations("mysql", "")
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
