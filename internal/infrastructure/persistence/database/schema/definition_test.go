package schema

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	persistenceengine "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence"
)

func TestWorkerScopesReplaceOwnerSpecificQueueScopeTable(t *testing.T) {
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
				if migration.ID == "data_exchange_worker_scopes_v1" {
					for _, column := range []string{"owner", "scope_key", "cursor", "checkpoint", "capacity", "lease_owner", "lease_expires_at", "fencing_token", "updated_at"} {
						if !strings.Contains(migration.SQL, column) {
							t.Fatalf("worker scope column %q missing from %s", column, migration.SQL)
						}
					}
					t.Logf("checksum=%s", fmt.Sprintf("%x", sha256.Sum256([]byte(migration.SQL))))
					return
				}
			}
			t.Fatal("shared worker scope migration is missing")
		})
	}
}
