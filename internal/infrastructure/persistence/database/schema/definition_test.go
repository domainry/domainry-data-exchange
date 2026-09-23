package schema

import (
	"strings"
	"testing"

	persistenceengine "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence"
)

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
