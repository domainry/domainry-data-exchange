package module

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	persistenceengine "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence"
	persistence "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence/database/schema"
)

func TestReleasedMigrationChecksumsRemainStable(t *testing.T) {
	expected := map[string][]string{
		"sqlite":   {"f7b5da7ffbe57206ef4974545576047f0d81876183728218f10da2b3186fcea1", "58da2f808f2c33f492bb3d6fe7263dcc40cd7b28c17fe0ba97fe7067722b29a3", "51f469844558e045957271470bf3fb94d18549c10b76974e22fbaad33afece17", "407ac7fde574ac30b257d0f23dfe203dcc5fe044d9c4599a7ba2cb4a895745ea", "903240de4e44136067afdbbd6fb86ea7e115c5b765e2c8884d4cb0783a91c1de"},
		"mysql":    {"bdd63c9319bae1bd963a621802032c82952bf6fa1b7c534310ac415ddb736e40", "e0a622ab0303250e19644f58b9b84b93c3b60e7f22e30cf152ee3e55ec66540c", "e67b2bf1e422c2f181e38b3bb21fb3f153b78b979e98a9ecca70323eb67ee035", "dc86f32ed60b638d4b32b9f4a99304a4694999d0b49de98ebe29465b864a9c74", "adea63defd1f5387ccefc6ae333485e0f364a45a21d8b68797c9b72eaee03551"},
		"postgres": {"9cfb56abbc80532e98770c09f38c2c159a542c98a8c9627adb03d509ff9ce0e4", "6c9b396575e96196e114c21b15ae099455a46fd700d692ad53a351b87b33838e", "bdbaa924893d6356d4c1dbe2a80a63aba172947cd03d97a9e16fe6153b20f5e5", "ab775172313c54b6562ae2568de9e2480ebe2f0870d557899703f45c4276c11f", "5072e7ac933e9d4f7261b9cb1eea3a8e545db1e043b7bdc3f8244a8ef80e8c44"},
	}
	for driver, checksums := range expected {
		engine, err := persistenceengine.NewEngine(driver)
		if err != nil {
			t.Fatal(err)
		}
		migrations, err := persistence.SchemaMigrations(engine, "tenant")
		if err != nil {
			t.Fatal(err)
		}
		for index, want := range checksums {
			got := fmt.Sprintf("%x", sha256.Sum256([]byte(migrations[index].SQL)))
			if got != want {
				t.Errorf("%s migration %s checksum=%s want=%s", driver, migrations[index].ID, got, want)
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

func TestNewRecoveryMigrationsUseDialectQuotedORMDDL(t *testing.T) {
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
			statement := migrations[len(migrations)-2].SQL
			if !strings.Contains(statement, test.quotedTable) || !strings.Contains(statement, test.quotedColumn) {
				t.Fatalf("ORM DDL=%q", statement)
			}
		})
	}
}
