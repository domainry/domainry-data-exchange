package module

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence"
)

func TestReleasedMigrationChecksumsRemainStable(t *testing.T) {
	expected := map[string][]string{
		"sqlite":   {"59a453de3c4edd50956b63a390cbc2a750304bb6b76476a51c199e61a703c218", "17900a2e42cddfceafdf3275b811786fd34d53be0b500ac74388cae1e5540a6c", "b2de24b34599c0b9630080708104a1b7bb5efd34a947beaa9ff7ed4b537612dd", "d4b49e99ad2cce10d6eb6d174355b15b1059add659eb0709e0ae05afb2441510", "61a134fdb7bd7ef90cfc2a5872f0569ad6df6f7668f02877e626641683a11f02"},
		"mysql":    {"4701fb1114dd5fd006eba5452dfcb8a045f65e24f65a6383d50d082880158598", "c6fb49a2c6da0a507ee351da5b3de0f159c117c64f9722aac67ab2a615199001", "7187a4cc6084117e75604a73e8c4b4db4e09c38800f171f897134da18b1d3797", "a1db73d7604fed7676b69b9539796510bd9e3106b9582278413bb8ce1528c373", "ccc1596fb203792da9f6d1844c1b22a1551d138a53f626b14bc0c48d3b9b51ab"},
		"postgres": {"fbbe8edfb02a106da05b13849a981f82c5abfd7c7ac857225550de3759df8a8e", "4170bda0096b51baa2ddeb4b3b54c317a14ae0691816c9b909d83cc9c832f6e1", "afbf6dbdf1848d86eb5f9ce48ef03d82f93dfa3f01b37d4954cd16e0703884b9", "3e934a14ee76896cd35dd92804112d6855fa7246fe1e6e2466a2370852389578", "6533ecbfab2856faf35d9e7cb61a2ebf4ca6311323e63ea2432ca680604fcc93"},
	}
	for driver, checksums := range expected {
		engine, err := persistence.NewEngine(driver)
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
	engine, err := persistence.NewEngine("mysql")
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
		{"sqlite", `"data_exchange_jobs"`, `"attempt_count"`},
		{"postgres", `"data_exchange_jobs"`, `"attempt_count"`},
		{"mysql", "`data_exchange_jobs`", "`attempt_count`"},
	}
	for _, test := range tests {
		t.Run(test.driver, func(t *testing.T) {
			engine, err := persistence.NewEngine(test.driver)
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
