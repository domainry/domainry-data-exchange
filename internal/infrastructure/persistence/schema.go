package persistence

import (
	"fmt"
	"strings"

	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	ormbuilder "github.com/domainry/domainry-orm/builder"
	ormdialect "github.com/domainry/domainry-orm/dialect"
)

// SchemaMigrations preserves the byte identity of the released v1/v2 raw DDL
// because the host migration ledger checksums source migrations. Re-rendering
// those historical statements through the ORM would corrupt existing ledgers.
// Every new migration below is built with domainry-orm.
func SchemaMigrations(driver, schema string) ([]modulehost.Migration, error) {
	driver = strings.ToLower(strings.TrimSpace(driver))
	if driver != "sqlite" && driver != "mysql" && driver != "postgres" && driver != "pgx" {
		return nil, fmt.Errorf("Data Exchange database driver %q is unsupported", driver)
	}
	text := "TEXT"
	key := text
	large := "TEXT"
	if driver == "mysql" {
		text, key, large = "VARCHAR(512)", "VARCHAR(191)", "LONGTEXT"
	}
	prefix := ""
	if strings.TrimSpace(schema) != "" && driver != "sqlite" {
		prefix = strings.TrimSpace(schema) + "."
	}
	jobs := `CREATE TABLE IF NOT EXISTS ` + prefix + `data_exchange_jobs (` +
		`id ` + key + ` PRIMARY KEY, workspace_id ` + key + ` NOT NULL, provider ` + key + ` NOT NULL, operation ` + key + ` NOT NULL, object_key ` + key + ` NOT NULL, ` +
		`idempotency_key ` + key + ` NOT NULL, request_sha256 ` + key + ` NOT NULL, request_payload ` + large + ` NOT NULL, status ` + key + ` NOT NULL, checkpoint_value INTEGER NOT NULL DEFAULT 0, checkpoint_cursor ` + key + ` NOT NULL DEFAULT '', total_value INTEGER NOT NULL DEFAULT 0, result_chunks INTEGER NOT NULL DEFAULT 0, ` +
		`source_sha256 ` + key + ` NOT NULL DEFAULT '', source_bytes BIGINT NOT NULL DEFAULT 0, source_chunks INTEGER NOT NULL DEFAULT 0, artifact_id ` + key + ` NOT NULL DEFAULT '', error_code ` + key + ` NOT NULL DEFAULT '', ` +
		`lease_owner ` + key + ` NOT NULL DEFAULT '', lease_expires_at ` + key + ` NOT NULL DEFAULT '', fencing_token BIGINT NOT NULL DEFAULT 0, ` +
		`actor_id ` + key + ` NOT NULL, role_key ` + key + ` NOT NULL DEFAULT '', created_at ` + key + ` NOT NULL, updated_at ` + key + ` NOT NULL)`
	chunks := `CREATE TABLE IF NOT EXISTS ` + prefix + `data_exchange_chunks (` +
		`workspace_id ` + key + ` NOT NULL, job_id ` + key + ` NOT NULL, direction ` + key + ` NOT NULL, sequence_no INTEGER NOT NULL, content ` + large + ` NOT NULL, content_sha256 ` + key + ` NOT NULL, created_at ` + key + ` NOT NULL, ` +
		`PRIMARY KEY(workspace_id, job_id, direction, sequence_no))`
	artifacts := `CREATE TABLE IF NOT EXISTS ` + prefix + `data_exchange_artifacts (` +
		`id ` + key + ` PRIMARY KEY, workspace_id ` + key + ` NOT NULL, job_id ` + key + ` NOT NULL, filename ` + text + ` NOT NULL, content_type ` + text + ` NOT NULL, content_sha256 ` + key + ` NOT NULL, size_bytes BIGINT NOT NULL, expires_at ` + key + ` NOT NULL, created_at ` + key + ` NOT NULL)`
	queueScopes := `CREATE TABLE IF NOT EXISTS ` + prefix + `data_exchange_queue_scopes (scope_key ` + key + ` PRIMARY KEY, updated_at ` + key + ` NOT NULL)`
	ownerReference := `ALTER TABLE ` + prefix + `data_exchange_jobs ADD COLUMN reference_id ` + key + ` NOT NULL DEFAULT ''`
	renderer, err := ormdialect.ParseRenderer(driver, schema, "")
	if err != nil {
		return nil, err
	}
	attempts, _, err := ormbuilder.NewAddColumnBuilder(renderer, "data_exchange_jobs", ormbuilder.DefineColumn("attempt_count", ormbuilder.IntegerType()).NotNull().DefaultValue(0)).Build()
	if err != nil {
		return nil, err
	}
	nextAttempt, _, err := ormbuilder.NewAddColumnBuilder(renderer, "data_exchange_jobs", ormbuilder.DefineColumn("next_attempt_at", ormbuilder.TextKeyType(191)).NotNull().DefaultValue("")).Build()
	if err != nil {
		return nil, err
	}
	return []modulehost.Migration{
		{ID: "data_exchange_jobs_v1", SQL: jobs}, {ID: "data_exchange_chunks_v1", SQL: chunks}, {ID: "data_exchange_artifacts_v1", SQL: artifacts},
		{ID: "data_exchange_queue_scopes_v1", SQL: queueScopes}, {ID: "data_exchange_job_owner_reference_v2", SQL: ownerReference},
		{ID: "data_exchange_job_attempt_count_v3", SQL: attempts}, {ID: "data_exchange_job_next_attempt_v4", SQL: nextAttempt},
	}, nil
}
