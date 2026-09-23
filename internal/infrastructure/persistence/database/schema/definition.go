package schema

import (
	"fmt"
	ormschema "github.com/domainry/domainry-orm/schema"

	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	persistenceengine "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence"
)

// SchemaMigrations describes the final pre-release owner schema. Shared
// Artifact metadata and subject lifecycle state are supplied by the host.
func SchemaMigrations(engine persistenceengine.Engine, schema string) ([]modulehost.Migration, error) {
	if engine == nil {
		return nil, fmt.Errorf("Data Exchange database engine is required")
	}
	profile := engine.HistoricalSchema()
	key, large := profile.KeyType, profile.LargeType
	jobsTable := profile.Table(schema, "_data_exchange_jobs")
	chunksTable := profile.Table(schema, "_data_exchange_job_chunks")
	jobs := `CREATE TABLE IF NOT EXISTS ` + jobsTable + ` (` +
		`id ` + key + ` PRIMARY KEY, workspace_id ` + key + ` NOT NULL, provider ` + key + ` NOT NULL, operation ` + key + ` NOT NULL, object_key ` + key + ` NOT NULL, ` +
		`idempotency_key ` + key + ` NOT NULL, request_sha256 ` + key + ` NOT NULL, request_payload ` + large + ` NOT NULL, status ` + key + ` NOT NULL, checkpoint_value INTEGER NOT NULL DEFAULT 0, checkpoint_cursor ` + key + ` NOT NULL DEFAULT '', total_value INTEGER NOT NULL DEFAULT 0, result_chunks INTEGER NOT NULL DEFAULT 0, ` +
		`source_sha256 ` + key + ` NOT NULL DEFAULT '', source_bytes BIGINT NOT NULL DEFAULT 0, source_chunks INTEGER NOT NULL DEFAULT 0, artifact_id ` + key + ` NOT NULL DEFAULT '', error_code ` + key + ` NOT NULL DEFAULT '', ` +
		`lease_owner ` + key + ` NOT NULL DEFAULT '', lease_expires_at ` + key + ` NOT NULL DEFAULT '', fencing_token BIGINT NOT NULL DEFAULT 0, ` +
		`actor_id ` + key + ` NOT NULL, role_key ` + key + ` NOT NULL DEFAULT '', created_at ` + key + ` NOT NULL, updated_at ` + key + ` NOT NULL)`
	chunks := `CREATE TABLE IF NOT EXISTS ` + chunksTable + ` (` +
		`workspace_id ` + key + ` NOT NULL, job_id ` + key + ` NOT NULL, direction ` + key + ` NOT NULL, sequence_no INTEGER NOT NULL, content ` + large + ` NOT NULL, content_sha256 ` + key + ` NOT NULL, created_at ` + key + ` NOT NULL, ` +
		`PRIMARY KEY(workspace_id, job_id, direction, sequence_no))`
	ownerReference := `ALTER TABLE ` + jobsTable + ` ADD COLUMN reference_id ` + key + ` NOT NULL DEFAULT ''`
	renderer := engine.Dialect().WithSchema(schema)
	attempts, _, err := ormschema.NewAddColumn(renderer, "_data_exchange_jobs", ormschema.Column("attempt_count", ormschema.Integer()).NotNull().DefaultValue(0)).Build()
	if err != nil {
		return nil, err
	}
	nextAttempt, _, err := ormschema.NewAddColumn(renderer, "_data_exchange_jobs", ormschema.Column("next_attempt_at", ormschema.TextKey(191)).NotNull().DefaultValue("")).Build()
	if err != nil {
		return nil, err
	}
	return []modulehost.Migration{
		{ID: "data_exchange_jobs_v1", SQL: jobs}, {ID: "data_exchange_job_chunks_v1", SQL: chunks},
		{ID: "data_exchange_job_owner_reference_v2", SQL: ownerReference},
		{ID: "data_exchange_job_attempt_count_v3", SQL: attempts}, {ID: "data_exchange_job_next_attempt_v4", SQL: nextAttempt},
	}, nil
}
