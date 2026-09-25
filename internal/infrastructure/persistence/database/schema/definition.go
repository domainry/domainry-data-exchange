package schema

import (
	"fmt"

	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	persistenceengine "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence"
	"github.com/domainry/domainry-foundation/schemaownership"
	ormschema "github.com/domainry/domainry-orm/schema"
)

const (
	MigrationOwner     = "data_exchange"
	JobsTableName      = "_data_exchange_jobs"
	JobChunksTableName = "_data_exchange_job_chunks"
)

// SchemaMigrations describes the canonical fresh Data Exchange schema. Shared
// Artifact and Worker Scope tables remain owned and installed by Foundation.
func SchemaMigrations(engine persistenceengine.Engine, schema string) ([]modulehost.Migration, error) {
	if engine == nil {
		return nil, fmt.Errorf("Data Exchange database engine is required")
	}
	renderer := engine.Dialect().WithSchema(schema)
	jobs, _, err := jobsTable(renderer).Build()
	if err != nil {
		return nil, fmt.Errorf("build Data Exchange jobs table: %w", err)
	}
	chunks, _, err := jobChunksTable(renderer).Build()
	if err != nil {
		return nil, fmt.Errorf("build Data Exchange job chunks table: %w", err)
	}
	return []modulehost.Migration{
		{ID: "data_exchange_jobs_v1", SQL: jobs},
		{ID: "data_exchange_job_chunks_v1", SQL: chunks},
	}, nil
}

func jobsTable(renderer ormschema.Renderer) *ormschema.TableBuilder {
	return ormschema.NewTable(renderer, JobsTableName).Columns(
		required("id", ormschema.TextKey(191)),
		required("workspace_id", ormschema.TextKey(191)),
		required("provider", ormschema.TextKey(191)),
		required("operation", ormschema.TextKey(191)),
		required("object_key", ormschema.TextKey(191)),
		required("idempotency_key", ormschema.TextKey(191)),
		required("request_sha256", ormschema.TextKey(64)),
		required("request_payload", ormschema.LongText()),
		required("status", ormschema.TextKey(64)),
		required("checkpoint_value", ormschema.Integer()).DefaultValue(0),
		required("checkpoint_cursor", ormschema.TextKey(191)).DefaultValue(""),
		required("total_value", ormschema.Integer()).DefaultValue(0),
		required("result_chunks", ormschema.Integer()).DefaultValue(0),
		required("source_sha256", ormschema.TextKey(64)).DefaultValue(""),
		required("source_bytes", ormschema.BigInt()).DefaultValue(0),
		required("source_chunks", ormschema.Integer()).DefaultValue(0),
		required("artifact_id", ormschema.TextKey(191)).DefaultValue(""),
		required("error_code", ormschema.TextKey(191)).DefaultValue(""),
		required("lease_owner", ormschema.TextKey(191)).DefaultValue(""),
		required("lease_expires_at", ormschema.BigInt()).DefaultValue(0),
		required("fencing_token", ormschema.BigInt()).DefaultValue(0),
		required("actor_id", ormschema.TextKey(191)),
		required("role_key", ormschema.TextKey(191)).DefaultValue(""),
		required("created_at", ormschema.BigInt()),
		required("updated_at", ormschema.BigInt()),
		required("reference_id", ormschema.TextKey(191)).DefaultValue(""),
		required("attempt_count", ormschema.Integer()).DefaultValue(0),
		required("next_attempt_at", ormschema.BigInt()).DefaultValue(0),
	).PrimaryKey("id")
}

func jobChunksTable(renderer ormschema.Renderer) *ormschema.TableBuilder {
	return ormschema.NewTable(renderer, JobChunksTableName).Columns(
		required("workspace_id", ormschema.TextKey(191)),
		required("job_id", ormschema.TextKey(191)),
		required("direction", ormschema.TextKey(32)),
		required("sequence_no", ormschema.Integer()),
		required("content", ormschema.LongText()),
		required("content_sha256", ormschema.TextKey(64)),
		required("created_at", ormschema.BigInt()),
	).PrimaryKey("workspace_id", "job_id", "direction", "sequence_no")
}

func SchemaOwnership() []schemaownership.Table {
	return []schemaownership.Table{
		{
			Name: JobsTableName, Owner: MigrationOwner, WorkspaceScope: schemaownership.ScopeWorkspace,
			RetentionClass: schemaownership.RetentionUserErase, PrimaryKey: []string{"id"},
			BoundedQueryPath: "workspace plus job identity for point reads; actor-scoped history and worker claim paths enforce limits",
			DeletionPolicy:   "subject erasure redacts the durable job record, removes payload and personal references, and retains only non-personal execution history",
		},
		{
			Name: JobChunksTableName, Owner: MigrationOwner, WorkspaceScope: schemaownership.ScopeWorkspace,
			RetentionClass: schemaownership.RetentionUserErase, PrimaryKey: []string{"workspace_id", "job_id", "direction", "sequence_no"},
			BoundedQueryPath: "workspace, job and direction primary-key prefix; content is streamed in sequence order instead of materialized",
			DeletionPolicy:   "subject erasure physically deletes every source and result chunk for the subject's jobs; artifact expiry may make retained result content inaccessible",
		},
	}
}

func OwnedTables() []string { return schemaownership.Names(SchemaOwnership()) }

func required(name string, kind ormschema.ColumnType) ormschema.ColumnDefinition {
	return ormschema.Column(name, kind).NotNull()
}
