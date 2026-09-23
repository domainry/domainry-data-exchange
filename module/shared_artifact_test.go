package module

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	sharedartifact "github.com/domainry/domainry-foundation/artifact"
)

// testArtifactStore is host-owned test infrastructure. Data Exchange module
// migrations deliberately do not create either shared Artifact table.
type testArtifactStore struct{ db *sql.DB }

func newTestArtifactStore(t *testing.T, db *sql.DB) *testArtifactStore {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS _artifacts (
workspace_id TEXT NOT NULL, id TEXT PRIMARY KEY, owner TEXT NOT NULL, kind TEXT NOT NULL,
idempotency_key TEXT NOT NULL, created_by TEXT NOT NULL, filename TEXT NOT NULL,
media_type TEXT NOT NULL, content_sha256 TEXT NOT NULL, size_bytes BIGINT NOT NULL,
storage_reference TEXT NOT NULL, status TEXT NOT NULL, expires_at TEXT NOT NULL DEFAULT '',
scan_status TEXT NOT NULL, download_token_sha256 TEXT NOT NULL DEFAULT '',
authorization_scope_sha256 TEXT NOT NULL DEFAULT '', metadata_json TEXT NOT NULL,
created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
UNIQUE(workspace_id, owner, kind, idempotency_key))`,
		`CREATE TABLE IF NOT EXISTS _artifact_bindings (
workspace_id TEXT NOT NULL, id TEXT PRIMARY KEY, artifact_id TEXT NOT NULL, owner TEXT NOT NULL,
kind TEXT NOT NULL, resource_type TEXT NOT NULL, resource_id TEXT NOT NULL,
field_key TEXT NOT NULL DEFAULT '', metadata_json TEXT NOT NULL, created_at TEXT NOT NULL,
UNIQUE(workspace_id, artifact_id, owner, kind, resource_type, resource_id, field_key))`,
	} {
		if _, err := db.ExecContext(t.Context(), statement); err != nil {
			t.Fatal(err)
		}
	}
	return &testArtifactStore{db: db}
}

func (s *testArtifactStore) executor(ctx context.Context) sharedartifact.Executor {
	return sharedartifact.ExecutorFromContext(ctx, s.db)
}

func (s *testArtifactStore) Register(ctx context.Context, value sharedartifact.Artifact) (sharedartifact.Artifact, bool, error) {
	_, err := s.executor(ctx).ExecContext(ctx, `INSERT INTO _artifacts (
workspace_id,id,owner,kind,idempotency_key,created_by,filename,media_type,content_sha256,size_bytes,
storage_reference,status,expires_at,scan_status,download_token_sha256,authorization_scope_sha256,
metadata_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		value.WorkspaceID, value.ID, value.Owner, value.Kind, value.IdempotencyKey, value.CreatedBy,
		value.Filename, value.MediaType, value.ContentSHA256, value.SizeBytes, value.StorageReference,
		string(value.Status), formatArtifactTestTime(value.ExpiresAt), string(value.ScanStatus),
		value.DownloadTokenSHA256, value.AuthorizationScopeSHA256, string(value.Metadata),
		value.CreatedAt.UTC().Format(time.RFC3339Nano), value.UpdatedAt.UTC().Format(time.RFC3339Nano))
	if err == nil {
		return value, true, nil
	}
	existing, found, readErr := s.byIdempotency(ctx, value.WorkspaceID, value.Owner, value.Kind, value.IdempotencyKey)
	if readErr != nil {
		return sharedartifact.Artifact{}, false, readErr
	}
	if !found || existing.ID != value.ID || existing.ContentSHA256 != value.ContentSHA256 || existing.SizeBytes != value.SizeBytes || existing.StorageReference != value.StorageReference {
		return sharedartifact.Artifact{}, false, sharedartifact.ErrIdentityConflict
	}
	return existing, false, nil
}

func (s *testArtifactStore) ByID(ctx context.Context, workspace, id string) (sharedartifact.Artifact, bool, error) {
	return s.find(ctx, `workspace_id=? AND id=?`, workspace, id)
}

func (s *testArtifactStore) ByDownloadTokenHash(ctx context.Context, workspace, token string) (sharedartifact.Artifact, bool, error) {
	return s.find(ctx, `workspace_id=? AND download_token_sha256=?`, workspace, token)
}

func (s *testArtifactStore) byIdempotency(ctx context.Context, workspace, owner, kind, key string) (sharedartifact.Artifact, bool, error) {
	return s.find(ctx, `workspace_id=? AND owner=? AND kind=? AND idempotency_key=?`, workspace, owner, kind, key)
}

func (s *testArtifactStore) find(ctx context.Context, predicate string, arguments ...any) (sharedartifact.Artifact, bool, error) {
	row := s.executor(ctx).QueryRowContext(ctx, `SELECT workspace_id,id,owner,kind,idempotency_key,created_by,filename,media_type,content_sha256,size_bytes,storage_reference,status,expires_at,scan_status,download_token_sha256,authorization_scope_sha256,metadata_json,created_at,updated_at FROM _artifacts WHERE `+predicate, arguments...)
	value, err := scanTestArtifact(row)
	if errors.Is(err, sql.ErrNoRows) {
		return sharedartifact.Artifact{}, false, nil
	}
	return value, err == nil, err
}

func (s *testArtifactStore) Transition(ctx context.Context, workspace, id string, expected, next sharedartifact.Status, scan sharedartifact.ScanStatus, at time.Time) (bool, error) {
	result, err := s.executor(ctx).ExecContext(ctx, `UPDATE _artifacts SET status=?,scan_status=?,updated_at=? WHERE workspace_id=? AND id=? AND status=?`, string(next), string(scan), at.UTC().Format(time.RFC3339Nano), workspace, id, string(expected))
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count == 1, err
}

func (s *testArtifactStore) Bind(ctx context.Context, value sharedartifact.Binding) (sharedartifact.Binding, bool, error) {
	_, err := s.executor(ctx).ExecContext(ctx, `INSERT INTO _artifact_bindings (workspace_id,id,artifact_id,owner,kind,resource_type,resource_id,field_key,metadata_json,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)`, value.WorkspaceID, value.ID, value.ArtifactID, value.Owner, value.Kind, value.ResourceType, value.ResourceID, value.FieldKey, string(value.Metadata), value.CreatedAt.UTC().Format(time.RFC3339Nano))
	if err == nil {
		return value, true, nil
	}
	values, readErr := s.Bindings(ctx, value.WorkspaceID, value.ArtifactID)
	if readErr != nil {
		return sharedartifact.Binding{}, false, readErr
	}
	for _, existing := range values {
		if existing.Owner == value.Owner && existing.Kind == value.Kind && existing.ResourceType == value.ResourceType && existing.ResourceID == value.ResourceID && existing.FieldKey == value.FieldKey {
			return existing, false, nil
		}
	}
	return sharedartifact.Binding{}, false, sharedartifact.ErrBindingConflict
}

func (s *testArtifactStore) Bindings(ctx context.Context, workspace, artifactID string) ([]sharedartifact.Binding, error) {
	rows, err := s.executor(ctx).QueryContext(ctx, `SELECT workspace_id,id,artifact_id,owner,kind,resource_type,resource_id,field_key,metadata_json,created_at FROM _artifact_bindings WHERE workspace_id=? AND artifact_id=? ORDER BY created_at,id`, workspace, artifactID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []sharedartifact.Binding{}
	for rows.Next() {
		var value sharedartifact.Binding
		var metadata, createdAt string
		if err = rows.Scan(&value.WorkspaceID, &value.ID, &value.ArtifactID, &value.Owner, &value.Kind, &value.ResourceType, &value.ResourceID, &value.FieldKey, &metadata, &createdAt); err != nil {
			return nil, err
		}
		value.Metadata = json.RawMessage(metadata)
		value.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

type artifactTestScanner interface{ Scan(...any) error }

func scanTestArtifact(row artifactTestScanner) (sharedartifact.Artifact, error) {
	var value sharedartifact.Artifact
	var status, scanStatus, expiresAt, metadata, createdAt, updatedAt string
	err := row.Scan(&value.WorkspaceID, &value.ID, &value.Owner, &value.Kind, &value.IdempotencyKey, &value.CreatedBy, &value.Filename, &value.MediaType, &value.ContentSHA256, &value.SizeBytes, &value.StorageReference, &status, &expiresAt, &scanStatus, &value.DownloadTokenSHA256, &value.AuthorizationScopeSHA256, &metadata, &createdAt, &updatedAt)
	if err != nil {
		return value, err
	}
	value.Status, value.ScanStatus, value.Metadata = sharedartifact.Status(status), sharedartifact.ScanStatus(scanStatus), json.RawMessage(metadata)
	if expiresAt != "" {
		value.ExpiresAt, err = time.Parse(time.RFC3339Nano, expiresAt)
	}
	if err == nil {
		value.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	}
	if err == nil {
		value.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt)
	}
	return value, err
}

func formatArtifactTestTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

var _ sharedartifact.Store = (*testArtifactStore)(nil)
