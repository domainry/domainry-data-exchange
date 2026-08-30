package database

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	persistenceengine "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence"
	ormdialect "github.com/domainry/domainry-orm/dialect"
	ormbuilder "github.com/domainry/domainry-orm/query"
)

const sourceChunkBytes = 1 << 20
const maxProcessingAttempts = 3
const retryInitialDelay = time.Second

type Store struct {
	db               *sql.DB
	engine           persistenceengine.Engine
	renderer         ormdialect.Renderer
	workspaceContext func(context.Context, string, string) context.Context
}

func NewStore(db *sql.DB, engine persistenceengine.Engine, schema string, workspaceContext func(context.Context, string, string) context.Context) (*Store, error) {
	if engine == nil {
		return nil, fmt.Errorf("Data Exchange database engine is required")
	}
	return &Store{db: db, engine: engine, renderer: engine.Dialect().WithSchema(schema), workspaceContext: workspaceContext}, nil
}
func (s *Store) Scoped(ctx context.Context, workspace, actor string) context.Context {
	if s.workspaceContext != nil {
		return s.workspaceContext(ctx, workspace, actor)
	}
	return ctx
}
func jobID(scope dataexchange.Scope, provider, operation, objectKey, key string) string {
	d := sha256.Sum256([]byte(scope.WorkspaceID + "\x00" + provider + "\x00" + operation + "\x00" + objectKey + "\x00" + key))
	return "data_exchange:" + hex.EncodeToString(d[:12])
}
func fingerprint(parts ...[]byte) string {
	h := sha256.New()
	for _, p := range parts {
		_, _ = h.Write(p)
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

type sqlExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

type statementBuilder interface {
	Build() (string, []any, error)
}

func execute(ctx context.Context, executor sqlExecer, statement statementBuilder) (sql.Result, error) {
	query, args, err := statement.Build()
	if err != nil {
		return nil, err
	}
	return executor.ExecContext(ctx, query, args...)
}

func (s *Store) registerScope(ctx context.Context, executor sqlExecer, workspace string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	insert := ormbuilder.NewInsertBuilder(s.renderer, "_data_exchange_queue_scopes").
		Columns("scope_key", "updated_at").Values(workspace, now)
	insert, err := s.engine.ApplyUpsert(insert, []string{"scope_key"}, ormbuilder.Assign("updated_at", now))
	if err != nil {
		return err
	}
	_, err = execute(ctx, executor, insert)
	return err
}

type importRequestPayload struct {
	Filename    string          `json:"filename"`
	ContentType string          `json:"content_type"`
	MaxBytes    int64           `json:"max_bytes"`
	Options     json.RawMessage `json:"options,omitempty"`
}

func (s *Store) SubmitImport(ctx context.Context, r dataexchange.ImportRequest) (dataexchange.Job, bool, error) {
	ctx = s.Scoped(ctx, r.Scope.WorkspaceID, r.Scope.ActorID)
	id := jobID(r.Scope, r.Provider, "import", r.ObjectKey, r.IdempotencyKey)
	stagingID := fmt.Sprintf("%s:staging:%d", id, time.Now().UTC().UnixNano())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return dataexchange.Job{}, false, err
	}
	defer tx.Rollback()
	limit := r.MaxBytes
	if limit <= 0 {
		limit = 128 << 20
	}
	h := sha256.New()
	buf := make([]byte, sourceChunkBytes)
	var total int64
	chunks := 0
	for {
		n, readErr := r.Source.Read(buf)
		if n > 0 {
			total += int64(n)
			if total > limit {
				return dataexchange.Job{}, false, fmt.Errorf("%w: %d bytes", dataexchange.ErrSourceTooLarge, limit)
			}
			part := append([]byte(nil), buf[:n]...)
			_, _ = h.Write(part)
			digest := sha256.Sum256(part)
			insert := ormbuilder.NewInsertBuilder(s.renderer, "_data_exchange_job_chunks").
				Columns("workspace_id", "job_id", "direction", "sequence_no", "content", "content_sha256", "created_at").
				Values(r.Scope.WorkspaceID, stagingID, "source", chunks, part, hex.EncodeToString(digest[:]), time.Now().UTC().Format(time.RFC3339Nano))
			if _, err = execute(ctx, tx, insert); err != nil {
				return dataexchange.Job{}, false, err
			}
			chunks++
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return dataexchange.Job{}, false, fmt.Errorf("%w: %v", dataexchange.ErrSourceUnreadable, readErr)
		}
	}
	sourceHash := hex.EncodeToString(h.Sum(nil))
	payload, err := json.Marshal(importRequestPayload{Filename: r.Filename, ContentType: r.ContentType, MaxBytes: limit, Options: append(json.RawMessage(nil), r.Options...)})
	if err != nil {
		return dataexchange.Job{}, false, fmt.Errorf("encode Data Exchange import request: %w", err)
	}
	requestHash := fingerprint([]byte(r.Provider), []byte(r.ObjectKey), payload, []byte(sourceHash))
	var existingHash string
	lookup, lookupArgs, buildErr := ormbuilder.NewSelectBuilder(s.renderer, "_data_exchange_jobs").Columns("request_sha256").Where(ormbuilder.And(
		ormbuilder.Equal("workspace_id", r.Scope.WorkspaceID), ormbuilder.Equal("provider", r.Provider), ormbuilder.Equal("operation", "import"),
		ormbuilder.Equal("object_key", r.ObjectKey), ormbuilder.Equal("idempotency_key", r.IdempotencyKey),
	)).Build()
	if buildErr != nil {
		return dataexchange.Job{}, false, buildErr
	}
	lookupErr := tx.QueryRowContext(ctx, lookup, lookupArgs...).Scan(&existingHash)
	if lookupErr == nil {
		if existingHash != requestHash {
			return dataexchange.Job{}, false, fmt.Errorf("Data Exchange idempotency key reused with a different source")
		}
		_ = tx.Rollback()
		existing, ok := s.lookupIdempotent(ctx, r.Scope, r.Provider, "import", r.ObjectKey, r.IdempotencyKey)
		if !ok {
			return dataexchange.Job{}, false, fmt.Errorf("Data Exchange idempotent import disappeared")
		}
		return existing, true, nil
	}
	if !errors.Is(lookupErr, sql.ErrNoRows) {
		return dataexchange.Job{}, false, lookupErr
	}
	now := time.Now().UTC()
	insertJob := ormbuilder.NewInsertBuilder(s.renderer, "_data_exchange_jobs").
		Columns("id", "workspace_id", "provider", "operation", "object_key", "idempotency_key", "request_sha256", "request_payload", "status", "source_sha256", "source_bytes", "source_chunks", "actor_id", "role_key", "created_at", "updated_at").
		Values(id, r.Scope.WorkspaceID, r.Provider, "import", r.ObjectKey, r.IdempotencyKey, requestHash, payload, "queued", sourceHash, total, chunks, r.Scope.ActorID, r.Scope.RoleKey, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	if _, err = execute(ctx, tx, insertJob); err != nil {
		_ = tx.Rollback()
		if existing, ok := s.lookupIdempotent(ctx, r.Scope, r.Provider, "import", r.ObjectKey, r.IdempotencyKey); ok {
			hash, hashErr := s.requestHash(ctx, existing.ID)
			if hashErr == nil && hash == requestHash {
				return existing, true, nil
			}
			return dataexchange.Job{}, false, fmt.Errorf("Data Exchange idempotency key reused with a different source")
		}
		return dataexchange.Job{}, false, err
	}
	move := ormbuilder.NewUpdateBuilder(s.renderer, "_data_exchange_job_chunks").Set("job_id", id).Where(ormbuilder.And(
		ormbuilder.Equal("workspace_id", r.Scope.WorkspaceID), ormbuilder.Equal("job_id", stagingID), ormbuilder.Equal("direction", "source"),
	))
	if _, err = execute(ctx, tx, move); err != nil {
		return dataexchange.Job{}, false, err
	}
	if err = s.registerScope(ctx, tx, r.Scope.WorkspaceID); err != nil {
		return dataexchange.Job{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return dataexchange.Job{}, false, err
	}
	return dataexchange.Job{ID: id, Provider: r.Provider, Operation: "import", Status: "queued", WorkspaceID: r.Scope.WorkspaceID, ObjectKey: r.ObjectKey, ActorID: r.Scope.ActorID, RoleKey: r.Scope.RoleKey, Options: append([]byte(nil), r.Options...), CreatedAt: now, UpdatedAt: now}, false, nil
}

func (s *Store) requestHash(ctx context.Context, id string) (string, error) {
	var value string
	query, args, err := ormbuilder.NewSelectBuilder(s.renderer, "_data_exchange_jobs").Columns("request_sha256").Where(ormbuilder.Equal("id", id)).Build()
	if err != nil {
		return "", err
	}
	err = s.db.QueryRowContext(ctx, query, args...).Scan(&value)
	return value, err
}

func (s *Store) SubmitExport(ctx context.Context, r dataexchange.ExportRequest) (dataexchange.Job, bool, error) {
	ctx = s.Scoped(ctx, r.Scope.WorkspaceID, r.Scope.ActorID)
	id := jobID(r.Scope, r.Provider, "export", r.ObjectKey, r.IdempotencyKey)
	payload := r.Options
	if payload == nil {
		payload = []byte{}
	}
	hash := fingerprint([]byte(r.Provider), []byte(r.ObjectKey), []byte(strings.TrimSpace(r.ReferenceID)), payload)
	now := time.Now().UTC()
	tx, beginErr := s.db.BeginTx(ctx, nil)
	if beginErr != nil {
		return dataexchange.Job{}, false, beginErr
	}
	defer tx.Rollback()
	insertJob := ormbuilder.NewInsertBuilder(s.renderer, "_data_exchange_jobs").
		Columns("id", "workspace_id", "provider", "operation", "object_key", "idempotency_key", "request_sha256", "request_payload", "status", "actor_id", "role_key", "reference_id", "created_at", "updated_at").
		Values(id, r.Scope.WorkspaceID, r.Provider, "export", r.ObjectKey, r.IdempotencyKey, hash, payload, "queued", r.Scope.ActorID, r.Scope.RoleKey, strings.TrimSpace(r.ReferenceID), now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	_, err := execute(ctx, tx, insertJob)
	if err != nil {
		_ = tx.Rollback()
		if j, ok := s.lookupIdempotent(ctx, r.Scope, r.Provider, "export", r.ObjectKey, r.IdempotencyKey); ok {
			existing, _ := s.requestHash(ctx, j.ID)
			if existing != hash {
				return dataexchange.Job{}, false, fmt.Errorf("Data Exchange idempotency key reused with a different request")
			}
			return j, true, nil
		}
		return dataexchange.Job{}, false, err
	}
	if err = s.registerScope(ctx, tx, r.Scope.WorkspaceID); err != nil {
		return dataexchange.Job{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return dataexchange.Job{}, false, err
	}
	return dataexchange.Job{ID: id, Provider: r.Provider, Operation: "export", Status: "queued", WorkspaceID: r.Scope.WorkspaceID, ObjectKey: r.ObjectKey, ActorID: r.Scope.ActorID, RoleKey: r.Scope.RoleKey, ReferenceID: strings.TrimSpace(r.ReferenceID), Options: append([]byte(nil), payload...), CreatedAt: now, UpdatedAt: now}, false, nil
}

type scanner interface{ Scan(...any) error }

var jobColumns = []string{"id", "provider", "operation", "status", "checkpoint_value", "checkpoint_cursor", "total_value", "result_chunks", "artifact_id", "error_code", "created_at", "updated_at", "workspace_id", "object_key", "actor_id", "role_key", "reference_id", "request_payload"}

func scanJob(row scanner) (dataexchange.Job, error) {
	var j dataexchange.Job
	var created, updated string
	var options []byte
	err := row.Scan(&j.ID, &j.Provider, &j.Operation, &j.Status, &j.Checkpoint, &j.Cursor, &j.Total, &j.ResultChunks, &j.ArtifactID, &j.ErrorCode, &created, &updated, &j.WorkspaceID, &j.ObjectKey, &j.ActorID, &j.RoleKey, &j.ReferenceID, &options)
	if err != nil {
		return j, err
	}
	j.Options = append([]byte(nil), options...)
	if j.Operation == "import" {
		var payload importRequestPayload
		if json.Unmarshal(options, &payload) == nil {
			j.Options = append([]byte(nil), payload.Options...)
		}
	}
	j.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	j.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
	return j, nil
}
func (s *Store) lookupIdempotent(ctx context.Context, scope dataexchange.Scope, provider, operation, objectKey, key string) (dataexchange.Job, bool) {
	query, args, err := ormbuilder.NewSelectBuilder(s.renderer, "_data_exchange_jobs").Columns(jobColumns...).Where(ormbuilder.And(
		ormbuilder.Equal("workspace_id", scope.WorkspaceID), ormbuilder.Equal("provider", provider), ormbuilder.Equal("operation", operation),
		ormbuilder.Equal("object_key", objectKey), ormbuilder.Equal("idempotency_key", key),
	)).Build()
	if err != nil {
		return dataexchange.Job{}, false
	}
	j, e := scanJob(s.db.QueryRowContext(ctx, query, args...))
	return j, e == nil
}
func (s *Store) Job(ctx context.Context, r dataexchange.JobRequest) (dataexchange.Job, error) {
	ctx = s.Scoped(ctx, r.Scope.WorkspaceID, r.Scope.ActorID)
	query, args, err := ormbuilder.NewSelectBuilder(s.renderer, "_data_exchange_jobs").Columns(jobColumns...).Where(ormbuilder.And(
		ormbuilder.Equal("id", r.JobID), ormbuilder.Equal("workspace_id", r.Scope.WorkspaceID), ormbuilder.Equal("actor_id", r.Scope.ActorID),
	)).Build()
	if err != nil {
		return dataexchange.Job{}, err
	}
	return scanJob(s.db.QueryRowContext(ctx, query, args...))
}
func (s *Store) Cancel(ctx context.Context, r dataexchange.JobRequest) (dataexchange.Job, error) {
	ctx = s.Scoped(ctx, r.Scope.WorkspaceID, r.Scope.ActorID)
	update := ormbuilder.NewUpdateBuilder(s.renderer, "_data_exchange_jobs").Set("status", "cancelled").Set("updated_at", time.Now().UTC().Format(time.RFC3339Nano)).Where(ormbuilder.And(
		ormbuilder.Equal("id", r.JobID), ormbuilder.Equal("workspace_id", r.Scope.WorkspaceID), ormbuilder.Equal("actor_id", r.Scope.ActorID), ormbuilder.In("status", "queued", "running"),
	))
	if _, e := execute(ctx, s.db, update); e != nil {
		return dataexchange.Job{}, e
	}
	return s.Job(ctx, r)
}

type WorkItem struct {
	Job       dataexchange.Job
	Scope     dataexchange.Scope
	ObjectKey string
	Payload   []byte
	Attempts  int
}

func (s *Store) Claim(ctx context.Context, owner string, ttl time.Duration) (WorkItem, bool, error) {
	query, args, err := ormbuilder.NewSelectBuilder(s.renderer, "_data_exchange_queue_scopes").Columns("scope_key").OrderBy(ormbuilder.Ascending("updated_at"), ormbuilder.Ascending("scope_key")).Build()
	if err != nil {
		return WorkItem{}, false, err
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return WorkItem{}, false, err
	}
	workspaces := make([]string, 0)
	for rows.Next() {
		var workspace string
		if err = rows.Scan(&workspace); err != nil {
			_ = rows.Close()
			return WorkItem{}, false, err
		}
		workspaces = append(workspaces, workspace)
	}
	if err = rows.Close(); err != nil {
		return WorkItem{}, false, err
	}
	for _, workspace := range workspaces {
		item, found, claimErr := s.claimWorkspace(s.Scoped(ctx, workspace, owner), workspace, owner, ttl)
		if claimErr != nil {
			return WorkItem{}, false, claimErr
		}
		if found {
			return item, true, nil
		}
	}
	return WorkItem{}, false, nil
}

func (s *Store) claimWorkspace(ctx context.Context, workspace, owner string, ttl time.Duration) (WorkItem, bool, error) {
	var x WorkItem
	var created, updated string
	now := time.Now().UTC()
	readyQueued := ormbuilder.And(ormbuilder.Equal("status", "queued"), ormbuilder.Or(
		ormbuilder.Equal("next_attempt_at", ""), ormbuilder.LessThan("next_attempt_at", now.Format(time.RFC3339Nano)),
	))
	claimable := ormbuilder.Or(
		readyQueued,
		ormbuilder.And(ormbuilder.Equal("status", "running"), ormbuilder.NotEqual("lease_expires_at", ""), ormbuilder.LessThan("lease_expires_at", now.Format(time.RFC3339Nano))),
	)
	query, args, buildErr := ormbuilder.NewSelectBuilder(s.renderer, "_data_exchange_jobs").
		Columns("id", "provider", "operation", "status", "checkpoint_value", "checkpoint_cursor", "total_value", "result_chunks", "fencing_token", "artifact_id", "error_code", "created_at", "updated_at", "workspace_id", "actor_id", "role_key", "reference_id", "object_key", "request_payload", "attempt_count").
		Where(ormbuilder.And(ormbuilder.Equal("workspace_id", workspace), claimable)).OrderBy(ormbuilder.Ascending("created_at")).Limit(1).Build()
	if buildErr != nil {
		return x, false, buildErr
	}
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&x.Job.ID, &x.Job.Provider, &x.Job.Operation, &x.Job.Status, &x.Job.Checkpoint, &x.Job.Cursor, &x.Job.Total, &x.Job.ResultChunks, &x.Job.FencingToken, &x.Job.ArtifactID, &x.Job.ErrorCode, &created, &updated, &x.Scope.WorkspaceID, &x.Scope.ActorID, &x.Scope.RoleKey, &x.Job.ReferenceID, &x.ObjectKey, &x.Payload, &x.Attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return x, false, nil
	}
	if err != nil {
		return x, false, err
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	update := ormbuilder.NewUpdateBuilder(s.renderer, "_data_exchange_jobs").
		Set("status", "running").Set("lease_owner", owner).Set("lease_expires_at", now.Add(ttl).Format(time.RFC3339Nano)).
		Set("next_attempt_at", "").
		SetExpression("fencing_token", ormbuilder.Add(ormbuilder.Column("fencing_token"), ormbuilder.Value(1))).
		Set("updated_at", now.Format(time.RFC3339Nano)).
		Where(ormbuilder.And(ormbuilder.Equal("id", x.Job.ID), ormbuilder.Equal("workspace_id", workspace), claimable))
	res, err := execute(ctx, s.db, update)
	if err != nil {
		return x, false, err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return x, false, nil
	}
	x.Job.Status = "running"
	x.Job.FencingToken++
	x.Job.LeaseOwner = owner
	return x, true, nil
}

func streamRows(rows *sql.Rows) (io.ReadCloser, error) {
	pr, pw := io.Pipe()
	go func() {
		defer rows.Close()
		for rows.Next() {
			var b []byte
			if e := rows.Scan(&b); e != nil {
				_ = pw.CloseWithError(e)
				return
			}
			if _, e := pw.Write(b); e != nil {
				return
			}
		}
		_ = pw.CloseWithError(rows.Err())
	}()
	return pr, nil
}
func (s *Store) Chunks(ctx context.Context, workspace, job, direction string) (io.ReadCloser, error) {
	ctx = s.Scoped(ctx, workspace, "data-exchange-worker")
	query, args, e := ormbuilder.NewSelectBuilder(s.renderer, "_data_exchange_job_chunks").Columns("content").Where(ormbuilder.And(
		ormbuilder.Equal("workspace_id", workspace), ormbuilder.Equal("job_id", job), ormbuilder.Equal("direction", direction),
	)).OrderBy(ormbuilder.Ascending("sequence_no")).Build()
	if e != nil {
		return nil, e
	}
	rows, e := s.db.QueryContext(ctx, query, args...)
	if e != nil {
		return nil, e
	}
	return streamRows(rows)
}
func (s *Store) ResultIdentity(ctx context.Context, workspace, job string) (string, int64, error) {
	reader, err := s.Chunks(ctx, workspace, job, "result")
	if err != nil {
		return "", 0, err
	}
	defer reader.Close()
	h := sha256.New()
	size, err := io.Copy(h, reader)
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), size, nil
}
func (s *Store) CommitResultPage(ctx context.Context, x WorkItem, seq int, content []byte, nextCursor string, checkpoint, total int) error {
	ctx = s.Scoped(ctx, x.Scope.WorkspaceID, "data-exchange-worker")
	d := sha256.Sum256(content)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	insert := ormbuilder.NewInsertBuilder(s.renderer, "_data_exchange_job_chunks").
		Columns("workspace_id", "job_id", "direction", "sequence_no", "content", "content_sha256", "created_at").
		Values(x.Scope.WorkspaceID, x.Job.ID, "result", seq, content, hex.EncodeToString(d[:]), time.Now().UTC().Format(time.RFC3339Nano))
	if _, err = execute(ctx, tx, insert); err != nil {
		return err
	}
	update := ormbuilder.NewUpdateBuilder(s.renderer, "_data_exchange_jobs").
		Set("checkpoint_value", checkpoint).Set("checkpoint_cursor", nextCursor).Set("total_value", total).Set("result_chunks", seq+1).
		Set("updated_at", time.Now().UTC().Format(time.RFC3339Nano)).Where(fencedJob(x))
	result, err := execute(ctx, tx, update)
	if err != nil {
		return err
	}
	updated, _ := result.RowsAffected()
	if updated != 1 {
		return fmt.Errorf("Data Exchange job %s fencing token is stale", x.Job.ID)
	}
	return tx.Commit()
}
func (s *Store) Progress(ctx context.Context, x WorkItem, checkpoint, total int) error {
	update := ormbuilder.NewUpdateBuilder(s.renderer, "_data_exchange_jobs").Set("checkpoint_value", checkpoint).Set("total_value", total).
		Set("updated_at", time.Now().UTC().Format(time.RFC3339Nano)).Where(fencedJob(x))
	result, e := execute(ctx, s.db, update)
	if e != nil {
		return e
	}
	updated, _ := result.RowsAffected()
	if updated != 1 {
		return fmt.Errorf("Data Exchange job %s fencing token is stale", x.Job.ID)
	}
	return nil
}

func fencedJob(x WorkItem) ormbuilder.Predicate {
	return ormbuilder.And(ormbuilder.Equal("id", x.Job.ID), ormbuilder.Equal("status", "running"), ormbuilder.Equal("lease_owner", x.Job.LeaseOwner), ormbuilder.Equal("fencing_token", x.Job.FencingToken))
}

type ArtifactRecord struct {
	ID, Filename, ContentType, SHA256 string
	Size                              int64
	ExpiresAt                         time.Time
}

func (s *Store) Complete(ctx context.Context, x WorkItem, a *ArtifactRecord) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	aid := ""
	if a != nil {
		aid = a.ID
		insert := ormbuilder.NewInsertBuilder(s.renderer, "_data_exchange_artifacts").
			Columns("id", "workspace_id", "job_id", "filename", "content_type", "content_sha256", "size_bytes", "expires_at", "created_at").
			Values(a.ID, x.Scope.WorkspaceID, x.Job.ID, a.Filename, a.ContentType, a.SHA256, a.Size, a.ExpiresAt.Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano))
		if _, e = execute(ctx, tx, insert); e != nil {
			return e
		}
	}
	update := ormbuilder.NewUpdateBuilder(s.renderer, "_data_exchange_jobs").Set("status", "completed").Set("artifact_id", aid).
		Set("lease_owner", "").Set("lease_expires_at", "").Set("updated_at", time.Now().UTC().Format(time.RFC3339Nano)).Where(fencedJob(x))
	result, e := execute(ctx, tx, update)
	if e != nil {
		return e
	}
	updated, _ := result.RowsAffected()
	if updated != 1 {
		return fmt.Errorf("Data Exchange job %s is no longer running", x.Job.ID)
	}
	return tx.Commit()
}
func (s *Store) Fail(ctx context.Context, x WorkItem, code string) error {
	now := time.Now().UTC()
	attempts := x.Attempts + 1
	status, next := "failed", ""
	if attempts < maxProcessingAttempts {
		status = "queued"
		delay := retryInitialDelay * time.Duration(1<<x.Attempts)
		next = now.Add(delay).Format(time.RFC3339Nano)
	}
	update := ormbuilder.NewUpdateBuilder(s.renderer, "_data_exchange_jobs").Set("status", status).Set("error_code", code).
		Set("attempt_count", attempts).Set("next_attempt_at", next).Set("lease_owner", "").Set("lease_expires_at", "").
		Set("updated_at", now.Format(time.RFC3339Nano)).Where(fencedJob(x))
	result, err := execute(ctx, s.db, update)
	if err != nil {
		return err
	}
	updated, _ := result.RowsAffected()
	if updated != 1 {
		return fmt.Errorf("Data Exchange job %s fencing token is stale", x.Job.ID)
	}
	return nil
}

func (s *Store) Heartbeat(ctx context.Context, x WorkItem, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	now := time.Now().UTC()
	update := ormbuilder.NewUpdateBuilder(s.renderer, "_data_exchange_jobs").Set("lease_expires_at", now.Add(ttl).Format(time.RFC3339Nano)).
		Set("updated_at", now.Format(time.RFC3339Nano)).Where(fencedJob(x))
	result, err := execute(ctx, s.db, update)
	if err != nil {
		return err
	}
	updated, _ := result.RowsAffected()
	if updated != 1 {
		return fmt.Errorf("Data Exchange job %s fencing token is stale", x.Job.ID)
	}
	return nil
}
func (s *Store) Artifact(ctx context.Context, r dataexchange.JobRequest) (dataexchange.Artifact, error) {
	ctx = s.Scoped(ctx, r.Scope.WorkspaceID, r.Scope.ActorID)
	var a dataexchange.Artifact
	var expiresAt string
	query, args, buildErr := ormbuilder.NewSelectBuilder(s.renderer, "_data_exchange_artifacts").Alias("a").Projections(
		ormbuilder.Project(ormbuilder.QualifiedColumn("a", "id")), ormbuilder.Project(ormbuilder.QualifiedColumn("a", "filename")),
		ormbuilder.Project(ormbuilder.QualifiedColumn("a", "content_type")), ormbuilder.Project(ormbuilder.QualifiedColumn("a", "content_sha256")),
		ormbuilder.Project(ormbuilder.QualifiedColumn("a", "size_bytes")), ormbuilder.Project(ormbuilder.QualifiedColumn("a", "expires_at")),
	).Join(ormbuilder.InnerJoin("_data_exchange_jobs", "j", ormbuilder.EqualExpressions(ormbuilder.QualifiedColumn("j", "id"), ormbuilder.QualifiedColumn("a", "job_id")))).Where(ormbuilder.And(
		ormbuilder.EqualValue(ormbuilder.QualifiedColumn("j", "id"), r.JobID), ormbuilder.EqualValue(ormbuilder.QualifiedColumn("j", "workspace_id"), r.Scope.WorkspaceID),
		ormbuilder.EqualValue(ormbuilder.QualifiedColumn("j", "actor_id"), r.Scope.ActorID), ormbuilder.EqualValue(ormbuilder.QualifiedColumn("j", "status"), "completed"),
	)).Build()
	if buildErr != nil {
		return a, buildErr
	}
	if e := s.db.QueryRowContext(ctx, query, args...).Scan(&a.ID, &a.Filename, &a.ContentType, &a.SHA256, &a.Size, &expiresAt); e != nil {
		return a, e
	}
	a.ExpiresAt, _ = time.Parse(time.RFC3339Nano, expiresAt)
	content, e := s.Chunks(ctx, r.Scope.WorkspaceID, r.JobID, "result")
	if e != nil {
		return a, e
	}
	a.Content = content
	return a, nil
}
