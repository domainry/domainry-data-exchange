package module

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
)

const sourceChunkBytes = 1 << 20

type sqlStore struct {
	db               *sql.DB
	driver, prefix   string
	workspaceContext func(context.Context, string, string) context.Context
}

func newSQLStore(db *sql.DB, driver, schema string, workspaceContext func(context.Context, string, string) context.Context) *sqlStore {
	prefix := ""
	if strings.TrimSpace(schema) != "" && driver != "sqlite" {
		prefix = strings.TrimSpace(schema) + "."
	}
	return &sqlStore{db: db, driver: strings.ToLower(driver), prefix: prefix, workspaceContext: workspaceContext}
}
func (s *sqlStore) scoped(ctx context.Context, workspace, actor string) context.Context {
	if s.workspaceContext != nil {
		return s.workspaceContext(ctx, workspace, actor)
	}
	return ctx
}
func (s *sqlStore) table(name string) string { return s.prefix + name }
func (s *sqlStore) placeholder(i int) string {
	if s.driver == "postgres" || s.driver == "pgx" {
		return fmt.Sprintf("$%d", i)
	}
	return "?"
}
func marks(s *sqlStore, n int) string {
	p := make([]string, n)
	for i := range p {
		p[i] = s.placeholder(i + 1)
	}
	return strings.Join(p, ",")
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

func (s *sqlStore) registerScope(ctx context.Context, executor sqlExecer, workspace string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	query := `INSERT INTO ` + s.table("data_exchange_queue_scopes") + ` (scope_key,updated_at) VALUES (` + marks(s, 2) + `)`
	if s.driver == "mysql" {
		query += ` ON DUPLICATE KEY UPDATE updated_at=VALUES(updated_at)`
	} else {
		query += ` ON CONFLICT(scope_key) DO UPDATE SET updated_at=excluded.updated_at`
	}
	_, err := executor.ExecContext(ctx, query, workspace, now)
	return err
}

func (s *sqlStore) submitImport(ctx context.Context, r dataexchange.ImportRequest) (dataexchange.Job, bool, error) {
	ctx = s.scoped(ctx, r.Scope.WorkspaceID, r.Scope.ActorID)
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
			q := `INSERT INTO ` + s.table("data_exchange_chunks") + ` (workspace_id,job_id,direction,sequence_no,content,content_sha256,created_at) VALUES (` + marks(s, 7) + `)`
			if _, err = tx.ExecContext(ctx, q, r.Scope.WorkspaceID, stagingID, "source", chunks, string(part), hex.EncodeToString(digest[:]), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
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
	payload, _ := json.Marshal(map[string]any{"filename": r.Filename, "content_type": r.ContentType, "max_bytes": limit})
	requestHash := fingerprint([]byte(r.Provider), []byte(r.ObjectKey), payload, []byte(sourceHash))
	var existingHash string
	lookup := `SELECT request_sha256 FROM ` + s.table("data_exchange_jobs") + ` WHERE workspace_id=` + s.placeholder(1) + ` AND provider=` + s.placeholder(2) + ` AND operation='import' AND object_key=` + s.placeholder(3) + ` AND idempotency_key=` + s.placeholder(4)
	lookupErr := tx.QueryRowContext(ctx, lookup, r.Scope.WorkspaceID, r.Provider, r.ObjectKey, r.IdempotencyKey).Scan(&existingHash)
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
	q := `INSERT INTO ` + s.table("data_exchange_jobs") + ` (id,workspace_id,provider,operation,object_key,idempotency_key,request_sha256,request_payload,status,source_sha256,source_bytes,source_chunks,actor_id,role_key,created_at,updated_at) VALUES (` + marks(s, 16) + `)`
	if _, err = tx.ExecContext(ctx, q, id, r.Scope.WorkspaceID, r.Provider, "import", r.ObjectKey, r.IdempotencyKey, requestHash, string(payload), "queued", sourceHash, total, chunks, r.Scope.ActorID, r.Scope.RoleKey, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
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
	move := `UPDATE ` + s.table("data_exchange_chunks") + ` SET job_id=` + s.placeholder(1) + ` WHERE workspace_id=` + s.placeholder(2) + ` AND job_id=` + s.placeholder(3) + ` AND direction='source'`
	if _, err = tx.ExecContext(ctx, move, id, r.Scope.WorkspaceID, stagingID); err != nil {
		return dataexchange.Job{}, false, err
	}
	if err = s.registerScope(ctx, tx, r.Scope.WorkspaceID); err != nil {
		return dataexchange.Job{}, false, err
	}
	if err = tx.Commit(); err != nil {
		return dataexchange.Job{}, false, err
	}
	return dataexchange.Job{ID: id, Provider: r.Provider, Operation: "import", Status: "queued", WorkspaceID: r.Scope.WorkspaceID, ObjectKey: r.ObjectKey, ActorID: r.Scope.ActorID, RoleKey: r.Scope.RoleKey, CreatedAt: now, UpdatedAt: now}, false, nil
}

func (s *sqlStore) requestHash(ctx context.Context, id string) (string, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT request_sha256 FROM `+s.table("data_exchange_jobs")+` WHERE id=`+s.placeholder(1), id).Scan(&value)
	return value, err
}

func (s *sqlStore) submitExport(ctx context.Context, r dataexchange.ExportRequest) (dataexchange.Job, bool, error) {
	ctx = s.scoped(ctx, r.Scope.WorkspaceID, r.Scope.ActorID)
	id := jobID(r.Scope, r.Provider, "export", r.ObjectKey, r.IdempotencyKey)
	payload := r.Options
	if payload == nil {
		payload = []byte{}
	}
	hash := fingerprint([]byte(r.Provider), []byte(r.ObjectKey), payload)
	now := time.Now().UTC()
	tx, beginErr := s.db.BeginTx(ctx, nil)
	if beginErr != nil {
		return dataexchange.Job{}, false, beginErr
	}
	defer tx.Rollback()
	q := `INSERT INTO ` + s.table("data_exchange_jobs") + ` (id,workspace_id,provider,operation,object_key,idempotency_key,request_sha256,request_payload,status,actor_id,role_key,created_at,updated_at) VALUES (` + marks(s, 13) + `)`
	_, err := tx.ExecContext(ctx, q, id, r.Scope.WorkspaceID, r.Provider, "export", r.ObjectKey, r.IdempotencyKey, hash, string(payload), "queued", r.Scope.ActorID, r.Scope.RoleKey, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	if err != nil {
		_ = tx.Rollback()
		if j, ok := s.lookupIdempotent(ctx, r.Scope, r.Provider, "export", r.ObjectKey, r.IdempotencyKey); ok {
			var existing string
			_ = s.db.QueryRowContext(ctx, `SELECT request_sha256 FROM `+s.table("data_exchange_jobs")+` WHERE id=`+s.placeholder(1), j.ID).Scan(&existing)
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
	return dataexchange.Job{ID: id, Provider: r.Provider, Operation: "export", Status: "queued", WorkspaceID: r.Scope.WorkspaceID, ObjectKey: r.ObjectKey, ActorID: r.Scope.ActorID, RoleKey: r.Scope.RoleKey, Options: append([]byte(nil), payload...), CreatedAt: now, UpdatedAt: now}, false, nil
}

type scanner interface{ Scan(...any) error }

func scanJob(row scanner) (dataexchange.Job, error) {
	var j dataexchange.Job
	var created, updated string
	var options []byte
	err := row.Scan(&j.ID, &j.Provider, &j.Operation, &j.Status, &j.Checkpoint, &j.Cursor, &j.Total, &j.ResultChunks, &j.ArtifactID, &j.ErrorCode, &created, &updated, &j.WorkspaceID, &j.ObjectKey, &j.ActorID, &j.RoleKey, &options)
	if err != nil {
		return j, err
	}
	j.Options = append([]byte(nil), options...)
	j.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
	j.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)
	return j, nil
}
func (s *sqlStore) lookupIdempotent(ctx context.Context, scope dataexchange.Scope, provider, operation, objectKey, key string) (dataexchange.Job, bool) {
	q := `SELECT id,provider,operation,status,checkpoint_value,checkpoint_cursor,total_value,result_chunks,artifact_id,error_code,created_at,updated_at,workspace_id,object_key,actor_id,role_key,request_payload FROM ` + s.table("data_exchange_jobs") + ` WHERE workspace_id=` + s.placeholder(1) + ` AND provider=` + s.placeholder(2) + ` AND operation=` + s.placeholder(3) + ` AND object_key=` + s.placeholder(4) + ` AND idempotency_key=` + s.placeholder(5)
	j, e := scanJob(s.db.QueryRowContext(ctx, q, scope.WorkspaceID, provider, operation, objectKey, key))
	return j, e == nil
}
func (s *sqlStore) job(ctx context.Context, r dataexchange.JobRequest) (dataexchange.Job, error) {
	ctx = s.scoped(ctx, r.Scope.WorkspaceID, r.Scope.ActorID)
	q := `SELECT id,provider,operation,status,checkpoint_value,checkpoint_cursor,total_value,result_chunks,artifact_id,error_code,created_at,updated_at,workspace_id,object_key,actor_id,role_key,request_payload FROM ` + s.table("data_exchange_jobs") + ` WHERE id=` + s.placeholder(1) + ` AND workspace_id=` + s.placeholder(2) + ` AND actor_id=` + s.placeholder(3)
	return scanJob(s.db.QueryRowContext(ctx, q, r.JobID, r.Scope.WorkspaceID, r.Scope.ActorID))
}
func (s *sqlStore) cancel(ctx context.Context, r dataexchange.JobRequest) (dataexchange.Job, error) {
	ctx = s.scoped(ctx, r.Scope.WorkspaceID, r.Scope.ActorID)
	q := `UPDATE ` + s.table("data_exchange_jobs") + ` SET status='cancelled',updated_at=` + s.placeholder(1) + ` WHERE id=` + s.placeholder(2) + ` AND workspace_id=` + s.placeholder(3) + ` AND actor_id=` + s.placeholder(4) + ` AND status IN ('queued','running')`
	if _, e := s.db.ExecContext(ctx, q, time.Now().UTC().Format(time.RFC3339Nano), r.JobID, r.Scope.WorkspaceID, r.Scope.ActorID); e != nil {
		return dataexchange.Job{}, e
	}
	return s.job(ctx, r)
}

type workItem struct {
	Job       dataexchange.Job
	Scope     dataexchange.Scope
	ObjectKey string
	Payload   []byte
}

func (s *sqlStore) claim(ctx context.Context, owner string, ttl time.Duration) (workItem, bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT scope_key FROM `+s.table("data_exchange_queue_scopes")+` ORDER BY updated_at,scope_key`)
	if err != nil {
		return workItem{}, false, err
	}
	workspaces := make([]string, 0)
	for rows.Next() {
		var workspace string
		if err = rows.Scan(&workspace); err != nil {
			_ = rows.Close()
			return workItem{}, false, err
		}
		workspaces = append(workspaces, workspace)
	}
	if err = rows.Close(); err != nil {
		return workItem{}, false, err
	}
	for _, workspace := range workspaces {
		item, found, claimErr := s.claimWorkspace(s.scoped(ctx, workspace, owner), workspace, owner, ttl)
		if claimErr != nil {
			return workItem{}, false, claimErr
		}
		if found {
			return item, true, nil
		}
	}
	return workItem{}, false, nil
}

func (s *sqlStore) claimWorkspace(ctx context.Context, workspace, owner string, ttl time.Duration) (workItem, bool, error) {
	var x workItem
	var created, updated string
	now := time.Now().UTC()
	q := `SELECT id,provider,operation,status,checkpoint_value,checkpoint_cursor,total_value,result_chunks,fencing_token,artifact_id,error_code,created_at,updated_at,workspace_id,actor_id,role_key,object_key,request_payload FROM ` + s.table("data_exchange_jobs") + ` WHERE workspace_id=` + s.placeholder(1) + ` AND (status='queued' OR (status='running' AND lease_expires_at<>'' AND lease_expires_at<` + s.placeholder(2) + `)) ORDER BY created_at LIMIT 1`
	err := s.db.QueryRowContext(ctx, q, workspace, now.Format(time.RFC3339Nano)).Scan(&x.Job.ID, &x.Job.Provider, &x.Job.Operation, &x.Job.Status, &x.Job.Checkpoint, &x.Job.Cursor, &x.Job.Total, &x.Job.ResultChunks, &x.Job.FencingToken, &x.Job.ArtifactID, &x.Job.ErrorCode, &created, &updated, &x.Scope.WorkspaceID, &x.Scope.ActorID, &x.Scope.RoleKey, &x.ObjectKey, &x.Payload)
	if errors.Is(err, sql.ErrNoRows) {
		return x, false, nil
	}
	if err != nil {
		return x, false, err
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	u := `UPDATE ` + s.table("data_exchange_jobs") + ` SET status='running',lease_owner=` + s.placeholder(1) + `,lease_expires_at=` + s.placeholder(2) + `,fencing_token=fencing_token+1,updated_at=` + s.placeholder(3) + ` WHERE id=` + s.placeholder(4) + ` AND workspace_id=` + s.placeholder(5) + ` AND (status='queued' OR (status='running' AND lease_expires_at<>'' AND lease_expires_at<` + s.placeholder(6) + `))`
	res, err := s.db.ExecContext(ctx, u, owner, now.Add(ttl).Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), x.Job.ID, workspace, now.Format(time.RFC3339Nano))
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
func (s *sqlStore) chunks(ctx context.Context, workspace, job, direction string) (io.ReadCloser, error) {
	ctx = s.scoped(ctx, workspace, "data-exchange-worker")
	q := `SELECT content FROM ` + s.table("data_exchange_chunks") + ` WHERE workspace_id=` + s.placeholder(1) + ` AND job_id=` + s.placeholder(2) + ` AND direction=` + s.placeholder(3) + ` ORDER BY sequence_no`
	rows, e := s.db.QueryContext(ctx, q, workspace, job, direction)
	if e != nil {
		return nil, e
	}
	return streamRows(rows)
}
func (s *sqlStore) resultIdentity(ctx context.Context, workspace, job string) (string, int64, error) {
	reader, err := s.chunks(ctx, workspace, job, "result")
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
func (s *sqlStore) commitResultPage(ctx context.Context, x workItem, seq int, content []byte, nextCursor string, checkpoint, total int) error {
	ctx = s.scoped(ctx, x.Scope.WorkspaceID, "data-exchange-worker")
	d := sha256.Sum256(content)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	q := `INSERT INTO ` + s.table("data_exchange_chunks") + ` (workspace_id,job_id,direction,sequence_no,content,content_sha256,created_at) VALUES (` + marks(s, 7) + `)`
	if _, err = tx.ExecContext(ctx, q, x.Scope.WorkspaceID, x.Job.ID, "result", seq, string(content), hex.EncodeToString(d[:]), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	u := `UPDATE ` + s.table("data_exchange_jobs") + ` SET checkpoint_value=` + s.placeholder(1) + `,checkpoint_cursor=` + s.placeholder(2) + `,total_value=` + s.placeholder(3) + `,result_chunks=` + s.placeholder(4) + `,updated_at=` + s.placeholder(5) + ` WHERE id=` + s.placeholder(6) + ` AND status='running' AND lease_owner=` + s.placeholder(7) + ` AND fencing_token=` + s.placeholder(8)
	result, err := tx.ExecContext(ctx, u, checkpoint, nextCursor, total, seq+1, time.Now().UTC().Format(time.RFC3339Nano), x.Job.ID, x.Job.LeaseOwner, x.Job.FencingToken)
	if err != nil {
		return err
	}
	updated, _ := result.RowsAffected()
	if updated != 1 {
		return fmt.Errorf("Data Exchange job %s fencing token is stale", x.Job.ID)
	}
	return tx.Commit()
}
func (s *sqlStore) progress(ctx context.Context, x workItem, checkpoint, total int) error {
	q := `UPDATE ` + s.table("data_exchange_jobs") + ` SET checkpoint_value=` + s.placeholder(1) + `,total_value=` + s.placeholder(2) + `,updated_at=` + s.placeholder(3) + ` WHERE id=` + s.placeholder(4) + ` AND status='running' AND lease_owner=` + s.placeholder(5) + ` AND fencing_token=` + s.placeholder(6)
	result, e := s.db.ExecContext(ctx, q, checkpoint, total, time.Now().UTC().Format(time.RFC3339Nano), x.Job.ID, x.Job.LeaseOwner, x.Job.FencingToken)
	if e != nil {
		return e
	}
	updated, _ := result.RowsAffected()
	if updated != 1 {
		return fmt.Errorf("Data Exchange job %s fencing token is stale", x.Job.ID)
	}
	return nil
}

type artifactRecord struct {
	ID, Filename, ContentType, SHA256 string
	Size                              int64
	ExpiresAt                         time.Time
}

func (s *sqlStore) complete(ctx context.Context, x workItem, a *artifactRecord) error {
	tx, e := s.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	aid := ""
	if a != nil {
		aid = a.ID
		q := `INSERT INTO ` + s.table("data_exchange_artifacts") + ` (id,workspace_id,job_id,filename,content_type,content_sha256,size_bytes,expires_at,created_at) VALUES (` + marks(s, 9) + `)`
		if _, e = tx.ExecContext(ctx, q, a.ID, x.Scope.WorkspaceID, x.Job.ID, a.Filename, a.ContentType, a.SHA256, a.Size, a.ExpiresAt.Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano)); e != nil {
			return e
		}
	}
	q := `UPDATE ` + s.table("data_exchange_jobs") + ` SET status='completed',artifact_id=` + s.placeholder(1) + `,lease_owner='',lease_expires_at='',updated_at=` + s.placeholder(2) + ` WHERE id=` + s.placeholder(3) + ` AND status='running' AND lease_owner=` + s.placeholder(4) + ` AND fencing_token=` + s.placeholder(5)
	result, e := tx.ExecContext(ctx, q, aid, time.Now().UTC().Format(time.RFC3339Nano), x.Job.ID, x.Job.LeaseOwner, x.Job.FencingToken)
	if e != nil {
		return e
	}
	updated, _ := result.RowsAffected()
	if updated != 1 {
		return fmt.Errorf("Data Exchange job %s is no longer running", x.Job.ID)
	}
	return tx.Commit()
}
func (s *sqlStore) fail(ctx context.Context, x workItem, code string) error {
	q := `UPDATE ` + s.table("data_exchange_jobs") + ` SET status='failed',error_code=` + s.placeholder(1) + `,lease_owner='',lease_expires_at='',updated_at=` + s.placeholder(2) + ` WHERE id=` + s.placeholder(3) + ` AND status='running' AND lease_owner=` + s.placeholder(4) + ` AND fencing_token=` + s.placeholder(5)
	_, e := s.db.ExecContext(ctx, q, code, time.Now().UTC().Format(time.RFC3339Nano), x.Job.ID, x.Job.LeaseOwner, x.Job.FencingToken)
	return e
}

func (s *sqlStore) heartbeat(ctx context.Context, x workItem, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	q := `UPDATE ` + s.table("data_exchange_jobs") + ` SET lease_expires_at=` + s.placeholder(1) + `,updated_at=` + s.placeholder(2) + ` WHERE id=` + s.placeholder(3) + ` AND status='running' AND lease_owner=` + s.placeholder(4) + ` AND fencing_token=` + s.placeholder(5)
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, q, now.Add(ttl).Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), x.Job.ID, x.Job.LeaseOwner, x.Job.FencingToken)
	if err != nil {
		return err
	}
	updated, _ := result.RowsAffected()
	if updated != 1 {
		return fmt.Errorf("Data Exchange job %s fencing token is stale", x.Job.ID)
	}
	return nil
}
func (s *sqlStore) artifact(ctx context.Context, r dataexchange.JobRequest) (dataexchange.Artifact, error) {
	ctx = s.scoped(ctx, r.Scope.WorkspaceID, r.Scope.ActorID)
	var a dataexchange.Artifact
	var expiresAt string
	q := `SELECT a.id,a.filename,a.content_type,a.content_sha256,a.size_bytes,a.expires_at FROM ` + s.table("data_exchange_artifacts") + ` a JOIN ` + s.table("data_exchange_jobs") + ` j ON j.id=a.job_id WHERE j.id=` + s.placeholder(1) + ` AND j.workspace_id=` + s.placeholder(2) + ` AND j.actor_id=` + s.placeholder(3) + ` AND j.status='completed'`
	if e := s.db.QueryRowContext(ctx, q, r.JobID, r.Scope.WorkspaceID, r.Scope.ActorID).Scan(&a.ID, &a.Filename, &a.ContentType, &a.SHA256, &a.Size, &expiresAt); e != nil {
		return a, e
	}
	a.ExpiresAt, _ = time.Parse(time.RFC3339Nano, expiresAt)
	content, e := s.chunks(ctx, r.Scope.WorkspaceID, r.JobID, "result")
	if e != nil {
		return a, e
	}
	a.Content = content
	return a, nil
}
