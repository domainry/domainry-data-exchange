package dataexchange

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	sdk "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-orm/query"
)

type subjectJobPlan struct {
	ID        string `json:"id"`
	Chunks    int    `json:"chunks"`
	Artifacts int    `json:"artifacts"`
}

type subjectErasurePlan struct {
	WorkspaceID string           `json:"workspace_id"`
	SubjectID   string           `json:"subject_id"`
	RequestID   string           `json:"request_id"`
	Jobs        []subjectJobPlan `json:"jobs"`
}

// A no-op upsert takes the subject row lock on every submit and prepare. The
// erasure fence and a competing submission therefore cannot both commit.
func (s *Store) lockSubjectScope(ctx context.Context, tx *sql.Tx, workspace, subject string) (string, error) {
	insert := query.NewInsertBuilder(s.renderer, "_data_exchange_subject_erasure_fences").
		Columns("workspace_id", "subject_id", "request_id").Values(workspace, subject, "")
	insert, err := s.engine.ApplyUpsert(insert, []string{"workspace_id", "subject_id"}, query.Assign("subject_id", subject))
	if err != nil {
		return "", err
	}
	if _, err = execute(ctx, tx, insert); err != nil {
		return "", err
	}
	statement, args, err := query.NewWorkspaceSelectBuilder(s.renderer, "_data_exchange_subject_erasure_fences", workspace).
		Columns("request_id").Where(query.Equal("subject_id", subject)).Build()
	if err != nil {
		return "", err
	}
	var request string
	err = tx.QueryRowContext(ctx, statement, args...).Scan(&request)
	return request, err
}

func (s *Store) checkSubjectSubmission(ctx context.Context, tx *sql.Tx, scope sdk.Scope) error {
	request, err := s.lockSubjectScope(ctx, tx, scope.WorkspaceID, scope.ActorID)
	if err != nil {
		return err
	}
	if request != "" {
		return fmt.Errorf("Data Exchange subject is fenced for erasure")
	}
	return nil
}

func (s *Store) PreviewSubject(ctx context.Context, workspace, subject string) (json.RawMessage, error) {
	ctx = s.Scoped(ctx, workspace, "subject-lifecycle")
	statement, args, err := query.NewWorkspaceSelectBuilder(s.renderer, "_data_exchange_jobs", workspace).
		Columns("id", "provider", "operation", "status").Where(query.Equal("actor_id", subject)).OrderBy(query.Ascending("id")).Build()
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []map[string]string{}
	for rows.Next() {
		var id, provider, operation, status string
		if err = rows.Scan(&id, &provider, &operation, &status); err != nil {
			return nil, err
		}
		items = append(items, map[string]string{"id": id, "provider": provider, "operation": operation, "status": status})
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"jobs": items})
}

func (s *Store) erasureReceipt(ctx context.Context, tx *sql.Tx, request sdk.SubjectErasureRequest) (string, string, error) {
	statement, args, err := query.NewWorkspaceSelectBuilder(s.renderer, "_data_exchange_subject_erasure_receipts", request.WorkspaceID).
		Columns("subject_id", "plan_json", "result_json").Where(query.Equal("request_id", request.RequestID)).Build()
	if err != nil {
		return "", "", err
	}
	var subject, plan, result string
	if err = tx.QueryRowContext(ctx, statement, args...).Scan(&subject, &plan, &result); err != nil {
		return "", "", err
	}
	if subject != request.SubjectID {
		return "", "", fmt.Errorf("Data Exchange erasure request subject mismatch")
	}
	return plan, result, nil
}

func (s *Store) PrepareSubjectErasure(ctx context.Context, request sdk.SubjectErasureRequest) (json.RawMessage, error) {
	ctx = s.Scoped(ctx, request.WorkspaceID, "subject-lifecycle")
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = s.lockSubjectScope(ctx, tx, request.WorkspaceID, request.SubjectID); err != nil {
		return nil, err
	}
	if saved, _, found := s.erasureReceipt(ctx, tx, request); found == nil {
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		return json.RawMessage(saved), nil
	} else if !errors.Is(found, sql.ErrNoRows) {
		return nil, found
	}
	selectJobs := query.NewWorkspaceSelectBuilder(s.renderer, "_data_exchange_jobs", request.WorkspaceID).
		Columns("id", "status").Where(query.Equal("actor_id", request.SubjectID)).OrderBy(query.Ascending("id"))
	if s.engine.Capabilities().RowLock {
		selectJobs.ForUpdate()
	}
	statement, args, err := selectJobs.Build()
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	plan := subjectErasurePlan{WorkspaceID: request.WorkspaceID, SubjectID: request.SubjectID, RequestID: request.RequestID, Jobs: []subjectJobPlan{}}
	for rows.Next() {
		var id, status string
		if err = rows.Scan(&id, &status); err != nil {
			rows.Close()
			return nil, err
		}
		if status == "running" {
			rows.Close()
			return nil, fmt.Errorf("Data Exchange erasure blocked by executing job")
		}
		plan.Jobs = append(plan.Jobs, subjectJobPlan{ID: id})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	// Locking each job before cancelling queued work also fences Claim's CAS.
	// A worker that read a queued row before preparation cannot claim it later.
	for i := range plan.Jobs {
		job := &plan.Jobs[i]
		for _, inventory := range []struct {
			table string
			count *int
		}{
			{"_data_exchange_job_chunks", &job.Chunks}, {"_data_exchange_artifacts", &job.Artifacts},
		} {
			statement, args, err = query.NewWorkspaceSelectBuilder(s.renderer, inventory.table, request.WorkspaceID).
				Projections(query.Project(query.CountAll())).Where(query.Equal("job_id", job.ID)).Build()
			if err != nil {
				return nil, err
			}
			if err = tx.QueryRowContext(ctx, statement, args...).Scan(inventory.count); err != nil {
				return nil, err
			}
		}
		cancel := query.NewWorkspaceUpdateBuilder(s.renderer, "_data_exchange_jobs", request.WorkspaceID).
			Set("status", "cancelled").Set("error_code", "subject_erasure_pending").Set("next_attempt_at", "").
			SetExpression("fencing_token", query.Add(query.Column("fencing_token"), query.Value(1))).
			Where(query.And(query.Equal("id", job.ID), query.Equal("actor_id", request.SubjectID), query.Equal("status", "queued")))
		if _, err = execute(ctx, tx, cancel); err != nil {
			return nil, err
		}
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return nil, err
	}
	fence := query.NewWorkspaceUpdateBuilder(s.renderer, "_data_exchange_subject_erasure_fences", request.WorkspaceID).
		Set("request_id", request.RequestID).Where(query.And(query.Equal("subject_id", request.SubjectID), query.Equal("request_id", "")))
	if _, err = execute(ctx, tx, fence); err != nil {
		return nil, err
	}
	insert := query.NewInsertBuilder(s.renderer, "_data_exchange_subject_erasure_receipts").
		Columns("workspace_id", "request_id", "subject_id", "plan_json", "result_json").
		Values(request.WorkspaceID, request.RequestID, request.SubjectID, string(raw), "")
	if _, err = execute(ctx, tx, insert); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return raw, nil
}

func (s *Store) ErasePreparedSubject(ctx context.Context, request sdk.SubjectErasureRequest, raw json.RawMessage) (json.RawMessage, error) {
	var plan subjectErasurePlan
	if json.Unmarshal(raw, &plan) != nil || plan.WorkspaceID != request.WorkspaceID || plan.SubjectID != request.SubjectID || plan.RequestID != request.RequestID {
		return nil, fmt.Errorf("Data Exchange erasure plan scope mismatch")
	}
	ctx = s.Scoped(ctx, request.WorkspaceID, "subject-lifecycle")
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if fence, locked := s.lockSubjectScope(ctx, tx, request.WorkspaceID, request.SubjectID); locked != nil {
		return nil, locked
	} else if fence == "" {
		return nil, fmt.Errorf("Data Exchange erasure fence missing")
	}
	saved, result, err := s.erasureReceipt(ctx, tx, request)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(bytes.TrimSpace(raw), []byte(saved)) {
		return nil, fmt.Errorf("Data Exchange erasure plan differs from durable plan")
	}
	if result != "" {
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		return json.RawMessage(result), nil
	}
	chunks, artifacts := 0, 0
	for _, job := range plan.Jobs {
		for _, table := range []string{"_data_exchange_artifacts", "_data_exchange_job_chunks"} {
			deletion := query.NewWorkspaceDeleteBuilder(s.renderer, table, request.WorkspaceID).Where(query.Equal("job_id", job.ID))
			if _, err = execute(ctx, tx, deletion); err != nil {
				return nil, err
			}
		}
		redact := query.NewWorkspaceUpdateBuilder(s.renderer, "_data_exchange_jobs", request.WorkspaceID).
			Set("status", "cancelled").Set("error_code", "subject_erased").Set("request_payload", []byte{}).
			Set("idempotency_key", "erased:"+fingerprint([]byte(job.ID))).Set("request_sha256", "").
			Set("source_sha256", "").Set("source_bytes", 0).Set("source_chunks", 0).Set("result_chunks", 0).
			Set("checkpoint_value", 0).Set("checkpoint_cursor", "").Set("total_value", 0).
			Set("artifact_id", "").Set("reference_id", "").Set("role_key", "").
			Set("lease_owner", "").Set("lease_expires_at", "").Set("next_attempt_at", "").
			SetExpression("fencing_token", query.Add(query.Column("fencing_token"), query.Value(1))).
			Where(query.And(query.Equal("id", job.ID), query.Equal("actor_id", request.SubjectID), query.NotEqual("status", "running")))
		res, err := execute(ctx, tx, redact)
		if err != nil {
			return nil, err
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return nil, err
		}
		if affected != 1 {
			return nil, fmt.Errorf("Data Exchange erasure job ownership or execution changed")
		}
		chunks += job.Chunks
		artifacts += job.Artifacts
	}
	outcome, err := json.Marshal(map[string]any{"request_id": request.RequestID, "jobs_redacted": len(plan.Jobs), "chunks_deleted": chunks, "artifacts_deleted": artifacts})
	if err != nil {
		return nil, err
	}
	update := query.NewWorkspaceUpdateBuilder(s.renderer, "_data_exchange_subject_erasure_receipts", request.WorkspaceID).
		Set("result_json", string(outcome)).Where(query.And(query.Equal("request_id", request.RequestID), query.Equal("subject_id", request.SubjectID)))
	if _, err = execute(ctx, tx, update); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return outcome, nil
}
