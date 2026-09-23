package dataexchange

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	sdk "github.com/domainry/domainry-data-exchange-sdk"
	sharedartifact "github.com/domainry/domainry-foundation/artifact"
	lifecyclemodel "github.com/domainry/domainry-lifecycle-sdk/model"
	"github.com/domainry/domainry-orm/query"
)

const (
	sharedSubjectRequestsTable       = "_subject_requests"
	sharedSubjectExecutionStepsTable = "_subject_steps"
	lifecycleSubjectOwner            = "lifecycle"
	subjectEraseFenceOperation       = "erase_fence"
	dataExchangeSubjectOwner         = "data_exchange"
	subjectErasePlanOperation        = "erase_plan"
	subjectEraseOperation            = "erase"
)

func sharedSubjectErasureRequestIDs(renderer query.Renderer, workspace, subject string) *query.SelectBuilder {
	return query.NewWorkspaceSelectBuilder(renderer, sharedSubjectRequestsTable, workspace).Columns("id").Where(query.And(
		query.NotEqual("request_type", "external_erasure"),
		query.Equal("kind", "erase"),
		query.Equal("resolved_identity", subject),
	))
}

func sharedSubjectFenceRequests(renderer query.Renderer, workspace, subject, request string) *query.SelectBuilder {
	predicates := []query.Predicate{
		query.Equal("owner", lifecycleSubjectOwner),
		query.Equal("operation", subjectEraseFenceOperation),
		query.InSubquery("request_id", sharedSubjectErasureRequestIDs(renderer, workspace, subject)),
	}
	if request != "" {
		predicates = append(predicates, query.Equal("request_id", request))
	}
	return query.NewWorkspaceSelectBuilder(renderer, sharedSubjectExecutionStepsTable, workspace).
		Columns("request_id").Where(query.And(predicates...))
}

type subjectJobPlan struct {
	ID         string `json:"id"`
	Chunks     int    `json:"chunks"`
	ArtifactID string `json:"artifact_id,omitempty"`
}

type subjectErasurePlan struct {
	WorkspaceID string           `json:"workspace_id"`
	SubjectID   string           `json:"subject_id"`
	RequestID   string           `json:"request_id"`
	Jobs        []subjectJobPlan `json:"jobs"`
}

type subjectStepQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type subjectStepExecutor interface {
	subjectStepQueryer
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (s *Store) checkSubjectSubmission(ctx context.Context, tx *sql.Tx, scope sdk.Scope) error {
	if !s.SubjectLifecyclePersistenceBound() {
		return nil
	}
	statement, args, err := sharedSubjectFenceRequests(s.renderer, scope.WorkspaceID, scope.ActorID, "").Limit(1).Build()
	if err != nil {
		return err
	}
	var request string
	if err = tx.QueryRowContext(ctx, statement, args...).Scan(&request); errors.Is(err, sql.ErrNoRows) {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("Data Exchange subject is fenced for erasure")
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

func (s *Store) sharedSubjectStep(ctx context.Context, executor subjectStepQueryer, workspace, request, operation string) (json.RawMessage, bool, error) {
	statement, args, err := query.NewWorkspaceSelectBuilder(s.renderer, sharedSubjectExecutionStepsTable, workspace).
		Columns("payload_json").Where(query.And(
		query.Equal("request_id", request),
		query.Equal("owner", dataExchangeSubjectOwner),
		query.Equal("operation", operation),
	)).Build()
	if err != nil {
		return nil, false, err
	}
	var raw string
	if err = executor.QueryRowContext(ctx, statement, args...).Scan(&raw); errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}
	var step lifecyclemodel.SubjectExecutionStep
	if json.Unmarshal([]byte(raw), &step) != nil || step.WorkspaceID != workspace || step.RequestID != request || step.Owner != dataExchangeSubjectOwner || step.Operation != operation || !json.Valid(step.Payload) {
		return nil, false, fmt.Errorf("Data Exchange shared subject execution step invalid")
	}
	return append(json.RawMessage(nil), step.Payload...), true, nil
}

func (s *Store) saveSharedSubjectStep(ctx context.Context, executor subjectStepExecutor, workspace, request, operation string, payload json.RawMessage) error {
	if !json.Valid(payload) {
		return fmt.Errorf("Data Exchange shared subject execution payload invalid")
	}
	if previous, found, err := s.sharedSubjectStep(ctx, executor, workspace, request, operation); err != nil {
		return err
	} else if found {
		if !bytes.Equal(previous, payload) {
			return fmt.Errorf("Data Exchange shared subject execution step payload conflict")
		}
		return nil
	}
	completedAt := time.Now().UTC()
	step := lifecyclemodel.SubjectExecutionStep{
		WorkspaceID: workspace,
		RequestID:   request,
		Owner:       dataExchangeSubjectOwner,
		Operation:   operation,
		Payload:     append(json.RawMessage(nil), payload...),
		CompletedAt: completedAt,
	}
	raw, err := json.Marshal(step)
	if err != nil {
		return err
	}
	insert := query.NewWorkspaceInsertBuilder(s.renderer, sharedSubjectExecutionStepsTable, workspace).
		Columns("request_id", "owner", "operation", "payload_json", "completed_at").
		Values(request, dataExchangeSubjectOwner, operation, string(raw), completedAt.Format(time.RFC3339Nano))
	_, err = execute(ctx, executor, insert)
	return err
}

func (s *Store) requireSharedSubjectFence(ctx context.Context, executor subjectStepQueryer, request sdk.SubjectErasureRequest) error {
	statement, args, err := sharedSubjectFenceRequests(s.renderer, request.WorkspaceID, request.SubjectID, request.RequestID).Build()
	if err != nil {
		return err
	}
	var fencedRequest string
	if err = executor.QueryRowContext(ctx, statement, args...).Scan(&fencedRequest); errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("Data Exchange erasure requires Lifecycle fence")
	} else if err != nil {
		return err
	}
	return nil
}

func (s *Store) PrepareSubjectErasure(ctx context.Context, request sdk.SubjectErasureRequest) (json.RawMessage, error) {
	if !s.SubjectLifecyclePersistenceBound() {
		return nil, fmt.Errorf("Data Exchange shared subject lifecycle persistence is not bound")
	}
	ctx = s.Scoped(ctx, request.WorkspaceID, "subject-lifecycle")
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if saved, found, err := s.sharedSubjectStep(ctx, tx, request.WorkspaceID, request.RequestID, subjectErasePlanOperation); err != nil {
		return nil, err
	} else if found {
		var previous subjectErasurePlan
		if json.Unmarshal(saved, &previous) != nil || previous.WorkspaceID != request.WorkspaceID || previous.SubjectID != request.SubjectID || previous.RequestID != request.RequestID {
			return nil, fmt.Errorf("Data Exchange shared erasure plan scope mismatch")
		}
		return saved, nil
	}
	if err = s.requireSharedSubjectFence(ctx, tx, request); err != nil {
		return nil, err
	}
	selectJobs := query.NewWorkspaceSelectBuilder(s.renderer, "_data_exchange_jobs", request.WorkspaceID).
		Columns("id", "status", "artifact_id").Where(query.Equal("actor_id", request.SubjectID)).OrderBy(query.Ascending("id"))
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
		var id, status, artifactID string
		if err = rows.Scan(&id, &status, &artifactID); err != nil {
			rows.Close()
			return nil, err
		}
		if status == "running" {
			rows.Close()
			return nil, fmt.Errorf("Data Exchange erasure blocked by executing job")
		}
		plan.Jobs = append(plan.Jobs, subjectJobPlan{ID: id, ArtifactID: artifactID})
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
		statement, args, err = query.NewWorkspaceSelectBuilder(s.renderer, "_data_exchange_job_chunks", request.WorkspaceID).
			Projections(query.Project(query.CountAll())).Where(query.Equal("job_id", job.ID)).Build()
		if err != nil {
			return nil, err
		}
		if err = tx.QueryRowContext(ctx, statement, args...).Scan(&job.Chunks); err != nil {
			return nil, err
		}
		if job.ArtifactID != "" {
			artifact, found, readErr := s.artifacts.ByID(sharedartifact.WithExecutor(ctx, tx), request.WorkspaceID, job.ArtifactID)
			if readErr != nil {
				return nil, readErr
			}
			if !found || artifact.Owner != sharedartifact.OwnerDataExchange || artifact.Kind != "output" {
				return nil, fmt.Errorf("Data Exchange job %s references an invalid shared artifact", job.ID)
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
	if err = s.saveSharedSubjectStep(ctx, tx, request.WorkspaceID, request.RequestID, subjectErasePlanOperation, raw); err != nil {
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
	if !s.SubjectLifecyclePersistenceBound() {
		return nil, fmt.Errorf("Data Exchange shared subject lifecycle persistence is not bound")
	}
	ctx = s.Scoped(ctx, request.WorkspaceID, "subject-lifecycle")
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err = s.requireSharedSubjectFence(ctx, tx, request); err != nil {
		return nil, err
	}
	saved, found, err := s.sharedSubjectStep(ctx, tx, request.WorkspaceID, request.RequestID, subjectErasePlanOperation)
	if err != nil {
		return nil, err
	}
	if !found || !bytes.Equal(saved, raw) {
		return nil, fmt.Errorf("Data Exchange erasure plan differs from shared Lifecycle step")
	}
	if result, completed, err := s.sharedSubjectStep(ctx, tx, request.WorkspaceID, request.RequestID, subjectEraseOperation); err != nil {
		return nil, err
	} else if completed {
		return result, nil
	}
	chunks, artifacts := 0, 0
	for _, job := range plan.Jobs {
		deletion := query.NewWorkspaceDeleteBuilder(s.renderer, "_data_exchange_job_chunks", request.WorkspaceID).Where(query.Equal("job_id", job.ID))
		if _, err = execute(ctx, tx, deletion); err != nil {
			return nil, err
		}
		if job.ArtifactID != "" {
			sharedContext := sharedartifact.WithExecutor(ctx, tx)
			artifact, found, readErr := s.artifacts.ByID(sharedContext, request.WorkspaceID, job.ArtifactID)
			if readErr != nil {
				return nil, readErr
			}
			if !found || artifact.Owner != sharedartifact.OwnerDataExchange || artifact.Kind != "output" {
				return nil, fmt.Errorf("Data Exchange job %s shared artifact changed after preparation", job.ID)
			}
			if artifact.Status != sharedartifact.StatusDeleted {
				changed, transitionErr := s.artifacts.Transition(sharedContext, request.WorkspaceID, artifact.ID, artifact.Status, sharedartifact.StatusDeleted, artifact.ScanStatus, time.Now().UTC())
				if transitionErr != nil {
					return nil, transitionErr
				}
				if !changed {
					return nil, fmt.Errorf("Data Exchange job %s shared artifact changed during erasure", job.ID)
				}
			}
			artifacts++
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
	}
	outcome, err := json.Marshal(map[string]any{"request_id": request.RequestID, "jobs_redacted": len(plan.Jobs), "chunks_deleted": chunks, "artifacts_deleted": artifacts})
	if err != nil {
		return nil, err
	}
	if err = s.saveSharedSubjectStep(ctx, tx, request.WorkspaceID, request.RequestID, subjectEraseOperation, outcome); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return outcome, nil
}
