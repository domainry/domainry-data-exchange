package module

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	sdk "github.com/domainry/domainry-data-exchange-sdk"
	model "github.com/domainry/domainry-data-exchange/internal/domain/dataexchange/model"
	"github.com/domainry/domainry-foundation/requestcontext"
)

func bindSharedSubjectLifecycle(t *testing.T, binding interface{ BindSubjectLifecyclePersistence() error }, db *sql.DB) {
	t.Helper()
	for _, statement := range []string{
		`CREATE TABLE _subject_requests (id TEXT NOT NULL, workspace_id TEXT NOT NULL, request_type TEXT NOT NULL, kind TEXT NOT NULL, resolved_identity TEXT NOT NULL, PRIMARY KEY(workspace_id,id))`,
		`CREATE TABLE _subject_steps (workspace_id TEXT NOT NULL, request_id TEXT NOT NULL, owner TEXT NOT NULL, operation TEXT NOT NULL, payload_json TEXT NOT NULL, completed_at TEXT NOT NULL, PRIMARY KEY(workspace_id,request_id,owner,operation))`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if err := binding.BindSubjectLifecyclePersistence(); err != nil {
		t.Fatal(err)
	}
}

func beginSharedSubjectErasure(t *testing.T, db *sql.DB, workspace, subject, request string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO _subject_requests(id,workspace_id,request_type,kind,resolved_identity) VALUES(?,?,'subject_request','erase',?)`, request, workspace, subject); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO _subject_steps(workspace_id,request_id,owner,operation,payload_json,completed_at) VALUES(?,?,'lifecycle','erase_fence','{}',?)`, workspace, request, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
}

func TestSubjectErasureFailsClosedUntilSharedLifecyclePersistenceIsBound(t *testing.T) {
	binding, _, _, _ := openArtifactTestBindingStore(t)
	ctx := requestcontext.WithWorkspaceID(t.Context(), "workspace")
	request := sdk.SubjectErasureRequest{WorkspaceID: "workspace", SubjectID: "alice", RequestID: "erase-alice"}
	if _, err := binding.SubjectLifecycle().PrepareSubjectErasure(ctx, request); err == nil || !strings.Contains(err.Error(), "not bound") {
		t.Fatalf("unbound shared Lifecycle persistence error=%v", err)
	}
}

func TestSubjectErasureDeletesDurableContentWithRollbackAndRetry(t *testing.T) {
	binding, store, db, provider := openArtifactTestBindingStore(t)
	bindSharedSubjectLifecycle(t, binding, db)
	provider.content = []byte(`{"email":"PRIVATE@example.test"}`)
	owner := sdk.Scope{WorkspaceID: "workspace", ActorID: "alice"}
	peer := sdk.Scope{WorkspaceID: "workspace", ActorID: "bob"}
	foreign := sdk.Scope{WorkspaceID: "other-workspace", ActorID: "alice"}
	jobs := []sdk.Job{}
	for _, scope := range []sdk.Scope{owner, peer, foreign} {
		job, _, err := binding.SubmitExport(t.Context(), sdk.ExportRequest{Scope: scope, Provider: "identity", ObjectKey: "user", IdempotencyKey: "PRIVATE@example.test", Options: []byte(`{"email":"PRIVATE@example.test"}`)})
		if err != nil {
			t.Fatal(err)
		}
		jobs = append(jobs, job)
		if job.ActorID != scope.ActorID || job.WorkspaceID != scope.WorkspaceID {
			t.Fatalf("submission returned another subject's job: %#v", job)
		}
		x, claimed, err := store.Claim(t.Context(), "worker", time.Minute)
		if err != nil || !claimed {
			t.Fatalf("claim=%v err=%v", claimed, err)
		}
		if err = binding.Process(t.Context(), x); err != nil {
			t.Fatal(err)
		}
	}
	importJob, _, err := binding.SubmitImport(t.Context(), sdk.ImportRequest{Scope: owner, Provider: "identity", ObjectKey: "user", IdempotencyKey: "upload-PRIVATE", Filename: "PRIVATE.csv", Source: strings.NewReader("email\nPRIVATE@example.test\n")})
	if err != nil {
		t.Fatal(err)
	}
	system := requestcontext.WithWorkspaceID(t.Context(), owner.WorkspaceID)
	subjects := binding.SubjectLifecycle()
	request := sdk.SubjectErasureRequest{WorkspaceID: owner.WorkspaceID, SubjectID: owner.ActorID, RequestID: "erase-alice"}
	if _, err = subjects.PrepareSubjectErasure(t.Context(), request); err == nil {
		t.Fatal("unscoped erasure accepted")
	}
	wrong := request
	wrong.WorkspaceID = foreign.WorkspaceID
	if _, err = subjects.PrepareSubjectErasure(system, wrong); err == nil {
		t.Fatal("cross-workspace erasure accepted")
	}
	held := request
	held.LegalHolds = json.RawMessage(`[{"id":"hold"}]`)
	if _, err = subjects.PrepareSubjectErasure(system, held); err == nil {
		t.Fatal("legal hold ignored")
	}
	beginSharedSubjectErasure(t, db, owner.WorkspaceID, owner.ActorID, request.RequestID)
	plan, err := subjects.PrepareSubjectErasure(system, request)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(plan, []byte("PRIVATE")) {
		t.Fatalf("plan contains personal data: %s", plan)
	}
	replayedPlan, err := subjects.PrepareSubjectErasure(system, request)
	if err != nil || !bytes.Equal(plan, replayedPlan) {
		t.Fatalf("plan replay changed: %s %v", replayedPlan, err)
	}
	if _, _, err = binding.SubmitExport(t.Context(), sdk.ExportRequest{Scope: owner, Provider: "identity", ObjectKey: "user", IdempotencyKey: "new-after-fence"}); err == nil {
		t.Fatal("fenced subject submitted export")
	}
	if _, _, err = binding.SubmitImport(t.Context(), sdk.ImportRequest{Scope: owner, Provider: "identity", ObjectKey: "user", IdempotencyKey: "new-upload-after-fence", Source: strings.NewReader("email\nnew\n")}); err == nil {
		t.Fatal("fenced subject submitted import")
	}
	if _, claimed, err := store.Claim(t.Context(), "worker", time.Minute); err != nil || claimed {
		t.Fatalf("cancelled import claimed=%v err=%v", claimed, err)
	}
	tampered := append(json.RawMessage(nil), plan...)
	tampered = bytes.Replace(tampered, []byte(jobs[0].ID), []byte(jobs[1].ID), 1)
	if _, err = subjects.ErasePreparedSubject(system, request, tampered); err == nil {
		t.Fatal("forged plan accepted")
	}
	// Force failure after artifact/chunk deletion. The whole source transaction
	// must roll back, including content and the final retry receipt.
	if _, err = db.Exec(`CREATE TRIGGER erasure_failure BEFORE UPDATE OF request_payload ON _data_exchange_jobs BEGIN SELECT RAISE(ABORT,'injected erasure failure'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err = subjects.ErasePreparedSubject(system, request, plan); err == nil {
		t.Fatal("injected source failure ignored")
	}
	artifact, err := binding.Download(jobAuthorizedContext(t.Context(), owner, sdk.ActionDataExchangeJobDownload), sdk.JobRequest{Scope: owner, JobID: jobs[0].ID})
	if err != nil {
		t.Fatalf("rollback lost artifact: %v", err)
	}
	content, err := io.ReadAll(artifact.Content)
	artifact.Content.Close()
	if err != nil || !bytes.Contains(content, []byte("PRIVATE")) {
		t.Fatalf("rollback content=%s err=%v", content, err)
	}
	if _, err = db.Exec(`DROP TRIGGER erasure_failure`); err != nil {
		t.Fatal(err)
	}
	first, err := subjects.ErasePreparedSubject(system, request, plan)
	if err != nil {
		t.Fatal(err)
	}
	second, err := subjects.ErasePreparedSubject(system, request, plan)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatalf("shared result-step replay changed: %s %v", second, err)
	}
	var sharedSteps int
	if err = db.QueryRow(`SELECT COUNT(*) FROM _subject_steps WHERE workspace_id=? AND request_id=? AND owner='data_exchange'`, request.WorkspaceID, request.RequestID).Scan(&sharedSteps); err != nil || sharedSteps != 2 {
		t.Fatalf("shared subject steps=%d err=%v", sharedSteps, err)
	}
	for _, retired := range []string{"_data_exchange_subject_erasure_fences", "_data_exchange_subject_erasure_receipts"} {
		var tables int
		if err = db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?`, retired).Scan(&tables); err != nil || tables != 0 {
			t.Fatalf("retired subject table %s count=%d err=%v", retired, tables, err)
		}
	}
	for _, job := range []sdk.Job{jobs[0], importJob} {
		var count int
		if err = db.QueryRow(`SELECT COUNT(*) FROM _data_exchange_job_chunks WHERE workspace_id=? AND job_id=?`, owner.WorkspaceID, job.ID).Scan(&count); err != nil || count != 0 {
			t.Fatalf("chunks remain=%d err=%v", count, err)
		}
		if err = db.QueryRow(`SELECT COUNT(*) FROM _artifacts a JOIN _artifact_bindings b ON b.workspace_id=a.workspace_id AND b.artifact_id=a.id WHERE b.workspace_id=? AND b.owner='data_exchange' AND b.kind='job' AND b.resource_type='data_exchange_job' AND b.resource_id=? AND a.status<>'deleted'`, owner.WorkspaceID, job.ID).Scan(&count); err != nil || count != 0 {
			t.Fatalf("active shared artifact remains=%d err=%v", count, err)
		}
		var payload []byte
		var key, code, status string
		if err = db.QueryRow(`SELECT request_payload,idempotency_key,error_code,status FROM _data_exchange_jobs WHERE workspace_id=? AND id=?`, owner.WorkspaceID, job.ID).Scan(&payload, &key, &code, &status); err != nil {
			t.Fatal(err)
		}
		if len(payload) != 0 || strings.Contains(key, "PRIVATE") || code != "subject_erased" || status != "cancelled" {
			t.Fatalf("job still contains personal data: %s %s %s %s", payload, key, code, status)
		}
	}
	if _, err = binding.Download(jobAuthorizedContext(t.Context(), owner, sdk.ActionDataExchangeJobDownload), sdk.JobRequest{Scope: owner, JobID: jobs[0].ID}); err == nil {
		t.Fatal("erased artifact still downloads")
	}
	for i, scope := range []sdk.Scope{peer, foreign} {
		artifact, err := binding.Download(jobAuthorizedContext(t.Context(), scope, sdk.ActionDataExchangeJobDownload), sdk.JobRequest{Scope: scope, JobID: jobs[i+1].ID})
		if err != nil {
			t.Fatalf("other subject artifact lost: %v", err)
		}
		content, err := io.ReadAll(artifact.Content)
		artifact.Content.Close()
		if err != nil || !bytes.Contains(content, []byte("PRIVATE")) {
			t.Fatalf("other artifact changed: %s %v", content, err)
		}
	}
}

func TestSubjectErasureBlocksRunningJobAndFencesStaleWorker(t *testing.T) {
	binding, store, db, _ := openArtifactTestBindingStore(t)
	bindSharedSubjectLifecycle(t, binding, db)
	scope := sdk.Scope{WorkspaceID: "workspace", ActorID: "alice"}
	job, _, err := binding.SubmitExport(t.Context(), sdk.ExportRequest{Scope: scope, Provider: "identity", ObjectKey: "user", IdempotencyKey: "running"})
	if err != nil {
		t.Fatal(err)
	}
	x, claimed, err := store.Claim(t.Context(), "worker", time.Minute)
	if err != nil || !claimed {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
	system := requestcontext.WithWorkspaceID(t.Context(), scope.WorkspaceID)
	subjects := binding.SubjectLifecycle()
	request := sdk.SubjectErasureRequest{WorkspaceID: scope.WorkspaceID, SubjectID: scope.ActorID, RequestID: "erase-running"}
	beginSharedSubjectErasure(t, db, scope.WorkspaceID, scope.ActorID, request.RequestID)
	if _, err = subjects.PrepareSubjectErasure(system, request); err == nil {
		t.Fatal("running job ignored")
	}
	if err = binding.Process(t.Context(), x); err != nil {
		t.Fatal(err)
	}
	plan, err := subjects.PrepareSubjectErasure(system, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = subjects.ErasePreparedSubject(system, request, plan); err != nil {
		t.Fatal(err)
	}
	if err = store.CommitResultPage(t.Context(), x, 99, []byte("PRIVATE@example.test"), "", 1, 1); err == nil {
		t.Fatal("stale worker recreated result chunk")
	}
	if err = store.Complete(t.Context(), x, &model.ArtifactRecord{ID: "stale-artifact", Filename: "PRIVATE.csv", ContentType: "text/csv", ExpiresAt: time.Now().Add(time.Hour)}); err == nil {
		t.Fatal("stale worker recreated artifact")
	}
	if err = store.Heartbeat(t.Context(), x, time.Minute); err == nil {
		t.Fatal("stale worker retained lease")
	}
	var count int
	if err = db.QueryRow(`SELECT COUNT(*) FROM _data_exchange_job_chunks WHERE workspace_id=? AND job_id=?`, scope.WorkspaceID, job.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("stale chunk write survived: %d %v", count, err)
	}
	if err = db.QueryRow(`SELECT COUNT(*) FROM _artifacts WHERE workspace_id=? AND id='stale-artifact'`, scope.WorkspaceID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("stale shared artifact write survived: %d %v", count, err)
	}
}
