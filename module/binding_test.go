package module

import (
	"context"
	"database/sql"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	_ "modernc.org/sqlite"
)

type testMigrations struct{ db *sql.DB }

func (*testMigrations) Driver() string { return "sqlite" }
func (*testMigrations) Schema() string { return "" }
func (m *testMigrations) ApplyOwnedMigrations(ctx context.Context, _ string, items []modulehost.Migration) error {
	for _, item := range items {
		if _, err := m.db.ExecContext(ctx, item.SQL); err != nil {
			return err
		}
	}
	return nil
}

type testImportProvider struct {
	mu                 sync.Mutex
	validated, applied int
	reject             bool
}

func (p *testImportProvider) ValidateImportBatch(_ context.Context, b dataexchange.ImportBatch) (dataexchange.ImportBatchResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.validated += len(b.Rows)
	if p.reject {
		return dataexchange.ImportBatchResult{Rejected: len(b.Rows)}, nil
	}
	return dataexchange.ImportBatchResult{Accepted: len(b.Rows)}, nil
}
func (p *testImportProvider) ApplyImportBatch(_ context.Context, b dataexchange.ImportBatch) (dataexchange.ImportBatchResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.applied += len(b.Rows)
	return dataexchange.ImportBatchResult{Accepted: len(b.Rows)}, nil
}

type testExportProvider struct {
	mu          sync.Mutex
	completions []dataexchange.ExportCompletion
}

func (*testExportProvider) PlanExport(_ context.Context, r dataexchange.ExportPlanRequest) (dataexchange.ExportPlan, error) {
	return dataexchange.ExportPlan{Filename: "governed-contact.csv", ContentType: "text/csv; charset=utf-8", ExpiresAt: r.CreatedAt.Add(time.Hour)}, nil
}

func (*testExportProvider) ReadExportPage(_ context.Context, r dataexchange.ExportPageRequest) (dataexchange.ExportPage, error) {
	if r.Cursor == "" {
		return dataexchange.ExportPage{Columns: []string{"id", "name"}, Rows: [][]string{{"1", "one"}}, NextCursor: "next", Total: 2}, nil
	}
	return dataexchange.ExportPage{Columns: []string{"id", "name"}, Rows: [][]string{{"2", "two"}}, Total: 2}, nil
}

func (p *testExportProvider) CompleteExport(_ context.Context, completion dataexchange.ExportCompletion) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.completions = append(p.completions, completion)
	return nil
}

type testHost struct {
	db      *sql.DB
	imports map[string]modulehost.ImportProvider
	exports map[string]modulehost.ExportProvider
}

func (h *testHost) Database() *sql.DB                                                 { return h.db }
func (h *testHost) WorkspaceContext(ctx context.Context, _, _ string) context.Context { return ctx }
func (h *testHost) Migrations() modulehost.MigrationRegistrar                         { return &testMigrations{db: h.db} }
func (h *testHost) ImportProvider(k string) (modulehost.ImportProvider, bool) {
	p, ok := h.imports[k]
	return p, ok
}
func (h *testHost) ExportProvider(k string) (modulehost.ExportProvider, bool) {
	p, ok := h.exports[k]
	return p, ok
}

func openTestBinding(t *testing.T) (dataexchange.Binding, *testImportProvider, *testExportProvider) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "exchange.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ip := &testImportProvider{}
	ep := &testExportProvider{}
	h := &testHost{db: db, imports: map[string]modulehost.ImportProvider{"records": ip}, exports: map[string]modulehost.ExportProvider{"records": ep}}
	binding, err := NewFactory(Options{}).OpenModule(context.Background(), dataexchange.ApplicationRef{ApplicationID: "app", RuntimeID: "runtime"}, h)
	if err != nil {
		t.Fatal(err)
	}
	return binding, ip, ep
}

func waitCompleted(t *testing.T, b dataexchange.Binding, scope dataexchange.Scope, id string) dataexchange.Job {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		j, err := b.Job(context.Background(), dataexchange.JobRequest{Scope: scope, JobID: id})
		if err == nil && (j.Status == "completed" || j.Status == "failed") {
			return j
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("job did not complete")
	return dataexchange.Job{}
}

func TestModuleStreamsImportChunksAndRunsTwoPasses(t *testing.T) {
	b, p, _ := openTestBinding(t)
	scope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "actor"}
	large := strings.Repeat("x", sourceChunkBytes+64)
	job, replay, err := b.SubmitImport(context.Background(), dataexchange.ImportRequest{Scope: scope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "import-1", Source: strings.NewReader("id,name\n1," + large + "\n2,two\n"), MaxBytes: 2 << 20})
	if err != nil || replay {
		t.Fatalf("submit: replay=%v err=%v", replay, err)
	}
	done := b.Start(context.Background(), dataexchange.WorkerConfig{Enabled: true, PollInterval: time.Millisecond})
	t.Cleanup(func() { _ = b.Close(context.Background()); <-done })
	completed := waitCompleted(t, b, scope, job.ID)
	if completed.Status != "completed" {
		t.Fatalf("status=%s code=%s", completed.Status, completed.ErrorCode)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.validated != 2 || p.applied != 2 {
		t.Fatalf("validated=%d applied=%d", p.validated, p.applied)
	}
}

func TestModulePagedExportProducesDownloadableArtifact(t *testing.T) {
	b, _, provider := openTestBinding(t)
	scope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "actor"}
	job, _, err := b.SubmitExport(context.Background(), dataexchange.ExportRequest{Scope: scope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "export-1", ReferenceID: "audit-1"})
	if err != nil {
		t.Fatal(err)
	}
	done := b.Start(context.Background(), dataexchange.WorkerConfig{Enabled: true, PollInterval: time.Millisecond})
	t.Cleanup(func() { _ = b.Close(context.Background()); <-done })
	completed := waitCompleted(t, b, scope, job.ID)
	if completed.Status != "completed" {
		t.Fatalf("status=%s code=%s", completed.Status, completed.ErrorCode)
	}
	if string(completed.Options) != "" || completed.ReferenceID != "audit-1" {
		t.Fatalf("options=%q", completed.Options)
	}
	artifact, err := b.Download(context.Background(), dataexchange.JobRequest{Scope: scope, JobID: job.ID})
	if err != nil {
		t.Fatal(err)
	}
	defer artifact.Content.Close()
	content, err := io.ReadAll(artifact.Content)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(content), "id,name\n1,one\n2,two\n"; got != want {
		t.Fatalf("content=%q", got)
	}
	if artifact.Filename != "governed-contact.csv" || artifact.ExpiresAt.IsZero() {
		t.Fatalf("artifact metadata=%+v", artifact)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if len(provider.completions) != 1 || provider.completions[0].Artifact.SHA256 != artifact.SHA256 || provider.completions[0].Rows != 2 || provider.completions[0].ReferenceID != "audit-1" {
		t.Fatalf("completions=%+v", provider.completions)
	}
}

func TestModuleNeverAppliesRejectedImport(t *testing.T) {
	b, p, _ := openTestBinding(t)
	p.reject = true
	scope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "actor"}
	job, _, err := b.SubmitImport(context.Background(), dataexchange.ImportRequest{Scope: scope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "rejected-import", Source: strings.NewReader("id,name\n1,one\n")})
	if err != nil {
		t.Fatal(err)
	}
	done := b.Start(context.Background(), dataexchange.WorkerConfig{Enabled: true, PollInterval: time.Millisecond})
	t.Cleanup(func() { _ = b.Close(context.Background()); <-done })
	failed := waitCompleted(t, b, scope, job.ID)
	if failed.Status != "failed" {
		t.Fatalf("status=%s", failed.Status)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.applied != 0 {
		t.Fatalf("rejected import applied %d rows", p.applied)
	}
}

func TestExpiredLeaseResumesExportFromAtomicCursor(t *testing.T) {
	contract, _, _ := openTestBinding(t)
	b := contract.(*binding)
	scope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "actor"}
	job, _, err := b.SubmitExport(context.Background(), dataexchange.ExportRequest{Scope: scope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "resume-export"})
	if err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := b.store.claim(context.Background(), "worker-one", time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	first := []byte("id,name\n1,one\n")
	if err = b.store.commitResultPage(context.Background(), claimed, 0, first, "next", 1, 2); err != nil {
		t.Fatal(err)
	}
	if _, err = b.store.db.Exec(`UPDATE data_exchange_jobs SET lease_expires_at=? WHERE id=?`, time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano), job.ID); err != nil {
		t.Fatal(err)
	}
	reclaimed, ok, err := b.store.claim(context.Background(), "worker-two", time.Second)
	if err != nil || !ok {
		t.Fatalf("reclaim: ok=%v err=%v", ok, err)
	}
	if reclaimed.Job.Cursor != "next" || reclaimed.Job.ResultChunks != 1 {
		t.Fatalf("cursor=%q chunks=%d", reclaimed.Job.Cursor, reclaimed.Job.ResultChunks)
	}
	if err = b.store.progress(context.Background(), claimed, 2, 2); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("stale worker progress error=%v", err)
	}
	if err = b.processExport(context.Background(), reclaimed); err != nil {
		t.Fatal(err)
	}
	artifact, err := b.Download(context.Background(), dataexchange.JobRequest{Scope: scope, JobID: job.ID})
	if err != nil {
		t.Fatal(err)
	}
	defer artifact.Content.Close()
	content, err := io.ReadAll(artifact.Content)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(content), "id,name\n1,one\n2,two\n"; got != want {
		t.Fatalf("content=%q", got)
	}
	_ = claimed
}

func TestImportIdempotencyVerifiesStreamFingerprint(t *testing.T) {
	b, _, _ := openTestBinding(t)
	scope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "actor"}
	request := func(source string) dataexchange.ImportRequest {
		return dataexchange.ImportRequest{Scope: scope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "same-key", Filename: "contact.csv", ContentType: "text/csv", Source: strings.NewReader(source)}
	}
	first, replay, err := b.SubmitImport(context.Background(), request("id,name\n1,one\n"))
	if err != nil || replay {
		t.Fatalf("first: replay=%v err=%v", replay, err)
	}
	second, replay, err := b.SubmitImport(context.Background(), request("id,name\n1,one\n"))
	if err != nil || !replay || second.ID != first.ID {
		t.Fatalf("replay: job=%q replay=%v err=%v", second.ID, replay, err)
	}
	if _, _, err = b.SubmitImport(context.Background(), request("id,name\n1,different\n")); err == nil || !strings.Contains(err.Error(), "different source") {
		t.Fatalf("different source error=%v", err)
	}
}
