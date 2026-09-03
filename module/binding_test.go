package module

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	exchange "github.com/domainry/domainry-data-exchange/internal/adapter/dataexchangesdk"
	persistenceengine "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence"
	persistence "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence/database/dataexchange"
	persistenceschema "github.com/domainry/domainry-data-exchange/internal/infrastructure/persistence/database/schema"
	"github.com/domainry/domainry-foundation/modulehttp"
	identitysdk "github.com/domainry/domainry-identity-sdk"
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
	validateAttempts   []int
	validateFinal      []bool
	reject             bool
}

func (p *testImportProvider) ValidateImportBatch(_ context.Context, b dataexchange.ImportBatch) (dataexchange.ImportBatchResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.validated += len(b.Rows)
	p.validateAttempts = append(p.validateAttempts, b.Attempt)
	p.validateFinal = append(p.validateFinal, b.Final)
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

type testArtifactImportProvider struct {
	validated, applied               string
	validatedOptions, appliedOptions string
}

func (*testArtifactImportProvider) ValidateImportBatch(context.Context, dataexchange.ImportBatch) (dataexchange.ImportBatchResult, error) {
	return dataexchange.ImportBatchResult{}, nil
}
func (*testArtifactImportProvider) ApplyImportBatch(context.Context, dataexchange.ImportBatch) (dataexchange.ImportBatchResult, error) {
	return dataexchange.ImportBatchResult{}, nil
}
func (p *testArtifactImportProvider) ValidateImportArtifact(_ context.Context, artifact dataexchange.ImportArtifact) (dataexchange.ImportArtifactResult, error) {
	raw, err := io.ReadAll(artifact.Content)
	p.validated = string(raw)
	p.validatedOptions = string(artifact.Options)
	return dataexchange.ImportArtifactResult{Records: 2, Receipt: "validated"}, err
}
func (p *testArtifactImportProvider) ApplyImportArtifact(_ context.Context, artifact dataexchange.ImportArtifact) (dataexchange.ImportArtifactResult, error) {
	raw, err := io.ReadAll(artifact.Content)
	p.applied = string(raw)
	p.appliedOptions = string(artifact.Options)
	return dataexchange.ImportArtifactResult{Records: 2, Receipt: "applied"}, err
}

type testArtifactExportProvider struct {
	completions []dataexchange.ExportCompletion
	content     []byte
}

func (*testArtifactExportProvider) ReadExportPage(context.Context, dataexchange.ExportPageRequest) (dataexchange.ExportPage, error) {
	return dataexchange.ExportPage{}, nil
}
func (p *testArtifactExportProvider) BuildExportArtifact(_ context.Context, request dataexchange.ExportArtifactRequest) (dataexchange.ExportArtifact, error) {
	content := p.content
	if content == nil {
		content = []byte(`{"datasets":[{"name":"users"}]}`)
	}
	return dataexchange.ExportArtifact{
		Filename: "identity-bundle.json", ContentType: "application/json", ExpiresAt: request.CreatedAt.Add(time.Hour),
		Content: io.NopCloser(bytes.NewReader(content)), Records: 2,
	}, nil
}
func (p *testArtifactExportProvider) CompleteExport(_ context.Context, completion dataexchange.ExportCompletion) error {
	p.completions = append(p.completions, completion)
	return nil
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

func openArtifactTestBinding(t *testing.T) (dataexchange.Binding, *testArtifactImportProvider, *testArtifactExportProvider) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "artifact-exchange.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	imports := &testArtifactImportProvider{}
	exports := &testArtifactExportProvider{}
	host := &testHost{db: db, imports: map[string]modulehost.ImportProvider{"identity": imports}, exports: map[string]modulehost.ExportProvider{"identity": exports}}
	binding, err := NewFactory(Options{}).OpenModule(t.Context(), dataexchange.ApplicationRef{ApplicationID: "app", RuntimeID: "runtime"}, host)
	if err != nil {
		t.Fatal(err)
	}
	return binding, imports, exports
}

func openArtifactTestBindingStore(t *testing.T) (*exchange.Binding, *persistence.Store, *sql.DB, *testArtifactExportProvider) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "artifact-exchange.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	imports := &testArtifactImportProvider{}
	exports := &testArtifactExportProvider{}
	host := &testHost{db: db, imports: map[string]modulehost.ImportProvider{"identity": imports}, exports: map[string]modulehost.ExportProvider{"identity": exports}}
	engine, err := persistenceengine.NewEngine("sqlite")
	if err != nil {
		t.Fatal(err)
	}
	migrations, err := persistenceschema.SchemaMigrations(engine, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Migrations().ApplyOwnedMigrations(t.Context(), "data_exchange", migrations); err != nil {
		t.Fatal(err)
	}
	store, err := persistence.NewStore(db, engine, "", host.WorkspaceContext)
	if err != nil {
		t.Fatal(err)
	}
	return exchange.NewBinding(dataexchange.ApplicationRef{ApplicationID: "app", RuntimeID: "runtime"}, host, store), store, db, exports
}

func openTestBindingStore(t *testing.T) (dataexchange.Binding, *persistence.Store, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "exchange.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	h := &testHost{db: db, imports: map[string]modulehost.ImportProvider{"records": &testImportProvider{}}, exports: map[string]modulehost.ExportProvider{"records": &testExportProvider{}}}
	engine, err := persistenceengine.NewEngine("sqlite")
	if err != nil {
		t.Fatal(err)
	}
	migrations, err := persistenceschema.SchemaMigrations(engine, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Migrations().ApplyOwnedMigrations(t.Context(), "data_exchange", migrations); err != nil {
		t.Fatal(err)
	}
	store, err := persistence.NewStore(db, engine, "", h.WorkspaceContext)
	if err != nil {
		t.Fatal(err)
	}
	return exchange.NewBinding(dataexchange.ApplicationRef{ApplicationID: "app", RuntimeID: "runtime"}, h, store), store, db
}

func waitCompleted(t *testing.T, b dataexchange.Binding, scope dataexchange.Scope, id string) dataexchange.Job {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		j, err := b.Job(jobAuthorizedContext(context.Background(), scope, dataexchange.ActionDataExchangeJobGet), dataexchange.JobRequest{Scope: scope, JobID: id})
		if err == nil && (j.Status == "completed" || j.Status == "failed") {
			return j
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("job did not complete")
	return dataexchange.Job{}
}

func jobAuthorizedContext(ctx context.Context, scope dataexchange.Scope, permissions ...string) context.Context {
	bundle := &identitysdk.AccessBundle{
		ContractVersion: identitysdk.CurrentPolicyBundleVersion,
		Subject:         identitysdk.Subject{WorkspaceID: identitysdk.WorkspaceID(scope.WorkspaceID), SubjectID: identitysdk.SubjectID(scope.ActorID)},
	}
	for _, permission := range permissions {
		separator := strings.LastIndexByte(permission, '.')
		if separator <= 0 || separator == len(permission)-1 {
			continue
		}
		resource, action := permission[:separator], permission[separator+1:]
		bundle.FunctionGrants = append(bundle.FunctionGrants, identitysdk.FunctionGrant{Resource: identitysdk.ResourceType(resource), Action: identitysdk.Action(action), Effect: identitysdk.EffectAllow})
		bundle.DataPolicies = append(bundle.DataPolicies, identitysdk.DataPolicy{
			Key: permission, Resource: identitysdk.ResourceType(resource), Action: identitysdk.Action(action), Effect: identitysdk.EffectAllow,
			DataScopes: []identitysdk.DataScope{identitysdk.DataScopeOwner}, Predicate: identitysdk.Predicate{Fact: "owner_user_id", Operator: identitysdk.OperatorEqual, Value: "$subject.id"},
		})
	}
	return identitysdk.WithRequestIdentity(ctx, identitysdk.RequestIdentity{Principal: identitysdk.Principal{
		Known: true, WorkspaceID: scope.WorkspaceID, UserID: scope.ActorID, RoleKey: scope.RoleKey, AccessBundle: bundle,
	}})
}

func TestModuleStreamsImportChunksAndRunsTwoPasses(t *testing.T) {
	b, p, _ := openTestBinding(t)
	scope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "actor"}
	large := strings.Repeat("x", (1<<20)+64)
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
	if len(p.validateAttempts) != 1 || p.validateAttempts[0] != 1 || len(p.validateFinal) != 1 || !p.validateFinal[0] {
		t.Fatalf("attempts=%v final=%v", p.validateAttempts, p.validateFinal)
	}
}

func TestModuleBindingExposesOwnedHTTPSurface(t *testing.T) {
	binding, _, _ := openTestBinding(t)
	provider, ok := binding.(modulehttp.Provider)
	if !ok {
		t.Fatal("Data Exchange Module binding does not expose HTTP surfaces")
	}
	surfaces := provider.HTTPSurfaces()
	if len(surfaces) != 1 {
		t.Fatalf("surfaces=%d", len(surfaces))
	}
	if err := modulehttp.ValidateSurface(surfaces[0]); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerBatchSizeClaimsMultipleJobsPerPoll(t *testing.T) {
	binding, _, _ := openTestBinding(t)
	scope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "actor"}
	first, _, err := binding.SubmitExport(t.Context(), dataexchange.ExportRequest{Scope: scope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "batch-size-first"})
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := binding.SubmitExport(t.Context(), dataexchange.ExportRequest{Scope: scope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "batch-size-second"})
	if err != nil {
		t.Fatal(err)
	}
	done := binding.Start(t.Context(), dataexchange.WorkerConfig{Enabled: true, PollInterval: time.Hour, BatchSize: 2})
	t.Cleanup(func() { _ = binding.Close(context.Background()); <-done })
	if completed := waitCompleted(t, binding, scope, first.ID); completed.Status != "completed" {
		t.Fatalf("first=%+v", completed)
	}
	if completed := waitCompleted(t, binding, scope, second.ID); completed.Status != "completed" {
		t.Fatalf("second=%+v", completed)
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
	artifact, err := b.Download(jobAuthorizedContext(context.Background(), scope, dataexchange.ActionDataExchangeJobDownload), dataexchange.JobRequest{Scope: scope, JobID: job.ID})
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

func TestModuleProcessesCanonicalImportArtifactWithoutCSVDecoding(t *testing.T) {
	binding, provider, _ := openArtifactTestBinding(t)
	scope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "actor"}
	content := `{"datasets":[{"name":"users"}]}`
	job, replayed, err := binding.SubmitImport(t.Context(), dataexchange.ImportRequest{
		Scope: scope, Provider: "identity", ObjectKey: "identity-portability", IdempotencyKey: "identity-import-1",
		Filename: "identity-bundle.json", ContentType: "application/json", Options: []byte(`{"provider_readiness":{"oidc":true}}`), Source: strings.NewReader(content),
	})
	if err != nil || replayed {
		t.Fatalf("submit artifact import: replayed=%v err=%v", replayed, err)
	}
	done := binding.Start(t.Context(), dataexchange.WorkerConfig{Enabled: true, PollInterval: time.Millisecond})
	t.Cleanup(func() { _ = binding.Close(context.Background()); <-done })
	completed := waitCompleted(t, binding, scope, job.ID)
	if completed.Status != "completed" || provider.validated != content || provider.applied != content || provider.validatedOptions != `{"provider_readiness":{"oidc":true}}` || provider.appliedOptions != provider.validatedOptions {
		t.Fatalf("completed=%+v validated=%q applied=%q", completed, provider.validated, provider.applied)
	}
}

func TestModulePersistsCanonicalExportArtifact(t *testing.T) {
	binding, _, provider := openArtifactTestBinding(t)
	scope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "actor"}
	job, _, err := binding.SubmitExport(t.Context(), dataexchange.ExportRequest{Scope: scope, Provider: "identity", ObjectKey: "identity-portability", IdempotencyKey: "identity-export-1"})
	if err != nil {
		t.Fatal(err)
	}
	done := binding.Start(t.Context(), dataexchange.WorkerConfig{Enabled: true, PollInterval: time.Millisecond})
	t.Cleanup(func() { _ = binding.Close(context.Background()); <-done })
	completed := waitCompleted(t, binding, scope, job.ID)
	if completed.Status != "completed" {
		t.Fatalf("completed=%+v", completed)
	}
	artifact, err := binding.Download(jobAuthorizedContext(t.Context(), scope, dataexchange.ActionDataExchangeJobDownload), dataexchange.JobRequest{Scope: scope, JobID: job.ID})
	if err != nil {
		t.Fatal(err)
	}
	defer artifact.Content.Close()
	raw, err := io.ReadAll(artifact.Content)
	if err != nil || string(raw) != `{"datasets":[{"name":"users"}]}` || artifact.ContentType != "application/json" {
		t.Fatalf("artifact=%+v content=%q err=%v", artifact, raw, err)
	}
	if len(provider.completions) != 1 || provider.completions[0].Rows != 2 {
		t.Fatalf("completions=%+v", provider.completions)
	}
}

func TestExpiredLeaseResumesCanonicalArtifactFromByteCursor(t *testing.T) {
	binding, store, db, provider := openArtifactTestBindingStore(t)
	const chunkSize = 8 << 20
	provider.content = bytes.Repeat([]byte("identity-bundle-byte\n"), chunkSize/21+500)
	scope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "actor"}
	job, _, err := binding.SubmitExport(t.Context(), dataexchange.ExportRequest{Scope: scope, Provider: "identity", ObjectKey: "identity-portability", IdempotencyKey: "resume-artifact"})
	if err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := store.Claim(t.Context(), "worker-one", time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if err := store.CommitResultPage(t.Context(), claimed, 0, provider.content[:chunkSize], strconv.Itoa(chunkSize), 2, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE _data_exchange_jobs SET lease_expires_at=? WHERE id=?`, time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano), job.ID); err != nil {
		t.Fatal(err)
	}
	reclaimed, ok, err := store.Claim(t.Context(), "worker-two", time.Second)
	if err != nil || !ok {
		t.Fatalf("reclaim: ok=%v err=%v", ok, err)
	}
	if reclaimed.Job.Cursor != strconv.Itoa(chunkSize) || reclaimed.Job.ResultChunks != 1 {
		t.Fatalf("cursor=%q chunks=%d", reclaimed.Job.Cursor, reclaimed.Job.ResultChunks)
	}
	if err := binding.Process(t.Context(), reclaimed); err != nil {
		t.Fatal(err)
	}
	artifact, err := binding.Download(jobAuthorizedContext(t.Context(), scope, dataexchange.ActionDataExchangeJobDownload), dataexchange.JobRequest{Scope: scope, JobID: job.ID})
	if err != nil {
		t.Fatal(err)
	}
	defer artifact.Content.Close()
	content, err := io.ReadAll(artifact.Content)
	if err != nil || !bytes.Equal(content, provider.content) {
		t.Fatalf("resumed artifact bytes=%d want=%d err=%v", len(content), len(provider.content), err)
	}
	if len(provider.completions) != 1 || provider.completions[0].ResultChunks != 2 {
		t.Fatalf("completions=%+v", provider.completions)
	}
}

func TestExpiredLeaseRejectsChangedCanonicalArtifactPrefix(t *testing.T) {
	binding, store, db, provider := openArtifactTestBindingStore(t)
	const chunkSize = 8 << 20
	provider.content = bytes.Repeat([]byte("a"), chunkSize+64)
	scope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "actor"}
	job, _, err := binding.SubmitExport(t.Context(), dataexchange.ExportRequest{Scope: scope, Provider: "identity", ObjectKey: "identity-portability", IdempotencyKey: "changed-artifact"})
	if err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := store.Claim(t.Context(), "worker-one", time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if err := store.CommitResultPage(t.Context(), claimed, 0, append([]byte(nil), provider.content[:chunkSize]...), strconv.Itoa(chunkSize), 2, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE _data_exchange_jobs SET lease_expires_at=? WHERE id=?`, time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano), job.ID); err != nil {
		t.Fatal(err)
	}
	provider.content = append([]byte(nil), provider.content...)
	provider.content[0] = 'b'
	reclaimed, ok, err := store.Claim(t.Context(), "worker-two", time.Second)
	if err != nil || !ok {
		t.Fatalf("reclaim: ok=%v err=%v", ok, err)
	}
	if err := binding.Process(t.Context(), reclaimed); err == nil || !strings.Contains(err.Error(), "prefix changed") {
		t.Fatalf("changed canonical artifact was accepted: %v", err)
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
	if p.validated != 3 || p.applied != 0 {
		t.Fatalf("validated=%d applied=%d; want three bounded attempts and no apply", p.validated, p.applied)
	}
}

func TestJobRequestConstraintsApplyAtomicallyToQueryAndCancel(t *testing.T) {
	binding, _, _ := openTestBinding(t)
	scope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "actor"}
	job, _, err := binding.SubmitExport(t.Context(), dataexchange.ExportRequest{
		Scope: scope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "ownership-constraint",
	})
	if err != nil {
		t.Fatal(err)
	}
	wrongOwner := dataexchange.JobRequest{Scope: scope, JobID: job.ID, Provider: "reports", Operation: "export"}
	if _, err := binding.Job(jobAuthorizedContext(t.Context(), scope, dataexchange.ActionDataExchangeJobGet), wrongOwner); !errors.Is(err, dataexchange.ErrJobNotFound) {
		t.Fatalf("cross-provider query error=%v; want ErrJobNotFound", err)
	}
	if _, err := binding.Cancel(jobAuthorizedContext(t.Context(), scope, dataexchange.ActionDataExchangeJobCancel), wrongOwner); !errors.Is(err, dataexchange.ErrJobNotFound) {
		t.Fatalf("cross-provider cancel error=%v; want ErrJobNotFound", err)
	}
	current, err := binding.Job(jobAuthorizedContext(t.Context(), scope, dataexchange.ActionDataExchangeJobGet), dataexchange.JobRequest{Scope: scope, JobID: job.ID, Provider: "records", Operation: "export"})
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != "queued" {
		t.Fatalf("cross-provider cancel changed status to %q", current.Status)
	}
	if _, err := binding.Job(jobAuthorizedContext(t.Context(), scope, dataexchange.ActionDataExchangeJobGet), dataexchange.JobRequest{Scope: scope, JobID: job.ID, Provider: "records", Operation: "import"}); !errors.Is(err, dataexchange.ErrJobNotFound) {
		t.Fatalf("cross-operation query error=%v; want ErrJobNotFound", err)
	}
}

func TestArtifactDownloadRejectsExpiredAndCorruptContent(t *testing.T) {
	completeExport := func(t *testing.T) (dataexchange.Binding, *sql.DB, dataexchange.Scope, string) {
		t.Helper()
		binding, _, db := openTestBindingStore(t)
		scope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "actor"}
		job, _, err := binding.SubmitExport(t.Context(), dataexchange.ExportRequest{
			Scope: scope, Provider: "records", ObjectKey: "contact", IdempotencyKey: t.Name(),
		})
		if err != nil {
			t.Fatal(err)
		}
		done := binding.Start(t.Context(), dataexchange.WorkerConfig{Enabled: true, PollInterval: time.Millisecond})
		t.Cleanup(func() { _ = binding.Close(context.Background()); <-done })
		if completed := waitCompleted(t, binding, scope, job.ID); completed.Status != "completed" {
			t.Fatalf("completed=%+v", completed)
		}
		return binding, db, scope, job.ID
	}

	t.Run("expired", func(t *testing.T) {
		binding, db, scope, jobID := completeExport(t)
		if _, err := db.Exec(`UPDATE _data_exchange_artifacts SET expires_at=? WHERE job_id=?`, time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano), jobID); err != nil {
			t.Fatal(err)
		}
		artifact, err := binding.Download(jobAuthorizedContext(t.Context(), scope, dataexchange.ActionDataExchangeJobDownload), dataexchange.JobRequest{Scope: scope, JobID: jobID, Provider: "records", Operation: "export"})
		if err != nil {
			t.Fatal(err)
		}
		defer artifact.Content.Close()
		if _, err := io.ReadAll(artifact.Content); !errors.Is(err, dataexchange.ErrArtifactExpired) {
			t.Fatalf("expired artifact read error=%v", err)
		}
	})

	t.Run("chunk sha", func(t *testing.T) {
		binding, db, scope, jobID := completeExport(t)
		if _, err := db.Exec(`UPDATE _data_exchange_job_chunks SET content_sha256='bad' WHERE job_id=? AND direction='result'`, jobID); err != nil {
			t.Fatal(err)
		}
		artifact, err := binding.Download(jobAuthorizedContext(t.Context(), scope, dataexchange.ActionDataExchangeJobDownload), dataexchange.JobRequest{Scope: scope, JobID: jobID, Provider: "records", Operation: "export"})
		if err != nil {
			t.Fatal(err)
		}
		defer artifact.Content.Close()
		if _, err := io.ReadAll(artifact.Content); !errors.Is(err, dataexchange.ErrContentCorrupt) {
			t.Fatalf("corrupt chunk error=%v", err)
		}
	})

	t.Run("artifact identity", func(t *testing.T) {
		binding, db, scope, jobID := completeExport(t)
		if _, err := db.Exec(`UPDATE _data_exchange_artifacts SET size_bytes=size_bytes+1 WHERE job_id=?`, jobID); err != nil {
			t.Fatal(err)
		}
		artifact, err := binding.Download(jobAuthorizedContext(t.Context(), scope, dataexchange.ActionDataExchangeJobDownload), dataexchange.JobRequest{Scope: scope, JobID: jobID, Provider: "records", Operation: "export"})
		if err != nil {
			t.Fatal(err)
		}
		defer artifact.Content.Close()
		if _, err := io.ReadAll(artifact.Content); !errors.Is(err, dataexchange.ErrContentCorrupt) {
			t.Fatalf("artifact identity error=%v", err)
		}
	})
}

func TestExpiredLeaseResumesExportFromAtomicCursor(t *testing.T) {
	contract, store, db := openTestBindingStore(t)
	b := contract.(*exchange.Binding)
	scope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "actor"}
	job, _, err := b.SubmitExport(context.Background(), dataexchange.ExportRequest{Scope: scope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "resume-export"})
	if err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := store.Claim(context.Background(), "worker-one", time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	first := []byte("id,name\n1,one\n")
	if err = store.CommitResultPage(context.Background(), claimed, 0, first, "next", 1, 2); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`UPDATE _data_exchange_jobs SET lease_expires_at=? WHERE id=?`, time.Now().UTC().Add(-time.Second).Format(time.RFC3339Nano), job.ID); err != nil {
		t.Fatal(err)
	}
	reclaimed, ok, err := store.Claim(context.Background(), "worker-two", time.Second)
	if err != nil || !ok {
		t.Fatalf("reclaim: ok=%v err=%v", ok, err)
	}
	if reclaimed.Job.Cursor != "next" || reclaimed.Job.ResultChunks != 1 {
		t.Fatalf("cursor=%q chunks=%d", reclaimed.Job.Cursor, reclaimed.Job.ResultChunks)
	}
	if err = store.Progress(context.Background(), claimed, 2, 2); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("stale worker progress error=%v", err)
	}
	if err = b.Process(context.Background(), reclaimed); err != nil {
		t.Fatal(err)
	}
	artifact, err := b.Download(jobAuthorizedContext(context.Background(), scope, dataexchange.ActionDataExchangeJobDownload), dataexchange.JobRequest{Scope: scope, JobID: job.ID})
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

func TestExportIdempotencyRejectsChangedOwnerReference(t *testing.T) {
	b, _, _ := openTestBinding(t)
	scope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "actor"}
	request := dataexchange.ExportRequest{Scope: scope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "same-export", ReferenceID: "audit-one"}
	if _, replayed, err := b.SubmitExport(t.Context(), request); err != nil || replayed {
		t.Fatalf("first submit replayed=%v err=%v", replayed, err)
	}
	request.ReferenceID = "audit-two"
	if _, _, err := b.SubmitExport(t.Context(), request); err == nil || !strings.Contains(err.Error(), "different request") {
		t.Fatalf("changed owner reference was accepted: %v", err)
	}
}

func TestModuleJobOwnerScopeKeepsActorAndWorkspaceIsolation(t *testing.T) {
	binding, _, _ := openTestBinding(t)
	peerScope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "bob"}
	peer, _, err := binding.SubmitExport(t.Context(), dataexchange.ExportRequest{
		Scope: peerScope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "owner-peer",
	})
	if err != nil {
		t.Fatal(err)
	}
	foreignScope := dataexchange.Scope{WorkspaceID: "workspace-other", ActorID: "bob"}
	foreign, _, err := binding.SubmitExport(t.Context(), dataexchange.ExportRequest{
		Scope: foreignScope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "owner-foreign",
	})
	if err != nil {
		t.Fatal(err)
	}

	managerScope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "manager"}
	managerCtx := jobAuthorizedContext(t.Context(), managerScope, dataexchange.ActionDataExchangeJobGet)
	if _, err := binding.Job(managerCtx, dataexchange.JobRequest{Scope: managerScope, JobID: peer.ID}); !errors.Is(err, dataexchange.ErrJobNotFound) {
		t.Fatalf("peer read error=%v", err)
	}
	bobCtx := jobAuthorizedContext(t.Context(), peerScope, dataexchange.ActionDataExchangeJobGet)
	if _, err := binding.Job(bobCtx, dataexchange.JobRequest{Scope: peerScope, JobID: peer.ID}); err != nil {
		t.Fatalf("owner read: %v", err)
	}
	if _, err := binding.Job(bobCtx, dataexchange.JobRequest{Scope: peerScope, JobID: foreign.ID}); !errors.Is(err, dataexchange.ErrJobNotFound) {
		t.Fatalf("cross-workspace read error=%v", err)
	}
}

func TestModuleCancelRequiresExactPermissionAndOwnerScope(t *testing.T) {
	binding, _, _ := openTestBinding(t)
	peerScope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "bob"}
	peer, _, err := binding.SubmitExport(t.Context(), dataexchange.ExportRequest{
		Scope: peerScope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "cancel-peer",
	})
	if err != nil {
		t.Fatal(err)
	}

	managerScope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "manager"}
	managerCancel := jobAuthorizedContext(t.Context(), managerScope, dataexchange.ActionDataExchangeJobCancel)
	if _, err := binding.Cancel(managerCancel, dataexchange.JobRequest{Scope: managerScope, JobID: peer.ID}); !errors.Is(err, dataexchange.ErrJobNotFound) {
		t.Fatalf("peer cancel error=%v", err)
	}

	bobRead := jobAuthorizedContext(t.Context(), peerScope, dataexchange.ActionDataExchangeJobGet)
	current, err := binding.Job(bobRead, dataexchange.JobRequest{Scope: peerScope, JobID: peer.ID})
	if err != nil || current.Status != "queued" {
		t.Fatalf("queued candidate: job=%+v err=%v", current, err)
	}
	if _, err := binding.Cancel(bobRead, dataexchange.JobRequest{Scope: peerScope, JobID: peer.ID}); err == nil || !strings.Contains(err.Error(), "job_permission_denied") {
		t.Fatalf("different exact Permission cancelled job: %v", err)
	}
	bobCancel := jobAuthorizedContext(t.Context(), peerScope, dataexchange.ActionDataExchangeJobCancel)
	cancelled, err := binding.Cancel(bobCancel, dataexchange.JobRequest{Scope: peerScope, JobID: peer.ID})
	if err != nil || cancelled.Status != "cancelled" {
		t.Fatalf("cancel=%+v err=%v", cancelled, err)
	}
}

func TestModuleArtifactDownloadRequiresExactPermissionAndWorkspace(t *testing.T) {
	binding, _, _ := openTestBinding(t)
	bobScope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "bob"}
	job, _, err := binding.SubmitExport(t.Context(), dataexchange.ExportRequest{
		Scope: bobScope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "download-scope",
	})
	if err != nil {
		t.Fatal(err)
	}
	done := binding.Start(t.Context(), dataexchange.WorkerConfig{Enabled: true, PollInterval: time.Millisecond})
	t.Cleanup(func() { _ = binding.Close(context.Background()); <-done })
	if completed := waitCompleted(t, binding, bobScope, job.ID); completed.Status != "completed" {
		t.Fatalf("completed=%+v", completed)
	}

	aliceScope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "alice"}
	wrongExact := jobAuthorizedContext(t.Context(), aliceScope, dataexchange.ActionDataExchangeJobGet)
	if _, err := binding.Download(wrongExact, dataexchange.JobRequest{Scope: aliceScope, JobID: job.ID}); err == nil || !strings.Contains(err.Error(), "job_permission_denied") {
		t.Fatalf("different exact Permission downloaded artifact: %v", err)
	}
	aliceDownload := jobAuthorizedContext(t.Context(), aliceScope, dataexchange.ActionDataExchangeJobDownload)
	if _, err := binding.Download(aliceDownload, dataexchange.JobRequest{Scope: aliceScope, JobID: job.ID}); !errors.Is(err, dataexchange.ErrJobNotFound) {
		t.Fatalf("peer download error=%v", err)
	}
	bobDownload := jobAuthorizedContext(t.Context(), bobScope, dataexchange.ActionDataExchangeJobDownload)
	artifact, err := binding.Download(bobDownload, dataexchange.JobRequest{Scope: bobScope, JobID: job.ID})
	if err != nil {
		t.Fatal(err)
	}
	defer artifact.Content.Close()
	if _, err := io.ReadAll(artifact.Content); err != nil {
		t.Fatal(err)
	}
	foreignScope := dataexchange.Scope{WorkspaceID: "workspace-other", ActorID: "bob"}
	foreignCtx := jobAuthorizedContext(t.Context(), foreignScope, dataexchange.ActionDataExchangeJobDownload)
	if _, err := binding.Download(foreignCtx, dataexchange.JobRequest{Scope: foreignScope, JobID: job.ID}); !errors.Is(err, dataexchange.ErrJobNotFound) {
		t.Fatalf("cross-workspace download error=%v", err)
	}
}
