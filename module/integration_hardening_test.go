package module

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	dataexchangemodel "github.com/domainry/domainry-data-exchange/internal/domain/dataexchange/model"
	"github.com/domainry/domainry-foundation/modulehttp"
	_ "modernc.org/sqlite"
)

type ledgerMigrationRegistrar struct {
	db    *sql.DB
	mu    sync.Mutex
	calls int
}

func (*ledgerMigrationRegistrar) Driver() string { return "sqlite" }
func (*ledgerMigrationRegistrar) Schema() string { return "" }

func (r *ledgerMigrationRegistrar) ApplyOwnedMigrations(ctx context.Context, owner string, migrations []modulehost.Migration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if owner != "data_exchange" {
		return fmt.Errorf("unexpected migration owner %q", owner)
	}
	for _, migration := range migrations {
		path := owner + ":" + migration.ID
		checksum := sha256.Sum256([]byte(migration.SQL))
		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		var applied string
		err = tx.QueryRowContext(ctx, `SELECT checksum FROM _schema_migrations WHERE path = ?`, path).Scan(&applied)
		if err == nil {
			_ = tx.Rollback()
			if applied != hex.EncodeToString(checksum[:]) {
				return fmt.Errorf("migration checksum drift for %s", path)
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			_ = tx.Rollback()
			return err
		}
		if _, err = tx.ExecContext(ctx, migration.SQL); err != nil {
			_ = tx.Rollback()
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO _schema_migrations(path, owner, checksum) VALUES (?, ?, ?)`, path, owner, hex.EncodeToString(checksum[:])); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

type scopedCall struct{ workspace, actor string }

type integratedModuleHost struct {
	db         *sql.DB
	registrar  *ledgerMigrationRegistrar
	imports    map[string]modulehost.ImportProvider
	exports    map[string]modulehost.ExportProvider
	scopeMu    sync.Mutex
	scopeCalls []scopedCall
}

func (h *integratedModuleHost) Database() *sql.DB                         { return h.db }
func (h *integratedModuleHost) Migrations() modulehost.MigrationRegistrar { return h.registrar }
func (h *integratedModuleHost) WorkspaceContext(ctx context.Context, workspace, actor string) context.Context {
	h.scopeMu.Lock()
	h.scopeCalls = append(h.scopeCalls, scopedCall{workspace: workspace, actor: actor})
	h.scopeMu.Unlock()
	return ctx
}
func (h *integratedModuleHost) ImportProvider(key string) (modulehost.ImportProvider, bool) {
	provider, ok := h.imports[key]
	return provider, ok
}
func (h *integratedModuleHost) ExportProvider(key string) (modulehost.ExportProvider, bool) {
	provider, ok := h.exports[key]
	return provider, ok
}

func newIntegratedModuleHost(t *testing.T, imports map[string]modulehost.ImportProvider, exports map[string]modulehost.ExportProvider) *integratedModuleHost {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "host.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE _schema_migrations (path TEXT PRIMARY KEY, owner TEXT NOT NULL, checksum TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO _schema_migrations(path, owner, checksum) VALUES ('host:bootstrap', 'host', 'host-owned')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE host_business_state (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	registrar := &ledgerMigrationRegistrar{db: db}
	return &integratedModuleHost{db: db, registrar: registrar, imports: imports, exports: exports}
}

func TestModuleBindingUsesHostDatabaseLedgerAndServesDurableWorkflow(t *testing.T) {
	importProvider := &testImportProvider{}
	exportProvider := &testExportProvider{}
	host := newIntegratedModuleHost(t,
		map[string]modulehost.ImportProvider{"records": importProvider},
		map[string]modulehost.ExportProvider{"records": exportProvider},
	)
	application := dataexchange.ApplicationRef{ApplicationID: "app", RuntimeID: "runtime"}
	binding, err := NewFactory(Options{}).OpenModule(t.Context(), application, host)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewFactory(Options{}).OpenModule(t.Context(), application, host)
	if err != nil {
		t.Fatalf("reopen through the host migration registrar: %v", err)
	}
	t.Cleanup(func() { _ = binding.Close(context.Background()); _ = second.Close(context.Background()) })

	var ledgerRows, ledgerTables int
	if err := host.db.QueryRow(`SELECT COUNT(*) FROM _schema_migrations`).Scan(&ledgerRows); err != nil || ledgerRows != 10 {
		t.Fatalf("shared host migration ledger rows=%d err=%v", ledgerRows, err)
	}
	if err := host.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name LIKE '%schema_migrations%'`).Scan(&ledgerTables); err != nil || ledgerTables != 1 {
		t.Fatalf("migration ledger tables=%d err=%v", ledgerTables, err)
	}
	if host.registrar.calls != 2 {
		t.Fatalf("host migration registrar calls=%d", host.registrar.calls)
	}

	scope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "actor", RoleKey: "member"}
	importRequest := func() dataexchange.ImportRequest {
		return dataexchange.ImportRequest{
			Scope: scope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "workflow-import",
			Filename: "contacts.csv", ContentType: "text/csv", Source: strings.NewReader("id,name\n1,one\n2,two\n"),
		}
	}
	importJob, replayed, err := binding.SubmitImport(t.Context(), importRequest())
	if err != nil || replayed {
		t.Fatalf("submit import: replayed=%v err=%v", replayed, err)
	}
	replayedImport, replayed, err := binding.SubmitImport(t.Context(), importRequest())
	if err != nil || !replayed || replayedImport.ID != importJob.ID {
		t.Fatalf("replay import: job=%+v replayed=%v err=%v", replayedImport, replayed, err)
	}
	exportRequest := dataexchange.ExportRequest{
		Scope: scope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "workflow-export", ReferenceID: "audit-1",
	}
	exportJob, replayed, err := binding.SubmitExport(t.Context(), exportRequest)
	if err != nil || replayed {
		t.Fatalf("submit export: replayed=%v err=%v", replayed, err)
	}
	replayedExport, replayed, err := binding.SubmitExport(t.Context(), exportRequest)
	if err != nil || !replayed || replayedExport.ID != exportJob.ID {
		t.Fatalf("replay export: job=%+v replayed=%v err=%v", replayedExport, replayed, err)
	}

	done := binding.Start(t.Context(), dataexchange.WorkerConfig{Enabled: true, PollInterval: time.Millisecond, BatchSize: 2, LeaseTTL: time.Second})
	t.Cleanup(func() { _ = binding.Close(context.Background()); <-done })
	if completed := waitCompleted(t, binding, scope, importJob.ID); completed.Status != "completed" || completed.Checkpoint != 2 || completed.Total != 2 {
		t.Fatalf("import completion=%+v", completed)
	}
	if completed := waitCompleted(t, binding, scope, exportJob.ID); completed.Status != "completed" || completed.Checkpoint != 2 || completed.Total != 2 || completed.ResultChunks != 2 || completed.ArtifactID == "" {
		t.Fatalf("export completion=%+v", completed)
	}

	var jobs, sourceChunks, resultChunks, artifacts int
	for query, destination := range map[string]*int{
		`SELECT COUNT(*) FROM _data_exchange_jobs`: &jobs,
		`SELECT COUNT(*) FROM _data_exchange_job_chunks WHERE job_id = '` + importJob.ID + `' AND direction='source'`: &sourceChunks,
		`SELECT COUNT(*) FROM _data_exchange_job_chunks WHERE job_id = '` + exportJob.ID + `' AND direction='result'`: &resultChunks,
		`SELECT COUNT(*) FROM _data_exchange_artifacts WHERE job_id = '` + exportJob.ID + `'`:                         &artifacts,
	} {
		if err := host.db.QueryRow(query).Scan(destination); err != nil {
			t.Fatal(err)
		}
	}
	if jobs != 2 || sourceChunks != 1 || resultChunks != 2 || artifacts != 1 {
		t.Fatalf("durable rows jobs=%d source_chunks=%d result_chunks=%d artifacts=%d", jobs, sourceChunks, resultChunks, artifacts)
	}

	httpProvider, ok := binding.(modulehttp.Provider)
	if !ok || len(httpProvider.HTTPAdapters()) != 1 {
		t.Fatal("module binding did not expose exactly one HTTP adapter")
	}
	adapter := httpProvider.HTTPAdapters()[0]
	request := httptest.NewRequest(http.MethodGet, "/data-exchange/jobs/"+exportJob.ID+"/download?provider=records&operation=export", nil)
	request = request.WithContext(jobAuthorizedContext(request.Context(), scope, dataexchange.ActionDataExchangeJobDownload))
	response := httptest.NewRecorder()
	adapter.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != "id,name\n1,one\n2,two\n" || response.Header().Get("Content-Disposition") != "attachment; filename=governed-contact.csv" {
		t.Fatalf("download status=%d headers=%v body=%q", response.Code, response.Header(), response.Body.String())
	}

	host.scopeMu.Lock()
	scopeCalls := append([]scopedCall(nil), host.scopeCalls...)
	host.scopeMu.Unlock()
	if !slices.Contains(scopeCalls, scopedCall{workspace: scope.WorkspaceID, actor: scope.ActorID}) ||
		!slices.Contains(scopeCalls, scopedCall{workspace: scope.WorkspaceID, actor: application.RuntimeID + ":" + application.ApplicationID}) {
		t.Fatalf("host workspace context calls=%+v", scopeCalls)
	}
}

type erroringReader struct {
	payload []byte
	read    bool
}

func (r *erroringReader) Read(buffer []byte) (int, error) {
	if r.read {
		return 0, errors.New("source failed")
	}
	r.read = true
	return copy(buffer, r.payload), errors.New("source failed")
}

func TestStoreTransactionsRollbackStagedContentAndTerminalWrites(t *testing.T) {
	binding, store, db := openTestBindingStore(t)
	scope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "actor"}
	_, _, err := binding.SubmitImport(t.Context(), dataexchange.ImportRequest{
		Scope: scope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "broken-source",
		Source: &erroringReader{payload: []byte("id,name\n1,one\n")},
	})
	if !errors.Is(err, dataexchange.ErrSourceUnreadable) {
		t.Fatalf("source error=%v", err)
	}
	for _, table := range []string{"_data_exchange_jobs", "_data_exchange_job_chunks", "_data_exchange_queue_scopes"} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s rows=%d err=%v after rolled-back import", table, count, err)
		}
	}

	job, _, err := binding.SubmitExport(t.Context(), dataexchange.ExportRequest{
		Scope: scope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "stale-transaction",
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := store.Claim(t.Context(), "worker", time.Second)
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}
	stale := claimed
	stale.Job.FencingToken--
	if err := store.CommitResultPage(t.Context(), stale, 0, []byte("id\n1\n"), "", 1, 1); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("stale page commit error=%v", err)
	}
	var chunks, checkpoint, resultChunks int
	if err := db.QueryRow(`SELECT COUNT(*) FROM _data_exchange_job_chunks WHERE job_id=? AND direction='result'`, job.ID).Scan(&chunks); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT checkpoint_value, result_chunks FROM _data_exchange_jobs WHERE id=?`, job.ID).Scan(&checkpoint, &resultChunks); err != nil {
		t.Fatal(err)
	}
	if chunks != 0 || checkpoint != 0 || resultChunks != 0 {
		t.Fatalf("stale page transaction leaked chunks=%d checkpoint=%d result_chunks=%d", chunks, checkpoint, resultChunks)
	}

	artifact := &dataexchangemodel.ArtifactRecord{ID: job.ID + ":artifact", Filename: "result.csv", ContentType: "text/csv", SHA256: strings.Repeat("a", 64), Size: 5, ExpiresAt: time.Now().Add(time.Hour)}
	if err := store.Complete(t.Context(), stale, artifact); err == nil || !strings.Contains(err.Error(), "no longer running") {
		t.Fatalf("stale completion error=%v", err)
	}
	var artifactRows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM _data_exchange_artifacts WHERE job_id=?`, job.ID).Scan(&artifactRows); err != nil || artifactRows != 0 {
		t.Fatalf("rolled-back artifact rows=%d err=%v", artifactRows, err)
	}
}

type recoveringExportProvider struct {
	mu          sync.Mutex
	cursors     []string
	failed      bool
	completions int
}

func (*recoveringExportProvider) PlanExport(_ context.Context, request dataexchange.ExportPlanRequest) (dataexchange.ExportPlan, error) {
	return dataexchange.ExportPlan{Filename: "recovered.csv", ContentType: "text/csv", ExpiresAt: request.CreatedAt.Add(time.Hour)}, nil
}

func (p *recoveringExportProvider) ReadExportPage(_ context.Context, request dataexchange.ExportPageRequest) (dataexchange.ExportPage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cursors = append(p.cursors, request.Cursor)
	if request.Cursor == "" {
		return dataexchange.ExportPage{Columns: []string{"id"}, Rows: [][]string{{"1"}}, NextCursor: "next", Total: 2}, nil
	}
	if !p.failed {
		p.failed = true
		return dataexchange.ExportPage{}, errors.New("temporary export failure")
	}
	return dataexchange.ExportPage{Columns: []string{"id"}, Rows: [][]string{{"2"}}, Total: 2}, nil
}

func (p *recoveringExportProvider) CompleteExport(context.Context, dataexchange.ExportCompletion) error {
	p.mu.Lock()
	p.completions++
	p.mu.Unlock()
	return nil
}

func TestWorkerRecoversPartialExportWithoutDuplicatingCommittedChunks(t *testing.T) {
	provider := &recoveringExportProvider{}
	host := newIntegratedModuleHost(t, map[string]modulehost.ImportProvider{}, map[string]modulehost.ExportProvider{"records": provider})
	binding, err := NewFactory(Options{}).OpenModule(t.Context(), dataexchange.ApplicationRef{ApplicationID: "app", RuntimeID: "runtime"}, host)
	if err != nil {
		t.Fatal(err)
	}
	scope := dataexchange.Scope{WorkspaceID: "workspace", ActorID: "actor"}
	job, _, err := binding.SubmitExport(t.Context(), dataexchange.ExportRequest{Scope: scope, Provider: "records", ObjectKey: "contact", IdempotencyKey: "recover-once"})
	if err != nil {
		t.Fatal(err)
	}
	done := binding.Start(t.Context(), dataexchange.WorkerConfig{Enabled: true, PollInterval: 5 * time.Millisecond, LeaseTTL: time.Second})
	t.Cleanup(func() { _ = binding.Close(context.Background()); <-done })
	completed := waitCompleted(t, binding, scope, job.ID)
	if completed.Status != "completed" || completed.ErrorCode != "" || completed.ResultChunks != 2 || completed.Checkpoint != 2 {
		t.Fatalf("recovered job=%+v", completed)
	}
	artifact, err := binding.Download(jobAuthorizedContext(t.Context(), scope, dataexchange.ActionDataExchangeJobDownload), dataexchange.JobRequest{Scope: scope, JobID: job.ID})
	if err != nil {
		t.Fatal(err)
	}
	defer artifact.Content.Close()
	content, err := io.ReadAll(artifact.Content)
	if err != nil || string(content) != "id\n1\n2\n" {
		t.Fatalf("artifact=%q err=%v", content, err)
	}
	var attempts, chunks int
	if err := host.db.QueryRow(`SELECT attempt_count FROM _data_exchange_jobs WHERE id=?`, job.ID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := host.db.QueryRow(`SELECT COUNT(*) FROM _data_exchange_job_chunks WHERE job_id=? AND direction='result'`, job.ID).Scan(&chunks); err != nil {
		t.Fatal(err)
	}
	provider.mu.Lock()
	cursors := append([]string(nil), provider.cursors...)
	completions := provider.completions
	provider.mu.Unlock()
	if attempts != 1 || chunks != 2 || !slices.Equal(cursors, []string{"", "next", "next"}) || completions != 1 {
		t.Fatalf("attempts=%d chunks=%d cursors=%v completions=%d", attempts, chunks, cursors, completions)
	}
}
