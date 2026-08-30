package module

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	"github.com/domainry/domainry-data-exchange/fileengine"
)

const importBatchRows = 500
const importMaxRows = 1_000_000
const importMaxColumns = 512
const exportPageMaxBytes = 16 << 20

type binding struct {
	application dataexchange.ApplicationRef
	host        modulehost.Host
	store       *sqlStore
	closeOnce   sync.Once
	cancel      context.CancelFunc
}

func newBinding(a dataexchange.ApplicationRef, h modulehost.Host, s *sqlStore) *binding {
	return &binding{application: a, host: h, store: s}
}
func (*binding) Descriptor() dataexchange.Descriptor {
	return dataexchange.Descriptor{ProtocolVersion: dataexchange.ProtocolVersionV1, Mode: dataexchange.DeploymentModeModule, Capabilities: []string{"streaming_import", "paged_export", "durable_chunks", "artifact_lifecycle"}}
}
func (b *binding) SubmitImport(ctx context.Context, r dataexchange.ImportRequest) (dataexchange.Job, bool, error) {
	if e := r.Scope.Validate(); e != nil {
		return dataexchange.Job{}, false, e
	}
	if r.Source == nil || strings.TrimSpace(r.Provider) == "" || strings.TrimSpace(r.ObjectKey) == "" || strings.TrimSpace(r.IdempotencyKey) == "" {
		return dataexchange.Job{}, false, fmt.Errorf("Data Exchange import request is incomplete")
	}
	if _, ok := b.host.ImportProvider(r.Provider); !ok {
		return dataexchange.Job{}, false, fmt.Errorf("Data Exchange import provider %q is unavailable", r.Provider)
	}
	return b.store.submitImport(ctx, r)
}
func (b *binding) SubmitExport(ctx context.Context, r dataexchange.ExportRequest) (dataexchange.Job, bool, error) {
	if e := r.Scope.Validate(); e != nil {
		return dataexchange.Job{}, false, e
	}
	if strings.TrimSpace(r.Provider) == "" || strings.TrimSpace(r.ObjectKey) == "" || strings.TrimSpace(r.IdempotencyKey) == "" {
		return dataexchange.Job{}, false, fmt.Errorf("Data Exchange export request is incomplete")
	}
	if _, ok := b.host.ExportProvider(r.Provider); !ok {
		return dataexchange.Job{}, false, fmt.Errorf("Data Exchange export provider %q is unavailable", r.Provider)
	}
	return b.store.submitExport(ctx, r)
}
func (b *binding) Job(ctx context.Context, r dataexchange.JobRequest) (dataexchange.Job, error) {
	if e := r.Scope.Validate(); e != nil {
		return dataexchange.Job{}, e
	}
	return b.store.job(ctx, r)
}
func (b *binding) Cancel(ctx context.Context, r dataexchange.JobRequest) (dataexchange.Job, error) {
	if e := r.Scope.Validate(); e != nil {
		return dataexchange.Job{}, e
	}
	return b.store.cancel(ctx, r)
}
func (b *binding) Download(ctx context.Context, r dataexchange.JobRequest) (dataexchange.Artifact, error) {
	if e := r.Scope.Validate(); e != nil {
		return dataexchange.Artifact{}, e
	}
	return b.store.artifact(ctx, r)
}
func (b *binding) Start(parent context.Context, c dataexchange.WorkerConfig) <-chan struct{} {
	done := make(chan struct{})
	if !c.Enabled {
		close(done)
		return done
	}
	ctx, cancel := context.WithCancel(parent)
	b.cancel = cancel
	if c.PollInterval <= 0 {
		c.PollInterval = 250 * time.Millisecond
	}
	go func() {
		defer close(done)
		owner := b.application.RuntimeID + ":" + b.application.ApplicationID
		t := time.NewTicker(c.PollInterval)
		defer t.Stop()
		for {
			if x, ok, e := b.store.claim(ctx, owner, c.LeaseTTL); e == nil && ok {
				if e = b.processWithHeartbeat(ctx, x, c.LeaseTTL); e != nil {
					failCtx := b.store.scoped(context.WithoutCancel(ctx), x.Scope.WorkspaceID, owner)
					_ = b.store.fail(failCtx, x, "processing_failed")
				}
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	return done
}

func (b *binding) processWithHeartbeat(parent context.Context, x workItem, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		interval := ttl / 3
		if interval < 10*time.Millisecond {
			interval = 10 * time.Millisecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := b.store.heartbeat(b.store.scoped(ctx, x.Scope.WorkspaceID, x.Job.LeaseOwner), x, ttl); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	err := b.process(ctx, x)
	cancel()
	<-done
	return err
}
func (b *binding) Close(context.Context) error {
	b.closeOnce.Do(func() {
		if b.cancel != nil {
			b.cancel()
		}
	})
	return nil
}
func (b *binding) process(ctx context.Context, x workItem) error {
	ctx = b.store.scoped(ctx, x.Scope.WorkspaceID, x.Scope.ActorID)
	switch x.Job.Operation {
	case "import":
		return b.processImport(ctx, x)
	case "export":
		return b.processExport(ctx, x)
	default:
		return fmt.Errorf("unsupported operation %q", x.Job.Operation)
	}
}

func (b *binding) processImport(ctx context.Context, x workItem) error {
	p, ok := b.host.ImportProvider(x.Job.Provider)
	if !ok {
		return fmt.Errorf("import provider unavailable")
	}
	total, rejected, e := b.readImport(ctx, x, p.ValidateImportBatch)
	if e != nil {
		return e
	}
	if rejected > 0 {
		return fmt.Errorf("Data Exchange import rejected %d rows", rejected)
	}
	if e = b.store.progress(ctx, x, 0, total); e != nil {
		return e
	}
	_, rejected, e = b.readImport(ctx, x, p.ApplyImportBatch)
	if e != nil {
		return e
	}
	if rejected > 0 {
		return fmt.Errorf("Data Exchange import apply rejected %d rows", rejected)
	}
	if e = b.store.progress(ctx, x, total, total); e != nil {
		return e
	}
	return b.store.complete(ctx, x, nil)
}

type importHandler func(context.Context, dataexchange.ImportBatch) (dataexchange.ImportBatchResult, error)

func (b *binding) readImport(ctx context.Context, x workItem, handle importHandler) (int, int, error) {
	source, e := b.store.chunks(ctx, x.Scope.WorkspaceID, x.Job.ID, "source")
	if e != nil {
		return 0, 0, e
	}
	defer source.Close()
	total, batchNo := 0, 0
	rejected := 0
	var headers []string
	rows := make([]dataexchange.ImportRow, 0, importBatchRows)
	flush := func() error {
		if len(rows) == 0 {
			return nil
		}
		batchNo++
		copyRows := append([]dataexchange.ImportRow(nil), rows...)
		result, err := handle(ctx, dataexchange.ImportBatch{Scope: x.Scope, ObjectKey: x.ObjectKey, JobID: x.Job.ID, ChunkID: fmt.Sprintf("%s:%d", x.Job.ID, batchNo), Headers: append([]string(nil), headers...), Rows: copyRows})
		rejected += result.Rejected
		rows = rows[:0]
		return err
	}
	headers, e = fileengine.DecodeCSV(ctx, source, fileengine.CSVDecodeLimits{MaxRows: importMaxRows, MaxColumns: importMaxColumns}, func(decodedHeaders []string, record fileengine.CSVRecord) error {
		headers = decodedHeaders
		total++
		rows = append(rows, dataexchange.ImportRow{Number: record.Number, Values: record.Values})
		if len(rows) == importBatchRows {
			return flush()
		}
		return nil
	})
	if e != nil {
		return total, rejected, e
	}
	if len(headers) == 0 {
		return total, rejected, fileengine.ErrHeaderRequired
	}
	if e = flush(); e != nil {
		return total, rejected, e
	}
	return total, rejected, nil
}

func (b *binding) processExport(ctx context.Context, x workItem) error {
	p, ok := b.host.ExportProvider(x.Job.Provider)
	if !ok {
		return fmt.Errorf("export provider unavailable")
	}
	plan := dataexchange.ExportPlan{
		Filename:    x.ObjectKey + ".csv",
		ContentType: "text/csv; charset=utf-8",
		ExpiresAt:   x.Job.CreatedAt.Add(24 * time.Hour),
	}
	if planner, planned := p.(modulehost.ExportPlanningProvider); planned {
		var e error
		plan, e = planner.PlanExport(ctx, dataexchange.ExportPlanRequest{Scope: x.Scope, ObjectKey: x.ObjectKey, Options: x.Payload, JobID: x.Job.ID, CreatedAt: x.Job.CreatedAt})
		if e != nil {
			return e
		}
	}
	plan.Filename = strings.TrimSpace(plan.Filename)
	plan.ContentType = strings.TrimSpace(plan.ContentType)
	if plan.Filename == "" || plan.ContentType == "" || plan.ExpiresAt.IsZero() {
		return fmt.Errorf("export provider returned an incomplete artifact plan")
	}
	cursor := x.Job.Cursor
	seq, total := x.Job.ResultChunks, x.Job.Checkpoint
	header := seq > 0
	// A terminal page commit stores an empty cursor. If the process crashes
	// between that commit and completion, resume finalization instead of reading
	// the export again and duplicating every row.
	for seq == 0 || cursor != "" {
		page, e := p.ReadExportPage(ctx, dataexchange.ExportPageRequest{Scope: x.Scope, ObjectKey: x.ObjectKey, Options: x.Payload, Cursor: cursor, PageSize: 500, JobID: x.Job.ID, ArtifactExpiresAt: plan.ExpiresAt})
		if e != nil {
			return e
		}
		var buf bytes.Buffer
		w := fileengine.NewCSVEncoder(&buf, exportPageMaxBytes)
		if !header {
			if len(page.Columns) == 0 {
				return fmt.Errorf("export provider returned no columns")
			}
			if e = w.Write(page.Columns); e != nil {
				return e
			}
			header = true
		}
		for _, row := range page.Rows {
			if len(row) != len(page.Columns) {
				return fmt.Errorf("export provider returned a row with %d columns; expected %d", len(row), len(page.Columns))
			}
			if e = w.Write(row); e != nil {
				return e
			}
		}
		if e = w.Close(); e != nil {
			return e
		}
		content := append([]byte(nil), buf.Bytes()...)
		nextCursor := page.NextCursor
		total += len(page.Rows)
		if len(content) > 0 {
			if e = b.store.commitResultPage(ctx, x, seq, content, nextCursor, total, page.Total); e != nil {
				return e
			}
			seq++
		}
		if nextCursor == "" {
			break
		}
		if nextCursor == cursor {
			return fmt.Errorf("export provider cursor did not advance")
		}
		cursor = nextCursor
	}
	sha, size, e := b.store.resultIdentity(ctx, x.Scope.WorkspaceID, x.Job.ID)
	if e != nil {
		return e
	}
	a := &artifactRecord{ID: x.Job.ID + ":artifact", Filename: plan.Filename, ContentType: plan.ContentType, SHA256: sha, Size: size, ExpiresAt: plan.ExpiresAt}
	if finalizer, finalizes := p.(modulehost.ExportCompletionProvider); finalizes {
		if e = finalizer.CompleteExport(ctx, dataexchange.ExportCompletion{
			Scope: x.Scope, ObjectKey: x.ObjectKey, Options: append([]byte(nil), x.Payload...), JobID: x.Job.ID,
			Artifact: dataexchange.Artifact{ID: a.ID, Filename: a.Filename, ContentType: a.ContentType, SHA256: a.SHA256, Size: a.Size, ExpiresAt: a.ExpiresAt},
			Rows:     total, ResultChunks: seq,
		}); e != nil {
			return e
		}
	}
	return b.store.complete(ctx, x, a)
}

var _ dataexchange.Binding = (*binding)(nil)
