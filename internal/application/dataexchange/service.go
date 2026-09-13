package dataexchangeapplication

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
	dataexchangemodel "github.com/domainry/domainry-data-exchange/internal/domain/dataexchange/model"
	dataexchangerepository "github.com/domainry/domainry-data-exchange/internal/domain/dataexchange/repository"
	dataexchangeservice "github.com/domainry/domainry-data-exchange/internal/domain/dataexchange/service"
)

const importBatchRows = 500
const importMaxRows = 1_000_000
const importMaxColumns = 512
const exportPageMaxBytes = 16 << 20

type Service struct {
	application dataexchange.ApplicationRef
	host        modulehost.Host
	store       dataexchangerepository.JobRepository
	closeOnce   sync.Once
	cancel      context.CancelFunc
}

func NewService(a dataexchange.ApplicationRef, h modulehost.Host, s dataexchangerepository.JobRepository) *Service {
	return &Service{application: a, host: h, store: s}
}
func (*Service) Descriptor() dataexchange.Descriptor {
	return dataexchange.Descriptor{ProtocolVersion: dataexchange.ProtocolVersionV1, Mode: dataexchange.DeploymentModeModule, Capabilities: []string{"streaming_import", "paged_export", "durable_chunks", "artifact_lifecycle", "subject_erasure"}}
}
func (b *Service) SubmitImport(ctx context.Context, r dataexchange.ImportRequest) (dataexchange.Job, bool, error) {
	if e := dataexchangeservice.ValidateImportRequest(r); e != nil {
		return dataexchange.Job{}, false, e
	}
	if _, ok := b.host.ImportProvider(r.Provider); !ok {
		return dataexchange.Job{}, false, fmt.Errorf("Data Exchange import provider %q is unavailable", r.Provider)
	}
	return b.store.SubmitImport(ctx, r)
}
func (b *Service) SubmitExport(ctx context.Context, r dataexchange.ExportRequest) (dataexchange.Job, bool, error) {
	if e := dataexchangeservice.ValidateExportRequest(r); e != nil {
		return dataexchange.Job{}, false, e
	}
	if _, ok := b.host.ExportProvider(r.Provider); !ok {
		return dataexchange.Job{}, false, fmt.Errorf("Data Exchange export provider %q is unavailable", r.Provider)
	}
	return b.store.SubmitExport(ctx, r)
}
func (b *Service) Jobs(ctx context.Context, r dataexchange.JobListRequest) ([]dataexchange.Job, error) {
	if e := r.Validate(); e != nil {
		return nil, e
	}
	access, err := dataexchangeservice.ResolveJobAccess(ctx, dataexchange.JobRequest{
		Scope: r.Scope, JobID: "list", Provider: r.Provider, Operation: r.Operation,
	}, dataexchange.ActionDataExchangeJobList)
	if err != nil {
		return nil, err
	}
	return b.store.Jobs(ctx, r, access)
}
func (b *Service) Job(ctx context.Context, r dataexchange.JobRequest) (dataexchange.Job, error) {
	return b.JobForAction(ctx, r, dataexchange.ActionDataExchangeJobGet)
}

// JobForAction is used by the shared download HTTP path so its prerequisite
// lookup is governed by the download Permission rather than accidentally
// requiring the independent get Permission as well.
func (b *Service) JobForAction(ctx context.Context, r dataexchange.JobRequest, permissionKey string) (dataexchange.Job, error) {
	if e := r.Validate(); e != nil {
		return dataexchange.Job{}, e
	}
	switch permissionKey {
	case dataexchange.ActionDataExchangeJobGet, dataexchange.ActionDataExchangeJobDownload:
	default:
		return dataexchange.Job{}, fmt.Errorf("unsupported Data Exchange job permission %q", permissionKey)
	}
	access, err := dataexchangeservice.ResolveJobAccess(ctx, r, permissionKey)
	if err != nil {
		return dataexchange.Job{}, err
	}
	return b.store.Job(ctx, r, access)
}
func (b *Service) Cancel(ctx context.Context, r dataexchange.JobRequest) (dataexchange.Job, error) {
	if e := r.Validate(); e != nil {
		return dataexchange.Job{}, e
	}
	access, err := dataexchangeservice.ResolveJobAccess(ctx, r, dataexchange.ActionDataExchangeJobCancel)
	if err != nil {
		return dataexchange.Job{}, err
	}
	return b.store.Cancel(ctx, r, access)
}
func (b *Service) Download(ctx context.Context, r dataexchange.JobRequest) (dataexchange.Artifact, error) {
	if e := r.Validate(); e != nil {
		return dataexchange.Artifact{}, e
	}
	access, err := dataexchangeservice.ResolveJobAccess(ctx, r, dataexchange.ActionDataExchangeJobDownload)
	if err != nil {
		return dataexchange.Artifact{}, err
	}
	return b.store.Artifact(ctx, r, access)
}
func (b *Service) Start(parent context.Context, c dataexchange.WorkerConfig) <-chan struct{} {
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
	if c.BatchSize <= 0 {
		c.BatchSize = 1
	}
	go func() {
		defer close(done)
		owner := b.application.RuntimeID + ":" + b.application.ApplicationID
		t := time.NewTicker(c.PollInterval)
		defer t.Stop()
		for {
			for claimed := 0; claimed < c.BatchSize; claimed++ {
				x, ok, e := b.store.Claim(ctx, owner, c.LeaseTTL)
				if e != nil || !ok {
					break
				}
				if e = b.processWithHeartbeat(ctx, x, c.LeaseTTL); e != nil {
					failCtx := b.store.Scoped(context.WithoutCancel(ctx), x.Scope.WorkspaceID, owner)
					plan := dataexchangeservice.PlanProcessingFailure(x.Attempts, "processing_failed", time.Now().UTC())
					_ = b.store.Fail(failCtx, x, plan)
				}
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

func (b *Service) processWithHeartbeat(parent context.Context, x dataexchangemodel.WorkItem, ttl time.Duration) error {
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
				if err := b.store.Heartbeat(b.store.Scoped(ctx, x.Scope.WorkspaceID, x.Job.LeaseOwner), x, ttl); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	err := b.Process(ctx, x)
	cancel()
	<-done
	return err
}
func (b *Service) Close(context.Context) error {
	b.closeOnce.Do(func() {
		if b.cancel != nil {
			b.cancel()
		}
	})
	return nil
}
func (b *Service) Process(ctx context.Context, x dataexchangemodel.WorkItem) error {
	ctx = b.store.Scoped(ctx, x.Scope.WorkspaceID, x.Scope.ActorID)
	switch x.Job.Operation {
	case "import":
		return b.processImport(ctx, x)
	case "export":
		return b.processExport(ctx, x)
	default:
		return fmt.Errorf("unsupported operation %q", x.Job.Operation)
	}
}

func (b *Service) processImport(ctx context.Context, x dataexchangemodel.WorkItem) error {
	p, ok := b.host.ImportProvider(x.Job.Provider)
	if !ok {
		return fmt.Errorf("import provider unavailable")
	}
	if artifactProvider, supportsArtifacts := p.(modulehost.ImportArtifactProvider); supportsArtifacts {
		return b.processImportArtifact(ctx, x, artifactProvider)
	}
	total, rejected, e := b.readImport(ctx, x, p.ValidateImportBatch)
	if e != nil {
		return e
	}
	if rejected > 0 {
		return fmt.Errorf("Data Exchange import rejected %d rows", rejected)
	}
	if e = b.store.Progress(ctx, x, 0, total); e != nil {
		return e
	}
	_, rejected, e = b.readImport(ctx, x, p.ApplyImportBatch)
	if e != nil {
		return e
	}
	if rejected > 0 {
		return fmt.Errorf("Data Exchange import apply rejected %d rows", rejected)
	}
	if e = b.store.Progress(ctx, x, total, total); e != nil {
		return e
	}
	return b.store.Complete(ctx, x, nil)
}

func (b *Service) processImportArtifact(ctx context.Context, x dataexchangemodel.WorkItem, provider modulehost.ImportArtifactProvider) error {
	metadata := struct {
		Filename    string          `json:"filename"`
		ContentType string          `json:"content_type"`
		Options     json.RawMessage `json:"options,omitempty"`
	}{}
	if err := json.Unmarshal(x.Payload, &metadata); err != nil {
		return fmt.Errorf("decode Data Exchange import artifact metadata: %w", err)
	}
	invoke := func(handle func(context.Context, dataexchange.ImportArtifact) (dataexchange.ImportArtifactResult, error)) (dataexchange.ImportArtifactResult, error) {
		source, err := b.store.Chunks(ctx, x.Scope.WorkspaceID, x.Job.ID, "source")
		if err != nil {
			return dataexchange.ImportArtifactResult{}, err
		}
		defer source.Close()
		return handle(ctx, dataexchange.ImportArtifact{
			Scope: x.Scope, ObjectKey: x.ObjectKey, JobID: x.Job.ID,
			Filename: metadata.Filename, ContentType: metadata.ContentType, Options: append([]byte(nil), metadata.Options...), Content: source,
		})
	}
	validated, err := invoke(provider.ValidateImportArtifact)
	if err != nil {
		return err
	}
	if err := b.store.Progress(ctx, x, 0, validated.Records); err != nil {
		return err
	}
	applied, err := invoke(provider.ApplyImportArtifact)
	if err != nil {
		return err
	}
	if applied.Records != validated.Records {
		return fmt.Errorf("Data Exchange import artifact applied %d records; validated %d", applied.Records, validated.Records)
	}
	if err := b.store.Progress(ctx, x, applied.Records, applied.Records); err != nil {
		return err
	}
	return b.store.Complete(ctx, x, nil)
}

type importHandler func(context.Context, dataexchange.ImportBatch) (dataexchange.ImportBatchResult, error)

func (b *Service) readImport(ctx context.Context, x dataexchangemodel.WorkItem, handle importHandler) (int, int, error) {
	source, e := b.store.Chunks(ctx, x.Scope.WorkspaceID, x.Job.ID, "source")
	if e != nil {
		return 0, 0, e
	}
	defer source.Close()
	total, batchNo := 0, 0
	rejected := 0
	var headers []string
	rows := make([]dataexchange.ImportRow, 0, importBatchRows)
	flush := func(final bool) error {
		if len(rows) == 0 {
			return nil
		}
		batchNo++
		copyRows := append([]dataexchange.ImportRow(nil), rows...)
		result, err := handle(ctx, dataexchange.ImportBatch{
			Scope: x.Scope, ObjectKey: x.ObjectKey, JobID: x.Job.ID, ChunkID: fmt.Sprintf("%s:%d", x.Job.ID, batchNo),
			Attempt: x.Attempts + 1, Final: final, Headers: append([]string(nil), headers...), Rows: copyRows,
		})
		rejected += result.Rejected
		rows = rows[:0]
		return err
	}
	headers, e = dataexchange.DecodeCSV(ctx, source, dataexchange.CSVDecodeLimits{MaxRows: importMaxRows, MaxColumns: importMaxColumns}, func(decodedHeaders []string, record dataexchange.CSVRecord) error {
		headers = decodedHeaders
		// Keep one full batch buffered until another row proves it is not the
		// terminal batch. This lets providers release attempt-scoped state on an
		// exact batch-size boundary without buffering more than one batch.
		if len(rows) == importBatchRows {
			if err := flush(false); err != nil {
				return err
			}
		}
		total++
		rows = append(rows, dataexchange.ImportRow{Number: record.Number, Values: record.Values})
		return nil
	})
	if e != nil {
		return total, rejected, e
	}
	if len(headers) == 0 {
		return total, rejected, dataexchange.ErrHeaderRequired
	}
	if e = flush(true); e != nil {
		return total, rejected, e
	}
	return total, rejected, nil
}

func (b *Service) processExport(ctx context.Context, x dataexchangemodel.WorkItem) error {
	p, ok := b.host.ExportProvider(x.Job.Provider)
	if !ok {
		return fmt.Errorf("export provider unavailable")
	}
	if artifactProvider, supportsArtifacts := p.(modulehost.ExportArtifactProvider); supportsArtifacts {
		return b.processExportArtifact(ctx, x, p, artifactProvider)
	}
	plan := dataexchange.ExportPlan{
		Filename:    x.ObjectKey + ".csv",
		ContentType: "text/csv; charset=utf-8",
		ExpiresAt:   x.Job.CreatedAt.Add(24 * time.Hour),
	}
	if planner, planned := p.(modulehost.ExportPlanningProvider); planned {
		var e error
		plan, e = planner.PlanExport(ctx, dataexchange.ExportPlanRequest{Scope: x.Scope, ObjectKey: x.ObjectKey, ReferenceID: x.Job.ReferenceID, Options: x.Payload, JobID: x.Job.ID, CreatedAt: x.Job.CreatedAt})
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
		page, e := p.ReadExportPage(ctx, dataexchange.ExportPageRequest{Scope: x.Scope, ObjectKey: x.ObjectKey, ReferenceID: x.Job.ReferenceID, Options: x.Payload, Cursor: cursor, PageSize: 500, JobID: x.Job.ID, ArtifactExpiresAt: plan.ExpiresAt})
		if e != nil {
			return e
		}
		var buf bytes.Buffer
		w := dataexchange.NewCSVEncoder(&buf, exportPageMaxBytes)
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
			if e = b.store.CommitResultPage(ctx, x, seq, content, nextCursor, total, page.Total); e != nil {
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
	sha, size, e := b.store.ResultIdentity(ctx, x.Scope.WorkspaceID, x.Job.ID)
	if e != nil {
		return e
	}
	a := &dataexchangemodel.ArtifactRecord{ID: x.Job.ID + ":artifact", Filename: plan.Filename, ContentType: plan.ContentType, SHA256: sha, Size: size, ExpiresAt: plan.ExpiresAt}
	if finalizer, finalizes := p.(modulehost.ExportCompletionProvider); finalizes {
		if e = finalizer.CompleteExport(ctx, dataexchange.ExportCompletion{
			Scope: x.Scope, ObjectKey: x.ObjectKey, ReferenceID: x.Job.ReferenceID, Options: append([]byte(nil), x.Payload...), JobID: x.Job.ID,
			Artifact: dataexchange.Artifact{ID: a.ID, Filename: a.Filename, ContentType: a.ContentType, SHA256: a.SHA256, Size: a.Size, ExpiresAt: a.ExpiresAt},
			Rows:     total, ResultChunks: seq,
		}); e != nil {
			return e
		}
	}
	return b.store.Complete(ctx, x, a)
}

func (b *Service) processExportArtifact(ctx context.Context, x dataexchangemodel.WorkItem, provider modulehost.ExportProvider, artifactProvider modulehost.ExportArtifactProvider) error {
	artifact, err := artifactProvider.BuildExportArtifact(ctx, dataexchange.ExportArtifactRequest{
		Scope: x.Scope, ObjectKey: x.ObjectKey, ReferenceID: x.Job.ReferenceID,
		Options: append([]byte(nil), x.Payload...), JobID: x.Job.ID, CreatedAt: x.Job.CreatedAt,
	})
	if err != nil {
		return err
	}
	if artifact.Content == nil {
		return fmt.Errorf("export artifact provider returned no content")
	}
	defer artifact.Content.Close()
	artifact.Filename = strings.TrimSpace(artifact.Filename)
	artifact.ContentType = strings.TrimSpace(artifact.ContentType)
	if artifact.Filename == "" || artifact.ContentType == "" || artifact.ExpiresAt.IsZero() || artifact.Records < 0 {
		return fmt.Errorf("export artifact provider returned an incomplete artifact")
	}
	const chunkSize = 8 << 20
	buffer := make([]byte, chunkSize)
	sequence := x.Job.ResultChunks
	offset := int64(0)
	if strings.TrimSpace(x.Job.Cursor) != "" {
		parsed, parseErr := strconv.ParseInt(x.Job.Cursor, 10, 64)
		if parseErr != nil || parsed < 0 {
			return fmt.Errorf("invalid export artifact cursor %q", x.Job.Cursor)
		}
		offset = parsed
	}
	reader := bufio.NewReader(artifact.Content)
	if offset > 0 {
		committedSHA, committedSize, identityErr := b.store.ResultIdentity(ctx, x.Scope.WorkspaceID, x.Job.ID)
		if identityErr != nil {
			return identityErr
		}
		if committedSize != offset {
			return fmt.Errorf("resume export artifact cursor %d does not match committed bytes %d", offset, committedSize)
		}
		prefix := sha256.New()
		skipped, skipErr := io.CopyN(prefix, reader, offset)
		if skipErr != nil || skipped != offset {
			return fmt.Errorf("resume export artifact at byte %d: %w", offset, skipErr)
		}
		if hex.EncodeToString(prefix.Sum(nil)) != committedSHA {
			return fmt.Errorf("resume export artifact prefix changed before byte %d", offset)
		}
	}
	// An empty cursor with committed chunks is the durable terminal-page marker.
	// Rebuild metadata for finalization without duplicating artifact content.
	if sequence == 0 || strings.TrimSpace(x.Job.Cursor) != "" {
		for {
			read, readErr := io.ReadFull(reader, buffer)
			if read > 0 {
				offset += int64(read)
				nextCursor := ""
				if _, peekErr := reader.Peek(1); peekErr == nil {
					nextCursor = strconv.FormatInt(offset, 10)
				} else if peekErr != io.EOF {
					return peekErr
				}
				if err := b.store.CommitResultPage(ctx, x, sequence, append([]byte(nil), buffer[:read]...), nextCursor, artifact.Records, artifact.Records); err != nil {
					return err
				}
				sequence++
			}
			if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
				break
			}
			if readErr != nil {
				return readErr
			}
		}
	}
	sha, size, err := b.store.ResultIdentity(ctx, x.Scope.WorkspaceID, x.Job.ID)
	if err != nil {
		return err
	}
	record := &dataexchangemodel.ArtifactRecord{ID: x.Job.ID + ":artifact", Filename: artifact.Filename, ContentType: artifact.ContentType, SHA256: sha, Size: size, ExpiresAt: artifact.ExpiresAt}
	if finalizer, finalizes := provider.(modulehost.ExportCompletionProvider); finalizes {
		if err := finalizer.CompleteExport(ctx, dataexchange.ExportCompletion{
			Scope: x.Scope, ObjectKey: x.ObjectKey, ReferenceID: x.Job.ReferenceID, Options: append([]byte(nil), x.Payload...), JobID: x.Job.ID,
			Artifact: dataexchange.Artifact{ID: record.ID, Filename: record.Filename, ContentType: record.ContentType, SHA256: record.SHA256, Size: record.Size, ExpiresAt: record.ExpiresAt},
			Rows:     artifact.Records, ResultChunks: sequence,
		}); err != nil {
			return err
		}
	}
	return b.store.Complete(ctx, x, record)
}
