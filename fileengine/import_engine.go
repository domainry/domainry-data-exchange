package fileengine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	dataexchange "github.com/domainry/domainry-data-exchange-sdk"
	"github.com/domainry/domainry-data-exchange-sdk/modulehost"
)

type ImportSourceOpener func(context.Context) (io.ReadCloser, error)

type ImportEngineRequest struct {
	Batch     dataexchange.ImportBatch
	Open      ImportSourceOpener
	Limits    CSVDecodeLimits
	BatchSize int
	// OnValidated runs after the complete validation pass and before any apply
	// callback. Durable bindings use it to checkpoint totals atomically.
	OnValidated func(context.Context, ImportEngineResult) error
}

type ImportEngineResult struct {
	Validated int
	Applied   int
	Rejected  int
	Batches   int
	SHA256    string
	Bytes     int64
}

// ProcessCSVImport performs a validation pass before the apply pass, keeping
// only one bounded row batch in memory. The source is reopened and hashed for
// both passes so an apply can never consume bytes different from validation.
func ProcessCSVImport(ctx context.Context, provider modulehost.ImportProvider, request ImportEngineRequest) (ImportEngineResult, error) {
	if provider == nil || request.Open == nil {
		return ImportEngineResult{}, fmt.Errorf("data exchange import provider and source are required")
	}
	if request.BatchSize <= 0 {
		request.BatchSize = 100
	}
	validated, validationHash, validationBytes, err := runCSVImportPass(ctx, request, provider.ValidateImportBatch)
	if err != nil {
		return ImportEngineResult{}, err
	}
	result := ImportEngineResult{Validated: validated.Accepted, Rejected: validated.Rejected, Batches: validated.Batches, SHA256: validationHash, Bytes: validationBytes}
	if request.OnValidated != nil {
		if err := request.OnValidated(ctx, result); err != nil {
			return result, err
		}
	}
	if validated.Rejected > 0 {
		return result, nil
	}
	applied, applyHash, applyBytes, err := runCSVImportPass(ctx, request, provider.ApplyImportBatch)
	if err != nil {
		return result, err
	}
	if applyHash != validationHash || applyBytes != validationBytes {
		return result, fmt.Errorf("data exchange import source changed between validation and apply")
	}
	result.Applied = applied.Accepted
	if applied.Rejected > 0 {
		return result, fmt.Errorf("data exchange owner rejected rows after successful validation")
	}
	return result, nil
}

type importPassResult struct {
	Accepted int
	Rejected int
	Batches  int
}

func runCSVImportPass(ctx context.Context, request ImportEngineRequest, consume func(context.Context, dataexchange.ImportBatch) (dataexchange.ImportBatchResult, error)) (importPassResult, string, int64, error) {
	source, err := request.Open(ctx)
	if err != nil {
		return importPassResult{}, "", 0, err
	}
	defer source.Close()
	hasher := sha256.New()
	counter := &importByteCounter{writer: hasher}
	rows := make([]dataexchange.ImportRow, 0, request.BatchSize)
	result := importPassResult{}
	flush := func(headers []string) error {
		if len(rows) == 0 {
			return nil
		}
		batch := request.Batch
		batch.Headers = append([]string(nil), headers...)
		batch.Rows = append([]dataexchange.ImportRow(nil), rows...)
		batch.ChunkID = fmt.Sprintf("%s:%d", request.Batch.JobID, result.Batches)
		outcome, err := consume(ctx, batch)
		if err != nil {
			return err
		}
		if outcome.Accepted < 0 || outcome.Rejected < 0 || outcome.Accepted+outcome.Rejected != len(rows) {
			return fmt.Errorf("data exchange import provider returned an invalid batch result")
		}
		result.Accepted += outcome.Accepted
		result.Rejected += outcome.Rejected
		result.Batches++
		rows = rows[:0]
		return nil
	}
	var lastHeader []string
	_, err = DecodeCSV(ctx, io.TeeReader(source, counter), request.Limits, func(headers []string, row CSVRecord) error {
		lastHeader = headers
		rows = append(rows, dataexchange.ImportRow{Number: row.Number, Values: row.Values})
		if len(rows) == request.BatchSize {
			return flush(headers)
		}
		return nil
	})
	if err != nil {
		return importPassResult{}, "", 0, err
	}
	if err := flush(lastHeader); err != nil {
		return importPassResult{}, "", 0, err
	}
	return result, hex.EncodeToString(hasher.Sum(nil)), counter.bytes, nil
}

type importByteCounter struct {
	writer io.Writer
	bytes  int64
}

func (w *importByteCounter) Write(value []byte) (int, error) {
	n, err := w.writer.Write(value)
	w.bytes += int64(n)
	return n, err
}
