// Package fileengine provides bounded, streaming file mechanics for import
// and export owners. It deliberately knows nothing about Runtime schemas,
// principals, authorization, validation, or persistence.
package fileengine

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
)

var (
	ErrPayloadTooLarge = errors.New("data exchange payload exceeds byte limit")
	ErrTooManyRows     = errors.New("data exchange payload exceeds row limit")
	ErrTooManyColumns  = errors.New("data exchange payload exceeds column limit")
	ErrHeaderRequired  = errors.New("data exchange CSV header is required")
	ErrInvalidCSV      = errors.New("data exchange CSV is invalid")
)

// CSVDecodeLimits bounds untrusted input before an owner interprets any value.
// A zero value disables the corresponding bound.
type CSVDecodeLimits struct {
	MaxBytes   int64
	MaxRows    int
	MaxColumns int
}

// CSVRecord is one source row. Number is the one-based physical CSV line for
// ordinary single-line records and is stable for owner diagnostics.
type CSVRecord struct {
	Number int
	Values []string
}

// DecodeCSV streams an untrusted CSV into an owner callback. The callback is
// responsible for schema mapping and business validation. Values are copied so
// callers may retain them after the next decoder read.
func DecodeCSV(ctx context.Context, source io.Reader, limits CSVDecodeLimits, consume func([]string, CSVRecord) error) ([]string, error) {
	if source == nil {
		return nil, ErrHeaderRequired
	}
	readerSource := source
	var limited *io.LimitedReader
	if limits.MaxBytes > 0 {
		limited = &io.LimitedReader{R: source, N: limits.MaxBytes + 1}
		readerSource = limited
	}
	reader := csv.NewReader(readerSource)
	reader.TrimLeadingSpace = true
	header, err := reader.Read()
	if errors.Is(err, io.EOF) {
		return nil, ErrHeaderRequired
	}
	if err != nil {
		return nil, ErrInvalidCSV
	}
	if limits.MaxColumns > 0 && len(header) > limits.MaxColumns {
		return nil, ErrTooManyColumns
	}
	header = append([]string(nil), header...)
	rows := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		values, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, ErrInvalidCSV
		}
		if limits.MaxRows > 0 && rows >= limits.MaxRows {
			return nil, ErrTooManyRows
		}
		rows++
		if consume != nil {
			if err := consume(header, CSVRecord{Number: rows + 1, Values: append([]string(nil), values...)}); err != nil {
				return nil, err
			}
		}
	}
	if limited != nil && limited.N <= 0 {
		return nil, ErrPayloadTooLarge
	}
	return header, nil
}

// BoundedWriter rejects a write atomically when it would exceed Limit. It is
// suitable for format encoders that may surface the write failure on Flush.
type BoundedWriter struct {
	Writer io.Writer
	Limit  int64
	Bytes  int64
}

func (w *BoundedWriter) Write(value []byte) (int, error) {
	if w == nil || w.Writer == nil {
		return 0, io.ErrClosedPipe
	}
	if w.Limit > 0 && int64(len(value)) > w.Limit-w.Bytes {
		return 0, ErrPayloadTooLarge
	}
	n, err := w.Writer.Write(value)
	w.Bytes += int64(n)
	return n, err
}

// CSVEncoder owns format encoding and byte accounting while the calling owner
// remains responsible for selecting and authorizing every emitted value.
type CSVEncoder struct {
	writer  *csv.Writer
	bounded *BoundedWriter
	rows    int
}

func NewCSVEncoder(destination io.Writer, maxBytes int64) *CSVEncoder {
	bounded := &BoundedWriter{Writer: destination, Limit: maxBytes}
	return &CSVEncoder{writer: csv.NewWriter(bounded), bounded: bounded}
}

func (e *CSVEncoder) Write(record []string) error {
	if e == nil || e.writer == nil {
		return io.ErrClosedPipe
	}
	if err := e.writer.Write(record); err != nil {
		return err
	}
	e.rows++
	return nil
}

func (e *CSVEncoder) Close() error {
	if e == nil || e.writer == nil {
		return io.ErrClosedPipe
	}
	e.writer.Flush()
	return e.writer.Error()
}

func (e *CSVEncoder) BytesWritten() int64 {
	if e == nil || e.bounded == nil {
		return 0
	}
	return e.bounded.Bytes
}

func (e *CSVEncoder) RecordsWritten() int {
	if e == nil {
		return 0
	}
	return e.rows
}
