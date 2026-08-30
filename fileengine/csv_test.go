package fileengine

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func TestDecodeCSVStreamsBoundedRows(t *testing.T) {
	var rows []CSVRecord
	header, err := DecodeCSV(t.Context(), strings.NewReader("name,amount\nAda,12\nGrace,15\n"), CSVDecodeLimits{MaxBytes: 128, MaxRows: 2, MaxColumns: 2}, func(_ []string, row CSVRecord) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil || len(header) != 2 || len(rows) != 2 || rows[1].Number != 3 || rows[1].Values[0] != "Grace" {
		t.Fatalf("header=%v rows=%v err=%v", header, rows, err)
	}
}

func TestDecodeCSVEnforcesEveryLimit(t *testing.T) {
	tests := []struct {
		name   string
		value  string
		limits CSVDecodeLimits
		want   error
	}{
		{"bytes", "name\nAda\n", CSVDecodeLimits{MaxBytes: 4}, ErrPayloadTooLarge},
		{"rows", "name\nAda\nGrace\n", CSVDecodeLimits{MaxRows: 1}, ErrTooManyRows},
		{"columns", "a,b\n1,2\n", CSVDecodeLimits{MaxColumns: 1}, ErrTooManyColumns},
		{"header", "", CSVDecodeLimits{}, ErrHeaderRequired},
		{"invalid", "name\n\"unterminated", CSVDecodeLimits{}, ErrInvalidCSV},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := DecodeCSV(t.Context(), strings.NewReader(test.value), test.limits, nil)
			if !errors.Is(err, test.want) {
				t.Fatalf("err=%v want=%v", err, test.want)
			}
		})
	}
}

func TestCSVEncoderProducesBoundedCSV(t *testing.T) {
	var output bytes.Buffer
	encoder := NewCSVEncoder(&output, 32)
	if err := encoder.Write([]string{"name", "note"}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Write([]string{"Ada", "a,b"}); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	if output.String() != "name,note\nAda,\"a,b\"\n" || encoder.BytesWritten() != int64(output.Len()) || encoder.RecordsWritten() != 2 {
		t.Fatalf("output=%q bytes=%d records=%d", output.String(), encoder.BytesWritten(), encoder.RecordsWritten())
	}

	var limited bytes.Buffer
	tooSmall := NewCSVEncoder(&limited, 3)
	_ = tooSmall.Write([]string{"name"})
	if err := tooSmall.Close(); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("err=%v", err)
	}
}

func TestDecodeCSVPropagatesCancellationAndOwnerFailure(t *testing.T) {
	ownerErr := errors.New("owner rejected row")
	_, err := DecodeCSV(t.Context(), strings.NewReader("name\nAda\n"), CSVDecodeLimits{}, func([]string, CSVRecord) error { return ownerErr })
	if !errors.Is(err, ownerErr) {
		t.Fatalf("err=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = DecodeCSV(ctx, strings.NewReader("name\nAda\n"), CSVDecodeLimits{}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}
