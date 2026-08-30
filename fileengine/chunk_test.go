package fileengine

import (
	"context"
	"errors"
	"testing"
)

func TestChunkWriterBoundsMemoryAndPreservesOrder(t *testing.T) {
	var chunks []Chunk
	writer, err := NewChunkWriter(t.Context(), 4, func(_ context.Context, chunk Chunk) error {
		chunks = append(chunks, chunk)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"ab", "cdefg", "hij"} {
		if _, err := writer.Write([]byte(value)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 || string(chunks[0].Content) != "abcd" || string(chunks[1].Content) != "efgh" || string(chunks[2].Content) != "ij" {
		t.Fatalf("chunks=%v", chunks)
	}
	if chunks[2].Sequence != 2 || chunks[2].Offset != 8 || writer.BytesWritten() != 10 || writer.ChunksWritten() != 3 {
		t.Fatalf("last=%+v bytes=%d chunks=%d", chunks[2], writer.BytesWritten(), writer.ChunksWritten())
	}
}

func TestChunkWriterPropagatesBackpressureAndCancellation(t *testing.T) {
	commitErr := errors.New("durable commit failed")
	writer, _ := NewChunkWriter(t.Context(), 2, func(context.Context, Chunk) error { return commitErr })
	if _, err := writer.Write([]byte("abc")); !errors.Is(err, commitErr) {
		t.Fatalf("err=%v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cancelled, _ := NewChunkWriter(ctx, 2, nil)
	if _, err := cancelled.Write([]byte("a")); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if _, err := NewChunkWriter(t.Context(), 0, nil); !errors.Is(err, ErrChunkSizeInvalid) {
		t.Fatalf("err=%v", err)
	}
}
