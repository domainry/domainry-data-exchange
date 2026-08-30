package fileengine

import (
	"context"
	"errors"
	"io"
)

var ErrChunkSizeInvalid = errors.New("data exchange chunk size must be positive")

// Chunk is an immutable, ordered portion of an import source or export result.
// Offset is the byte offset in the complete artifact.
type Chunk struct {
	Sequence int
	Offset   int64
	Content  []byte
}

// ChunkWriter converts an encoder's arbitrary writes into bounded immutable
// chunks. The callback can durably commit each chunk before the writer accepts
// more output, which keeps memory bounded and propagates backpressure.
type ChunkWriter struct {
	ctx       context.Context
	chunkSize int
	commit    func(context.Context, Chunk) error
	buffer    []byte
	sequence  int
	offset    int64
	closed    bool
}

func NewChunkWriter(ctx context.Context, chunkSize int, commit func(context.Context, Chunk) error) (*ChunkWriter, error) {
	if chunkSize <= 0 {
		return nil, ErrChunkSizeInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return &ChunkWriter{ctx: ctx, chunkSize: chunkSize, commit: commit, buffer: make([]byte, 0, chunkSize)}, nil
}

func (w *ChunkWriter) Write(value []byte) (int, error) {
	if w == nil || w.closed {
		return 0, io.ErrClosedPipe
	}
	written := 0
	for len(value) > 0 {
		if err := w.ctx.Err(); err != nil {
			return written, err
		}
		remaining := w.chunkSize - len(w.buffer)
		count := len(value)
		if count > remaining {
			count = remaining
		}
		w.buffer = append(w.buffer, value[:count]...)
		value = value[count:]
		written += count
		if len(w.buffer) == w.chunkSize {
			if err := w.flush(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

func (w *ChunkWriter) Close() error {
	if w == nil || w.closed {
		return io.ErrClosedPipe
	}
	if err := w.flush(); err != nil {
		return err
	}
	w.closed = true
	return nil
}

func (w *ChunkWriter) flush() error {
	if len(w.buffer) == 0 {
		return nil
	}
	content := append([]byte(nil), w.buffer...)
	chunk := Chunk{Sequence: w.sequence, Offset: w.offset, Content: content}
	if w.commit != nil {
		if err := w.commit(w.ctx, chunk); err != nil {
			return err
		}
	}
	w.sequence++
	w.offset += int64(len(content))
	w.buffer = w.buffer[:0]
	return nil
}

func (w *ChunkWriter) ChunksWritten() int {
	if w == nil {
		return 0
	}
	return w.sequence
}

func (w *ChunkWriter) BytesWritten() int64 {
	if w == nil {
		return 0
	}
	return w.offset + int64(len(w.buffer))
}
