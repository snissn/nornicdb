package nornicdb

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestEmbedWorker_StartWorkers_NumWorkersBranches(t *testing.T) {
	newWorker := func(numWorkers int) *EmbedWorker {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "start-workers")
		return NewEmbedWorker(newMockEmbedder(), engine, &EmbedWorkerConfig{
			NumWorkers:       numWorkers,
			ScanInterval:     time.Hour,
			BatchDelay:       time.Millisecond,
			MaxRetries:       1,
			ChunkSize:        128,
			ChunkOverlap:     16,
			DeferWorkerStart: true,
		})
	}

	t.Run("num workers less than one defaults to one", func(t *testing.T) {
		w := newWorker(0)
		t.Cleanup(func() { w.Close() })

		w.StartWorkers()
		require.True(t, w.workersStarted)
	})

	t.Run("num workers greater than one starts worker pool", func(t *testing.T) {
		w := newWorker(2)
		t.Cleanup(func() { w.Close() })

		w.StartWorkers()
		require.True(t, w.workersStarted)
	})
}

func TestEmbedWorker_TriggerDebounce_ClosedAndStaleSequence(t *testing.T) {
	newWorker := func() *EmbedWorker {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "trigger-debounce")
		return NewEmbedWorker(newMockEmbedder(), engine, &EmbedWorkerConfig{
			NumWorkers:           1,
			ScanInterval:         time.Hour,
			BatchDelay:           time.Millisecond,
			MaxRetries:           1,
			ChunkSize:            128,
			ChunkOverlap:         16,
			DeferWorkerStart:     true,
			TriggerDebounceDelay: 20 * time.Millisecond,
		})
	}

	t.Run("closed worker suppresses debounce callback", func(t *testing.T) {
		w := newWorker()
		t.Cleanup(func() { w.Close() })

		w.Trigger()
		w.closed.Store(true)
		time.Sleep(35 * time.Millisecond)
		require.Zero(t, w.QueueLen())
	})

	t.Run("stale debounce sequence does not signal trigger", func(t *testing.T) {
		w := newWorker()
		t.Cleanup(func() { w.Close() })

		w.Trigger()
		w.triggerDebounceSeq.Add(1) // invalidate the callback sequence
		time.Sleep(35 * time.Millisecond)
		require.Zero(t, w.QueueLen())
	})
}

func TestEmbedWorker_ProcessNextBatch_ShouldYieldBranch(t *testing.T) {
	base := storage.NewMemoryEngine()
	engine := storage.NewNamespacedEngine(base, "yield")
	w := NewEmbedWorker(newMockEmbedder(), engine, &EmbedWorkerConfig{
		NumWorkers:       1,
		ScanInterval:     time.Hour,
		BatchDelay:       time.Millisecond,
		MaxRetries:       1,
		ChunkSize:        128,
		ChunkOverlap:     16,
		DeferWorkerStart: true,
	})
	t.Cleanup(func() { w.Close() })

	w.SetShouldYield(func() bool { return true })
	didWork := w.processNextBatch()
	require.False(t, didWork)
	require.Equal(t, 1, w.QueueLen())
}

func TestEmbedWorker_EmbedBatchHelpers_EdgeBranches(t *testing.T) {
	t.Run("embedChunksInBatches empty input returns nil", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "batch-empty")
		w := NewEmbedWorker(newMockEmbedder(), engine, &EmbedWorkerConfig{
			NumWorkers:       1,
			ScanInterval:     time.Hour,
			BatchDelay:       time.Millisecond,
			MaxRetries:       1,
			ChunkSize:        128,
			ChunkOverlap:     16,
			DeferWorkerStart: true,
		})
		t.Cleanup(func() { w.Close() })

		embs, err := w.embedChunksInBatches(nil, "node-1")
		require.NoError(t, err)
		require.Nil(t, embs)
	})

	t.Run("embedBatchWithRetry exits on context cancellation during backoff", func(t *testing.T) {
		base := storage.NewMemoryEngine()
		engine := storage.NewNamespacedEngine(base, "batch-cancel")
		w := NewEmbedWorker(&flakyBatchEmbedder{dims: 3, failUntil: 10}, engine, &EmbedWorkerConfig{
			NumWorkers:       1,
			ScanInterval:     time.Hour,
			BatchDelay:       time.Millisecond,
			MaxRetries:       2,
			ChunkSize:        128,
			ChunkOverlap:     16,
			DeferWorkerStart: true,
		})
		t.Cleanup(func() { w.Close() })

		go func() {
			time.Sleep(20 * time.Millisecond)
			w.cancel()
		}()

		embs, err := w.embedBatchWithRetry([]string{"a", "b"})
		require.Error(t, err)
		require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
		require.Nil(t, embs)
	})
}
