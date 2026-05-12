package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTracedEngine_AsyncFlushSpanCarriesKind pins the TRC-17 universal
// contract: EVERY nornicdb.storage.* span must carry a `kind` attribute.
// Previously the async-engine flush span omitted `kind`, which — when a
// background AsyncEngine flusher from a prior test leaked into a later
// test's TracerProvider — broke TestTracedEngine_EmitsStorageSpans because
// its first-match loop could pick the flush span and fail the assertion.
//
// This test forces an AsyncEngine flush in-process and asserts the flush
// span itself carries `kind="batch"`, making the contract invariant.
func TestTracedEngine_AsyncFlushSpanCarriesKind(t *testing.T) {
	exp, teardown := spanSetup(t)
	defer teardown()

	inner := storage.NewMemoryEngine()
	async := storage.NewAsyncEngine(inner, nil)
	t.Cleanup(func() { _ = async.Close() })

	// Force at least one node write and a flush so the span emits. The
	// AsyncEngine layer requires ID prefixing (it expects a namespace like
	// "nornic:…"), so we wrap in a NamespacedEngine the same way the rest
	// of the test suite does.
	store := storage.NewNamespacedEngine(async, "test")
	_, err := store.CreateNode(&storage.Node{
		ID:     storage.NodeID("flush-span-probe"),
		Labels: []string{"Probe"},
	})
	require.NoError(t, err)
	require.NoError(t, async.Flush())

	// Sync export: synchronous span processor inside spanSetup emits on
	// span End, so all spans should be present now.
	spans := exp.GetSpans()

	var sawFlush bool
	for _, s := range spans {
		if s.Name != "nornicdb.storage.flush" {
			continue
		}
		sawFlush = true
		attrs := spanAttrs(s)
		assert.Contains(t, attrs, "kind",
			"TRC-17: nornicdb.storage.flush span must carry kind attribute "+
				"(it is a storage.* span and the universal contract applies)")
		assert.Equal(t, "batch", attrs["kind"],
			"flush span kind must be the literal \"batch\"")
	}
	assert.True(t, sawFlush, "expected at least one nornicdb.storage.flush span after Flush()")

	// Give the BSP a beat to settle so the cleanup doesn't race the provider
	// shutdown — not required for correctness but keeps the test log quiet.
	time.Sleep(1 * time.Millisecond)
	_ = context.Background
}
