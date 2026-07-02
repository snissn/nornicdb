// Plan 04-04-07 tests: cmd/nornicdb wiring smoke for StorageMetrics +
// MVCCMetrics + bytes_metrics_sweeper.
//
// We don't boot a full server here — the orchestrator unit-tests the
// adapter helpers directly. Full server smoke is deferred to manual
// /metrics curl per the plan goal_backward.
package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// TestStorageMetricsCapabilities_FindBadgerThroughWrapper covers the Badger
// capability path through a NamespacedEngine wrapper.
func TestStorageMetricsCapabilities_FindBadgerThroughWrapper(t *testing.T) {
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer be.Close()

	ns := storage.NewNamespacedEngine(be, "test")
	got, ok := storage.FindCapability[storage.StorageByteStatsProvider](ns)
	require.True(t, ok)
	require.Same(t, be, got, "wrapped *BadgerEngine byte-stats capability should be found")

	attacher, ok := storage.FindCapability[storageMetricsAttacher](ns)
	require.True(t, ok)
	require.Same(t, be, attacher, "wrapped *BadgerEngine metrics attach capability should be found")
}

// TestEngineStorageProbe_RoutesAccessors verifies the probe adapter
// returns int64 values from the engine accessors.
func TestEngineStorageProbe_RoutesAccessors(t *testing.T) {
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer be.Close()

	probe := newEngineStorageProbe(be)
	require.Equal(t, int64(0), probe.NodeCount())
	require.Equal(t, int64(0), probe.EdgeCount())
}

// TestEngineMVCCProbe_RoutesAccessors verifies the MVCC probe adapter
// routes to the RISK-2 accessors.
func TestEngineMVCCProbe_RoutesAccessors(t *testing.T) {
	be, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer be.Close()

	probe, ok := newEngineMVCCProbe(be)
	require.True(t, ok)
	require.GreaterOrEqual(t, probe.PinnedBytes(), int64(0))
	require.GreaterOrEqual(t, probe.OldestReaderAgeSeconds(), float64(0))
	require.GreaterOrEqual(t, probe.ActiveReaders(), int64(0))
}

func TestStorageMetricsCapabilities_TreeDBSupportedMetrics(t *testing.T) {
	tree, err := storage.NewTreeDBEngine(t.TempDir())
	require.NoError(t, err)
	defer tree.Close()

	ns := storage.NewNamespacedEngine(tree, "test")
	byteStats, ok := storage.FindCapability[storage.StorageByteStatsProvider](ns)
	require.True(t, ok)
	require.Same(t, tree, byteStats)

	probe := newEngineStorageProbe(ns)
	require.Equal(t, int64(0), probe.NodeCount())
	require.Equal(t, int64(0), probe.EdgeCount())

	_, ok = newEngineMVCCProbe(ns)
	require.False(t, ok, "TreeDB does not expose MVCC metrics yet")
	_, ok = storage.FindCapability[storageMetricsAttacher](ns)
	require.False(t, ok, "TreeDB has no hot-path metrics attacher yet")
}
