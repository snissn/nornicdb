package storage

import (
	"testing"

	treedbpage "github.com/snissn/gomap/TreeDB/page"
	"github.com/stretchr/testify/require"
)

func TestFindCapability_WalksWrapperChain(t *testing.T) {
	be, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer be.Close()

	wrapped := NewNamespacedEngine(NewTracedEngine(be), "test")
	got, ok := FindCapability[*BadgerEngine](wrapped)
	require.True(t, ok)
	require.Same(t, be, got)
	require.Same(t, be, BaseEngine(wrapped))
}

func TestInspectEngineCapabilities_Badger(t *testing.T) {
	be, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer be.Close()

	caps := InspectEngineCapabilities(NewNamespacedEngine(be, "test"))
	require.Equal(t, "badger", caps.Backend)
	require.True(t, caps.StorageMaintenance)
	require.True(t, caps.StorageByteStats)
	require.False(t, caps.StorageDiagnostics)
	require.False(t, caps.TreeDBDurability)
	require.True(t, caps.TemporalMaintenance)
	require.True(t, caps.MVCCMaintenance)
}

func TestInspectEngineCapabilities_TreeDB(t *testing.T) {
	tree, err := NewTreeDBEngine(t.TempDir())
	require.NoError(t, err)
	defer tree.Close()

	caps := InspectEngineCapabilities(NewNamespacedEngine(tree, "test"))
	require.Equal(t, "treedb", caps.Backend)
	require.True(t, caps.StorageMaintenance)
	require.True(t, caps.StorageByteStats)
	require.True(t, caps.StorageDiagnostics)
	require.True(t, caps.TreeDBDurability)
	require.False(t, caps.TemporalMaintenance)
	require.False(t, caps.MVCCMaintenance)
	require.False(t, caps.MVCCLifecycle)
}

func TestStorageByteStatsProvidersAreBoundedAndNonNegative(t *testing.T) {
	be, err := NewBadgerEngineInMemory()
	require.NoError(t, err)
	defer be.Close()
	badgerStats := be.StorageByteStats()
	require.True(t, badgerStats.NodesSupported)
	require.GreaterOrEqual(t, badgerStats.Nodes, int64(0))
	require.True(t, badgerStats.EdgesSupported)
	require.GreaterOrEqual(t, badgerStats.Edges, int64(0))
	require.True(t, badgerStats.IndexSupported)
	require.GreaterOrEqual(t, badgerStats.Index, int64(0))
	require.True(t, badgerStats.WALSupported)
	require.GreaterOrEqual(t, badgerStats.WAL, int64(0))

	tree, err := NewTreeDBEngine(t.TempDir())
	require.NoError(t, err)
	defer tree.Close()
	treeStats := tree.StorageByteStats()
	require.False(t, treeStats.NodesSupported)
	require.Zero(t, treeStats.Nodes)
	require.False(t, treeStats.EdgesSupported)
	require.Zero(t, treeStats.Edges)
	require.True(t, treeStats.IndexSupported)
	require.GreaterOrEqual(t, treeStats.Index, int64(0))
	require.True(t, treeStats.WALSupported)
	require.GreaterOrEqual(t, treeStats.WAL, int64(0))
}

func TestTreeDBStorageByteStatsFromStatsUsesNativeCounters(t *testing.T) {
	stats := treeDBStorageByteStatsFromStats(map[string]string{
		"treedb.pages.total":              "3",
		"treedb.cache.wal_bytes_estimate": "512",
	})

	require.False(t, stats.NodesSupported)
	require.Zero(t, stats.Nodes)
	require.False(t, stats.EdgesSupported)
	require.Zero(t, stats.Edges)
	require.True(t, stats.IndexSupported)
	require.Equal(t, int64(3*treedbpage.PageSize), stats.Index)
	require.True(t, stats.WALSupported)
	require.Equal(t, int64(512), stats.WAL)
}

func TestTreeDBStorageByteStatsFromStatsSkipsInvalidCounters(t *testing.T) {
	stats := treeDBStorageByteStatsFromStats(map[string]string{
		"treedb.pages.total":              "not-a-number",
		"treedb.cache.wal_bytes_estimate": "-1",
	})

	require.False(t, stats.IndexSupported)
	require.Zero(t, stats.Index)
	require.True(t, stats.WALSupported)
	require.Zero(t, stats.WAL)
}
