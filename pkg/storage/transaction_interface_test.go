package storage

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

var (
	_ GraphTransaction    = (*BadgerTransaction)(nil)
	_ GraphTransaction    = (*namespacedGraphTransaction)(nil)
	_ TransactionalEngine = (*BadgerEngine)(nil)
	_ TransactionalEngine = (*MemoryEngine)(nil)
	_ TransactionalEngine = (*NamespacedEngine)(nil)
	_ TransactionalEngine = (*WALEngine)(nil)
	_ TransactionalEngine = (*AsyncEngine)(nil)
	_ TransactionalEngine = (*TracedEngine)(nil)
)

func TestNamespacedEngineBeginGraphTransactionPinsNamespace(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	namespaced := NewNamespacedEngine(engine, "tenant")
	tx, err := namespaced.BeginGraphTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	require.Equal(t, "tenant", tx.Namespace())
	require.NotEmpty(t, tx.TransactionID())
}

func TestNamespacedEngineBeginGraphTransactionAcceptsUserIDs(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	namespaced := NewNamespacedEngine(engine, "tenant")
	tx, err := namespaced.BeginGraphTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	id, err := tx.CreateNode(&Node{ID: "n1", Labels: []string{"Person"}})
	require.NoError(t, err)
	require.Equal(t, NodeID("n1"), id)

	node, err := tx.GetNode("n1")
	require.NoError(t, err)
	require.Equal(t, NodeID("n1"), node.ID)

	require.NoError(t, tx.Commit())
	node, err = namespaced.GetNode("n1")
	require.NoError(t, err)
	require.Equal(t, NodeID("n1"), node.ID)

	_, err = engine.GetNode("tenant:n1")
	require.NoError(t, err)
}

func TestNamespacedGraphTransactionGetFirstNodeByLabelFiltersNamespace(t *testing.T) {
	engine := NewMemoryEngine()
	defer engine.Close()

	tenantA := NewNamespacedEngine(engine, "a")
	_, err := tenantA.CreateNode(&Node{ID: "foreign", Labels: []string{"Person"}})
	require.NoError(t, err)

	tenantZ := NewNamespacedEngine(engine, "z")
	_, err = tenantZ.CreateNode(&Node{ID: "local", Labels: []string{"Person"}})
	require.NoError(t, err)

	tx, err := tenantZ.BeginGraphTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	node, err := tx.GetFirstNodeByLabel("Person")
	require.NoError(t, err)
	require.Equal(t, NodeID("local"), node.ID)
}

func TestNamespacedEngineBeginGraphTransactionPrimesNamespaceThroughWrappers(t *testing.T) {
	base := NewMemoryEngine()
	wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	walEngine := NewWALEngine(base, wal)
	asyncEngine := NewAsyncEngine(walEngine, &AsyncEngineConfig{FlushInterval: time.Hour})
	defer asyncEngine.Close()

	const namespace = "tenant_wrapped"
	base.mvccByNamespaceMu.RLock()
	_, existedBefore := base.mvccByNamespace[namespace]
	base.mvccByNamespaceMu.RUnlock()
	require.False(t, existedBefore)

	namespaced := NewNamespacedEngine(asyncEngine, namespace)
	tx, err := namespaced.BeginGraphTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	require.Equal(t, namespace, tx.Namespace())
	base.mvccByNamespaceMu.RLock()
	_, existsAfter := base.mvccByNamespace[namespace]
	base.mvccByNamespaceMu.RUnlock()
	require.True(t, existsAfter)
}

func TestWALEngineBeginGraphTransactionLogsCommittedMutations(t *testing.T) {
	config.EnableWAL()
	t.Cleanup(config.DisableWAL)

	base := NewMemoryEngine()
	defer base.Close()

	walDir := t.TempDir()
	wal, err := NewWAL(walDir, DefaultWALConfig())
	require.NoError(t, err)
	walEngine := NewWALEngine(base, wal)
	defer wal.Close()

	tx, err := walEngine.BeginGraphTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("tenant"))

	_, err = tx.CreateNode(&Node{ID: "tenant:n1", Labels: []string{"Person"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	entries, err := ReadWALEntriesFromDir(walDir)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Equal(t, OpTxBegin, entries[0].Operation)
	require.Equal(t, OpCreateNode, entries[1].Operation)
	require.Equal(t, OpTxCommit, entries[2].Operation)
	require.Equal(t, "tenant", entries[1].Database)

	var nodeData WALNodeData
	require.NoError(t, json.Unmarshal(entries[1].Data, &nodeData))
	require.Equal(t, tx.TransactionID(), nodeData.TxID)
	require.Equal(t, NodeID("n1"), nodeData.Node.ID)
}

func TestWALEngineBeginGraphTransactionSkipsWALWhenDisabled(t *testing.T) {
	t.Cleanup(config.WithWALDisabled())

	base := NewMemoryEngine()
	defer base.Close()

	walDir := t.TempDir()
	wal, err := NewWAL(walDir, DefaultWALConfig())
	require.NoError(t, err)
	walEngine := NewWALEngine(base, wal)
	defer wal.Close()

	tx, err := walEngine.BeginGraphTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("tenant"))

	_, err = tx.CreateNode(&Node{ID: "tenant:n1", Labels: []string{"Person"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	_, err = base.GetNode("tenant:n1")
	require.NoError(t, err)

	entries, err := ReadWALEntriesFromDir(walDir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestWALEngineBeginGraphTransactionDeleteNodeRecordsCascadedEdges(t *testing.T) {
	config.EnableWAL()
	t.Cleanup(config.DisableWAL)

	base := NewMemoryEngine()
	defer base.Close()

	_, err := base.CreateNode(&Node{ID: "tenant:n1", Labels: []string{"Person"}})
	require.NoError(t, err)
	_, err = base.CreateNode(&Node{ID: "tenant:n2", Labels: []string{"Person"}})
	require.NoError(t, err)
	require.NoError(t, base.CreateEdge(&Edge{
		ID:        "tenant:e1",
		StartNode: "tenant:n1",
		EndNode:   "tenant:n2",
		Type:      "KNOWS",
	}))

	wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
	require.NoError(t, err)
	walEngine := NewWALEngine(base, wal)
	defer wal.Close()

	tx, err := walEngine.BeginGraphTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("tenant"))

	require.NoError(t, tx.DeleteNode("tenant:n1"))

	walTx, ok := tx.(*walGraphTransaction)
	require.True(t, ok)
	entries := walTx.snapshotEntries()
	require.Len(t, entries, 1)
	require.Equal(t, OpDeleteNode, entries[0].op)
	require.Equal(t, "tenant", entries[0].database)

	data, ok := entries[0].data.(WALDeleteData)
	require.True(t, ok)
	require.Equal(t, "n1", data.ID)
	require.NotNil(t, data.OldNode)
	require.Equal(t, NodeID("n1"), data.OldNode.ID)
	require.Len(t, data.OldEdges, 1)
	require.Equal(t, EdgeID("e1"), data.OldEdges[0].ID)
	require.Equal(t, NodeID("n1"), data.OldEdges[0].StartNode)
	require.Equal(t, NodeID("n2"), data.OldEdges[0].EndNode)
}
