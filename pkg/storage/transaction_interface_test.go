package storage

import (
	"testing"
	"time"

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
