package storage

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestTreeDBEngine(t *testing.T) *TreeDBEngine {
	t.Helper()
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})
	return engine
}

func TestTreeDBErrorMapping_ConditionalTxnClosed(t *testing.T) {
	require.ErrorIs(t, mapTreeDBError(treedb.ErrConditionalTxnClosed), ErrTransactionClosed)
	require.ErrorIs(t, mapTreeDBError(treedb.ErrClosed), ErrStorageClosed)
}

func TestTreeDBEngine_HotBodyCacheLifecycle(t *testing.T) {
	engine := newTestTreeDBEngine(t)

	_, err := engine.CreateNode(&Node{
		ID:         "test:cache-a",
		Labels:     []string{"Cached"},
		Properties: map[string]any{"name": "a"},
	})
	require.NoError(t, err)

	got, err := engine.GetNode("test:cache-a")
	require.NoError(t, err)
	got.Labels[0] = "Mutated"
	got.Properties["name"] = "mutated"

	again, err := engine.GetNode("test:cache-a")
	require.NoError(t, err)
	require.Equal(t, []string{"Cached"}, again.Labels)
	require.Equal(t, "a", again.Properties["name"])

	again.Properties["name"] = "b"
	require.NoError(t, engine.UpdateNode(again))
	updated, err := engine.GetNode("test:cache-a")
	require.NoError(t, err)
	require.Equal(t, "b", updated.Properties["name"])

	require.NoError(t, engine.DeleteNode("test:cache-a"))
	_, err = engine.GetNode("test:cache-a")
	require.ErrorIs(t, err, ErrNotFound)
	engine.nodeCacheMu.RLock()
	_, cachedNode := engine.nodeCache["test:cache-a"]
	engine.nodeCacheMu.RUnlock()
	require.False(t, cachedNode)

	_, err = engine.CreateNode(&Node{ID: "test:cache-start", Labels: []string{"Cached"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:cache-end", Labels: []string{"Cached"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{
		ID:         "test:cache-edge",
		StartNode:  "test:cache-start",
		EndNode:    "test:cache-end",
		Type:       "REL",
		Properties: map[string]any{"kind": "cached"},
	}))

	edge, err := engine.GetEdge("test:cache-edge")
	require.NoError(t, err)
	edge.Type = "MUTATED"
	edge.Properties["kind"] = "mutated"
	edgeAgain, err := engine.GetEdge("test:cache-edge")
	require.NoError(t, err)
	require.Equal(t, "REL", edgeAgain.Type)
	require.Equal(t, "cached", edgeAgain.Properties["kind"])

	require.NoError(t, engine.DeleteEdge("test:cache-edge"))
	_, err = engine.GetEdge("test:cache-edge")
	require.ErrorIs(t, err, ErrNotFound)
	engine.edgeCacheMu.RLock()
	_, cachedEdge := engine.edgeCache["test:cache-edge"]
	engine.edgeCacheMu.RUnlock()
	require.False(t, cachedEdge)
}

func TestTreeDBEngine_BatchGetNodesUsesAndPopulatesBodyCache(t *testing.T) {
	newBatchCacheEngine := func(t *testing.T) *TreeDBEngine {
		t.Helper()
		engine := newTestTreeDBEngine(t)
		require.NoError(t, engine.BulkCreateNodes([]*Node{
			{
				ID:     "test:batch-cache-a",
				Labels: []string{"Cached"},
				EmbedMeta: map[string]any{
					"model": "mini",
					"stats": map[string]interface{}{"chunks": int64(2)},
					"tags":  []interface{}{"embed", map[string]interface{}{"kind": "doc"}},
				},
				Properties: map[string]any{
					"name":  "a",
					"meta":  map[string]interface{}{"rank": int64(1)},
					"tags":  []interface{}{"hot", map[string]interface{}{"nested": "ok"}},
					"uints": []interface{}{uint64(1), uint64(2)},
				},
			},
			{
				ID:                   "test:batch-cache-b",
				Labels:               []string{"Cached"},
				VisibilitySuppressed: true,
				Properties:           map[string]any{"name": "b"},
			},
		}))
		engine.nodeCacheMu.Lock()
		engine.nodeCache = make(map[NodeID]*Node, engine.nodeCacheMaxEntries)
		engine.nodeCacheMu.Unlock()
		return engine
	}

	t.Run("populates cache for loaded nodes only", func(t *testing.T) {
		engine := newBatchCacheEngine(t)

		first, err := engine.BatchGetNodes([]NodeID{
			"test:batch-cache-a",
			"test:batch-cache-b",
			"test:batch-cache-missing",
		})
		require.NoError(t, err)
		require.Len(t, first, 2)
		require.Equal(t, "a", first["test:batch-cache-a"].Properties["name"])
		require.True(t, first["test:batch-cache-b"].VisibilitySuppressed)

		engine.nodeCacheMu.RLock()
		_, cachedA := engine.nodeCache["test:batch-cache-a"]
		_, cachedB := engine.nodeCache["test:batch-cache-b"]
		_, cachedMissing := engine.nodeCache["test:batch-cache-missing"]
		engine.nodeCacheMu.RUnlock()
		require.True(t, cachedA)
		require.True(t, cachedB)
		require.False(t, cachedMissing)
	})

	t.Run("keeps batch miss embed metadata isolated from cache", func(t *testing.T) {
		engine := newBatchCacheEngine(t)

		first, err := engine.BatchGetNodes([]NodeID{"test:batch-cache-a"})
		require.NoError(t, err)
		first["test:batch-cache-a"].EmbedMeta["stats"].(map[string]interface{})["chunks"] = int64(99)
		first["test:batch-cache-a"].EmbedMeta["tags"].([]interface{})[1].(map[string]interface{})["kind"] = "mutated"

		again, err := engine.BatchGetNodes([]NodeID{"test:batch-cache-a"})
		require.NoError(t, err)
		require.Equal(t, int64(2), again["test:batch-cache-a"].EmbedMeta["stats"].(map[string]interface{})["chunks"])
		require.Equal(t, "doc", again["test:batch-cache-a"].EmbedMeta["tags"].([]interface{})[1].(map[string]interface{})["kind"])
	})

	t.Run("returns isolated copies for cache hits", func(t *testing.T) {
		engine := newBatchCacheEngine(t)

		_, err := engine.BatchGetNodes([]NodeID{"test:batch-cache-a", "test:batch-cache-b"})
		require.NoError(t, err)
		engine.nodeCacheMu.Lock()
		engine.nodeCache["test:batch-cache-a"].Properties["uints"] = []uint64{1, 2}
		engine.nodeCacheMu.Unlock()

		hit, err := engine.BatchGetNodes([]NodeID{"test:batch-cache-a", "test:batch-cache-b"})
		require.NoError(t, err)
		hit["test:batch-cache-a"].Labels[0] = "Mutated"
		hit["test:batch-cache-a"].Properties["name"] = "mutated"
		hit["test:batch-cache-a"].Properties["meta"].(map[string]interface{})["rank"] = int64(99)
		hit["test:batch-cache-a"].Properties["tags"].([]interface{})[1].(map[string]interface{})["nested"] = "mutated"
		hit["test:batch-cache-a"].Properties["uints"].([]uint64)[0] = 99
		hit["test:batch-cache-a"].EmbedMeta["stats"].(map[string]interface{})["chunks"] = int64(99)
		hit["test:batch-cache-a"].EmbedMeta["tags"].([]interface{})[1].(map[string]interface{})["kind"] = "mutated"

		again, err := engine.BatchGetNodes([]NodeID{"test:batch-cache-a", "test:batch-cache-b"})
		require.NoError(t, err)
		require.Equal(t, []string{"Cached"}, again["test:batch-cache-a"].Labels)
		require.Equal(t, "a", again["test:batch-cache-a"].Properties["name"])
		require.Equal(t, int64(1), again["test:batch-cache-a"].Properties["meta"].(map[string]interface{})["rank"])
		require.Equal(t, "ok", again["test:batch-cache-a"].Properties["tags"].([]interface{})[1].(map[string]interface{})["nested"])
		require.Equal(t, uint64(1), again["test:batch-cache-a"].Properties["uints"].([]uint64)[0])
		require.Equal(t, int64(2), again["test:batch-cache-a"].EmbedMeta["stats"].(map[string]interface{})["chunks"])
		require.Equal(t, "doc", again["test:batch-cache-a"].EmbedMeta["tags"].([]interface{})[1].(map[string]interface{})["kind"])
		require.True(t, again["test:batch-cache-b"].VisibilitySuppressed)
	})

	t.Run("preserves empty typed property slices on cache copies", func(t *testing.T) {
		engine := newBatchCacheEngine(t)

		engine.cacheStoreNode(&Node{
			ID:                   "test:batch-cache-empty-node",
			VisibilitySuppressed: true,
			Properties: map[string]any{
				"empty":   []string{},
				"nil":     []string(nil),
				"nilList": []interface{}(nil),
				"nilMap":  map[string]interface{}(nil),
			},
		})
		node, ok := engine.cacheLoadNode("test:batch-cache-empty-node")
		require.True(t, ok)
		require.True(t, node.VisibilitySuppressed)
		emptyNodeSlice := node.Properties["empty"].([]string)
		nilNodeSlice := node.Properties["nil"].([]string)
		nilInterfaceSlice := node.Properties["nilList"].([]interface{})
		nilMap := node.Properties["nilMap"].(map[string]interface{})
		require.NotNil(t, emptyNodeSlice)
		require.Len(t, emptyNodeSlice, 0)
		require.Nil(t, nilNodeSlice)
		require.Nil(t, nilInterfaceSlice)
		require.Nil(t, nilMap)

		engine.cacheStoreEdge(&Edge{
			ID:                   "test:batch-cache-empty-edge",
			StartNode:            "test:batch-cache-a",
			EndNode:              "test:batch-cache-b",
			Type:                 "LINKS",
			VisibilitySuppressed: true,
			Properties:           map[string]any{"empty": []string{}},
		})
		edge, ok := engine.cacheLoadEdge("test:batch-cache-empty-edge")
		require.True(t, ok)
		require.True(t, edge.VisibilitySuppressed)
		emptyEdgeSlice := edge.Properties["empty"].([]string)
		require.NotNil(t, emptyEdgeSlice)
		require.Len(t, emptyEdgeSlice, 0)
	})

	t.Run("skips stale miss cache population after mutation guard changes", func(t *testing.T) {
		engine := newBatchCacheEngine(t)

		guard := engine.guardSeq.Load()
		engine.cacheStoreNode(&Node{
			ID:         "test:batch-cache-a",
			Labels:     []string{"Cached"},
			Properties: map[string]any{"name": "fresh"},
		})
		engine.guardSeq.Add(1)
		stored := engine.cacheStoreNodeIfGuard(&Node{
			ID:         "test:batch-cache-a",
			Labels:     []string{"Cached"},
			Properties: map[string]any{"name": "stale"},
		}, guard)
		require.False(t, stored)

		got, ok := engine.cacheLoadNode("test:batch-cache-a")
		require.True(t, ok)
		require.Equal(t, "fresh", got.Properties["name"])
	})

	t.Run("rejects invalid ids", func(t *testing.T) {
		engine := newBatchCacheEngine(t)

		_, err := engine.BatchGetNodes([]NodeID{"test:batch-cache-a", ""})
		require.ErrorIs(t, err, ErrInvalidID)
	})

	t.Run("returns decode errors for malformed node bodies", func(t *testing.T) {
		engine := newBatchCacheEngine(t)

		require.NoError(t, engine.db.Set(nodeKey("test:batch-cache-bad"), []byte("not-a-node")))
		_, err := engine.BatchGetNodes([]NodeID{"test:batch-cache-bad"})
		require.Error(t, err)
	})

	t.Run("returns backend errors from GetMany", func(t *testing.T) {
		engine := newBatchCacheEngine(t)

		require.NoError(t, engine.db.Close())
		_, err := engine.BatchGetNodes([]NodeID{"test:batch-cache-a"})
		engine.closed.Store(true)
		require.ErrorIs(t, err, ErrStorageClosed)
	})
}

func TestTreeDBTransaction_CommitDeletedBodiesDoNotRemainCached(t *testing.T) {
	engine := newTestTreeDBEngine(t)

	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	_, err = tx.CreateNode(&Node{ID: "test:cache-delete-node", Labels: []string{"Doc"}})
	require.NoError(t, err)
	require.NoError(t, tx.DeleteNode("test:cache-delete-node"))
	require.NoError(t, tx.Commit())

	_, err = engine.GetNode("test:cache-delete-node")
	require.ErrorIs(t, err, ErrNotFound)
	engine.nodeCacheMu.RLock()
	_, cachedNode := engine.nodeCache["test:cache-delete-node"]
	engine.nodeCacheMu.RUnlock()
	require.False(t, cachedNode)

	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:cache-delete-a", Labels: []string{"Doc"}},
		{ID: "test:cache-delete-b", Labels: []string{"Doc"}},
	}))
	rawTx, err = engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx = rawTx.(*TreeDBTransaction)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.CreateEdge(&Edge{
		ID:        "test:cache-delete-edge",
		StartNode: "test:cache-delete-a",
		EndNode:   "test:cache-delete-b",
		Type:      "LINKS",
	}))
	require.NoError(t, tx.DeleteEdge("test:cache-delete-edge"))
	require.NoError(t, tx.Commit())

	_, err = engine.GetEdge("test:cache-delete-edge")
	require.ErrorIs(t, err, ErrNotFound)
	engine.edgeCacheMu.RLock()
	_, cachedEdge := engine.edgeCache["test:cache-delete-edge"]
	engine.edgeCacheMu.RUnlock()
	require.False(t, cachedEdge)
}

func TestTreeDBTransaction_CreateEdgeEndpointFastPathHonorsPendingAndDeletedNodes(t *testing.T) {
	engine := newTestTreeDBEngine(t)

	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	defer tx.Rollback()
	_, err = tx.CreateNode(&Node{ID: "test:pending-a", Labels: []string{"Doc"}})
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "test:pending-b", Labels: []string{"Doc"}})
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{
		ID:        "test:pending-edge",
		StartNode: "test:pending-a",
		EndNode:   "test:pending-b",
		Type:      "LINKS",
	}))
	require.NoError(t, tx.Commit())

	edge, err := engine.GetEdge("test:pending-edge")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:pending-a"), edge.StartNode)
	require.Equal(t, NodeID("test:pending-b"), edge.EndNode)

	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:deleted-a", Labels: []string{"Doc"}},
		{ID: "test:deleted-b", Labels: []string{"Doc"}},
	}))
	rawDelete, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	deleteTx := rawDelete.(*TreeDBTransaction)
	defer deleteTx.Rollback()
	require.NoError(t, deleteTx.DeleteNode("test:deleted-a"))
	err = deleteTx.CreateEdge(&Edge{
		ID:        "test:deleted-edge",
		StartNode: "test:deleted-a",
		EndNode:   "test:deleted-b",
		Type:      "LINKS",
	})
	require.ErrorIs(t, err, ErrInvalidEdge)
}

func TestTreeDBTransaction_BulkCreateEdgesClosedTransaction(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	require.NoError(t, tx.Rollback())

	require.NoError(t, tx.BulkCreateEdges(nil))
	err = tx.BulkCreateEdges([]*Edge{{
		ID:        "test:closed-edge",
		StartNode: "test:closed-a",
		EndNode:   "test:closed-b",
		Type:      "LINKS",
	}})
	require.ErrorIs(t, err, ErrTransactionClosed)
}

func TestTreeDBTransaction_BulkCreateEdgesFastPathHonorsVisibility(t *testing.T) {
	engine := newTestTreeDBEngine(t)

	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	defer tx.Rollback()

	_, err = tx.CreateNode(&Node{ID: "test:bulk-pending-a", Labels: []string{"Doc"}})
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{ID: "test:bulk-pending-b", Labels: []string{"Doc"}})
	require.NoError(t, err)
	require.NoError(t, tx.BulkCreateEdges([]*Edge{
		{
			ID:        "test:bulk-pending-edge-1",
			StartNode: "test:bulk-pending-a",
			EndNode:   "test:bulk-pending-b",
			Type:      "LINKS",
		},
		{
			ID:        "test:bulk-pending-edge-2",
			StartNode: "test:bulk-pending-a",
			EndNode:   "test:bulk-pending-b",
			Type:      "LINKS",
		},
	}))
	require.NoError(t, tx.Commit())

	edges, err := engine.GetOutgoingEdges("test:bulk-pending-a")
	require.NoError(t, err)
	require.Len(t, edges, 2)

	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:bulk-deleted-a", Labels: []string{"Doc"}},
		{ID: "test:bulk-deleted-b", Labels: []string{"Doc"}},
	}))
	rawDelete, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	deleteTx := rawDelete.(*TreeDBTransaction)
	defer deleteTx.Rollback()

	require.NoError(t, deleteTx.DeleteNode("test:bulk-deleted-a"))
	err = deleteTx.BulkCreateEdges([]*Edge{{
		ID:        "test:bulk-deleted-edge",
		StartNode: "test:bulk-deleted-a",
		EndNode:   "test:bulk-deleted-b",
		Type:      "LINKS",
	}})
	require.ErrorIs(t, err, ErrInvalidEdge)
}

func TestTreeDBTransaction_BulkCreateEdgesRejectsDuplicateAndMissingEndpoint(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:bulk-valid-a", Labels: []string{"Doc"}},
		{ID: "test:bulk-valid-b", Labels: []string{"Doc"}},
	}))

	rawDuplicate, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	duplicateTx := rawDuplicate.(*TreeDBTransaction)
	defer duplicateTx.Rollback()
	err = duplicateTx.BulkCreateEdges([]*Edge{
		{
			ID:        "test:bulk-duplicate-edge",
			StartNode: "test:bulk-valid-a",
			EndNode:   "test:bulk-valid-b",
			Type:      "LINKS",
		},
		{
			ID:        "test:bulk-duplicate-edge",
			StartNode: "test:bulk-valid-a",
			EndNode:   "test:bulk-valid-b",
			Type:      "LINKS",
		},
	})
	require.ErrorIs(t, err, ErrAlreadyExists)

	rawMissing, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	missingTx := rawMissing.(*TreeDBTransaction)
	defer missingTx.Rollback()
	err = missingTx.BulkCreateEdges([]*Edge{{
		ID:        "test:bulk-missing-edge",
		StartNode: "test:bulk-valid-a",
		EndNode:   "test:bulk-missing-b",
		Type:      "LINKS",
	}})
	require.ErrorIs(t, err, ErrInvalidEdge)
}

func TestTreeDBTransaction_BulkCreateEdgesDoesNotStagePartialBatch(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:bulk-partial-a", Labels: []string{"Doc"}},
		{ID: "test:bulk-partial-b", Labels: []string{"Doc"}},
	}))

	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	defer tx.Rollback()

	err = tx.BulkCreateEdges([]*Edge{
		{
			ID:        "test:bulk-partial-valid",
			StartNode: "test:bulk-partial-a",
			EndNode:   "test:bulk-partial-b",
			Type:      "LINKS",
		},
		{
			ID:        "test:bulk-partial-invalid",
			StartNode: "test:bulk-partial-a",
			EndNode:   "test:bulk-partial-missing",
			Type:      "LINKS",
		},
	})
	require.ErrorIs(t, err, ErrInvalidEdge)

	_, err = tx.GetEdge("test:bulk-partial-valid")
	require.ErrorIs(t, err, ErrNotFound)
	require.Empty(t, tx.createdEdges)
	require.Empty(t, tx.pendingEdges)
}

func TestTreeDBEngine_BulkCreateEdgesInvalidatesCachedAdjacency(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:bulk-cache-a", Labels: []string{"Doc"}},
		{ID: "test:bulk-cache-b", Labels: []string{"Doc"}},
	}))

	edges, err := engine.GetOutgoingEdges("test:bulk-cache-a")
	require.NoError(t, err)
	require.Empty(t, edges)

	require.NoError(t, engine.BulkCreateEdges([]*Edge{
		{
			ID:        "test:bulk-cache-edge-1",
			StartNode: "test:bulk-cache-a",
			EndNode:   "test:bulk-cache-b",
			Type:      "LINKS",
		},
		{
			ID:        "test:bulk-cache-edge-2",
			StartNode: "test:bulk-cache-a",
			EndNode:   "test:bulk-cache-b",
			Type:      "LINKS",
		},
	}))

	edges, err = engine.GetOutgoingEdges("test:bulk-cache-a")
	require.NoError(t, err)
	require.Len(t, edges, 2)
}

func TestTreeDBEngine_EdgeCacheHonorsMaxItemsOnBulkStore(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	engine.edgeCacheMaxItems = 1
	engine.edgeCache = make(map[EdgeID]*Edge, engine.edgeCacheMaxItems)
	engine.edgeCacheByPtr = make(map[*Edge]EdgeID, engine.edgeCacheMaxItems)

	engine.cacheStoreEdges([]*Edge{
		{ID: "test:bulk-cache-cap-1", StartNode: "test:a", EndNode: "test:b", Type: "LINKS"},
		{ID: "test:bulk-cache-cap-2", StartNode: "test:a", EndNode: "test:b", Type: "LINKS"},
	})

	require.LessOrEqual(t, len(engine.edgeCache), engine.edgeCacheMaxItems)
	require.Len(t, engine.edgeCacheByPtr, len(engine.edgeCache))
}

func TestNamespacedTreeDBEngine_DirectFastPathStripsNamespaceAndReturnsCopies(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	namespaced := NewNamespacedEngine(engine, "tenant")

	_, err := namespaced.CreateNode(&Node{
		ID:         "a",
		Labels:     []string{"Doc"},
		Properties: map[string]any{"name": "alpha"},
	})
	require.NoError(t, err)
	_, err = namespaced.CreateNode(&Node{ID: "b", Labels: []string{"Doc"}})
	require.NoError(t, err)
	require.NoError(t, namespaced.CreateEdge(&Edge{
		ID:         "e1",
		StartNode:  "a",
		EndNode:    "b",
		Type:       "LINKS",
		Properties: map[string]any{"weight": int64(7)},
	}))

	gotNode, err := namespaced.GetNode("a")
	require.NoError(t, err)
	require.Equal(t, NodeID("a"), gotNode.ID)
	require.Equal(t, "alpha", gotNode.Properties["name"])
	gotNode.ID = "mutated"
	gotNode.Properties["name"] = "mutated"

	againNode, err := namespaced.GetNode("a")
	require.NoError(t, err)
	require.Equal(t, NodeID("a"), againNode.ID)
	require.Equal(t, "alpha", againNode.Properties["name"])

	gotEdge, err := namespaced.GetEdge("e1")
	require.NoError(t, err)
	require.Equal(t, EdgeID("e1"), gotEdge.ID)
	require.Equal(t, NodeID("a"), gotEdge.StartNode)
	require.Equal(t, NodeID("b"), gotEdge.EndNode)
	require.Equal(t, int64(7), gotEdge.Properties["weight"])
	gotEdge.ID = "mutated"
	gotEdge.StartNode = "mutated"
	gotEdge.Properties["weight"] = int64(99)

	againEdge, err := namespaced.GetEdge("e1")
	require.NoError(t, err)
	require.Equal(t, EdgeID("e1"), againEdge.ID)
	require.Equal(t, NodeID("a"), againEdge.StartNode)
	require.Equal(t, NodeID("b"), againEdge.EndNode)
	require.Equal(t, int64(7), againEdge.Properties["weight"])

	rawNode, err := engine.GetNode("tenant:a")
	require.NoError(t, err)
	require.Equal(t, NodeID("tenant:a"), rawNode.ID)
	rawEdge, err := engine.GetEdge("tenant:e1")
	require.NoError(t, err)
	require.Equal(t, EdgeID("tenant:e1"), rawEdge.ID)
	require.Equal(t, NodeID("tenant:a"), rawEdge.StartNode)
	require.Equal(t, NodeID("tenant:b"), rawEdge.EndNode)
}

func TestTreeDBEngine_CRUDIndexesRevisionsAndReopen(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: dir})
	require.NoError(t, err)

	a := &Node{
		ID:         "test:a",
		Labels:     []string{"Person", "Employee"},
		Properties: map[string]any{"name": "Alice"},
	}
	b := &Node{
		ID:         "test:b",
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Bob"},
	}
	require.Equal(t, NodeID("test:a"), mustCreateTreeDBNode(t, engine, a))
	require.Equal(t, NodeID("test:b"), mustCreateTreeDBNode(t, engine, b))

	edge := &Edge{
		ID:         "test:e1",
		StartNode:  "test:a",
		EndNode:    "test:b",
		Type:       "KNOWS",
		Properties: map[string]any{"since": int64(2024)},
	}
	require.NoError(t, engine.CreateEdge(edge))

	gotA, err := engine.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, "Alice", gotA.Properties["name"])

	batch, err := engine.BatchGetNodes([]NodeID{"test:a", "test:b"})
	require.NoError(t, err)
	require.Len(t, batch, 2)

	people, err := engine.GetNodesByLabel("person")
	require.NoError(t, err)
	require.ElementsMatch(t, []NodeID{"test:a", "test:b"}, treeDBNodeIDs(people))

	outgoing, err := engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e1"}, treeDBEdgeIDs(outgoing))
	incoming, err := engine.GetIncomingEdges("test:b")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e1"}, treeDBEdgeIDs(incoming))
	byType, err := engine.GetEdgesByType("knows")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e1"}, treeDBEdgeIDs(byType))
	between, err := engine.GetEdgesBetween("test:a", "test:b")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e1"}, treeDBEdgeIDs(between))
	require.Equal(t, EdgeID("test:e1"), engine.GetEdgeBetween("test:a", "test:b", "KNOWS").ID)

	nodeCount, err := engine.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(2), nodeCount)
	edgeCount, err := engine.EdgeCount()
	require.NoError(t, err)
	require.Equal(t, int64(1), edgeCount)
	testNodeCount, err := engine.NodeCountByPrefix("test:")
	require.NoError(t, err)
	require.Equal(t, int64(2), testNodeCount)
	testEdgeCount, err := engine.EdgeCountByPrefix("test:")
	require.NoError(t, err)
	require.Equal(t, int64(1), testEdgeCount)
	personCount, err := engine.NodeCountByLabelInNamespace("test", "Person")
	require.NoError(t, err)
	require.Equal(t, int64(2), personCount)
	require.Contains(t, engine.ListNamespaces(), "test")

	rev1, err := engine.GetNodeEntryRevision("test:a")
	require.NoError(t, err)
	require.NotEqual(t, treedb.LegacyEntryRevision, rev1)
	gotA.Properties["name"] = "Alice Cooper"
	require.NoError(t, engine.UpdateNode(gotA))
	rev2, err := engine.GetNodeEntryRevision("test:a")
	require.NoError(t, err)
	require.Greater(t, uint64(rev2), uint64(rev1))

	require.NoError(t, engine.Sync())
	require.NoError(t, engine.Close())

	reopened, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: dir})
	require.NoError(t, err)
	defer reopened.Close()

	reopenedA, err := reopened.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, "Alice Cooper", reopenedA.Properties["name"])
	reopenedEdge, err := reopened.GetEdge("test:e1")
	require.NoError(t, err)
	require.Equal(t, EdgeID("test:e1"), reopenedEdge.ID)
	reopenedCount, err := reopened.NodeCountByPrefix("test:")
	require.NoError(t, err)
	require.Equal(t, int64(2), reopenedCount)
}

func TestTreeDBEngine_AdjacentEdgeCacheInvalidatesOnMutation(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:a", Labels: []string{"Node"}},
		{ID: "test:b", Labels: []string{"Node"}},
		{ID: "test:c", Labels: []string{"Node"}},
		{ID: "test:d", Labels: []string{"Node"}},
	}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}))

	outgoing, err := engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e1"}, treeDBEdgeIDs(outgoing))
	incoming, err := engine.GetIncomingEdges("test:b")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e1"}, treeDBEdgeIDs(incoming))
	require.True(t, treeDBAdjCacheHasOutgoing(engine, "test:a"))
	require.True(t, treeDBAdjCacheHasIncoming(engine, "test:b"))

	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e2", StartNode: "test:a", EndNode: "test:c", Type: "LINK"}))
	require.False(t, treeDBAdjCacheHasOutgoing(engine, "test:a"))
	require.False(t, treeDBAdjCacheHasIncoming(engine, "test:c"))
	outgoing, err = engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e1", "test:e2"}, treeDBEdgeIDs(outgoing))
	incoming, err = engine.GetIncomingEdges("test:c")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e2"}, treeDBEdgeIDs(incoming))

	e2, err := engine.GetEdge("test:e2")
	require.NoError(t, err)
	e2.StartNode = "test:d"
	require.NoError(t, engine.UpdateEdge(e2))
	require.False(t, treeDBAdjCacheHasOutgoing(engine, "test:a"))
	require.False(t, treeDBAdjCacheHasOutgoing(engine, "test:d"))
	require.False(t, treeDBAdjCacheHasIncoming(engine, "test:c"))
	outgoing, err = engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e1"}, treeDBEdgeIDs(outgoing))
	outgoing, err = engine.GetOutgoingEdges("test:d")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e2"}, treeDBEdgeIDs(outgoing))
	incoming, err = engine.GetIncomingEdges("test:c")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e2"}, treeDBEdgeIDs(incoming))

	require.NoError(t, engine.DeleteEdge("test:e1"))
	require.False(t, treeDBAdjCacheHasOutgoing(engine, "test:a"))
	require.False(t, treeDBAdjCacheHasIncoming(engine, "test:b"))
	outgoing, err = engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.Empty(t, outgoing)
	incoming, err = engine.GetIncomingEdges("test:b")
	require.NoError(t, err)
	require.Empty(t, incoming)
}

func TestTreeDBEngine_AdjacentEdgeCacheFiltersEndpointRaces(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:a", Labels: []string{"Node"}},
		{ID: "test:b", Labels: []string{"Node"}},
		{ID: "test:c", Labels: []string{"Node"}},
		{ID: "test:d", Labels: []string{"Node"}},
	}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}))

	outgoing, err := engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e1"}, treeDBEdgeIDs(outgoing))
	incoming, err := engine.GetIncomingEdges("test:b")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e1"}, treeDBEdgeIDs(incoming))

	updated, err := engine.GetEdge("test:e1")
	require.NoError(t, err)
	updated.StartNode = "test:d"
	updated.EndNode = "test:c"
	require.NoError(t, engine.UpdateEdge(updated))

	engine.adjCacheMu.Lock()
	engine.outgoingAdjCache["test:a"] = []EdgeID{"test:e1"}
	engine.incomingAdjCache["test:b"] = []EdgeID{"test:e1"}
	engine.incomingAdjCache["test:a"] = []EdgeID{"test:e1"}
	engine.adjCacheMu.Unlock()

	outgoing, err = engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.Empty(t, outgoing)
	incoming, err = engine.GetIncomingEdges("test:b")
	require.NoError(t, err)
	require.Empty(t, incoming)

	outgoing, incoming, err = engine.GetAdjacentEdges("test:a")
	require.NoError(t, err)
	require.Empty(t, outgoing)
	require.Empty(t, incoming)

	outgoing, err = engine.GetOutgoingEdges("test:d")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e1"}, treeDBEdgeIDs(outgoing))
	incoming, err = engine.GetIncomingEdges("test:c")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e1"}, treeDBEdgeIDs(incoming))
}

func TestTreeDBEngine_AdjacentEdgeCacheSkipsStaleFillAfterMutationGuard(t *testing.T) {
	engine := newTestTreeDBEngine(t)

	guard := engine.guardSeq.Load()
	engine.adjCacheStoreOutgoing("test:a", []EdgeID{"test:fresh-out"})
	engine.adjCacheStoreIncoming("test:a", []EdgeID{"test:fresh-in"})
	engine.guardSeq.Add(1)

	require.False(t, engine.adjCacheStoreOutgoingIfGuard("test:a", []EdgeID{"test:stale-out"}, guard))
	require.False(t, engine.adjCacheStoreIncomingIfGuard("test:a", []EdgeID{"test:stale-in"}, guard))

	outIDs, ok := engine.adjCacheLoadOutgoing("test:a")
	require.True(t, ok)
	require.Equal(t, []EdgeID{"test:fresh-out"}, outIDs)
	inIDs, ok := engine.adjCacheLoadIncoming("test:a")
	require.True(t, ok)
	require.Equal(t, []EdgeID{"test:fresh-in"}, inIDs)
}

func TestTreeDBEngine_AdjacentEdgeCacheInvalidatesOnDeleteNodeCascade(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:a", Labels: []string{"Node"}},
		{ID: "test:b", Labels: []string{"Node"}},
	}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "LINK"}))

	outgoing, err := engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e1"}, treeDBEdgeIDs(outgoing))
	incoming, err := engine.GetIncomingEdges("test:b")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e1"}, treeDBEdgeIDs(incoming))
	require.True(t, treeDBAdjCacheHasOutgoing(engine, "test:a"))
	require.True(t, treeDBAdjCacheHasIncoming(engine, "test:b"))

	require.NoError(t, engine.DeleteNode("test:a"))
	require.False(t, treeDBAdjCacheHasOutgoing(engine, "test:a"))
	require.False(t, treeDBAdjCacheHasIncoming(engine, "test:b"))
	outgoing, err = engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.Empty(t, outgoing)
	incoming, err = engine.GetIncomingEdges("test:b")
	require.NoError(t, err)
	require.Empty(t, incoming)
}

func TestTreeDBEngine_GetAdjacentEdgesUsesSharedCache(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:center", Labels: []string{"Node"}},
		{ID: "test:out", Labels: []string{"Node"}},
		{ID: "test:in", Labels: []string{"Node"}},
	}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:out-edge", StartNode: "test:center", EndNode: "test:out", Type: "LINK"}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:in-edge", StartNode: "test:in", EndNode: "test:center", Type: "LINK"}))

	outgoing, incoming, err := engine.GetAdjacentEdges("test:center")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:out-edge"}, treeDBEdgeIDs(outgoing))
	require.ElementsMatch(t, []EdgeID{"test:in-edge"}, treeDBEdgeIDs(incoming))
	require.True(t, treeDBAdjCacheHasOutgoing(engine, "test:center"))
	require.True(t, treeDBAdjCacheHasIncoming(engine, "test:center"))

	outgoing, incoming, err = engine.GetAdjacentEdges("test:center")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:out-edge"}, treeDBEdgeIDs(outgoing))
	require.ElementsMatch(t, []EdgeID{"test:in-edge"}, treeDBEdgeIDs(incoming))

	_, _, err = engine.GetAdjacentEdges("")
	require.ErrorIs(t, err, ErrInvalidID)
}

func TestTreeDBEngine_AdjacentEdgeCacheHonorsClosedEngine(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:center", Labels: []string{"Node"}},
		{ID: "test:out", Labels: []string{"Node"}},
		{ID: "test:in", Labels: []string{"Node"}},
	}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:out-edge", StartNode: "test:center", EndNode: "test:out", Type: "LINK"}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:in-edge", StartNode: "test:in", EndNode: "test:center", Type: "LINK"}))

	_, err := engine.GetOutgoingEdges("test:center")
	require.NoError(t, err)
	_, err = engine.GetIncomingEdges("test:center")
	require.NoError(t, err)
	_, _, err = engine.GetAdjacentEdges("test:center")
	require.NoError(t, err)
	require.True(t, treeDBAdjCacheHasOutgoing(engine, "test:center"))
	require.True(t, treeDBAdjCacheHasIncoming(engine, "test:center"))

	require.NoError(t, engine.Close())
	_, err = engine.GetOutgoingEdges("test:center")
	require.ErrorIs(t, err, ErrStorageClosed)
	_, err = engine.GetIncomingEdges("test:center")
	require.ErrorIs(t, err, ErrStorageClosed)
	_, _, err = engine.GetAdjacentEdges("test:center")
	require.ErrorIs(t, err, ErrStorageClosed)
}

func TestTreeDBEngine_UpdateEdgeEvictsMutatedTraversalCacheOnFailure(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:a", Labels: []string{"Node"}},
		{ID: "test:b", Labels: []string{"Node"}},
	}))
	require.NoError(t, engine.CreateEdge(&Edge{
		ID:         "test:e1",
		StartNode:  "test:a",
		EndNode:    "test:b",
		Type:       "LINK",
		Properties: map[string]any{"kind": "stored"},
	}))

	warmSharedTraversalEdge := func() *Edge {
		t.Helper()
		outgoing, err := engine.GetOutgoingEdges("test:a")
		require.NoError(t, err)
		require.Len(t, outgoing, 1)
		require.True(t, treeDBAdjCacheHasOutgoing(engine, "test:a"))
		outgoing, err = engine.GetOutgoingEdges("test:a")
		require.NoError(t, err)
		require.Len(t, outgoing, 1)
		return outgoing[0]
	}
	requireCleanPersistedEdge := func() {
		t.Helper()
		persisted, err := engine.GetEdge("test:e1")
		require.NoError(t, err)
		require.Equal(t, EdgeID("test:e1"), persisted.ID)
		require.Equal(t, "stored", persisted.Properties["kind"])
		require.NotContains(t, persisted.Properties, "leaked")
		require.Equal(t, NodeID("test:b"), persisted.EndNode)

		outgoing, err := engine.GetOutgoingEdges("test:a")
		require.NoError(t, err)
		require.Len(t, outgoing, 1)
		require.Equal(t, EdgeID("test:e1"), outgoing[0].ID)
		require.Equal(t, "stored", outgoing[0].Properties["kind"])
		require.NotContains(t, outgoing[0].Properties, "leaked")
		require.Equal(t, NodeID("test:b"), outgoing[0].EndNode)
	}

	shared := warmSharedTraversalEdge()
	shared.Properties["kind"] = "mutated"
	shared.Properties["leaked"] = "missing endpoint"
	shared.EndNode = "test:missing"
	require.Error(t, engine.UpdateEdge(shared))
	requireCleanPersistedEdge()

	shared = warmSharedTraversalEdge()
	shared.Properties["kind"] = "mutated"
	shared.Properties["leaked"] = "invalid endpoint"
	shared.EndNode = ""
	require.ErrorIs(t, engine.UpdateEdge(shared), ErrInvalidEdge)
	requireCleanPersistedEdge()

	shared = warmSharedTraversalEdge()
	shared.Properties["kind"] = "mutated"
	shared.Properties["leaked"] = "changed id"
	shared.ID = "test:missing-edge"
	require.ErrorIs(t, engine.UpdateEdge(shared), ErrNotFound)
	requireCleanPersistedEdge()
}

func TestTreeDBTransaction_ReadYourWritesAndConflict(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	_, err := engine.CreateNode(&Node{
		ID:         "test:conflict",
		Labels:     []string{"Conflict"},
		Properties: map[string]any{"version": int64(1)},
	})
	require.NoError(t, err)

	rawTx1, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx1 := rawTx1.(*TreeDBTransaction)
	defer tx1.Rollback()

	_, err = tx1.CreateNode(&Node{
		ID:         "test:pending",
		Labels:     []string{"Pending"},
		Properties: map[string]any{"created": true},
	})
	require.NoError(t, err)
	pending, err := tx1.GetNodesByLabel("pending")
	require.NoError(t, err)
	require.ElementsMatch(t, []NodeID{"test:pending"}, treeDBNodeIDs(pending))

	staged, err := tx1.GetNode("test:conflict")
	require.NoError(t, err)
	staged.Properties["version"] = int64(2)
	require.NoError(t, tx1.UpdateNode(staged))

	rawTx2, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx2 := rawTx2.(*TreeDBTransaction)
	concurrent, err := tx2.GetNode("test:conflict")
	require.NoError(t, err)
	concurrent.Properties["version"] = int64(3)
	require.NoError(t, tx2.UpdateNode(concurrent))
	require.NoError(t, tx2.Commit())

	err = tx1.Commit()
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrConflict), "commit error = %v", err)
	_, err = engine.GetNode("test:pending")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestTreeDBTransaction_LabelScanRepeatabilityWithinSnapshot(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:user-0", Labels: []string{"User"}},
		{ID: "test:user-1", Labels: []string{"User"}},
		{ID: "test:admin-0", Labels: []string{"Admin"}},
	}))

	rawScan, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	txScan := rawScan.(*TreeDBTransaction)
	defer txScan.Rollback()
	require.NoError(t, txScan.SetNamespace("test"))

	before, err := txScan.GetNodesByLabel("User")
	require.NoError(t, err)
	beforeIDs := treeDBNodeIDs(before)
	require.ElementsMatch(t, []NodeID{"test:user-0", "test:user-1"}, beforeIDs)

	_, err = engine.CreateNode(&Node{ID: "test:user-new", Labels: []string{"User"}})
	require.NoError(t, err)

	after, err := txScan.GetNodesByLabel("User")
	require.NoError(t, err)
	require.ElementsMatch(t, beforeIDs, treeDBNodeIDs(after))

	rawFresh, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	txFresh := rawFresh.(*TreeDBTransaction)
	defer txFresh.Rollback()
	require.NoError(t, txFresh.SetNamespace("test"))
	fresh, err := txFresh.GetNodesByLabel("User")
	require.NoError(t, err)
	require.ElementsMatch(t, []NodeID{"test:user-0", "test:user-1", "test:user-new"}, treeDBNodeIDs(fresh))
}

func TestTreeDBTransaction_AllNodesRepeatabilityWithinSnapshot(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:all-0", Labels: []string{"Node"}},
		{ID: "test:all-1", Labels: []string{"Node"}},
	}))

	rawScan, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	txScan := rawScan.(*TreeDBTransaction)
	defer txScan.Rollback()
	require.NoError(t, txScan.SetNamespace("test"))

	before, err := txScan.AllNodes()
	require.NoError(t, err)
	beforeIDs := treeDBNodeIDs(before)
	require.ElementsMatch(t, []NodeID{"test:all-0", "test:all-1"}, beforeIDs)

	_, err = engine.CreateNode(&Node{ID: "test:all-new", Labels: []string{"Node"}})
	require.NoError(t, err)

	after, err := txScan.AllNodes()
	require.NoError(t, err)
	require.ElementsMatch(t, beforeIDs, treeDBNodeIDs(after))
}

func TestTreeDBTransaction_UnscopedRangeScansFailClosed(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:unscoped-a", Labels: []string{"User"}},
		{ID: "test:unscoped-b", Labels: []string{"User"}},
	}))
	require.NoError(t, engine.CreateEdge(&Edge{
		ID:        "test:unscoped-e",
		StartNode: "test:unscoped-a",
		EndNode:   "test:unscoped-b",
		Type:      "REL",
	}))

	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	defer tx.Rollback()

	_, err = tx.GetNodesByLabel("User")
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = tx.AllNodes()
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = tx.GetEdgesByType("REL")
	require.ErrorIs(t, err, ErrNotImplemented)
	_, _, err = engine.DeleteByPrefix("")
	require.ErrorIs(t, err, ErrNotImplemented)
}

func TestTreeDBTransaction_EdgeTraversalRepeatabilityAcrossConcurrentDelete(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:edge-start", Labels: []string{"Node"}},
		{ID: "test:edge-target", Labels: []string{"Node"}, Properties: map[string]any{"name": "target"}},
	}))
	require.NoError(t, engine.CreateEdge(&Edge{
		ID:        "test:edge-link",
		StartNode: "test:edge-start",
		EndNode:   "test:edge-target",
		Type:      "LINKS",
	}))

	rawRead, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	txRead := rawRead.(*TreeDBTransaction)
	defer txRead.Rollback()
	require.NoError(t, txRead.SetNamespace("test"))

	before, err := txRead.GetEdgesBetween("test:edge-start", "test:edge-target")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:edge-link"}, treeDBEdgeIDs(before))
	target, err := txRead.GetNode("test:edge-target")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:edge-target"), target.ID)

	require.NoError(t, engine.DeleteNode("test:edge-target"))

	after, err := txRead.GetEdgesBetween("test:edge-start", "test:edge-target")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:edge-link"}, treeDBEdgeIDs(after))
	target, err = txRead.GetNode("test:edge-target")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:edge-target"), target.ID)

	fresh, err := engine.GetEdgesBetween("test:edge-start", "test:edge-target")
	require.NoError(t, err)
	require.Empty(t, fresh)
	_, err = engine.GetNode("test:edge-target")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestTreeDBTransaction_SnapshotReadPreconditionConflictsOnCommit(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	_, err := engine.CreateNode(&Node{
		ID:         "test:snapshot-conflict",
		Labels:     []string{"User"},
		Properties: map[string]any{"version": int64(1)},
	})
	require.NoError(t, err)

	rawRead, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	txRead := rawRead.(*TreeDBTransaction)
	defer txRead.Rollback()
	require.NoError(t, txRead.SetNamespace("test"))

	before, err := txRead.GetNodesByLabel("User")
	require.NoError(t, err)
	require.ElementsMatch(t, []NodeID{"test:snapshot-conflict"}, treeDBNodeIDs(before))

	updated, err := engine.GetNode("test:snapshot-conflict")
	require.NoError(t, err)
	updated.Properties["version"] = int64(2)
	require.NoError(t, engine.UpdateNode(updated))

	stillOld, err := txRead.GetNode("test:snapshot-conflict")
	require.NoError(t, err)
	require.Equal(t, int64(1), stillOld.Properties["version"])

	err = txRead.Commit()
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrConflict), "commit error = %v", err)
}

func TestTreeDBTransaction_FirstRangeReadUsesBeginSnapshot(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	_, err := engine.CreateNode(&Node{
		ID:         "test:first-range",
		Labels:     []string{"User"},
		Properties: map[string]any{"version": int64(1)},
	})
	require.NoError(t, err)

	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))

	require.NoError(t, engine.DeleteNode("test:first-range"))

	nodes, err := tx.GetNodesByLabel("User")
	require.NoError(t, err)
	require.ElementsMatch(t, []NodeID{"test:first-range"}, treeDBNodeIDs(nodes))

	_, err = tx.CreateNode(&Node{ID: "test:first-range-marker", Labels: []string{"Marker"}})
	require.NoError(t, err)
	err = tx.Commit()
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrConflict), "commit error = %v", err)
}

func TestTreeDBTransaction_NamespacedLabelScanIgnoresForeignNamespaceConflicts(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	tenantA := NewNamespacedEngine(engine, "tenant_a")
	tenantB := NewNamespacedEngine(engine, "tenant_b")

	_, err := tenantA.CreateNode(&Node{
		ID:         "a-user",
		Labels:     []string{"User"},
		Properties: map[string]any{"version": int64(1)},
	})
	require.NoError(t, err)
	_, err = tenantB.CreateNode(&Node{
		ID:         "b-user",
		Labels:     []string{"User"},
		Properties: map[string]any{"version": int64(1)},
	})
	require.NoError(t, err)

	tx, err := tenantA.BeginGraphTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	nodes, err := tx.GetNodesByLabel("User")
	require.NoError(t, err)
	require.ElementsMatch(t, []NodeID{"a-user"}, treeDBNodeIDs(nodes))

	foreign, err := tenantB.GetNode("b-user")
	require.NoError(t, err)
	foreign.Properties["version"] = int64(2)
	require.NoError(t, tenantB.UpdateNode(foreign))

	_, err = tx.CreateNode(&Node{ID: "a-marker", Labels: []string{"Marker"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func TestTreeDBEngine_NamespacedDirectGetUnprefixesIDs(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	tenant := NewNamespacedEngine(engine, "tenant_a")

	_, err := tenant.CreateNode(&Node{
		ID:         "user",
		Labels:     []string{"User"},
		Properties: map[string]any{"name": "Alice"},
	})
	require.NoError(t, err)
	_, err = tenant.CreateNode(&Node{ID: "other", Labels: []string{"User"}})
	require.NoError(t, err)
	require.NoError(t, tenant.CreateEdge(&Edge{
		ID:         "rel",
		StartNode:  "user",
		EndNode:    "other",
		Type:       "KNOWS",
		Properties: map[string]any{"since": int64(2024)},
	}))

	node, err := tenant.GetNode("user")
	require.NoError(t, err)
	require.Equal(t, NodeID("user"), node.ID)
	require.Equal(t, "Alice", node.Properties["name"])

	prefixedNode, err := tenant.GetNode("tenant_a:user")
	require.NoError(t, err)
	require.Equal(t, NodeID("user"), prefixedNode.ID)

	edge, err := tenant.GetEdge("rel")
	require.NoError(t, err)
	require.Equal(t, EdgeID("rel"), edge.ID)
	require.Equal(t, NodeID("user"), edge.StartNode)
	require.Equal(t, NodeID("other"), edge.EndNode)

	prefixedEdge, err := tenant.GetEdge("tenant_a:rel")
	require.NoError(t, err)
	require.Equal(t, EdgeID("rel"), prefixedEdge.ID)
	require.Equal(t, NodeID("user"), prefixedEdge.StartNode)
	require.Equal(t, NodeID("other"), prefixedEdge.EndNode)
}

func TestTreeDBTransaction_SetNamespaceRejectsCrossNamespacePointReads(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	tenantA := NewNamespacedEngine(engine, "tenant_a")
	tenantB := NewNamespacedEngine(engine, "tenant_b")

	_, err := tenantA.CreateNode(&Node{ID: "a-user", Labels: []string{"User"}})
	require.NoError(t, err)
	_, err = tenantB.CreateNode(&Node{ID: "b-user", Labels: []string{"User"}})
	require.NoError(t, err)
	require.NoError(t, tenantB.CreateEdge(&Edge{
		ID:        "b-rel",
		StartNode: "b-user",
		EndNode:   "b-user",
		Type:      "SELF",
	}))

	tx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("tenant_a"))

	node, err := tx.GetNode("tenant_a:a-user")
	require.NoError(t, err)
	require.Equal(t, NodeID("tenant_a:a-user"), node.ID)

	_, err = tx.GetNode("tenant_b:b-user")
	require.ErrorIs(t, err, ErrCrossNamespaceTransaction)

	_, err = tx.GetEdge("tenant_b:b-rel")
	require.ErrorIs(t, err, ErrCrossNamespaceTransaction)
}

func TestTreeDBTransaction_LabelScanPhantomConflictsOnCommit(t *testing.T) {
	engine := newTestTreeDBEngine(t)

	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))

	nodes, err := tx.GetNodesByLabel("User")
	require.NoError(t, err)
	require.Empty(t, nodes)

	_, err = engine.CreateNode(&Node{ID: "test:user-new", Labels: []string{"User"}})
	require.NoError(t, err)

	_, err = tx.CreateNode(&Node{ID: "test:marker", Labels: []string{"Marker"}})
	require.NoError(t, err)
	err = tx.Commit()
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrConflict), "commit error = %v", err)
}

func TestTreeDBTransaction_EdgeBetweenPhantomConflictsOnCommit(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:between-a", Labels: []string{"Node"}},
		{ID: "test:between-b", Labels: []string{"Node"}},
	}))

	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))

	edges, err := tx.GetEdgesBetween("test:between-a", "test:between-b")
	require.NoError(t, err)
	require.Empty(t, edges)

	require.NoError(t, engine.CreateEdge(&Edge{
		ID:        "test:between-e",
		StartNode: "test:between-a",
		EndNode:   "test:between-b",
		Type:      "LINKS",
	}))

	_, err = tx.CreateNode(&Node{ID: "test:between-marker", Labels: []string{"Marker"}})
	require.NoError(t, err)
	err = tx.Commit()
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrConflict), "commit error = %v", err)
}

func TestTreeDBEngine_GetEdgeBetweenHeadPromotesSurvivorOnDelete(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:head-a", Labels: []string{"Node"}},
		{ID: "test:head-b", Labels: []string{"Node"}},
	}))
	first := &Edge{ID: "test:head-e1", StartNode: "test:head-a", EndNode: "test:head-b", Type: "KNOWS"}
	second := &Edge{ID: "test:head-e2", StartNode: "test:head-a", EndNode: "test:head-b", Type: "KNOWS"}
	require.NoError(t, engine.CreateEdge(first))
	require.NoError(t, engine.CreateEdge(second))

	head, err := engine.db.GetAppend(treeDBEdgeBetweenHeadKey("test:head-a", "test:head-b", "KNOWS"), nil)
	require.NoError(t, err)
	require.Equal(t, []byte(second.ID), head)

	require.NoError(t, engine.DeleteEdge(second.ID))
	head, err = engine.db.GetAppend(treeDBEdgeBetweenHeadKey("test:head-a", "test:head-b", "KNOWS"), nil)
	require.NoError(t, err)
	require.Equal(t, []byte(first.ID), head)
	require.Equal(t, first.ID, engine.GetEdgeBetween("test:head-a", "test:head-b", "KNOWS").ID)
}

func TestTreeDBTransaction_EdgeBetweenHeadSkipsMovedPendingReplacement(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:move-a", Labels: []string{"Node"}},
		{ID: "test:move-b", Labels: []string{"Node"}},
		{ID: "test:move-c", Labels: []string{"Node"}},
	}))
	first := &Edge{ID: "test:move-e1", StartNode: "test:move-a", EndNode: "test:move-b", Type: "KNOWS"}
	second := &Edge{ID: "test:move-e2", StartNode: "test:move-a", EndNode: "test:move-b", Type: "KNOWS"}
	require.NoError(t, engine.CreateEdge(first))
	require.NoError(t, engine.CreateEdge(second))
	require.Equal(t, second.ID, engine.GetEdgeBetween("test:move-a", "test:move-b", "KNOWS").ID)

	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))

	moved := copyEdge(first)
	moved.EndNode = "test:move-c"
	require.NoError(t, tx.UpdateEdge(moved))
	require.NoError(t, tx.DeleteEdge(second.ID))
	require.NoError(t, tx.Commit())

	require.Nil(t, engine.GetEdgeBetween("test:move-a", "test:move-b", "KNOWS"))
	require.Equal(t, first.ID, engine.GetEdgeBetween("test:move-a", "test:move-c", "KNOWS").ID)
}

func TestTreeDBTransaction_EdgeBetweenHeadCanPromotePendingReplacement(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	deleted := &Edge{
		ID:        "test:pending-head-old",
		StartNode: "test:pending-head-a",
		EndNode:   "test:pending-head-b",
		Type:      "KNOWS",
	}
	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))

	tx.pendingEdges = map[EdgeID]*Edge{
		"test:pending-head-new": {
			ID:        "test:pending-head-new",
			StartNode: deleted.StartNode,
			EndNode:   deleted.EndNode,
			Type:      deleted.Type,
		},
	}
	replacement, err := tx.replacementEdgeBetweenHead(deleted)
	require.NoError(t, err)
	require.Equal(t, EdgeID("test:pending-head-new"), replacement)
}

func TestTreeDBTransaction_GetFirstNodeByLabelNotFound(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))

	node, err := tx.GetFirstNodeByLabel("Missing")
	require.Nil(t, node)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestTreeDBTransaction_CreateSkipOnlyBypassesUUIDIDs(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:existing", Labels: []string{"Entity"}})
	require.NoError(t, err)

	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	defer tx.Rollback()

	require.NoError(t, tx.SetSkipCreateExistenceCheck(true))
	_, err = tx.CreateNode(&Node{ID: "test:existing", Labels: []string{"Entity"}})
	require.ErrorIs(t, err, ErrAlreadyExists)

	_, err = tx.CreateNode(&Node{ID: "test:550e8400-e29b-41d4-a716-446655440000", Labels: []string{"Entity"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func TestTreeDBTransaction_DeleteNodeConflictsWithConcurrentEdgeCreate(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:guard-a", Labels: []string{"Guard"}},
		{ID: "test:guard-b", Labels: []string{"Guard"}},
	}))

	rawDelete, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	deleteTx := rawDelete.(*TreeDBTransaction)
	defer deleteTx.Rollback()
	require.NoError(t, deleteTx.DeleteNode("test:guard-a"))

	rawEdge, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	edgeTx := rawEdge.(*TreeDBTransaction)
	require.NoError(t, edgeTx.CreateEdge(&Edge{
		ID:        "test:guard-e",
		StartNode: "test:guard-a",
		EndNode:   "test:guard-b",
		Type:      "LINKS",
	}))
	require.NoError(t, edgeTx.Commit())

	err = deleteTx.Commit()
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrConflict), "commit error = %v", err)
	_, err = engine.GetNode("test:guard-a")
	require.NoError(t, err)
	_, err = engine.GetEdge("test:guard-e")
	require.NoError(t, err)
}

func TestTreeDBTransaction_UniqueConstraintSerializesCommitWindow(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.GetSchemaForNamespace("test").AddUniqueConstraint("unique_user_email", "User", "email"))

	rawTx1, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx1 := rawTx1.(*TreeDBTransaction)
	defer tx1.Rollback()
	_, err = tx1.CreateNode(&Node{
		ID:         "test:unique-1",
		Labels:     []string{"User"},
		Properties: map[string]any{"email": "same@example.com"},
	})
	require.NoError(t, err)

	rawTx2, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx2 := rawTx2.(*TreeDBTransaction)
	defer tx2.Rollback()
	_, err = tx2.CreateNode(&Node{
		ID:         "test:unique-2",
		Labels:     []string{"User"},
		Properties: map[string]any{"email": "same@example.com"},
	})
	require.NoError(t, err)

	require.NoError(t, tx1.Commit())
	err = tx2.Commit()
	require.Error(t, err)
	require.Contains(t, err.Error(), "same@example.com")
}

func TestTreeDBTransaction_UniqueConstraintScansExistingRowsWhenCacheIncomplete(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	_, err := engine.CreateNode(&Node{
		ID:         "test:existing-unique",
		Labels:     []string{"User"},
		Properties: map[string]any{"email": "existing@example.com"},
	})
	require.NoError(t, err)

	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddUniqueConstraint("unique_user_email", "User", "email"))
	_, found, constrained, cacheComplete := schema.LookupUniqueConstraintValueForPlanning("User", "email", "existing@example.com")
	require.True(t, constrained)
	require.False(t, found)
	require.False(t, cacheComplete)

	_, err = engine.CreateNode(&Node{
		ID:         "test:duplicate-unique",
		Labels:     []string{"User"},
		Properties: map[string]any{"email": "existing@example.com"},
	})
	require.Error(t, err)
	var constraintErr *ConstraintViolationError
	require.ErrorAs(t, err, &constraintErr)
	require.Equal(t, ConstraintUnique, constraintErr.Type)
	require.Contains(t, err.Error(), "existing@example.com")

	_, err = engine.CreateNode(&Node{
		ID:         "test:new-unique",
		Labels:     []string{"User"},
		Properties: map[string]any{"email": "new@example.com"},
	})
	require.NoError(t, err)
}

func TestTreeDBTransaction_UniqueConstraintRejectsPendingDuplicate(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.GetSchemaForNamespace("test").AddUniqueConstraint("unique_user_email", "User", "email"))

	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	defer tx.Rollback()
	_, err = tx.CreateNode(&Node{
		ID:         "test:pending-unique-a",
		Labels:     []string{"User"},
		Properties: map[string]any{"email": "pending@example.com"},
	})
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{
		ID:         "test:pending-unique-b",
		Labels:     []string{"User"},
		Properties: map[string]any{"email": "pending@example.com"},
	})
	require.Error(t, err)
	var constraintErr *ConstraintViolationError
	require.ErrorAs(t, err, &constraintErr)
	require.Equal(t, ConstraintUnique, constraintErr.Type)
	require.Contains(t, err.Error(), "pending@example.com")
}

func TestTreeDBTransaction_UnsupportedConstraintsFailClosed(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:       "user_email_exists",
		Type:       ConstraintExists,
		Label:      "User",
		Properties: []string{"email"},
	}))
	_, err := engine.CreateNode(&Node{
		ID:         "test:unsupported-user",
		Labels:     []string{"User"},
		Properties: map[string]any{"email": "present@example.com"},
	})
	require.ErrorIs(t, err, ErrNotImplemented)

	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	require.NoError(t, tx.SetDeferredConstraintValidation(true))
	_, err = tx.CreateNode(&Node{
		ID:         "test:unsupported-deferred-user",
		Labels:     []string{"User"},
		Properties: map[string]any{"email": "deferred@example.com"},
	})
	require.NoError(t, err)
	err = tx.Commit()
	require.ErrorIs(t, err, ErrNotImplemented)
	require.Contains(t, err.Error(), "constraint violation")
	require.False(t, tx.IsActive())
	require.NoError(t, tx.Rollback())

	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:unsupported-a", Labels: []string{"Endpoint"}},
		{ID: "test:unsupported-b", Labels: []string{"Endpoint"}},
	}))
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:       "rel_since_exists",
		Type:       ConstraintExists,
		EntityType: ConstraintEntityRelationship,
		Label:      "LINKS",
		Properties: []string{"since"},
	}))
	err = engine.CreateEdge(&Edge{
		ID:        "test:unsupported-e",
		StartNode: "test:unsupported-a",
		EndNode:   "test:unsupported-b",
		Type:      "LINKS",
	})
	require.ErrorIs(t, err, ErrNotImplemented)
}

func TestTreeDBTransaction_CompositeIndexUpdateRemovesOldEntries(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddCompositeIndex("person_location", "Person", []string{"country", "city"}))
	idx, ok := schema.GetCompositeIndex("person_location")
	require.True(t, ok)

	_, err := engine.CreateNode(&Node{
		ID:         "test:person-1",
		Labels:     []string{"Person"},
		Properties: map[string]any{"country": "US", "city": "NYC"},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []NodeID{"test:person-1"}, idx.LookupFull("US", "NYC"))

	node, err := engine.GetNode("test:person-1")
	require.NoError(t, err)
	node.Properties["city"] = "Boston"
	require.NoError(t, engine.UpdateNode(node))

	require.Empty(t, idx.LookupFull("US", "NYC"))
	require.ElementsMatch(t, []NodeID{"test:person-1"}, idx.LookupFull("US", "Boston"))
}

func TestTreeDBTransaction_SchemaStateUsesNetNodeMutations(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddUniqueConstraint("unique_email", "User", "email"))
	require.NoError(t, schema.AddPropertyIndex("idx_name", "User", []string{"name"}))
	require.NoError(t, schema.AddCompositeIndex("idx_location", "User", []string{"country", "city"}))
	composite, ok := schema.GetCompositeIndex("idx_location")
	require.True(t, ok)

	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	_, err = tx.CreateNode(&Node{
		ID:     "test:ephemeral-user",
		Labels: []string{"User"},
		Properties: map[string]any{
			"email":   "ephemeral-old@example.com",
			"name":    "Ephemeral Old",
			"country": "US",
			"city":    "SF",
		},
	})
	require.NoError(t, err)
	require.NoError(t, tx.UpdateNode(&Node{
		ID:     "test:ephemeral-user",
		Labels: []string{"User"},
		Properties: map[string]any{
			"email":   "ephemeral-new@example.com",
			"name":    "Ephemeral New",
			"country": "US",
			"city":    "NYC",
		},
	}))
	require.NoError(t, tx.DeleteNode("test:ephemeral-user"))
	require.NoError(t, tx.Commit())

	for _, email := range []string{"ephemeral-old@example.com", "ephemeral-new@example.com"} {
		_, found, constrained := schema.LookupUniqueConstraintValue("User", "email", email)
		require.True(t, constrained)
		require.False(t, found, "stale unique value %q remained registered", email)
	}
	require.Empty(t, schema.PropertyIndexLookup("User", "name", "Ephemeral Old"))
	require.Empty(t, schema.PropertyIndexLookup("User", "name", "Ephemeral New"))
	require.Empty(t, composite.LookupFull("US", "SF"))
	require.Empty(t, composite.LookupFull("US", "NYC"))
	_, err = engine.CreateNode(&Node{
		ID:         "test:reuse-ephemeral",
		Labels:     []string{"User"},
		Properties: map[string]any{"email": "ephemeral-new@example.com"},
	})
	require.NoError(t, err)

	_, err = engine.CreateNode(&Node{
		ID:     "test:persistent-user",
		Labels: []string{"User"},
		Properties: map[string]any{
			"email":   "old@example.com",
			"name":    "Old Name",
			"country": "US",
			"city":    "LA",
		},
	})
	require.NoError(t, err)
	rawTx, err = engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx = rawTx.(*TreeDBTransaction)
	require.NoError(t, tx.UpdateNode(&Node{
		ID:     "test:persistent-user",
		Labels: []string{"User"},
		Properties: map[string]any{
			"email":   "mid@example.com",
			"name":    "Mid Name",
			"country": "US",
			"city":    "CHI",
		},
	}))
	require.NoError(t, tx.UpdateNode(&Node{
		ID:     "test:persistent-user",
		Labels: []string{"User"},
		Properties: map[string]any{
			"email":   "final@example.com",
			"name":    "Final Name",
			"country": "US",
			"city":    "SEA",
		},
	}))
	require.NoError(t, tx.Commit())

	for _, email := range []string{"old@example.com", "mid@example.com"} {
		_, found, constrained := schema.LookupUniqueConstraintValue("User", "email", email)
		require.True(t, constrained)
		require.False(t, found, "stale unique value %q remained registered", email)
	}
	nodeID, found, constrained := schema.LookupUniqueConstraintValue("User", "email", "final@example.com")
	require.True(t, constrained)
	require.True(t, found)
	require.Equal(t, NodeID("test:persistent-user"), nodeID)
	require.Empty(t, schema.PropertyIndexLookup("User", "name", "Old Name"))
	require.Empty(t, schema.PropertyIndexLookup("User", "name", "Mid Name"))
	require.ElementsMatch(t, []NodeID{"test:persistent-user"}, schema.PropertyIndexLookup("User", "name", "Final Name"))
	require.Empty(t, composite.LookupFull("US", "LA"))
	require.Empty(t, composite.LookupFull("US", "CHI"))
	require.ElementsMatch(t, []NodeID{"test:persistent-user"}, composite.LookupFull("US", "SEA"))
}

func TestTreeDBEngine_DeleteByPrefixCascades(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "tenant:a", Labels: []string{"Tenant"}},
		{ID: "tenant:b", Labels: []string{"Tenant"}},
	}))
	_, err := engine.CreateNode(&Node{ID: "other:c", Labels: []string{"Other"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "tenant:e1", StartNode: "tenant:a", EndNode: "tenant:b", Type: "LINK"}))

	nodesDeleted, edgesDeleted, err := engine.DeleteByPrefix("tenant:")
	require.NoError(t, err)
	require.Equal(t, int64(2), nodesDeleted)
	require.Equal(t, int64(1), edgesDeleted)

	_, err = engine.GetNode("tenant:a")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetNode("tenant:b")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetEdge("tenant:e1")
	require.ErrorIs(t, err, ErrNotFound)
	other, err := engine.GetNode("other:c")
	require.NoError(t, err)
	require.Equal(t, NodeID("other:c"), other.ID)

	tenantNodes, err := engine.NodeCountByPrefix("tenant:")
	require.NoError(t, err)
	require.Equal(t, int64(0), tenantNodes)
}

func TestTreeDBEngine_PendingEmbeddingsIndex(t *testing.T) {
	engine := newTestTreeDBEngine(t)

	_, err := engine.CreateNode(&Node{ID: "test:skip", Labels: []string{"Doc"}, Properties: map[string]any{"text": "skip"}})
	require.NoError(t, err)
	require.Equal(t, 0, engine.PendingEmbeddingsCount())

	engine.SetEmbeddingsEnabled(true)
	skip, err := engine.GetNode("test:skip")
	require.NoError(t, err)
	skip.Properties["embedding_skipped"] = true
	require.NoError(t, engine.UpdateNode(skip))

	_, err = engine.CreateNode(&Node{ID: "test:embed", Labels: []string{"Doc"}, Properties: map[string]any{"text": "embed"}})
	require.NoError(t, err)
	require.Equal(t, 1, engine.PendingEmbeddingsCount())
	found := engine.FindNodeNeedingEmbedding()
	require.NotNil(t, found)
	require.Equal(t, NodeID("test:embed"), found.ID)

	engine.MarkNodeEmbedded("test:embed")
	require.Equal(t, 0, engine.PendingEmbeddingsCount())
	engine.AddToPendingEmbeddings("test:embed")
	require.Equal(t, 1, engine.PendingEmbeddingsCount())

	embedded, err := engine.GetNode("test:embed")
	require.NoError(t, err)
	embedded.ChunkEmbeddings = [][]float32{{1, 2, 3}}
	require.NoError(t, engine.UpdateNode(embedded))
	require.Equal(t, 0, engine.PendingEmbeddingsCount())

	cleared, err := engine.ClearAllEmbeddingsForPrefix("test:")
	require.NoError(t, err)
	require.Equal(t, 1, cleared)
	require.Equal(t, 1, engine.PendingEmbeddingsCount())
	found = engine.FindNodeNeedingEmbedding()
	require.NotNil(t, found)
	require.Equal(t, NodeID("test:embed"), found.ID)
}

func TestTreeDBEngine_UpdateNodeEmbeddingOnlyUpdatesExistingNodes(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	engine.SetEmbeddingsEnabled(true)
	_, err := engine.CreateNode(&Node{
		ID:         "test:embed-update",
		Labels:     []string{"Doc"},
		Properties: map[string]any{"content": "preserve"},
	})
	require.NoError(t, err)
	require.Equal(t, 1, engine.PendingEmbeddingsCount())

	err = engine.UpdateNodeEmbedding(&Node{
		ID:              "test:embed-update",
		ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
		EmbedMeta:       map[string]any{"embedding_model": "test-model"},
	})
	require.NoError(t, err)
	updated, err := engine.GetNode("test:embed-update")
	require.NoError(t, err)
	require.Equal(t, "preserve", updated.Properties["content"])
	require.Equal(t, [][]float32{{0.1, 0.2, 0.3}}, updated.ChunkEmbeddings)
	require.Equal(t, "test-model", updated.EmbedMeta["embedding_model"])
	require.Equal(t, 0, engine.PendingEmbeddingsCount())

	err = engine.UpdateNodeEmbedding(&Node{
		ID:              "test:missing-embed-update",
		ChunkEmbeddings: [][]float32{{0.4}},
	})
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetNode("test:missing-embed-update")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestTreeDBEngine_EventCallbacks(t *testing.T) {
	engine := newTestTreeDBEngine(t)

	var nodeCreated atomic.Int32
	var nodeUpdated atomic.Int32
	var nodeDeleted atomic.Int32
	var edgeCreated atomic.Int32
	var edgeUpdated atomic.Int32
	var edgeDeleted atomic.Int32
	engine.OnNodeCreated(func(*Node) { nodeCreated.Add(1) })
	engine.OnNodeUpdated(func(*Node) { nodeUpdated.Add(1) })
	engine.OnNodeDeleted(func(NodeID) { nodeDeleted.Add(1) })
	engine.OnEdgeCreated(func(*Edge) { edgeCreated.Add(1) })
	engine.OnEdgeUpdated(func(*Edge) { edgeUpdated.Add(1) })
	engine.OnEdgeDeleted(func(EdgeID) { edgeDeleted.Add(1) })

	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:event-a", Labels: []string{"Event"}},
		{ID: "test:event-b", Labels: []string{"Event"}},
	}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:event-e", StartNode: "test:event-a", EndNode: "test:event-b", Type: "EVENT"}))
	updatedNode, err := engine.GetNode("test:event-a")
	require.NoError(t, err)
	updatedNode.Properties = map[string]any{"updated": true}
	require.NoError(t, engine.UpdateNode(updatedNode))
	updatedEdge, err := engine.GetEdge("test:event-e")
	require.NoError(t, err)
	updatedEdge.Properties = map[string]any{"updated": true}
	require.NoError(t, engine.UpdateEdge(updatedEdge))
	require.NoError(t, engine.DeleteNode("test:event-a"))

	assert.Equal(t, int32(2), nodeCreated.Load())
	assert.Equal(t, int32(1), nodeUpdated.Load())
	assert.Equal(t, int32(1), nodeDeleted.Load())
	assert.Equal(t, int32(1), edgeCreated.Load())
	assert.Equal(t, int32(1), edgeUpdated.Load())
	assert.Equal(t, int32(1), edgeDeleted.Load())
}

func TestTreeDBEngine_NodeEventCallbackMutationDoesNotAffectCache(t *testing.T) {
	engine := newTestTreeDBEngine(t)

	engine.OnNodeCreated(func(node *Node) {
		node.Labels[0] = "CallbackMutated"
		node.Properties["name"] = "callback-created"
		node.Properties["callbackOnly"] = true
	})
	engine.OnNodeUpdated(func(node *Node) {
		node.Labels[0] = "CallbackUpdated"
		node.Properties["name"] = "callback-updated"
		node.Properties["callbackOnly"] = true
	})

	createInput := &Node{
		ID:         "test:event-cache",
		Labels:     []string{"Created"},
		Properties: map[string]any{"name": "stored"},
	}
	_, err := engine.CreateNode(createInput)
	require.NoError(t, err)
	require.Equal(t, []string{"Created"}, createInput.Labels)
	require.Equal(t, "stored", createInput.Properties["name"])
	require.NotContains(t, createInput.Properties, "callbackOnly")

	created, err := engine.GetNode("test:event-cache")
	require.NoError(t, err)
	require.Equal(t, []string{"Created"}, created.Labels)
	require.Equal(t, "stored", created.Properties["name"])
	require.NotContains(t, created.Properties, "callbackOnly")

	updateInput := &Node{
		ID:         "test:event-cache",
		Labels:     []string{"Updated"},
		Properties: map[string]any{"name": "updated"},
		CreatedAt:  created.CreatedAt,
	}
	require.NoError(t, engine.UpdateNode(updateInput))
	require.Equal(t, []string{"Updated"}, updateInput.Labels)
	require.Equal(t, "updated", updateInput.Properties["name"])
	require.NotContains(t, updateInput.Properties, "callbackOnly")

	updated, err := engine.GetNode("test:event-cache")
	require.NoError(t, err)
	require.Equal(t, []string{"Updated"}, updated.Labels)
	require.Equal(t, "updated", updated.Properties["name"])
	require.NotContains(t, updated.Properties, "callbackOnly")
}

func TestTreeDBEngine_EdgeEventCallbackMutationDoesNotAffectCache(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:event-edge-a", Labels: []string{"Event"}},
		{ID: "test:event-edge-b", Labels: []string{"Event"}},
	}))

	engine.OnEdgeCreated(func(edge *Edge) {
		edge.Type = "CALLBACK_CREATED"
		edge.Properties["name"] = "callback-created"
		edge.Properties["callbackOnly"] = true
	})
	engine.OnEdgeUpdated(func(edge *Edge) {
		edge.Type = "CALLBACK_UPDATED"
		edge.Properties["name"] = "callback-updated"
		edge.Properties["callbackOnly"] = true
	})

	createInput := &Edge{
		ID:         "test:event-edge",
		StartNode:  "test:event-edge-a",
		EndNode:    "test:event-edge-b",
		Type:       "CREATED",
		Properties: map[string]any{"name": "stored"},
	}
	require.NoError(t, engine.CreateEdge(createInput))
	require.Equal(t, "CREATED", createInput.Type)
	require.Equal(t, "stored", createInput.Properties["name"])
	require.NotContains(t, createInput.Properties, "callbackOnly")

	created, err := engine.GetEdge("test:event-edge")
	require.NoError(t, err)
	require.Equal(t, "CREATED", created.Type)
	require.Equal(t, "stored", created.Properties["name"])
	require.NotContains(t, created.Properties, "callbackOnly")

	updateInput := &Edge{
		ID:         "test:event-edge",
		StartNode:  "test:event-edge-a",
		EndNode:    "test:event-edge-b",
		Type:       "UPDATED",
		Properties: map[string]any{"name": "updated"},
		CreatedAt:  created.CreatedAt,
	}
	require.NoError(t, engine.UpdateEdge(updateInput))
	require.Equal(t, "UPDATED", updateInput.Type)
	require.Equal(t, "updated", updateInput.Properties["name"])
	require.NotContains(t, updateInput.Properties, "callbackOnly")

	updated, err := engine.GetEdge("test:event-edge")
	require.NoError(t, err)
	require.Equal(t, "UPDATED", updated.Type)
	require.Equal(t, "updated", updated.Properties["name"])
	require.NotContains(t, updated.Properties, "callbackOnly")
}

func TestTreeDBEngine_NoSchemaWritesDoNotCreateSchemaManager(t *testing.T) {
	engine := newTestTreeDBEngine(t)

	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:no-schema-a", Labels: []string{"NoSchema"}, Properties: map[string]any{"name": "a"}},
		{ID: "test:no-schema-b", Labels: []string{"NoSchema"}, Properties: map[string]any{"name": "b"}},
	}))
	require.NoError(t, engine.CreateEdge(&Edge{
		ID:        "test:no-schema-edge",
		StartNode: "test:no-schema-a",
		EndNode:   "test:no-schema-b",
		Type:      "NO_SCHEMA",
	}))

	engine.schemaMu.RLock()
	_, created := engine.schemas["test"]
	engine.schemaMu.RUnlock()
	require.False(t, created)
}

func TestTreeDBEngine_SchemaPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: dir})
	require.NoError(t, err)

	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddUniqueConstraint("unique_email", "User", "email"))
	_, err = engine.CreateNode(&Node{
		ID:         "test:user-1",
		Labels:     []string{"User"},
		Properties: map[string]any{"email": "alice@example.com"},
	})
	require.NoError(t, err)
	require.NoError(t, engine.Sync())
	require.NoError(t, engine.Close())

	reopened, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: dir})
	require.NoError(t, err)
	defer reopened.Close()

	reopenedSchema := reopened.GetSchemaForNamespace("test")
	nodeID, found, constrained := reopenedSchema.LookupUniqueConstraintValue("User", "email", "alice@example.com")
	require.True(t, constrained)
	require.True(t, found)
	require.Equal(t, NodeID("test:user-1"), nodeID)
}

func TestTreeDBEngine_StreamNodesByPrefixStops(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:stream-a", Labels: []string{"Stream"}},
		{ID: "test:stream-b", Labels: []string{"Stream"}},
	}))
	_, err := engine.CreateNode(&Node{ID: "other:stream-c", Labels: []string{"Stream"}})
	require.NoError(t, err)

	seen := 0
	err = engine.StreamNodesByPrefix(context.Background(), "test:", func(*Node) error {
		seen++
		return ErrIterationStopped
	})
	require.NoError(t, err)
	require.Equal(t, 1, seen)
}

func mustCreateTreeDBNode(t *testing.T, engine *TreeDBEngine, node *Node) NodeID {
	t.Helper()
	id, err := engine.CreateNode(node)
	require.NoError(t, err)
	return id
}

func treeDBNodeIDs(nodes []*Node) []NodeID {
	ids := make([]NodeID, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.ID)
	}
	return ids
}

func treeDBEdgeIDs(edges []*Edge) []EdgeID {
	ids := make([]EdgeID, 0, len(edges))
	for _, edge := range edges {
		ids = append(ids, edge.ID)
	}
	return ids
}

func treeDBAdjCacheHasOutgoing(engine *TreeDBEngine, nodeID NodeID) bool {
	engine.adjCacheMu.RLock()
	_, ok := engine.outgoingAdjCache[nodeID]
	engine.adjCacheMu.RUnlock()
	return ok
}

func treeDBAdjCacheHasIncoming(engine *TreeDBEngine, nodeID NodeID) bool {
	engine.adjCacheMu.RLock()
	_, ok := engine.incomingAdjCache[nodeID]
	engine.adjCacheMu.RUnlock()
	return ok
}
