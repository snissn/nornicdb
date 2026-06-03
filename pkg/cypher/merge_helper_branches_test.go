package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMergeTopLevelHelpers(t *testing.T) {
	node := &storage.Node{ID: "n1", Labels: []string{"A", "B"}, Properties: map[string]interface{}{"k": "v", "n": int64(1)}}
	require.True(t, mergeNodeHasLabels(node, []string{"A"}))
	require.False(t, mergeNodeHasLabels(node, []string{"A", "C"}))
	require.True(t, mergeNodeHasAnyLabel(node, []string{"C", "B"}))
	require.False(t, mergeNodeHasAnyLabel(node, []string{"X"}))

	require.True(t, mergeNodeMatches(node, []string{"A"}, map[string]interface{}{"k": "v"}))
	require.False(t, mergeNodeMatches(node, []string{"C"}, map[string]interface{}{"k": "v"}))
	require.True(t, mergeNodeMatchesAnyLabel(node, []string{"C", "A"}, map[string]interface{}{"n": int64(1)}))
	require.False(t, mergeNodeMatchesAnyLabel(nil, []string{"A"}, map[string]interface{}{"k": "v"}))

	require.True(t, mergeCreateConflict(storage.ErrAlreadyExists))
	require.True(t, mergeCreateConflict(fmt.Errorf("already exists in db")))
	require.False(t, mergeCreateConflict(fmt.Errorf("different")))

	require.True(t, mergePropsContainUnresolvedParamLiteral(map[string]interface{}{"a": "$x"}))
	require.False(t, mergePropsContainUnresolvedParamLiteral(map[string]interface{}{"a": "x"}))

	clone := cloneNodeForMergeMutation(node)
	require.NotSame(t, node, clone)
	require.Equal(t, node.Labels, clone.Labels)
	require.Equal(t, node.Properties, clone.Properties)
	clone.Labels[0] = "Changed"
	clone.Properties["k"] = "changed"
	require.Equal(t, "A", node.Labels[0])
	require.Equal(t, "v", node.Properties["k"])
}

func TestMergeCacheAndIndexHelpers(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "merge_helper_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (n:Person {id:'p1', email:'a@x'})", nil)
	require.NoError(t, err)
	nodes, err := store.GetNodesByLabel("Person")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	n := nodes[0]

	props := map[string]interface{}{"id": "p1", "email": "a@x"}
	exec.cacheMergeNode([]string{"Person"}, props, n)
	cached := exec.findMergeNodeInCache(store, []string{"Person"}, props)
	require.NotNil(t, cached)
	require.Equal(t, n.ID, cached.ID)

	exec.evictMergeNodeCacheEntries([]string{"Person"}, map[string]interface{}{"id": "p1"}, n.ID)
	cached = exec.findMergeNodeInCache(store, []string{"Person"}, map[string]interface{}{"id": "p1"})
	require.Nil(t, cached)

	exec.cacheMergeNode([]string{"Person"}, props, n)
	require.NoError(t, store.DeleteNode(n.ID))
	cached = exec.findMergeNodeInCache(store, []string{"Person"}, props)
	require.Nil(t, cached)

	idx := &storage.CompositeIndex{Properties: []string{"id", "email"}}
	require.True(t, mergeIndexMatchesAllProperties(idx, props))
	require.False(t, mergeIndexMatchesAllProperties(&storage.CompositeIndex{Properties: []string{"id"}}, props))
	require.False(t, mergeIndexMatchesAllProperties(nil, props))
	require.Equal(t, []interface{}{"p1", "a@x"}, compositeLookupValues(idx, props))

	_, err = exec.Execute(ctx, "CREATE (a:L {id:'1'}), (b:L {id:'2'})", nil)
	require.NoError(t, err)
	all, err := store.GetNodesByLabel("L")
	require.NoError(t, err)
	require.Len(t, all, 2)
	loaded := exec.loadMergeCandidateNodes(store, []storage.NodeID{all[0].ID, all[1].ID, all[0].ID})
	require.Len(t, loaded, 2)
}

func TestFindMergeNodeAnyLabel_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "merge_any_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (n:A {k:'x'}), (m:B {k:'y'})", nil)
	require.NoError(t, err)

	node, err := exec.findMergeNodeAnyLabel(store, []string{"A", "C"}, map[string]interface{}{"k": "x"})
	require.NoError(t, err)
	require.NotNil(t, node)
	require.Contains(t, node.Labels, "A")

	node, err = exec.findMergeNodeAnyLabel(store, []string{"Z"}, map[string]interface{}{"k": "none"})
	require.NoError(t, err)
	require.Nil(t, node)

	_, err = exec.Execute(ctx, "CREATE INDEX idx_a_k IF NOT EXISTS FOR (n:A) ON (n.k)", nil)
	require.NoError(t, err)
	node, err = exec.findMergeNodeAnyLabel(store, []string{"A", "B"}, map[string]interface{}{"k": "x"})
	require.NoError(t, err)
	require.NotNil(t, node)
	require.Contains(t, node.Labels, "A")
}
