package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCallTailSetBasedAndTraversalDepth_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "call_tail_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	for _, n := range []string{"a", "b", "c", "d"} {
		_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(n), Labels: []string{"N"}, Properties: map[string]interface{}{"id": n}})
		require.NoError(t, err)
	}
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "ab", Type: "REL", StartNode: "a", EndNode: "b"}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "bc", Type: "REL", StartNode: "b", EndNode: "c"}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "ad", Type: "ALT", StartNode: "a", EndNode: "d"}))

	a, err := store.GetNode("a")
	require.NoError(t, err)
	b, err := store.GetNode("b")
	require.NoError(t, err)

	// executeCallTailSetBased guard branches
	res, handled := exec.executeCallTailSetBased(ctx, &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{a}}}, "RETURN 1", nil, nil)
	require.False(t, handled)
	require.Nil(t, res)

	res, handled = exec.executeCallTailSetBased(ctx, &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{a}}}, "RETURN 1", []int{0}, nil)
	require.False(t, handled)
	require.Nil(t, res)

	res, handled = exec.executeCallTailSetBased(
		ctx,
		&ExecuteResult{Columns: []string{"n", "m"}, Rows: [][]interface{}{{a, b}}},
		"MATCH (n)-[rel]->(m) RETURN count(rel) AS c",
		[]int{0, 1},
		nil,
	)
	require.False(t, handled)
	require.Nil(t, res)

	// Empty seed-id branch returns handled empty result.
	res, handled = exec.executeCallTailSetBased(
		ctx,
		&ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{(*storage.Node)(nil)}}},
		"MATCH (n)-[rel]->(m) RETURN count(rel) AS c",
		[]int{0},
		[]string{"c"},
	)
	require.True(t, handled)
	require.Equal(t, []string{"c"}, res.Columns)
	require.Empty(t, res.Rows)

	// Relationship-tail branch success.
	res, handled = exec.executeCallTailSetBased(
		ctx,
		&ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{a}}},
		"MATCH (n)-[rel:REL]->(m) RETURN count(rel) AS c",
		[]int{0},
		[]string{"c"},
	)
	require.False(t, handled)
	require.Nil(t, res)

	tailRes, err := exec.executeCallTail(
		ctx,
		&ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{a}}},
		"MATCH (n)-[rel:REL]->(m) RETURN count(rel) AS c",
	)
	require.NoError(t, err)
	require.Equal(t, []string{"c"}, tailRes.Columns)
	require.Len(t, tailRes.Rows, 1)
	direct, err := exec.Execute(ctx, "MATCH (n {id:'a'})-[rel:REL]->(m) RETURN count(rel) AS c", nil)
	require.NoError(t, err)
	require.Len(t, direct.Rows, 1)
	require.EqualValues(t, direct.Rows[0][0], tailRes.Rows[0][0])

	// UNWIND-based set path with scalar propagation.
	res, handled = exec.executeCallTailSetBased(
		ctx,
		&ExecuteResult{Columns: []string{"n", "score"}, Rows: [][]interface{}{{a, int64(10)}, {b, int64(20)}}},
		"MATCH (n) RETURN n.id AS id, score",
		[]int{0, 1},
		[]string{"id", "score"},
	)
	require.True(t, handled)
	require.NotNil(t, res)
	require.NotEmpty(t, res.Columns)
	require.Len(t, res.Rows, 2)

	// maxDepthForTraversalMatchFromNode branches
	match := &TraversalMatch{
		EndNode: nodePatternInfo{labels: []string{"N"}},
		Relationship: RelationshipPattern{
			Direction: "outgoing",
			Types:     []string{"REL", "ALT"},
			MinHops:   1,
			MaxHops:   3,
		},
	}
	depthCtx := &callTailMaxDepthContext{
		nodeCache:  map[storage.NodeID]*storage.Node{a.ID: a, b.ID: b},
		visited:    map[storage.EdgeID]bool{},
		relTypeSet: map[string]struct{}{"REL": {}, "ALT": {}},
		best:       0,
	}
	require.NoError(t, exec.maxDepthForTraversalMatchFromNode(a, 0, match, depthCtx))
	require.GreaterOrEqual(t, depthCtx.best, 2)
	require.NoError(t, exec.maxDepthForTraversalMatchFromNode(nil, 0, match, depthCtx))
}

func TestApplyRemoveAndMergeWhereContext_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "remove_merge_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	n := &storage.Node{ID: "n1", Labels: []string{"Person", "Tmp"}, Properties: map[string]interface{}{"name": "A", "drop": int64(1)}}
	_, err := store.CreateNode(n)
	require.NoError(t, err)

	mr := &ExecuteResult{Columns: []string{"n"}, Rows: [][]interface{}{{n}, {"not-node"}}}
	out := &ExecuteResult{Stats: &QueryStats{}}
	require.NoError(t, exec.applyRemoveToMatchedRows(store, mr, "n.drop, n:Tmp", out))
	require.Equal(t, 1, out.Stats.PropertiesSet)

	nAfter, err := store.GetNode("n1")
	require.NoError(t, err)
	_, hasDrop := nAfter.Properties["drop"]
	require.False(t, hasDrop)
	require.Equal(t, []string{"Person"}, nAfter.Labels)

	require.True(t, exec.evaluateWhereForMergeContext(ctx, "", map[string]*storage.Node{"n": nAfter}, nil))
	require.True(t, exec.evaluateWhereForMergeContext(ctx, "n.name = 'A'", map[string]*storage.Node{"n": nAfter}, nil))
	require.False(t, exec.evaluateWhereForMergeContext(ctx, "n.name = 'B'", map[string]*storage.Node{"n": nAfter}, nil))
}
