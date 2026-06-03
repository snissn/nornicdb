package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecuteCallTailSetBased_BranchGuards(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_set_based_branches"))
	ctx := context.Background()
	seed := &ExecuteResult{Columns: []string{"node"}, Rows: [][]interface{}{{&storage.Node{ID: storage.NodeID("n1")}}}}

	res, ok := exec.executeCallTailSetBased(ctx, seed, "MATCH (node) RETURN node", nil, nil)
	require.False(t, ok)
	require.Nil(t, res)

	res, ok = exec.executeCallTailSetBased(ctx, seed, "RETURN node", []int{0}, nil)
	require.False(t, ok)
	require.Nil(t, res)

	// Typed relationship tails should fall back to per-row execution.
	res, ok = exec.executeCallTailSetBased(ctx, seed, "MATCH (node)-[:REL]->(m) RETURN node", []int{0}, nil)
	require.False(t, ok)
	require.Nil(t, res)
}

func TestExecuteCallTailSetBased_RelationshipBranchFallbacks(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_set_based_rel_branches"))
	ctx := context.Background()
	n1 := &storage.Node{ID: storage.NodeID("n1")}
	n2 := &storage.Node{ID: storage.NodeID("n2")}

	// Relationship path requires exactly one node seed column.
	seedTwoNodes := &ExecuteResult{
		Columns: []string{"a", "b"},
		Rows:    [][]interface{}{{n1, n2}},
	}
	res, ok := exec.executeCallTailSetBased(ctx, seedTwoNodes, "MATCH (a)-[r]->(b) RETURN a", []int{0, 1}, nil)
	require.False(t, ok)
	require.Nil(t, res)

	// Nil node seeds produce an immediate empty result when relationship fast path is selected.
	var nilNode *storage.Node
	seedNilNode := &ExecuteResult{
		Columns: []string{"node", "score"},
		Rows:    [][]interface{}{{nilNode, 0.9}},
	}
	res, ok = exec.executeCallTailSetBased(ctx, seedNilNode, "MATCH (node)-[r]->(m) WITH node, score RETURN node", []int{0, 1}, []string{"node"})
	require.True(t, ok)
	require.NotNil(t, res)
	require.Equal(t, []string{"node"}, res.Columns)
	require.Empty(t, res.Rows)

	// Scalar rewriting requires a WITH clause; without it the set-based route rejects.
	seedWithScalar := &ExecuteResult{
		Columns: []string{"node", "score"},
		Rows:    [][]interface{}{{n1, 0.9}},
	}
	res, ok = exec.executeCallTailSetBased(ctx, seedWithScalar, "MATCH (node)-[r]->(m) RETURN node, score", []int{0, 1}, nil)
	require.False(t, ok)
	require.Nil(t, res)
}
