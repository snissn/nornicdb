package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCollectOptionalMatchInitialNodes_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "opt_match_initial_nodes_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Person {name:'Alice', team:'red'}), (:Person {name:'Bob', team:'blue'}), (:Robot {name:'R2'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE INDEX person_name_idx IF NOT EXISTS FOR (n:Person) ON (n.name)", nil)
	require.NoError(t, err)

	np := nodePatternInfo{variable: "n", labels: []string{"Person"}}

	nodes, err := exec.collectOptionalMatchInitialNodes(ctx, np, "n.name = 'Alice'", "n.name = $name", map[string]interface{}{"name": "Alice"})
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "Alice", nodes[0].Properties["name"])

	nodes, err = exec.collectOptionalMatchInitialNodes(ctx, np, "n.name IN ['Alice','Bob']", "", nil)
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	nodes, err = exec.collectOptionalMatchInitialNodes(ctx, np, "n.name IS NOT NULL", "", nil)
	require.NoError(t, err)
	require.Len(t, nodes, 2)

	// Used-index but empty candidate set should fail-open to label scan, then WHERE keeps semantics.
	nodes, err = exec.collectOptionalMatchInitialNodes(ctx, np, "n.name = 'missing'", "", nil)
	require.NoError(t, err)
	require.Empty(t, nodes)

	// No index path: filter by inline pattern properties.
	nodes, err = exec.collectOptionalMatchInitialNodes(ctx, nodePatternInfo{variable: "r", labels: []string{"Robot"}, properties: map[string]interface{}{"name": "R2"}}, "", "", nil)
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, "R2", nodes[0].Properties["name"])
}

func TestResolveReturnExprFromVarMap_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "resolve_return_expr_cov"))
	ctx := context.Background()

	n1 := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice"}}
	n2 := &storage.Node{ID: storage.NodeID("n2"), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob"}}
	rel := &storage.Edge{ID: storage.EdgeID("r1"), Type: "KNOWS", StartNode: n1.ID, EndNode: n2.ID, Properties: map[string]interface{}{"weight": 0.8}}

	val := exec.resolveReturnExprFromVarMap(ctx, "m.name", map[string]interface{}{"n": n1}, "m", "r", n2, rel)
	require.Equal(t, "Bob", val)

	val = exec.resolveReturnExprFromVarMap(ctx, "r.weight", map[string]interface{}{"n": n1}, "m", "r", n2, rel)
	require.Equal(t, 0.8, val)

	val = exec.resolveReturnExprFromVarMap(ctx, "n.name", map[string]interface{}{"n": n1}, "m", "r", nil, nil)
	require.Equal(t, "Alice", val)

	val = exec.resolveReturnExprFromVarMap(ctx, "m", map[string]interface{}{"n": n1}, "m", "r", n2, rel)
	require.Equal(t, n2, val)

	val = exec.resolveReturnExprFromVarMap(ctx, "r", map[string]interface{}{"n": n1}, "m", "r", n2, rel)
	require.Equal(t, rel, val)

	val = exec.resolveReturnExprFromVarMap(ctx, "n", map[string]interface{}{"n": n1}, "m", "r", nil, nil)
	require.Equal(t, n1, val)

	val = exec.resolveReturnExprFromVarMap(ctx, "42", map[string]interface{}{}, "m", "r", nil, nil)
	require.EqualValues(t, 42, val)
}
