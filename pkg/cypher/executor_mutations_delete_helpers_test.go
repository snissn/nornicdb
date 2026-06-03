package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDeleteHelpers_CollectCandidatesAndProjection(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "delete_helpers_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `
CREATE (:Person {id:'p1', team:'red'}),
       (:Person {id:'p2', team:'blue'}),
       (:Person {id:'p3', team:'red'})
`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE INDEX person_team_idx IF NOT EXISTS FOR (n:Person) ON (n.team)", nil)
	require.NoError(t, err)

	nodes, handled, err := exec.collectDeleteWithLimitCandidates(ctx, "RETURN 1", "n", 10, nil)
	require.NoError(t, err)
	require.False(t, handled)
	require.Nil(t, nodes)

	nodes, handled, err = exec.collectDeleteWithLimitCandidates(ctx, "MATCH (n:Person)", "x", 10, nil)
	require.NoError(t, err)
	require.False(t, handled)
	require.Nil(t, nodes)

	nodes, handled, err = exec.collectDeleteWithLimitCandidates(ctx, "MATCH (n:Person) WHERE n.team = 'red'", "n", 1, nil)
	require.NoError(t, err)
	require.True(t, handled)
	require.Len(t, nodes, 1)

	nodes, handled, err = exec.collectDeleteWithLimitCandidates(ctx, "MATCH (n:Person) WHERE n.team IN $teams", "n", 10, map[string]interface{}{"teams": []string{"blue"}})
	require.NoError(t, err)
	require.True(t, handled)
	require.Len(t, nodes, 1)
	require.Equal(t, "blue", nodes[0].Properties["team"])

	nodes, handled, err = exec.collectDeleteWithLimitCandidates(ctx, "MATCH (n:Person) WHERE n.team IN $teams", "n", 10, map[string]interface{}{"teams": int64(1)})
	require.NoError(t, err)
	require.False(t, handled)
	require.Nil(t, nodes)

	nodes, handled, err = exec.collectDeleteWithLimitCandidates(ctx, "MATCH (n:Person) WHERE n.team > 'a'", "n", 10, nil)
	require.NoError(t, err)
	require.False(t, handled)
	require.Nil(t, nodes)

	res := &ExecuteResult{Stats: &QueryStats{NodesDeleted: 2, RelationshipsDeleted: 3}}
	exec.applyDeleteReturnProjection(res, "MATCH (n) DELETE n RETURN count(*), count(n), n.name AS nn, n, 42 AS literal", "n")
	require.Equal(t, []string{"count(*)", "count(n)", "nn", "n", "literal"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 5, res.Rows[0][0])
	require.EqualValues(t, 2, res.Rows[0][1])
	require.Nil(t, res.Rows[0][2])
	require.Nil(t, res.Rows[0][3])
	require.EqualValues(t, 42, res.Rows[0][4])
}

func TestDeleteHelpers_StreamEligibilityAndExecution(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "delete_stream_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	require.False(t, exec.isDeleteStreamingEligible("", "n", true))
	require.False(t, exec.isDeleteStreamingEligible("MATCH (n) WITH n", "n", true))
	require.False(t, exec.isDeleteStreamingEligible("MATCH (n)", "n,m", true))
	require.False(t, exec.isDeleteStreamingEligible("MATCH (n)", "n-1", true))
	require.False(t, exec.isDeleteStreamingEligible("MATCH (n)", "n", false))
	require.True(t, exec.isDeleteStreamingEligible("MATCH (n:Tmp)", "n", true))

	_, err := exec.Execute(ctx, "CREATE (a:Tmp {id:'a'}), (b:Tmp {id:'b'})", nil)
	require.NoError(t, err)

	res, err := exec.executeDeleteStreaming(ctx, "MATCH (n:Tmp)", "n", false)
	require.NoError(t, err)
	require.EqualValues(t, 2, res.Stats.NodesDeleted)

	verify, err := exec.Execute(ctx, "MATCH (n:Tmp) RETURN count(n)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, verify.Rows[0][0])

	_, err = exec.executeDeleteStreaming(ctx, "MATCH (", "n", false)
	require.NoError(t, err)

	// Fallback branch: rows returned but delete variable unresolved => no deletes.
	_, err = exec.Execute(ctx, "CREATE (:Ghost {id:'g1'})", nil)
	require.NoError(t, err)
	res, err = exec.executeDeleteStreaming(ctx, "MATCH (n:Ghost)", "missingVar", false)
	require.NoError(t, err)
	require.EqualValues(t, 0, res.Stats.NodesDeleted)

	// Fallback branch with non-node values from a WITH projection.
	res, err = exec.executeDeleteStreaming(ctx, "WITH 'does-not-exist' AS n", "n", false)
	require.NoError(t, err)
	require.EqualValues(t, 0, res.Stats.NodesDeleted)
}

func TestDeleteHelpers_StreamExecution_NodeEdgeAndStatsBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "delete_stream_edges_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (a:Tmp {id:'a'}), (b:Tmp {id:'b'}), (a)-[:R]->(b)", nil)
	require.NoError(t, err)

	res, err := exec.executeDeleteStreaming(ctx, "MATCH (n:Tmp {id:'a'})", "n", true)
	require.NoError(t, err)
	require.EqualValues(t, 1, res.Stats.NodesDeleted)
	require.EqualValues(t, 1, res.Stats.RelationshipsDeleted)

	verifyNodes, err := exec.Execute(ctx, "MATCH (n:Tmp) RETURN count(n)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 1, verifyNodes.Rows[0][0])
	verifyEdges, err := exec.Execute(ctx, "MATCH ()-[r:R]->() RETURN count(r)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, verifyEdges.Rows[0][0])

	_, err = exec.Execute(ctx, "CREATE (c:Tmp {id:'c'}), (d:Tmp {id:'d'}), (c)-[:R]->(d)", nil)
	require.NoError(t, err)

	res, err = exec.executeDeleteStreaming(ctx, "MATCH ()-[r:R]->()", "r", false)
	require.NoError(t, err)
	require.EqualValues(t, 1, res.Stats.RelationshipsDeleted)
	require.EqualValues(t, 0, res.Stats.NodesDeleted)

	verifyEdges, err = exec.Execute(ctx, "MATCH ()-[r:R]->() RETURN count(r)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, verifyEdges.Rows[0][0])
	verifyNodes, err = exec.Execute(ctx, "MATCH (n:Tmp) RETURN count(n)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 3, verifyNodes.Rows[0][0])
}

func TestDeleteHelpers_StreamExecution_ExpressionDeleteVarsBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "delete_stream_expr_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (a:Tmp {id:'a'}), (b:Tmp {id:'b'}), (c:Tmp {id:'c'})", nil)
	require.NoError(t, err)

	// String branch in executeDeleteStreaming switch via RETURN id(n).
	res, err := exec.executeDeleteStreaming(ctx, "MATCH (n:Tmp)", "id(n)", false)
	require.NoError(t, err)
	require.EqualValues(t, 3, res.Stats.NodesDeleted)

	verify, err := exec.Execute(ctx, "MATCH (n:Tmp) RETURN count(n)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, verify.Rows[0][0])

	_, err = exec.Execute(ctx, "CREATE (x:Tmp {id:'x'}), (y:Tmp {id:'y'}), (x)-[:R]->(y)", nil)
	require.NoError(t, err)

	// Map branch with _edgeId key via map projection expression.
	res, err = exec.executeDeleteStreaming(ctx, "MATCH ()-[r:R]->()", "{_edgeId: id(r)}", false)
	require.NoError(t, err)
	require.EqualValues(t, 1, res.Stats.RelationshipsDeleted)

	verify, err = exec.Execute(ctx, "MATCH ()-[r:R]->() RETURN count(r)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, verify.Rows[0][0])

	// Map branch with _nodeId key via map projection expression.
	res, err = exec.executeDeleteStreaming(ctx, "MATCH (n:Tmp)", "{_nodeId: id(n)}", false)
	require.NoError(t, err)
	require.EqualValues(t, 2, res.Stats.NodesDeleted)

	verify, err = exec.Execute(ctx, "MATCH (n:Tmp) RETURN count(n)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 0, verify.Rows[0][0])
}

func TestWherePartNodePattern(t *testing.T) {
	np := nodePatternInfo{labels: []string{"A"}}
	out := wherePartNodePattern(np, "n")
	require.Equal(t, "n", out.variable)
	require.Equal(t, []string{"A"}, out.labels)

	np2 := nodePatternInfo{variable: "x", labels: []string{"B"}}
	out2 := wherePartNodePattern(np2, "n")
	require.Equal(t, "x", out2.variable)
}
