package cypher

import (
	"context"
	"errors"
	"testing"

	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecuteMergeWithChain_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "merge_chain_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.executeMergeWithChain(ctx, "")
	require.Error(t, err)
	require.True(t, errors.Is(err, nerrors.ErrInvalidMergeChainQuery))

	res, err := exec.executeMergeWithChain(ctx, "MERGE (a:Node {id:'a1'}) WITH a MATCH (b:Node {id:'missing'}) MERGE (a)-[:REL]->(b) RETURN a.id AS aid")
	require.NoError(t, err)
	require.Equal(t, []string{"aid"}, res.Columns)
	require.Empty(t, res.Rows)

	res, err = exec.executeMergeWithChain(ctx, "MERGE (a:Node {id:'a2'}) WITH a OPTIONAL MATCH (b:Node {id:'missing'}) RETURN a.id AS aid, b.id AS bid")
	require.NoError(t, err)
	require.Equal(t, []string{"aid", "bid"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "a2", res.Rows[0][0])
	require.Nil(t, res.Rows[0][1])

	res, err = exec.executeMergeWithChain(ctx, "MERGE (a:Node {id:'a3'}) WITH a MERGE (b:Node {id:'b3'}) WITH a, b MERGE (a)-[:REL]->(b) RETURN a.id AS aid, b.id AS bid")
	require.NoError(t, err)
	require.Equal(t, []string{"aid", "bid"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "a3", res.Rows[0][0])
	require.Equal(t, "b3", res.Rows[0][1])

	verify, err := exec.Execute(ctx, "MATCH (a:Node {id:'a3'})-[r:REL]->(b:Node {id:'b3'}) RETURN count(r)", nil)
	require.NoError(t, err)
	require.Len(t, verify.Rows, 1)
	require.EqualValues(t, 1, verify.Rows[0][0])

	// FOREACH clause inside chain segment
	res, err = exec.executeMergeWithChain(ctx, "MERGE (a:Node {id:'a4'}) WITH a FOREACH (i IN [1,2] | CREATE (n:Tmp {k:i})) RETURN a.id AS aid")
	require.NoError(t, err)
	require.Equal(t, []string{"aid"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "a4", res.Rows[0][0])

	cnt, err := exec.Execute(ctx, "MATCH (n:Tmp) RETURN count(n)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, cnt.Rows[0][0])
}

func TestCollapseConsecutiveDuplicateWithClauses(t *testing.T) {
	in := "MERGE (n:Node {id:'1'})\nWITH n\nWITH n\nRETURN n"
	out := collapseConsecutiveDuplicateWithClauses(in)
	require.Equal(t, "MERGE (n:Node {id:'1'})\nWITH n\nRETURN n", out)
}

func TestProjectWithContext_ScalarFallback(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_proj_cov"))
	ctx := context.Background()
	n := &storage.Node{ID: "n1", Properties: map[string]interface{}{"name": "A"}}
	nodeCtx := map[string]*storage.Node{"n": n}
	relCtx := map[string]*storage.Edge{}
	scalarCtx := map[string]interface{}{"score": int64(7)}

	newNodes, _, newScalars := exec.projectWithContext(ctx, "n AS nodeAlias, score AS s", nodeCtx, relCtx, scalarCtx)
	require.Contains(t, newNodes, "nodeAlias")
	require.Contains(t, newScalars, "s")
	require.EqualValues(t, int64(7), newScalars["s"])
}

func TestApplyWithProjection_Branches_Additional(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_with_proj_cov"))
	ctx := context.Background()
	n := &storage.Node{ID: "n1", Properties: map[string]interface{}{"name": "A"}}
	nodeCtx := map[string]*storage.Node{"n": n}
	relCtx := map[string]*storage.Edge{}
	scalarCtx := map[string]interface{}{"score": int64(7)}

	remaining, outNodes, _, outScalars := exec.applyWithProjection(ctx, "* MATCH (n)", nodeCtx, relCtx, scalarCtx)
	require.Equal(t, "MATCH (n)", remaining)
	require.Equal(t, nodeCtx, outNodes)
	require.Equal(t, scalarCtx, outScalars)

	remaining, outNodes, _, outScalars = exec.applyWithProjection(ctx, "n AS m, score AS s MERGE (m)-[:R]->(m)", nodeCtx, relCtx, scalarCtx)
	require.Equal(t, "MERGE (m)-[:R]->(m)", remaining)
	require.Contains(t, outNodes, "m")
	require.Contains(t, outScalars, "s")
	require.EqualValues(t, int64(7), outScalars["s"])
}

func TestExecuteMergeWithChain_ErrorBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "merge_chain_err_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.executeMergeWithChain(ctx, "MERGE (a:Node {id:'e1'}) WITH a MERGE (a)-[:REL]->(missing) RETURN a.id AS aid")
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing")
}
