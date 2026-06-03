package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecutorSubqueryHelpers_ExtraBranches(t *testing.T) {
	lhs, rhs, ok := splitTopLevelEqualityCallSubquery("a = b")
	require.True(t, ok)
	require.Equal(t, "a", lhs)
	require.Equal(t, "b", rhs)

	_, _, ok = splitTopLevelEqualityCallSubquery("a = ")
	require.False(t, ok)
	_, _, ok = splitTopLevelEqualityCallSubquery("(a = b)")
	require.False(t, ok)

	idx := indexTopLevelKeywordCallSubquery("`RETURN` RETURN x", "RETURN")
	require.Equal(t, 9, idx)
	idx = indexTopLevelKeywordCallSubquery("[1,2,RETURN] RETURN x", "RETURN")
	require.Equal(t, 13, idx)

	require.True(t, containsStandaloneIdentifier("x + y", "x"))
	require.False(t, containsStandaloneIdentifier("foo.x", "x"))
	require.False(t, containsStandaloneIdentifier("xx", "x"))
}

func TestExecuteChainedCallSubquery_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "chain_call")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	seed := &ExecuteResult{Columns: []string{"seed"}, Rows: [][]interface{}{{int64(1)}, {int64(2)}}}

	_, err := exec.executeChainedCallSubquery(ctx, seed, "CALL { }")
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty body")

	_, err = exec.executeChainedCallSubquery(ctx, seed, "CALL { RETURN 1 AS x } IN TRANSACTIONS OF 2 ROWS RETURN x")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not supported")

	res, err := exec.executeChainedCallSubquery(ctx, seed, "CALL { RETURN 7 AS x } RETURN x")
	require.NoError(t, err)
	require.Equal(t, []string{"x"}, res.Columns)
	require.Len(t, res.Rows, 2)
	require.EqualValues(t, 7, res.Rows[0][0])
	require.EqualValues(t, 7, res.Rows[1][0])

	res, err = exec.executeChainedCallSubquery(ctx, seed, "CALL { WITH seed RETURN seed AS x } RETURN x")
	require.NoError(t, err)
	require.Equal(t, []string{"x"}, res.Columns)
	require.Len(t, res.Rows, 2)
	require.EqualValues(t, int64(1), res.Rows[0][0])
	require.EqualValues(t, int64(2), res.Rows[1][0])

	_, err = exec.executeChainedCallSubquery(ctx, seed, "CALL { WITH seed }")
	require.Error(t, err)
	require.Contains(t, err.Error(), "WITH must be followed by a query clause")

	// USE branch should route through scoped resolution and still execute.
	res, err = exec.executeChainedCallSubquery(ctx, seed, "CALL { USE missing_db RETURN 1 AS x } RETURN x")
	require.NoError(t, err)
	require.Equal(t, []string{"x"}, res.Columns)
	require.Len(t, res.Rows, 2)
}

func TestSeedNodesFromOuterMatch_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "seed_outer")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{ID: "s1", Labels: []string{"Seed"}, Properties: map[string]interface{}{"name": "A"}})
	require.NoError(t, err)

	nodes, err := exec.seedNodesFromOuterMatch(ctx, "MATCH (seed:Seed {name:'A'})", "seed")
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	require.Equal(t, storage.NodeID("s1"), nodes[0].ID)

	_, err = exec.seedNodesFromOuterMatch(ctx, "MATCH (seed:Seed) RETURN 1", "missing")
	require.Error(t, err)
}
