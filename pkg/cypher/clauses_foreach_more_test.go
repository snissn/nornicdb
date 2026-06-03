package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecuteForeachWithContext_ErrorBranches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "foreach_err_cov"))
	ctx := context.Background()

	_, err := exec.executeForeachWithContext(ctx, "RETURN 1", nil, nil)
	require.ErrorContains(t, err, "FOREACH clause not found")

	_, err = exec.executeForeachWithContext(ctx, "FOREACH x IN [1] | SET y = 1", nil, nil)
	require.ErrorContains(t, err, "requires parentheses")

	_, err = exec.executeForeachWithContext(ctx, "FOREACH (x IN [1] | CREATE (:X)", nil, nil)
	require.ErrorContains(t, err, "balanced parentheses")

	_, err = exec.executeForeachWithContext(ctx, "FOREACH (x [1] | CREATE (:X))", nil, nil)
	require.ErrorContains(t, err, "requires IN clause")

	_, err = exec.executeForeachWithContext(ctx, "FOREACH (x IN [1] CREATE (:X))", nil, nil)
	require.ErrorContains(t, err, "requires | separator")
}

func TestExecuteForeachWithContext_DefaultAndTrailingBranches(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "foreach_trailing_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	// Default execution path via CREATE and trailing RETURN clause.
	res, err := exec.executeForeachWithContext(ctx,
		"FOREACH (x IN [1,2] | CREATE (:Tmp {k:x})) RETURN 42 AS answer",
		map[string]*storage.Node{},
		map[string]*storage.Edge{},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"answer"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 42, res.Rows[0][0])
	require.EqualValues(t, 2, res.Stats.NodesCreated)

	verify, err := exec.Execute(ctx, "MATCH (t:Tmp) RETURN count(t)", nil)
	require.NoError(t, err)
	require.EqualValues(t, 2, verify.Rows[0][0])

	// list=nil path should perform zero iterations and still return a result.
	res2, err := exec.executeForeachWithContext(ctx,
		"FOREACH (x IN null | CREATE (:Tmp2 {k:x}))",
		map[string]*storage.Node{},
		map[string]*storage.Edge{},
	)
	require.NoError(t, err)
	require.NotNil(t, res2)
	require.EqualValues(t, 0, res2.Stats.NodesCreated)

	// default branch with scalar list (non-array) should execute once.
	res3, err := exec.executeForeachWithContext(ctx,
		"FOREACH (x IN 7 | CREATE (:Tmp3 {k:x}))",
		map[string]*storage.Node{},
		map[string]*storage.Edge{},
	)
	require.NoError(t, err)
	require.EqualValues(t, 1, res3.Stats.NodesCreated)

	verify3, err := exec.Execute(ctx, "MATCH (t:Tmp3) RETURN t.k", nil)
	require.NoError(t, err)
	require.Len(t, verify3.Rows, 1)
	require.EqualValues(t, 7, verify3.Rows[0][0])
}
