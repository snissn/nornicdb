package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestMatchVectorCosineFastPath_UsesVectorIndex(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX item_emb_idx FOR (n:Item) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	for i, q := range []string{
		"CREATE (:Item {uuid:'a', emb:[1.0,0.0,0.0]})",
		"CREATE (:Item {uuid:'b', emb:[0.0,1.0,0.0]})",
		"CREATE (:Item {uuid:'c', emb:[0.0,0.0,1.0]})",
	} {
		_, err = exec.Execute(ctx, q, nil)
		require.NoErrorf(t, err, "seed query %d failed", i)
	}

	query := "MATCH (n:Item) RETURN n.uuid AS uuid, vector.similarity.cosine(n.emb, $q) AS score ORDER BY score DESC LIMIT $k"
	res, err := exec.Execute(ctx, query, map[string]interface{}{"q": []float64{1.0, 0.0, 0.0}, "k": 2})
	require.NoError(t, err)
	require.Equal(t, []string{"uuid", "score"}, res.Columns)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "a", res.Rows[0][0])

	trace := exec.LastHotPathTrace()
	require.True(t, trace.CosineVectorIndexFastPath)
}

func TestMatchVectorCosineFastPath_RequiresMatchingIndex(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Item {uuid:'a', emb:[1.0,0.0,0.0]})", nil)
	require.NoError(t, err)

	query := "MATCH (n:Item) RETURN n.uuid AS uuid, vector.similarity.cosine(n.emb, $q) AS score ORDER BY score DESC LIMIT 1"
	res, err := exec.Execute(ctx, query, map[string]interface{}{"q": []float64{1.0, 0.0, 0.0}})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)

	trace := exec.LastHotPathTrace()
	require.False(t, trace.CosineVectorIndexFastPath, "must fall back when no matching vector index exists")
	require.Greater(t, counting.labelCalls, 0, "fallback path should use normal MATCH scanning")
}

func TestMatchVectorCosineFastPath_WriteThenSearchLoop_NoScanRegression(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX item_emb_idx FOR (n:Item) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CREATE (:Item {uuid:'seed', emb:[1.0,0.0,0.0]})", nil)
	require.NoError(t, err)

	query := "MATCH (n:Item) RETURN n.uuid AS uuid, vector.similarity.cosine(n.emb, $q) AS score ORDER BY score DESC LIMIT $k"
	params := map[string]interface{}{"q": []float64{1.0, 0.0, 0.0}, "k": 3}

	// Warm the vector query pipeline once, then assert write+search loops stay off scan paths.
	_, err = exec.Execute(ctx, query+" /* warmup */", params)
	require.NoError(t, err)
	counting.allNodesCalls = 0
	counting.labelCalls = 0
	counting.streamNodesCalls = 0

	for i := 0; i < 8; i++ {
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Item {uuid:'new-%d', emb:[1.0,0.0,0.0]})", i), nil)
		require.NoError(t, err)

		res, err := exec.Execute(ctx, fmt.Sprintf("%s /* loop_%d */", query, i), params)
		require.NoError(t, err)
		require.NotEmpty(t, res.Rows)

		trace := exec.LastHotPathTrace()
		require.True(t, trace.CosineVectorIndexFastPath, "loop %d should stay on vector-index fast path", i)
	}

	require.Equal(t, 0, counting.allNodesCalls, "write-then-search loop should not regress to full node scans")
	require.Equal(t, 0, counting.labelCalls, "write-then-search loop should not regress to label scans")
	require.Equal(t, 0, counting.streamNodesCalls, "write-then-search loop should not use stream-scan fallback")
}

func TestTryFastPathMatchVectorCosine_HandlesAscendingOrder(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX item_emb_idx FOR (n:Item) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Item {uuid:'best', emb:[1.0,0.0,0.0]})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Item {uuid:'worst', emb:[-1.0,0.0,0.0]})", nil)
	require.NoError(t, err)

	result, handled := exec.tryFastPathMatchVectorCosine(
		ctx,
		"MATCH (n:Item) RETURN vector.similarity.cosine(n.emb, [1.0,0.0,0.0]) AS score ORDER BY score ASC LIMIT 5",
		"MATCH (N:ITEM) RETURN VECTOR.SIMILARITY.COSINE(N.EMB, [1.0,0.0,0.0]) AS SCORE ORDER BY SCORE ASC LIMIT 5",
	)
	require.True(t, handled)
	require.NotNil(t, result)
	require.Len(t, result.Rows, 2)
	require.EqualValues(t, -1.0, result.Rows[0][0])
	require.EqualValues(t, 1.0, result.Rows[1][0])
}

func TestMatchVectorCosineFastPath_AscendingOrderWithParamVector(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX item_emb_idx FOR (n:Item) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Item {uuid:'a', emb:[1.0,0.0,0.0]})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Item {uuid:'b', emb:[0.0,1.0,0.0]})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Item {uuid:'c', emb:[-1.0,0.0,0.0]})", nil)
	require.NoError(t, err)

	query := "MATCH (n:Item) RETURN n.uuid AS uuid, vector.similarity.cosine(n.emb, $q) AS score ORDER BY score ASC LIMIT 2"
	res, err := exec.Execute(ctx, query, map[string]interface{}{"q": []float64{1.0, 0.0, 0.0}})
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "c", res.Rows[0][0])
	require.EqualValues(t, -1.0, res.Rows[0][1])
	require.Equal(t, "b", res.Rows[1][0])
	require.EqualValues(t, 0.0, res.Rows[1][1])
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
}
