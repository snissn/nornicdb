package cypher

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
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

func TestMatchVectorCosineFastPath_DoesNotCreateSearchServiceWhenUnwired(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX entity_name_idx FOR (n:Entity) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	for i := 0; i < 12; i++ {
		vec := []float64{0, 0, 0, 0}
		vec[i%4] = 1
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Entity {uuid:'e-%02d', group_id:'g', name_embedding:%s})", i, formatInlineFloat64Vector(vec)), nil)
		require.NoError(t, err)
	}

	query := `
MATCH (n:Entity)
WHERE n.group_id = $group_id
WITH n, vector.similarity.cosine(n.name_embedding, $search_vector) AS score
WHERE score > $min_score
RETURN n.uuid AS uuid, score
ORDER BY score DESC
LIMIT $limit
`
	params := map[string]interface{}{
		"group_id":      "g",
		"search_vector": []float64{1, 0, 0, 0},
		"min_score":     -1.0,
		"limit":         5,
	}

	for i := 0; i < 5; i++ {
		res, err := exec.Execute(ctx, fmt.Sprintf("%s /* query_%d */", query, i), params)
		require.NoError(t, err)
		require.Len(t, res.Rows, 5)
		require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
		require.Nil(t, exec.searchService, "query-time vector cosine fast path must not allocate a throwaway search service")
	}
}

func TestMatchVectorCosineFastPath_DoesNotReplaceMismatchedSearchService(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	attached := search.NewServiceWithDimensions(ns, 3)
	exec.SetSearchService(attached)

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX entity_name_idx FOR (n:Entity) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Entity {uuid:'e-1', name_embedding:[1.0,0.0,0.0,0.0]})", nil)
	require.NoError(t, err)

	query := "MATCH (n:Entity) WITH n, vector.similarity.cosine(n.name_embedding, $q) AS score RETURN n.uuid AS uuid, score ORDER BY score DESC LIMIT 1"
	res, err := exec.Execute(ctx, query, map[string]interface{}{"q": []float64{1, 0, 0, 0}})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "e-1", res.Rows[0][0])
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
	require.Same(t, attached, exec.searchService, "dimension mismatch should fall back to exact scoring, not replace the DB-owned search service")
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

	searchSvc := search.NewServiceWithDimensions(ns, 3)
	exec.SetSearchService(searchSvc)

	_, err = exec.Execute(ctx, "CREATE (:Item {uuid:'seed', emb:[1.0,0.0,0.0]})", nil)
	require.NoError(t, err)
	require.Equal(t, 1, searchSvc.CountPropertyVectorEntries("emb"))
	require.NoError(t, searchSvc.BuildIndexes(ctx))
	require.True(t, searchSvc.IsReady())

	query := "MATCH (n:Item) RETURN n.uuid AS uuid, vector.similarity.cosine(n.emb, $q) AS score ORDER BY score DESC LIMIT $k"
	params := map[string]interface{}{"q": []float64{1.0, 0.0, 0.0}, "k": 3}

	// Warm the owned vector query pipeline once, then assert write+search loops stay off scan paths.
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

func TestMatchVectorCosineFastPath_LazyWiredServiceWarmsBeforeIndexedQuery(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX entity_name_idx FOR (n:Entity) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	searchSvc := search.NewServiceWithDimensions(ns, 4)
	var warmCalls int32
	var warmErr atomic.Value
	searchSvc.SetLazyWarming(true, search.WarmFunc(func() {
		atomic.AddInt32(&warmCalls, 1)
		if err := searchSvc.BuildIndexes(context.Background()); err != nil {
			warmErr.Store(err)
		}
	}))
	exec.SetSearchService(searchSvc)

	_, err = exec.Execute(ctx, "CREATE (:Entity {uuid:'best', group_id:'g', name_embedding:[1.0,0.0,0.0,0.0]})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:Entity {uuid:'other', group_id:'g', name_embedding:[0.0,1.0,0.0,0.0]})", nil)
	require.NoError(t, err)
	require.False(t, searchSvc.IsReady(), "live-indexed vector properties must not be reported as full warmup readiness")
	require.True(t, searchSvc.CanServeVectorQueries(), "live-indexed vector properties make indexed queries service-backed before full warmup")

	counting.allNodesCalls = 0
	counting.labelCalls = 0
	counting.streamNodesCalls = 0

	query := `
MATCH (n:Entity)
WHERE n.group_id = $group_id
WITH n, vector.similarity.cosine(n.name_embedding, $search_vector) AS score
RETURN n.uuid AS uuid, score
ORDER BY score DESC
LIMIT $limit
`
	res, err := exec.Execute(ctx, query, map[string]interface{}{
		"group_id":      "g",
		"search_vector": []float64{1, 0, 0, 0},
		"limit":         1,
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "best", res.Rows[0][0])
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
	if errValue := warmErr.Load(); errValue != nil {
		require.NoError(t, errValue.(error))
	}
	require.True(t, searchSvc.IsReady())
	require.Equal(t, int32(1), atomic.LoadInt32(&warmCalls))
	require.Equal(t, 0, counting.allNodesCalls)
	require.Equal(t, 0, counting.labelCalls)
	require.Equal(t, 0, counting.streamNodesCalls)
}

func TestMatchVectorCosineFastPath_ExternalEmbeddingsUseLiveIndexBeforeReady(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX entity_name_idx FOR (n:Entity) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	searchSvc := search.NewServiceWithDimensions(ns, 4)
	exec.SetSearchService(searchSvc)

	for i := 0; i < 16; i++ {
		vec := []float64{0, 0, 0, 0}
		vec[i%4] = 1
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Entity {uuid:'e-%02d', group_id:'g', name_embedding:%s})", i, formatInlineFloat64Vector(vec)), nil)
		require.NoError(t, err)
	}
	require.Equal(t, 16, searchSvc.CountPropertyVectorEntries("name_embedding"))
	require.False(t, searchSvc.IsReady(), "live-indexed vector properties must not be reported as full warmup readiness")
	require.True(t, searchSvc.CanServeVectorQueries(), "live-indexed vector properties should not need a full BuildIndexes warmup before querying")

	counting.allNodesCalls = 0
	counting.labelCalls = 0
	counting.streamNodesCalls = 0

	query := `
MATCH (n:Entity)
WHERE n.group_id = $group_id
WITH n, vector.similarity.cosine(n.name_embedding, $search_vector) AS score
WHERE score > $min_score
RETURN n.uuid AS uuid, score
ORDER BY score DESC
LIMIT $limit
`
	res, err := exec.Execute(ctx, query, map[string]interface{}{
		"group_id":      "g",
		"search_vector": []float64{1, 0, 0, 0},
		"min_score":     -1.0,
		"limit":         5,
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 5)
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
	require.Equal(t, 0, counting.allNodesCalls, "live-index query must not full-scan storage before full warmup")
	require.Equal(t, 0, counting.labelCalls, "live-index query must not label-scan storage before full warmup")
	require.Equal(t, 0, counting.streamNodesCalls, "live-index query must not stream-scan storage before full warmup")
}

func TestMatchVectorCosineFastPath_SetNodeVectorPropertyAfterWarmupIsSearchable(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX sn_emb_idx FOR (n:SN) ON (n.emb) OPTIONS {indexConfig: {`vector.dimensions`: 4, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (:SN {uuid:'seed0'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (n:SN {uuid:'seed0'}) WITH n CALL db.create.setNodeVectorProperty(n,'emb',[0.0,0.0,1.0,0.0]) RETURN n", nil)
	require.NoError(t, err)

	rankQuery := `
MATCH (n:SN)
WHERE n.emb IS NOT NULL
WITH n, vector.similarity.cosine(n.emb, $q) AS s
RETURN n.uuid AS u, s
ORDER BY s DESC
LIMIT 1`
	res, err := exec.Execute(ctx, rankQuery, map[string]interface{}{"q": []float64{0.0, 0.0, 1.0, 0.0}})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "seed0", res.Rows[0][0])
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)

	_, err = exec.Execute(ctx, "CREATE (:SN {uuid:'sentinel'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (n:SN {uuid:'sentinel'}) WITH n CALL db.create.setNodeVectorProperty(n,'emb',[1.0,0.0,0.0,0.0]) RETURN n", nil)
	require.NoError(t, err)

	direct, err := exec.Execute(ctx, "MATCH (n:SN {uuid:'sentinel'}) RETURN vector.similarity.cosine(n.emb,[1.0,0.0,0.0,0.0]) AS s", nil)
	require.NoError(t, err)
	require.Len(t, direct.Rows, 1)
	require.EqualValues(t, 1.0, direct.Rows[0][0])

	res, err = exec.Execute(ctx, rankQuery, map[string]interface{}{"q": []float64{1.0, 0.0, 0.0, 0.0}})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "sentinel", res.Rows[0][0])
	require.EqualValues(t, 1.0, res.Rows[0][1])
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
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

func TestMatchVectorCosineFastPath_WithProjectionShape_UsesVectorIndex(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX chunk_emb_idx FOR (c:Chunk) ON (c.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	for _, q := range []string{
		"CREATE (:Chunk {uuid:'c1', emb:[1.0,0.0,0.0], group_id:'g'})",
		"CREATE (:Chunk {uuid:'c2', emb:[0.9,0.1,0.0], group_id:'g'})",
		"CREATE (:Chunk {uuid:'c3', emb:[0.0,1.0,0.0], group_id:'g'})",
	} {
		_, err = exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	query := `MATCH (c:Chunk)
WITH c, vector.similarity.cosine(c.emb, $q) AS score
WHERE score > $min
RETURN c.uuid AS uuid, score
ORDER BY score DESC
LIMIT $k`
	_, err = exec.Execute(ctx, query+" /* warmup */", map[string]interface{}{
		"q":   []float64{1.0, 0.0, 0.0},
		"min": 0.2,
		"k":   5,
	})
	require.NoError(t, err)
	counting.allNodesCalls = 0
	counting.labelCalls = 0
	counting.streamNodesCalls = 0
	res, err := exec.Execute(ctx, query, map[string]interface{}{
		"q":   []float64{1.0, 0.0, 0.0},
		"min": 0.2,
		"k":   5,
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "c1", res.Rows[0][0])
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
	require.Equal(t, 0, counting.allNodesCalls)
}

func TestMatchVectorCosineFastPath_WithProjection_WriteThenSearchLoop_NoScanRegression(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	counting := &countingStreamingEngine{Engine: ns}
	exec := NewStorageExecutor(counting)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX chunk_emb_idx FOR (c:Chunk) ON (c.emb) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	searchSvc := search.NewServiceWithDimensions(ns, 3)
	exec.SetSearchService(searchSvc)

	_, err = exec.Execute(ctx, "CREATE (:Chunk {uuid:'seed', emb:[1.0,0.0,0.0], group_id:'g'})", nil)
	require.NoError(t, err)
	require.Equal(t, 1, searchSvc.CountPropertyVectorEntries("emb"))
	require.NoError(t, searchSvc.BuildIndexes(ctx))
	require.True(t, searchSvc.IsReady())

	query := `MATCH (c:Chunk)
WITH c, vector.similarity.cosine(c.emb, $q) AS score
WHERE score > $min
RETURN c.uuid AS uuid, score
ORDER BY score DESC
LIMIT $k`
	params := map[string]interface{}{
		"q":   []float64{1.0, 0.0, 0.0},
		"min": -1.0,
		"k":   3,
	}

	_, err = exec.Execute(ctx, query+" /* warmup */", params)
	require.NoError(t, err)
	counting.allNodesCalls = 0
	counting.labelCalls = 0
	counting.streamNodesCalls = 0

	for i := 0; i < 6; i++ {
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Chunk {uuid:'n-%d', emb:[1.0,0.0,0.0], group_id:'g'})", i), nil)
		require.NoError(t, err)

		res, err := exec.Execute(ctx, fmt.Sprintf("%s /* projection_loop_%d */", query, i), params)
		require.NoError(t, err)
		require.NotEmpty(t, res.Rows)
		require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
	}

	require.Equal(t, 0, counting.allNodesCalls)
}

func TestFindCosineVectorIndexName_DisambiguatesByEntityType(t *testing.T) {
	schema := storage.NewSchemaManager()
	require.NoError(t, schema.AddVectorIndexForEntity("a_rel_idx", "Thing", "emb", 3, "cosine", storage.ConstraintEntityRelationship))
	require.NoError(t, schema.AddVectorIndexForEntity("z_node_idx", "Thing", "emb", 3, "cosine", storage.ConstraintEntityNode))

	nodeIndex, ok := findCosineVectorIndexName(schema, "Thing", "emb", storage.ConstraintEntityNode)
	require.True(t, ok)
	require.Equal(t, "z_node_idx", nodeIndex)

	relIndex, ok := findCosineVectorIndexName(schema, "Thing", "emb", storage.ConstraintEntityRelationship)
	require.True(t, ok)
	require.Equal(t, "a_rel_idx", relIndex)
}
