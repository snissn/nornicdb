package cypher

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	graphitiGroupID = "graphiti-bench-g1"
)

const graphitiBulkNodeSaveQuery = `
UNWIND $nodes AS node
MERGE (n:Entity {uuid: node.uuid})
SET n:$(node.labels)
SET n = node
WITH n, node CALL db.create.setNodeVectorProperty(n, "name_embedding", node.name_embedding)
RETURN n.uuid AS uuid
`

const graphitiBulkEdgeSaveQuery = `
UNWIND $entity_edges AS edge
MATCH (source:Entity {uuid: edge.source_node_uuid})
MATCH (target:Entity {uuid: edge.target_node_uuid})
MERGE (source)-[e:RELATES_TO {uuid: edge.uuid}]->(target)
SET e = edge
WITH e, edge CALL db.create.setRelationshipVectorProperty(e, "fact_embedding", edge.fact_embedding)
RETURN edge.uuid AS uuid
`

const graphitiBulkChunkSaveQuery = `
MATCH (x:Entity {uuid:$anchor})
UNWIND $chunks AS c
CREATE (ch:Chunk {
  uuid:c.uuid,
  group_id:c.group_id,
  emb:c.emb,
  text:c.text,
  tokens:c.tokens
})
RETURN ch.uuid AS uuid
`

const graphitiNodeSimilarityQuery = `
MATCH (n:Entity)
WHERE n.group_id = $group_id
WITH n, vector.similarity.cosine(n.name_embedding, $search_vector) AS score
WHERE score > $min_score
RETURN n.uuid AS uuid, score
ORDER BY score DESC
LIMIT $limit
`

const graphitiEdgeSimilarityQuery = `
MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity)
WITH DISTINCT e, n, m, vector.similarity.cosine(e.fact_embedding, $search_vector) AS score
WHERE score > $min_score
RETURN e.uuid AS uuid, properties(e) AS attributes, score
ORDER BY score DESC
LIMIT $limit
`

const graphitiChunkSimilarityQuery = `
MATCH (c:Chunk)
WHERE c.group_id = $group_id
WITH c, vector.similarity.cosine(c.emb, $search_vector) AS score
RETURN c.uuid AS uuid, score
ORDER BY score DESC
LIMIT $limit
`

type graphitiScenarioPayload struct {
	nodes  []map[string]interface{}
	edges  []map[string]interface{}
	chunks []map[string]interface{}
}

func TestGraphitiScenarioE2E_LargePayloads(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	dim := 32
	entityCount := 320
	edgeCount := 960
	chunkCount := 960
	if testing.Short() {
		entityCount = 80
		edgeCount = 160
		chunkCount = 160
	}

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX ent_idx FOR (n:Entity) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 32, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE VECTOR INDEX rel_idx FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 32, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE VECTOR INDEX chunk_idx FOR (c:Chunk) ON (c.emb) OPTIONS {indexConfig: {`vector.dimensions`: 32, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE FULLTEXT INDEX ent_ft IF NOT EXISTS FOR (n:Entity) ON EACH [n.name, n.summary]", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE FULLTEXT INDEX rel_ft IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON EACH [e.fact, e.name]", nil)
	require.NoError(t, err)

	payload := buildGraphitiScenarioPayload(entityCount, edgeCount, chunkCount, dim)

	res, err := exec.Execute(ctx, graphitiBulkNodeSaveQuery, map[string]interface{}{"nodes": payload.nodes})
	require.NoError(t, err)
	require.Len(t, res.Rows, len(payload.nodes))

	res, err = exec.Execute(ctx, graphitiBulkEdgeSaveQuery, map[string]interface{}{"entity_edges": payload.edges})
	require.NoError(t, err)
	require.Len(t, res.Rows, len(payload.edges))

	res, err = exec.Execute(ctx, graphitiBulkChunkSaveQuery, map[string]interface{}{
		"anchor": "entity-000000",
		"chunks": payload.chunks,
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, len(payload.chunks))

	countEntities := mustCountRows(t, exec, ctx, "MATCH (n:Entity {group_id: $g}) RETURN count(n)", map[string]interface{}{"g": graphitiGroupID})
	require.Equal(t, int64(entityCount), countEntities)
	countEdges := mustCountRows(t, exec, ctx, "MATCH (:Entity)-[e:RELATES_TO]->(:Entity) WHERE e.group_id = $g RETURN count(e)", map[string]interface{}{"g": graphitiGroupID})
	require.Equal(t, int64(edgeCount), countEdges)
	countChunks := mustCountRows(t, exec, ctx, "MATCH (c:Chunk {group_id: $g}) RETURN count(c)", map[string]interface{}{"g": graphitiGroupID})
	require.Equal(t, int64(chunkCount), countChunks)

	res, err = exec.Execute(ctx, graphitiNodeSimilarityQuery, map[string]interface{}{
		"group_id":      graphitiGroupID,
		"search_vector": unitVectorF64(0, dim),
		"min_score":     -1.0,
		"limit":         25,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath, "node similarity should route through vector-index fast path")
	assertNonIncreasingScores(t, res.Rows, 1)

	res, err = exec.Execute(ctx, graphitiEdgeSimilarityQuery, map[string]interface{}{
		"search_vector": unitVectorF64(0, dim),
		"min_score":     -1.0,
		"limit":         25,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath, "edge similarity should route through vector-index fast path")
	assertNonIncreasingScores(t, res.Rows, 2)

	res, err = exec.Execute(ctx, graphitiChunkSimilarityQuery, map[string]interface{}{
		"group_id":      graphitiGroupID,
		"search_vector": unitVectorF64(0, dim),
		"limit":         25,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.Rows)
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath, "chunk similarity should route through vector-index fast path")
	assertNonIncreasingScores(t, res.Rows, 1)

	// Neo4j compatibility: 3-arg fulltext query options map.
	res, err = exec.Execute(ctx, `CALL db.index.fulltext.queryNodes("ent_ft", "entity", {limit: 10, skip: 2}) YIELD node, score RETURN node.uuid AS uuid, score`, nil)
	require.NoError(t, err)
	require.LessOrEqual(t, len(res.Rows), 10)

	res, err = exec.Execute(ctx, `CALL db.index.fulltext.queryRelationships("rel_ft", "relates", {limit: 10, skip: 1}) YIELD relationship, score RETURN relationship.uuid AS uuid, score`, nil)
	require.NoError(t, err)
	require.LessOrEqual(t, len(res.Rows), 10)
}

func TestGraphitiScenarioE2E_RelationshipFulltextMultiTokenFacts(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE FULLTEXT INDEX rel_ft IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON EACH [e.fact, e.name]", nil)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Entity {uuid:'n-%02d'})", i), nil)
		require.NoError(t, err)
	}

	facts := []string{
		"NSQ topic migration regression requires an idempotent backfill worker",
		"Backfill replay found an NSQ consumer regression during migration validation",
		"The sftp_migration service publishes NSQ topic events for backfill progress",
		"Regression tests cover NSQ migration retries and delayed backfill batches",
		"NSQ queue lag increased when the migration backfill job retried facts",
		"Graphiti facts document the NSQ topic migration and backfill ordering",
		"Backfill checkpoints prevented migration regression for the NSQ consumer",
		"NSQ migration incident notes explain regression triage and backfill repair",
	}
	for i, fact := range facts {
		_, err = exec.Execute(ctx, `
MATCH (a:Entity {uuid:$a})
MATCH (b:Entity {uuid:$b})
CREATE (a)-[:RELATES_TO {uuid:$uuid, name:$name, fact:$fact}]->(b)
`, map[string]interface{}{
			"a":    fmt.Sprintf("n-%02d", i),
			"b":    fmt.Sprintf("n-%02d", i+1),
			"uuid": fmt.Sprintf("nsq-%02d", i),
			"name": "NSQ migration fact",
			"fact": fact,
		})
		require.NoError(t, err)
	}
	for i, fact := range []string{
		"OAuth token rotation completed without data replay",
		"Invoice import observed a timezone normalization warning",
		"Feature flag cleanup removed obsolete email experiments",
	} {
		_, err = exec.Execute(ctx, `
MATCH (a:Entity {uuid:'n-00'})
MATCH (b:Entity {uuid:'n-09'})
CREATE (a)-[:RELATES_TO {uuid:$uuid, name:'unrelated', fact:$fact}]->(b)
`, map[string]interface{}{
			"uuid": fmt.Sprintf("other-%02d", i),
			"fact": fact,
		})
		require.NoError(t, err)
	}

	res, err := exec.Execute(ctx, `
CALL db.index.fulltext.queryRelationships('rel_ft', 'NSQ migration regression and backfill') YIELD relationship, score
RETURN relationship.uuid AS uuid, score
ORDER BY score DESC
LIMIT 15
`, nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(res.Rows), 5)
	for _, row := range res.Rows {
		require.Contains(t, row[0], "nsq-", "multi-token fact search should not return unrelated relationship %v", row[0])
	}

	short, err := exec.Execute(ctx, `
CALL db.index.fulltext.queryRelationships('rel_ft', 'sftp_migration NSQ topic') YIELD relationship, score
RETURN relationship.uuid AS uuid, score
ORDER BY score DESC
LIMIT 5
`, nil)
	require.NoError(t, err)
	require.NotEmpty(t, short.Rows)
}

func BenchmarkGraphitiRelationshipFulltextMultiTokenFacts(b *testing.B) {
	base := newTestMemoryEngine(b)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	exec.cache = nil
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE FULLTEXT INDEX rel_ft IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON EACH [e.fact, e.name]", nil)
	require.NoError(b, err)

	for i := 0; i < 32; i++ {
		_, err = exec.Execute(ctx, fmt.Sprintf("CREATE (:Entity {uuid:'bench-n-%02d'})", i), nil)
		require.NoError(b, err)
	}

	relevantFacts := []string{
		"NSQ topic migration regression requires an idempotent backfill worker",
		"Backfill replay found an NSQ consumer regression during migration validation",
		"The sftp_migration service publishes NSQ topic events for backfill progress",
		"Regression tests cover NSQ migration retries and delayed backfill batches",
		"NSQ queue lag increased when the migration backfill job retried facts",
		"Graphiti facts document the NSQ topic migration and backfill ordering",
		"Backfill checkpoints prevented migration regression for the NSQ consumer",
		"NSQ migration incident notes explain regression triage and backfill repair",
	}
	unrelatedFacts := []string{
		"OAuth token rotation completed without data replay",
		"Invoice import observed a timezone normalization warning",
		"Feature flag cleanup removed obsolete email experiments",
		"Calendar sync retries converged after a provider throttle",
	}
	for i := 0; i < 128; i++ {
		_, err = exec.Execute(ctx, `
MATCH (a:Entity {uuid:$a})
MATCH (b:Entity {uuid:$b})
CREATE (a)-[:RELATES_TO {uuid:$uuid, name:$name, fact:$fact}]->(b)
`, map[string]interface{}{
			"a":    fmt.Sprintf("bench-n-%02d", i%31),
			"b":    fmt.Sprintf("bench-n-%02d", (i%31)+1),
			"uuid": fmt.Sprintf("bench-nsq-%03d", i),
			"name": "NSQ migration fact",
			"fact": relevantFacts[i%len(relevantFacts)],
		})
		require.NoError(b, err)
	}
	for i := 0; i < 128; i++ {
		_, err = exec.Execute(ctx, `
MATCH (a:Entity {uuid:$a})
MATCH (b:Entity {uuid:$b})
CREATE (a)-[:RELATES_TO {uuid:$uuid, name:'unrelated', fact:$fact}]->(b)
`, map[string]interface{}{
			"a":    fmt.Sprintf("bench-n-%02d", i%31),
			"b":    fmt.Sprintf("bench-n-%02d", (i%31)+1),
			"uuid": fmt.Sprintf("bench-other-%03d", i),
			"fact": unrelatedFacts[i%len(unrelatedFacts)],
		})
		require.NoError(b, err)
	}

	query := `
CALL db.index.fulltext.queryRelationships('rel_ft', 'NSQ migration regression and backfill') YIELD relationship, score
RETURN relationship.uuid AS uuid, score
ORDER BY score DESC
LIMIT 15
`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := exec.Execute(ctx, query, nil)
		require.NoError(b, err)
		require.GreaterOrEqual(b, len(res.Rows), 15)
	}
}

func BenchmarkGraphitiScenarioHotspots(b *testing.B) {
	base := newTestMemoryEngine(b)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	dim := 32
	entityCount := 256
	edgeCount := 768
	chunkCount := 768

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX ent_idx FOR (n:Entity) ON (n.name_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 32, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(b, err)
	_, err = exec.Execute(ctx, "CREATE VECTOR INDEX rel_idx FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 32, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(b, err)
	_, err = exec.Execute(ctx, "CREATE VECTOR INDEX chunk_idx FOR (c:Chunk) ON (c.emb) OPTIONS {indexConfig: {`vector.dimensions`: 32, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(b, err)

	payload := buildGraphitiScenarioPayload(entityCount, edgeCount, chunkCount, dim)
	_, err = exec.Execute(ctx, graphitiBulkNodeSaveQuery, map[string]interface{}{"nodes": payload.nodes})
	require.NoError(b, err)
	_, err = exec.Execute(ctx, graphitiBulkEdgeSaveQuery, map[string]interface{}{"entity_edges": payload.edges})
	require.NoError(b, err)
	_, err = exec.Execute(ctx, graphitiBulkChunkSaveQuery, map[string]interface{}{
		"anchor": "entity-000000",
		"chunks": payload.chunks,
	})
	require.NoError(b, err)

	queryVec := unitVectorF64(0, dim)
	b.ReportAllocs()

	b.Run("node_similarity_with_projection_filter", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := exec.Execute(ctx, graphitiNodeSimilarityQuery, map[string]interface{}{
				"group_id":      graphitiGroupID,
				"search_vector": queryVec,
				"min_score":     -1.0,
				"limit":         50,
			})
			require.NoError(b, err)
		}
	})

	b.Run("edge_similarity_with_projection_filter", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := exec.Execute(ctx, graphitiEdgeSimilarityQuery, map[string]interface{}{
				"search_vector": queryVec,
				"min_score":     -1.0,
				"limit":         50,
			})
			require.NoError(b, err)
		}
	})

	b.Run("chunk_similarity_projection", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := exec.Execute(ctx, graphitiChunkSimilarityQuery, map[string]interface{}{
				"group_id":      graphitiGroupID,
				"search_vector": queryVec,
				"limit":         50,
			})
			require.NoError(b, err)
		}
	})
}

func BenchmarkGraphitiRelationshipVectorFastPath(b *testing.B) {
	const (
		dim         = 32
		entityCount = 512
		edgeCount   = 4096
		chunkCount  = 0
		limit       = 50
	)

	setup := func(b *testing.B, attachSearch bool) (*StorageExecutor, context.Context) {
		b.Helper()

		base, err := storage.NewBadgerEngineInMemory()
		require.NoError(b, err)
		b.Cleanup(func() { _ = base.Close() })

		ns := storage.NewNamespacedEngine(base, "test")
		exec := NewStorageExecutor(ns)
		exec.cache = nil
		ctx := context.Background()

		_, err = exec.Execute(ctx, "CREATE VECTOR INDEX rel_idx FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 32, `vector.similarity_function`: 'cosine'}}", nil)
		require.NoError(b, err)

		payload := buildGraphitiScenarioPayload(entityCount, edgeCount, chunkCount, dim)
		_, err = exec.Execute(ctx, graphitiBulkNodeSaveQuery, map[string]interface{}{"nodes": payload.nodes})
		require.NoError(b, err)
		_, err = exec.Execute(ctx, graphitiBulkEdgeSaveQuery, map[string]interface{}{"entity_edges": payload.edges})
		require.NoError(b, err)

		if attachSearch {
			searchSvc := search.NewServiceWithDimensions(ns, dim)
			require.NoError(b, searchSvc.BuildIndexes(ctx))
			require.True(b, searchSvc.HasRelationshipVectorEntries("RELATES_TO", "fact_embedding"))
			exec.SetSearchService(searchSvc)
		}

		return exec, ctx
	}

	baselineExec, baselineCtx := setup(b, false)
	indexedExec, indexedCtx := setup(b, true)
	queryVec := unitVectorF64(0, dim)
	params := map[string]interface{}{
		"search_vector": queryVec,
		"min_score":     -1.0,
		"limit":         limit,
	}

	b.ReportAllocs()
	b.Run("storage_scan_baseline", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			res, err := baselineExec.Execute(baselineCtx, graphitiEdgeSimilarityQuery, params)
			require.NoError(b, err)
			require.NotEmpty(b, res.Rows)
		}
	})

	b.Run("indexed_relationship_vectors", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			res, err := indexedExec.Execute(indexedCtx, graphitiEdgeSimilarityQuery, params)
			require.NoError(b, err)
			require.NotEmpty(b, res.Rows)
		}
	})
}

func buildGraphitiScenarioPayload(entityCount, edgeCount, chunkCount, dim int) graphitiScenarioPayload {
	nodes := make([]map[string]interface{}, 0, entityCount)
	for i := 0; i < entityCount; i++ {
		uuid := fmt.Sprintf("entity-%06d", i)
		nodes = append(nodes, map[string]interface{}{
			"uuid":           uuid,
			"name":           fmt.Sprintf("entity %06d", i),
			"summary":        fmt.Sprintf("synthetic summary for entity %06d", i),
			"group_id":       graphitiGroupID,
			"labels":         []string{"Entity", fmt.Sprintf("Tier%d", i%4)},
			"name_embedding": deterministicVectorF64(i, dim),
			"custom_rank":    int64(i % 11),
		})
	}

	edges := make([]map[string]interface{}, 0, edgeCount)
	for i := 0; i < edgeCount; i++ {
		src := i % entityCount
		dst := (i + (i % 7) + 1) % entityCount
		edges = append(edges, map[string]interface{}{
			"uuid":             fmt.Sprintf("edge-%06d", i),
			"source_node_uuid": fmt.Sprintf("entity-%06d", src),
			"target_node_uuid": fmt.Sprintf("entity-%06d", dst),
			"name":             "RELATES_TO",
			"fact":             fmt.Sprintf("entity-%06d relates entity-%06d", src, dst),
			"group_id":         graphitiGroupID,
			"fact_embedding":   deterministicVectorF64(10_000+i, dim),
		})
	}

	chunks := make([]map[string]interface{}, 0, chunkCount)
	for i := 0; i < chunkCount; i++ {
		chunks = append(chunks, map[string]interface{}{
			"uuid":     fmt.Sprintf("chunk-%06d", i),
			"group_id": graphitiGroupID,
			"emb":      deterministicVectorF64(20_000+i, dim),
			"text":     fmt.Sprintf("chunk text %06d with graphiti-like payload", i),
			"tokens":   int64(128 + (i % 64)),
		})
	}

	return graphitiScenarioPayload{
		nodes:  nodes,
		edges:  edges,
		chunks: chunks,
	}
}

func deterministicVectorF64(seed, dim int) []float64 {
	v := make([]float64, dim)
	base := seed % dim
	v[base] = 1.0
	v[(base+3)%dim] = 0.35
	v[(base+7)%dim] = 0.2
	normalizeF64(v)
	return v
}

func unitVectorF64(idx, dim int) []float64 {
	v := make([]float64, dim)
	v[idx%dim] = 1.0
	return v
}

func normalizeF64(v []float64) {
	sum := 0.0
	for _, x := range v {
		sum += x * x
	}
	if sum == 0 {
		return
	}
	n := math.Sqrt(sum)
	for i := range v {
		v[i] = v[i] / n
	}
}

func mustCountRows(t testing.TB, exec *StorageExecutor, ctx context.Context, query string, params map[string]interface{}) int64 {
	res, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	return mustInt64(t, res.Rows[0][0])
}

func mustInt64(t testing.TB, v interface{}) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	default:
		require.Failf(t, "unexpected numeric type", "value=%v (%T)", v, v)
		return 0
	}
}

func assertNonIncreasingScores(t testing.TB, rows [][]interface{}, scoreIdx int) {
	prev := math.Inf(1)
	for i, row := range rows {
		require.Greater(t, len(row), scoreIdx, "row %d missing score index %d", i, scoreIdx)
		score, ok := toFloat64(row[scoreIdx])
		require.True(t, ok, "row %d score is not numeric: %T", i, row[scoreIdx])
		require.LessOrEqual(t, score, prev+1e-6, "scores should be sorted descending")
		prev = score
	}
}
