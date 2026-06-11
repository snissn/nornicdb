package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestE2E_VectorCosine_RelationshipProjectionFastPath_GraphitiShape(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	for _, q := range []string{
		"CREATE (:Entity {uuid:'a', group_id:'g'})",
		"CREATE (:Entity {uuid:'b', group_id:'g'})",
		"CREATE (:Entity {uuid:'c', group_id:'g'})",
		"CREATE (:Entity {uuid:'d', group_id:'other'})",
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	for _, q := range []string{
		"MATCH (a:Entity {uuid:'a'}), (b:Entity {uuid:'b'}) CREATE (a)-[:RELATES_TO {uuid:'e1', fact_embedding:[1.0,0.0,0.0]}]->(b)",
		"MATCH (a:Entity {uuid:'a'}), (c:Entity {uuid:'c'}) CREATE (a)-[:RELATES_TO {uuid:'e2', fact_embedding:[0.9,0.1,0.0]}]->(c)",
		"MATCH (a:Entity {uuid:'a'}), (d:Entity {uuid:'d'}) CREATE (a)-[:RELATES_TO {uuid:'e3', fact_embedding:[0.0,1.0,0.0]}]->(d)",
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}

	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX rel_idx FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	probe, err := exec.Execute(ctx, "CALL db.index.vector.queryRelationships('rel_idx', 5, [1.0,0.0,0.0]) YIELD relationship, score RETURN relationship, score", nil)
	require.NoError(t, err)
	require.NotEmpty(t, probe.Rows)

	query := `MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity)
WHERE n.group_id IN $groups
WITH DISTINCT e, n, m, vector.similarity.cosine(e.fact_embedding, $q) AS score
WHERE score > $min
RETURN e.uuid AS edge_uuid, n.uuid AS src_uuid, m.uuid AS dst_uuid, score
ORDER BY score DESC
LIMIT $limit`
	res, err := exec.Execute(ctx, query, map[string]interface{}{
		"groups": []string{"g"},
		"q":      []float64{1.0, 0.0, 0.0},
		"min":    0.2,
		"limit":  5,
	})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "e1", res.Rows[0][0])
}

func TestE2E_VectorCosine_RelationshipProjectionFastPath_DirectAnalog(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	for _, q := range []string{
		"CREATE (:Entity {uuid:'a'})",
		"CREATE (:Entity {uuid:'b'})",
		"CREATE (:Entity {uuid:'c'})",
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}
	for _, q := range []string{
		"MATCH (a:Entity {uuid:'a'}), (b:Entity {uuid:'b'}) CREATE (a)-[:RELATES_TO {uuid:'r1', fact_embedding:[1.0,0.0,0.0]}]->(b)",
		"MATCH (a:Entity {uuid:'a'}), (c:Entity {uuid:'c'}) CREATE (a)-[:RELATES_TO {uuid:'r2', fact_embedding:[0.0,1.0,0.0]}]->(c)",
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err)
	}
	_, err := exec.Execute(ctx, "CREATE VECTOR INDEX rel_idx FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)
	probe, err := exec.Execute(ctx, "CALL db.index.vector.queryRelationships('rel_idx', 5, [1.0,0.0,0.0]) YIELD relationship, score RETURN relationship, score", nil)
	require.NoError(t, err)
	require.NotEmpty(t, probe.Rows)

	direct := "MATCH ()-[e:RELATES_TO]->() WITH e, vector.similarity.cosine(e.fact_embedding,$q) AS s RETURN e.uuid, s ORDER BY s DESC LIMIT 5"
	res, err := exec.Execute(ctx, direct, map[string]interface{}{"q": []float64{1.0, 0.0, 0.0}})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().CosineVectorIndexFastPath)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "r1", res.Rows[0][0])
}
