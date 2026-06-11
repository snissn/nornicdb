package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCreateVectorIndex_RelationshipSyntaxFormsAccepted(t *testing.T) {
	store := newTestMemoryEngine(t)
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	q1 := "CREATE VECTOR INDEX rel_emb_idx IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}"
	_, err := exec.Execute(ctx, q1, nil)
	require.NoError(t, err)

	q2 := "CREATE VECTOR INDEX rel_emb_idx_dir IF NOT EXISTS FOR ()-[e:RELATES_TO]->() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}"
	_, err = exec.Execute(ctx, q2, nil)
	require.NoError(t, err)

	idx1, ok := store.GetSchema().GetVectorIndex("rel_emb_idx")
	require.True(t, ok)
	require.Equal(t, "RELATES_TO", idx1.Label)
	require.Equal(t, "fact_embedding", idx1.Property)
	require.Equal(t, 3, idx1.Dimensions)
	require.Equal(t, "cosine", idx1.SimilarityFunc)

	idx2, ok := store.GetSchema().GetVectorIndex("rel_emb_idx_dir")
	require.True(t, ok)
	require.Equal(t, "RELATES_TO", idx2.Label)
	require.Equal(t, "fact_embedding", idx2.Property)
}

func TestCreateVectorIndex_Relationship_E2EQueryRelationships(t *testing.T) {
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (a:Entity {uuid:'a'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (b:Entity {uuid:'b'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE (c:Entity {uuid:'c'})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "MATCH (a:Entity {uuid:'a'}), (b:Entity {uuid:'b'}) CREATE (a)-[:RELATES_TO {uuid:'r1', fact_embedding:[1.0,0.0,0.0]}]->(b)", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "MATCH (a:Entity {uuid:'a'}), (c:Entity {uuid:'c'}) CREATE (a)-[:RELATES_TO {uuid:'r2', fact_embedding:[0.0,1.0,0.0]}]->(c)", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CREATE VECTOR INDEX rel_emb_idx IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON (e.fact_embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}", nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, "CALL db.index.vector.queryRelationships('rel_emb_idx', 2, [1.0,0.0,0.0]) YIELD relationship, score RETURN relationship.uuid AS rid, score ORDER BY score DESC LIMIT 2", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "r1", res.Rows[0][0])
}
