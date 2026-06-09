package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBug1_SetMapReplacementDropsProperties(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	params := map[string]interface{}{
		"d": map[string]interface{}{
			"uuid":     "n1",
			"name":     "Ada",
			"group_id": "g",
		},
	}

	query := `
		MERGE (n:Bug {uuid: $d.uuid})
		SET n = $d
		RETURN properties(n) AS p
	`

	result, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	expectedProps := map[string]interface{}{
		"uuid":     "n1",
		"name":     "Ada",
		"group_id": "g",
	}

	p := result.Rows[0][0].(map[string]interface{})

	assert.Equal(t, expectedProps, p, "Properties should not be dropped during map replacement")
}

func TestBug1_ChainedSetLabelAndMapPreservesPropertiesAndLabels(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	testCases := []struct {
		name  string
		query string
	}{
		{
			name: "label_then_map",
			query: `
				MERGE (n:M {uuid: $d.uuid})
				SET n:Extra
				SET n = $d
				RETURN properties(n) AS p, labels(n) AS labels
			`,
		},
		{
			name: "map_then_label",
			query: `
				MERGE (n:M {uuid: $d.uuid})
				SET n = $d
				SET n:Extra
				RETURN properties(n) AS p, labels(n) AS labels
			`,
		},
		{
			name: "dynamic_label_then_map",
			query: `
				MERGE (n:M {uuid: $d.uuid})
				SET n:$(d.labels)
				SET n = $d
				RETURN properties(n) AS p, labels(n) AS labels
			`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			params := map[string]interface{}{
				"d": map[string]interface{}{
					"uuid":     "k-" + tc.name,
					"name":     "Ada",
					"group_id": "g",
					"labels":   []interface{}{"Extra"},
				},
			}

			res, err := exec.Execute(ctx, tc.query, params)
			require.NoError(t, err)
			require.Len(t, res.Rows, 1)

			props, ok := res.Rows[0][0].(map[string]interface{})
			require.True(t, ok)
			assert.Equal(t, map[string]interface{}{
				"uuid":     "k-" + tc.name,
				"name":     "Ada",
				"group_id": "g",
				"labels":   []interface{}{"Extra"},
			}, props)

			labels, ok := res.Rows[0][1].([]interface{})
			require.True(t, ok)
			assert.Contains(t, labels, "M")
			assert.Contains(t, labels, "Extra")
		})
	}
}

func TestBug1_UnwindDynamicLabelAndMapPreservesPropertiesAndLabels(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	res, err := exec.Execute(ctx, `
		UNWIND $nodes AS node
		MERGE (n:M {uuid: node.uuid})
		SET n:$(node.labels)
		SET n = node
		RETURN properties(n) AS p, labels(n) AS labels
	`, map[string]interface{}{
		"nodes": []map[string]interface{}{
			{
				"uuid":     "bulk-k",
				"name":     "Ada",
				"group_id": "g",
				"labels":   []interface{}{"Extra"},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)

	props, ok := res.Rows[0][0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, map[string]interface{}{
		"uuid":     "bulk-k",
		"name":     "Ada",
		"group_id": "g",
		"labels":   []interface{}{"Extra"},
	}, props)

	labels, ok := res.Rows[0][1].([]interface{})
	require.True(t, ok)
	assert.Contains(t, labels, "M")
	assert.Contains(t, labels, "Extra")
}

func TestBug2_UnwindSetAliasCorruptsMergeKey(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	params := map[string]interface{}{
		"rows": []map[string]interface{}{
			{"uuid": "good-uuid", "name": "x"},
		},
	}

	query := `
		UNWIND $rows AS row
		MERGE (n:Bug {uuid: row.uuid})
		SET n += row
		RETURN n.uuid AS uuid
	`

	result, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	uuid := result.Rows[0][0].(string)

	assert.Equal(t, "good-uuid", uuid, "Merge key should be the value from the row, not the literal string 'row.uuid'")
}

func TestBug3_DynamicLabelsAreApplied(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	params := map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"uuid":   "d1",
				"labels": []interface{}{"Person", "Author"},
			},
		},
	}

	query := `
		UNWIND $rows AS row
		MERGE (n:Bug {uuid: row.uuid})
		SET n:$(row.labels)
		RETURN labels(n) AS labels
	`

	result, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)

	labels, ok := result.Rows[0][0].([]interface{})
	require.True(t, ok)
	assert.Contains(t, labels, "Bug")
	assert.Contains(t, labels, "Person")
	assert.Contains(t, labels, "Author")
}

func TestBug4_SetNodeVectorPropertySetsPropertyAndPreservesReturn(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "v1",
		Labels: []string{"Bug"},
		Properties: map[string]interface{}{
			"uuid": "v1",
		},
	})
	require.NoError(t, err)

	query := `
		MATCH (n:Bug {uuid:'v1'})
		WITH n
		CALL db.create.setNodeVectorProperty('v1', 'emb', $v)
		RETURN n.uuid AS uuid
	`

	result, err := exec.Execute(ctx, query, map[string]interface{}{
		"v": []interface{}{0.1, 0.2, 0.3},
	})
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	assert.Equal(t, "v1", result.Rows[0][0])

	updated, err := exec.Execute(ctx, `MATCH (n:Bug {uuid:'v1'}) RETURN n.emb AS emb`, nil)
	require.NoError(t, err)
	require.Len(t, updated.Rows, 1)

	emb, ok := updated.Rows[0][0].([]float64)
	require.True(t, ok)
	assert.Equal(t, []float64{0.1, 0.2, 0.3}, emb)
}

func TestBug5_SetNodeVectorPropertyAcceptsNodeVariableArgument(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := store.CreateNode(&storage.Node{
		ID:     "v2",
		Labels: []string{"Bug"},
		Properties: map[string]interface{}{
			"uuid": "v2",
		},
	})
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
		MATCH (n:Bug {uuid:'v2'})
		CALL db.create.setNodeVectorProperty(n, 'emb', $v)
		RETURN n.uuid AS uuid
	`, map[string]interface{}{
		"v": []interface{}{0.1, 0.2, 0.3},
	})
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CALL db.index.vector.createNodeIndex('idx_bug_emb', 'Bug', 'emb', 3, 'cosine')", nil)
	require.NoError(t, err)

	knnRes, err := exec.Execute(ctx, `
		CALL db.index.vector.queryNodes('idx_bug_emb', 1, [0.1, 0.2, 0.3])
		YIELD node, score
		RETURN node.uuid AS uuid
	`, nil)
	require.NoError(t, err)
	require.NotEmpty(t, knnRes.Rows)
	assert.Equal(t, "v2", knnRes.Rows[0][0])
}

func TestBug6_UnwindMergeSetMapWithIndexedMapPropertyDoesNotPanic(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	err := store.GetSchema().AddPropertyIndex("idx_bulk_payload", "BulkNode", []string{"payload"})
	require.NoError(t, err)

	params := map[string]interface{}{
		"rows": []map[string]interface{}{
			{
				"uuid": "b1",
				"name": "a",
				"payload": map[string]interface{}{
					"k": "v",
				},
			},
			{
				"uuid": "b2",
				"name": "b",
			},
		},
	}

	require.NotPanics(t, func() {
		res, qErr := exec.Execute(ctx, `
			UNWIND $rows AS row
			MERGE (n:BulkNode {uuid: row.uuid})
			SET n = row
			RETURN n.uuid AS uuid
		`, params)
		require.NoError(t, qErr)
		require.Len(t, res.Rows, 2)
	})
}

func TestBug7_UnwindMergeWithInlineSetNodeVectorPropertyPersistsRows(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	query := `
		UNWIND $nodes AS node
		MERGE (n:Entity {uuid: node.uuid})
		SET n:$(node.labels)
		SET n = node
		WITH n, node
		CALL db.create.setNodeVectorProperty(n, "name_embedding", node.name_embedding)
		RETURN n.uuid AS uuid
	`

	params := map[string]interface{}{
		"nodes": []map[string]interface{}{
			{
				"uuid":           "g1",
				"name":           "Ada",
				"labels":         []interface{}{"Extra"},
				"name_embedding": []interface{}{0.1, 0.2, 0.3},
			},
			{
				"uuid":           "g2",
				"name":           "Grace",
				"labels":         []interface{}{"Extra"},
				"name_embedding": []interface{}{0.3, 0.2, 0.1},
			},
		},
	}

	res, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)

	uuids := make(map[string]bool, len(res.Rows))
	for _, row := range res.Rows {
		id, ok := row[0].(string)
		require.True(t, ok)
		uuids[id] = true
	}
	assert.True(t, uuids["g1"])
	assert.True(t, uuids["g2"])

	check, err := exec.Execute(ctx, `
		MATCH (n:Entity)
		WHERE n.uuid IN ['g1', 'g2']
		RETURN n.uuid AS uuid, labels(n) AS labels, n.name_embedding AS emb
	`, nil)
	require.NoError(t, err)
	require.Len(t, check.Rows, 2)
	for _, row := range check.Rows {
		assert.NotNil(t, row[2], "name_embedding should persist on entity")
		labels, ok := row[1].([]interface{})
		require.True(t, ok)
		assert.Contains(t, labels, "Entity")
		assert.Contains(t, labels, "Extra")
	}
}

func TestBug8_MatchUnwindCreateWithListPropertyPersistsAllRows(t *testing.T) {
	baseStore := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(baseStore, "test")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, `CREATE (:Anchor {uuid:'a'})`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
		MATCH (x:Anchor {uuid:'a'})
		UNWIND [{id:'r0', vec:[0.1,0.2]}, {id:'r1', vec:[0.3,0.4]}, {id:'r2', vec:[0.5,0.6]}] AS r
		CREATE (c:Item {id:r.id, vec:r.vec})
	`, nil)
	require.NoError(t, err)

	res, err := exec.Execute(ctx, `MATCH (c:Item) RETURN count(c) AS c`, nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Len(t, res.Rows[0], 1)
	assert.Equal(t, int64(3), res.Rows[0][0])
}
