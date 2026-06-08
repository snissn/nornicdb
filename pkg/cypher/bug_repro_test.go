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
