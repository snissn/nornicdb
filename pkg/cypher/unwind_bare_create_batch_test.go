package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExecuteUnwindBareCreateBatch_UsesBulkCreatePath(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "bare_create_batch")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
		UNWIND $rows AS row
		CREATE (:BenchProduct {
			productID: row.productID,
			productName: row.productName,
			unitPrice: row.unitPrice,
			active: true
		})
		RETURN count(*) AS created`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{"productID": "p1", "productName": "Chair", "unitPrice": 12.5},
			{"productID": "p2", "productName": "Table", "unitPrice": 25.0},
		},
	})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindMultiMatchCreateBatch)
	require.Equal(t, []string{"created"}, result.Columns)
	require.EqualValues(t, 2, result.Rows[0][0])
	require.EqualValues(t, 2, result.Stats.NodesCreated)

	nodes, err := store.GetNodesByLabel("BenchProduct")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	byID := map[string]*storage.Node{}
	for _, node := range nodes {
		byID[node.Properties["productID"].(string)] = node
	}
	require.Equal(t, "Chair", byID["p1"].Properties["productName"])
	require.Equal(t, 12.5, byID["p1"].Properties["unitPrice"])
	require.Equal(t, true, byID["p1"].Properties["active"])
}

func TestExecuteUnwindBareCreateBatch_MultipleCreates(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "bare_create_batch_multi")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	result, err := exec.Execute(ctx, `
		UNWIND $rows AS row
		CREATE (:BenchProduct {productID: row.productID})
		CREATE (:BenchAudit {productID: row.productID, source: 'seed'})`, map[string]interface{}{
		"rows": []map[string]interface{}{
			{"productID": "p1"},
			{"productID": "p2"},
		},
	})
	require.NoError(t, err)
	require.True(t, exec.LastHotPathTrace().UnwindMultiMatchCreateBatch)
	require.EqualValues(t, 4, result.Stats.NodesCreated)

	products, err := store.GetNodesByLabel("BenchProduct")
	require.NoError(t, err)
	require.Len(t, products, 2)
	audits, err := store.GetNodesByLabel("BenchAudit")
	require.NoError(t, err)
	require.Len(t, audits, 2)
	require.Equal(t, "seed", audits[0].Properties["source"])
}

func TestExecuteUnwindBareCreateBatch_RejectsUnsupportedShapes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "bare_create_batch_reject"))
	ctx := context.Background()
	items := []interface{}{map[string]interface{}{"name": "a"}}

	_, ok, err := exec.executeUnwindBareCreateBatch(ctx, "row", items, "CREATE (:Person {name: row.name}) SET n.flag = true")
	require.NoError(t, err)
	require.False(t, ok)

	_, ok, err = exec.executeUnwindBareCreateBatch(ctx, "row", []interface{}{"not-a-map"}, "CREATE (:Person {name: row.name})")
	require.NoError(t, err)
	require.False(t, ok)

	_, ok, err = exec.executeUnwindBareCreateBatch(ctx, "row", items, "CREATE (:Person {name: row.name})")
	require.NoError(t, err)
	require.True(t, ok)
}
