package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type directBulkProbeEngine struct {
	*storage.NamespacedEngine
	bulkCreateNodeCalls int
	graphTxBegins       int
}

func (e *directBulkProbeEngine) BulkCreateNodes(nodes []*storage.Node) error {
	e.bulkCreateNodeCalls++
	return e.NamespacedEngine.BulkCreateNodes(nodes)
}

func (e *directBulkProbeEngine) BeginGraphTransaction() (storage.GraphTransaction, error) {
	e.graphTxBegins++
	return e.NamespacedEngine.BeginGraphTransaction()
}

type walProbeEngine struct {
	*directBulkProbeEngine
}

func (e *walProbeEngine) GetWAL() *storage.WAL {
	return &storage.WAL{}
}

func TestExecuteUnwindBareCreateBatch_DirectPathBypassesImplicitTransaction(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := &directBulkProbeEngine{
		NamespacedEngine: storage.NewNamespacedEngine(base, "bare_create_direct"),
	}
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	query := `
		UNWIND $rows AS row
		CREATE (:BenchProduct {
			productID: row.productID,
			productName: row.productName
		})
		RETURN count(*) AS created`
	params := map[string]interface{}{
		"rows": []map[string]interface{}{
			{"productID": "p1", "productName": "Chair"},
			{"productID": "p2", "productName": "Table"},
		},
	}
	variable, items, restQuery, ok := exec.parseParameterizedUnwindBatch(withParams(ctx, params), strings.TrimSpace(query))
	require.True(t, ok)
	require.Equal(t, "row", variable)
	require.Len(t, items, 2)
	require.True(t, startsWithKeywordFold(strings.TrimSpace(restQuery), "CREATE"))

	result, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Equal(t, 1, store.bulkCreateNodeCalls)
	require.Zero(t, store.graphTxBegins)
	require.Equal(t, []string{"created"}, result.Columns)
	require.EqualValues(t, 2, result.Rows[0][0])
	require.EqualValues(t, 2, result.Stats.NodesCreated)

	nodes, err := store.GetNodesByLabel("BenchProduct")
	require.NoError(t, err)
	require.Len(t, nodes, 2)
}

func TestTryUnwindBareCreateDirectBatch_SkipsWhenWALPresent(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := &walProbeEngine{directBulkProbeEngine: &directBulkProbeEngine{
		NamespacedEngine: storage.NewNamespacedEngine(base, "bare_create_wal_skip"),
	}}
	exec := NewStorageExecutor(store)
	ctx := withParams(context.Background(), map[string]interface{}{
		"rows": []map[string]interface{}{{"productID": "p1"}},
	})

	result, err, handled := exec.tryUnwindBareCreateDirectBatch(ctx, `
		UNWIND $rows AS row
		CREATE (:BenchProduct {productID: row.productID})`)
	require.NoError(t, err)
	require.Nil(t, result)
	require.False(t, handled)
	require.Zero(t, store.bulkCreateNodeCalls)
}

func TestTryUnwindBareCreateDirectBatch_SkipsWhenUseDatabaseContextPresent(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := &directBulkProbeEngine{
		NamespacedEngine: storage.NewNamespacedEngine(base, "bare_create_use_skip"),
	}
	exec := NewStorageExecutor(store)
	ctx := withParams(context.Background(), map[string]interface{}{
		"rows": []map[string]interface{}{{"productID": "p1"}},
	})
	ctx = context.WithValue(ctx, ctxKeyUseDatabase, "tenant_ctx")

	result, err, handled := exec.tryUnwindBareCreateDirectBatch(ctx, `
		UNWIND $rows AS row
		CREATE (:BenchProduct {productID: row.productID})`)
	require.NoError(t, err)
	require.Nil(t, result)
	require.False(t, handled)
	require.Zero(t, store.bulkCreateNodeCalls)
}

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
