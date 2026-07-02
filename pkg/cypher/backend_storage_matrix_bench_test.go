package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	backendCypherBatchSize = 100
	backendCypherReadNodes = 256
	backendCypherPathNodes = 64
)

type backendCypherCase struct {
	name string
	open func(*testing.B) (storage.Engine, func())
}

func backendCypherCases() []backendCypherCase {
	return []backendCypherCase{
		{
			name: "badger_namespaced",
			open: func(b *testing.B) (storage.Engine, func()) {
				b.Helper()
				inner, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: b.TempDir()})
				if err != nil {
					b.Fatal(err)
				}
				return storage.NewNamespacedEngine(inner, "nornic"), func() { _ = inner.Close() }
			},
		},
		{
			name: "treedb_namespaced",
			open: func(b *testing.B) (storage.Engine, func()) {
				b.Helper()
				inner, err := storage.NewTreeDBEngineWithOptions(storage.TreeDBOptions{Dir: b.TempDir()})
				if err != nil {
					b.Fatal(err)
				}
				return storage.NewNamespacedEngine(inner, "nornic"), func() { _ = inner.Close() }
			},
		},
	}
}

func BenchmarkBackendCypherMatrix(b *testing.B) {
	workloads := []struct {
		name string
		run  func(*testing.B, *StorageExecutor, storage.Engine)
	}{
		{name: "BareCreateBatch100", run: benchmarkBackendCypherBareCreate},
		{name: "LabelCountRead256", run: benchmarkBackendCypherLabelCount},
		{name: "RelationshipCount255", run: benchmarkBackendCypherRelationshipCount},
		{name: "ShortestPath64", run: benchmarkBackendCypherShortestPath},
	}

	for _, backend := range backendCypherCases() {
		backend := backend
		b.Run(backend.name, func(b *testing.B) {
			for _, workload := range workloads {
				workload := workload
				b.Run(workload.name, func(b *testing.B) {
					engine, cleanup := backend.open(b)
					defer cleanup()
					exec := NewStorageExecutor(engine)
					workload.run(b, exec, engine)
				})
			}
		})
	}
}

func benchmarkBackendCypherBareCreate(b *testing.B, exec *StorageExecutor, _ storage.Engine) {
	ctx := context.Background()
	rows := backendCypherRows(0, backendCypherBatchSize)
	query := `
		UNWIND $rows AS row
		CREATE (:BenchProduct {
			productID: row.productID,
			productName: row.productName,
			unitPrice: row.unitPrice,
			description: row.description
		})`

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		refreshBackendCypherRows(rows, i*backendCypherBatchSize)
		b.StartTimer()
		if _, err := exec.Execute(ctx, query, map[string]interface{}{"rows": rows}); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkBackendCypherLabelCount(b *testing.B, exec *StorageExecutor, engine storage.Engine) {
	seedBackendCypherChain(b, engine, backendCypherReadNodes)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := exec.Execute(ctx, "MATCH (n:BenchNode) RETURN count(n) AS count", nil)
		if err != nil {
			b.Fatal(err)
		}
		requireBackendCypherSingleInt(b, result, "count", backendCypherReadNodes)
	}
}

func benchmarkBackendCypherRelationshipCount(b *testing.B, exec *StorageExecutor, engine storage.Engine) {
	seedBackendCypherChain(b, engine, backendCypherReadNodes)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := exec.Execute(ctx, "MATCH ()-[r:BENCH_LINK]->() RETURN count(r) AS count", nil)
		if err != nil {
			b.Fatal(err)
		}
		requireBackendCypherSingleInt(b, result, "count", backendCypherReadNodes-1)
	}
}

func benchmarkBackendCypherShortestPath(b *testing.B, exec *StorageExecutor, engine storage.Engine) {
	seedBackendCypherChain(b, engine, backendCypherPathNodes)
	ctx := context.Background()
	query := `
		MATCH (start:BenchNode {nodeID: $startID}), (end:BenchNode {nodeID: $endID})
		MATCH p = shortestPath((start)-[:BENCH_LINK*]-(end))
		RETURN length(p) AS hops`
	params := map[string]interface{}{
		"startID": int64(0),
		"endID":   int64(backendCypherPathNodes - 1),
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := exec.Execute(ctx, query, params)
		if err != nil {
			b.Fatal(err)
		}
		requireBackendCypherSingleInt(b, result, "hops", backendCypherPathNodes-1)
	}
}

func backendCypherRows(base, count int) []interface{} {
	rows := make([]interface{}, count)
	for i := 0; i < count; i++ {
		rows[i] = map[string]interface{}{
			"productID":   int64(base + i),
			"productName": fmt.Sprintf("Product-%d", base+i),
			"unitPrice":   float64((i%20)+1) * 1.25,
			"description": "backend storage matrix payload",
		}
	}
	return rows
}

func refreshBackendCypherRows(rows []interface{}, base int) {
	for i := range rows {
		row := rows[i].(map[string]interface{})
		row["productID"] = int64(base + i)
		row["productName"] = fmt.Sprintf("Product-%d", base+i)
	}
}

func requireBackendCypherSingleInt(b *testing.B, result *ExecuteResult, column string, want int) {
	b.Helper()
	if len(result.Rows) != 1 {
		b.Fatalf("expected one row, got %d", len(result.Rows))
	}
	columnIndex := -1
	for i, gotColumn := range result.Columns {
		if gotColumn == column {
			columnIndex = i
			break
		}
	}
	if columnIndex < 0 {
		b.Fatalf("expected column %q in columns %#v", column, result.Columns)
	}
	row := result.Rows[0]
	if columnIndex >= len(row) {
		b.Fatalf("expected column %q at index %d in row %#v", column, columnIndex, row)
	}
	got := row[columnIndex]
	if gotInt := toInt64(got); gotInt != int64(want) {
		b.Fatalf("expected %s=%d, got %v (%T)", column, want, got, got)
	}
}

func seedBackendCypherChain(b *testing.B, engine storage.Engine, nodes int) {
	b.Helper()
	for i := 0; i < nodes; i++ {
		if _, err := engine.CreateNode(&storage.Node{
			ID:         storage.NodeID("bench-node-" + itoa(i)),
			Labels:     []string{"BenchNode"},
			Properties: map[string]any{"nodeID": int64(i)},
		}); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < nodes-1; i++ {
		if err := engine.CreateEdge(&storage.Edge{
			ID:        storage.EdgeID("bench-edge-" + itoa(i)),
			StartNode: storage.NodeID("bench-node-" + itoa(i)),
			EndNode:   storage.NodeID("bench-node-" + itoa(i+1)),
			Type:      "BENCH_LINK",
		}); err != nil {
			b.Fatal(err)
		}
	}
}
