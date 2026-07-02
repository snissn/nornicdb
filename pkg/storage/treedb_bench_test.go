package storage

import (
	"encoding/binary"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

const treeDBBulkBenchSize = 128

func newDirectTreeDBBenchDB(b *testing.B) *treedb.DB {
	b.Helper()
	opts := treedb.OptionsFor(treedb.ProfileLegacyWALDurable, b.TempDir())
	treedb.DisableValueLogDictCompression(&opts)
	db, err := treedb.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	return db
}

func BenchmarkPersistentBadgerEngine_CreateNode(b *testing.B) {
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := engine.CreateNode(&Node{
			ID:         NodeID("bench:n" + itoaBench(i)),
			Labels:     []string{"Benchmark"},
			Properties: map[string]any{"index": i},
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPersistentBadgerEngine_GetNode(b *testing.B) {
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	const nodes = 10000
	for i := 0; i < nodes; i++ {
		if _, err := engine.CreateNode(&Node{
			ID:     NodeID("bench:n" + itoaBench(i)),
			Labels: []string{"Benchmark"},
		}); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := engine.GetNode(NodeID("bench:n" + itoaBench(i%nodes))); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDirectTreeDB_GraphCreateNodeEquivalent(b *testing.B) {
	db := newDirectTreeDBBenchDB(b)
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := NodeID("bench:direct-n" + itoaBench(i))
		node := &Node{
			ID:         id,
			Labels:     []string{"Benchmark"},
			Properties: map[string]any{"index": i},
		}
		tx, err := db.NewConditionalTxn()
		if err != nil {
			b.Fatal(err)
		}
		tx.ReserveReadSet(1)
		tx.ReserveWrites(3)

		exists, err := tx.Has(nodeKey(id))
		if err != nil {
			_ = tx.Close()
			b.Fatal(err)
		}
		if exists {
			_ = tx.Close()
			b.Fatal("duplicate benchmark node")
		}
		data, err := serializeNode(node)
		if err != nil {
			_ = tx.Close()
			b.Fatal(err)
		}
		bodyKey := nodeKey(id)
		labelKey := treeDBLabelIndexKey("Benchmark", id)
		guardKey := treeDBNodeNamespaceGuardKey("bench")
		var guardValue [8]byte
		binary.LittleEndian.PutUint64(guardValue[:], uint64(i+1))

		held := make([][]byte, 0, 6)
		set := func(key, value []byte) {
			held = append(held, key, value)
			if err := tx.SetView(key, value); err != nil {
				_ = tx.Close()
				b.Fatal(err)
			}
		}
		set(bodyKey, data)
		set(labelKey, treeDBEmptyValue)
		set(guardKey, guardValue[:])
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
		_ = held
	}
}

func BenchmarkTreeDBEngine_CreateNode(b *testing.B) {
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := engine.CreateNode(&Node{
			ID:         NodeID("bench:n" + itoaBench(i)),
			Labels:     []string{"Benchmark"},
			Properties: map[string]any{"index": i},
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTreeDBEngine_BulkCreateNodes(b *testing.B) {
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nodes := treeDBBenchBulkNodes("bench:bulk-n", i*treeDBBulkBenchSize, treeDBBulkBenchSize)
		b.StartTimer()
		if err := engine.BulkCreateNodes(nodes); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTreeDBEngine_GetNode(b *testing.B) {
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	const nodes = 10000
	for i := 0; i < nodes; i++ {
		if _, err := engine.CreateNode(&Node{
			ID:     NodeID("bench:n" + itoaBench(i)),
			Labels: []string{"Benchmark"},
		}); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := engine.GetNode(NodeID("bench:n" + itoaBench(i%nodes))); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDirectTreeDB_GraphBulkCreateNodesEquivalent(b *testing.B) {
	db := newDirectTreeDBBenchDB(b)
	defer db.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nodes := treeDBBenchBulkNodes("bench:direct-bulk-n", i*treeDBBulkBenchSize, treeDBBulkBenchSize)
		b.StartTimer()

		tx, err := db.NewConditionalTxn()
		if err != nil {
			b.Fatal(err)
		}
		tx.ReserveReadSet(len(nodes))
		tx.ReserveWrites(len(nodes) * 3)
		held := make([][]byte, 0, len(nodes)*6)
		set := func(key, value []byte) {
			held = append(held, key, value)
			if err := tx.SetView(key, value); err != nil {
				_ = tx.Close()
				b.Fatal(err)
			}
		}
		var guardValue [8]byte
		binary.LittleEndian.PutUint64(guardValue[:], uint64(i+1))
		guardKey := treeDBNodeNamespaceGuardKey("bench")
		for _, node := range nodes {
			exists, err := tx.Has(nodeKey(node.ID))
			if err != nil {
				_ = tx.Close()
				b.Fatal(err)
			}
			if exists {
				_ = tx.Close()
				b.Fatal("duplicate benchmark node")
			}
			data, err := serializeNode(node)
			if err != nil {
				_ = tx.Close()
				b.Fatal(err)
			}
			set(nodeKey(node.ID), data)
			set(treeDBLabelIndexKey("Benchmark", node.ID), treeDBEmptyValue)
			set(guardKey, guardValue[:])
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
		_ = held
	}
}

func BenchmarkTreeDBEngine_CreateEdge(b *testing.B) {
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	if _, err := engine.CreateNode(&Node{ID: "bench:start", Labels: []string{"Benchmark"}}); err != nil {
		b.Fatal(err)
	}
	if _, err := engine.CreateNode(&Node{ID: "bench:end", Labels: []string{"Benchmark"}}); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := engine.CreateEdge(&Edge{
			ID:        EdgeID("bench:e" + itoaBench(i)),
			StartNode: "bench:start",
			EndNode:   "bench:end",
			Type:      "BENCH",
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTreeDBEngine_BulkCreateEdges(b *testing.B) {
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	if err := engine.BulkCreateNodes([]*Node{
		{ID: "bench:bulk-start", Labels: []string{"Benchmark"}},
		{ID: "bench:bulk-end", Labels: []string{"Benchmark"}},
	}); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		edges := treeDBBenchBulkEdges("bench:bulk-e", i*treeDBBulkBenchSize, treeDBBulkBenchSize, "bench:bulk-start", "bench:bulk-end")
		b.StartTimer()
		if err := engine.BulkCreateEdges(edges); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTreeDBEngine_TxnCreateNode(b *testing.B) {
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := engine.BeginGraphTransaction()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := tx.CreateNode(&Node{
			ID:         NodeID("bench:txn-n" + itoaBench(i)),
			Labels:     []string{"Benchmark"},
			Properties: map[string]any{"index": i},
		}); err != nil {
			_ = tx.Rollback()
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNamespacedTreeDBEngine_CreateNode(b *testing.B) {
	inner, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer inner.Close()
	engine := NewNamespacedEngine(inner, "nornic")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := engine.CreateNode(&Node{
			ID:         NodeID("n" + itoaBench(i)),
			Labels:     []string{"Benchmark"},
			Properties: map[string]any{"index": i},
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNamespacedTreeDBEngine_BulkCreateNodes(b *testing.B) {
	inner, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer inner.Close()
	engine := NewNamespacedEngine(inner, "nornic")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nodes := treeDBBenchBulkNodes("bulk-n", i*treeDBBulkBenchSize, treeDBBulkBenchSize)
		b.StartTimer()
		if err := engine.BulkCreateNodes(nodes); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNamespacedTreeDBEngine_GetNode(b *testing.B) {
	inner, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer inner.Close()
	engine := NewNamespacedEngine(inner, "nornic")

	const nodes = 10000
	for i := 0; i < nodes; i++ {
		if _, err := engine.CreateNode(&Node{
			ID:     NodeID("n" + itoaBench(i)),
			Labels: []string{"Benchmark"},
		}); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := engine.GetNode(NodeID("n" + itoaBench(i%nodes))); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNamespacedTreeDBEngine_CreateEdge(b *testing.B) {
	inner, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer inner.Close()
	engine := NewNamespacedEngine(inner, "nornic")

	if _, err := engine.CreateNode(&Node{ID: "start", Labels: []string{"Benchmark"}}); err != nil {
		b.Fatal(err)
	}
	if _, err := engine.CreateNode(&Node{ID: "end", Labels: []string{"Benchmark"}}); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := engine.CreateEdge(&Edge{
			ID:        EdgeID("e" + itoaBench(i)),
			StartNode: "start",
			EndNode:   "end",
			Type:      "BENCH",
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNamespacedTreeDBEngine_BulkCreateEdges(b *testing.B) {
	inner, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer inner.Close()
	engine := NewNamespacedEngine(inner, "nornic")

	if err := engine.BulkCreateNodes([]*Node{
		{ID: "bulk-start", Labels: []string{"Benchmark"}},
		{ID: "bulk-end", Labels: []string{"Benchmark"}},
	}); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		edges := treeDBBenchBulkEdges("bulk-e", i*treeDBBulkBenchSize, treeDBBulkBenchSize, "bulk-start", "bulk-end")
		b.StartTimer()
		if err := engine.BulkCreateEdges(edges); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNamespacedTreeDBEngine_TxnCreateNode(b *testing.B) {
	inner, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer inner.Close()
	engine := NewNamespacedEngine(inner, "nornic")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := engine.BeginGraphTransaction()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := tx.CreateNode(&Node{
			ID:         NodeID("txn-n" + itoaBench(i)),
			Labels:     []string{"Benchmark"},
			Properties: map[string]any{"index": i},
		}); err != nil {
			_ = tx.Rollback()
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func treeDBBenchBulkNodes(prefix string, base, count int) []*Node {
	nodes := make([]*Node, count)
	for i := 0; i < count; i++ {
		nodes[i] = &Node{
			ID:     NodeID(prefix + itoaBench(base+i)),
			Labels: []string{"Benchmark"},
		}
	}
	return nodes
}

func treeDBBenchBulkEdges(prefix string, base, count int, start, end NodeID) []*Edge {
	edges := make([]*Edge, count)
	for i := 0; i < count; i++ {
		edges[i] = &Edge{
			ID:        EdgeID(prefix + itoaBench(base+i)),
			StartNode: start,
			EndNode:   end,
			Type:      "BENCH",
		}
	}
	return edges
}
