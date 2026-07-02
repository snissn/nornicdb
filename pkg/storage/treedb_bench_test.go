package storage

import "testing"

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
