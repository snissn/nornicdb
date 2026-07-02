package storage

import (
	"encoding/binary"
	"errors"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
)

const (
	storageBackendBenchReadNodes = 1000
	storageBackendBenchDegree    = 8
)

func newPersistentBadgerBenchEngine(b *testing.B) *BadgerEngine {
	b.Helper()
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	return engine
}

func seedBenchNodes(b *testing.B, engine Engine, prefix string, count int) []NodeID {
	b.Helper()
	ids := make([]NodeID, count)
	for i := 0; i < count; i++ {
		id := NodeID(prefix + itoaBench(i))
		ids[i] = id
		if _, err := engine.CreateNode(&Node{
			ID:         id,
			Labels:     []string{"Benchmark"},
			Properties: map[string]any{"index": i},
		}); err != nil {
			b.Fatal(err)
		}
	}
	return ids
}

func seedBenchFanout(b *testing.B, engine Engine, nodePrefix, edgePrefix string, degree int) NodeID {
	b.Helper()
	start := NodeID(nodePrefix + "start")
	if _, err := engine.CreateNode(&Node{ID: start, Labels: []string{"Benchmark"}}); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < degree; i++ {
		end := NodeID(nodePrefix + "end-" + itoaBench(i))
		if _, err := engine.CreateNode(&Node{ID: end, Labels: []string{"Benchmark"}}); err != nil {
			b.Fatal(err)
		}
		if err := engine.CreateEdge(&Edge{
			ID:        EdgeID(edgePrefix + itoaBench(i)),
			StartNode: start,
			EndNode:   end,
			Type:      "BENCH",
		}); err != nil {
			b.Fatal(err)
		}
	}
	return start
}

func BenchmarkPersistentBadgerEngine_BulkCreateNodes(b *testing.B) {
	engine := newPersistentBadgerBenchEngine(b)
	defer engine.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nodes := treeDBBenchBulkNodes("bench:badger-bulk-n", i*treeDBBulkBenchSize, treeDBBulkBenchSize)
		b.StartTimer()
		if err := engine.BulkCreateNodes(nodes); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPersistentBadgerEngine_CreateEdge(b *testing.B) {
	engine := newPersistentBadgerBenchEngine(b)
	defer engine.Close()

	if _, err := engine.CreateNode(&Node{ID: "bench:badger-start", Labels: []string{"Benchmark"}}); err != nil {
		b.Fatal(err)
	}
	if _, err := engine.CreateNode(&Node{ID: "bench:badger-end", Labels: []string{"Benchmark"}}); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := engine.CreateEdge(&Edge{
			ID:        EdgeID("bench:badger-e" + itoaBench(i)),
			StartNode: "bench:badger-start",
			EndNode:   "bench:badger-end",
			Type:      "BENCH",
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPersistentBadgerEngine_BulkCreateEdges(b *testing.B) {
	engine := newPersistentBadgerBenchEngine(b)
	defer engine.Close()

	if err := engine.BulkCreateNodes([]*Node{
		{ID: "bench:badger-bulk-start", Labels: []string{"Benchmark"}},
		{ID: "bench:badger-bulk-end", Labels: []string{"Benchmark"}},
	}); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		edges := treeDBBenchBulkEdges("bench:badger-bulk-e", i*treeDBBulkBenchSize, treeDBBulkBenchSize, "bench:badger-bulk-start", "bench:badger-bulk-end")
		b.StartTimer()
		if err := engine.BulkCreateEdges(edges); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPersistentBadgerEngine_TxnCreateNode(b *testing.B) {
	engine := newPersistentBadgerBenchEngine(b)
	defer engine.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, err := engine.BeginGraphTransaction()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := tx.CreateNode(&Node{
			ID:         NodeID("bench:badger-txn-n" + itoaBench(i)),
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

func BenchmarkPersistentBadgerEngine_GetNodesByLabel(b *testing.B) {
	engine := newPersistentBadgerBenchEngine(b)
	defer engine.Close()
	seedBenchNodes(b, engine, "bench:badger-label-n", storageBackendBenchReadNodes)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodes, err := engine.GetNodesByLabel("Benchmark")
		if err != nil {
			b.Fatal(err)
		}
		if len(nodes) != storageBackendBenchReadNodes {
			b.Fatalf("expected %d nodes, got %d", storageBackendBenchReadNodes, len(nodes))
		}
	}
}

func BenchmarkPersistentBadgerEngine_BatchGetNodes(b *testing.B) {
	engine := newPersistentBadgerBenchEngine(b)
	defer engine.Close()
	ids := seedBenchNodes(b, engine, "bench:badger-batch-n", storageBackendBenchReadNodes)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodes, err := engine.BatchGetNodes(ids)
		if err != nil {
			b.Fatal(err)
		}
		if len(nodes) != len(ids) {
			b.Fatalf("expected %d nodes, got %d", len(ids), len(nodes))
		}
	}
}

func BenchmarkPersistentBadgerEngine_GetOutgoingEdges(b *testing.B) {
	engine := newPersistentBadgerBenchEngine(b)
	defer engine.Close()
	start := seedBenchFanout(b, engine, "bench:badger-adj-", "bench:badger-adj-e", storageBackendBenchDegree)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		edges, err := engine.GetOutgoingEdges(start)
		if err != nil {
			b.Fatal(err)
		}
		if len(edges) != storageBackendBenchDegree {
			b.Fatalf("expected %d edges, got %d", storageBackendBenchDegree, len(edges))
		}
	}
}

func BenchmarkNamespacedPersistentBadgerEngine_CreateNode(b *testing.B) {
	inner := newPersistentBadgerBenchEngine(b)
	defer inner.Close()
	engine := NewNamespacedEngine(inner, "nornic")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := engine.CreateNode(&Node{
			ID:         NodeID("badger-n" + itoaBench(i)),
			Labels:     []string{"Benchmark"},
			Properties: map[string]any{"index": i},
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNamespacedPersistentBadgerEngine_BulkCreateNodes(b *testing.B) {
	inner := newPersistentBadgerBenchEngine(b)
	defer inner.Close()
	engine := NewNamespacedEngine(inner, "nornic")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		nodes := treeDBBenchBulkNodes("badger-bulk-n", i*treeDBBulkBenchSize, treeDBBulkBenchSize)
		b.StartTimer()
		if err := engine.BulkCreateNodes(nodes); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNamespacedPersistentBadgerEngine_GetNode(b *testing.B) {
	inner := newPersistentBadgerBenchEngine(b)
	defer inner.Close()
	engine := NewNamespacedEngine(inner, "nornic")
	seedBenchNodes(b, engine, "badger-get-n", storageBackendBenchReadNodes)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := engine.GetNode(NodeID("badger-get-n" + itoaBench(i%storageBackendBenchReadNodes))); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNamespacedPersistentBadgerEngine_CreateEdge(b *testing.B) {
	inner := newPersistentBadgerBenchEngine(b)
	defer inner.Close()
	engine := NewNamespacedEngine(inner, "nornic")

	if _, err := engine.CreateNode(&Node{ID: "badger-start", Labels: []string{"Benchmark"}}); err != nil {
		b.Fatal(err)
	}
	if _, err := engine.CreateNode(&Node{ID: "badger-end", Labels: []string{"Benchmark"}}); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := engine.CreateEdge(&Edge{
			ID:        EdgeID("badger-e" + itoaBench(i)),
			StartNode: "badger-start",
			EndNode:   "badger-end",
			Type:      "BENCH",
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNamespacedPersistentBadgerEngine_BulkCreateEdges(b *testing.B) {
	inner := newPersistentBadgerBenchEngine(b)
	defer inner.Close()
	engine := NewNamespacedEngine(inner, "nornic")

	if err := engine.BulkCreateNodes([]*Node{
		{ID: "badger-bulk-start", Labels: []string{"Benchmark"}},
		{ID: "badger-bulk-end", Labels: []string{"Benchmark"}},
	}); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		edges := treeDBBenchBulkEdges("badger-bulk-e", i*treeDBBulkBenchSize, treeDBBulkBenchSize, "badger-bulk-start", "badger-bulk-end")
		b.StartTimer()
		if err := engine.BulkCreateEdges(edges); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkNamespacedPersistentBadgerEngine_TxnCreateNode(b *testing.B) {
	inner := newPersistentBadgerBenchEngine(b)
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
			ID:         NodeID("badger-txn-n" + itoaBench(i)),
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

func BenchmarkNamespacedPersistentBadgerEngine_GetNodesByLabel(b *testing.B) {
	inner := newPersistentBadgerBenchEngine(b)
	defer inner.Close()
	engine := NewNamespacedEngine(inner, "nornic")
	seedBenchNodes(b, engine, "badger-label-n", storageBackendBenchReadNodes)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodes, err := engine.GetNodesByLabel("Benchmark")
		if err != nil {
			b.Fatal(err)
		}
		if len(nodes) != storageBackendBenchReadNodes {
			b.Fatalf("expected %d nodes, got %d", storageBackendBenchReadNodes, len(nodes))
		}
	}
}

func BenchmarkNamespacedPersistentBadgerEngine_GetOutgoingEdges(b *testing.B) {
	inner := newPersistentBadgerBenchEngine(b)
	defer inner.Close()
	engine := NewNamespacedEngine(inner, "nornic")
	start := seedBenchFanout(b, engine, "badger-adj-", "badger-adj-e", storageBackendBenchDegree)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		edges, err := engine.GetOutgoingEdges(start)
		if err != nil {
			b.Fatal(err)
		}
		if len(edges) != storageBackendBenchDegree {
			b.Fatalf("expected %d edges, got %d", storageBackendBenchDegree, len(edges))
		}
	}
}

func BenchmarkTreeDBEngine_GetNodesByLabel(b *testing.B) {
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()
	seedBenchNodes(b, engine, "bench:tree-label-n", storageBackendBenchReadNodes)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodes, err := engine.GetNodesByLabel("Benchmark")
		if err != nil {
			b.Fatal(err)
		}
		if len(nodes) != storageBackendBenchReadNodes {
			b.Fatalf("expected %d nodes, got %d", storageBackendBenchReadNodes, len(nodes))
		}
	}
}

func BenchmarkTreeDBEngine_BatchGetNodes(b *testing.B) {
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()
	ids := seedBenchNodes(b, engine, "bench:tree-batch-n", storageBackendBenchReadNodes)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodes, err := engine.BatchGetNodes(ids)
		if err != nil {
			b.Fatal(err)
		}
		if len(nodes) != len(ids) {
			b.Fatalf("expected %d nodes, got %d", len(ids), len(nodes))
		}
	}
}

func BenchmarkTreeDBEngine_GetOutgoingEdges(b *testing.B) {
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()
	start := seedBenchFanout(b, engine, "bench:tree-adj-", "bench:tree-adj-e", storageBackendBenchDegree)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		edges, err := engine.GetOutgoingEdges(start)
		if err != nil {
			b.Fatal(err)
		}
		if len(edges) != storageBackendBenchDegree {
			b.Fatalf("expected %d edges, got %d", storageBackendBenchDegree, len(edges))
		}
	}
}

func BenchmarkNamespacedTreeDBEngine_GetNodesByLabel(b *testing.B) {
	inner, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer inner.Close()
	engine := NewNamespacedEngine(inner, "nornic")
	seedBenchNodes(b, engine, "tree-label-n", storageBackendBenchReadNodes)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodes, err := engine.GetNodesByLabel("Benchmark")
		if err != nil {
			b.Fatal(err)
		}
		if len(nodes) != storageBackendBenchReadNodes {
			b.Fatalf("expected %d nodes, got %d", storageBackendBenchReadNodes, len(nodes))
		}
	}
}

func BenchmarkNamespacedTreeDBEngine_GetOutgoingEdges(b *testing.B) {
	inner, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: b.TempDir()})
	if err != nil {
		b.Fatal(err)
	}
	defer inner.Close()
	engine := NewNamespacedEngine(inner, "nornic")
	start := seedBenchFanout(b, engine, "tree-adj-", "tree-adj-e", storageBackendBenchDegree)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		edges, err := engine.GetOutgoingEdges(start)
		if err != nil {
			b.Fatal(err)
		}
		if len(edges) != storageBackendBenchDegree {
			b.Fatalf("expected %d edges, got %d", storageBackendBenchDegree, len(edges))
		}
	}
}

func BenchmarkDirectTreeDB_GraphGetNodeEquivalent(b *testing.B) {
	db := newDirectTreeDBBenchDB(b)
	defer db.Close()
	ids := directTreeDBSeedNodes(b, db, "bench:direct-get-n", storageBackendBenchReadNodes)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node, err := directTreeDBGetNode(db, ids[i%len(ids)])
		if err != nil {
			b.Fatal(err)
		}
		if node == nil {
			b.Fatal("expected benchmark node")
		}
	}
}

func BenchmarkDirectTreeDB_GraphCreateEdgeEquivalent(b *testing.B) {
	db := newDirectTreeDBBenchDB(b)
	defer db.Close()
	directTreeDBPutNode(b, db, &Node{ID: "bench:direct-edge-start", Labels: []string{"Benchmark"}}, 1)
	directTreeDBPutNode(b, db, &Node{ID: "bench:direct-edge-end", Labels: []string{"Benchmark"}}, 2)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		edge := &Edge{
			ID:        EdgeID("bench:direct-e" + itoaBench(i)),
			StartNode: "bench:direct-edge-start",
			EndNode:   "bench:direct-edge-end",
			Type:      "BENCH",
		}
		if err := directTreeDBPutEdge(db, edge, uint64(i+1)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDirectTreeDB_GraphBulkCreateEdgesEquivalent(b *testing.B) {
	db := newDirectTreeDBBenchDB(b)
	defer db.Close()
	directTreeDBPutNode(b, db, &Node{ID: "bench:direct-bulk-start", Labels: []string{"Benchmark"}}, 1)
	directTreeDBPutNode(b, db, &Node{ID: "bench:direct-bulk-end", Labels: []string{"Benchmark"}}, 2)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		edges := treeDBBenchBulkEdges("bench:direct-bulk-e", i*treeDBBulkBenchSize, treeDBBulkBenchSize, "bench:direct-bulk-start", "bench:direct-bulk-end")
		b.StartTimer()
		if err := directTreeDBPutEdges(db, edges, uint64(i+1)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDirectTreeDB_GraphGetNodesByLabelEquivalent(b *testing.B) {
	db := newDirectTreeDBBenchDB(b)
	defer db.Close()
	directTreeDBSeedNodes(b, db, "bench:direct-label-n", storageBackendBenchReadNodes)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodes, err := directTreeDBGetNodesByLabel(db, "Benchmark")
		if err != nil {
			b.Fatal(err)
		}
		if len(nodes) != storageBackendBenchReadNodes {
			b.Fatalf("expected %d nodes, got %d", storageBackendBenchReadNodes, len(nodes))
		}
	}
}

func BenchmarkDirectTreeDB_GraphGetOutgoingEdgesEquivalent(b *testing.B) {
	db := newDirectTreeDBBenchDB(b)
	defer db.Close()
	start := NodeID("bench:direct-adj-start")
	directTreeDBPutNode(b, db, &Node{ID: start, Labels: []string{"Benchmark"}}, 1)
	for i := 0; i < storageBackendBenchDegree; i++ {
		end := NodeID("bench:direct-adj-end-" + itoaBench(i))
		directTreeDBPutNode(b, db, &Node{ID: end, Labels: []string{"Benchmark"}}, uint64(i+2))
		if err := directTreeDBPutEdge(db, &Edge{
			ID:        EdgeID("bench:direct-adj-e" + itoaBench(i)),
			StartNode: start,
			EndNode:   end,
			Type:      "BENCH",
		}, uint64(i+1)); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		edges, err := directTreeDBGetOutgoingEdges(db, start)
		if err != nil {
			b.Fatal(err)
		}
		if len(edges) != storageBackendBenchDegree {
			b.Fatalf("expected %d edges, got %d", storageBackendBenchDegree, len(edges))
		}
	}
}

func directTreeDBSeedNodes(b *testing.B, db *treedb.DB, prefix string, count int) []NodeID {
	b.Helper()
	ids := make([]NodeID, count)
	for i := 0; i < count; i++ {
		id := NodeID(prefix + itoaBench(i))
		ids[i] = id
		directTreeDBPutNode(b, db, &Node{
			ID:         id,
			Labels:     []string{"Benchmark"},
			Properties: map[string]any{"index": i},
		}, uint64(i+1))
	}
	return ids
}

func directTreeDBPutNode(b *testing.B, db *treedb.DB, node *Node, seq uint64) {
	b.Helper()
	tx, err := newDirectTreeDBBenchTxn(db, 1, 3)
	if err != nil {
		b.Fatal(err)
	}
	exists, err := tx.has(nodeKey(node.ID))
	if err != nil {
		tx.close()
		b.Fatal(err)
	}
	if exists {
		tx.close()
		b.Fatalf("duplicate benchmark node %q", node.ID)
	}
	data, err := serializeNode(node)
	if err != nil {
		tx.close()
		b.Fatal(err)
	}
	var guardValue [8]byte
	binary.LittleEndian.PutUint64(guardValue[:], seq)
	if err := tx.set(nodeKey(node.ID), data); err != nil {
		tx.close()
		b.Fatal(err)
	}
	if err := tx.set(treeDBLabelIndexKey("Benchmark", node.ID), treeDBEmptyValue); err != nil {
		tx.close()
		b.Fatal(err)
	}
	if err := tx.set(treeDBNodeNamespaceGuardKey("bench"), guardValue[:]); err != nil {
		tx.close()
		b.Fatal(err)
	}
	if err := tx.commit(); err != nil {
		b.Fatal(err)
	}
}

func directTreeDBPutEdge(db *treedb.DB, edge *Edge, seq uint64) error {
	return directTreeDBPutEdges(db, []*Edge{edge}, seq)
}

func directTreeDBPutEdges(db *treedb.DB, edges []*Edge, seq uint64) error {
	writeCount := 0
	for _, edge := range edges {
		writeCount += treeDBEdgeCreateWriteCount(edge)
	}
	tx, err := newDirectTreeDBBenchTxn(db, len(edges), writeCount)
	if err != nil {
		return err
	}
	var guardValue [8]byte
	binary.LittleEndian.PutUint64(guardValue[:], seq)

	for _, edge := range edges {
		exists, err := tx.has(edgeKey(edge.ID))
		if err != nil {
			tx.close()
			return err
		}
		if exists {
			tx.close()
			return ErrAlreadyExists
		}
		data, err := serializeEdge(edge)
		if err != nil {
			tx.close()
			return err
		}
		if err := tx.set(edgeKey(edge.ID), data); err != nil {
			tx.close()
			return err
		}
		if err := tx.set(treeDBOutgoingIndexKey(edge.StartNode, edge.ID), treeDBEmptyValue); err != nil {
			tx.close()
			return err
		}
		if err := tx.set(treeDBIncomingIndexKey(edge.EndNode, edge.ID), treeDBEmptyValue); err != nil {
			tx.close()
			return err
		}
		if err := tx.set(treeDBEdgeTypeIndexKey(edge.Type, edge.ID), treeDBEmptyValue); err != nil {
			tx.close()
			return err
		}
		if err := tx.set(treeDBEdgeBetweenIndexKey(edge.StartNode, edge.EndNode, edge.Type, edge.ID), treeDBEmptyValue); err != nil {
			tx.close()
			return err
		}
		if err := tx.set(treeDBEdgeBetweenHeadKey(edge.StartNode, edge.EndNode, edge.Type), []byte(edge.ID)); err != nil {
			tx.close()
			return err
		}
		if err := tx.set(treeDBNodeEdgeGuardKey(edge.StartNode), guardValue[:]); err != nil {
			tx.close()
			return err
		}
		if edge.EndNode != edge.StartNode {
			if err := tx.set(treeDBNodeEdgeGuardKey(edge.EndNode), guardValue[:]); err != nil {
				tx.close()
				return err
			}
		}
		if err := tx.set(treeDBEdgeNamespaceGuardKey("bench"), guardValue[:]); err != nil {
			tx.close()
			return err
		}
	}
	return tx.commit()
}

func directTreeDBGetNode(db *treedb.DB, id NodeID) (*Node, error) {
	data, _, err := db.GetVersionedAppend(nodeKey(id), nil)
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	return deserializeNode(data)
}

func directTreeDBGetEdge(db *treedb.DB, id EdgeID) (*Edge, error) {
	data, _, err := db.GetVersionedAppend(edgeKey(id), nil)
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	return deserializeEdge(data)
}

func directTreeDBGetNodesByLabel(db *treedb.DB, label string) ([]*Node, error) {
	prefix := treeDBLabelIndexPrefix(label)
	it, err := db.Iterator(prefix, treeDBRangeEnd(prefix))
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	defer it.Close()

	nodes := make([]*Node, 0, storageBackendBenchReadNodes)
	for ; it.Valid(); it.Next() {
		node, err := directTreeDBGetNode(db, NodeID(string(it.Key()[len(prefix):])))
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, mapTreeDBError(it.Error())
}

func directTreeDBGetOutgoingEdges(db *treedb.DB, nodeID NodeID) ([]*Edge, error) {
	prefix := treeDBOutgoingIndexPrefix(nodeID)
	it, err := db.Iterator(prefix, treeDBRangeEnd(prefix))
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	defer it.Close()

	edges := make([]*Edge, 0, storageBackendBenchDegree)
	for ; it.Valid(); it.Next() {
		edge, err := directTreeDBGetEdge(db, EdgeID(string(it.Key()[len(prefix):])))
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}
	return edges, mapTreeDBError(it.Error())
}
