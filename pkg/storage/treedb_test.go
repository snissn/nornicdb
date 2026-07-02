package storage

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestTreeDBEngine(t *testing.T) *TreeDBEngine {
	t.Helper()
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, engine.Close())
	})
	return engine
}

func TestTreeDBEngine_HotBodyCacheLifecycle(t *testing.T) {
	engine := newTestTreeDBEngine(t)

	_, err := engine.CreateNode(&Node{
		ID:         "test:cache-a",
		Labels:     []string{"Cached"},
		Properties: map[string]any{"name": "a"},
	})
	require.NoError(t, err)

	got, err := engine.GetNode("test:cache-a")
	require.NoError(t, err)
	got.Labels[0] = "Mutated"
	got.Properties["name"] = "mutated"

	again, err := engine.GetNode("test:cache-a")
	require.NoError(t, err)
	require.Equal(t, []string{"Cached"}, again.Labels)
	require.Equal(t, "a", again.Properties["name"])

	again.Properties["name"] = "b"
	require.NoError(t, engine.UpdateNode(again))
	updated, err := engine.GetNode("test:cache-a")
	require.NoError(t, err)
	require.Equal(t, "b", updated.Properties["name"])

	require.NoError(t, engine.DeleteNode("test:cache-a"))
	_, err = engine.GetNode("test:cache-a")
	require.ErrorIs(t, err, ErrNotFound)
	engine.nodeCacheMu.RLock()
	_, cachedNode := engine.nodeCache["test:cache-a"]
	engine.nodeCacheMu.RUnlock()
	require.False(t, cachedNode)

	_, err = engine.CreateNode(&Node{ID: "test:cache-start", Labels: []string{"Cached"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:cache-end", Labels: []string{"Cached"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{
		ID:         "test:cache-edge",
		StartNode:  "test:cache-start",
		EndNode:    "test:cache-end",
		Type:       "REL",
		Properties: map[string]any{"kind": "cached"},
	}))

	edge, err := engine.GetEdge("test:cache-edge")
	require.NoError(t, err)
	edge.Type = "MUTATED"
	edge.Properties["kind"] = "mutated"
	edgeAgain, err := engine.GetEdge("test:cache-edge")
	require.NoError(t, err)
	require.Equal(t, "REL", edgeAgain.Type)
	require.Equal(t, "cached", edgeAgain.Properties["kind"])

	require.NoError(t, engine.DeleteEdge("test:cache-edge"))
	_, err = engine.GetEdge("test:cache-edge")
	require.ErrorIs(t, err, ErrNotFound)
	engine.edgeCacheMu.RLock()
	_, cachedEdge := engine.edgeCache["test:cache-edge"]
	engine.edgeCacheMu.RUnlock()
	require.False(t, cachedEdge)
}

func TestTreeDBEngine_CRUDIndexesRevisionsAndReopen(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: dir})
	require.NoError(t, err)

	a := &Node{
		ID:         "test:a",
		Labels:     []string{"Person", "Employee"},
		Properties: map[string]any{"name": "Alice"},
	}
	b := &Node{
		ID:         "test:b",
		Labels:     []string{"Person"},
		Properties: map[string]any{"name": "Bob"},
	}
	require.Equal(t, NodeID("test:a"), mustCreateTreeDBNode(t, engine, a))
	require.Equal(t, NodeID("test:b"), mustCreateTreeDBNode(t, engine, b))

	edge := &Edge{
		ID:         "test:e1",
		StartNode:  "test:a",
		EndNode:    "test:b",
		Type:       "KNOWS",
		Properties: map[string]any{"since": int64(2024)},
	}
	require.NoError(t, engine.CreateEdge(edge))

	gotA, err := engine.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, "Alice", gotA.Properties["name"])

	batch, err := engine.BatchGetNodes([]NodeID{"test:a", "test:b"})
	require.NoError(t, err)
	require.Len(t, batch, 2)

	people, err := engine.GetNodesByLabel("person")
	require.NoError(t, err)
	require.ElementsMatch(t, []NodeID{"test:a", "test:b"}, treeDBNodeIDs(people))

	outgoing, err := engine.GetOutgoingEdges("test:a")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e1"}, treeDBEdgeIDs(outgoing))
	incoming, err := engine.GetIncomingEdges("test:b")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e1"}, treeDBEdgeIDs(incoming))
	byType, err := engine.GetEdgesByType("knows")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e1"}, treeDBEdgeIDs(byType))
	between, err := engine.GetEdgesBetween("test:a", "test:b")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:e1"}, treeDBEdgeIDs(between))
	require.Equal(t, EdgeID("test:e1"), engine.GetEdgeBetween("test:a", "test:b", "KNOWS").ID)

	nodeCount, err := engine.NodeCount()
	require.NoError(t, err)
	require.Equal(t, int64(2), nodeCount)
	edgeCount, err := engine.EdgeCount()
	require.NoError(t, err)
	require.Equal(t, int64(1), edgeCount)
	testNodeCount, err := engine.NodeCountByPrefix("test:")
	require.NoError(t, err)
	require.Equal(t, int64(2), testNodeCount)
	testEdgeCount, err := engine.EdgeCountByPrefix("test:")
	require.NoError(t, err)
	require.Equal(t, int64(1), testEdgeCount)
	personCount, err := engine.NodeCountByLabelInNamespace("test", "Person")
	require.NoError(t, err)
	require.Equal(t, int64(2), personCount)
	require.Contains(t, engine.ListNamespaces(), "test")

	rev1, err := engine.GetNodeEntryRevision("test:a")
	require.NoError(t, err)
	require.NotEqual(t, treedb.LegacyEntryRevision, rev1)
	gotA.Properties["name"] = "Alice Cooper"
	require.NoError(t, engine.UpdateNode(gotA))
	rev2, err := engine.GetNodeEntryRevision("test:a")
	require.NoError(t, err)
	require.Greater(t, uint64(rev2), uint64(rev1))

	require.NoError(t, engine.Sync())
	require.NoError(t, engine.Close())

	reopened, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: dir})
	require.NoError(t, err)
	defer reopened.Close()

	reopenedA, err := reopened.GetNode("test:a")
	require.NoError(t, err)
	require.Equal(t, "Alice Cooper", reopenedA.Properties["name"])
	reopenedEdge, err := reopened.GetEdge("test:e1")
	require.NoError(t, err)
	require.Equal(t, EdgeID("test:e1"), reopenedEdge.ID)
	reopenedCount, err := reopened.NodeCountByPrefix("test:")
	require.NoError(t, err)
	require.Equal(t, int64(2), reopenedCount)
}

func TestTreeDBTransaction_ReadYourWritesAndConflict(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	_, err := engine.CreateNode(&Node{
		ID:         "test:conflict",
		Labels:     []string{"Conflict"},
		Properties: map[string]any{"version": int64(1)},
	})
	require.NoError(t, err)

	rawTx1, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx1 := rawTx1.(*TreeDBTransaction)
	defer tx1.Rollback()

	_, err = tx1.CreateNode(&Node{
		ID:         "test:pending",
		Labels:     []string{"Pending"},
		Properties: map[string]any{"created": true},
	})
	require.NoError(t, err)
	pending, err := tx1.GetNodesByLabel("pending")
	require.NoError(t, err)
	require.ElementsMatch(t, []NodeID{"test:pending"}, treeDBNodeIDs(pending))

	staged, err := tx1.GetNode("test:conflict")
	require.NoError(t, err)
	staged.Properties["version"] = int64(2)
	require.NoError(t, tx1.UpdateNode(staged))

	rawTx2, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx2 := rawTx2.(*TreeDBTransaction)
	concurrent, err := tx2.GetNode("test:conflict")
	require.NoError(t, err)
	concurrent.Properties["version"] = int64(3)
	require.NoError(t, tx2.UpdateNode(concurrent))
	require.NoError(t, tx2.Commit())

	err = tx1.Commit()
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrConflict), "commit error = %v", err)
	_, err = engine.GetNode("test:pending")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestTreeDBTransaction_LabelScanRepeatabilityWithinSnapshot(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:user-0", Labels: []string{"User"}},
		{ID: "test:user-1", Labels: []string{"User"}},
		{ID: "test:admin-0", Labels: []string{"Admin"}},
	}))

	rawScan, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	txScan := rawScan.(*TreeDBTransaction)
	defer txScan.Rollback()
	require.NoError(t, txScan.SetNamespace("test"))

	before, err := txScan.GetNodesByLabel("User")
	require.NoError(t, err)
	beforeIDs := treeDBNodeIDs(before)
	require.ElementsMatch(t, []NodeID{"test:user-0", "test:user-1"}, beforeIDs)

	_, err = engine.CreateNode(&Node{ID: "test:user-new", Labels: []string{"User"}})
	require.NoError(t, err)

	after, err := txScan.GetNodesByLabel("User")
	require.NoError(t, err)
	require.ElementsMatch(t, beforeIDs, treeDBNodeIDs(after))

	rawFresh, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	txFresh := rawFresh.(*TreeDBTransaction)
	defer txFresh.Rollback()
	require.NoError(t, txFresh.SetNamespace("test"))
	fresh, err := txFresh.GetNodesByLabel("User")
	require.NoError(t, err)
	require.ElementsMatch(t, []NodeID{"test:user-0", "test:user-1", "test:user-new"}, treeDBNodeIDs(fresh))
}

func TestTreeDBTransaction_AllNodesRepeatabilityWithinSnapshot(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:all-0", Labels: []string{"Node"}},
		{ID: "test:all-1", Labels: []string{"Node"}},
	}))

	rawScan, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	txScan := rawScan.(*TreeDBTransaction)
	defer txScan.Rollback()
	require.NoError(t, txScan.SetNamespace("test"))

	before, err := txScan.AllNodes()
	require.NoError(t, err)
	beforeIDs := treeDBNodeIDs(before)
	require.ElementsMatch(t, []NodeID{"test:all-0", "test:all-1"}, beforeIDs)

	_, err = engine.CreateNode(&Node{ID: "test:all-new", Labels: []string{"Node"}})
	require.NoError(t, err)

	after, err := txScan.AllNodes()
	require.NoError(t, err)
	require.ElementsMatch(t, beforeIDs, treeDBNodeIDs(after))
}

func TestTreeDBTransaction_UnscopedRangeScansFailClosed(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:unscoped-a", Labels: []string{"User"}},
		{ID: "test:unscoped-b", Labels: []string{"User"}},
	}))
	require.NoError(t, engine.CreateEdge(&Edge{
		ID:        "test:unscoped-e",
		StartNode: "test:unscoped-a",
		EndNode:   "test:unscoped-b",
		Type:      "REL",
	}))

	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	defer tx.Rollback()

	_, err = tx.GetNodesByLabel("User")
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = tx.AllNodes()
	require.ErrorIs(t, err, ErrNotImplemented)
	_, err = tx.GetEdgesByType("REL")
	require.ErrorIs(t, err, ErrNotImplemented)
	_, _, err = engine.DeleteByPrefix("")
	require.ErrorIs(t, err, ErrNotImplemented)
}

func TestTreeDBTransaction_EdgeTraversalRepeatabilityAcrossConcurrentDelete(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:edge-start", Labels: []string{"Node"}},
		{ID: "test:edge-target", Labels: []string{"Node"}, Properties: map[string]any{"name": "target"}},
	}))
	require.NoError(t, engine.CreateEdge(&Edge{
		ID:        "test:edge-link",
		StartNode: "test:edge-start",
		EndNode:   "test:edge-target",
		Type:      "LINKS",
	}))

	rawRead, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	txRead := rawRead.(*TreeDBTransaction)
	defer txRead.Rollback()
	require.NoError(t, txRead.SetNamespace("test"))

	before, err := txRead.GetEdgesBetween("test:edge-start", "test:edge-target")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:edge-link"}, treeDBEdgeIDs(before))
	target, err := txRead.GetNode("test:edge-target")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:edge-target"), target.ID)

	require.NoError(t, engine.DeleteNode("test:edge-target"))

	after, err := txRead.GetEdgesBetween("test:edge-start", "test:edge-target")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:edge-link"}, treeDBEdgeIDs(after))
	target, err = txRead.GetNode("test:edge-target")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:edge-target"), target.ID)

	fresh, err := engine.GetEdgesBetween("test:edge-start", "test:edge-target")
	require.NoError(t, err)
	require.Empty(t, fresh)
	_, err = engine.GetNode("test:edge-target")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestTreeDBTransaction_SnapshotReadPreconditionConflictsOnCommit(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	_, err := engine.CreateNode(&Node{
		ID:         "test:snapshot-conflict",
		Labels:     []string{"User"},
		Properties: map[string]any{"version": int64(1)},
	})
	require.NoError(t, err)

	rawRead, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	txRead := rawRead.(*TreeDBTransaction)
	defer txRead.Rollback()
	require.NoError(t, txRead.SetNamespace("test"))

	before, err := txRead.GetNodesByLabel("User")
	require.NoError(t, err)
	require.ElementsMatch(t, []NodeID{"test:snapshot-conflict"}, treeDBNodeIDs(before))

	updated, err := engine.GetNode("test:snapshot-conflict")
	require.NoError(t, err)
	updated.Properties["version"] = int64(2)
	require.NoError(t, engine.UpdateNode(updated))

	stillOld, err := txRead.GetNode("test:snapshot-conflict")
	require.NoError(t, err)
	require.Equal(t, int64(1), stillOld.Properties["version"])

	err = txRead.Commit()
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrConflict), "commit error = %v", err)
}

func TestTreeDBTransaction_NamespacedLabelScanIgnoresForeignNamespaceConflicts(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	tenantA := NewNamespacedEngine(engine, "tenant_a")
	tenantB := NewNamespacedEngine(engine, "tenant_b")

	_, err := tenantA.CreateNode(&Node{
		ID:         "a-user",
		Labels:     []string{"User"},
		Properties: map[string]any{"version": int64(1)},
	})
	require.NoError(t, err)
	_, err = tenantB.CreateNode(&Node{
		ID:         "b-user",
		Labels:     []string{"User"},
		Properties: map[string]any{"version": int64(1)},
	})
	require.NoError(t, err)

	tx, err := tenantA.BeginGraphTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	nodes, err := tx.GetNodesByLabel("User")
	require.NoError(t, err)
	require.ElementsMatch(t, []NodeID{"a-user"}, treeDBNodeIDs(nodes))

	foreign, err := tenantB.GetNode("b-user")
	require.NoError(t, err)
	foreign.Properties["version"] = int64(2)
	require.NoError(t, tenantB.UpdateNode(foreign))

	_, err = tx.CreateNode(&Node{ID: "a-marker", Labels: []string{"Marker"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func TestTreeDBTransaction_LabelScanPhantomConflictsOnCommit(t *testing.T) {
	engine := newTestTreeDBEngine(t)

	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))

	nodes, err := tx.GetNodesByLabel("User")
	require.NoError(t, err)
	require.Empty(t, nodes)

	_, err = engine.CreateNode(&Node{ID: "test:user-new", Labels: []string{"User"}})
	require.NoError(t, err)

	_, err = tx.CreateNode(&Node{ID: "test:marker", Labels: []string{"Marker"}})
	require.NoError(t, err)
	err = tx.Commit()
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrConflict), "commit error = %v", err)
}

func TestTreeDBTransaction_EdgeBetweenPhantomConflictsOnCommit(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:between-a", Labels: []string{"Node"}},
		{ID: "test:between-b", Labels: []string{"Node"}},
	}))

	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("test"))

	edges, err := tx.GetEdgesBetween("test:between-a", "test:between-b")
	require.NoError(t, err)
	require.Empty(t, edges)

	require.NoError(t, engine.CreateEdge(&Edge{
		ID:        "test:between-e",
		StartNode: "test:between-a",
		EndNode:   "test:between-b",
		Type:      "LINKS",
	}))

	_, err = tx.CreateNode(&Node{ID: "test:between-marker", Labels: []string{"Marker"}})
	require.NoError(t, err)
	err = tx.Commit()
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrConflict), "commit error = %v", err)
}

func TestTreeDBEngine_GetEdgeBetweenHeadPromotesSurvivorOnDelete(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:head-a", Labels: []string{"Node"}},
		{ID: "test:head-b", Labels: []string{"Node"}},
	}))
	first := &Edge{ID: "test:head-e1", StartNode: "test:head-a", EndNode: "test:head-b", Type: "KNOWS"}
	second := &Edge{ID: "test:head-e2", StartNode: "test:head-a", EndNode: "test:head-b", Type: "KNOWS"}
	require.NoError(t, engine.CreateEdge(first))
	require.NoError(t, engine.CreateEdge(second))

	head, err := engine.db.GetAppend(treeDBEdgeBetweenHeadKey("test:head-a", "test:head-b", "KNOWS"), nil)
	require.NoError(t, err)
	require.Equal(t, []byte(second.ID), head)

	require.NoError(t, engine.DeleteEdge(second.ID))
	head, err = engine.db.GetAppend(treeDBEdgeBetweenHeadKey("test:head-a", "test:head-b", "KNOWS"), nil)
	require.NoError(t, err)
	require.Equal(t, []byte(first.ID), head)
	require.Equal(t, first.ID, engine.GetEdgeBetween("test:head-a", "test:head-b", "KNOWS").ID)
}

func TestTreeDBTransaction_CreateSkipOnlyBypassesUUIDIDs(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:existing", Labels: []string{"Entity"}})
	require.NoError(t, err)

	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	defer tx.Rollback()

	require.NoError(t, tx.SetSkipCreateExistenceCheck(true))
	_, err = tx.CreateNode(&Node{ID: "test:existing", Labels: []string{"Entity"}})
	require.ErrorIs(t, err, ErrAlreadyExists)

	_, err = tx.CreateNode(&Node{ID: "test:550e8400-e29b-41d4-a716-446655440000", Labels: []string{"Entity"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func TestTreeDBTransaction_DeleteNodeConflictsWithConcurrentEdgeCreate(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:guard-a", Labels: []string{"Guard"}},
		{ID: "test:guard-b", Labels: []string{"Guard"}},
	}))

	rawDelete, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	deleteTx := rawDelete.(*TreeDBTransaction)
	defer deleteTx.Rollback()
	require.NoError(t, deleteTx.DeleteNode("test:guard-a"))

	rawEdge, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	edgeTx := rawEdge.(*TreeDBTransaction)
	require.NoError(t, edgeTx.CreateEdge(&Edge{
		ID:        "test:guard-e",
		StartNode: "test:guard-a",
		EndNode:   "test:guard-b",
		Type:      "LINKS",
	}))
	require.NoError(t, edgeTx.Commit())

	err = deleteTx.Commit()
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrConflict), "commit error = %v", err)
	_, err = engine.GetNode("test:guard-a")
	require.NoError(t, err)
	_, err = engine.GetEdge("test:guard-e")
	require.NoError(t, err)
}

func TestTreeDBTransaction_UniqueConstraintSerializesCommitWindow(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.GetSchemaForNamespace("test").AddUniqueConstraint("unique_user_email", "User", "email"))

	rawTx1, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx1 := rawTx1.(*TreeDBTransaction)
	defer tx1.Rollback()
	_, err = tx1.CreateNode(&Node{
		ID:         "test:unique-1",
		Labels:     []string{"User"},
		Properties: map[string]any{"email": "same@example.com"},
	})
	require.NoError(t, err)

	rawTx2, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx2 := rawTx2.(*TreeDBTransaction)
	defer tx2.Rollback()
	_, err = tx2.CreateNode(&Node{
		ID:         "test:unique-2",
		Labels:     []string{"User"},
		Properties: map[string]any{"email": "same@example.com"},
	})
	require.NoError(t, err)

	require.NoError(t, tx1.Commit())
	err = tx2.Commit()
	require.Error(t, err)
	require.Contains(t, err.Error(), "same@example.com")
}

func TestTreeDBTransaction_UnsupportedConstraintsFailClosed(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:       "user_email_exists",
		Type:       ConstraintExists,
		Label:      "User",
		Properties: []string{"email"},
	}))
	_, err := engine.CreateNode(&Node{
		ID:         "test:unsupported-user",
		Labels:     []string{"User"},
		Properties: map[string]any{"email": "present@example.com"},
	})
	require.ErrorIs(t, err, ErrNotImplemented)

	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	require.NoError(t, tx.SetDeferredConstraintValidation(true))
	_, err = tx.CreateNode(&Node{
		ID:         "test:unsupported-deferred-user",
		Labels:     []string{"User"},
		Properties: map[string]any{"email": "deferred@example.com"},
	})
	require.NoError(t, err)
	err = tx.Commit()
	require.ErrorIs(t, err, ErrNotImplemented)
	require.Contains(t, err.Error(), "constraint violation")
	require.False(t, tx.IsActive())
	require.NoError(t, tx.Rollback())

	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:unsupported-a", Labels: []string{"Endpoint"}},
		{ID: "test:unsupported-b", Labels: []string{"Endpoint"}},
	}))
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:       "rel_since_exists",
		Type:       ConstraintExists,
		EntityType: ConstraintEntityRelationship,
		Label:      "LINKS",
		Properties: []string{"since"},
	}))
	err = engine.CreateEdge(&Edge{
		ID:        "test:unsupported-e",
		StartNode: "test:unsupported-a",
		EndNode:   "test:unsupported-b",
		Type:      "LINKS",
	})
	require.ErrorIs(t, err, ErrNotImplemented)
}

func TestTreeDBTransaction_CompositeIndexUpdateRemovesOldEntries(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddCompositeIndex("person_location", "Person", []string{"country", "city"}))
	idx, ok := schema.GetCompositeIndex("person_location")
	require.True(t, ok)

	_, err := engine.CreateNode(&Node{
		ID:         "test:person-1",
		Labels:     []string{"Person"},
		Properties: map[string]any{"country": "US", "city": "NYC"},
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []NodeID{"test:person-1"}, idx.LookupFull("US", "NYC"))

	node, err := engine.GetNode("test:person-1")
	require.NoError(t, err)
	node.Properties["city"] = "Boston"
	require.NoError(t, engine.UpdateNode(node))

	require.Empty(t, idx.LookupFull("US", "NYC"))
	require.ElementsMatch(t, []NodeID{"test:person-1"}, idx.LookupFull("US", "Boston"))
}

func TestTreeDBTransaction_SchemaStateUsesNetNodeMutations(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddUniqueConstraint("unique_email", "User", "email"))
	require.NoError(t, schema.AddPropertyIndex("idx_name", "User", []string{"name"}))
	require.NoError(t, schema.AddCompositeIndex("idx_location", "User", []string{"country", "city"}))
	composite, ok := schema.GetCompositeIndex("idx_location")
	require.True(t, ok)

	rawTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx := rawTx.(*TreeDBTransaction)
	_, err = tx.CreateNode(&Node{
		ID:     "test:ephemeral-user",
		Labels: []string{"User"},
		Properties: map[string]any{
			"email":   "ephemeral-old@example.com",
			"name":    "Ephemeral Old",
			"country": "US",
			"city":    "SF",
		},
	})
	require.NoError(t, err)
	require.NoError(t, tx.UpdateNode(&Node{
		ID:     "test:ephemeral-user",
		Labels: []string{"User"},
		Properties: map[string]any{
			"email":   "ephemeral-new@example.com",
			"name":    "Ephemeral New",
			"country": "US",
			"city":    "NYC",
		},
	}))
	require.NoError(t, tx.DeleteNode("test:ephemeral-user"))
	require.NoError(t, tx.Commit())

	for _, email := range []string{"ephemeral-old@example.com", "ephemeral-new@example.com"} {
		_, found, constrained := schema.LookupUniqueConstraintValue("User", "email", email)
		require.True(t, constrained)
		require.False(t, found, "stale unique value %q remained registered", email)
	}
	require.Empty(t, schema.PropertyIndexLookup("User", "name", "Ephemeral Old"))
	require.Empty(t, schema.PropertyIndexLookup("User", "name", "Ephemeral New"))
	require.Empty(t, composite.LookupFull("US", "SF"))
	require.Empty(t, composite.LookupFull("US", "NYC"))
	_, err = engine.CreateNode(&Node{
		ID:         "test:reuse-ephemeral",
		Labels:     []string{"User"},
		Properties: map[string]any{"email": "ephemeral-new@example.com"},
	})
	require.NoError(t, err)

	_, err = engine.CreateNode(&Node{
		ID:     "test:persistent-user",
		Labels: []string{"User"},
		Properties: map[string]any{
			"email":   "old@example.com",
			"name":    "Old Name",
			"country": "US",
			"city":    "LA",
		},
	})
	require.NoError(t, err)
	rawTx, err = engine.BeginGraphTransaction()
	require.NoError(t, err)
	tx = rawTx.(*TreeDBTransaction)
	require.NoError(t, tx.UpdateNode(&Node{
		ID:     "test:persistent-user",
		Labels: []string{"User"},
		Properties: map[string]any{
			"email":   "mid@example.com",
			"name":    "Mid Name",
			"country": "US",
			"city":    "CHI",
		},
	}))
	require.NoError(t, tx.UpdateNode(&Node{
		ID:     "test:persistent-user",
		Labels: []string{"User"},
		Properties: map[string]any{
			"email":   "final@example.com",
			"name":    "Final Name",
			"country": "US",
			"city":    "SEA",
		},
	}))
	require.NoError(t, tx.Commit())

	for _, email := range []string{"old@example.com", "mid@example.com"} {
		_, found, constrained := schema.LookupUniqueConstraintValue("User", "email", email)
		require.True(t, constrained)
		require.False(t, found, "stale unique value %q remained registered", email)
	}
	nodeID, found, constrained := schema.LookupUniqueConstraintValue("User", "email", "final@example.com")
	require.True(t, constrained)
	require.True(t, found)
	require.Equal(t, NodeID("test:persistent-user"), nodeID)
	require.Empty(t, schema.PropertyIndexLookup("User", "name", "Old Name"))
	require.Empty(t, schema.PropertyIndexLookup("User", "name", "Mid Name"))
	require.ElementsMatch(t, []NodeID{"test:persistent-user"}, schema.PropertyIndexLookup("User", "name", "Final Name"))
	require.Empty(t, composite.LookupFull("US", "LA"))
	require.Empty(t, composite.LookupFull("US", "CHI"))
	require.ElementsMatch(t, []NodeID{"test:persistent-user"}, composite.LookupFull("US", "SEA"))
}

func TestTreeDBEngine_DeleteByPrefixCascades(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "tenant:a", Labels: []string{"Tenant"}},
		{ID: "tenant:b", Labels: []string{"Tenant"}},
	}))
	_, err := engine.CreateNode(&Node{ID: "other:c", Labels: []string{"Other"}})
	require.NoError(t, err)
	require.NoError(t, engine.CreateEdge(&Edge{ID: "tenant:e1", StartNode: "tenant:a", EndNode: "tenant:b", Type: "LINK"}))

	nodesDeleted, edgesDeleted, err := engine.DeleteByPrefix("tenant:")
	require.NoError(t, err)
	require.Equal(t, int64(2), nodesDeleted)
	require.Equal(t, int64(1), edgesDeleted)

	_, err = engine.GetNode("tenant:a")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetNode("tenant:b")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetEdge("tenant:e1")
	require.ErrorIs(t, err, ErrNotFound)
	other, err := engine.GetNode("other:c")
	require.NoError(t, err)
	require.Equal(t, NodeID("other:c"), other.ID)

	tenantNodes, err := engine.NodeCountByPrefix("tenant:")
	require.NoError(t, err)
	require.Equal(t, int64(0), tenantNodes)
}

func TestTreeDBEngine_PendingEmbeddingsIndex(t *testing.T) {
	engine := newTestTreeDBEngine(t)

	_, err := engine.CreateNode(&Node{ID: "test:skip", Labels: []string{"Doc"}, Properties: map[string]any{"text": "skip"}})
	require.NoError(t, err)
	require.Equal(t, 0, engine.PendingEmbeddingsCount())

	engine.SetEmbeddingsEnabled(true)
	skip, err := engine.GetNode("test:skip")
	require.NoError(t, err)
	skip.Properties["embedding_skipped"] = true
	require.NoError(t, engine.UpdateNode(skip))

	_, err = engine.CreateNode(&Node{ID: "test:embed", Labels: []string{"Doc"}, Properties: map[string]any{"text": "embed"}})
	require.NoError(t, err)
	require.Equal(t, 1, engine.PendingEmbeddingsCount())
	found := engine.FindNodeNeedingEmbedding()
	require.NotNil(t, found)
	require.Equal(t, NodeID("test:embed"), found.ID)

	engine.MarkNodeEmbedded("test:embed")
	require.Equal(t, 0, engine.PendingEmbeddingsCount())
	engine.AddToPendingEmbeddings("test:embed")
	require.Equal(t, 1, engine.PendingEmbeddingsCount())

	embedded, err := engine.GetNode("test:embed")
	require.NoError(t, err)
	embedded.ChunkEmbeddings = [][]float32{{1, 2, 3}}
	require.NoError(t, engine.UpdateNode(embedded))
	require.Equal(t, 0, engine.PendingEmbeddingsCount())

	cleared, err := engine.ClearAllEmbeddingsForPrefix("test:")
	require.NoError(t, err)
	require.Equal(t, 1, cleared)
	require.Equal(t, 1, engine.PendingEmbeddingsCount())
	found = engine.FindNodeNeedingEmbedding()
	require.NotNil(t, found)
	require.Equal(t, NodeID("test:embed"), found.ID)
}

func TestTreeDBEngine_EventCallbacks(t *testing.T) {
	engine := newTestTreeDBEngine(t)

	var nodeCreated atomic.Int32
	var nodeUpdated atomic.Int32
	var nodeDeleted atomic.Int32
	var edgeCreated atomic.Int32
	var edgeUpdated atomic.Int32
	var edgeDeleted atomic.Int32
	engine.OnNodeCreated(func(*Node) { nodeCreated.Add(1) })
	engine.OnNodeUpdated(func(*Node) { nodeUpdated.Add(1) })
	engine.OnNodeDeleted(func(NodeID) { nodeDeleted.Add(1) })
	engine.OnEdgeCreated(func(*Edge) { edgeCreated.Add(1) })
	engine.OnEdgeUpdated(func(*Edge) { edgeUpdated.Add(1) })
	engine.OnEdgeDeleted(func(EdgeID) { edgeDeleted.Add(1) })

	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:event-a", Labels: []string{"Event"}},
		{ID: "test:event-b", Labels: []string{"Event"}},
	}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:event-e", StartNode: "test:event-a", EndNode: "test:event-b", Type: "EVENT"}))
	updatedNode, err := engine.GetNode("test:event-a")
	require.NoError(t, err)
	updatedNode.Properties = map[string]any{"updated": true}
	require.NoError(t, engine.UpdateNode(updatedNode))
	updatedEdge, err := engine.GetEdge("test:event-e")
	require.NoError(t, err)
	updatedEdge.Properties = map[string]any{"updated": true}
	require.NoError(t, engine.UpdateEdge(updatedEdge))
	require.NoError(t, engine.DeleteNode("test:event-a"))

	assert.Equal(t, int32(2), nodeCreated.Load())
	assert.Equal(t, int32(1), nodeUpdated.Load())
	assert.Equal(t, int32(1), nodeDeleted.Load())
	assert.Equal(t, int32(1), edgeCreated.Load())
	assert.Equal(t, int32(1), edgeUpdated.Load())
	assert.Equal(t, int32(1), edgeDeleted.Load())
}

func TestTreeDBEngine_SchemaPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: dir})
	require.NoError(t, err)

	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddUniqueConstraint("unique_email", "User", "email"))
	_, err = engine.CreateNode(&Node{
		ID:         "test:user-1",
		Labels:     []string{"User"},
		Properties: map[string]any{"email": "alice@example.com"},
	})
	require.NoError(t, err)
	require.NoError(t, engine.Sync())
	require.NoError(t, engine.Close())

	reopened, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: dir})
	require.NoError(t, err)
	defer reopened.Close()

	reopenedSchema := reopened.GetSchemaForNamespace("test")
	nodeID, found, constrained := reopenedSchema.LookupUniqueConstraintValue("User", "email", "alice@example.com")
	require.True(t, constrained)
	require.True(t, found)
	require.Equal(t, NodeID("test:user-1"), nodeID)
}

func TestTreeDBEngine_StreamNodesByPrefixStops(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:stream-a", Labels: []string{"Stream"}},
		{ID: "test:stream-b", Labels: []string{"Stream"}},
	}))
	_, err := engine.CreateNode(&Node{ID: "other:stream-c", Labels: []string{"Stream"}})
	require.NoError(t, err)

	seen := 0
	err = engine.StreamNodesByPrefix(context.Background(), "test:", func(*Node) error {
		seen++
		return ErrIterationStopped
	})
	require.NoError(t, err)
	require.Equal(t, 1, seen)
}

func mustCreateTreeDBNode(t *testing.T, engine *TreeDBEngine, node *Node) NodeID {
	t.Helper()
	id, err := engine.CreateNode(node)
	require.NoError(t, err)
	return id
}

func treeDBNodeIDs(nodes []*Node) []NodeID {
	ids := make([]NodeID, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.ID)
	}
	return ids
}

func treeDBEdgeIDs(edges []*Edge) []EdgeID {
	ids := make([]EdgeID, 0, len(edges))
	for _, edge := range edges {
		ids = append(ids, edge.ID)
	}
	return ids
}
