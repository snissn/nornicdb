package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBadgerTransaction_PendingCreateNodeOperationIndexLocked(t *testing.T) {
	engine := newTestEngine(t)
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()

	tx.operations = []Operation{
		{Type: OpCreateNode, NodeID: "test:n1"},
		{Type: OpUpdateNode, NodeID: "test:n1"},
	}
	require.Equal(t, 0, tx.pendingCreateNodeOperationIndexLocked("test:n1"))

	tx.operations = append(tx.operations, Operation{Type: OpDeleteNode, NodeID: "test:n1"})
	require.Equal(t, -1, tx.pendingCreateNodeOperationIndexLocked("test:n1"))
	require.Equal(t, -1, tx.pendingCreateNodeOperationIndexLocked("test:missing"))
}

func TestBadgerTransaction_ConflictHelpers(t *testing.T) {
	engine := newTestEngine(t)

	// Seed a visible node/edge with current heads.
	_, err := engine.CreateNode(&Node{ID: "test:a", Labels: []string{"User"}, Properties: map[string]any{"email": "a@example.com"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&Node{ID: "test:b", Labels: []string{"User"}, Properties: map[string]any{"email": "b@example.com"}})
	require.NoError(t, err)
	err = engine.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:a", EndNode: "test:b", Type: "REL"})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	// Old readTS to force SI conflicts against current heads.
	tx.readTS = MVCCVersion{CommitTimestamp: time.Unix(0, 0).UTC(), CommitSequence: 0}

	err = tx.checkNodeCreateConflict("test:a")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrConflict)

	err = tx.checkEdgeCreateConflict("test:e1")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrConflict)

	err = tx.checkEdgeWriteConflict("test:e1")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrConflict)

	// Endpoint deleted-after-start branch -> conflict.
	_, err = tx.CreateNode(&Node{ID: "test:c", Labels: []string{"User"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx2, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx2.SetNamespace("test"))
	tx2.readTS = MVCCVersion{CommitTimestamp: time.Unix(0, 0).UTC(), CommitSequence: 0}
	defer tx2.Rollback()

	// Tombstone endpoint after tx2 readTS.
	require.NoError(t, engine.DeleteNode("test:c"))
	err = tx2.checkEdgeEndpointConflicts(&Edge{ID: "test:ec", StartNode: "test:c", EndNode: "test:b", Type: "REL"})
	require.Error(t, err)
}

func TestBadgerTransaction_GetNodesByLabelLocked_Branches(t *testing.T) {
	engine := newTestEngine(t)
	_, err := engine.CreateNode(&Node{ID: "test:u1", Labels: []string{"User"}})
	require.NoError(t, err)

	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	// readTS == zero branch.
	nodes, err := tx.getNodesByLabelLocked("User")
	require.NoError(t, err)
	require.NotEmpty(t, nodes)

	// readTS != zero branch.
	tx.readTS = MVCCVersion{CommitTimestamp: time.Now().UTC(), CommitSequence: 1}
	nodes, err = tx.getNodesByLabelLocked("User")
	require.NoError(t, err)
	require.NotEmpty(t, nodes)
}
