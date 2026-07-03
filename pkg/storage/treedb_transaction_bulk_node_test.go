package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTreeDBTransaction_BulkCreateNodesStagesAndCommits(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	tx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	bulk, ok := tx.(interface{ BulkCreateNodes([]*Node) error })
	require.True(t, ok)
	require.NoError(t, bulk.BulkCreateNodes([]*Node{
		{ID: "test:bulk-node-1", Labels: []string{"BulkNode"}, Properties: map[string]any{"name": "a"}},
		{ID: "test:bulk-node-2", Labels: []string{"BulkNode"}, Properties: map[string]any{"name": "b"}},
	}))

	pending, err := tx.GetNode("test:bulk-node-1")
	require.NoError(t, err)
	require.Equal(t, "a", pending.Properties["name"])
	_, err = engine.GetNode("test:bulk-node-1")
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, tx.Commit())
	got, err := engine.GetNode("test:bulk-node-1")
	require.NoError(t, err)
	require.Equal(t, "a", got.Properties["name"])
	byLabel, err := engine.GetNodesByLabel("BulkNode")
	require.NoError(t, err)
	require.Len(t, byLabel, 2)
}

func TestTreeDBTransaction_BulkCreateNodesRejectsDuplicateWithoutPartialStage(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	tx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	bulk := tx.(interface{ BulkCreateNodes([]*Node) error })
	err = bulk.BulkCreateNodes([]*Node{
		{ID: "test:dup-node", Labels: []string{"BulkNode"}},
		{ID: "test:dup-node", Labels: []string{"BulkNode"}},
	})
	require.True(t, errors.Is(err, ErrAlreadyExists), "got %v", err)

	_, err = tx.GetNode("test:dup-node")
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, tx.Rollback())

	_, err = engine.GetNode("test:dup-node")
	require.ErrorIs(t, err, ErrNotFound)
}
