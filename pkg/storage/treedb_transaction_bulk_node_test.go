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

func TestTreeDBTransaction_BulkCreateNodesRejectsBatchUniqueConstraintConflict(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{
		Name:       "unique_bulk_email",
		Type:       ConstraintUnique,
		Label:      "BulkUser",
		Properties: []string{"email"},
	}))

	tx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	bulk := tx.(interface{ BulkCreateNodes([]*Node) error })
	err = bulk.BulkCreateNodes([]*Node{
		{ID: "test:bulk-user-1", Labels: []string{"BulkUser"}, Properties: map[string]any{"email": "same@example.test"}},
		{ID: "test:bulk-user-2", Labels: []string{"BulkUser"}, Properties: map[string]any{"email": "same@example.test"}},
	})
	var constraintErr *ConstraintViolationError
	require.ErrorAs(t, err, &constraintErr)
	require.Equal(t, ConstraintUnique, constraintErr.Type)

	_, err = tx.GetNode("test:bulk-user-1")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = tx.GetNode("test:bulk-user-2")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestTreeDBTransaction_BulkCreateNodesDefersConstraintValidation(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	require.NoError(t, engine.GetSchemaForNamespace("test").AddConstraint(Constraint{
		Name:       "unique_deferred_bulk_email",
		Type:       ConstraintUnique,
		Label:      "DeferredBulkUser",
		Properties: []string{"email"},
	}))

	tx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	require.NoError(t, tx.SetDeferredConstraintValidation(true))
	defer tx.Rollback()

	bulk := tx.(interface{ BulkCreateNodes([]*Node) error })
	require.NoError(t, bulk.BulkCreateNodes([]*Node{
		{ID: "test:deferred-user-1", Labels: []string{"DeferredBulkUser"}, Properties: map[string]any{"email": "same@example.test"}},
		{ID: "test:deferred-user-2", Labels: []string{"DeferredBulkUser"}, Properties: map[string]any{"email": "same@example.test"}},
	}))
	pending, err := tx.GetNode("test:deferred-user-1")
	require.NoError(t, err)
	require.Equal(t, "same@example.test", pending.Properties["email"])

	err = tx.Commit()
	var constraintErr *ConstraintViolationError
	require.ErrorAs(t, err, &constraintErr)
	require.Equal(t, ConstraintUnique, constraintErr.Type)
}

func TestTreeDBTransaction_BulkCreateNodesRejectsCrossNamespaceBatchWithoutPartialStage(t *testing.T) {
	engine := newTestTreeDBEngine(t)
	tx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	defer tx.Rollback()

	bulk := tx.(interface{ BulkCreateNodes([]*Node) error })
	err = bulk.BulkCreateNodes([]*Node{
		{ID: "test:bulk-node-1", Labels: []string{"BulkNode"}},
		{ID: "other:bulk-node-2", Labels: []string{"BulkNode"}},
	})
	require.ErrorIs(t, err, ErrCrossNamespaceTransaction)

	_, err = tx.GetNode("test:bulk-node-1")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = engine.GetNode("test:bulk-node-1")
	require.ErrorIs(t, err, ErrNotFound)
}
