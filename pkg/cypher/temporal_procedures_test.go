package cypher

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTemporalAssertNoOverlap(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	_, err := engine.CreateNode(&storage.Node{
		ID:     "v1",
		Labels: []string{"FactVersion"},
		Properties: map[string]interface{}{
			"fact_key":   "k1",
			"valid_from": start,
			"valid_to":   end,
		},
	})
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion','fact_key','valid_from','valid_to','k1','2024-01-15','2024-02-15')", nil)
	require.Error(t, err)

	result, err := exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion','fact_key','valid_from','valid_to','k1','2024-02-01','2024-03-01')", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Equal(t, true, result.Rows[0][0])
}

func TestTemporalProcedures_HelperBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// parseTemporalCallArgs malformed syntax branches.
	_, err := parseTemporalCallArgs(ctx, "CALL db.temporal.assertNoOverlap('x'", "DB.TEMPORAL.ASSERTNOOVERLAP")
	require.Error(t, err)

	// coerceStringArg fallback (non-string gets fmt.Sprint conversion).
	res, err := exec.Execute(ctx, "CALL db.temporal.assertNoOverlap(123,456,789,1011,12,1700000000,null)", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, true, res.Rows[0][0])

	// asOf with numeric datetime (int64 parsing branch).
	_, err = engine.CreateNode(&storage.Node{
		ID:     "num-time",
		Labels: []string{"123"},
		Properties: map[string]interface{}{
			"456":  int64(12),
			"789":  int64(1699999999),
			"1011": nil,
		},
	})
	require.NoError(t, err)
	asOfRes, err := exec.Execute(ctx, "CALL db.temporal.asOf(123,456,12,789,1011,1700000000) YIELD node", nil)
	require.NoError(t, err)
	require.Len(t, asOfRes.Rows, 1)
}

func TestTemporalAsOf(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	v1Start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	v1End := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	_, err := engine.CreateNode(&storage.Node{
		ID:     "v1",
		Labels: []string{"FactVersion"},
		Properties: map[string]interface{}{
			"fact_key":   "k1",
			"valid_from": v1Start,
			"valid_to":   v1End,
		},
	})
	require.NoError(t, err)

	v2Start := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	_, err = engine.CreateNode(&storage.Node{
		ID:     "v2",
		Labels: []string{"FactVersion"},
		Properties: map[string]interface{}{
			"fact_key":   "k1",
			"valid_from": v2Start,
			"valid_to":   nil,
		},
	})
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion','fact_key','k1','valid_from','valid_to','2024-01-15') YIELD node", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 1)

	switch node := result.Rows[0][0].(type) {
	case *storage.Node:
		require.Equal(t, storage.NodeID("v1"), node.ID)
	case storage.Node:
		require.Equal(t, storage.NodeID("v1"), node.ID)
	default:
		t.Fatalf("unexpected node type: %T", node)
	}
}

func TestTemporalAsOf_WithSnapshotVersion(t *testing.T) {
	// temporal.asOf with a snapshot version is a multi-version feature;
	// exercise it against the MVCC-retention variant.
	base := storage.NewMemoryEngineWithMVCCHistory()
	t.Cleanup(func() { _ = base.Close() })
	engine := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	validFrom := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	validTo := time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)
	_, err := engine.CreateNode(&storage.Node{
		ID:     "snap-v1",
		Labels: []string{"FactVersion"},
		Properties: map[string]interface{}{
			"fact_key":   "k1",
			"valid_from": validFrom,
			"valid_to":   validTo,
		},
	})
	require.NoError(t, err)
	head, err := engine.GetNodeCurrentHead("snap-v1")
	require.NoError(t, err)

	require.NoError(t, engine.DeleteNode("snap-v1"))

	result, err := exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion','fact_key','k1','valid_from','valid_to','2024-01-15T00:00:00Z') YIELD node", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)

	query := fmt.Sprintf(
		"CALL db.temporal.asOf('FactVersion','fact_key','k1','valid_from','valid_to','2024-01-15T00:00:00Z','%s',%d) YIELD node",
		head.Version.CommitTimestamp.Format(time.RFC3339Nano),
		head.Version.CommitSequence,
	)
	result, err = exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	node, ok := result.Rows[0][0].(*storage.Node)
	require.True(t, ok)
	require.Equal(t, storage.NodeID("snap-v1"), node.ID)
}

func TestTemporalProcedures_ErrorAndSelectionBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Bad argument count/shape branches.
	_, err := exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion')", nil)
	require.Error(t, err)
	_, err = exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion')", nil)
	require.Error(t, err)

	// Invalid datetime coercion branches.
	_, err = exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion','fact_key','valid_from','valid_to','k1','not-a-datetime',null)", nil)
	require.Error(t, err)
	_, err = exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion','fact_key','k1','valid_from','valid_to','bad-datetime') YIELD node", nil)
	require.Error(t, err)

	// Seed with mixed records:
	// - one invalid existing start (ignored)
	// - two valid intervals where latest matching start should be selected by asOf
	_, err = engine.CreateNode(&storage.Node{
		ID:     "bad-start",
		Labels: []string{"FactVersion"},
		Properties: map[string]interface{}{
			"fact_key":   "k1",
			"valid_from": "invalid",
			"valid_to":   nil,
		},
	})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{
		ID:     "v1",
		Labels: []string{"FactVersion"},
		Properties: map[string]interface{}{
			"fact_key":   "k1",
			"valid_from": "2024-01-01T00:00:00Z",
			"valid_to":   "2024-03-01T00:00:00Z",
		},
	})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{
		ID:     "v2",
		Labels: []string{"FactVersion"},
		Properties: map[string]interface{}{
			"fact_key":   "k1",
			"valid_from": "2024-02-01T00:00:00Z",
			"valid_to":   nil,
		},
	})
	require.NoError(t, err)

	// Non-overlapping interval should pass.
	okRes, err := exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion','fact_key','valid_from','valid_to','k1','2023-01-01T00:00:00Z','2023-12-31T00:00:00Z')", nil)
	require.NoError(t, err)
	require.Equal(t, true, okRes.Rows[0][0])

	// Overlap should fail.
	_, err = exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion','fact_key','valid_from','valid_to','k1','2024-02-15T00:00:00Z','2024-04-01T00:00:00Z')", nil)
	require.Error(t, err)

	// asOf selects most recent valid start covering timestamp.
	asOfRes, err := exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion','fact_key','k1','valid_from','valid_to','2024-02-15T00:00:00Z') YIELD node", nil)
	require.NoError(t, err)
	require.Len(t, asOfRes.Rows, 1)
	n, ok := asOfRes.Rows[0][0].(*storage.Node)
	require.True(t, ok)
	require.Equal(t, storage.NodeID("v2"), n.ID)

	// No matching key returns empty rows.
	noneRes, err := exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion','fact_key','missing','valid_from','valid_to','2024-02-15T00:00:00Z') YIELD node", nil)
	require.NoError(t, err)
	require.Empty(t, noneRes.Rows)
}

func TestTemporalProcedures_RequiredStringArgsBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CALL db.temporal.assertNoOverlap(null,'fact_key','valid_from','valid_to','k1','2024-01-01T00:00:00Z',null)", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "label is required")

	_, err = exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('','fact_key','valid_from','valid_to','k1','2024-01-01T00:00:00Z',null)", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "label cannot be empty")

	_, err = exec.Execute(ctx, "CALL db.temporal.asOf('', 'fact_key','k1','valid_from','valid_to','2024-01-01T00:00:00Z') YIELD node", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "label cannot be empty")
}

func TestTemporalProcedures_LabelLookupErrorBranches(t *testing.T) {
	failStore := &failingNodeLookupEngine{
		Engine:     storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"),
		byLabelErr: errors.New("label lookup failed"),
	}
	exec := NewStorageExecutor(failStore)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion','fact_key','valid_from','valid_to','k1','2024-01-01T00:00:00Z',null)", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read nodes for label")

	_, err = exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion','fact_key','k1','valid_from','valid_to','2024-01-01T00:00:00Z') YIELD node", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to read nodes for label")
}

func TestTemporalProcedures_StrictArgValidationAdditionalBranches(t *testing.T) {
	base := newTestMemoryEngine(t)
	engine := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(engine)
	ctx := context.Background()

	// Invalid call name/syntax branches.
	_, err := exec.callDbTemporalAssertNoOverlap(ctx, "CALL db.temporal.wrong()")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid db.temporal.assertnooverlap syntax")
	_, err = exec.callDbTemporalAsOf(ctx, "CALL db.temporal.other()")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid db.temporal.asof syntax")

	// assertNoOverlap: required string args beyond label.
	_, err = exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion',null,'valid_from','valid_to','k1','2024-01-01T00:00:00Z',null)", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "keyProp is required")

	_, err = exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion','fact_key',null,'valid_to','k1','2024-01-01T00:00:00Z',null)", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "validFromProp is required")

	_, err = exec.Execute(ctx, "CALL db.temporal.assertNoOverlap('FactVersion','fact_key','valid_from',null,'k1','2024-01-01T00:00:00Z',null)", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "validToProp is required")

	// asOf: required string args for key/from/to props.
	_, err = exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion',null,'k1','valid_from','valid_to','2024-01-01T00:00:00Z') YIELD node", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "keyProp is required")

	_, err = exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion','fact_key','k1',null,'valid_to','2024-01-01T00:00:00Z') YIELD node", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "validFromProp is required")

	_, err = exec.Execute(ctx, "CALL db.temporal.asOf('FactVersion','fact_key','k1','valid_from',null,'2024-01-01T00:00:00Z') YIELD node", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "validToProp is required")
}
