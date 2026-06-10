package cypher

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestTemporalParam_SubstitutionPreservesTypedBinding(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal_param_subst"))
	dt := time.Date(2026, 6, 9, 12, 0, 0, 0, time.FixedZone("UTC+2", 2*3600))

	query := "CREATE (n:T {uuid:$uuid, created_at:$dt}) RETURN n"
	out := exec.substituteParams(query, map[string]interface{}{
		"uuid": "t",
		"dt":   dt,
	})

	// Scalar params still substitute, but temporal params must remain bound so
	// parsePropertyValue can resolve the typed value from context.
	require.Contains(t, out, "uuid:'t'")
	require.True(t, strings.Contains(out, "created_at:$dt"), "expected temporal param to remain bound, got: %s", out)
}

func TestTemporalParam_CreateAndReadBackAsTime(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "temporal_param_roundtrip"))
	ctx := context.Background()
	dt := time.Date(2026, 6, 9, 12, 0, 0, 0, time.FixedZone("UTC+2", 2*3600))

	_, err := exec.Execute(ctx, "CREATE (n:T {uuid:'t', created_at:$dt})", map[string]interface{}{"dt": dt})
	require.NoError(t, err)

	result, err := exec.Execute(ctx, "MATCH (n:T {uuid:'t'}) RETURN n.created_at", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0], 1)

	got, ok := result.Rows[0][0].(time.Time)
	require.True(t, ok, "expected time.Time, got %T", result.Rows[0][0])
	require.Equal(t, dt.UTC(), got.UTC())
}
