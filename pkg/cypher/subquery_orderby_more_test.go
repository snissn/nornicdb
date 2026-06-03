package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestApplyOrderByToResult_DottedPropertyBranches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	result := &ExecuteResult{
		Columns: []string{"t"},
		Rows: [][]interface{}{
			{map[string]interface{}{"createdAt": int64(2)}},
			{map[string]interface{}{"properties": map[string]interface{}{"createdAt": int64(1)}}},
			{map[string]interface{}{"createdAt": int64(3)}},
		},
	}

	ordered := exec.applyOrderByToResult(result, "ORDER BY t.createdAt ASC")
	require.Len(t, ordered.Rows, 3)
	require.EqualValues(t, int64(1), extractPropertyFromValue(ordered.Rows[0][0], "createdAt"))
	require.EqualValues(t, int64(2), extractPropertyFromValue(ordered.Rows[1][0], "createdAt"))
	require.EqualValues(t, int64(3), extractPropertyFromValue(ordered.Rows[2][0], "createdAt"))

	ordered = exec.applyOrderByToResult(result, "ORDER BY t.createdAt DESC LIMIT 2")
	require.Len(t, ordered.Rows, 3)
	require.EqualValues(t, int64(3), extractPropertyFromValue(ordered.Rows[0][0], "createdAt"))
}

func TestApplyOrderByToResult_UnknownAndEmptyBranches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	base := &ExecuteResult{
		Columns: []string{"name"},
		Rows: [][]interface{}{
			{"b"},
			{"a"},
		},
	}

	unchanged := exec.applyOrderByToResult(base, "ORDER BY missing")
	require.Equal(t, [][]interface{}{{"b"}, {"a"}}, unchanged.Rows)

	unchanged = exec.applyOrderByToResult(base, "ORDER BY")
	require.Equal(t, [][]interface{}{{"b"}, {"a"}}, unchanged.Rows)
}

func TestApplyResultModifiers_KZeroAndUnknownOrderColumn(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	in := &ExecuteResult{
		Columns: []string{"age"},
		Rows: [][]interface{}{
			{int64(3)},
			{int64(1)},
			{int64(2)},
		},
	}

	// k=0 branch: LIMIT 0 with ORDER BY should return no rows.
	out, err := exec.applyResultModifiers(in, "ORDER BY age ASC LIMIT 0")
	require.NoError(t, err)
	require.Empty(t, out.Rows)

	in2 := &ExecuteResult{
		Columns: []string{"name"},
		Rows: [][]interface{}{
			{"c"},
			{"a"},
			{"b"},
		},
	}

	// Unknown ORDER BY column with LIMIT should preserve previous behavior
	// and still apply LIMIT after no-op ORDER BY.
	out2, err := exec.applyResultModifiers(in2, "ORDER BY missing DESC LIMIT 2")
	require.NoError(t, err)
	require.Len(t, out2.Rows, 2)
	require.Equal(t, "c", out2.Rows[0][0])
	require.Equal(t, "a", out2.Rows[1][0])
}
