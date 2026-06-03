package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestBuildJoinedResult_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "joined_result_cov"))
	ctx := context.Background()

	a := &storage.Node{ID: storage.NodeID("a"), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Alice", "age": int64(30)}}
	b := &storage.Node{ID: storage.NodeID("b"), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Bob", "age": int64(25)}}
	c := &storage.Node{ID: storage.NodeID("c"), Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "Cara", "age": int64(35)}}
	r1 := &storage.Edge{ID: storage.EdgeID("r1"), Type: "KNOWS", StartNode: a.ID, EndNode: b.ID}
	r2 := &storage.Edge{ID: storage.EdgeID("r2"), Type: "KNOWS", StartNode: a.ID, EndNode: c.ID}

	rows := []joinedRow{
		{initialNode: a, relatedNode: b, relationship: r1},
		{initialNode: a, relatedNode: c, relationship: r2},
	}

	_, err := exec.buildJoinedResult(ctx, rows, "s", "t", "r", "MATCH (s)-[r]->(t)")
	require.Error(t, err)
	require.Contains(t, err.Error(), "RETURN clause required")

	res, err := exec.buildJoinedResult(ctx, rows, "s", "t", "r", "MATCH (s)-[r]->(t) RETURN s.name AS sname, t.name AS tname ORDER BY tname DESC SKIP 1 LIMIT 1")
	require.NoError(t, err)
	require.Equal(t, []string{"sname", "tname"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "Alice", res.Rows[0][0])
	require.Equal(t, "Bob", res.Rows[0][1])

	res, err = exec.buildJoinedResult(ctx, rows, "s", "t", "r", "MATCH (s)-[r]->(t) RETURN count(*) AS c")
	require.NoError(t, err)
	require.Equal(t, []string{"c"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 2, res.Rows[0][0])

	res, err = exec.buildJoinedResult(ctx, rows, "s", "t", "r", "MATCH (s)-[r]->(t) RETURN s.name AS sname, collect(t.name) AS related ORDER BY sname")
	require.NoError(t, err)
	require.Equal(t, []string{"sname", "related"}, res.Columns)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "Alice", res.Rows[0][0])
	related, ok := res.Rows[0][1].([]interface{})
	require.True(t, ok)
	require.ElementsMatch(t, []interface{}{"Bob", "Cara"}, related)
}
