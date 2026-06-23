package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBadgerEngine_GetNodeProjectedSkipsUnrequestedVectorProperty(t *testing.T) {
	engine := createTestBadgerEngine(t)
	embedding := make([]float64, 1024)
	for i := range embedding {
		embedding[i] = float64(i) / 1024
	}
	node := testNode("projected-node")
	node.Labels = []string{"Entity"}
	node.Properties = map[string]any{
		"uuid":           "entity-1",
		"group_id":       "episode-1",
		"name_embedding": embedding,
	}

	_, err := engine.CreateNode(node)
	require.NoError(t, err)

	projected, err := engine.GetNodeProjected(node.ID, []string{"uuid", "group_id"})
	require.NoError(t, err)
	require.Equal(t, node.ID, projected.ID)
	require.Equal(t, []string{"Entity"}, projected.Labels)
	require.Equal(t, "entity-1", projected.Properties["uuid"])
	require.Equal(t, "episode-1", projected.Properties["group_id"])
	require.NotContains(t, projected.Properties, "name_embedding")

	full, err := engine.GetNode(node.ID)
	require.NoError(t, err)
	require.Contains(t, full.Properties, "name_embedding")
	require.Len(t, full.Properties["name_embedding"], 1024)
}

func TestBadgerEngine_GetNodeProjectedEmptyPropertyList(t *testing.T) {
	engine := createTestBadgerEngine(t)
	node := testNode("projected-empty")
	node.Properties = map[string]any{"uuid": "entity-1"}

	_, err := engine.CreateNode(node)
	require.NoError(t, err)

	projected, err := engine.GetNodeProjected(node.ID, []string{})
	require.NoError(t, err)
	require.Equal(t, node.ID, projected.ID)
	require.Empty(t, projected.Properties)
}
