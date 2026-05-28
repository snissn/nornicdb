package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type labelCountGuardEngine struct {
	*storage.MemoryEngine
	forbidLabelScan bool
}

func (e *labelCountGuardEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if e.forbidLabelScan {
		return nil, fmt.Errorf("GetNodesByLabel should not be called for simple labeled count")
	}
	return e.MemoryEngine.GetNodesByLabel(label)
}

func (e *labelCountGuardEngine) AllNodes() ([]*storage.Node, error) {
	if e.forbidLabelScan {
		return nil, fmt.Errorf("AllNodes should not be called for simple labeled count")
	}
	return e.MemoryEngine.AllNodes()
}

func (e *labelCountGuardEngine) NodeCountByLabel(label string) (int64, error) {
	return storage.CountNodesWithLabel(context.Background(), e.MemoryEngine, label)
}

func TestMatchCountUsesLabelCountFastPath(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = base.Close() })
	eng := &labelCountGuardEngine{MemoryEngine: base}
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err := eng.CreateNode(&storage.Node{ID: storage.NodeID(fmt.Sprintf("nornic:p-%d", i)), Labels: []string{"Person"}})
		require.NoError(t, err)
	}
	_, err := eng.CreateNode(&storage.Node{ID: "nornic:o-1", Labels: []string{"Other"}})
	require.NoError(t, err)

	eng.forbidLabelScan = true

	res, err := exec.Execute(ctx, "MATCH (n:Person) RETURN count(n) AS cnt", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(3), res.Rows[0][0])
}
