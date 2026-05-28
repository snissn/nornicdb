package storage

import (
	"errors"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/stretchr/testify/require"
)

type adjacentFallbackEngine struct {
	Engine
	outgoing []*Edge
	incoming []*Edge
	outErr   error
	inErr    error
}

func (e *adjacentFallbackEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	if e.outErr != nil {
		return nil, e.outErr
	}
	return e.outgoing, nil
}

func (e *adjacentFallbackEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	if e.inErr != nil {
		return nil, e.inErr
	}
	return e.incoming, nil
}

type adjacentDirectEngine struct {
	*adjacentFallbackEngine
	adjOut []*Edge
	adjIn  []*Edge
	adjErr error
}

func (e *adjacentDirectEngine) GetAdjacentEdges(nodeID NodeID) ([]*Edge, []*Edge, error) {
	if e.adjErr != nil {
		return nil, nil, e.adjErr
	}
	return e.adjOut, e.adjIn, nil
}

func TestRemoteExtraNormalizeBoltValues(t *testing.T) {
	node := dbtype.Node{ElementId: "node-element", Labels: []string{"Doc", "File"}, Props: map[string]interface{}{"id": "node-id", "name": "readme"}}
	normalizedNode := normalizeBoltValue(node).(map[string]interface{})
	require.Equal(t, "node-element", normalizedNode["elementId"])
	require.Equal(t, "node-id", normalizedNode["id"])
	require.Equal(t, []interface{}{"Doc", "File"}, normalizedNode["labels"])
	require.Equal(t, map[string]interface{}{"id": "node-id", "name": "readme"}, normalizedNode["properties"])

	rel := dbtype.Relationship{ElementId: "rel-element", StartElementId: "start", EndElementId: "end", Type: "LINKS", Props: map[string]interface{}{"id": "rel-id", "weight": 2}}
	normalizedRel := normalizeBoltValue(rel).(map[string]interface{})
	require.Equal(t, "rel-element", normalizedRel["elementId"])
	require.Equal(t, "rel-id", normalizedRel["id"])
	require.Equal(t, "LINKS", normalizedRel["type"])
	require.Equal(t, "start", normalizedRel["startNodeElementId"])
	require.Equal(t, "end", normalizedRel["endNodeElementId"])
	require.Equal(t, map[string]interface{}{"id": "rel-id", "weight": 2}, normalizedRel["properties"])

	require.Equal(t, []interface{}{"a", "b"}, toInterfaceSlice([]string{"a", "b"}))
	require.Equal(t, "plain", normalizeBoltValue("plain"))
}

func TestStorageExtraMVCCConstructorAndPendingNoop(t *testing.T) {
	engine := NewMemoryEngineWithMVCCHistory()
	require.NotNil(t, engine)
	t.Cleanup(func() { _ = engine.Close() })

	badger := newIsolatedBadgerEngine(t)
	badger.AddToPendingEmbeddings("nornic:n1")
	require.Equal(t, 1, badger.PendingEmbeddingsCount())
	badger.InvalidatePendingEmbeddingsIndex()
	require.Equal(t, 1, badger.PendingEmbeddingsCount())
}

func TestNamespacedExtraAdjacentEdgesBranches(t *testing.T) {
	fallback := &adjacentFallbackEngine{
		Engine: NewMemoryEngine(),
		outgoing: []*Edge{
			{ID: "tenant:e1", StartNode: "tenant:n1", EndNode: "tenant:n2"},
			{ID: "other:e2", StartNode: "other:n1", EndNode: "other:n2"},
		},
		incoming: []*Edge{{ID: "tenant:e3", StartNode: "tenant:n3", EndNode: "tenant:n1"}},
	}
	ns := NewNamespacedEngine(fallback, "tenant")
	out, in, err := ns.GetAdjacentEdges("n1")
	require.NoError(t, err)
	require.Equal(t, []*Edge{{ID: "e1", StartNode: "n1", EndNode: "n2"}}, out)
	require.Equal(t, []*Edge{{ID: "e3", StartNode: "n3", EndNode: "n1"}}, in)

	wantErr := errors.New("adjacent failed")
	direct := &adjacentDirectEngine{adjacentFallbackEngine: &adjacentFallbackEngine{Engine: NewMemoryEngine()}, adjErr: wantErr}
	ns = NewNamespacedEngine(direct, "tenant")
	_, _, err = ns.GetAdjacentEdges("n1")
	require.ErrorIs(t, err, wantErr)

	direct.adjErr = nil
	direct.adjOut = []*Edge{{ID: "tenant:e4", StartNode: "tenant:n1", EndNode: "tenant:n4"}}
	direct.adjIn = []*Edge{{ID: "other:e5", StartNode: "other:n5", EndNode: "other:n1"}}
	out, in, err = ns.GetAdjacentEdges("n1")
	require.NoError(t, err)
	require.Equal(t, []*Edge{{ID: "e4", StartNode: "n1", EndNode: "n4"}}, out)
	require.Empty(t, in)

	fallback.outErr = wantErr
	ns = NewNamespacedEngine(fallback, "tenant")
	_, _, err = ns.GetAdjacentEdges("n1")
	require.ErrorIs(t, err, wantErr)

	fallback.outErr = nil
	fallback.inErr = wantErr
	_, _, err = ns.GetAdjacentEdges("n1")
	require.ErrorIs(t, err, wantErr)
}
