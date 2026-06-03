package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type createMatchErrEngine struct {
	storage.Engine
	labelErrs       map[string]error
	allErr          error
	edgesBetweenErr error
}

func (e *createMatchErrEngine) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if err, ok := e.labelErrs[label]; ok && err != nil {
		return nil, err
	}
	return e.Engine.GetNodesByLabel(label)
}

func (e *createMatchErrEngine) AllNodes() ([]*storage.Node, error) {
	if e.allErr != nil {
		return nil, e.allErr
	}
	return e.Engine.AllNodes()
}

func (e *createMatchErrEngine) GetEdgesBetween(startID, endID storage.NodeID) ([]*storage.Edge, error) {
	if e.edgesBetweenErr != nil {
		return nil, e.edgesBetweenErr
	}
	return e.Engine.GetEdgesBetween(startID, endID)
}

func TestExecuteCompoundMatchCreate_SurfacesMatchLabelLookupError(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "create_match_err_label")

	_, err := store.CreateNode(&storage.Node{ID: "a1", Labels: []string{"A"}, Properties: map[string]interface{}{"id": "a1"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "b1", Labels: []string{"B"}, Properties: map[string]interface{}{"id": "b1"}})
	require.NoError(t, err)

	lookupErr := errors.New("label lookup failed")
	errExec := NewStorageExecutor(&createMatchErrEngine{
		Engine:    store,
		labelErrs: map[string]error{"A": lookupErr},
	})

	_, err = errExec.executeCompoundMatchCreate(context.Background(),
		"MATCH (a:A),(b:B) WHERE NOT (a)-[:R]->(b) CREATE (a)-[:R]->(b) RETURN count(*) AS n")
	require.Error(t, err)
	require.ErrorIs(t, err, lookupErr)
}

func TestExecuteCompoundMatchCreate_SurfacesAllNodesError(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "create_match_err_all")

	allErr := errors.New("all nodes failed")
	errExec := NewStorageExecutor(&createMatchErrEngine{Engine: store, allErr: allErr})

	_, err := errExec.executeCompoundMatchCreate(context.Background(),
		"MATCH (a),(b) WHERE NOT (a)-[:R]->(b) CREATE (a)-[:R]->(b) RETURN count(*) AS n")
	require.Error(t, err)
	require.ErrorIs(t, err, allErr)
}

func TestExecuteCompoundMatchCreate_SurfacesRelationshipCheckError(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "create_match_err_rel")

	_, err := store.CreateNode(&storage.Node{ID: "a1", Labels: []string{"A"}, Properties: map[string]interface{}{"id": "a1"}})
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "b1", Labels: []string{"B"}, Properties: map[string]interface{}{"id": "b1"}})
	require.NoError(t, err)
	query := "MATCH (a:A),(b:B) WHERE a.id = 'a1' AND b.id = 'b1' AND NOT (a)-[:R]->(b) CREATE (a)-[:R]->(b) RETURN count(*) AS n"

	okExec := NewStorageExecutor(store)
	okRes, err := okExec.executeCompoundMatchCreate(context.Background(), query)
	require.NoError(t, err)
	require.NotNil(t, okRes)

	outErr := errors.New("edges-between lookup failed")
	errExec := NewStorageExecutor(&createMatchErrEngine{Engine: store, edgesBetweenErr: outErr})

	_, err = errExec.executeCompoundMatchCreate(context.Background(), query)
	require.Error(t, err)
	require.ErrorIs(t, err, outErr)
}

func TestBuildCombinationsUsingWhereJoin_Branches(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	// Single-variable component path (unconstrained `c` component).
	single, ok := exec.buildCombinationsUsingWhereJoin(
		[]struct {
			variable string
			nodes    []*storage.Node
		}{
			{variable: "a", nodes: []*storage.Node{{ID: "a1", Properties: map[string]interface{}{"id": "x"}}}},
			{variable: "b", nodes: []*storage.Node{{ID: "b1", Properties: map[string]interface{}{"id": "x"}}}},
			{variable: "c", nodes: []*storage.Node{{ID: "c1", Properties: map[string]interface{}{"id": "z"}}}},
		},
		"a.id = b.id",
	)
	require.True(t, ok)
	require.Len(t, single, 1)
	require.Equal(t, storage.NodeID("a1"), single[0]["a"].ID)
	require.Equal(t, storage.NodeID("b1"), single[0]["b"].ID)
	require.Equal(t, storage.NodeID("c1"), single[0]["c"].ID)

	// Multi-variable path with rows that trigger nil/missing branches before producing one valid join.
	joined, ok := exec.buildCombinationsUsingWhereJoin(
		[]struct {
			variable string
			nodes    []*storage.Node
		}{
			{
				variable: "a",
				nodes: []*storage.Node{
					nil,
					{ID: "a_nil_props", Properties: nil},
					{ID: "a_missing", Properties: map[string]interface{}{"other": "v"}},
					{ID: "a_ok", Properties: map[string]interface{}{"id": "k1"}},
				},
			},
			{
				variable: "b",
				nodes: []*storage.Node{
					{ID: "b_nil_props", Properties: nil},
					{ID: "b_missing", Properties: map[string]interface{}{"other": "v"}},
					{ID: "b_no_match", Properties: map[string]interface{}{"id": "k2"}},
					{ID: "b_ok", Properties: map[string]interface{}{"id": "k1"}},
				},
			},
		},
		"a.id = b.id",
	)
	require.True(t, ok)
	require.Len(t, joined, 1)
	require.Equal(t, storage.NodeID("a_ok"), joined[0]["a"].ID)
	require.Equal(t, storage.NodeID("b_ok"), joined[0]["b"].ID)
}
