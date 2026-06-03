package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type callGetNodeErrEngine struct {
	storage.Engine
	err error
}

func (e *callGetNodeErrEngine) GetNode(id storage.NodeID) (*storage.Node, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.Engine.GetNode(id)
}

type callOutgoingErrEngine struct {
	storage.Engine
	err error
}

func (e *callOutgoingErrEngine) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.Engine.GetOutgoingEdges(id)
}

type callIncomingErrEngine struct {
	storage.Engine
	err error
}

func (e *callIncomingErrEngine) GetIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.Engine.GetIncomingEdges(id)
}

func TestCallTailHelperParsersAndPredicates(t *testing.T) {
	t.Run("relationship_type_constraint_detection", func(t *testing.T) {
		assert.True(t, callTailHasRelationshipTypeConstraint("MATCH (a)-[:R]->(b) RETURN a"))
		assert.False(t, callTailHasRelationshipTypeConstraint("MATCH (a)-[r]->(b) RETURN a"))
		assert.True(t, callTailHasRelationshipTypeConstraint("MATCH (a)-[':R']->(b) RETURN a"))
		assert.False(t, callTailHasRelationshipTypeConstraint("MATCH (a)-[r:R->(b) RETURN a"))
	})

	t.Run("clause_splitting_and_alias_parsing", func(t *testing.T) {
		assert.Equal(t, "RETURN n", stripOrderByFromClause("RETURN n ORDER BY n.name DESC"))
		assert.Equal(t, "RETURN n", stripOrderByFromClause("RETURN n"))
		assert.Equal(t, "n.name DESC", extractOrderByClause("RETURN n ORDER BY n.name DESC"))
		assert.Equal(t, "", extractOrderByClause("RETURN n ORDER n.name DESC"))

		left, right, ok := splitPathAssignment("p = (a)-[:R]->(b)")
		require.True(t, ok)
		assert.Equal(t, "p", left)
		assert.Equal(t, "(a)-[:R]->(b)", right)
		_, _, ok = splitPathAssignment("=")
		assert.False(t, ok)

		alias, ok := parseAggregateExprAlias("max(length(p))", "maxDepth", "max", "length(p)")
		require.True(t, ok)
		assert.Equal(t, "maxDepth", alias)
		_, ok = parseAggregateExprAlias("max(length(p))", "max depth", "max", "length(p)")
		assert.False(t, ok)

		alias, ok = parseCountStarAlias("count(*) AS total")
		require.True(t, ok)
		assert.Equal(t, "total", alias)
		_, ok = parseCountStarAlias("count(x) AS total")
		assert.False(t, ok)

		assert.Equal(t, []string{"a=1", "b=2"}, splitConjunction("a=1 AND b=2"))

		minWeight, cats, ok := parseConstrainedTraversalPredicates([]string{
			"any(r IN relationships(p) WHERE r.weight >= $minWeight)",
			"any(n IN nodes(p) WHERE n.category IN $cats)",
		}, "p")
		require.True(t, ok)
		assert.Equal(t, "$minWeight", minWeight)
		assert.Equal(t, "$cats", cats)

		_, _, ok = parseConstrainedTraversalPredicates([]string{"any(r IN relationships(p) WHERE r.weight >= 1.0)"}, "p")
		assert.False(t, ok)
	})
}

func TestCallTailLiteralAndParamResolvers(t *testing.T) {
	ctx := withParams(context.Background(), map[string]interface{}{
		"i":       int64(7),
		"f":       3.5,
		"strings": []string{"a", "b"},
		"mixed":   []interface{}{"x", "y"},
		"bad":     []interface{}{1, "y"},
		"nope":    true,
	})

	n, ok := resolveOptionalIntLiteralOrParam(ctx, "")
	require.True(t, ok)
	assert.Equal(t, -1, n)

	n, ok = resolveIntLiteralOrParam(ctx, "$i")
	require.True(t, ok)
	assert.Equal(t, 7, n)
	n, ok = resolveIntLiteralOrParam(ctx, "42")
	require.True(t, ok)
	assert.Equal(t, 42, n)
	_, ok = resolveIntLiteralOrParam(ctx, "$missing")
	assert.False(t, ok)
	_, ok = resolveIntLiteralOrParam(ctx, "$nope")
	assert.False(t, ok)

	f, ok := resolveFloatLiteralOrParam(ctx, "$f")
	require.True(t, ok)
	assert.Equal(t, 3.5, f)
	f, ok = resolveFloatLiteralOrParam(ctx, "2.25")
	require.True(t, ok)
	assert.Equal(t, 2.25, f)
	_, ok = resolveFloatLiteralOrParam(ctx, "$missing")
	assert.False(t, ok)

	ss, ok := resolveStringSliceLiteralOrParam(ctx, "$strings")
	require.True(t, ok)
	assert.Equal(t, []string{"a", "b"}, ss)
	ss, ok = resolveStringSliceLiteralOrParam(ctx, "$mixed")
	require.True(t, ok)
	assert.Equal(t, []string{"x", "y"}, ss)
	_, ok = resolveStringSliceLiteralOrParam(ctx, "$bad")
	assert.False(t, ok)
	ss, ok = resolveStringSliceLiteralOrParam(ctx, `["left", 'right']`)
	require.True(t, ok)
	assert.Equal(t, []string{"left", "right"}, ss)
	_, ok = resolveStringSliceLiteralOrParam(ctx, "not-a-list")
	assert.False(t, ok)
}

func TestCallTailTraversalHelperBranches(t *testing.T) {
	base := storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_tail_helper_cov")
	exec := NewStorageExecutor(base)

	n1 := &storage.Node{ID: storage.NodeID("n1"), Labels: []string{"N"}, Properties: map[string]interface{}{"category": "allowed"}}
	n2 := &storage.Node{ID: storage.NodeID("n2"), Labels: []string{"N"}, Properties: map[string]interface{}{"category": "other"}}
	_, err := base.CreateNode(n1)
	require.NoError(t, err)
	_, err = base.CreateNode(n2)
	require.NoError(t, err)
	require.NoError(t, base.CreateEdge(&storage.Edge{ID: storage.EdgeID("e1"), Type: "REL", StartNode: n1.ID, EndNode: n2.ID, Properties: map[string]interface{}{"weight": 2.0}}))

	ctx := &callTailTraversalContext{nodeCache: map[storage.NodeID]*storage.Node{}}
	loaded, err := exec.callTailLoadNode(n1.ID, ctx)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.Equal(t, n1.ID, loaded.ID)
	ctx.nodeCache[n1.ID] = &storage.Node{ID: n1.ID, Labels: []string{"Cached"}, Properties: map[string]interface{}{}}
	loaded, err = exec.callTailLoadNode(n1.ID, ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"Cached"}, loaded.Labels)
	loaded, err = exec.callTailLoadNode(storage.NodeID("missing"), ctx)
	require.ErrorIs(t, err, storage.ErrNotFound)
	assert.Nil(t, loaded)

	getNodeErr := errors.New("get node failed")
	errExec := NewStorageExecutor(&callGetNodeErrEngine{Engine: base, err: getNodeErr})
	_, err = errExec.callTailLoadNode(n2.ID, &callTailTraversalContext{nodeCache: map[storage.NodeID]*storage.Node{}})
	require.ErrorIs(t, err, getNodeErr)

	matchOut := &TraversalMatch{Relationship: RelationshipPattern{Direction: "outgoing", MinHops: 1, MaxHops: 2, Types: []string{"REL"}}}
	edges, err := exec.callTailTraversalEdges(n1, matchOut)
	require.NoError(t, err)
	require.Len(t, edges, 1)

	assert.True(t, callTailEdgeMatchesTypes(edges[0], nil, nil))
	assert.True(t, callTailEdgeMatchesTypes(edges[0], []string{"REL"}, nil))
	assert.False(t, callTailEdgeMatchesTypes(edges[0], []string{"OTHER"}, nil))
	assert.True(t, callTailEdgeMatchesTypes(edges[0], []string{"OTHER", "REL"}, map[string]struct{}{"REL": {}}))
	assert.Equal(t, n2.ID, callTailNextNodeID(n1.ID, edges[0], "outgoing"))
	assert.Equal(t, n1.ID, callTailNextNodeID(n1.ID, edges[0], "incoming"))

	depth, err := exec.maxDepthForTraversalMatch(n1, matchOut)
	require.NoError(t, err)
	assert.Equal(t, 1, depth)

	outErr := errors.New("outgoing failed")
	errExec = NewStorageExecutor(&callOutgoingErrEngine{Engine: base, err: outErr})
	_, err = errExec.maxDepthForTraversalMatch(n1, matchOut)
	require.ErrorIs(t, err, outErr)

	inErr := errors.New("incoming failed")
	matchIn := &TraversalMatch{Relationship: RelationshipPattern{Direction: "incoming", MinHops: 1, MaxHops: 2, Types: []string{"REL"}}}
	errExec = NewStorageExecutor(&callIncomingErrEngine{Engine: base, err: inErr})
	_, err = errExec.maxDepthForTraversalMatch(n2, matchIn)
	require.ErrorIs(t, err, inErr)

	allowed := map[string]struct{}{"allowed": {}}
	depth, ok, err := exec.maxDepthForPredicateTraversal(n1, matchOut, callTailPathPredicate{minWeight: ptrFloat64(1.5), allowedCategory: allowed})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, 1, depth)

	count, err := exec.countTraversalPathsWithCap(n1, matchOut, 1, callTailPathPredicate{})
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	nearest, reachable, err := exec.shortestReachableStats(n1, matchOut)
	require.NoError(t, err)
	assert.Equal(t, 1, nearest)
	assert.Equal(t, 1, reachable)
}

func ptrFloat64(v float64) *float64 {
	return &v
}
