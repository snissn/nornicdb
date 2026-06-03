package cypher

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCallTailTraversal_MorePredicateAndCapBranches(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_traversal_more")
	exec := NewStorageExecutor(store)

	for _, n := range []*storage.Node{
		{ID: "n1", Labels: []string{"N"}, Properties: map[string]interface{}{"category": "allow"}},
		{ID: "n2", Labels: []string{"N"}, Properties: map[string]interface{}{"category": "allow"}},
		{ID: "n3", Labels: []string{"N"}, Properties: map[string]interface{}{"category": "deny"}},
		{ID: "n4", Labels: nil, Properties: map[string]interface{}{"category": "allow"}},
	} {
		_, err := store.CreateNode(n)
		require.NoError(t, err)
	}
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e12", Type: "REL", StartNode: "n1", EndNode: "n2", Properties: map[string]interface{}{"weight": 2.0}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e13", Type: "REL", StartNode: "n1", EndNode: "n3", Properties: map[string]interface{}{"weight": 0.3}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e24", Type: "REL", StartNode: "n2", EndNode: "n4", Properties: map[string]interface{}{"weight": 3.0}}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e31", Type: "BACK", StartNode: "n3", EndNode: "n1"}))

	n1, err := store.GetNode("n1")
	require.NoError(t, err)

	match := &TraversalMatch{EndNode: nodePatternInfo{labels: []string{"N"}}, Relationship: RelationshipPattern{Direction: "outgoing", Types: []string{"REL"}, MinHops: 1, MaxHops: 3}}

	count, err := exec.countTraversalPathsWithCap(n1, match, 1, callTailPathPredicate{requireAllNodesLabeled: true})
	require.NoError(t, err)
	require.Equal(t, 1, count)

	allowed := map[string]struct{}{"allow": {}}
	depth, found, err := exec.maxDepthForPredicateTraversal(n1, match, callTailPathPredicate{minWeight: ptrFloat64(1.0), allowedCategory: allowed})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 1, depth)

	depth, found, err = exec.maxDepthForPredicateTraversal(n1, match, callTailPathPredicate{minWeight: ptrFloat64(9.0), allowedCategory: allowed})
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, 0, depth)

	// Direct traversal call with trackMax=true and visited edge pre-populated to force skip branch.
	ctx := &callTailTraversalContext{
		nodeCache:    map[storage.NodeID]*storage.Node{n1.ID: n1},
		visitedEdges: map[storage.EdgeID]bool{"e13": true},
		relTypeSet:   map[string]struct{}{"REL": {}},
	}
	c, maxDepth, err := exec.walkCallTailTraversal(n1, 0, match, ctx, callTailPathPredicate{allowedCategory: allowed}, false, -1, predicateMatchesCategory(n1, allowed), true)
	require.NoError(t, err)
	require.GreaterOrEqual(t, c, 1)
	require.GreaterOrEqual(t, maxDepth, 1)
}

func TestMaxDepthForTraversalMatchFromNode_DirectionAndTypeBranches(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_depth_more")
	exec := NewStorageExecutor(store)

	for _, n := range []*storage.Node{
		{ID: "a", Labels: []string{"N"}},
		{ID: "b", Labels: []string{"N"}},
		{ID: "c", Labels: []string{"N"}},
	} {
		_, err := store.CreateNode(n)
		require.NoError(t, err)
	}
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "ab", Type: "R1", StartNode: "a", EndNode: "b"}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "bc", Type: "R2", StartNode: "b", EndNode: "c"}))
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "ca", Type: "R3", StartNode: "c", EndNode: "a"}))

	a, err := store.GetNode("a")
	require.NoError(t, err)
	b, err := store.GetNode("b")
	require.NoError(t, err)

	mBoth := &TraversalMatch{EndNode: nodePatternInfo{labels: []string{"N"}}, Relationship: RelationshipPattern{Direction: "both", Types: []string{"R1", "R2"}, MinHops: 1, MaxHops: 3}}
	ctx := &callTailMaxDepthContext{nodeCache: map[storage.NodeID]*storage.Node{a.ID: a, b.ID: b}, visited: map[storage.EdgeID]bool{}, relTypeSet: map[string]struct{}{"R1": {}, "R2": {}}}
	require.NoError(t, exec.maxDepthForTraversalMatchFromNode(a, 0, mBoth, ctx))
	require.GreaterOrEqual(t, ctx.best, 1)

	mIncoming := &TraversalMatch{EndNode: nodePatternInfo{labels: []string{"N"}}, Relationship: RelationshipPattern{Direction: "incoming", Types: []string{"R3"}, MinHops: 1, MaxHops: 2}}
	ctx2 := &callTailMaxDepthContext{nodeCache: map[storage.NodeID]*storage.Node{a.ID: a}, visited: map[storage.EdgeID]bool{}, relTypeSet: map[string]struct{}{"R3": {}}}
	require.NoError(t, exec.maxDepthForTraversalMatchFromNode(a, 0, mIncoming, ctx2))
	require.GreaterOrEqual(t, ctx2.best, 1)
}
