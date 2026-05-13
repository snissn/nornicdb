// Package search – regression tests for the nodeMatchesFilters and
// filterByTypeAndProperties fixes.
//
// Bug 1: An empty value list in a filter (e.g. {"tag":[]}) caused
//
//	nodeMatchesFilters to return false for every node, so a client
//	sending {"filters":{"key":[]}} silently got zero results.
//
// Bug 2: filterByType + filterByProperties called sequentially issued two
//
//	per-candidate GetNode round-trips. filterByTypeAndProperties replaces
//	both with a single BatchGetNodes call.
package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newFilterTestEngine(tb testing.TB) storage.Engine {
	tb.Helper()
	eng := storage.NewMemoryEngine()
	tb.Cleanup(func() { eng.Close() })
	return eng
}

// makeNode creates and persists a node, returning it.
func makeNode(tb testing.TB, eng storage.Engine, id string, labels []string, props map[string]any) *storage.Node {
	tb.Helper()
	n := &storage.Node{
		ID:         storage.NodeID(id),
		Labels:     labels,
		Properties: props,
	}
	_, err := eng.CreateNode(n)
	require.NoError(tb, err)
	return n
}

// ---------------------------------------------------------------------------
// nodeMatchesFilters – unit tests (no storage)
// ---------------------------------------------------------------------------

// TestNodeMatchesFilters_EmptyValueList_IgnoresKey is the regression for Bug 1:
// a filter key with an empty slice must be treated as "no constraint" rather
// than "impossible to satisfy", so nodes are NOT discarded.
func TestNodeMatchesFilters_EmptyValueList_IgnoresKey(t *testing.T) {
	node := &storage.Node{
		Properties: map[string]any{"color": "blue"},
	}

	// An empty slice for "collection" must not filter the node out.
	filters := map[string][]string{"collection": {}}
	assert.True(t, nodeMatchesFilters(node, filters),
		"empty filter value list must be treated as no constraint")
}

// TestNodeMatchesFilters_EmptyValueList_WithOtherConstraints verifies that an
// empty-list key is ignored even when other populated keys are present.
func TestNodeMatchesFilters_EmptyValueList_WithOtherConstraints(t *testing.T) {
	node := &storage.Node{
		Properties: map[string]any{"color": "blue", "size": "large"},
	}

	// "color" has a real constraint; "extra" is empty → must still match.
	filters := map[string][]string{
		"color": {"blue"},
		"extra": {},
	}
	assert.True(t, nodeMatchesFilters(node, filters),
		"node satisfying non-empty filters must pass even if other keys have empty lists")
}

// TestNodeMatchesFilters_AllEmpty passes when ALL filter keys have empty lists
// (vacuously true – nothing to constrain).
func TestNodeMatchesFilters_AllEmpty(t *testing.T) {
	node := &storage.Node{
		Properties: map[string]any{},
	}
	filters := map[string][]string{
		"foo": {},
		"bar": {},
	}
	assert.True(t, nodeMatchesFilters(node, filters))
}

// TestNodeMatchesFilters_NilFilters passes trivially (nil map → zero iterations).
func TestNodeMatchesFilters_NilFilters(t *testing.T) {
	node := &storage.Node{Properties: map[string]any{}}
	assert.True(t, nodeMatchesFilters(node, nil))
}

// TestNodeMatchesFilters_PropertyPresent_SingleValue asserts that a node with
// the exact property value passes.
func TestNodeMatchesFilters_PropertyPresent_SingleValue(t *testing.T) {
	node := &storage.Node{
		Properties: map[string]any{"env": "prod"},
	}
	assert.True(t, nodeMatchesFilters(node, map[string][]string{"env": {"prod"}}))
}

// TestNodeMatchesFilters_PropertyMissing returns false when the property is absent.
func TestNodeMatchesFilters_PropertyMissing(t *testing.T) {
	node := &storage.Node{Properties: map[string]any{}}
	assert.False(t, nodeMatchesFilters(node, map[string][]string{"env": {"prod"}}))
}

// TestNodeMatchesFilters_ValueMismatch returns false when the property exists
// but none of the wanted values match.
func TestNodeMatchesFilters_ValueMismatch(t *testing.T) {
	node := &storage.Node{
		Properties: map[string]any{"env": "staging"},
	}
	assert.False(t, nodeMatchesFilters(node, map[string][]string{"env": {"prod", "dev"}}))
}

// TestNodeMatchesFilters_MultiValueOR any of the wanted values satisfies the filter.
func TestNodeMatchesFilters_MultiValueOR(t *testing.T) {
	node := &storage.Node{
		Properties: map[string]any{"env": "dev"},
	}
	assert.True(t, nodeMatchesFilters(node, map[string][]string{"env": {"prod", "dev"}}))
}

// TestNodeMatchesFilters_MultiKeyAND all keys must match.
func TestNodeMatchesFilters_MultiKeyAND(t *testing.T) {
	node := &storage.Node{
		Properties: map[string]any{"env": "prod", "region": "us-east-1"},
	}
	// Both keys satisfied.
	assert.True(t, nodeMatchesFilters(node, map[string][]string{
		"env":    {"prod"},
		"region": {"us-east-1"},
	}))
	// One key fails → overall false.
	assert.False(t, nodeMatchesFilters(node, map[string][]string{
		"env":    {"prod"},
		"region": {"eu-west-1"},
	}))
}

// TestNodeMatchesFilters_SliceProperty matches when any element of a slice
// property equals a wanted value.
func TestNodeMatchesFilters_SliceProperty(t *testing.T) {
	node := &storage.Node{
		Properties: map[string]any{"tags": []any{"go", "database", "graph"}},
	}
	assert.True(t, nodeMatchesFilters(node, map[string][]string{"tags": {"graph"}}))
	assert.False(t, nodeMatchesFilters(node, map[string][]string{"tags": {"python"}}))
}

// ---------------------------------------------------------------------------
// filterByTypeAndProperties – integration tests (with storage)
// ---------------------------------------------------------------------------

// buildFilterResults turns a slice of node IDs into indexResults.
func buildFilterResults(ids ...string) []indexResult {
	out := make([]indexResult, len(ids))
	for i, id := range ids {
		out[i] = indexResult{ID: id, Score: float64(len(ids) - i)}
	}
	return out
}

// TestFilterByTypeAndProperties_EmptyFilterList_ReturnsAll is the end-to-end
// regression for Bug 1: sending Filters with empty value lists must not drop
// any results.
func TestFilterByTypeAndProperties_EmptyFilterList_ReturnsAll(t *testing.T) {
	eng := newFilterTestEngine(t)
	svc := NewService(eng)
	ctx := context.Background()

	makeNode(t, eng, "nornic:n1", []string{"Doc"}, map[string]any{"color": "blue"})
	makeNode(t, eng, "nornic:n2", []string{"Doc"}, map[string]any{"color": "red"})

	results := buildFilterResults("nornic:n1", "nornic:n2")

	// An empty value list for "collection" must not filter out any node.
	filters := map[string][]string{"collection": {}}
	seenOrphans := map[string]bool{}

	out := svc.filterByTypeAndProperties(ctx, results, nil, filters, seenOrphans)

	require.Len(t, out, 2, "empty filter value list must not discard any results")
	assert.Equal(t, "nornic:n1", out[0].ID)
	assert.Equal(t, "nornic:n2", out[1].ID)
}

// TestFilterByTypeAndProperties_NoFilters_ReturnsAll passes the full slice
// through when neither types nor filters are requested.
func TestFilterByTypeAndProperties_NoFilters_ReturnsAll(t *testing.T) {
	eng := newFilterTestEngine(t)
	svc := NewService(eng)
	ctx := context.Background()

	makeNode(t, eng, "nornic:a", []string{"X"}, map[string]any{})
	makeNode(t, eng, "nornic:b", []string{"Y"}, map[string]any{})

	results := buildFilterResults("nornic:a", "nornic:b")
	out := svc.filterByTypeAndProperties(ctx, results, nil, nil, map[string]bool{})
	require.Len(t, out, 2)
}

// TestFilterByTypeAndProperties_TypeFilter_LabelMatch keeps only nodes whose
// Labels contain the requested type (case-insensitive).
func TestFilterByTypeAndProperties_TypeFilter_LabelMatch(t *testing.T) {
	eng := newFilterTestEngine(t)
	svc := NewService(eng)
	ctx := context.Background()

	makeNode(t, eng, "nornic:person1", []string{"Person"}, map[string]any{"name": "Alice"})
	makeNode(t, eng, "nornic:org1", []string{"Organisation"}, map[string]any{"name": "ACME"})
	makeNode(t, eng, "nornic:person2", []string{"Person"}, map[string]any{"name": "Bob"})

	results := buildFilterResults("nornic:person1", "nornic:org1", "nornic:person2")
	out := svc.filterByTypeAndProperties(ctx, results, []string{"person"}, nil, map[string]bool{})

	require.Len(t, out, 2)
	ids := []string{out[0].ID, out[1].ID}
	assert.Contains(t, ids, "nornic:person1")
	assert.Contains(t, ids, "nornic:person2")
}

// TestFilterByTypeAndProperties_TypeFilter_TypeProperty keeps nodes where the
// "type" string property matches (the secondary label path).
func TestFilterByTypeAndProperties_TypeFilter_TypeProperty(t *testing.T) {
	eng := newFilterTestEngine(t)
	svc := NewService(eng)
	ctx := context.Background()

	makeNode(t, eng, "nornic:n1", []string{}, map[string]any{"type": "Widget"})
	makeNode(t, eng, "nornic:n2", []string{}, map[string]any{"type": "Gadget"})

	results := buildFilterResults("nornic:n1", "nornic:n2")
	out := svc.filterByTypeAndProperties(ctx, results, []string{"widget"}, nil, map[string]bool{})

	require.Len(t, out, 1)
	assert.Equal(t, "nornic:n1", out[0].ID)
}

// TestFilterByTypeAndProperties_PropertyFilter_ScalarMatch filters on an exact
// scalar property value.
func TestFilterByTypeAndProperties_PropertyFilter_ScalarMatch(t *testing.T) {
	eng := newFilterTestEngine(t)
	svc := NewService(eng)
	ctx := context.Background()

	makeNode(t, eng, "nornic:prod", []string{"Svc"}, map[string]any{"env": "prod"})
	makeNode(t, eng, "nornic:stage", []string{"Svc"}, map[string]any{"env": "staging"})

	results := buildFilterResults("nornic:prod", "nornic:stage")
	out := svc.filterByTypeAndProperties(ctx, results, nil,
		map[string][]string{"env": {"prod"}}, map[string]bool{})

	require.Len(t, out, 1)
	assert.Equal(t, "nornic:prod", out[0].ID)
}

// TestFilterByTypeAndProperties_PropertyFilter_MultiValueOR keeps nodes that
// match any of the allowed values.
func TestFilterByTypeAndProperties_PropertyFilter_MultiValueOR(t *testing.T) {
	eng := newFilterTestEngine(t)
	svc := NewService(eng)
	ctx := context.Background()

	makeNode(t, eng, "nornic:n1", []string{"X"}, map[string]any{"env": "prod"})
	makeNode(t, eng, "nornic:n2", []string{"X"}, map[string]any{"env": "dev"})
	makeNode(t, eng, "nornic:n3", []string{"X"}, map[string]any{"env": "staging"})

	results := buildFilterResults("nornic:n1", "nornic:n2", "nornic:n3")
	out := svc.filterByTypeAndProperties(ctx, results, nil,
		map[string][]string{"env": {"prod", "dev"}}, map[string]bool{})

	require.Len(t, out, 2)
	ids := []string{out[0].ID, out[1].ID}
	assert.Contains(t, ids, "nornic:n1")
	assert.Contains(t, ids, "nornic:n2")
}

// TestFilterByTypeAndProperties_CombinedTypeAndProperty applies both type and
// property constraints in a single call and verifies the intersection.
func TestFilterByTypeAndProperties_CombinedTypeAndProperty(t *testing.T) {
	eng := newFilterTestEngine(t)
	svc := NewService(eng)
	ctx := context.Background()

	makeNode(t, eng, "nornic:a", []string{"Service"}, map[string]any{"env": "prod"})
	makeNode(t, eng, "nornic:b", []string{"Service"}, map[string]any{"env": "dev"})
	makeNode(t, eng, "nornic:c", []string{"Database"}, map[string]any{"env": "prod"})

	results := buildFilterResults("nornic:a", "nornic:b", "nornic:c")
	out := svc.filterByTypeAndProperties(ctx, results,
		[]string{"service"},
		map[string][]string{"env": {"prod"}},
		map[string]bool{})

	// Only "nornic:a" is a Service AND env=prod.
	require.Len(t, out, 1)
	assert.Equal(t, "nornic:a", out[0].ID)
}

// TestFilterByTypeAndProperties_EmptyValueList_CombinedWithPopulatedFilter
// is the precise regression scenario: {"filters":{"collection":[], "env":["prod"]}}.
// The empty "collection" key must be ignored; only "env" constrains the result.
func TestFilterByTypeAndProperties_EmptyValueList_CombinedWithPopulatedFilter(t *testing.T) {
	eng := newFilterTestEngine(t)
	svc := NewService(eng)
	ctx := context.Background()

	makeNode(t, eng, "nornic:keep", []string{"Svc"}, map[string]any{"env": "prod"})
	makeNode(t, eng, "nornic:drop", []string{"Svc"}, map[string]any{"env": "staging"})

	results := buildFilterResults("nornic:keep", "nornic:drop")
	filters := map[string][]string{
		"collection": {}, // empty → must be a no-op
		"env":        {"prod"},
	}
	out := svc.filterByTypeAndProperties(ctx, results, nil, filters, map[string]bool{})

	require.Len(t, out, 1, "empty-list filter key must not eliminate valid results")
	assert.Equal(t, "nornic:keep", out[0].ID)
}

// TestFilterByTypeAndProperties_AllEmptyFilters_ReturnsAll ensures that when
// every filter key has an empty value list the function is effectively a no-op.
func TestFilterByTypeAndProperties_AllEmptyFilters_ReturnsAll(t *testing.T) {
	eng := newFilterTestEngine(t)
	svc := NewService(eng)
	ctx := context.Background()

	makeNode(t, eng, "nornic:x", []string{"A"}, map[string]any{})
	makeNode(t, eng, "nornic:y", []string{"B"}, map[string]any{})

	results := buildFilterResults("nornic:x", "nornic:y")
	filters := map[string][]string{"tag": {}, "category": {}}

	out := svc.filterByTypeAndProperties(ctx, results, nil, filters, map[string]bool{})
	require.Len(t, out, 2)
}

// TestFilterByTypeAndProperties_ScorePreserved verifies that the original
// indexResult scores are not mutated by the filtering pass.
func TestFilterByTypeAndProperties_ScorePreserved(t *testing.T) {
	eng := newFilterTestEngine(t)
	svc := NewService(eng)
	ctx := context.Background()

	makeNode(t, eng, "nornic:n1", []string{"X"}, map[string]any{"env": "prod"})

	results := []indexResult{{ID: "nornic:n1", Score: 0.987654}}
	out := svc.filterByTypeAndProperties(ctx, results, nil,
		map[string][]string{"env": {"prod"}}, map[string]bool{})

	require.Len(t, out, 1)
	assert.InDelta(t, 0.987654, out[0].Score, 1e-9, "score must be preserved exactly")
}

// TestFilterByTypeAndProperties_OrphanedID_Skipped verifies that a candidate
// whose node no longer exists in storage is silently skipped (not returned)
// when a filter/type constraint forces a storage fetch per candidate.
func TestFilterByTypeAndProperties_OrphanedID_Skipped(t *testing.T) {
	eng := newFilterTestEngine(t)
	svc := NewService(eng)
	ctx := context.Background()

	makeNode(t, eng, "nornic:real", []string{"X"}, map[string]any{})
	// "nornic:ghost" is referenced in the index but has no backing node.

	results := buildFilterResults("nornic:real", "nornic:ghost")
	// Provide a type filter so BatchGetNodes is invoked and the ghost is detected.
	out := svc.filterByTypeAndProperties(ctx, results, []string{"x"}, nil, map[string]bool{})

	// "nornic:ghost" must be silently dropped; only "nornic:real" survives.
	require.Len(t, out, 1)
	assert.Equal(t, "nornic:real", out[0].ID)
}

// TestFilterByTypeAndProperties_EmptyInput returns nil/empty without errors.
func TestFilterByTypeAndProperties_EmptyInput(t *testing.T) {
	eng := newFilterTestEngine(t)
	svc := NewService(eng)
	ctx := context.Background()

	out := svc.filterByTypeAndProperties(ctx, nil, []string{"X"},
		map[string][]string{"env": {"prod"}}, map[string]bool{})
	assert.Empty(t, out)
}
