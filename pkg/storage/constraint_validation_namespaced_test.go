package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRefreshUniqueConstraint_KeepsPrefixedIDs_UnderNamespacedEngine pins the
// fix at constraint_validation.go:71. Removing EnsureNodeIDDatabasePrefixForEngine
// from the rebuild path would re-introduce the documented "false UNIQUE on
// MATCH/SET against a pre-existing node" failure mode that downstream Bolt
// consumers pin as a contract.
//
// See docs/plans/consumer-pinned-error-contract-plan.md §2.3. Do not relax
// this test without coordinating with known consumers.
func TestRefreshUniqueConstraint_KeepsPrefixedIDs_UnderNamespacedEngine(t *testing.T) {
	inner := NewMemoryEngine()
	t.Cleanup(func() { _ = inner.Close() })

	const namespace = "tenant_a"
	ns := NewNamespacedEngine(inner, namespace)

	// 1. Bootstrap a UNIQUE constraint and one node BEFORE the rebuild,
	//    matching a real consumer's "constraint exists at first boot" case.
	schema := inner.GetSchemaForNamespace(namespace)
	require.NoError(t, schema.AddConstraint(Constraint{
		Name:       "unique_uid",
		Type:       ConstraintUnique,
		Label:      "T",
		Properties: []string{"uid"},
	}))

	// Drive through the namespaced wrapper so the storage ID acquires the
	// namespace prefix the way real consumers do — NOT via the bare engine.
	_, err := ns.CreateNode(&Node{
		ID:         "n-1",
		Labels:     []string{"T"},
		Properties: map[string]any{"uid": "abc-123"},
	})
	require.NoError(t, err)

	// 2. Force the namespace-aware rebuild path. Pre-fix, this populated
	//    the cache with unprefixed IDs because AllNodes-via-the-namespaced-
	//    engine returns user-prefix-stripped nodes.
	require.NoError(t, RefreshUniqueConstraintValuesForEngine(ns, schema))

	// 3. The cache must hold the storage-prefixed ID. CheckUniqueConstraint
	//    against the SAME node's storage ID must NOT raise a violation —
	//    that is the post-fix behavior.
	storageID := EnsureNodeIDDatabasePrefixForEngine(ns, "n-1")
	require.NoError(
		t,
		schema.CheckUniqueConstraint("T", "uid", "abc-123", storageID),
		"false UNIQUE on the matched node itself: see consumer-pinned-error-contract-plan.md §2.3",
	)

	// 4. A different storage ID claiming the same value MUST still violate.
	require.Error(t, schema.CheckUniqueConstraint("T", "uid", "abc-123", NodeID(namespace+":n-2")))
}
