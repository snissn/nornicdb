package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExtractIndexedEqualityFromWhereTerm_Branches(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "merge_extract_idx_cov"))

	prop, lit, ok := exec.extractIndexedEqualityFromWhereTerm("n", "")
	require.False(t, ok)
	require.Empty(t, prop)
	require.Nil(t, lit)

	prop, lit, ok = exec.extractIndexedEqualityFromWhereTerm("n", "(n.key = 'v1')")
	require.True(t, ok)
	require.Equal(t, "key", prop)
	require.Equal(t, "v1", lit)

	prop, lit, ok = exec.extractIndexedEqualityFromWhereTerm("n", "n.key = 'v1' AND 'x' IS NOT NULL")
	require.True(t, ok)
	require.Equal(t, "key", prop)
	require.Equal(t, "v1", lit)

	_, _, ok = exec.extractIndexedEqualityFromWhereTerm("n", "n.key = 'v1' AND n.other > 2")
	require.False(t, ok)

	_, _, ok = exec.extractIndexedEqualityFromWhereTerm("n", "n.key > 'v1'")
	require.False(t, ok)
}

func TestLookupWhereCandidatesUsingPropertyIndex_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "merge_lookup_idx_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE (:Doc {id:'d1', key:'k1'}), (:Doc {id:'d2', key:'k2'})", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE INDEX idx_doc_key_cov IF NOT EXISTS FOR (n:Doc) ON (n.key)", nil)
	require.NoError(t, err)

	nodes, ok := exec.lookupWhereCandidatesUsingPropertyIndex(
		nodePatternInfo{variable: "n"},
		"n.key = 'k1'",
		store,
	)
	require.False(t, ok)
	require.Nil(t, nodes)

	nodes, ok = exec.lookupWhereCandidatesUsingPropertyIndex(
		nodePatternInfo{variable: "n", labels: []string{"Doc"}},
		"n.key = 'k1' OR n.key = 'k2'",
		store,
	)
	require.False(t, ok)
	require.Nil(t, nodes)

	nodes, ok = exec.lookupWhereCandidatesUsingPropertyIndex(
		nodePatternInfo{variable: "n", labels: []string{"Doc"}},
		"n.unknown = 'x'",
		store,
	)
	require.False(t, ok)
	require.Nil(t, nodes)

	nodes, ok = exec.lookupWhereCandidatesUsingPropertyIndex(
		nodePatternInfo{variable: "n", labels: []string{"Doc"}},
		"n.key = 'k2' AND 'safe' IS NOT NULL",
		store,
	)
	require.True(t, ok)
	require.Len(t, nodes, 1)
	require.Equal(t, "d2", nodes[0].Properties["id"])
}
