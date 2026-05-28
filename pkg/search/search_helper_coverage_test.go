package search

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDisabledBM25IndexMethods(t *testing.T) {
	idx := disabledBM25Index{}

	idx.Index("doc-1", "hello world")
	idx.Remove("doc-1")
	idx.Clear()
	idx.IndexBatch([]FulltextBatchEntry{{ID: "doc-2", Text: "batch text"}})

	require.Nil(t, idx.Search("hello", 5))
	require.Nil(t, idx.PhraseSearch("hello world", 5))
	require.Nil(t, idx.LexicalSeedDocIDs(3, 2))
	require.Equal(t, 0, idx.Count())
	require.False(t, idx.IsDirty())

	doc, ok := idx.GetDocument("doc-1")
	require.False(t, ok)
	require.Empty(t, doc)
	require.NoError(t, idx.Save("ignored"))
	require.NoError(t, idx.SaveNoCopy("ignored"))
	require.NoError(t, idx.Load("ignored"))
}

func TestServiceWarmAndIntrospectionHelpers(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { engine.Close() })

	svc := NewServiceWithDimensions(engine, 4)
	require.Equal(t, "unknown", svc.CurrentStrategy())
	require.Equal(t, 0, svc.FulltextDocCount())

	svc.MarkWarmDone()
	svc.MarkWarmDone()
	select {
	case <-svc.warmDone:
	default:
		t.Fatal("MarkWarmDone should close warmDone")
	}

	svc.fulltextIndex = nil
	require.Equal(t, 0, svc.FulltextDocCount())

	svc.MarkReadyDisabled()
	require.True(t, svc.IsReady())
	require.Equal(t, 0, svc.FulltextDocCount())
	require.Equal(t, "v2", svc.BM25Engine())
}

func TestPropertyVectorHelpers(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { engine.Close() })

	svc := NewServiceWithDimensions(engine, 4)
	svc.buildInProgress.Store(true)
	svc.nodePropVector = map[string]map[string]string{
		"node-1": {"title": "node-1-prop-title", "body": "node-1-prop-body"},
		"node-2": {"title": "node-2-prop-title"},
	}

	require.NoError(t, svc.vectorIndex.Add("node-1-prop-title", []float32{1, 0, 0, 0}))
	require.NoError(t, svc.vectorIndex.Add("node-1-prop-body", []float32{0, 1, 0, 0}))
	require.NoError(t, svc.vectorIndex.Add("node-2-prop-title", []float32{0, 0, 1, 0}))

	require.Equal(t, 2, svc.CountPropertyVectorEntries("title"))
	require.Equal(t, 1, svc.CountPropertyVectorEntries("body"))
	require.Equal(t, 0, svc.CountPropertyVectorEntries("missing"))
	require.Equal(t, 0, (*Service)(nil).CountPropertyVectorEntries("title"))

	svc.RemovePropertyVectorIndex("title")
	require.Equal(t, 0, svc.CountPropertyVectorEntries("title"))
	require.Equal(t, 1, svc.CountPropertyVectorEntries("body"))
	require.Contains(t, svc.nodePropVector["node-1"], "body")
	_, exists := svc.nodePropVector["node-2"]
	require.False(t, exists)

	svc.RemovePropertyVectorIndex("")
	require.Equal(t, 1, svc.CountPropertyVectorEntries("body"))
}

func TestFilterByPropertiesDirect(t *testing.T) {
	eng := newFilterTestEngine(t)
	svc := NewService(eng)
	ctx := context.Background()

	makeNode(t, eng, "nornic:n1", []string{"Doc"}, map[string]any{"color": "blue", "tag": "keep"})
	makeNode(t, eng, "nornic:n2", []string{"Doc"}, map[string]any{"color": "red", "tag": "skip"})

	results := buildFilterResults("nornic:n1", "nornic:n2", "nornic:missing")
	seenOrphans := map[string]bool{}

	filtered := svc.filterByProperties(ctx, results, map[string][]string{"color": {"blue"}}, seenOrphans)
	require.Len(t, filtered, 1)
	require.Equal(t, "nornic:n1", filtered[0].ID)
	require.True(t, seenOrphans["nornic:missing"])

	all := svc.filterByProperties(ctx, results[:2], nil, seenOrphans)
	require.Equal(t, results[:2], all)
}
