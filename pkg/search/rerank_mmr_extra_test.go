package search

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type coverageReranker struct {
	enabled bool
	results []RerankResult
	err     error
	seen    []RerankCandidate
}

func (r *coverageReranker) Name() string  { return "coverage_reranker" }
func (r *coverageReranker) Enabled() bool { return r.enabled }
func (r *coverageReranker) IsAvailable(ctx context.Context) bool {
	return r.enabled
}
func (r *coverageReranker) Rerank(ctx context.Context, query string, candidates []RerankCandidate) ([]RerankResult, error) {
	r.seen = append([]RerankCandidate(nil), candidates...)
	return r.results, r.err
}

func TestSearchRerankExtraApplyMMRBranches(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { engine.Close() })
	svc := NewServiceWithDimensions(engine, 2)

	results := []rrfResult{{ID: "nornic:a", RRFScore: 0.9}, {ID: "nornic:b", RRFScore: 0.8}, {ID: "nornic:c", RRFScore: 0.7}}
	require.Equal(t, results[:1], svc.applyMMR(context.Background(), results[:1], []float32{1, 0}, 3, 0.5, nil))
	require.Equal(t, results, svc.applyMMR(context.Background(), results, []float32{1, 0}, 3, 1.0, nil))

	_, err := engine.CreateNode(&storage.Node{ID: "nornic:a", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 0}}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "nornic:b", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{1, 0}}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "nornic:c", Labels: []string{"Doc"}, ChunkEmbeddings: [][]float32{{0, 1}}})
	require.NoError(t, err)

	diverse := svc.applyMMR(context.Background(), results, []float32{1, 0}, 2, 0.2, map[string]bool{})
	require.Len(t, diverse, 2)
	require.Equal(t, "nornic:a", diverse[0].ID)
	require.NotEqual(t, diverse[0].ID, diverse[1].ID)

	noEmbeddingResults := []rrfResult{{ID: "nornic:d", RRFScore: 0.6}}
	_, err = engine.CreateNode(&storage.Node{ID: "nornic:d", Labels: []string{"Doc"}})
	require.NoError(t, err)
	selected := svc.applyMMR(context.Background(), noEmbeddingResults, []float32{1, 0}, 3, 0.5, map[string]bool{})
	require.Equal(t, noEmbeddingResults, selected)
}

func TestSearchRerankExtraApplyStage2Branches(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { engine.Close() })
	svc := NewServiceWithDimensions(engine, 2)
	ctx := context.Background()

	base := []rrfResult{{ID: "nornic:a", RRFScore: 0.9, VectorRank: 1, BM25Rank: 2}, {ID: "nornic:b", RRFScore: 0.8, VectorRank: 2, BM25Rank: 1}}
	require.Equal(t, base, svc.applyStage2Rerank(ctx, "query", base, &SearchOptions{}, nil, nil))
	require.Equal(t, []rrfResult{}, svc.applyStage2Rerank(ctx, "query", []rrfResult{}, &SearchOptions{}, nil, &coverageReranker{enabled: true}))
	require.Equal(t, base, svc.applyStage2Rerank(ctx, "query", base, &SearchOptions{}, nil, &coverageReranker{enabled: false}))

	_, err := engine.CreateNode(&storage.Node{ID: "nornic:a", Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "Alpha", "content": "alpha content"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "nornic:b", Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "Beta", "content": "beta content"}})
	require.NoError(t, err)

	failing := &coverageReranker{enabled: true, err: errors.New("rerank failed")}
	require.Equal(t, base[:1], svc.applyStage2Rerank(ctx, "query", base, &SearchOptions{RerankTopK: 1}, nil, failing))
	require.Len(t, failing.seen, 1)

	flat := &coverageReranker{enabled: true, results: []RerankResult{{ID: "nornic:b", BiScore: 0.8, FinalScore: 0.51}, {ID: "nornic:a", BiScore: 0.9, FinalScore: 0.50}}}
	require.Equal(t, base, svc.applyStage2Rerank(ctx, "query", base, &SearchOptions{}, nil, flat))

	reranker := &coverageReranker{enabled: true, results: []RerankResult{{ID: "nornic:b", BiScore: 0.8, FinalScore: 0.95}, {ID: "missing", BiScore: 0.1, FinalScore: 0.2}, {ID: "nornic:a", BiScore: 0.9, FinalScore: 0.1}}}
	reranked := svc.applyStage2Rerank(ctx, "query", base, &SearchOptions{RerankMinScore: 0.15}, nil, reranker)
	require.Equal(t, []rrfResult{{ID: "nornic:b", RRFScore: 0.95, VectorRank: 2, BM25Rank: 1, OriginalScore: 0.8}, {ID: "missing", RRFScore: 0.2, OriginalScore: 0.1}}, reranked)
}
