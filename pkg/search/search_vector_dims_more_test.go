package search

import (
	"testing"

	"github.com/orneryd/nornicdb/pkg/gpu"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestService_MaybeAutoSetVectorDimensions_MoreBranches(t *testing.T) {
	// Nil and invalid-dimension guards are no-ops.
	var nilSvc *Service
	nilSvc.maybeAutoSetVectorDimensions(4)

	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)
	oldDims := svc.vectorIndex.GetDimensions()
	svc.maybeAutoSetVectorDimensions(0)
	require.Equal(t, oldDims, svc.vectorIndex.GetDimensions())

	// Prepare dependent indexes that should be invalidated on dimension change.
	embCfg := gpu.DefaultEmbeddingIndexConfig(3)
	embCfg.GPUEnabled = false
	embCfg.AutoSync = false
	svc.gpuEmbeddingIndex = gpu.NewEmbeddingIndex(nil, embCfg)
	clusterIdx := gpu.NewClusterIndex(nil, embCfg, gpu.DefaultKMeansConfig())
	svc.clusterIndex = clusterIdx
	svc.clusterHNSW = map[int]*HNSWIndex{0: NewHNSWIndex(3, DefaultHNSWConfig())}
	svc.ivfpqIndex = &IVFPQIndex{profile: IVFPQProfile{Dimensions: 3}}
	svc.vectorPipeline = NewVectorSearchPipeline(NewBruteForceCandidateGen(svc.vectorIndex), NewCPUExactScorer(svc.vectorIndex))

	svc.maybeAutoSetVectorDimensions(5)
	require.Equal(t, 5, svc.vectorIndex.GetDimensions())
	require.Nil(t, svc.vectorPipeline)
	require.Nil(t, svc.gpuEmbeddingIndex)
	require.NotNil(t, svc.clusterIndex)
	require.Nil(t, svc.clusterHNSW)
	require.Nil(t, svc.ivfpqIndex)

	// Once vectors exist, dimensions must not auto-change.
	require.NoError(t, svc.vectorIndex.Add("keep", []float32{1, 0, 0, 0, 0}))
	svc.ivfpqIndex = &IVFPQIndex{profile: IVFPQProfile{Dimensions: 5}}
	svc.maybeAutoSetVectorDimensions(7)
	require.Equal(t, 5, svc.vectorIndex.GetDimensions())
	require.NotNil(t, svc.ivfpqIndex)
}

func TestService_GetVectorForCypher_MoreBranches(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)

	// vectorIndex path returns a copy, not alias to raw backing storage.
	require.NoError(t, svc.vectorIndex.Add("idx", []float32{1, 0, 0}))
	v, ok := svc.getVectorForCypher("idx")
	require.True(t, ok)
	require.Equal(t, []float32{1, 0, 0}, v)
	v[0] = 9
	v2, ok := svc.getVectorForCypher("idx")
	require.True(t, ok)
	require.Equal(t, float32(1), v2[0])

	// vector file store takes precedence when configured.
	vfs, err := NewVectorFileStore(t.TempDir()+"/vectors", 3)
	require.NoError(t, err)
	defer func() { _ = vfs.Close() }()
	require.NoError(t, vfs.Add("vfs", []float32{0, 1, 0}))
	svc.vectorFileStore = vfs
	v3, ok := svc.getVectorForCypher("vfs")
	require.True(t, ok)
	require.Equal(t, []float32{0, 1, 0}, v3)

	// Missing id returns false.
	_, ok = svc.getVectorForCypher("missing")
	require.False(t, ok)
}

func TestService_MinSimilarityAndCountDimensionFallbacks(t *testing.T) {
	svc := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)

	// resolveMinSimilarity precedence: explicit option > service default > fallback.
	explicit := 0.77
	require.Equal(t, explicit, *svc.resolveMinSimilarity(&SearchOptions{MinSimilarity: &explicit}))

	svc.SetDefaultMinSimilarity(0.33)
	require.Equal(t, 0.33, *svc.resolveMinSimilarity(&SearchOptions{}))

	svc.SetDefaultMinSimilarity(-1)
	require.Equal(t, 0.5, *svc.resolveMinSimilarity(nil))

	// File-store-backed count/dimension branches.
	vfs, err := NewVectorFileStore(t.TempDir()+"/vectors", 4)
	require.NoError(t, err)
	defer func() { _ = vfs.Close() }()
	require.NoError(t, vfs.Add("nornic:v1", []float32{1, 0, 0, 0}))
	svc.vectorFileStore = vfs

	require.Equal(t, 1, svc.EmbeddingCount())
	require.Equal(t, 4, svc.VectorIndexDimensions())

	// Empty file store should fall back to in-memory index for count/dimensions.
	svcFallback := NewServiceWithDimensions(storage.NewMemoryEngine(), 3)
	emptyVFS, err := NewVectorFileStore(t.TempDir()+"/vectors-empty", 7)
	require.NoError(t, err)
	defer func() { _ = emptyVFS.Close() }()
	svcFallback.vectorFileStore = emptyVFS
	require.Equal(t, 0, svcFallback.EmbeddingCount())
	require.Equal(t, 3, svcFallback.VectorIndexDimensions())

	// No vector backends => zero count/dimensions fallback branches.
	svcNoIndex := NewService(storage.NewMemoryEngine())
	svcNoIndex.vectorIndex = nil
	require.Equal(t, 0, svcNoIndex.EmbeddingCount())
	require.Equal(t, 0, svcNoIndex.VectorIndexDimensions())
}
