package server

import (
	"context"
	"testing"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/stretchr/testify/require"
)

func TestNew_PerDatabaseRerankerResolverBranches(t *testing.T) {
	tmpDir := t.TempDir()
	db, err := nornicdb.Open(tmpDir, nornicdb.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	cfg := DefaultConfig()
	cfg.MCPEnabled = false
	cfg.EmbeddingEnabled = false
	cfg.Features = &nornicConfig.FeatureFlagsConfig{
		HeimdallEnabled:      false,
		SearchRerankEnabled:  true,
		SearchRerankProvider: "ollama",
		SearchRerankAPIURL:   "",
		SearchRerankModel:    "",
	}

	server, err := New(db, nil, cfg)
	require.NoError(t, err)
	require.NotNil(t, server.dbConfigStore)
	t.Cleanup(func() { _ = server.Stop(context.Background()) })

	dbName := server.dbManager.DefaultDatabaseName()
	storageEngine, err := server.dbManager.GetStorage(dbName)
	require.NoError(t, err)

	// enabled=false override should force nil reranker return path.
	require.NoError(t, server.dbConfigStore.SetOverrides(context.Background(), dbName, map[string]string{
		"NORNICDB_SEARCH_RERANK_ENABLED": "false",
	}))
	server.db.ResetSearchService(dbName)
	_, err = server.db.GetOrCreateSearchService(dbName, storageEngine)
	require.NoError(t, err)

	// local provider path with nil global resolver.
	require.NoError(t, server.dbConfigStore.SetOverrides(context.Background(), dbName, map[string]string{
		"NORNICDB_SEARCH_RERANK_ENABLED":  "true",
		"NORNICDB_SEARCH_RERANK_PROVIDER": "local",
	}))
	server.db.ResetSearchService(dbName)
	_, err = server.db.GetOrCreateSearchService(dbName, storageEngine)
	require.NoError(t, err)

	// external ollama path with default URL fallback and external reranker cache.
	require.NoError(t, server.dbConfigStore.SetOverrides(context.Background(), dbName, map[string]string{
		"NORNICDB_SEARCH_RERANK_ENABLED":  "true",
		"NORNICDB_SEARCH_RERANK_PROVIDER": "ollama",
		"NORNICDB_SEARCH_RERANK_API_URL":  "",
		"NORNICDB_SEARCH_RERANK_MODEL":    "",
		"NORNICDB_SEARCH_RERANK_API_KEY":  "test-key",
	}))
	server.db.ResetSearchService(dbName)
	_, err = server.db.GetOrCreateSearchService(dbName, storageEngine)
	require.NoError(t, err)

	// Second call exercises cached external reranker path.
	server.db.ResetSearchService(dbName)
	_, err = server.db.GetOrCreateSearchService(dbName, storageEngine)
	require.NoError(t, err)
}
