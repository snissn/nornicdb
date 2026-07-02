package nornicdb

import (
	"context"
	"errors"
	"testing"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestOpen_StorageBackendSelectorKeepsLegacyMemoryMode(t *testing.T) {
	cfg := DefaultConfig()

	db, err := Open("", cfg)
	require.NoError(t, err)
	defer db.Close()

	require.Equal(t, nornicConfig.StorageBackendMemory, db.config.Database.StorageBackend)
	require.IsType(t, &storage.MemoryEngine{}, db.GetBaseStorageForManager())
}

func TestOpen_StorageBackendSelectorMemoryRequiresEmptyDataDir(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Database.StorageBackend = nornicConfig.StorageBackendMemory

	_, err := Open(t.TempDir(), cfg)
	require.ErrorContains(t, err, "memory storage backend cannot be used with data directory")
}

func TestOpen_StorageBackendSelectorTreeDBOpens(t *testing.T) {
	cfg := treeDBStorageTestConfig()

	db, err := Open(t.TempDir(), cfg)
	require.NoError(t, err)
	defer db.Close()

	require.Equal(t, nornicConfig.StorageBackendTreeDB, db.config.Database.StorageBackend)
	require.False(t, db.config.Database.AsyncWritesEnabled)
	require.IsType(t, &storage.TreeDBEngine{}, db.GetBaseStorageForManager())
}

func TestOpen_StorageBackendSelectorTreeDBUsesNativeDurabilityPath(t *testing.T) {
	cfg := treeDBStorageTestConfig()
	cfg.Database.StrictDurability = true
	cfg.Database.AsyncWritesEnabled = true

	db, err := Open(t.TempDir(), cfg)
	require.NoError(t, err)
	defer db.Close()

	base := db.GetBaseStorageForManager()
	treeEngine, ok := base.(*storage.TreeDBEngine)
	require.True(t, ok)
	require.Nil(t, db.wal)
	require.False(t, db.config.Database.AsyncWritesEnabled)
	_, isWAL := base.(*storage.WALEngine)
	require.False(t, isWAL)
	_, isAsync := base.(*storage.AsyncEngine)
	require.False(t, isAsync)

	info := treeEngine.DurabilityInfo()
	require.True(t, info.NativeWAL)
	require.False(t, info.NornicWAL)
	require.False(t, info.CommandWAL)
	require.False(t, info.AsyncWrites)
	require.True(t, info.SyncWrites)
	require.False(t, info.ReplicationSupported)
}

func TestOpen_StorageBackendSelectorTreeDBCreateGetReopen(t *testing.T) {
	cfg := treeDBStorageTestConfig()
	dir := t.TempDir()
	ctx := context.Background()

	db, err := Open(dir, cfg)
	require.NoError(t, err)

	created, err := db.CreateNode(ctx, []string{"TreeDBOpen"}, map[string]interface{}{"name": "alice"})
	require.NoError(t, err)
	got, err := db.GetNode(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, "alice", got.Properties["name"])
	require.NoError(t, db.Close())

	reopened, err := Open(dir, cfg)
	require.NoError(t, err)
	defer reopened.Close()

	again, err := reopened.GetNode(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, "alice", again.Properties["name"])
	require.Equal(t, []string{"TreeDBOpen"}, again.Labels)
}

func TestOpen_StorageBackendSelectorTreeDBRequiresPersistentDataDir(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Database.StorageBackend = nornicConfig.StorageBackendTreeDB

	_, err := Open("", cfg)
	require.ErrorContains(t, err, "treedb storage backend requires a persistent data directory")
}

func TestOpen_StorageBackendSelectorTreeDBEncryptionFailsClosed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Database.StorageBackend = nornicConfig.StorageBackendTreeDB
	cfg.Database.EncryptionEnabled = true
	cfg.Database.EncryptionPassword = "test-password"

	_, err := Open(t.TempDir(), cfg)
	require.ErrorContains(t, err, "treedb storage backend does not support encryption yet")
}

func TestOpen_StorageBackendSelectorTreeDBMemoryDecayFailsClosed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Database.StorageBackend = nornicConfig.StorageBackendTreeDB
	cfg.Memory.DecayEnabled = true

	_, err := Open(t.TempDir(), cfg)
	require.ErrorContains(t, err, "treedb storage backend does not support memory decay yet")
}

func TestOpen_StorageBackendSelectorTreeDBClusterModeFailsClosed(t *testing.T) {
	t.Setenv("NORNICDB_CLUSTER_MODE", "raft")
	cfg := treeDBStorageTestConfig()

	_, err := Open(t.TempDir(), cfg)
	require.Error(t, err)
	require.True(t, errors.Is(err, storage.ErrNotImplemented), "err=%v", err)
	require.ErrorContains(t, err, "treedb cluster replication integration")
}

func treeDBStorageTestConfig() *Config {
	cfg := DefaultConfig()
	cfg.Database.StorageBackend = nornicConfig.StorageBackendTreeDB
	cfg.Memory.SearchBM25Enabled = false
	cfg.Memory.SearchBM25Warming = "lazy"
	cfg.Memory.SearchVectorEnabled = false
	cfg.Memory.SearchVectorWarming = "lazy"
	cfg.Memory.DecayEnabled = false
	return cfg
}
