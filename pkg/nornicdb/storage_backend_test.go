package nornicdb

import (
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

func TestOpen_StorageBackendSelectorTreeDBNotImplemented(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Database.StorageBackend = nornicConfig.StorageBackendTreeDB

	_, err := Open(t.TempDir(), cfg)
	require.ErrorContains(t, err, "treedb storage backend is not implemented yet")
}

func TestOpen_StorageBackendSelectorTreeDBEncryptionFailsClosed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Database.StorageBackend = nornicConfig.StorageBackendTreeDB
	cfg.Database.EncryptionEnabled = true
	cfg.Database.EncryptionPassword = "test-password"

	_, err := Open(t.TempDir(), cfg)
	require.ErrorContains(t, err, "treedb storage backend does not support encryption yet")
}
