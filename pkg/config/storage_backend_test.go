package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadDefaults_StorageBackend(t *testing.T) {
	cfg := LoadDefaults()
	require.Equal(t, StorageBackendBadger, cfg.Database.StorageBackend)
	require.NoError(t, cfg.Validate())
}

func TestLoadFromEnv_StorageBackend(t *testing.T) {
	t.Setenv("NORNICDB_STORAGE_BACKEND", "TreeDB")

	cfg := LoadFromEnv()
	require.Equal(t, StorageBackendTreeDB, cfg.Database.StorageBackend)
}

func TestLoadFromFile_StorageBackendYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nornicdb.yaml")
	err := os.WriteFile(path, []byte(`
storage:
  backend: badger
database:
  storage_backend: treedb
`), 0600)
	require.NoError(t, err)

	cfg, err := LoadFromFile(path)
	require.NoError(t, err)
	require.Equal(t, StorageBackendTreeDB, cfg.Database.StorageBackend)
}

func TestLoadFromFile_StorageBackendEnvPrecedence(t *testing.T) {
	t.Setenv("NORNICDB_STORAGE_BACKEND", "memory")
	path := filepath.Join(t.TempDir(), "nornicdb.yaml")
	err := os.WriteFile(path, []byte(`
database:
  storage_backend: badger
`), 0600)
	require.NoError(t, err)

	cfg, err := LoadFromFile(path)
	require.NoError(t, err)
	require.Equal(t, StorageBackendMemory, cfg.Database.StorageBackend)
}

func TestValidate_InvalidStorageBackend(t *testing.T) {
	cfg := LoadDefaults()
	cfg.Database.StorageBackend = "sqlite"

	require.ErrorContains(t, cfg.Validate(), "invalid storage backend")
}
