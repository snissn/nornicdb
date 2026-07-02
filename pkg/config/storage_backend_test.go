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

func TestNormalizeStorageBackend(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "empty defaults to badger", input: "", want: StorageBackendBadger},
		{name: "badger canonical", input: " Badger ", want: StorageBackendBadger},
		{name: "treedb canonical", input: "TreeDB", want: StorageBackendTreeDB},
		{name: "memory canonical", input: "MEMORY", want: StorageBackendMemory},
		{name: "invalid rejected", input: "sqlite", want: "sqlite", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeStorageBackend(tt.input)
			require.Equal(t, tt.want, got)
			err := ValidateStorageBackend(got)
			if tt.wantErr {
				require.ErrorContains(t, err, "invalid storage backend")
				return
			}
			require.NoError(t, err)
		})
	}
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
