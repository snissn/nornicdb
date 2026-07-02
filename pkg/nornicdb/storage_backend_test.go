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

func TestOpen_StorageBackendSelectorTreeDBCapabilities(t *testing.T) {
	cfg := treeDBStorageTestConfig()

	db, err := Open(t.TempDir(), cfg)
	require.NoError(t, err)
	defer db.Close()

	caps := db.StorageCapabilities()
	require.Equal(t, nornicConfig.StorageBackendTreeDB, caps.Backend)
	require.True(t, caps.StorageMaintenance)
	require.True(t, caps.StorageByteStats)
	require.True(t, caps.StorageDiagnostics)
	require.True(t, caps.TreeDBDurability)
	require.False(t, caps.TemporalMaintenance)
	require.False(t, caps.MVCCMaintenance)
	require.False(t, caps.MVCCLifecycle)

	status, err := db.LifecycleStatus()
	require.NoError(t, err)
	require.Equal(t, false, status["enabled"])
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

func TestOpen_StorageBackendSelectorBackendNeutralGraphWorkflow(t *testing.T) {
	tests := []struct {
		name    string
		backend string
	}{
		{name: "badger", backend: nornicConfig.StorageBackendBadger},
		{name: "treedb", backend: nornicConfig.StorageBackendTreeDB},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx := context.Background()

			db, err := Open(dir, backendWorkflowTestConfig(tt.backend))
			require.NoError(t, err)

			author, err := db.CreateNode(ctx,
				[]string{"N1WorkflowPerson", "Author"},
				map[string]interface{}{"name": "Ada", "kind": "person"},
			)
			require.NoError(t, err)

			document, err := db.CreateNode(ctx,
				[]string{"N1WorkflowDocument"},
				map[string]interface{}{"title": "N1 draft", "version": "1"},
			)
			require.NoError(t, err)

			updatedDocument, err := db.UpdateNode(ctx, document.ID, map[string]interface{}{
				"title":  "N1 validated",
				"status": "reviewed",
			})
			require.NoError(t, err)
			require.Equal(t, "N1 validated", updatedDocument.Properties["title"])
			require.Equal(t, "1", updatedDocument.Properties["version"])
			require.Equal(t, "reviewed", updatedDocument.Properties["status"])
			require.ElementsMatch(t, []string{"N1WorkflowDocument"}, updatedDocument.Labels)

			createdEdge, err := db.CreateEdge(ctx, author.ID, document.ID, "AUTHORED", map[string]interface{}{
				"role": "primary",
			})
			require.NoError(t, err)
			require.Equal(t, author.ID, createdEdge.Source)
			require.Equal(t, document.ID, createdEdge.Target)
			require.Equal(t, "AUTHORED", createdEdge.Type)
			require.Equal(t, "primary", createdEdge.Properties["role"])

			readEdge, err := db.GetEdge(ctx, createdEdge.ID)
			require.NoError(t, err)
			require.Equal(t, createdEdge.ID, readEdge.ID)
			require.Equal(t, "primary", readEdge.Properties["role"])

			edgesForAuthor, err := db.GetEdgesForNode(ctx, author.ID)
			require.NoError(t, err)
			requireEdgePresent(t, edgesForAuthor, createdEdge.ID)

			labels, err := db.GetLabels(ctx)
			require.NoError(t, err)
			require.Contains(t, labels, "N1WorkflowPerson")
			require.Contains(t, labels, "Author")
			require.Contains(t, labels, "N1WorkflowDocument")

			types, err := db.GetRelationshipTypes(ctx)
			require.NoError(t, err)
			require.Contains(t, types, "AUTHORED")

			transient, err := db.CreateNode(ctx,
				[]string{"N1WorkflowTransient"},
				map[string]interface{}{"state": "temporary"},
			)
			require.NoError(t, err)
			transientEdge, err := db.CreateEdge(ctx, transient.ID, author.ID, "TEMP_REL", map[string]interface{}{"scope": "delete"})
			require.NoError(t, err)
			require.NoError(t, db.DeleteNode(ctx, transient.ID))
			_, err = db.GetNode(ctx, transient.ID)
			require.ErrorIs(t, err, ErrNotFound)
			_, err = db.GetEdge(ctx, transientEdge.ID)
			require.ErrorIs(t, err, ErrNotFound)

			require.NoError(t, db.Close())

			reopened, err := Open(dir, backendWorkflowTestConfig(tt.backend))
			require.NoError(t, err)
			defer reopened.Close()

			reopenedAuthor, err := reopened.GetNode(ctx, author.ID)
			require.NoError(t, err)
			require.Equal(t, "Ada", reopenedAuthor.Properties["name"])
			require.Equal(t, "person", reopenedAuthor.Properties["kind"])
			require.ElementsMatch(t, []string{"N1WorkflowPerson", "Author"}, reopenedAuthor.Labels)

			reopenedDocument, err := reopened.GetNode(ctx, document.ID)
			require.NoError(t, err)
			require.Equal(t, "N1 validated", reopenedDocument.Properties["title"])
			require.Equal(t, "1", reopenedDocument.Properties["version"])
			require.Equal(t, "reviewed", reopenedDocument.Properties["status"])
			require.ElementsMatch(t, []string{"N1WorkflowDocument"}, reopenedDocument.Labels)

			reopenedEdge, err := reopened.GetEdge(ctx, createdEdge.ID)
			require.NoError(t, err)
			require.Equal(t, author.ID, reopenedEdge.Source)
			require.Equal(t, document.ID, reopenedEdge.Target)
			require.Equal(t, "AUTHORED", reopenedEdge.Type)
			require.Equal(t, "primary", reopenedEdge.Properties["role"])

			reopenedEdgesForDocument, err := reopened.GetEdgesForNode(ctx, document.ID)
			require.NoError(t, err)
			requireEdgePresent(t, reopenedEdgesForDocument, createdEdge.ID)

			reopenedLabels, err := reopened.GetLabels(ctx)
			require.NoError(t, err)
			require.Contains(t, reopenedLabels, "N1WorkflowPerson")
			require.Contains(t, reopenedLabels, "N1WorkflowDocument")
			require.NotContains(t, reopenedLabels, "N1WorkflowTransient")

			reopenedTypes, err := reopened.GetRelationshipTypes(ctx)
			require.NoError(t, err)
			require.Contains(t, reopenedTypes, "AUTHORED")
			require.NotContains(t, reopenedTypes, "TEMP_REL")

			_, err = reopened.GetNode(ctx, transient.ID)
			require.ErrorIs(t, err, ErrNotFound)
			_, err = reopened.GetEdge(ctx, transientEdge.ID)
			require.ErrorIs(t, err, ErrNotFound)
		})
	}
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

func backendWorkflowTestConfig(backend string) *Config {
	cfg := DefaultConfig()
	cfg.Database.StorageBackend = backend
	cfg.Database.AsyncWritesEnabled = false
	cfg.Memory.SearchBM25Enabled = false
	cfg.Memory.SearchBM25Warming = "lazy"
	cfg.Memory.SearchVectorEnabled = false
	cfg.Memory.SearchVectorWarming = "lazy"
	cfg.Memory.DecayEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	return cfg
}

func requireEdgePresent(t *testing.T, edges []*GraphEdge, id string) {
	t.Helper()

	for _, edge := range edges {
		if edge.ID == id {
			return
		}
	}
	require.Failf(t, "edge not found", "edge %q not found in %#v", id, edges)
}
