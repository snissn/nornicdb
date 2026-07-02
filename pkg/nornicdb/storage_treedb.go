package nornicdb

import (
	"fmt"
	"os"
	"path/filepath"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type embeddingGate interface {
	SetEmbeddingsEnabled(bool)
}

type embeddingClearer interface {
	ClearAllEmbeddings() (int, error)
	ClearAllEmbeddingsForPrefix(string) (int, error)
}

func openTreeDBStorage(db *DB, dataDir string, config *Config) error {
	if clusterMode := os.Getenv("NORNICDB_CLUSTER_MODE"); clusterMode != "" && clusterMode != string(replication.ModeStandalone) {
		return fmt.Errorf("%w: treedb cluster replication integration is reserved for the WAL/replication lane", storage.ErrNotImplemented)
	}
	if config.Memory.DecayEnabled {
		return fmt.Errorf("%w: treedb storage backend does not support memory decay yet", storage.ErrNotImplemented)
	}

	treeDir := filepath.Join(dataDir, "treedb")
	treeEngine, err := storage.NewTreeDBEngineWithOptions(storage.TreeDBOptions{
		Dir:                 treeDir,
		SyncWrites:          config.Database.StrictDurability,
		NodeCacheMaxEntries: config.Database.BadgerNodeCacheMaxEntries,
		Logger:              config.Logger,
	})
	if err != nil {
		return fmt.Errorf("failed to open TreeDB storage: %w", err)
	}

	config.Database.AsyncWritesEnabled = false

	var baseStorage storage.Engine = treeEngine
	db.baseStorage = baseStorage

	globalConfig := nornicConfig.LoadFromEnv()
	defaultDBName := globalConfig.Database.DefaultDatabase
	if defaultDBName == "" {
		defaultDBName = "nornic"
	}
	db.storage = storage.NewNamespacedEngine(baseStorage, defaultDBName)
	fmt.Printf("📂 Using TreeDB storage at %s (native conditional transactions)\n", treeDir)
	fmt.Printf("📦 Wrapped TreeDB storage with namespace '%s' (all operations are namespaced)\n", defaultDBName)
	return nil
}

func setEmbeddingsEnabledIfSupported(eng storage.Engine, enabled bool) {
	if gate, ok := storage.FindCapability[embeddingGate](eng); ok {
		gate.SetEmbeddingsEnabled(enabled)
	}
}

func findEmbeddingClearer(eng storage.Engine) (embeddingClearer, bool) {
	return storage.FindCapability[embeddingClearer](eng)
}
