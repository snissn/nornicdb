package nornicdb

import (
	"context"
	"path/filepath"
	"testing"

	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/retention"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDBCachingAccessorAndNoopBranches(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	queue := NewEmbedQueue(nil, engine, &EmbedQueueConfig{DeferWorkerStart: true})
	t.Cleanup(func() { queue.Close() })

	db := &DB{
		storage:    engine,
		config:     &nornicConfig.Config{},
		embedQueue: queue,
		buildCtx:   context.Background(),
	}

	require.Equal(t, queue, db.GetEmbedQueue())
	require.Nil(t, db.GetAccessFlusher())
	require.Nil(t, db.GetReplicator())
	require.Nil(t, db.GetRetentionManager())

	called := false
	db.SetRetentionAuditCallback(func(action, recordID, category string) { called = true })
	require.NotNil(t, db.onRetentionAction)
	db.onRetentionAction("delete", "n1", "doc")
	require.True(t, called)

	shouldYield := func() bool { return true }
	db.SetEmbedQueueShouldYield(shouldYield)
	require.NotNil(t, db.embedQueueYieldFn)
	queue.mu.Lock()
	gotYield := queue.shouldYield
	queue.mu.Unlock()
	require.NotNil(t, gotYield)
	require.True(t, gotYield())

	db.RunRetentionSweep(context.Background())
	db.startRetentionSweep(context.Background())
	require.Zero(t, (*EmbedQueue)(nil).QueueLen())
}

func TestDBRetentionPoliciesPathBranches(t *testing.T) {
	require.Equal(t, "retention-policies.json", (*DB)(nil).retentionPoliciesPath())
	require.Equal(t, "retention-policies.json", (&DB{}).retentionPoliciesPath())

	db := &DB{config: &nornicConfig.Config{}}
	require.Equal(t, "retention-policies.json", db.retentionPoliciesPath())

	db.config.Database.DataDir = t.TempDir()
	require.Equal(t, filepath.Join(db.config.Database.DataDir, "retention-policies.json"), db.retentionPoliciesPath())

	db.config.Retention.PoliciesFile = "custom-retention.json"
	require.Equal(t, "custom-retention.json", db.retentionPoliciesPath())
}

func TestDBCollectSubjectRetentionRecords(t *testing.T) {
	engine := storage.NewMemoryEngine()
	t.Cleanup(func() { _ = engine.Close() })
	_, err := engine.CreateNode(&storage.Node{ID: "nornic:n1", Labels: []string{"Doc"}, Properties: map[string]interface{}{"owner_id": "alice", "category": "note"}})
	require.NoError(t, err)
	_, err = engine.CreateNode(&storage.Node{ID: "nornic:n2", Labels: []string{"Doc"}, Properties: map[string]interface{}{"owner_id": "bob"}})
	require.NoError(t, err)

	db := &DB{storage: engine, config: &nornicConfig.Config{}}
	records, err := db.CollectSubjectRetentionRecords(context.Background(), "alice")
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, "nornic:n1", records[0].ID)
	require.Equal(t, "alice", records[0].SubjectID)
	require.Equal(t, retention.CategoryUser, records[0].Category)

	records, err = db.CollectSubjectRetentionRecords(context.Background(), "missing")
	require.NoError(t, err)
	require.Empty(t, records)
}
