package storage

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	"github.com/stretchr/testify/require"
)

func TestTreeDBEngine_DurabilityInfoReportsNativeWAL(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{
		Dir:        dir,
		SyncWrites: true,
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, engine.Close())
	}()

	info := engine.DurabilityInfo()
	require.Equal(t, dir, info.Dir)
	require.Equal(t, string(treedb.ProfileLegacyWALDurable), info.Profile)
	require.NotEmpty(t, info.DurabilityMode)
	require.Equal(t, "cached", info.WritePathMode)
	require.Equal(t, "on", info.RedoLog)
	require.True(t, info.NativeWAL)
	require.False(t, info.CommandWAL)
	require.False(t, info.NornicWAL)
	require.False(t, info.AsyncWrites)
	require.True(t, info.SyncWrites)
	require.False(t, info.ReplicationSupported)
}

func TestTreeDBEngine_DurabilityInfoAfterCloseUsesCachedPolicy(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{
		Dir:        dir,
		SyncWrites: true,
	})
	require.NoError(t, err)
	require.NoError(t, engine.Close())

	var info TreeDBDurabilityInfo
	require.NotPanics(t, func() {
		info = engine.DurabilityInfo()
	})
	require.Equal(t, dir, info.Dir)
	require.Equal(t, string(treedb.ProfileLegacyWALDurable), info.Profile)
	require.Equal(t, "on", info.RedoLog)
	require.True(t, info.NativeWAL)
	require.False(t, info.CommandWAL)
	require.False(t, info.NornicWAL)
	require.False(t, info.AsyncWrites)
	require.True(t, info.SyncWrites)
	require.False(t, info.ReplicationSupported)
	require.Empty(t, info.DurabilityMode)
	require.Empty(t, info.WritePathMode)
}

func TestTreeDBEngine_DurabilityInfoAndStatsConcurrentClose(t *testing.T) {
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{
		Dir:        t.TempDir(),
		SyncWrites: true,
	})
	require.NoError(t, err)

	start := make(chan struct{})
	stop := make(chan struct{})
	var stopOnce sync.Once
	var wg sync.WaitGroup
	var ops atomic.Uint64
	defer func() {
		stopOnce.Do(func() { close(stop) })
		wg.Wait()
	}()

	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				select {
				case <-stop:
					return
				default:
					_ = engine.DurabilityInfo()
					_ = engine.Stats()
					ops.Add(1)
				}
			}
		}()
	}

	close(start)
	require.Eventually(t, func() bool {
		return ops.Load() >= 100
	}, time.Second, time.Millisecond)

	require.NoError(t, engine.Close())
	stopOnce.Do(func() { close(stop) })
}

func TestTreeDBEngine_CommandWALProfilesFailClosed(t *testing.T) {
	for _, profile := range []treedb.Profile{
		treedb.ProfileCommandWALDurable,
		treedb.ProfileCommandWALRelaxed,
	} {
		t.Run(string(profile), func(t *testing.T) {
			_, err := NewTreeDBEngineWithOptions(TreeDBOptions{
				Dir:     t.TempDir(),
				Profile: string(profile),
			})
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrNotImplemented), "err=%v", err)
			require.ErrorContains(t, err, "treedb command WAL profile")
		})
	}
}

func TestTreeDBEngine_DurableTransactionBoundariesSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	open := func(t *testing.T) *TreeDBEngine {
		t.Helper()
		engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{
			Dir:        dir,
			SyncWrites: true,
		})
		require.NoError(t, err)
		return engine
	}

	engine := open(t)
	tx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{
		ID:         "test:tx-a",
		Labels:     []string{"Durable"},
		Properties: map[string]any{"state": "committed"},
	})
	require.NoError(t, err)
	_, err = tx.CreateNode(&Node{
		ID:     "test:tx-b",
		Labels: []string{"Durable"},
	})
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{
		ID:        "test:tx-e",
		StartNode: "test:tx-a",
		EndNode:   "test:tx-b",
		Type:      "LINKS",
	}))
	require.NoError(t, tx.Commit())

	rollbackTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	_, err = rollbackTx.CreateNode(&Node{
		ID:     "test:rolled-back",
		Labels: []string{"Durable"},
	})
	require.NoError(t, err)
	require.NoError(t, rollbackTx.Rollback())

	conflictTx, err := engine.BeginGraphTransaction()
	require.NoError(t, err)
	pending, err := conflictTx.GetNode("test:tx-a")
	require.NoError(t, err)
	pending.Properties["state"] = "lost"
	require.NoError(t, conflictTx.UpdateNode(pending))
	_, err = conflictTx.CreateNode(&Node{
		ID:     "test:conflict-created",
		Labels: []string{"Durable"},
	})
	require.NoError(t, err)

	winner, err := engine.GetNode("test:tx-a")
	require.NoError(t, err)
	winner.Properties["state"] = "winner"
	require.NoError(t, engine.UpdateNode(winner))
	err = conflictTx.Commit()
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrConflict), "commit error=%v", err)

	require.NoError(t, engine.Close())

	reopened := open(t)
	defer func() {
		require.NoError(t, reopened.Close())
	}()
	got, err := reopened.GetNode("test:tx-a")
	require.NoError(t, err)
	require.Equal(t, "winner", got.Properties["state"])
	_, err = reopened.GetEdge("test:tx-e")
	require.NoError(t, err)
	_, err = reopened.GetNode("test:rolled-back")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = reopened.GetNode("test:conflict-created")
	require.ErrorIs(t, err, ErrNotFound)
}
