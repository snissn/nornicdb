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

func TestTreeDBEngine_PersistenceReopenCyclesRetainGraphIndexesSchemaAndMetadata(t *testing.T) {
	dir := t.TempDir()
	open := func(t *testing.T) *TreeDBEngine {
		t.Helper()
		engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{
			Dir:        dir,
			SyncWrites: true,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = engine.Close() })
		return engine
	}

	engine := open(t)
	requireTreeDBDurabilitySchema(t, engine)
	engine.SetEmbeddingsEnabled(true)

	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{
			ID:     "test:alice",
			Labels: []string{"Person", "Employee"},
			Properties: map[string]any{
				"name":              "Alice",
				"email":             "alice@example.com",
				"country":           "US",
				"city":              "NYC",
				"rank":              int64(10),
				"active":            true,
				"embedding_skipped": true,
			},
		},
		{
			ID:     "test:bob",
			Labels: []string{"Person"},
			Properties: map[string]any{
				"name":              "Bob",
				"email":             "bob@example.com",
				"country":           "US",
				"city":              "NYC",
				"rank":              int64(20),
				"embedding_skipped": true,
			},
		},
		{
			ID:         "test:pending-doc",
			Labels:     []string{"Doc"},
			Properties: map[string]any{"text": "needs embedding"},
		},
	}))
	require.Equal(t, 1, engine.PendingEmbeddingsCount())

	require.NoError(t, engine.UpdateNodeEmbedding(&Node{
		ID:              "test:alice",
		ChunkEmbeddings: [][]float32{{0.1, 0.2, 0.3}},
		EmbedMeta:       map[string]any{"embedding_model": "mini", "has_embedding": true},
	}))

	require.NoError(t, engine.CreateEdge(&Edge{
		ID:        "test:knows",
		StartNode: "test:alice",
		EndNode:   "test:bob",
		Type:      "KNOWS",
		Properties: map[string]any{
			"since":  int64(2024),
			"weight": 0.75,
		},
	}))

	require.NoError(t, engine.Sync())
	requireTreeDBDurableGraphState(t, engine, treeDBDurableGraphExpectation{
		aliceLabels: []string{"Person", "Employee"},
		aliceCity:   "NYC",
		employeeIDs: []NodeID{"test:alice"},
	})
	require.NoError(t, engine.Close())

	reopened := open(t)
	requireTreeDBDurableGraphState(t, reopened, treeDBDurableGraphExpectation{
		aliceLabels:         []string{"Person", "Employee"},
		aliceCity:           "NYC",
		employeeIDs:         []NodeID{"test:alice"},
		uniqueCacheComplete: true,
	})

	alice, err := reopened.GetNode("test:alice")
	require.NoError(t, err)
	alice.Labels = []string{"Person", "Reviewer"}
	alice.Properties["city"] = "LA"
	alice.Properties["rank"] = int64(42)
	require.NoError(t, reopened.UpdateNode(alice))
	require.NoError(t, reopened.Sync())
	requireTreeDBDurableGraphState(t, reopened, treeDBDurableGraphExpectation{
		aliceLabels:         []string{"Person", "Reviewer"},
		aliceCity:           "LA",
		reviewerIDs:         []NodeID{"test:alice"},
		uniqueCacheComplete: true,
	})
	require.NoError(t, reopened.Close())

	reopenedAgain := open(t)
	defer func() {
		require.NoError(t, reopenedAgain.Close())
	}()
	requireTreeDBDurableGraphState(t, reopenedAgain, treeDBDurableGraphExpectation{
		aliceLabels:         []string{"Person", "Reviewer"},
		aliceCity:           "LA",
		reviewerIDs:         []NodeID{"test:alice"},
		uniqueCacheComplete: true,
	})
}

func TestTreeDBEngine_SyncPersistsWritesBeforeReopenAndClosedEngineFailsClosed(t *testing.T) {
	dir := t.TempDir()
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	require.False(t, engine.DurabilityInfo().SyncWrites)

	require.NoError(t, engine.BulkCreateNodes([]*Node{
		{ID: "test:sync-a", Labels: []string{"Sync"}, Properties: map[string]any{"name": "a"}},
		{ID: "test:sync-b", Labels: []string{"Sync"}, Properties: map[string]any{"name": "b"}},
	}))
	require.NoError(t, engine.CreateEdge(&Edge{
		ID:        "test:sync-edge",
		StartNode: "test:sync-a",
		EndNode:   "test:sync-b",
		Type:      "SYNCED",
	}))
	require.NoError(t, engine.Sync())
	require.NoError(t, engine.Close())

	reopened, err := NewTreeDBEngineWithOptions(TreeDBOptions{Dir: dir})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	got, err := reopened.GetNode("test:sync-a")
	require.NoError(t, err)
	require.Equal(t, "a", got.Properties["name"])
	byType, err := reopened.GetEdgesByType("SYNCED")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:sync-edge"}, treeDBEdgeIDs(byType))
	require.NoError(t, reopened.Close())

	require.ErrorIs(t, reopened.Sync(), ErrStorageClosed)
	_, err = reopened.CreateNode(&Node{ID: "test:closed", Labels: []string{"Closed"}})
	require.ErrorIs(t, err, ErrStorageClosed)
	require.Nil(t, reopened.Stats())
}

func TestTreeDBEngine_CapabilitySnapshotDocumentsDurabilityLimits(t *testing.T) {
	engine, err := NewTreeDBEngineWithOptions(TreeDBOptions{
		Dir:        t.TempDir(),
		SyncWrites: true,
	})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, engine.Close())
	}()

	caps := InspectEngineCapabilities(engine)
	require.Equal(t, "treedb", caps.Backend)
	require.True(t, caps.StorageMaintenance)
	require.True(t, caps.StorageByteStats)
	require.True(t, caps.StorageDiagnostics)
	require.True(t, caps.TreeDBDurability)
	require.False(t, caps.TemporalMaintenance)
	require.False(t, caps.MVCCMaintenance)
	require.False(t, caps.MVCCLifecycle)

	info := engine.DurabilityInfo()
	require.True(t, info.NativeWAL)
	require.False(t, info.CommandWAL)
	require.False(t, info.NornicWAL)
	require.False(t, info.AsyncWrites)
	require.False(t, info.ReplicationSupported)
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
		t.Cleanup(func() { _ = engine.Close() })
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

type treeDBDurableGraphExpectation struct {
	aliceLabels         []string
	aliceCity           string
	employeeIDs         []NodeID
	reviewerIDs         []NodeID
	uniqueCacheComplete bool
}

func requireTreeDBDurabilitySchema(t *testing.T, engine *TreeDBEngine) {
	t.Helper()

	schema := engine.GetSchemaForNamespace("test")
	require.NoError(t, schema.AddUniqueConstraint("person_email_unique", "Person", "email"))
	require.NoError(t, schema.AddPropertyTypeConstraint("person_rank_type", "Person", "rank", PropertyTypeInteger))
	require.NoError(t, schema.AddPropertyIndex("person_email_idx", "Person", []string{"email"}))
	require.NoError(t, schema.AddPropertyIndex("person_city_idx", "Person", []string{"city"}))
	require.NoError(t, schema.AddCompositeIndex("person_location_idx", "Person", []string{"country", "city"}))
	require.NoError(t, schema.AddRangeIndex("person_rank_range", "Person", "rank"))
	require.NoError(t, schema.AddFulltextIndex("doc_text_idx", []string{"Doc"}, []string{"text"}))
	require.NoError(t, schema.AddVectorIndex("doc_embedding_idx", "Doc", "embedding", 3, "cosine"))
}

func requireTreeDBDurableGraphState(t *testing.T, engine *TreeDBEngine, want treeDBDurableGraphExpectation) {
	t.Helper()

	alice, err := engine.GetNode("test:alice")
	require.NoError(t, err)
	require.Equal(t, want.aliceLabels, alice.Labels)
	require.Equal(t, "Alice", alice.Properties["name"])
	require.Equal(t, "alice@example.com", alice.Properties["email"])
	require.Equal(t, "US", alice.Properties["country"])
	require.Equal(t, want.aliceCity, alice.Properties["city"])
	require.NotZero(t, alice.CreatedAt)
	require.NotZero(t, alice.UpdatedAt)
	require.Equal(t, [][]float32{{0.1, 0.2, 0.3}}, alice.ChunkEmbeddings)
	require.NotNil(t, alice.EmbedMeta)
	require.Equal(t, "mini", alice.EmbedMeta["embedding_model"])
	require.Equal(t, true, alice.EmbedMeta["has_embedding"])

	bob, err := engine.GetNode("test:bob")
	require.NoError(t, err)
	require.Equal(t, []string{"Person"}, bob.Labels)
	require.Equal(t, "Bob", bob.Properties["name"])
	require.Equal(t, "bob@example.com", bob.Properties["email"])

	batch, err := engine.BatchGetNodes([]NodeID{"test:alice", "test:bob"})
	require.NoError(t, err)
	require.Len(t, batch, 2)

	people, err := engine.GetNodesByLabel("person")
	require.NoError(t, err)
	require.ElementsMatch(t, []NodeID{"test:alice", "test:bob"}, treeDBNodeIDs(people))

	employees, err := engine.GetNodesByLabel("employee")
	require.NoError(t, err)
	require.ElementsMatch(t, want.employeeIDs, treeDBNodeIDs(employees))

	reviewers, err := engine.GetNodesByLabel("reviewer")
	require.NoError(t, err)
	require.ElementsMatch(t, want.reviewerIDs, treeDBNodeIDs(reviewers))

	labelCount, err := engine.NodeCountByLabelInNamespace("test", "Person")
	require.NoError(t, err)
	require.Equal(t, int64(2), labelCount)
	nodeCount, err := engine.NodeCountByPrefix("test:")
	require.NoError(t, err)
	require.Equal(t, int64(3), nodeCount)
	edgeCount, err := engine.EdgeCountByPrefix("test:")
	require.NoError(t, err)
	require.Equal(t, int64(1), edgeCount)
	require.Contains(t, engine.ListNamespaces(), "test")

	edge, err := engine.GetEdge("test:knows")
	require.NoError(t, err)
	require.Equal(t, EdgeID("test:knows"), edge.ID)
	require.Equal(t, NodeID("test:alice"), edge.StartNode)
	require.Equal(t, NodeID("test:bob"), edge.EndNode)
	require.Equal(t, "KNOWS", edge.Type)
	require.Equal(t, int64(2024), edge.Properties["since"])
	require.Equal(t, 0.75, edge.Properties["weight"])

	outgoing, err := engine.GetOutgoingEdges("test:alice")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:knows"}, treeDBEdgeIDs(outgoing))
	incoming, err := engine.GetIncomingEdges("test:bob")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:knows"}, treeDBEdgeIDs(incoming))
	byType, err := engine.GetEdgesByType("knows")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:knows"}, treeDBEdgeIDs(byType))
	between, err := engine.GetEdgesBetween("test:alice", "test:bob")
	require.NoError(t, err)
	require.ElementsMatch(t, []EdgeID{"test:knows"}, treeDBEdgeIDs(between))
	head := engine.GetEdgeBetween("test:alice", "test:bob", "KNOWS")
	require.NotNil(t, head)
	require.Equal(t, EdgeID("test:knows"), head.ID)

	pending := engine.FindNodeNeedingEmbedding()
	require.NotNil(t, pending)
	require.Equal(t, NodeID("test:pending-doc"), pending.ID)
	require.Equal(t, 1, engine.PendingEmbeddingsCount())

	schema := engine.GetSchemaForNamespace("test")
	require.NotNil(t, schema)
	_, found, constrained, cacheComplete := schema.LookupUniqueConstraintValueForPlanning("Person", "email", "alice@example.com")
	require.True(t, constrained)
	require.True(t, found)
	if want.uniqueCacheComplete {
		require.True(t, cacheComplete)
	}

	emailMatches := schema.PropertyIndexLookup("Person", "email", "alice@example.com")
	require.ElementsMatch(t, []NodeID{"test:alice"}, emailMatches)
	cityMatches := schema.PropertyIndexLookup("Person", "city", want.aliceCity)
	require.Contains(t, cityMatches, NodeID("test:alice"))

	nycMatches := schema.PropertyIndexLookup("Person", "city", "NYC")
	if want.aliceCity == "NYC" {
		require.ElementsMatch(t, []NodeID{"test:alice", "test:bob"}, nycMatches)
	} else {
		require.ElementsMatch(t, []NodeID{"test:bob"}, nycMatches)
	}

	composite, ok := schema.GetCompositeIndex("person_location_idx")
	require.True(t, ok)
	if want.aliceCity == "NYC" {
		require.ElementsMatch(t, []NodeID{"test:alice", "test:bob"}, composite.LookupFull("US", "NYC"))
	} else {
		require.ElementsMatch(t, []NodeID{"test:alice"}, composite.LookupFull("US", want.aliceCity))
		require.ElementsMatch(t, []NodeID{"test:bob"}, composite.LookupFull("US", "NYC"))
	}

	_, ok = schema.GetRangeIndex("person_rank_range")
	require.True(t, ok)
	_, ok = schema.GetFulltextIndex("doc_text_idx")
	require.True(t, ok)
	_, ok = schema.GetVectorIndex("doc_embedding_idx")
	require.True(t, ok)

	_, err = engine.CreateNode(&Node{
		ID:     "test:duplicate-email",
		Labels: []string{"Person"},
		Properties: map[string]any{
			"email": "alice@example.com",
			"rank":  int64(99),
		},
	})
	require.Error(t, err)
	var constraintErr *ConstraintViolationError
	require.ErrorAs(t, err, &constraintErr)
	require.Equal(t, ConstraintUnique, constraintErr.Type)
}
