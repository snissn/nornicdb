package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	treedbpage "github.com/snissn/gomap/TreeDB/page"
)

const defaultTreeDBProfile = string(treedb.ProfileLegacyWALDurable)

var treeDBEmptyValue = []byte{}

const (
	treeDBPrefixNodeEdgeGuard = byte(0x1A)
	treeDBPrefixScanGuard     = byte(0x7e)
)

// TreeDBOptions configures the native TreeDB storage engine.
type TreeDBOptions struct {
	Dir            string
	Profile        string
	SyncWrites     bool
	FlushThreshold int64
	MemtableMode   string
	MemtableShards int
	// NodeCacheMaxEntries is the maximum number of graph bodies held in the
	// in-process hot cache. When exceeded, the cache is cleared. Set to 0 to use
	// the default shared with Badger.
	NodeCacheMaxEntries int
	// ValueLogDictCompression enables TreeDB value-log dictionary training.
	// The default is false because NornicDB graph records are small and TreeDB's
	// current trainer emits process-global log output when dictionaries rotate.
	ValueLogDictCompression bool
	Logger                  *slog.Logger
}

// TreeDBDurabilityInfo reports the durable write path selected for TreeDBEngine.
type TreeDBDurabilityInfo struct {
	// Dir is the persistent TreeDB directory used by the engine.
	Dir string
	// Profile is the parsed TreeDB profile name used to open the database.
	Profile string
	// DurabilityMode is TreeDB's live durability mode when the handle is open.
	DurabilityMode string
	// WritePathMode is TreeDB's live write-path mode from diagnostic stats.
	WritePathMode string
	// RedoLog describes the active redo-log policy: on, off, or command_wal.
	RedoLog string
	// NativeWAL reports whether TreeDB's native redo log protects writes.
	NativeWAL bool
	// CommandWAL reports whether TreeDB command-WAL replay is active.
	CommandWAL bool
	// NornicWAL reports whether NornicDB wraps TreeDB with its own WAL.
	NornicWAL bool
	// AsyncWrites reports whether this backend acknowledges asynchronous writes.
	AsyncWrites bool
	// SyncWrites reports whether commits force TreeDB synchronous commit.
	SyncWrites bool
	// ReplicationSupported reports whether this path is ready for raft replay.
	ReplicationSupported bool
}

// TreeDBEngine stores the NornicDB property graph directly in TreeDB.
//
// TreeDBEngine uses TreeDB's native conditional transactions for graph writes.
// Each write records read preconditions for touched primary keys, then commits
// all primary and secondary-index mutations atomically through TreeDB.
type TreeDBEngine struct {
	db         *treedb.DB
	dir        string
	profile    string
	syncWrites bool
	nativeWAL  bool
	commandWAL bool
	log        *slog.Logger

	// dbMu protects TreeDB diagnostic reads that lack TreeDB's lifecycle lock.
	dbMu   sync.RWMutex
	closed atomic.Bool

	nodeCount atomic.Int64
	edgeCount atomic.Int64
	guardSeq  atomic.Uint64

	embeddingsEnabled atomic.Bool

	nodeCache           map[NodeID]*Node
	nodeCacheMu         sync.RWMutex
	edgeCache           map[EdgeID]*Edge
	edgeCacheMu         sync.RWMutex
	outgoingAdjCache    map[NodeID][]EdgeID
	incomingAdjCache    map[NodeID][]EdgeID
	adjCacheMu          sync.RWMutex
	nodeCacheMaxEntries int
	edgeCacheMaxItems   int
	adjCacheMaxNodes    int
	cacheHits           int64
	cacheMisses         int64

	countsMu            sync.RWMutex
	namespaceNodeCounts map[string]int64
	namespaceEdgeCounts map[string]int64

	schemaMu sync.RWMutex
	schema   *SchemaManager
	schemas  map[string]*SchemaManager

	callbackMu    sync.RWMutex
	onNodeCreated NodeEventCallback
	onNodeUpdated NodeEventCallback
	onNodeDeleted NodeDeleteCallback
	onEdgeCreated EdgeEventCallback
	onEdgeUpdated EdgeEventCallback
	onEdgeDeleted EdgeDeleteCallback
}

var (
	_ Engine                      = (*TreeDBEngine)(nil)
	_ TransactionalEngine         = (*TreeDBEngine)(nil)
	_ PrefixStatsEngine           = (*TreeDBEngine)(nil)
	_ LabelStatsEngine            = (*TreeDBEngine)(nil)
	_ NamespaceLabelStatsProvider = (*TreeDBEngine)(nil)
	_ NamespaceSchemaProvider     = (*TreeDBEngine)(nil)
	_ NamespaceLister             = (*TreeDBEngine)(nil)
	_ AdjacentEdgesEngine         = (*TreeDBEngine)(nil)
	_ StreamingEngine             = (*TreeDBEngine)(nil)
	_ PrefixStreamingEngine       = (*TreeDBEngine)(nil)
	_ StorageEventNotifier        = (*TreeDBEngine)(nil)
	_ StructuredLogger            = (*TreeDBEngine)(nil)
)

// NewTreeDBEngine opens a persistent TreeDB-backed graph engine.
func NewTreeDBEngine(dir string) (*TreeDBEngine, error) {
	return NewTreeDBEngineWithOptions(TreeDBOptions{Dir: dir})
}

// NewTreeDBEngineWithOptions opens a persistent TreeDB-backed graph engine.
func NewTreeDBEngineWithOptions(opts TreeDBOptions) (*TreeDBEngine, error) {
	if strings.TrimSpace(opts.Dir) == "" {
		return nil, fmt.Errorf("treedb storage backend requires a persistent data directory")
	}

	profile, ok := treedb.ParseProfile(opts.Profile, treedb.ProfileLegacyWALDurable)
	if !ok {
		return nil, fmt.Errorf("unsupported treedb profile %q", opts.Profile)
	}
	switch profile {
	case treedb.ProfileCommandWALDurable, treedb.ProfileCommandWALRelaxed:
		return nil, fmt.Errorf("%w: treedb command WAL profile is reserved for the WAL/replication integration lane", ErrNotImplemented)
	}

	tdOpts := treedb.OptionsFor(profile, opts.Dir)
	if opts.FlushThreshold > 0 {
		tdOpts.FlushThreshold = opts.FlushThreshold
	}
	if opts.MemtableMode != "" {
		tdOpts.MemtableMode = opts.MemtableMode
	}
	if opts.MemtableShards > 0 {
		tdOpts.MemtableShards = opts.MemtableShards
	}
	if opts.ValueLogDictCompression {
		treedb.EnableValueLogDictCompression(&tdOpts)
	} else {
		treedb.DisableValueLogDictCompression(&tdOpts)
	}
	tdOpts.CommandWAL = false

	db, err := treedb.Open(tdOpts)
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	e := &TreeDBEngine{
		db:                  db,
		dir:                 opts.Dir,
		profile:             string(profile),
		syncWrites:          opts.SyncWrites,
		nativeWAL:           treeDBProfileUsesNativeWAL(profile),
		commandWAL:          tdOpts.CommandWAL,
		log:                 logger.With("component", "storage", "engine", "treedb"),
		schema:              NewSchemaManager(),
		schemas:             make(map[string]*SchemaManager),
		namespaceNodeCounts: make(map[string]int64),
		namespaceEdgeCounts: make(map[string]int64),
		nodeCacheMaxEntries: opts.NodeCacheMaxEntries,
	}
	if e.nodeCacheMaxEntries <= 0 {
		e.nodeCacheMaxEntries = defaultBadgerNodeCacheMaxEntries
	}
	e.edgeCacheMaxItems = e.nodeCacheMaxEntries
	e.adjCacheMaxNodes = e.nodeCacheMaxEntries
	e.nodeCache = make(map[NodeID]*Node, e.nodeCacheMaxEntries)
	e.edgeCache = make(map[EdgeID]*Edge, e.edgeCacheMaxItems)
	e.outgoingAdjCache = make(map[NodeID][]EdgeID, e.adjCacheMaxNodes)
	e.incomingAdjCache = make(map[NodeID][]EdgeID, e.adjCacheMaxNodes)
	if err := e.loadPersistedSchemas(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := e.initializeCounts(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return e, nil
}

func treeDBProfileUsesNativeWAL(profile treedb.Profile) bool {
	switch profile {
	case treedb.ProfileDurable, treedb.ProfileLegacyWALDurable, treedb.ProfileWALOnFast, treedb.ProfileLegacyWALRelaxedFast:
		return true
	default:
		return false
	}
}

// TreeDB returns the underlying TreeDB handle. Callers must not close it.
func (e *TreeDBEngine) TreeDB() *treedb.DB {
	if e == nil {
		return nil
	}
	return e.db
}

// DurabilityInfo returns the TreeDB durability and wrapper decisions for this engine.
//
// Example:
//
//	info := engine.DurabilityInfo()
//	if info.SyncWrites && info.NativeWAL {
//		// writes are acknowledged after TreeDB's native synchronous commit path
//	}
func (e *TreeDBEngine) DurabilityInfo() TreeDBDurabilityInfo {
	if e == nil {
		return TreeDBDurabilityInfo{}
	}
	info := TreeDBDurabilityInfo{
		Dir:                  e.dir,
		Profile:              e.profile,
		RedoLog:              treeDBRedoLogPolicy(e.nativeWAL),
		NativeWAL:            e.nativeWAL,
		CommandWAL:           e.commandWAL,
		NornicWAL:            false,
		AsyncWrites:          false,
		SyncWrites:           e.syncWrites,
		ReplicationSupported: false,
	}
	if e.ensureOpen() == nil {
		e.dbMu.RLock()
		defer e.dbMu.RUnlock()
	}
	if e.ensureOpen() == nil {
		info.DurabilityMode = e.db.DurabilityMode()
		stats := e.db.Stats()
		if mode := stats["treedb.write_path.mode"]; mode != "" {
			info.WritePathMode = mode
		}
		if redoLog := stats["treedb.write_path.redo_log"]; redoLog != "" {
			info.RedoLog = redoLog
		}
		info.CommandWAL = info.RedoLog == "command_wal" || info.WritePathMode == "command_wal_cached"
		info.NativeWAL = info.RedoLog != "off"
	}
	return info
}

func treeDBRedoLogPolicy(nativeWAL bool) string {
	if nativeWAL {
		return "on"
	}
	return "off"
}

// Logger returns the engine logger for wrappers.
func (e *TreeDBEngine) Logger() *slog.Logger {
	if e == nil || e.log == nil {
		return slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return e.log
}

// SetEmbeddingsEnabled toggles TreeDB's persistent pending-embed index.
func (e *TreeDBEngine) SetEmbeddingsEnabled(enabled bool) {
	if e != nil {
		e.embeddingsEnabled.Store(enabled)
	}
}

// IsEmbeddingsEnabled reports whether TreeDB maintains pending-embed markers.
func (e *TreeDBEngine) IsEmbeddingsEnabled() bool {
	return e != nil && e.embeddingsEnabled.Load()
}

func (e *TreeDBEngine) shouldIndexPendingEmbed(node *Node) bool {
	if e == nil || !e.embeddingsEnabled.Load() || node == nil {
		return false
	}
	if isSystemNamespaceID(string(node.ID)) {
		return false
	}
	return NodeNeedsEmbedding(node)
}

func (e *TreeDBEngine) ensureOpen() error {
	if e == nil || e.db == nil || e.closed.Load() {
		return ErrStorageClosed
	}
	return nil
}

// Close closes the underlying TreeDB handle.
func (e *TreeDBEngine) Close() error {
	if e == nil || e.db == nil {
		return nil
	}
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	e.dbMu.Lock()
	defer e.dbMu.Unlock()
	return mapTreeDBError(e.db.Close())
}

// Checkpoint flushes TreeDB's cached state to its durable backing files.
func (e *TreeDBEngine) Checkpoint() error {
	if err := e.ensureOpen(); err != nil {
		return err
	}
	return mapTreeDBError(e.db.Checkpoint())
}

// Sync forces a TreeDB checkpoint.
func (e *TreeDBEngine) Sync() error {
	return e.Checkpoint()
}

// RunGC runs TreeDB's index compaction and online vacuum maintenance.
func (e *TreeDBEngine) RunGC() error {
	if err := e.CompactIndex(); err != nil {
		return err
	}
	return e.VacuumIndexOnline(context.Background())
}

// CompactIndex compacts the TreeDB index.
func (e *TreeDBEngine) CompactIndex() error {
	if err := e.ensureOpen(); err != nil {
		return err
	}
	return mapTreeDBError(e.db.CompactIndex())
}

// VacuumIndexOnline vacuums the TreeDB index online.
func (e *TreeDBEngine) VacuumIndexOnline(ctx context.Context) error {
	if err := e.ensureOpen(); err != nil {
		return err
	}
	return mapTreeDBError(e.db.VacuumIndexOnline(ctx))
}

// Stats returns TreeDB diagnostic stats.
func (e *TreeDBEngine) Stats() map[string]string {
	if err := e.ensureOpen(); err != nil {
		return nil
	}
	e.dbMu.RLock()
	defer e.dbMu.RUnlock()
	if err := e.ensureOpen(); err != nil {
		return nil
	}
	return e.db.Stats()
}

// Size returns TreeDB's native diagnostic byte estimates for page-file and WAL
// state.
func (e *TreeDBEngine) Size() (lsm, vlog int64) {
	stats := e.StorageByteStats()
	return stats.Index, stats.WAL
}

// StorageByteStats returns bounded byte gauges for TreeDB observability.
// TreeDB does not currently expose per-node or per-edge byte attribution, so
// those gauges are marked unsupported rather than populated by a full graph
// scan.
func (e *TreeDBEngine) StorageByteStats() StorageByteStats {
	return treeDBStorageByteStatsFromStats(e.Stats())
}

func treeDBStorageByteStatsFromStats(stats map[string]string) StorageByteStats {
	var out StorageByteStats
	if pages, ok := parseTreeDBStatInt64(stats, "treedb.pages.total"); ok {
		out.Index = pages * int64(treedbpage.PageSize)
		out.IndexSupported = true
	}
	if wal, ok := parseTreeDBStatInt64(stats, "treedb.cache.wal_bytes_estimate"); ok {
		out.WAL = wal
		out.WALSupported = true
		return out
	}
	if wal, ok := parseTreeDBStatInt64(stats, "treedb.command_wal.bytes"); ok {
		out.WAL = wal
		out.WALSupported = true
		return out
	}
	if wal, ok := parseTreeDBStatInt64(stats, "treedb.command_wal.bytes.total"); ok {
		out.WAL = wal
		out.WALSupported = true
	}
	return out
}

func parseTreeDBStatInt64(stats map[string]string, key string) (int64, bool) {
	raw, ok := stats[key]
	if !ok {
		return 0, false
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	if value < 0 {
		value = 0
	}
	return value, true
}

func (e *TreeDBEngine) initializeCounts() error {
	if err := e.ensureOpen(); err != nil {
		return err
	}
	nodeCount, nodeNamespaces, err := e.countPrimaryKeys(prefixNode)
	if err != nil {
		return err
	}
	edgeCount, edgeNamespaces, err := e.countPrimaryKeys(prefixEdge)
	if err != nil {
		return err
	}
	e.nodeCount.Store(nodeCount)
	e.edgeCount.Store(edgeCount)
	e.countsMu.Lock()
	e.namespaceNodeCounts = nodeNamespaces
	e.namespaceEdgeCounts = edgeNamespaces
	e.countsMu.Unlock()
	return nil
}

func (e *TreeDBEngine) countPrimaryKeys(prefix byte) (int64, map[string]int64, error) {
	out := make(map[string]int64)
	start := []byte{prefix}
	it, err := e.db.Iterator(start, treeDBRangeEnd(start))
	if err != nil {
		return 0, nil, mapTreeDBError(err)
	}
	defer it.Close()

	var count int64
	for ; it.Valid(); it.Next() {
		key := it.Key()
		if len(key) <= 1 {
			continue
		}
		count++
		if ns, ok := namespacePrefixFromID(string(key[1:])); ok {
			out[ns]++
		}
	}
	if err := it.Error(); err != nil {
		return 0, nil, mapTreeDBError(err)
	}
	return count, out, nil
}

// NodeCount returns the total number of persisted node bodies.
func (e *TreeDBEngine) NodeCount() (int64, error) {
	if err := e.ensureOpen(); err != nil {
		return 0, err
	}
	return e.nodeCount.Load(), nil
}

// EdgeCount returns the total number of persisted edge bodies.
func (e *TreeDBEngine) EdgeCount() (int64, error) {
	if err := e.ensureOpen(); err != nil {
		return 0, err
	}
	return e.edgeCount.Load(), nil
}

// NodeCountByPrefix returns the number of nodes whose ID has prefix.
func (e *TreeDBEngine) NodeCountByPrefix(prefix string) (int64, error) {
	if err := e.ensureOpen(); err != nil {
		return 0, err
	}
	e.countsMu.RLock()
	if count, ok := e.namespaceNodeCounts[prefix]; ok {
		e.countsMu.RUnlock()
		return count, nil
	}
	e.countsMu.RUnlock()
	return e.countByIDPrefix(prefixNode, prefix)
}

// EdgeCountByPrefix returns the number of edges whose ID has prefix.
func (e *TreeDBEngine) EdgeCountByPrefix(prefix string) (int64, error) {
	if err := e.ensureOpen(); err != nil {
		return 0, err
	}
	e.countsMu.RLock()
	if count, ok := e.namespaceEdgeCounts[prefix]; ok {
		e.countsMu.RUnlock()
		return count, nil
	}
	e.countsMu.RUnlock()
	return e.countByIDPrefix(prefixEdge, prefix)
}

func (e *TreeDBEngine) countByIDPrefix(kind byte, idPrefix string) (int64, error) {
	start := append([]byte{kind}, idPrefix...)
	it, err := e.db.Iterator(start, treeDBRangeEnd(start))
	if err != nil {
		return 0, mapTreeDBError(err)
	}
	defer it.Close()
	var count int64
	for ; it.Valid(); it.Next() {
		count++
	}
	return count, mapTreeDBError(it.Error())
}

// NodeCountByLabel returns the number of nodes with label.
func (e *TreeDBEngine) NodeCountByLabel(label string) (int64, error) {
	if err := e.ensureOpen(); err != nil {
		return 0, err
	}
	return e.countLabelWithNamespace(label, "")
}

// NodeCountByLabelInNamespace returns the number of nodes with label in namespace.
func (e *TreeDBEngine) NodeCountByLabelInNamespace(namespace, label string) (int64, error) {
	if err := e.ensureOpen(); err != nil {
		return 0, err
	}
	prefix := namespace
	if prefix != "" && !strings.HasSuffix(prefix, ":") {
		prefix += ":"
	}
	return e.countLabelWithNamespace(label, prefix)
}

func (e *TreeDBEngine) countLabelWithNamespace(label, namespacePrefix string) (int64, error) {
	prefix := treeDBLabelIndexPrefix(label)
	it, err := e.db.Iterator(prefix, treeDBRangeEnd(prefix))
	if err != nil {
		return 0, mapTreeDBError(err)
	}
	defer it.Close()
	var count int64
	for ; it.Valid(); it.Next() {
		id := string(it.Key()[len(prefix):])
		if namespacePrefix == "" || strings.HasPrefix(id, namespacePrefix) {
			count++
		}
	}
	return count, mapTreeDBError(it.Error())
}

// ListNamespaces returns namespaces observed in persisted IDs or schema state.
func (e *TreeDBEngine) ListNamespaces() []string {
	names := make(map[string]struct{})
	e.countsMu.RLock()
	for prefix := range e.namespaceNodeCounts {
		names[strings.TrimSuffix(prefix, ":")] = struct{}{}
	}
	for prefix := range e.namespaceEdgeCounts {
		names[strings.TrimSuffix(prefix, ":")] = struct{}{}
	}
	e.countsMu.RUnlock()
	e.schemaMu.RLock()
	for ns := range e.schemas {
		names[ns] = struct{}{}
	}
	e.schemaMu.RUnlock()
	out := make([]string, 0, len(names))
	for ns := range names {
		out = append(out, ns)
	}
	return out
}

// GetSchema returns the engine-global schema manager.
func (e *TreeDBEngine) GetSchema() *SchemaManager {
	if e == nil {
		return nil
	}
	return e.GetSchemaForNamespace("nornic")
}

// GetSchemaForNamespace returns the schema manager for namespace.
func (e *TreeDBEngine) GetSchemaForNamespace(namespace string) *SchemaManager {
	if e == nil {
		return nil
	}
	if namespace == "" {
		namespace = "nornic"
	}
	e.schemaMu.RLock()
	sm := e.schemas[namespace]
	e.schemaMu.RUnlock()
	if sm != nil {
		return sm
	}
	e.schemaMu.Lock()
	defer e.schemaMu.Unlock()
	if sm = e.schemas[namespace]; sm != nil {
		return sm
	}
	sm = NewSchemaManager()
	sm.SetPersister(func(def *SchemaDefinition) error {
		return e.persistSchemaDefinition(namespace, def)
	})
	e.schemas[namespace] = sm
	return sm
}

func (e *TreeDBEngine) lookupSchemaForNamespace(namespace string) *SchemaManager {
	if e == nil {
		return nil
	}
	if namespace == "" {
		namespace = "nornic"
	}
	e.schemaMu.RLock()
	sm := e.schemas[namespace]
	e.schemaMu.RUnlock()
	return sm
}

func (e *TreeDBEngine) loadPersistedSchemas() error {
	if err := e.ensureOpen(); err != nil {
		return err
	}
	prefix := []byte{prefixSchema}
	it, err := e.db.Iterator(prefix, treeDBRangeEnd(prefix))
	if err != nil {
		return mapTreeDBError(err)
	}
	defer it.Close()

	type loaded struct {
		namespace string
		schema    *SchemaManager
	}
	var schemas []loaded
	for ; it.Valid(); it.Next() {
		namespace, ok := parseSchemaNamespaceFromKey(it.Key())
		if !ok || namespace == "" {
			return fmt.Errorf("schema: invalid schema key: %x", it.Key())
		}
		var def SchemaDefinition
		if err := json.Unmarshal(it.Value(), &def); err != nil {
			return fmt.Errorf("schema: decode %q: %w", namespace, err)
		}
		if def.Version == 0 {
			def.Version = schemaDefinitionVersion
		}
		sm := NewSchemaManager()
		if err := sm.ReplaceFromDefinition(&def); err != nil {
			return fmt.Errorf("schema: apply %q: %w", namespace, err)
		}
		ns := namespace
		sm.SetPersister(func(def *SchemaDefinition) error {
			return e.persistSchemaDefinition(ns, def)
		})
		schemas = append(schemas, loaded{namespace: namespace, schema: sm})
	}
	if err := it.Error(); err != nil {
		return mapTreeDBError(err)
	}
	if len(schemas) > 0 {
		e.schemaMu.Lock()
		for _, loaded := range schemas {
			e.schemas[loaded.namespace] = loaded.schema
		}
		e.schemaMu.Unlock()
	}
	for _, loaded := range schemas {
		if err := e.rebuildSchemaDerivedState(loaded.namespace, loaded.schema); err != nil {
			return err
		}
	}
	return nil
}

func (e *TreeDBEngine) persistSchemaDefinition(namespace string, def *SchemaDefinition) error {
	if namespace == "" {
		return fmt.Errorf("schema: namespace is required")
	}
	if def == nil {
		return fmt.Errorf("schema: definition is required")
	}
	if def.Version == 0 {
		def.Version = schemaDefinitionVersion
	}
	blob, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("schema: marshal %q: %w", namespace, err)
	}
	if e.syncWrites {
		return mapTreeDBError(e.db.SetSync(schemaKey(namespace), blob))
	}
	return mapTreeDBError(e.db.Set(schemaKey(namespace), blob))
}

func (e *TreeDBEngine) rebuildSchemaDerivedState(namespace string, sm *SchemaManager) error {
	if namespace == "" || sm == nil {
		return nil
	}
	sm.mu.RLock()
	hasUnique := len(sm.uniqueConstraints) > 0
	hasPropertyIndexes := len(sm.propertyIndexes) > 0
	hasCompositeIndexes := len(sm.compositeIndexes) > 0
	uniqueConstraints := make([]*UniqueConstraint, 0, len(sm.uniqueConstraints))
	for _, uc := range sm.uniqueConstraints {
		uniqueConstraints = append(uniqueConstraints, uc)
	}
	propertyIndexes := make([]*PropertyIndex, 0, len(sm.propertyIndexes))
	for _, idx := range sm.propertyIndexes {
		propertyIndexes = append(propertyIndexes, idx)
	}
	compositeIndexes := make([]*CompositeIndex, 0, len(sm.compositeIndexes))
	for _, idx := range sm.compositeIndexes {
		compositeIndexes = append(compositeIndexes, idx)
	}
	sm.mu.RUnlock()
	if !hasUnique && !hasPropertyIndexes && !hasCompositeIndexes {
		return nil
	}
	for _, uc := range uniqueConstraints {
		uc.mu.Lock()
		uc.values = make(map[interface{}]NodeID)
		uc.valuesCacheComplete = false
		uc.mu.Unlock()
	}
	for _, idx := range propertyIndexes {
		idx.mu.Lock()
		idx.values = make(map[interface{}][]NodeID)
		idx.sortedNonNilKeys = nil
		idx.keysDirty = true
		idx.mu.Unlock()
	}
	for _, idx := range compositeIndexes {
		idx.mu.Lock()
		idx.fullIndex = make(map[string][]NodeID)
		idx.prefixIndex = make(map[string][]NodeID)
		idx.mu.Unlock()
	}

	nodes, err := e.allNodesWithPrefix(namespace + ":")
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if hasUnique {
			for _, label := range node.Labels {
				for propName, propValue := range node.Properties {
					if err := sm.CheckUniqueConstraint(label, propName, propValue, node.ID); err != nil {
						return fmt.Errorf("schema: rebuild unique values: namespace=%q: %w", namespace, err)
					}
					sm.RegisterUniqueValue(label, propName, propValue, node.ID)
				}
			}
		}
		if hasPropertyIndexes {
			for _, label := range node.Labels {
				for propName, propValue := range node.Properties {
					if _, ok := sm.GetPropertyIndex(label, propName); !ok {
						continue
					}
					if err := sm.PropertyIndexInsert(label, propName, node.ID, propValue); err != nil {
						return fmt.Errorf("schema: rebuild property indexes: namespace=%q label=%q property=%q: %w", namespace, label, propName, err)
					}
				}
			}
		}
		if hasCompositeIndexes {
			for _, label := range node.Labels {
				for _, idx := range sm.GetCompositeIndexesForLabel(label) {
					if idx == nil {
						continue
					}
					if err := idx.IndexNode(node.ID, node.Properties); err != nil {
						return fmt.Errorf("schema: rebuild composite indexes: namespace=%q index=%q: %w", namespace, idx.Name, err)
					}
				}
			}
		}
	}
	for _, uc := range uniqueConstraints {
		uc.mu.Lock()
		uc.valuesCacheComplete = true
		uc.mu.Unlock()
	}
	return nil
}

// BeginGraphTransaction starts a native TreeDB conditional graph transaction.
func (e *TreeDBEngine) BeginGraphTransaction() (GraphTransaction, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	tx, err := e.db.NewConditionalTxnWithSnapshot()
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	scanSnapshot := e.db.AcquireSnapshot()
	if scanSnapshot == nil {
		_ = tx.Close()
		return nil, fmt.Errorf("%w: TreeDB scan snapshot unavailable", ErrNotImplemented)
	}
	tx.ReserveReadSet(8)
	tx.ReserveWrites(16)
	treeTx := newTreeDBTransaction(e, tx)
	treeTx.snapshot = scanSnapshot
	return treeTx, nil
}

func (e *TreeDBEngine) beginTreeDBTransaction() (*TreeDBTransaction, error) {
	tx, err := e.BeginGraphTransaction()
	if err != nil {
		return nil, err
	}
	treeTx, ok := tx.(*TreeDBTransaction)
	if !ok {
		_ = tx.Rollback()
		return nil, ErrNotImplemented
	}
	return treeTx, nil
}

func (e *TreeDBEngine) beginTreeDBPointTransaction() (*TreeDBTransaction, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	tx, err := e.db.NewConditionalTxn()
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	tx.ReserveReadSet(8)
	tx.ReserveWrites(16)
	return newTreeDBTransaction(e, tx), nil
}

// CreateNode creates a node through a native conditional write.
func (e *TreeDBEngine) CreateNode(node *Node) (NodeID, error) {
	tx, err := e.beginTreeDBPointTransaction()
	if err != nil {
		return "", err
	}
	id, err := tx.CreateNode(node)
	if err != nil {
		_ = tx.Rollback()
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return id, nil
}

// UpdateNode updates a node through a native conditional write.
func (e *TreeDBEngine) UpdateNode(node *Node) error {
	if node == nil {
		return ErrInvalidData
	}
	tx, err := e.beginTreeDBPointTransaction()
	if err != nil {
		return err
	}
	if _, err := tx.GetNode(node.ID); err != nil {
		if errors.Is(err, ErrNotFound) {
			_, err = tx.CreateNode(node)
		}
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	}
	if err := tx.UpdateNode(node); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// UpdateNodeEmbedding updates embedding fields on an existing node without creating missing nodes.
func (e *TreeDBEngine) UpdateNodeEmbedding(node *Node) error {
	if node == nil {
		return ErrInvalidData
	}
	if node.ID == "" {
		return ErrInvalidID
	}
	if err := treeDBValidPrefixedID("node", string(node.ID)); err != nil {
		return err
	}
	tx, err := e.beginTreeDBPointTransaction()
	if err != nil {
		return err
	}
	existing, err := tx.GetNode(node.ID)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	next := copyNode(existing)
	update := copyNode(node)
	next.ChunkEmbeddings = update.ChunkEmbeddings
	if update.EmbedMeta != nil {
		next.EmbedMeta = update.EmbedMeta
	}
	next.UpdatedAt = time.Now()
	if err := tx.UpdateNode(next); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// DeleteNode deletes a node and its incident edges.
func (e *TreeDBEngine) DeleteNode(id NodeID) error {
	tx, err := e.beginTreeDBTransaction()
	if err != nil {
		return err
	}
	if err := tx.DeleteNode(id); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// CreateEdge creates an edge through a native conditional write.
func (e *TreeDBEngine) CreateEdge(edge *Edge) error {
	tx, err := e.beginTreeDBPointTransaction()
	if err != nil {
		return err
	}
	if err := tx.CreateEdge(edge); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// UpdateEdge updates an edge through a native conditional write.
func (e *TreeDBEngine) UpdateEdge(edge *Edge) error {
	tx, err := e.beginTreeDBTransaction()
	if err != nil {
		return err
	}
	if err := tx.UpdateEdge(edge); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// DeleteEdge deletes an edge through a native conditional write.
func (e *TreeDBEngine) DeleteEdge(id EdgeID) error {
	tx, err := e.beginTreeDBTransaction()
	if err != nil {
		return err
	}
	if err := tx.DeleteEdge(id); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// BulkCreateNodes creates nodes in one native TreeDB conditional transaction.
func (e *TreeDBEngine) BulkCreateNodes(nodes []*Node) error {
	tx, err := e.beginTreeDBPointTransaction()
	if err != nil {
		return err
	}
	tx.reserveNodeCreateBatch(nodes)
	for _, node := range nodes {
		if _, err := tx.CreateNode(node); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// BulkCreateEdges creates edges in one native TreeDB conditional transaction.
func (e *TreeDBEngine) BulkCreateEdges(edges []*Edge) error {
	tx, err := e.beginTreeDBPointTransaction()
	if err != nil {
		return err
	}
	if err := tx.BulkCreateEdges(edges); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// BulkDeleteNodes deletes nodes in one native TreeDB conditional transaction.
func (e *TreeDBEngine) BulkDeleteNodes(ids []NodeID) error {
	tx, err := e.beginTreeDBTransaction()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := tx.DeleteNode(id); err != nil && !errors.Is(err, ErrNotFound) {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// BulkDeleteEdges deletes edges in one native TreeDB conditional transaction.
func (e *TreeDBEngine) BulkDeleteEdges(ids []EdgeID) error {
	tx, err := e.beginTreeDBTransaction()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := tx.DeleteEdge(id); err != nil && !errors.Is(err, ErrNotFound) {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// DeleteByPrefix deletes nodes and edges whose IDs start with prefix.
func (e *TreeDBEngine) DeleteByPrefix(prefix string) (int64, int64, error) {
	if err := e.ensureOpen(); err != nil {
		return 0, 0, err
	}
	tx, err := e.beginTreeDBTransaction()
	if err != nil {
		return 0, 0, err
	}
	namespace := treeDBNamespaceForGuard(prefix)
	nodeIDs, err := tx.collectNodeIDs(append([]byte{prefixNode}, prefix...), treeDBNodeNamespaceReadGuardKey(namespace), 1)
	if err != nil {
		_ = tx.Rollback()
		return 0, 0, err
	}
	edgeIDs, err := tx.collectEdgeIDs(append([]byte{prefixEdge}, prefix...), treeDBEdgeNamespaceReadGuardKey(namespace), 1)
	if err != nil {
		_ = tx.Rollback()
		return 0, 0, err
	}
	deletedEdges := make(map[EdgeID]struct{}, len(edgeIDs))
	for id := range nodeIDs {
		if err := tx.DeleteNode(id); err != nil && !errors.Is(err, ErrNotFound) {
			_ = tx.Rollback()
			return 0, 0, err
		}
	}
	for _, id := range tx.deletedEdgeIDs {
		deletedEdges[id] = struct{}{}
	}
	for id := range edgeIDs {
		if _, ok := deletedEdges[id]; ok {
			continue
		}
		if err := tx.DeleteEdge(id); err != nil && !errors.Is(err, ErrNotFound) {
			_ = tx.Rollback()
			return 0, 0, err
		}
		deletedEdges[id] = struct{}{}
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	return int64(len(nodeIDs)), int64(len(deletedEdges)), nil
}

func (e *TreeDBEngine) nodeIDsByPrefix(prefix string) ([]NodeID, error) {
	start := append([]byte{prefixNode}, prefix...)
	it, err := e.db.Iterator(start, treeDBRangeEnd(start))
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	defer it.Close()
	var ids []NodeID
	for ; it.Valid(); it.Next() {
		key := it.Key()
		if len(key) > 1 {
			ids = append(ids, NodeID(string(key[1:])))
		}
	}
	return ids, mapTreeDBError(it.Error())
}

func (e *TreeDBEngine) edgeIDsByPrefix(prefix string) ([]EdgeID, error) {
	start := append([]byte{prefixEdge}, prefix...)
	it, err := e.db.Iterator(start, treeDBRangeEnd(start))
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	defer it.Close()
	var ids []EdgeID
	for ; it.Valid(); it.Next() {
		key := it.Key()
		if len(key) > 1 {
			ids = append(ids, EdgeID(string(key[1:])))
		}
	}
	return ids, mapTreeDBError(it.Error())
}

// GetNode returns a node by ID.
func (e *TreeDBEngine) GetNode(id NodeID) (*Node, error) {
	if id == "" {
		return nil, ErrInvalidID
	}
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	if node, ok := e.cacheLoadNode(id); ok {
		atomic.AddInt64(&e.cacheHits, 1)
		return node, nil
	}
	atomic.AddInt64(&e.cacheMisses, 1)
	node, _, err := e.getNodeWithRevision(id)
	if err == nil && node != nil {
		e.cacheStoreNode(node)
	}
	return node, err
}

func (e *TreeDBEngine) getNodeWithNamespacePrefix(prefix string, id NodeID) (*Node, error) {
	node, err := e.GetNode(treeDBNodeIDWithNamespacePrefix(prefix, id))
	if err != nil || node == nil {
		return node, err
	}
	node.ID = treeDBUnprefixNodeID(prefix, node.ID)
	return node, nil
}

// GetNodeEntryRevision returns TreeDB's native revision for a node body.
func (e *TreeDBEngine) GetNodeEntryRevision(id NodeID) (treedb.EntryRevision, error) {
	if id == "" {
		return treedb.LegacyEntryRevision, ErrInvalidID
	}
	if err := e.ensureOpen(); err != nil {
		return treedb.LegacyEntryRevision, err
	}
	_, revision, err := e.getNodeWithRevision(id)
	return revision, err
}

func (e *TreeDBEngine) getNodeWithRevision(id NodeID) (*Node, treedb.EntryRevision, error) {
	data, revision, err := e.db.GetVersionedAppend(nodeKey(id), nil)
	if err != nil {
		return nil, treedb.LegacyEntryRevision, mapTreeDBError(err)
	}
	node, err := deserializeNode(data)
	if err != nil {
		return nil, revision, err
	}
	return node, revision, nil
}

// GetEdge returns an edge by ID.
func (e *TreeDBEngine) GetEdge(id EdgeID) (*Edge, error) {
	if id == "" {
		return nil, ErrInvalidID
	}
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	if edge, ok := e.cacheLoadEdge(id); ok {
		return edge, nil
	}
	edge, _, err := e.getEdgeWithRevision(id)
	if err == nil && edge != nil {
		e.cacheStoreEdge(edge)
	}
	return edge, err
}

func (e *TreeDBEngine) getEdgeWithNamespacePrefix(prefix string, id EdgeID) (*Edge, error) {
	edge, err := e.GetEdge(treeDBEdgeIDWithNamespacePrefix(prefix, id))
	if err != nil || edge == nil {
		return edge, err
	}
	edge.ID = treeDBUnprefixEdgeID(prefix, edge.ID)
	edge.StartNode = treeDBUnprefixNodeID(prefix, edge.StartNode)
	edge.EndNode = treeDBUnprefixNodeID(prefix, edge.EndNode)
	return edge, nil
}

// GetEdgeEntryRevision returns TreeDB's native revision for an edge body.
func (e *TreeDBEngine) GetEdgeEntryRevision(id EdgeID) (treedb.EntryRevision, error) {
	if id == "" {
		return treedb.LegacyEntryRevision, ErrInvalidID
	}
	if err := e.ensureOpen(); err != nil {
		return treedb.LegacyEntryRevision, err
	}
	_, revision, err := e.getEdgeWithRevision(id)
	return revision, err
}

func (e *TreeDBEngine) getEdgeWithRevision(id EdgeID) (*Edge, treedb.EntryRevision, error) {
	data, revision, err := e.db.GetVersionedAppend(edgeKey(id), nil)
	if err != nil {
		return nil, treedb.LegacyEntryRevision, mapTreeDBError(err)
	}
	edge, err := deserializeEdge(data)
	if err != nil {
		return nil, revision, err
	}
	return edge, revision, nil
}

// BatchGetNodes fetches multiple nodes through TreeDB's native batched read.
func (e *TreeDBEngine) BatchGetNodes(ids []NodeID) (map[NodeID]*Node, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	out := make(map[NodeID]*Node, len(ids))
	if len(ids) == 0 {
		return out, nil
	}

	// Collect raw pointers under read lock; copy outside lock to avoid
	// blocking writers (cacheStoreNode/cacheDeleteNode) during expensive
	// deep-copies, mirroring the cacheLoadNode pattern.
	type batchNodeCacheHit struct {
		id   NodeID
		node *Node
	}
	cacheHits := make([]batchNodeCacheHit, 0, len(ids))
	missing := make([]NodeID, 0, len(ids))
	e.nodeCacheMu.RLock()
	for _, id := range ids {
		if id == "" {
			e.nodeCacheMu.RUnlock()
			return nil, ErrInvalidID
		}
		if cached, ok := e.nodeCache[id]; ok && cached != nil {
			cacheHits = append(cacheHits, batchNodeCacheHit{id: id, node: cached})
			continue
		}
		missing = append(missing, id)
	}
	e.nodeCacheMu.RUnlock()
	for _, hit := range cacheHits {
		out[hit.id] = copyNode(hit.node)
	}
	if len(missing) == 0 {
		return out, nil
	}

	keys := make([][]byte, len(missing))
	for i, id := range missing {
		keys[i] = nodeKey(id)
	}
	cacheGuard := e.guardSeq.Load()
	values, err := e.db.GetMany(keys)
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	loaded := make([]*Node, 0, len(values))
	for i, data := range values {
		if len(data) == 0 {
			continue
		}
		node, err := deserializeNode(data)
		if err != nil {
			return nil, err
		}
		out[missing[i]] = node
		loaded = append(loaded, node)
	}
	for _, node := range loaded {
		e.cacheStoreNodeIfGuard(node, cacheGuard)
	}
	return out, nil
}

// GetNodesByLabel returns all nodes with label.
func (e *TreeDBEngine) GetNodesByLabel(label string) ([]*Node, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	prefix := treeDBLabelIndexPrefix(label)
	it, err := e.db.Iterator(prefix, treeDBRangeEnd(prefix))
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	defer it.Close()
	var nodes []*Node
	for ; it.Valid(); it.Next() {
		id := NodeID(string(it.Key()[len(prefix):]))
		node, err := e.GetNode(id)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, mapTreeDBError(it.Error())
}

// GetFirstNodeByLabel returns the first node with label.
func (e *TreeDBEngine) GetFirstNodeByLabel(label string) (*Node, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	prefix := treeDBLabelIndexPrefix(label)
	it, err := e.db.Iterator(prefix, treeDBRangeEnd(prefix))
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		id := NodeID(string(it.Key()[len(prefix):]))
		node, err := e.GetNode(id)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return node, nil
	}
	return nil, mapTreeDBError(it.Error())
}

// AllNodes returns all nodes.
func (e *TreeDBEngine) AllNodes() ([]*Node, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	return e.allNodesWithPrefix("")
}

// GetAllNodes returns all nodes and drops scan errors for legacy callers.
func (e *TreeDBEngine) GetAllNodes() []*Node {
	nodes, _ := e.AllNodes()
	return nodes
}

func (e *TreeDBEngine) allNodesWithPrefix(idPrefix string) ([]*Node, error) {
	start := append([]byte{prefixNode}, idPrefix...)
	it, err := e.db.Iterator(start, treeDBRangeEnd(start))
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	defer it.Close()
	var nodes []*Node
	for ; it.Valid(); it.Next() {
		node, err := deserializeNode(it.Value())
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, mapTreeDBError(it.Error())
}

// AllEdges returns all edges.
func (e *TreeDBEngine) AllEdges() ([]*Edge, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	return e.allEdgesWithPrefix("")
}

func (e *TreeDBEngine) allEdgesWithPrefix(idPrefix string) ([]*Edge, error) {
	start := append([]byte{prefixEdge}, idPrefix...)
	it, err := e.db.Iterator(start, treeDBRangeEnd(start))
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	defer it.Close()
	var edges []*Edge
	for ; it.Valid(); it.Next() {
		edge, err := deserializeEdge(it.Value())
		if err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}
	return edges, mapTreeDBError(it.Error())
}

// GetOutgoingEdges returns all outgoing edges for nodeID.
// Returned edges may share cached storage and must not be mutated.
func (e *TreeDBEngine) GetOutgoingEdges(nodeID NodeID) ([]*Edge, error) {
	if nodeID == "" {
		return nil, ErrInvalidID
	}
	if ids, ok := e.adjCacheLoadOutgoing(nodeID); ok {
		return e.materializeAdjEdges(ids)
	}
	edges, ids, err := e.collectEdgesAndIDsByIndexPrefix(treeDBOutgoingIndexPrefix(nodeID))
	if err != nil {
		return nil, err
	}
	e.adjCacheStoreOutgoing(nodeID, ids)
	return edges, nil
}

// GetIncomingEdges returns all incoming edges for nodeID.
// Returned edges may share cached storage and must not be mutated.
func (e *TreeDBEngine) GetIncomingEdges(nodeID NodeID) ([]*Edge, error) {
	if nodeID == "" {
		return nil, ErrInvalidID
	}
	if ids, ok := e.adjCacheLoadIncoming(nodeID); ok {
		return e.materializeAdjEdges(ids)
	}
	edges, ids, err := e.collectEdgesAndIDsByIndexPrefix(treeDBIncomingIndexPrefix(nodeID))
	if err != nil {
		return nil, err
	}
	e.adjCacheStoreIncoming(nodeID, ids)
	return edges, nil
}

// GetAdjacentEdges returns both outgoing and incoming edges for nodeID.
// Returned edges may share cached storage and must not be mutated.
func (e *TreeDBEngine) GetAdjacentEdges(nodeID NodeID) ([]*Edge, []*Edge, error) {
	if nodeID == "" {
		return nil, nil, ErrInvalidID
	}
	cachedOutIDs, outHit := e.adjCacheLoadOutgoing(nodeID)
	cachedInIDs, inHit := e.adjCacheLoadIncoming(nodeID)
	if outHit && inHit {
		outgoing, err := e.materializeAdjEdges(cachedOutIDs)
		if err != nil {
			return nil, nil, err
		}
		incoming, err := e.materializeAdjEdges(cachedInIDs)
		if err != nil {
			return nil, nil, err
		}
		return outgoing, incoming, nil
	}

	var outgoing, incoming []*Edge
	if !outHit {
		var outIDs []EdgeID
		var err error
		outgoing, outIDs, err = e.collectEdgesAndIDsByIndexPrefix(treeDBOutgoingIndexPrefix(nodeID))
		if err != nil {
			return nil, nil, err
		}
		e.adjCacheStoreOutgoing(nodeID, outIDs)
	} else {
		var err error
		outgoing, err = e.materializeAdjEdges(cachedOutIDs)
		if err != nil {
			return nil, nil, err
		}
	}

	if !inHit {
		var inIDs []EdgeID
		var err error
		incoming, inIDs, err = e.collectEdgesAndIDsByIndexPrefix(treeDBIncomingIndexPrefix(nodeID))
		if err != nil {
			return nil, nil, err
		}
		e.adjCacheStoreIncoming(nodeID, inIDs)
	} else {
		var err error
		incoming, err = e.materializeAdjEdges(cachedInIDs)
		if err != nil {
			return nil, nil, err
		}
	}

	return outgoing, incoming, nil
}

// GetEdgesByType returns all edges with edgeType.
func (e *TreeDBEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	return e.collectEdgesByIndexPrefix(treeDBEdgeTypeIndexPrefix(edgeType))
}

func (e *TreeDBEngine) collectEdgesByIndexPrefix(prefix []byte) ([]*Edge, error) {
	ids, err := e.collectEdgeIDsByIndexPrefix(prefix)
	if err != nil {
		return nil, err
	}
	return e.materializeEdges(ids)
}

func (e *TreeDBEngine) collectEdgesAndIDsByIndexPrefix(prefix []byte) ([]*Edge, []EdgeID, error) {
	ids, err := e.collectEdgeIDsByIndexPrefix(prefix)
	if err != nil {
		return nil, nil, err
	}
	edges, err := e.materializeAdjEdges(ids)
	if err != nil {
		return nil, nil, err
	}
	return edges, ids, nil
}

func (e *TreeDBEngine) collectEdgeIDsByIndexPrefix(prefix []byte) ([]EdgeID, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	it, err := e.db.Iterator(prefix, treeDBRangeEnd(prefix))
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	defer it.Close()
	var ids []EdgeID
	for ; it.Valid(); it.Next() {
		ids = append(ids, EdgeID(string(it.Key()[len(prefix):])))
	}
	return ids, mapTreeDBError(it.Error())
}

func (e *TreeDBEngine) materializeAdjEdges(ids []EdgeID) ([]*Edge, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	edges := make([]*Edge, 0, len(ids))
	for _, id := range ids {
		if cached, ok := e.cacheLoadEdgeReadOnly(id); ok {
			edges = append(edges, cached)
			continue
		}
		edge, err := e.GetEdge(id)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}
	return edges, nil
}

func (e *TreeDBEngine) materializeEdges(ids []EdgeID) ([]*Edge, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	edges := make([]*Edge, 0, len(ids))
	for _, id := range ids {
		edge, err := e.GetEdge(id)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}
	return edges, nil
}

// GetEdgesBetween returns all edges from startID to endID.
func (e *TreeDBEngine) GetEdgesBetween(startID, endID NodeID) ([]*Edge, error) {
	if startID == "" || endID == "" {
		return nil, ErrInvalidID
	}
	return e.collectEdgesByBetweenPrefix(treeDBEdgeBetweenIndexPrefix(startID, endID))
}

func (e *TreeDBEngine) collectEdgesByBetweenPrefix(prefix []byte) ([]*Edge, error) {
	if err := e.ensureOpen(); err != nil {
		return nil, err
	}
	it, err := e.db.Iterator(prefix, treeDBRangeEnd(prefix))
	if err != nil {
		return nil, mapTreeDBError(err)
	}
	defer it.Close()
	var edges []*Edge
	for ; it.Valid(); it.Next() {
		edgeID := treeDBEdgeIDFromBetweenKey(it.Key())
		if edgeID == "" {
			continue
		}
		edge, err := e.GetEdge(edgeID)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}
	return edges, mapTreeDBError(it.Error())
}

// GetEdgeBetween returns one edge from startID to endID with edgeType.
func (e *TreeDBEngine) GetEdgeBetween(startID, endID NodeID, edgeType string) *Edge {
	if startID == "" || endID == "" || edgeType == "" {
		return nil
	}
	if err := e.ensureOpen(); err != nil {
		return nil
	}
	data, err := e.db.GetAppend(treeDBEdgeBetweenHeadKey(startID, endID, edgeType), nil)
	if err == nil && len(data) > 0 {
		edge, err := e.GetEdge(EdgeID(string(data)))
		if err == nil && treeDBEdgeMatchesBetween(edge, startID, endID, edgeType) {
			return edge
		}
	}
	edges, err := e.collectEdgesByBetweenPrefix(treeDBTypedEdgeBetweenIndexPrefix(startID, endID, edgeType))
	if err != nil || len(edges) == 0 {
		return nil
	}
	return edges[0]
}

// GetInDegree returns the number of incoming edges.
func (e *TreeDBEngine) GetInDegree(nodeID NodeID) int {
	edges, err := e.GetIncomingEdges(nodeID)
	if err != nil {
		return 0
	}
	return len(edges)
}

// GetOutDegree returns the number of outgoing edges.
func (e *TreeDBEngine) GetOutDegree(nodeID NodeID) int {
	edges, err := e.GetOutgoingEdges(nodeID)
	if err != nil {
		return 0
	}
	return len(edges)
}

// StreamNodes streams all nodes.
func (e *TreeDBEngine) StreamNodes(ctx context.Context, fn func(*Node) error) error {
	if err := e.ensureOpen(); err != nil {
		return err
	}
	it, err := e.db.Iterator([]byte{prefixNode}, treeDBRangeEnd([]byte{prefixNode}))
	if err != nil {
		return mapTreeDBError(err)
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		node, err := deserializeNode(it.Value())
		if err != nil {
			return err
		}
		if err := fn(node); err != nil {
			if err == ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return mapTreeDBError(it.Error())
}

// StreamEdges streams all edges.
func (e *TreeDBEngine) StreamEdges(ctx context.Context, fn func(*Edge) error) error {
	if err := e.ensureOpen(); err != nil {
		return err
	}
	it, err := e.db.Iterator([]byte{prefixEdge}, treeDBRangeEnd([]byte{prefixEdge}))
	if err != nil {
		return mapTreeDBError(err)
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		edge, err := deserializeEdge(it.Value())
		if err != nil {
			return err
		}
		if err := fn(edge); err != nil {
			if err == ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return mapTreeDBError(it.Error())
}

// StreamNodeChunks streams nodes in chunks.
func (e *TreeDBEngine) StreamNodeChunks(ctx context.Context, chunkSize int, fn func([]*Node) error) error {
	if chunkSize <= 0 {
		chunkSize = 1000
	}
	if err := e.ensureOpen(); err != nil {
		return err
	}
	it, err := e.db.Iterator([]byte{prefixNode}, treeDBRangeEnd([]byte{prefixNode}))
	if err != nil {
		return mapTreeDBError(err)
	}
	defer it.Close()
	chunk := make([]*Node, 0, chunkSize)
	for ; it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		node, err := deserializeNode(it.Value())
		if err != nil {
			return err
		}
		chunk = append(chunk, node)
		if len(chunk) == chunkSize {
			if err := fn(chunk); err != nil {
				if err == ErrIterationStopped {
					return nil
				}
				return err
			}
			chunk = chunk[:0]
		}
	}
	if err := it.Error(); err != nil {
		return mapTreeDBError(err)
	}
	if len(chunk) == 0 {
		return nil
	}
	if err := fn(chunk); err != nil && err != ErrIterationStopped {
		return err
	}
	return nil
}

// StreamNodesByPrefix streams nodes whose IDs have prefix.
func (e *TreeDBEngine) StreamNodesByPrefix(ctx context.Context, prefix string, fn func(*Node) error) error {
	if err := e.ensureOpen(); err != nil {
		return err
	}
	seekPrefix := append([]byte{prefixNode}, prefix...)
	it, err := e.db.Iterator(seekPrefix, treeDBRangeEnd(seekPrefix))
	if err != nil {
		return mapTreeDBError(err)
	}
	defer it.Close()
	for ; it.Valid(); it.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		node, err := deserializeNode(it.Value())
		if err != nil {
			return err
		}
		if err := fn(node); err != nil {
			if err == ErrIterationStopped {
				return nil
			}
			return err
		}
	}
	return mapTreeDBError(it.Error())
}

func (e *TreeDBEngine) applyCountDeltas(nodeDelta, edgeDelta int64, nodePrefixDeltas, edgePrefixDeltas map[string]int64) {
	if nodeDelta != 0 {
		e.nodeCount.Add(nodeDelta)
	}
	if edgeDelta != 0 {
		e.edgeCount.Add(edgeDelta)
	}
	if len(nodePrefixDeltas) == 0 && len(edgePrefixDeltas) == 0 {
		return
	}
	e.countsMu.Lock()
	defer e.countsMu.Unlock()
	for prefix, delta := range nodePrefixDeltas {
		e.namespaceNodeCounts[prefix] += delta
		if e.namespaceNodeCounts[prefix] <= 0 {
			delete(e.namespaceNodeCounts, prefix)
		}
	}
	for prefix, delta := range edgePrefixDeltas {
		e.namespaceEdgeCounts[prefix] += delta
		if e.namespaceEdgeCounts[prefix] <= 0 {
			delete(e.namespaceEdgeCounts, prefix)
		}
	}
}

func (e *TreeDBEngine) notifyNodeCreated(node *Node) {
	e.callbackMu.RLock()
	cb := e.onNodeCreated
	e.callbackMu.RUnlock()
	if cb != nil {
		cb(node)
	}
}

func (e *TreeDBEngine) notifyNodeUpdated(node *Node) {
	e.callbackMu.RLock()
	cb := e.onNodeUpdated
	e.callbackMu.RUnlock()
	if cb != nil {
		cb(node)
	}
}

func (e *TreeDBEngine) notifyNodeDeleted(id NodeID) {
	e.callbackMu.RLock()
	cb := e.onNodeDeleted
	e.callbackMu.RUnlock()
	if cb != nil {
		cb(id)
	}
}

func (e *TreeDBEngine) notifyEdgeCreated(edge *Edge) {
	e.callbackMu.RLock()
	cb := e.onEdgeCreated
	e.callbackMu.RUnlock()
	if cb != nil {
		cb(edge)
	}
}

func (e *TreeDBEngine) notifyEdgeUpdated(edge *Edge) {
	e.callbackMu.RLock()
	cb := e.onEdgeUpdated
	e.callbackMu.RUnlock()
	if cb != nil {
		cb(edge)
	}
}

func (e *TreeDBEngine) notifyEdgeDeleted(id EdgeID) {
	e.callbackMu.RLock()
	cb := e.onEdgeDeleted
	e.callbackMu.RUnlock()
	if cb != nil {
		cb(id)
	}
}

// OnNodeCreated sets the node-created callback.
func (e *TreeDBEngine) OnNodeCreated(callback NodeEventCallback) {
	e.callbackMu.Lock()
	defer e.callbackMu.Unlock()
	e.onNodeCreated = callback
}

// OnNodeUpdated sets the node-updated callback.
func (e *TreeDBEngine) OnNodeUpdated(callback NodeEventCallback) {
	e.callbackMu.Lock()
	defer e.callbackMu.Unlock()
	e.onNodeUpdated = callback
}

// OnNodeDeleted sets the node-deleted callback.
func (e *TreeDBEngine) OnNodeDeleted(callback NodeDeleteCallback) {
	e.callbackMu.Lock()
	defer e.callbackMu.Unlock()
	e.onNodeDeleted = callback
}

// OnEdgeCreated sets the edge-created callback.
func (e *TreeDBEngine) OnEdgeCreated(callback EdgeEventCallback) {
	e.callbackMu.Lock()
	defer e.callbackMu.Unlock()
	e.onEdgeCreated = callback
}

// OnEdgeUpdated sets the edge-updated callback.
func (e *TreeDBEngine) OnEdgeUpdated(callback EdgeEventCallback) {
	e.callbackMu.Lock()
	defer e.callbackMu.Unlock()
	e.onEdgeUpdated = callback
}

// OnEdgeDeleted sets the edge-deleted callback.
func (e *TreeDBEngine) OnEdgeDeleted(callback EdgeDeleteCallback) {
	e.callbackMu.Lock()
	defer e.callbackMu.Unlock()
	e.onEdgeDeleted = callback
}

// FindNodeNeedingEmbedding returns one node from TreeDB's persistent pending index.
func (e *TreeDBEngine) FindNodeNeedingEmbedding() *Node {
	if e.ensureOpen() != nil {
		return nil
	}
	prefix := []byte{prefixPendingEmbed}
	it, err := e.db.Iterator(prefix, treeDBRangeEnd(prefix))
	if err != nil {
		return nil
	}
	defer it.Close()

	tx, err := e.beginTreeDBTransaction()
	if err != nil {
		return nil
	}
	defer tx.Rollback()

	var found *Node
	removedStale := 0
	removedNoLongerNeeds := 0
	for ; it.Valid(); it.Next() {
		key := it.Key()
		if len(key) <= 1 {
			continue
		}
		nodeID := NodeID(string(key[1:]))
		if isSystemNamespaceID(string(nodeID)) {
			_ = tx.deleteKey(pendingEmbedKey(nodeID))
			removedNoLongerNeeds++
			continue
		}
		node, err := tx.GetNode(nodeID)
		if errors.Is(err, ErrNotFound) {
			_ = tx.deleteKey(pendingEmbedKey(nodeID))
			removedStale++
			continue
		}
		if err != nil || node == nil {
			_ = tx.deleteKey(pendingEmbedKey(nodeID))
			removedStale++
			continue
		}
		if !NodeNeedsEmbedding(node) {
			_ = tx.deleteKey(pendingEmbedKey(nodeID))
			removedNoLongerNeeds++
			continue
		}
		found = node
		break
	}
	if removedStale > 0 || removedNoLongerNeeds > 0 {
		if err := tx.Commit(); err != nil {
			e.log.Debug("pending embeddings cleanup failed",
				"subsystem", "embeddings_index",
				slog.Any("error", err),
			)
		} else {
			e.log.Info("pending embeddings cleanup",
				"subsystem", "embeddings_index",
				"removed_stale", removedStale,
				"removed_no_longer_needed", removedNoLongerNeeds,
			)
		}
	}
	return found
}

// MarkNodeEmbedded removes a node from the pending embeddings index.
func (e *TreeDBEngine) MarkNodeEmbedded(nodeID NodeID) {
	tx, err := e.beginTreeDBPointTransaction()
	if err != nil {
		return
	}
	if err := tx.deleteKey(pendingEmbedKey(nodeID)); err != nil {
		_ = tx.Rollback()
		return
	}
	_ = tx.Commit()
}

// AddToPendingEmbeddings adds a node to the pending embeddings index.
func (e *TreeDBEngine) AddToPendingEmbeddings(nodeID NodeID) {
	tx, err := e.beginTreeDBPointTransaction()
	if err != nil {
		return
	}
	if err := tx.setKey(pendingEmbedKey(nodeID), treeDBEmptyValue); err != nil {
		_ = tx.Rollback()
		return
	}
	_ = tx.Commit()
}

// PendingEmbeddingsCount returns the number of pending embedding markers.
func (e *TreeDBEngine) PendingEmbeddingsCount() int {
	if e.ensureOpen() != nil {
		return 0
	}
	prefix := []byte{prefixPendingEmbed}
	it, err := e.db.Iterator(prefix, treeDBRangeEnd(prefix))
	if err != nil {
		return 0
	}
	defer it.Close()
	count := 0
	for ; it.Valid(); it.Next() {
		count++
	}
	return count
}

// InvalidatePendingEmbeddingsIndex is a no-op for TreeDB's persistent index.
func (e *TreeDBEngine) InvalidatePendingEmbeddingsIndex() {}

// RefreshPendingEmbeddingsIndex reconciles TreeDB pending markers against node bodies.
func (e *TreeDBEngine) RefreshPendingEmbeddingsIndex() int {
	if e.ensureOpen() != nil {
		return 0
	}
	added := 0
	removed := 0

	tx, err := e.beginTreeDBTransaction()
	if err != nil {
		return 0
	}
	defer tx.Rollback()

	pendingPrefix := []byte{prefixPendingEmbed}
	pending, err := e.db.Iterator(pendingPrefix, treeDBRangeEnd(pendingPrefix))
	if err != nil {
		return 0
	}
	for ; pending.Valid(); pending.Next() {
		key := pending.Key()
		if len(key) <= 1 {
			continue
		}
		nodeID := NodeID(string(key[1:]))
		node, err := tx.GetNode(nodeID)
		if isSystemNamespaceID(string(nodeID)) || errors.Is(err, ErrNotFound) || err != nil || !NodeNeedsEmbedding(node) {
			_ = tx.deleteKey(pendingEmbedKey(nodeID))
			removed++
		}
	}
	pending.Close()

	nodePrefix := []byte{prefixNode}
	nodes, err := e.db.Iterator(nodePrefix, treeDBRangeEnd(nodePrefix))
	if err != nil {
		return 0
	}
	for ; nodes.Valid(); nodes.Next() {
		key := nodes.Key()
		if len(key) <= 1 {
			continue
		}
		nodeID := NodeID(string(key[1:]))
		if isSystemNamespaceID(string(nodeID)) {
			continue
		}
		node, err := deserializeNode(nodes.Value())
		if err != nil || !e.shouldIndexPendingEmbed(node) {
			continue
		}
		exists, err := tx.tx.Has(pendingEmbedKey(nodeID))
		if err != nil {
			continue
		}
		if !exists {
			if err := tx.setKey(pendingEmbedKey(nodeID), treeDBEmptyValue); err == nil {
				added++
			}
		}
	}
	nodes.Close()

	if added > 0 || removed > 0 {
		if err := tx.Commit(); err != nil {
			e.log.Debug("pending embeddings refresh failed",
				"subsystem", "embeddings_index",
				slog.Any("error", err),
			)
			return 0
		}
		e.log.Info("pending embeddings index refreshed",
			"subsystem", "embeddings_index",
			"added", added,
			"removed_stale", removed,
		)
	}
	return added
}

// ClearAllEmbeddings removes managed embeddings from all nodes.
func (e *TreeDBEngine) ClearAllEmbeddings() (int, error) {
	return e.ClearAllEmbeddingsForPrefix("")
}

// ClearAllEmbeddingsForPrefix removes managed embeddings from matching nodes.
func (e *TreeDBEngine) ClearAllEmbeddingsForPrefix(idPrefix string) (int, error) {
	if err := e.ensureOpen(); err != nil {
		return 0, err
	}
	var ids []NodeID
	err := e.StreamNodesByPrefix(context.Background(), idPrefix, func(node *Node) error {
		if len(node.ChunkEmbeddings) > 0 && len(node.ChunkEmbeddings[0]) > 0 {
			ids = append(ids, node.ID)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	cleared := 0
	for _, id := range ids {
		node, err := e.GetNode(id)
		if err != nil {
			continue
		}
		node.ChunkEmbeddings = nil
		if err := e.UpdateNode(node); err != nil {
			e.log.Warn("failed to clear embedding for node",
				"subsystem", "embeddings_index",
				"node_id", string(id),
				slog.Any("error", err),
			)
			continue
		}
		cleared++
	}
	_ = e.RefreshPendingEmbeddingsIndex()
	return cleared, nil
}

func mapTreeDBError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, treedb.ErrKeyNotFound):
		return ErrNotFound
	case errors.Is(err, treedb.ErrConcurrentModification):
		return ErrConflict
	case errors.Is(err, treedb.ErrConditionalTxnClosed):
		return ErrTransactionClosed
	case errors.Is(err, treedb.ErrClosed):
		return ErrStorageClosed
	case errors.Is(err, treedb.ErrConditionalTxnUnsupported):
		return ErrNotImplemented
	default:
		return err
	}
}

func treeDBRangeEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func treeDBNodeIDWithNamespacePrefix(prefix string, id NodeID) NodeID {
	if prefix == "" || strings.HasPrefix(string(id), prefix) {
		return id
	}
	return NodeID(prefix + string(id))
}

func treeDBEdgeIDWithNamespacePrefix(prefix string, id EdgeID) EdgeID {
	if prefix == "" || strings.HasPrefix(string(id), prefix) {
		return id
	}
	return EdgeID(prefix + string(id))
}

func treeDBUnprefixNodeID(prefix string, id NodeID) NodeID {
	if prefix == "" {
		return id
	}
	s := string(id)
	if strings.HasPrefix(s, prefix) {
		return NodeID(s[len(prefix):])
	}
	return id
}

func treeDBUnprefixEdgeID(prefix string, id EdgeID) EdgeID {
	if prefix == "" {
		return id
	}
	s := string(id)
	if strings.HasPrefix(s, prefix) {
		return EdgeID(s[len(prefix):])
	}
	return id
}

func treeDBNodeEdgeGuardKey(nodeID NodeID) []byte {
	key := make([]byte, 0, 1+len(nodeID))
	key = append(key, treeDBPrefixNodeEdgeGuard)
	key = append(key, string(nodeID)...)
	return key
}

func treeDBLabelIndexKey(label string, id NodeID) []byte {
	normalized := normalizeLabel(label)
	key := make([]byte, 0, 1+len(normalized)+1+len(id))
	key = append(key, prefixLabelIndex)
	key = append(key, normalized...)
	key = append(key, 0)
	key = append(key, string(id)...)
	return key
}

func treeDBLabelIndexPrefix(label string) []byte {
	normalized := normalizeLabel(label)
	key := make([]byte, 0, 1+len(normalized)+1)
	key = append(key, prefixLabelIndex)
	key = append(key, normalized...)
	key = append(key, 0)
	return key
}

func treeDBLabelIndexPrefixForNamespace(label, namespace string) []byte {
	key := treeDBLabelIndexPrefix(label)
	return append(key, treeDBNamespaceIDPrefix(namespace)...)
}

func treeDBOutgoingIndexKey(nodeID NodeID, edgeID EdgeID) []byte {
	key := make([]byte, 0, 1+len(nodeID)+1+len(edgeID))
	key = append(key, prefixOutgoingIndex)
	key = append(key, string(nodeID)...)
	key = append(key, 0)
	key = append(key, string(edgeID)...)
	return key
}

func treeDBOutgoingIndexPrefix(nodeID NodeID) []byte {
	key := make([]byte, 0, 1+len(nodeID)+1)
	key = append(key, prefixOutgoingIndex)
	key = append(key, string(nodeID)...)
	key = append(key, 0)
	return key
}

func treeDBIncomingIndexKey(nodeID NodeID, edgeID EdgeID) []byte {
	key := make([]byte, 0, 1+len(nodeID)+1+len(edgeID))
	key = append(key, prefixIncomingIndex)
	key = append(key, string(nodeID)...)
	key = append(key, 0)
	key = append(key, string(edgeID)...)
	return key
}

func treeDBIncomingIndexPrefix(nodeID NodeID) []byte {
	key := make([]byte, 0, 1+len(nodeID)+1)
	key = append(key, prefixIncomingIndex)
	key = append(key, string(nodeID)...)
	key = append(key, 0)
	return key
}

func treeDBEdgeTypeIndexKey(edgeType string, edgeID EdgeID) []byte {
	normalized := normalizeLabel(edgeType)
	key := make([]byte, 0, 1+len(normalized)+1+len(edgeID))
	key = append(key, prefixEdgeTypeIndex)
	key = append(key, normalized...)
	key = append(key, 0)
	key = append(key, string(edgeID)...)
	return key
}

func treeDBEdgeTypeIndexPrefix(edgeType string) []byte {
	normalized := normalizeLabel(edgeType)
	key := make([]byte, 0, 1+len(normalized)+1)
	key = append(key, prefixEdgeTypeIndex)
	key = append(key, normalized...)
	key = append(key, 0)
	return key
}

func treeDBEdgeTypeIndexPrefixForNamespace(edgeType, namespace string) []byte {
	key := treeDBEdgeTypeIndexPrefix(edgeType)
	return append(key, treeDBNamespaceIDPrefix(namespace)...)
}

func treeDBEdgeBetweenIndexKey(startID, endID NodeID, edgeType string, edgeID EdgeID) []byte {
	normalized := normalizeLabel(edgeType)
	key := make([]byte, 0, 1+len(startID)+1+len(endID)+1+len(normalized)+1+len(edgeID))
	key = append(key, prefixEdgeBetweenIndex)
	key = append(key, string(startID)...)
	key = append(key, 0)
	key = append(key, string(endID)...)
	key = append(key, 0)
	key = append(key, normalized...)
	key = append(key, 0)
	key = append(key, string(edgeID)...)
	return key
}

func treeDBEdgeBetweenIndexPrefix(startID, endID NodeID) []byte {
	key := make([]byte, 0, 1+len(startID)+1+len(endID)+1)
	key = append(key, prefixEdgeBetweenIndex)
	key = append(key, string(startID)...)
	key = append(key, 0)
	key = append(key, string(endID)...)
	key = append(key, 0)
	return key
}

func treeDBTypedEdgeBetweenIndexPrefix(startID, endID NodeID, edgeType string) []byte {
	normalized := normalizeLabel(edgeType)
	key := make([]byte, 0, 1+len(startID)+1+len(endID)+1+len(normalized)+1)
	key = append(key, prefixEdgeBetweenIndex)
	key = append(key, string(startID)...)
	key = append(key, 0)
	key = append(key, string(endID)...)
	key = append(key, 0)
	key = append(key, normalized...)
	key = append(key, 0)
	return key
}

func treeDBEdgeBetweenHeadKey(startID, endID NodeID, edgeType string) []byte {
	normalized := normalizeLabel(edgeType)
	key := make([]byte, 0, 1+len(startID)+1+len(endID)+1+len(normalized))
	key = append(key, prefixEdgeBetweenHead)
	key = append(key, string(startID)...)
	key = append(key, 0)
	key = append(key, string(endID)...)
	key = append(key, 0)
	key = append(key, normalized...)
	return key
}

func treeDBEdgeIDFromBetweenKey(key []byte) EdgeID {
	idx := bytes.LastIndexByte(key, 0)
	if idx < 0 || idx+1 >= len(key) {
		return ""
	}
	return EdgeID(string(key[idx+1:]))
}

func treeDBEdgeMatchesBetween(edge *Edge, startID, endID NodeID, edgeType string) bool {
	return edge != nil &&
		edge.StartNode == startID &&
		edge.EndNode == endID &&
		strings.EqualFold(edge.Type, edgeType)
}

func treeDBNodePrefixForNamespace(namespace string) []byte {
	key := []byte{prefixNode}
	return append(key, treeDBNamespaceIDPrefix(namespace)...)
}

func treeDBEdgePrefixForNamespace(namespace string) []byte {
	key := []byte{prefixEdge}
	return append(key, treeDBNamespaceIDPrefix(namespace)...)
}

func treeDBNamespaceIDPrefix(namespace string) []byte {
	if namespace == "" {
		return nil
	}
	namespace = strings.TrimSuffix(namespace, ":")
	if namespace == "" {
		return nil
	}
	key := make([]byte, 0, len(namespace)+1)
	key = append(key, namespace...)
	key = append(key, ':')
	return key
}

func treeDBNamespaceForGuard(prefix string) string {
	if prefix == "" {
		return ""
	}
	if idx := strings.IndexByte(prefix, ':'); idx > 0 {
		return prefix[:idx]
	}
	return ""
}

func treeDBNodeNamespaceGuardKey(namespace string) []byte {
	namespace = strings.TrimSuffix(namespace, ":")
	key := make([]byte, 0, 2+len(namespace))
	key = append(key, treeDBPrefixScanGuard, 'N')
	key = append(key, namespace...)
	return key
}

func treeDBNodeNamespaceReadGuardKey(namespace string) []byte {
	if namespace == "" {
		return nil
	}
	return treeDBNodeNamespaceGuardKey(namespace)
}

func treeDBEdgeNamespaceGuardKey(namespace string) []byte {
	namespace = strings.TrimSuffix(namespace, ":")
	key := make([]byte, 0, 2+len(namespace))
	key = append(key, treeDBPrefixScanGuard, 'E')
	key = append(key, namespace...)
	return key
}

func treeDBEdgeNamespaceReadGuardKey(namespace string) []byte {
	if namespace == "" {
		return nil
	}
	return treeDBEdgeNamespaceGuardKey(namespace)
}

func treeDBValidPrefixedID(kind string, id string) error {
	if id == "" {
		return ErrInvalidID
	}
	if !strings.Contains(id, ":") {
		return fmt.Errorf("%s ID must be prefixed with namespace (e.g., 'nornic:%s-123'), got unprefixed ID: %s", kind, kind, id)
	}
	return nil
}

func treeDBNamespaceFromID(id string) (string, error) {
	ns, ok := namespacePrefixFromID(id)
	if !ok {
		return "", fmt.Errorf("ID must be prefixed with namespace, got: %s", id)
	}
	return strings.TrimSuffix(ns, ":"), nil
}

func treeDBLabelContains(labels []string, label string) bool {
	normalized := normalizeLabel(label)
	for _, candidate := range labels {
		if normalizeLabel(candidate) == normalized {
			return true
		}
	}
	return false
}
