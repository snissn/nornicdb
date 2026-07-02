package storage

// StorageMaintenanceEngine is an optional extension interface for slow-path
// durability and garbage-collection maintenance.
type StorageMaintenanceEngine interface {
	Sync() error
	RunGC() error
	Size() (lsm, vlog int64)
}

// StorageByteStats reports bounded storage-size gauges for observability.
type StorageByteStats struct {
	// Nodes is the estimated byte footprint for stored node records.
	Nodes int64
	// NodesSupported reports whether Nodes is native/bounded accounting.
	NodesSupported bool
	// Edges is the estimated byte footprint for stored edge records.
	Edges int64
	// EdgesSupported reports whether Edges is native/bounded accounting.
	EdgesSupported bool
	// Index is the estimated byte footprint for index/page-file state.
	Index int64
	// IndexSupported reports whether Index is native/bounded accounting.
	IndexSupported bool
	// WAL is the estimated byte footprint for write-ahead or redo-log state.
	WAL int64
	// WALSupported reports whether WAL is native/bounded accounting.
	WALSupported bool
}

// StorageByteStatsProvider is an optional extension interface for metrics
// sweeps. Implementations must use bounded/native accounting and avoid full
// graph scans.
type StorageByteStatsProvider interface {
	StorageByteStats() StorageByteStats
}

// StorageDiagnosticsEngine is an optional extension interface for backend
// diagnostic stats exposed through admin and metrics slow paths.
type StorageDiagnosticsEngine interface {
	Stats() map[string]string
}

// TreeDBDurabilityReporter is implemented by TreeDB-backed engines that expose
// their selected durable write path.
type TreeDBDurabilityReporter interface {
	DurabilityInfo() TreeDBDurabilityInfo
}

// EngineCapabilities is a slow-path snapshot of optional backend support.
type EngineCapabilities struct {
	// Backend is the selected physical backend name.
	Backend string
	// StorageMaintenance reports support for sync, GC, and size maintenance.
	StorageMaintenance bool
	// StorageByteStats reports bounded native byte accounting support.
	StorageByteStats bool
	// StorageDiagnostics reports support for backend diagnostic stats.
	StorageDiagnostics bool
	// TreeDBDurability reports support for TreeDB durability inspection.
	TreeDBDurability bool
	// TemporalMaintenance reports support for temporal index maintenance.
	TemporalMaintenance bool
	// MVCCMaintenance reports support for MVCC rebuild and prune operations.
	MVCCMaintenance bool
	// MVCCLifecycle reports support for MVCC lifecycle controls.
	MVCCLifecycle bool
}

// FindCapability walks an Engine wrapper chain and returns the first engine
// implementing T. It is intended for startup/admin/metrics slow paths, not
// per-operation CRUD dispatch.
func FindCapability[T any](eng Engine) (T, bool) {
	var zero T
	for depth := 0; eng != nil && depth < 16; depth++ {
		if capable, ok := any(eng).(T); ok {
			return capable, true
		}
		next := unwrapEngineOnce(eng)
		if next == nil || next == eng {
			return zero, false
		}
		eng = next
	}
	return zero, false
}

// BaseEngine returns the innermost engine after following known wrapper
// accessors. It is a slow-path helper for diagnostics and tests.
func BaseEngine(eng Engine) Engine {
	for depth := 0; eng != nil && depth < 16; depth++ {
		next := unwrapEngineOnce(eng)
		if next == nil || next == eng {
			return eng
		}
		eng = next
	}
	return eng
}

// InspectEngineCapabilities reports native optional backend capabilities
// without invoking expensive backend operations. Wrapper-level no-op adapters
// are ignored so the snapshot reflects what the selected physical backend can
// actually do.
func InspectEngineCapabilities(eng Engine) EngineCapabilities {
	base := BaseEngine(eng)
	caps := EngineCapabilities{Backend: backendName(base)}
	_, caps.StorageMaintenance = FindCapability[StorageMaintenanceEngine](base)
	_, caps.StorageByteStats = FindCapability[StorageByteStatsProvider](base)
	_, caps.StorageDiagnostics = FindCapability[StorageDiagnosticsEngine](base)
	_, caps.TreeDBDurability = FindCapability[TreeDBDurabilityReporter](base)
	_, caps.TemporalMaintenance = FindCapability[TemporalMaintenanceEngine](base)
	_, caps.MVCCMaintenance = FindCapability[MVCCMaintenanceEngine](base)
	_, caps.MVCCLifecycle = FindCapability[MVCCLifecycleEngine](base)
	return caps
}

func unwrapEngineOnce(eng Engine) Engine {
	switch e := eng.(type) {
	case interface{ GetInnerEngine() Engine }:
		return e.GetInnerEngine()
	case interface{ UnwrapEngine() Engine }:
		return e.UnwrapEngine()
	case interface{ Unwrap() Engine }:
		return e.Unwrap()
	case interface{ GetEngine() Engine }:
		return e.GetEngine()
	case interface{ GetUnderlying() Engine }:
		return e.GetUnderlying()
	default:
		return nil
	}
}

func backendName(eng Engine) string {
	switch eng.(type) {
	case *BadgerEngine:
		return "badger"
	case *TreeDBEngine:
		return "treedb"
	case *MemoryEngine:
		return "memory"
	case nil:
		return ""
	default:
		return "unknown"
	}
}
