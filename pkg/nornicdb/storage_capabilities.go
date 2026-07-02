package nornicdb

import "github.com/orneryd/nornicdb/pkg/storage"

// StorageCapabilities returns a slow-path snapshot of optional backend
// capabilities available through the DB's base storage chain.
func (db *DB) StorageCapabilities() storage.EngineCapabilities {
	db.mu.RLock()
	eng := db.baseStorage
	if eng == nil {
		eng = db.storage
	}
	db.mu.RUnlock()
	return storage.InspectEngineCapabilities(eng)
}

func (db *DB) temporalMaintenanceNoLock() (storage.TemporalMaintenanceEngine, bool) {
	return storage.FindCapability[storage.TemporalMaintenanceEngine](db.baseStorage)
}

func (db *DB) mvccMaintenanceNoLock() (storage.MVCCMaintenanceEngine, bool) {
	return storage.FindCapability[storage.MVCCMaintenanceEngine](db.baseStorage)
}

func (db *DB) mvccLifecycleNoLock() (storage.MVCCLifecycleEngine, bool) {
	return storage.FindCapability[storage.MVCCLifecycleEngine](db.baseStorage)
}
