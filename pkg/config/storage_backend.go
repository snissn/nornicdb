package config

import (
	"fmt"
	"strings"
)

const (
	// StorageBackendBadger selects the existing Badger-backed persistent engine.
	StorageBackendBadger = "badger"
	// StorageBackendTreeDB selects the TreeDB-backed persistent engine.
	StorageBackendTreeDB = "treedb"
	// StorageBackendMemory selects the in-memory engine used by tests and ephemeral sessions.
	StorageBackendMemory = "memory"
)

// NormalizeStorageBackend canonicalizes a storage backend selector.
func NormalizeStorageBackend(backend string) string {
	backend = strings.ToLower(strings.TrimSpace(backend))
	if backend == "" {
		return StorageBackendBadger
	}
	return backend
}

// ValidateStorageBackend rejects unsupported storage backend selectors.
func ValidateStorageBackend(backend string) error {
	switch NormalizeStorageBackend(backend) {
	case StorageBackendBadger, StorageBackendTreeDB, StorageBackendMemory:
		return nil
	default:
		return fmt.Errorf("invalid storage backend %q (supported: badger, treedb, memory)", backend)
	}
}
