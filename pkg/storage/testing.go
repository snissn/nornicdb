package storage

// Helpers in this file are exported for use by external test packages
// (pkg/cypher/..., integration tests). They are compiled into the
// production binary but intentionally limited to construction helpers
// that would otherwise be pasted across _test.go files.

import "fmt"

// NewMemoryEngineWithMVCCHistory creates an in-memory engine that retains
// historical MVCC versions. Use this for tests and callers that exercise
// the multi-version surface (temporal.asOf, time-travel reads, prior-version
// snapshots). The default NewMemoryEngine runs head-only, matching the
// production default.
func NewMemoryEngineWithMVCCHistory() *MemoryEngine {
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{
		InMemory: true,
		EngineOptions: EngineOptions{
			RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 100},
		},
	})
	if err != nil {
		panic(fmt.Sprintf("failed to create in-memory BadgerEngine: %v", err))
	}
	return &MemoryEngine{BadgerEngine: engine}
}
