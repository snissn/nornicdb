// Plan 04-04-04: D-07 30s sweep lifecycle.Component populating
// nornicdb_storage_bytes{kind} gauges by calling a backend-native byte stats
// provider for each kind bucket.
//
// Lifecycle semantics (RESEARCH §Architecture Pattern 2):
//   - Start spawns a goroutine that ticks every interval (default 30s).
//   - Each tick wraps `defer recover()` so a transient backend stats panic
//     does not crash the supervisor errgroup (RISK-8).
//   - Shutdown closes the ticker and waits for the goroutine to exit
//     within the supervisor's drain budget (5s typical).
//
// Five `kind` buckets (closed enum AllowedStorageBytesKinds):
//   - nodes  — prefixNode
//   - edges  — prefixEdge
//   - index  — prefixLabelIndex + prefixEdgeBetweenIndex + prefixTemporalIndex
//     (sum across the three index prefixes; user-created indexes
//     are not separately accounted by D-13c)
//   - wal    — backend-specific write-log heuristic; RISK-6
//   - search — sum of IndexSizeBytes() across all per-database search
//     services (Plan 04-05 owns the IndexSizeBytes accessor;
//     this sweeper consumes via the SearchSizeFn callback so the
//     storage layer never imports pkg/search)
//
// The sweeper is registered in cmd/nornicdb between pprof and workers
// components per RESEARCH §Q4 — i.e. it Drains AFTER the workers and
// BEFORE the telemetry listener, so the final scrape during drain still
// reflects the last known sizes.
package storage

import (
	"context"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/observability"
)

// DefaultBytesMetricsInterval is the D-07 default sweep cadence.
const DefaultBytesMetricsInterval = 30 * time.Second

// SearchSizeFn returns the cumulative search-index byte size across all
// per-database search services. Plan 04-05 wires this via search.SearchService
// IndexSizeBytes(); for Plan 04-04 it can be nil (returns 0).
type SearchSizeFn func() int64

// BytesMetricsSweeper is the lifecycle.Component that populates the
// nornicdb_storage_bytes{kind} gauges every interval via a backend-native
// StorageByteStatsProvider.
type BytesMetricsSweeper struct {
	metrics    *observability.StorageMetrics
	provider   StorageByteStatsProvider
	searchSize SearchSizeFn
	interval   time.Duration

	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

// NewBytesMetricsSweeper constructs the sweeper. interval ≤ 0 falls back to
// DefaultBytesMetricsInterval. metrics or provider nil disables the sweep
// (Start returns nil immediately) — defensive for partial-init.
func NewBytesMetricsSweeper(metrics *observability.StorageMetrics, provider StorageByteStatsProvider, searchSize SearchSizeFn, interval time.Duration) *BytesMetricsSweeper {
	if interval <= 0 {
		interval = DefaultBytesMetricsInterval
	}
	return &BytesMetricsSweeper{
		metrics:    metrics,
		provider:   provider,
		searchSize: searchSize,
		interval:   interval,
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
}

// Name implements lifecycle.Component.
func (s *BytesMetricsSweeper) Name() string { return "storage_bytes_metrics" }

// Start implements lifecycle.Component. Blocks until ctx is cancelled or
// Shutdown is called. Performs an initial sweep immediately so the gauges
// have non-zero values by the first scrape.
func (s *BytesMetricsSweeper) Start(ctx context.Context) error {
	defer close(s.done)
	if s.metrics == nil || s.provider == nil {
		// Wait for cancellation but emit nothing.
		select {
		case <-ctx.Done():
		case <-s.stop:
		}
		return nil
	}
	// Initial sweep — don't wait for the first tick.
	s.sweep()
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-s.stop:
			return nil
		case <-t.C:
			s.sweep()
		}
	}
}

// Shutdown implements lifecycle.Component. Idempotent; subsequent calls
// are no-ops.
func (s *BytesMetricsSweeper) Shutdown(ctx context.Context) error {
	s.stopOnce.Do(func() {
		close(s.stop)
	})
	select {
	case <-s.done:
	case <-ctx.Done():
	}
	return nil
}

// sweep updates the five kind-gauges. Wrapped in defer-recover per
// RESEARCH RISK-8 so a backend stats panic during concurrent maintenance cannot
// poison the supervisor errgroup. Each kind is computed independently;
// failure of one accessor does not block the others.
func (s *BytesMetricsSweeper) sweep() {
	defer func() {
		if r := recover(); r != nil {
			// Swallow panic — better to publish stale gauges than crash
			// the supervisor. The /metrics scrape will still surface the
			// last known values.
			_ = r
		}
	}()

	stats := s.provider.StorageByteStats()
	setSupportedStorageBytes(s.metrics, "nodes", stats.Nodes, stats.NodesSupported)
	setSupportedStorageBytes(s.metrics, "edges", stats.Edges, stats.EdgesSupported)
	setSupportedStorageBytes(s.metrics, "index", stats.Index, stats.IndexSupported)
	setSupportedStorageBytes(s.metrics, "wal", stats.WAL, stats.WALSupported)
	if stats.WALSupported {
		// wal_lag_bytes carries the same heuristic on its dedicated gauge so
		// alert rules don't have to filter the kind label.
		s.metrics.WALLagBytes.Set(float64(nonNegativeInt64(stats.WAL)))
	}

	// search: callback is owned by Plan 04-05; nil tolerated.
	if s.searchSize != nil {
		s.metrics.Bytes.WithLabelValues("search").Set(float64(s.searchSize()))
	} else {
		s.metrics.Bytes.WithLabelValues("search").Set(0)
	}
}

func setSupportedStorageBytes(metrics *observability.StorageMetrics, kind string, value int64, supported bool) {
	if !supported {
		return
	}
	metrics.Bytes.WithLabelValues(kind).Set(float64(nonNegativeInt64(value)))
}

func nonNegativeInt64(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}
