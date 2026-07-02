// Plan 04-04-07: cmd/nornicdb wiring for StorageMetrics + MVCCMetrics.
// The adapters below route metrics through storage capability interfaces so
// Badger keeps its existing metrics and TreeDB can expose supported metrics
// without concrete-engine checks in startup wiring.
package main

import (
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type storageMetricsAttacher interface {
	AttachMetrics(*observability.StorageMetrics, *observability.MVCCMetrics)
}

type idDictionaryStatsProvider interface {
	IDDictCounters() (nodeMax, edgeMax uint64)
	IDDictFreelistPending() (nodes, edges int64)
}

// engineStorageProbe satisfies observability.StorageProbe for any storage
// engine. ID dictionary gauges are populated only when the backend implements
// the optional idDictionaryStatsProvider capability.
type engineStorageProbe struct {
	engine storage.Engine
	idDict idDictionaryStatsProvider
}

func newEngineStorageProbe(engine storage.Engine) engineStorageProbe {
	idDict, _ := storage.FindCapability[idDictionaryStatsProvider](engine)
	return engineStorageProbe{engine: engine, idDict: idDict}
}

func (p engineStorageProbe) NodeCount() int64 {
	if p.engine == nil {
		return 0
	}
	n, err := p.engine.NodeCount()
	if err != nil {
		return 0
	}
	return n
}

func (p engineStorageProbe) EdgeCount() int64 {
	if p.engine == nil {
		return 0
	}
	n, err := p.engine.EdgeCount()
	if err != nil {
		return 0
	}
	return n
}

func (p engineStorageProbe) IDDictCounterNodes() uint64 {
	if p.idDict == nil {
		return 0
	}
	n, _ := p.idDict.IDDictCounters()
	return n
}

func (p engineStorageProbe) IDDictCounterEdges() uint64 {
	if p.idDict == nil {
		return 0
	}
	_, e := p.idDict.IDDictCounters()
	return e
}

func (p engineStorageProbe) IDDictFreelistNodes() int64 {
	if p.idDict == nil {
		return 0
	}
	n, _ := p.idDict.IDDictFreelistPending()
	return n
}

func (p engineStorageProbe) IDDictFreelistEdges() int64 {
	if p.idDict == nil {
		return 0
	}
	_, e := p.idDict.IDDictFreelistPending()
	return e
}

type mvccStatsProvider interface {
	PinnedBytes() int64
	OldestReaderAgeSeconds() float64
	ActiveReaders() int64
}

// engineMVCCProbe satisfies observability.MVCCProbe when the backend exposes
// MVCC metrics. Unsupported backends simply do not register MVCC metrics.
type engineMVCCProbe struct {
	provider mvccStatsProvider
}

func newEngineMVCCProbe(engine storage.Engine) (engineMVCCProbe, bool) {
	provider, ok := storage.FindCapability[mvccStatsProvider](engine)
	return engineMVCCProbe{provider: provider}, ok
}

func (p engineMVCCProbe) PinnedBytes() int64              { return p.provider.PinnedBytes() }
func (p engineMVCCProbe) OldestReaderAgeSeconds() float64 { return p.provider.OldestReaderAgeSeconds() }
func (p engineMVCCProbe) ActiveReaders() int64            { return p.provider.ActiveReaders() }
