package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// namespaceForNodeIDs returns the shared namespace of a slice of node IDs,
// or an error if the slice is empty / mixed-namespace. Used by the engine's
// non-transactional bulk APIs (BulkCreateNodes / BulkDeleteNodes) to route
// the batch's MVCC version allocation through a single namespace's
// counter — the per-database invariant that BadgerTransaction enforces
// at the transaction layer also has to hold for these batch APIs.
func namespaceForNodeIDs(ids []NodeID) (string, error) {
	var ns string
	for _, id := range ids {
		if id == "" {
			continue
		}
		other := namespaceForNodeID(id)
		if other == "" {
			return "", fmt.Errorf("node ID must be prefixed with namespace, got: %s", id)
		}
		if ns == "" {
			ns = other
			continue
		}
		if ns != other {
			return "", fmt.Errorf("%w: batch contains both %q and %q",
				ErrCrossNamespaceTransaction, ns, other)
		}
	}
	if ns == "" {
		return "", fmt.Errorf("batch contains no usable IDs")
	}
	return ns, nil
}

// namespaceForEdgeIDs is the edge-id analogue of namespaceForNodeIDs.
func namespaceForEdgeIDs(ids []EdgeID) (string, error) {
	var ns string
	for _, id := range ids {
		if id == "" {
			continue
		}
		other := namespaceForEdgeID(id)
		if other == "" {
			return "", fmt.Errorf("edge ID must be prefixed with namespace, got: %s", id)
		}
		if ns == "" {
			ns = other
			continue
		}
		if ns != other {
			return "", fmt.Errorf("%w: batch contains both %q and %q",
				ErrCrossNamespaceTransaction, ns, other)
		}
	}
	if ns == "" {
		return "", fmt.Errorf("batch contains no usable IDs")
	}
	return ns, nil
}

// namespaceMVCCState holds the per-database MVCC sequence counter and the
// high-water clamp on commit timestamps. One instance lives in
// BadgerEngine.mvccByNamespace per active namespace.
//
// The two atomics are independent: seq is the strictly-increasing logical
// commit sequence used by snapshotIsolationConflict for total ordering, and
// highWaterNanos is the largest commit-timestamp ever stamped onto a version
// in this namespace. Reads clamp their sample to >= highWaterNanos so a
// backward NTP step cannot make a new transaction observe an earlier
// timestamp than something already committed.
//
// A persistKey is cached so allocate paths never re-encode the namespace
// segment for the on-disk sequence key on the hot path.
type namespaceMVCCState struct {
	seq            atomic.Uint64
	highWaterNanos atomic.Int64
	persistKey     []byte
}

// namespaceMVCC returns the per-namespace MVCC state, lazily loading the
// persisted sequence and seeding the high-water clamp from the legacy
// engine-global counter on first access.
//
// Backward compatibility: the legacy engine-global mvccSequenceKey is read
// once at engine open into b.mvccLegacyGlobalSeed. The first time any
// namespace is touched, its starting sequence is set to
// max(persisted-per-ns, legacyGlobalSeed) — guaranteeing that no version
// in any namespace can ever sit at or below a sequence previously emitted
// under the global counter, so MVCC head records persisted by older
// binaries remain strictly older than anything we mint going forward.
func (b *BadgerEngine) namespaceMVCC(namespace string) (*namespaceMVCCState, error) {
	if namespace == "" {
		return nil, fmt.Errorf("namespaceMVCC: empty namespace")
	}

	b.mvccByNamespaceMu.RLock()
	state, ok := b.mvccByNamespace[namespace]
	b.mvccByNamespaceMu.RUnlock()
	if ok {
		return state, nil
	}

	persisted, err := b.loadPersistedNamespaceSequence(namespace)
	if err != nil {
		return nil, err
	}

	b.mvccByNamespaceMu.Lock()
	defer b.mvccByNamespaceMu.Unlock()
	if existing, ok := b.mvccByNamespace[namespace]; ok {
		return existing, nil
	}
	if b.mvccByNamespace == nil {
		b.mvccByNamespace = make(map[string]*namespaceMVCCState)
	}
	starting := persisted
	if b.mvccLegacyGlobalSeed > starting {
		starting = b.mvccLegacyGlobalSeed
	}
	state = &namespaceMVCCState{
		persistKey: mvccNamespaceSequenceKey(namespace),
	}
	state.seq.Store(starting)
	b.mvccByNamespace[namespace] = state
	return state, nil
}

// loadPersistedNamespaceSequence reads the persisted commit sequence for
// namespace from disk; returns 0 if the key is absent (fresh namespace).
func (b *BadgerEngine) loadPersistedNamespaceSequence(namespace string) (uint64, error) {
	var seq uint64
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(mvccNamespaceSequenceKey(namespace))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) != 8 {
				return fmt.Errorf("invalid mvcc sequence length: %d", len(val))
			}
			seq = binary.BigEndian.Uint64(val)
			return nil
		})
	})
	if err != nil {
		return 0, err
	}
	return seq, nil
}

// nextNamespaceMVCCSequence atomically advances the namespace's commit
// sequence and reports whether the counter has saturated. Saturation
// triggers the same timestamp-bumping fallback that the engine-global path
// used: highWaterNanos is force-advanced by 1ns instead of wrapping the
// uint64 to zero.
func (s *namespaceMVCCState) nextSequence() (uint64, bool) {
	for {
		cur := s.seq.Load()
		if cur == maxMVCCCommitSequence {
			return cur, true
		}
		next := cur + 1
		if s.seq.CompareAndSwap(cur, next) {
			return next, false
		}
	}
}

// reserveCommitTimestamp clamps the namespace's high-water mark to
// max(commitTime, current-high-water+forceAdvance) and returns the
// resulting commit timestamp. forceAdvance is set when the sequence has
// saturated, in which case we must guarantee timestamp strict ordering
// across versions that share the saturated sequence value.
func (s *namespaceMVCCState) reserveCommitTimestamp(commitTime time.Time, forceAdvance bool) (time.Time, error) {
	stampNanos := commitTime.UTC().UnixNano()
	for {
		cur := s.highWaterNanos.Load()
		next := stampNanos
		if forceAdvance {
			if cur == int64(^uint64(0)>>1) {
				return time.Time{}, fmt.Errorf("mvcc commit timestamp exhausted: %w", ErrExhausted)
			}
			if next <= cur {
				next = cur + 1
			}
		} else if next <= cur {
			return commitTime.UTC(), nil
		}
		if s.highWaterNanos.CompareAndSwap(cur, next) {
			return time.Unix(0, next).UTC(), nil
		}
	}
}
