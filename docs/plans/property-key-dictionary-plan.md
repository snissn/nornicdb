# Property-Key Dictionary Plan

**Status:** Proposed
**Date:** May 13, 2026
**Scope:** Introduce a per-namespace property-key dictionary that tokenizes property-key _names_ to varint IDs in node and edge bodies. Backwards-compatible with existing on-disk storage versions: the storage version is bumped, new writes go through the tokenized codec, and reads dispatch on a per-record format byte so pre-existing untokenized bodies continue to decode without rewriting.

---

## 1. Objective

Eliminate repeated property-key name strings from node and edge bodies. Under the current msgpack encoding, every entity's property map pays ~80–100 B of pure overhead for key names like `productName`, `unitPrice`, `description`. Over a 409k-entity seed this already accounts for ~30 MB of wasted bytes; at 1 B entities the savings project to roughly 75 GB.

The dictionary is **per namespace** (per logical database) so multi-tenant instances keep their key ID spaces isolated. Tenants with schema sprawl cannot exhaust another tenant's ID space, and each dictionary compacts independently. Property-key IDs are **varint-encoded**: the top ~20 keys in any real schema cover >80% of occurrences and fit in one byte, while the ceiling remains effectively infinite at ~2 B distinct keys per namespace.

This plan is **backwards compatible**. The on-disk storage version is bumped to signal that tokenized bodies _may_ exist, but pre-existing bodies remain valid in place. Decoding dispatches on a per-record format byte: untokenized bodies decode through the legacy path, tokenized bodies through the new path. There is no required migration step — old bodies are upgraded lazily on next write (or by an optional offline rewrite tool, out of scope here).

---

## 2. Problem Statement

A Northwind Product node today occupies ~296 B in its encoded property map. Of that, ~79 B is key-name overhead (`productID`, `productName`, `sku`, `unitPrice`, `unitsInStock`, `discontinued`, `description`, `tags`). Every copy of the same schema pays this cost again.

Neo4j avoids this with a global `PropertyKeyTokenStore`: each key name is stored once and referenced by a 32-bit token inside record bodies. NornicDB's current msgpack encoding has no such indirection — `node.Properties` is serialized as a string-keyed map by the msgpack library, which writes each key name inline.

The savings at scale are material:

- Northwind 100k seed: ~30 MB on property maps alone
- Extrapolated to 1 B entities with the same schema shape: ~75 GB

Beyond byte savings, tokenizing property keys produces a single source of truth for the set of keys a namespace has ever used, which is useful for schema introspection, observability, and future planner decisions (e.g., "does this key exist at all before we plan an index probe").

---

## 3. Scope

### In scope

- A new `propertyKeyDictionary` mirroring `idDictionary`'s structure: per-namespace forward (name → id) and reverse (id → name) maps, backed by Badger keys.
- Varint encoding of property key IDs inside node and edge bodies, gated by a per-record format byte.
- A storage-version bump signaling that tokenized bodies may be present.
- A decode-time helper (`shouldTokenizeProperties` / `propertyCodecFor`) that returns the correct codec for a given record's format byte, so call sites stay format-agnostic.
- Namespace threading on the encode path: every `encodeNode` / `encodeEdge` call site is updated to take a namespace argument and a Badger txn (so dictionary writes commit atomically with the body).

### Out of scope

- Tokenizing property _values_. Values have unbounded cardinality and no Zipfian concentration worth exploiting at this layer. Handled separately if ever needed.
- Tokenizing Labels. Labels already get numID'd in secondary index keys via idDict. The label field on the body is small and infrequent relative to property keys.
- Tokenizing Edge Types. Same reasoning as labels.
- Freelist / recycling of property-key IDs. In real schemas property key names are effectively immortal — once "productName" exists, it exists forever. Recycling is a complexity cost with no realistic reward.
- A forced open-time rewrite of every body. Old bodies remain valid until they are next written.
- An offline rewrite / vacuum tool. Worth doing eventually for operators who want to reclaim the savings on cold data, but not part of this plan.

---

## 4. Design

### 4.1 Storage version and the decode seam

The on-disk storage version is bumped (e.g. `storageVersionPropKeyDictV1`). The version number signals: "this engine is capable of producing tokenized bodies and has the dictionary keyspace populated; do not assume every body is tokenized." It does **not** mean every body has been rewritten.

The decode seam is a small helper that all node/edge readers go through:

```go
// propertyCodecFor inspects the leading format byte of a body and returns the
// codec that should decode it. It is the single place that knows the mapping
// from format byte to codec, so call sites stay format-agnostic.
func propertyCodecFor(data []byte) propertyCodec

// shouldTokenizeProperties answers the encode-side question: should a *new*
// write go through the tokenized codec? Yes iff the engine's storage version
// is >= storageVersionPropKeyDictV1. Old engines never write tokenized bodies;
// new engines always do.
func (e *BadgerEngine) shouldTokenizeProperties() bool
```

`propertyCodecFor` is the mirror of `shouldTokenizeProperties`: encode-time decisions read the engine version, decode-time decisions read the per-record format byte. The two are intentionally decoupled so that an engine running at the new version still reads pre-existing untokenized bodies correctly.

### 4.2 Dictionary structure

A single engine-wide `propertyKeyDictionary` struct holds state for all namespaces. The outer map is keyed by namespace string; inner maps are the forward and reverse indexes for that namespace.

```go
type propertyKeyDictionary struct {
    mu      sync.RWMutex
    // namespace -> (name -> varint id)
    forward map[string]map[string]uint64
    // namespace -> (id -> name)
    reverse map[string]map[uint64]string
    // namespace -> monotonically increasing next ID
    nextID  map[string]*atomic.Uint64

    // Per-txn staged high-water mark, flushed at commit.
    // Same pattern as idDictionary's txnCounters.
    txnMu       sync.Mutex
    txnCounters map[*badger.Txn]map[string]uint64 // txn -> namespace -> max
}
```

Locks are split: the outer RWMutex guards structural additions (new namespace appearing, new ID in a namespace). Lookups on the fast path take RLock only. Allocation takes Lock.

### 4.3 Badger key layout

Two new key prefixes, both namespace-scoped:

```
prefixPropKeyForward  :: 1 byte
  + varint(len(namespace))
  + namespace bytes
  + varint(len(key_name))
  + key_name bytes
  -> varint(id)   (value)

prefixPropKeyReverse  :: 1 byte
  + varint(len(namespace))
  + namespace bytes
  + varint(id)
  -> key_name bytes   (value)

prefixPropKeyCounter  :: 1 byte
  + varint(len(namespace))
  + namespace bytes
  -> varint(next_id)   (value)
```

Keys use length-prefixed namespace to guarantee no ambiguity between e.g. namespaces `"ab"` and `"a"` + key `"b..."`.

These prefixes are new and unknown to older binaries. Badger carries unknown prefixes through untouched, so an older binary running against a data directory that has a property-key dictionary will simply not see those keys — which is fine, because an older binary also never produces tokenized bodies that would need to consult them.

### 4.4 Allocation API

Mirrors `idDictionary.resolveOrAllocateNodeNumIDInTxn`:

```go
// ResolveOrAllocateInTxn returns the varint id for (namespace, key), allocating
// a new id + persisting forward+reverse entries if the key has never been seen.
// The namespace counter is staged on the txn and flushed once at commit, not
// per allocation (reuses the flushTxnCounters pattern from idDictionary).
func (d *propertyKeyDictionary) ResolveOrAllocateInTxn(
    txn *badger.Txn, namespace, key string,
) (uint64, error)

// Lookup returns the key name for a given id. Read-only, never allocates.
// Returns ("", false) if the id is unknown.
func (d *propertyKeyDictionary) Lookup(namespace string, id uint64) (string, bool)
```

Concurrent-create race handling follows idDict: the losing goroutine's allocated ID becomes orphaned (safe — nothing references it yet). Since there's no recycle, orphaned IDs are simply skipped on next allocation.

Counter batching: `recordTxnCounterUse(txn, namespace, maxID)` stages per-txn state; `flushTxnCounters(txn)` writes one counter key per dirty namespace at commit. Called from `BadgerTransaction.Commit` alongside the existing idDict flush.

### 4.5 Open-time hydration

At engine open, after idDict hydration:

1. **Hydrate the dictionary** — single prefix scan of `prefixPropKeyForward`, populating `forward`, `reverse`, and seeding `nextID` from the persisted counter keys. On a fresh data directory the scan finds nothing and the dictionary starts empty.
2. **Record the engine's storage version** — if the on-disk version is below `storageVersionPropKeyDictV1`, the open-time path bumps it. The version bump is the only durable side effect; no bodies are rewritten.

There is no migration loop, no sentinel key, and no progress logging. The cost of opening an engine is unchanged for existing data directories beyond the dictionary hydration scan, which is bounded by the number of distinct property keys ever seen (typically <1000 for real schemas, scanning in well under a second).

### 4.6 Format version byte

New format bytes:

- Nodes: existing encoding has no dedicated format byte on the outer wrapper. Add `nodeFormatTokenizedV1 = 0x10` as the first byte of a tokenized node body, followed by the existing msgpack payload shape (but with uint64 property keys instead of strings). Untokenized bodies have no leading 0x10, so `propertyCodecFor` falls through to the legacy decoder.
- Edges: `edgeFormatCompactV2 = 0x03`, layered onto the existing compact-edge codec. Same framing, but the msgpack Properties map is `map[uint64]any` instead of `map[string]any`. Decoder dispatch already exists at `edge_compact.go:187` (switches on `data[0]`); we add the new arm.

The node decoder gains a similar leading-byte switch at its entry point. Both switches are encapsulated inside `propertyCodecFor` so callers don't repeat the dispatch logic.

### 4.7 Encoding API

Every `encodeNode` / `encodeEdge` call site is updated to thread a namespace argument and (where not already present) the Badger txn. Nine node call sites and three edge call sites are affected (inventory in Section 6).

```go
// Old
func encodeNode(n *Node) ([]byte, bool, error)

// New
func encodeNode(n *Node, namespace string, txn *badger.Txn) ([]byte, bool, error)
```

Inside `encodeNode`:

```go
if engine.shouldTokenizeProperties() {
    return encodeNodeTokenized(n, namespace, txn)
}
return encodeNodeLegacy(n)
```

The `txn` parameter is required on the tokenized path because property-key allocation happens inside the Badger transaction so that the forward/reverse/counter writes commit atomically with the body that references them. On the legacy path it is unused.

Decoder API stays string-keyed: callers continue to see `map[string]any` property maps. The reverse lookup happens inside `decodeNode` / `decodeEdgeCompact` using the in-memory reverse map — no Badger read on the decode hot path. The decoder decides which codec to use by calling `propertyCodecFor(data)`, so the same call site handles both formats transparently.

### 4.8 Lazy upgrade on write

Because the encode path always emits tokenized bodies once the storage version is bumped, any node or edge that is _modified_ after the upgrade is silently rewritten in the new format. Cold data that is never updated remains in the old format indefinitely; this is acceptable because the decoder handles both. Operators who want to reclaim savings on cold data can run an offline rewrite (out of scope) or simply tolerate a gradual decay of legacy bodies.

---

## 5. Non-goals and explicit exclusions

- **No cross-namespace key sharing.** The same key name in different namespaces gets different IDs. This is by design.
- **No ID recycling.** Once a property key is allocated it keeps its ID forever, even if every entity with that key is deleted. The cost is ~30 B per ever-used key in the on-disk forward/reverse entries.
- **No forced rewrite of pre-existing bodies.** They stay in legacy format until next write, or forever if never written. This is the explicit backward-compatibility property.

### Backward and forward compatibility

Badger itself is key-space agnostic — any prefix it doesn't recognize is simply carried forward untouched. The codec is designed to preserve that property in both directions:

- **Old → new (in-place upgrade).** An older binary's written bodies start with the existing format bytes (or no format byte). The new reader's `propertyCodecFor` dispatch handles these untokenized bodies via the legacy decode path. The dictionary keyspace starts empty and grows as new writes allocate IDs. No flag day, no migration window — old and new bodies coexist in the same keyspace indefinitely.
- **New → old (rolling downgrade).** Tokenized bodies start with `nodeFormatTokenizedV1 = 0x10` or `edgeFormatCompactV2 = 0x03`. An older binary that sees these format bytes will fail the decode loudly (it has no codec arm for them). The new prefixes (`prefixPropKeyForward`, `prefixPropKeyReverse`, `prefixPropKeyCounter`) are invisible to older binaries because they iterate on prefixes they know; unknown prefixes pass through Badger's keyspace untouched. The older binary therefore cannot mutate new tokenized bodies but also cannot corrupt them. A full downgrade from a partially-tokenized data directory requires either restoring from a pre-upgrade snapshot or running an offline rewrite back to the legacy format. Neither is part of this plan.
- **Storage-version semantics.** The version bump records "tokenized bodies _may_ exist," not "every body is tokenized." Code paths that need to know what a specific record looks like consult its format byte via `propertyCodecFor`, never the engine version. The engine version only governs the encode-side question of which codec to emit for new writes.
- **No schema flag day required.** Operators with replicas can upgrade one node at a time. A replica running the old binary continues to serve reads against its (untouched) untokenized bodies; a replica running the new binary serves both formats. New writes on the upgraded node land in tokenized form; the property-key prefixes are invisible to peers running the old binary. Replication of new writes between mixed-version replicas is governed by the Raft transport layer and Raft log framing, which is unaffected by body format — the wire protocol carries Cypher commands, not encoded bodies, so each replica materializes through its own local codec.

---

## 6. Implementation inventory

### New files

| File                                          | Purpose                                                                                   |
| --------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `pkg/storage/property_key_dictionary.go`      | Dict struct, ResolveOrAllocateInTxn, Lookup, flushTxnCounters, loadFromBadger             |
| `pkg/storage/property_key_dictionary_test.go` | Unit tests: alloc, resolve, hydrate, two-namespace isolation, commit-flushes-counter-once |
| `pkg/storage/property_codec.go`               | `propertyCodecFor(data)` dispatch helper + `shouldTokenizeProperties()` engine method     |
| `pkg/storage/property_codec_test.go`          | Format-byte dispatch tests; encode/decode round-trip across both codecs                   |

### Modified files (encoding sites)

| File:Line                           | Current                                             | New                                            |
| ----------------------------------- | --------------------------------------------------- | ---------------------------------------------- |
| `pkg/storage/badger_helpers.go:665` | `encodeNode(n *Node) ([]byte, bool, error)`         | `encodeNode(n, ns, txn)` — dispatches on engine version |
| `pkg/storage/badger_helpers.go:721` | `decodeNode(data)`                                  | `decodeNode(data, ns)` — dispatches via `propertyCodecFor` |
| `pkg/storage/badger_helpers.go:731` | `decodeNodeWithEmbeddings(txn, data, id)`           | signature unchanged, namespace derived from id |
| `pkg/storage/edge_compact.go:56`    | `encodeEdgeCompactWithNums(edge, startNum, endNum)` | add `ns, txn`; emit V2 if engine version >= V1 |
| `pkg/storage/edge_compact.go:103`   | `decodeEdgeCompact(data)`                           | `decodeEdgeCompact(data, ns)` — extends existing format-byte switch |
| `pkg/storage/edge_compact.go:159`   | `encodeEdgeInTxn(txn, edge)`                        | thread ns down from caller                     |

### Modified files (call sites)

Nine `encodeNode` call sites:

- `pkg/storage/badger_bulk.go:61`
- `pkg/storage/badger_deindex_enqueue.go:62`
- `pkg/storage/badger_deindex_enqueue.go:76`
- `pkg/storage/badger_nodes.go:67`, `:230`, `:308`, `:477`
- `pkg/storage/badger_transaction.go:356`
- `pkg/storage/badger_helpers.go:665` (definition)

Three edge encode paths:

- `pkg/storage/edge_compact.go:159` (definition)
- `pkg/storage/badger_bulk.go:268`
- `pkg/storage/badger_edges.go` (wrapper calls)

### Modified files (engine init)

- `pkg/storage/badger.go` — add `propKeyDict *propertyKeyDictionary` field on `BadgerEngine`; initialize alongside `idDict`; call `loadFromBadger` on open; bump on-disk storage version to `storageVersionPropKeyDictV1` if below.
- `pkg/storage/badger_transaction.go:Commit` — call `engine.propKeyDict.flushTxnCounters(tx.badgerTx)` in the same section as the existing idDict flush.

---

## 7. Test plan

### Dict-level tests

- `TestPropertyKeyDict_ResolveAndLookup` — allocate keys in namespace A, confirm forward+reverse round-trip.
- `TestPropertyKeyDict_NamespaceIsolation` — same key name in namespaces A and B gets different IDs.
- `TestPropertyKeyDict_HydrateFromBadger` — close engine, reopen, confirm dict state preserved.
- `TestPropertyKeyDict_CounterFlushesOncePerNamespace` — stage N allocations in one txn, verify exactly one counter Set per namespace at commit.
- `TestPropertyKeyDict_ConcurrentAllocate` — two goroutines racing to allocate the same key get the same ID back; no duplicate reverse entry persisted.

### Codec dispatch tests

- `TestPropertyCodecFor_DispatchesOnFormatByte` — body with leading `0x10` routes to tokenized; body without routes to legacy.
- `TestShouldTokenizeProperties_OffBeforeBump` — engine at pre-V1 storage version returns false; same engine after open-time bump returns true.

### Encoding round-trip tests

- `TestEncodeDecodeNode_TokenizedFormat` — encode a node with mixed property types (string, int, float, bool, array), decode, verify struct equality.
- `TestEncodeDecodeNode_LegacyFormat_StillReadable` — write a body via the legacy encoder, open a V1 engine, confirm decode succeeds and properties match.
- `TestEncodeDecodeEdge_TokenizedFormat` — same for edges.
- `TestEncodeDecodeEdge_LegacyFormat_StillReadable` — same for edges.
- `TestEncodedBodySize_ReductionFor100PropertyCorpus` — generate 100 nodes sharing 8 property keys; assert tokenized total encoded size is within 75–80% of the legacy format (sanity bound).

### Mixed-format integration tests

- `TestMixedBodies_SameStore` — seed a directory with legacy bodies, open a V1 engine, write new bodies, confirm both decode correctly and queries spanning both return correct results.
- `TestLazyUpgradeOnWrite` — read a legacy node, mutate one property, write it back, confirm the rewritten body is tokenized while sibling unmodified nodes remain legacy.

### Integration

- Full `pkg/storage` + `pkg/cypher` + `pkg/nornicdb` suites pass unchanged.
- Bench re-run on a freshly seeded northwind corpus: on-disk size for the data-files bucket drops 20–25%. (No reduction expected on a pre-existing corpus until those bodies are rewritten.)

---

## 8. Rollout

This is a backwards-compatible codec addition. The new decoder understands old bodies; the new encoder always emits the new format once the storage version is bumped. The rollout is therefore not a flag day:

1. Land the plan and all implementation commits on a single branch.
2. Tag a release-candidate binary.
3. Operators run the RC against a copy of their data directory first, confirm the engine opens cleanly (the storage-version bump is idempotent) and queries return correct results across pre-existing bodies.
4. Upgrade production nodes one at a time. Each node bumps its local storage version on first open under the new binary; opens take ~the same time as before plus a small dictionary hydration scan. No body rewrite happens at open.
5. In a Raft cluster, followers running the old binary continue serving their local pre-existing bodies. Newly written bodies on upgraded nodes are tokenized and consequently unreadable by an old-binary peer, but log shipping carries Cypher commands rather than encoded bodies, so each replica materializes through its own local codec. The mixed-version window is bounded by how long the operator chooses to run mixed binaries.
6. Downgrade path: an upgraded node has tokenized bodies for any record written post-upgrade. A pre-upgrade binary cannot decode those. To downgrade, either restore from a pre-upgrade snapshot or run an offline rewrite back to the legacy format. Neither is part of this plan; operators should treat the upgrade as forward-only in practice.

---

## 9. Risks

| Risk                                                                         | Mitigation                                                                                                                                                                       |
| ---------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Decode site bypasses `propertyCodecFor` and assumes legacy format            | Concentrate the leading-byte switch in one helper; remove direct `msgpack.Unmarshal` calls on body bytes from call sites and route them through `decodeNode` / `decodeEdgeCompact`. |
| Dict hydration at open time is slow for large key corpora                    | Single prefix scan; at 500k distinct keys × 50 B per entry = 25 MB total, scans in under a second on any modern SSD.                                                             |
| Namespace argument threading misses a call site                              | Removing the old `encodeNode(n)` signature entirely is the forcing function — compilation will fail on every missed site.                                                        |
| Mixed-format store leaks legacy bodies forever on cold data                  | Acceptable by design. An offline rewrite is a follow-up if savings on cold data become important.                                                                                |
| Old-binary peer encounters a tokenized body during a rolling upgrade         | Old binary fails the decode loudly (no silent corruption). Operators are expected to complete the rolling upgrade promptly; mixed-version reads of post-upgrade writes are not supported. |

---

## 10. Expected outcomes

- **Data-file size reduction:** 20–25% on _newly written_ bodies, translating to ~18% reduction on the total data-files bucket once a workload has churned through its hot set. Cold data retains its legacy size until rewritten.
- **Write path:** marginally faster. One dict lookup + varint emit per property, vs. one string write per property. Net is a wash or slight win because varint emission is cheaper than msgpack string-with-length emission.
- **Read path:** marginally faster on tokenized bodies (in-memory `map[uint64]string` reverse lookup is hotter than string-interned msgpack headers); unchanged on legacy bodies.
- **Operational:** zero-cost upgrade — no migration window, no progress monitoring, no resumption logic. The dictionary files grow O(distinct keys ever used) per namespace, which is tiny compared to the data they compress.
