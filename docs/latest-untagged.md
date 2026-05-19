# Latest (Untagged) Changelog

Changes that have landed on `main` since the last tagged release. Promoted into a versioned section of `CHANGELOG.md` when the next tag cuts.

## MVCC counter sharded per database; transactions pinned to one namespace

The MVCC commit-sequence counter and high-water timestamp clamp moved off `BadgerEngine` (engine-global) onto a per-namespace `namespaceMVCCState` map keyed by database name. Each transaction is pinned to a single namespace at the first prefixed write (or eagerly via `BadgerTransaction.SetNamespace`); cross-namespace writes return the new sentinel `ErrCrossNamespaceTransaction`. Snapshot-isolation conflict detection compares `CommitSequence` values that now share a namespace by construction, so noisy tenants can no longer interleave version numbers with quiet ones, and a `DROP DATABASE` drops its counter alongside its keys.

Behind the storage layer, the lifecycle prune planner consults a **per-namespace safe-floor callback** instead of a single global `oldestReader`: cross-namespace counters are not comparable, so a head in namespace A must be evaluated against namespace A's oldest reader, not the global minimum across every tenant. `ReaderRegistry.OldestReaderVersionsByNamespace()` groups active readers by `info.Namespace` and returns the per-group minimum; namespaces with no active reader fall through to the sentinel "no floor" so TTL and `MaxVersionsPerKey` alone bound their pruning.

### Backward compatibility — no breaking storage changes

The on-disk MVCC head/version payloads are unchanged. The counter's persistence key moved from the legacy engine-global `[prefixMVCCMeta, 0x01]` to per-namespace `[prefixMVCCMeta, 0x04, namespace bytes…]` (new subkey `prefixMVCCMetaNamespaceSeq`). On engine open, the legacy global key is read once into `mvccLegacyGlobalSeed`; the first time any namespace is touched, its starting sequence is set to `max(persisted-per-ns, legacyGlobalSeed)` so versions previously emitted under the global counter remain strictly less than anything new namespaces will mint. Existing databases come up unchanged.

### Cross-namespace transactions — invariant enforced, dead branches deleted

Production never opened a transaction whose pending writes spanned multiple namespaces; the cypher executor's `transactionStorageWrapper` (executor.go:1932) and the explicit-tx path (transaction.go:313) both pin a single namespace at construction. The defensive multi-namespace branches in `acquireUniqueConstraintCommitLocks` (badger_transaction.go:2418), `validateNodeConstraints`, `checkUniqueConstraint`, `checkNodeKeyConstraint`, `checkTemporalConstraint`, `validateEdgeConstraints`, and `validatePolicyOnNodeLabelChange` were unreachable — and unreachable code that *looks* live is worse than absent code, because callers grow false confidence in capabilities the transaction layer cannot honor. All of those paths now look up the schema once from `tx.namespace` instead of re-parsing the prefix off each entity ID.

### Read-path lazy pin — closes a lost-update regression caught by `TestDB_UpdateConcurrentIncrements`

The transaction's `readTS` is rebound from a wall-clock-only sample to its namespace's actual `(timestamp, sequence)` pair the moment the namespace is pinned. Pre-pin reads (e.g. the `GetNode` at the head of a `db.Update` retry loop) now also lazy-pin from the entity's ID prefix; without this, a transaction that reads before any write would carry `readTS.CommitSequence == 0`, fail the `version.Compare(head.FloorVersion) < 0` visibility gate against any committed head, and the `Read → Modify → Write → Conflict-Retry` loop in `DB.Update` would silently lose increments under contention.

### Pinned with deterministic regression tests

- `pkg/storage/badger_transaction_namespace_pin_test.go` — 12 tests asserting (a) first prefixed write pins the namespace, (b) same-namespace writes succeed, (c) `CreateNode`/`UpdateNode`/`DeleteNode` reject cross-namespace writes with `ErrCrossNamespaceTransaction`, (d) `CreateEdge` and `BulkCreateEdges` reject mismatched endpoints/IDs, (e) `SetNamespace` is idempotent within a namespace and rejects empty/conflicting input, (f) a rejected write does not alter the pinned namespace.
- `pkg/storage/badger_mvcc_per_namespace_test.go` — 6 tests asserting (a) two namespaces have independent sequences, (b) a high-throughput namespace cannot accelerate another's counter under concurrent allocation, (c) the high-water clamp is per-namespace, (d) a transaction in one namespace cannot observe entities in another, (e) sequence saturation falls back to timestamp-only ordering per-namespace without rewriting the persisted key, (f) the legacy global counter seeds new namespaces on upgrade so first-allocation = `legacySeed + 1`.
- `pkg/storage/lifecycle/per_namespace_floor_test.go` — 4 tests asserting (a) `OldestReaderVersionsByNamespace` filters readers by `Namespace` and returns per-namespace minimums, (b) deregistration drops the namespace's floor entry, (c) the planner spares a head whose namespace has an active reader pinning it but prunes a head in a namespace with no reader, (d) a nil floor callback treats every namespace as having no reader.
- Existing tests `TestCurrentMVCCReadVersion_ClampsToHighWater`, `TestAllocateMVCCVersion_AdvancesHighWater`, `TestBeginTransaction_DoesNotConflictAfterClockSkew`, and `TestAllocateMVCCVersion_FallsBackToTimestampOrderingWhenSequenceExhausted` rewritten against the per-namespace state.

### Cleanup of orphan test fixtures

`TestImplicitTx_UnwindBulkCreate_ArchitecturePayload_PersistsAcrossRestart` (server) was loading a renamed-and-deleted doc; pointed at the surviving `docs/architecture/cognitive-slm-architecture.md`. Three test fixtures (`pkg/cypher/cartesian_where_pushdown_test.go`, `pkg/cypher/translation_query_family_test_helpers_test.go`, `pkg/cypher/traversal_start_seed_topk_test.go`) were creating prefixed nodes but unprefixed edge IDs against raw `MemoryEngine`s — a pattern that worked before the per-tx namespace pin and now (correctly) fails. Updated to either wrap in `NamespacedEngine` or carry the prefix on edge IDs uniformly. `pkg/inference/edge_decay_test.go` and `pkg/storage/badger_transaction_reads_test.go` had similar mixed-namespace fixtures; split into per-namespace transactions and uniformly prefixed IDs.

## Bolt wire contract — single commit-failure wrapper across both commit paths

The implicit-autocommit path at `pkg/cypher/executor.go:1978` historically wrapped commit failures with `"failed to commit implicit transaction: ..."`, while the explicit `BEGIN/COMMIT` path at `pkg/cypher/transaction.go:181` used `"commit failed: ..."`. Downstream Bolt consumers classify retryable transients on the substring; the two-shape wire surface meant a classifier that retried one path silently surfaced the other as a permanent failure.

Aligned both paths to the `"commit failed: ..."` shape. Updated three downstream test sites (`pkg/bolt/server_test.go`, `pkg/server/server_db_error_mapping_test.go`, `testing/e2e/vector_traversal_shapes_bench_test.go`) plus a doc comment in `pkg/storage/badger_mvcc_clock_skew_test.go`.

### Pinned with regression tests

Added five new contract tests so the wire shape can't drift again:

- `pkg/storage/constraint_validation_namespaced_test.go` — `RefreshUniqueConstraintValuesForEngine` keeps storage-prefixed IDs under `NamespacedEngine`. Pins the fix at `pkg/storage/constraint_validation.go:71` against a known false-UNIQUE failure mode.
- `pkg/storage/conflict_message_contract_test.go` — five canonical optimistic-conflict messages (node, node-with-version-detail, edge, edge-alternate, adjacent-edge) plus `errors.Is(err, ErrConflict)` chain preservation.
- `pkg/cypher/commit_failure_message_contract_test.go` — node and relationship variants of the `commit failed: constraint violation: … already exists` shape.
- `pkg/cypher/merge_idempotency_under_concurrent_test.go` — two writers MERGE on the same uid; the loser sees the pinned wire shape and a retry succeeds with the loser's `SET` visible.
- `pkg/config/defaults_consumer_contract_test.go` — `LoadDefaults()` returns the documented values for `Database.AsyncWritesEnabled=true`, `Database.PersistSearchIndexes=false`, `Auth.Enabled=false`, `Server.BoltPort=7687`, `Server.HTTPPort=7474`.

Each pinned source-of-truth line gained a one-line `// Wire contract:` comment pointing back to the plan.

## Documentation — agent-ready skills

New [`docs/skills/`](skills/README.md) directory containing self-contained, agent-ready skill files for every consumer-facing surface:

- [`cypher-queries.skill.md`](skills/cypher-queries.skill.md) — hot-path Cypher cookbook (point lookup, batch retrieval, pagination, search, traversal, batched UNWIND/MERGE writes, cleanup, multi-tenant isolation).
- [`bolt-client.skill.md`](skills/bolt-client.skill.md) — Bolt connection defaults, retry classification, MERGE under concurrent writers, batch sizing.
- [`grpc.skill.md`](skills/grpc.skill.md) — Qdrant-compatible gRPC + `NornicSearch.SearchText`. RPC catalog, collection→database mapping, point→node mapping, limits, auth.
- [`qdrant-migration.skill.md`](skills/qdrant-migration.skill.md) — Qdrant → NornicDB end-to-end through the gRPC compat layer.
- [`neo4j-migration.skill.md`](skills/neo4j-migration.skill.md) — Neo4j → NornicDB over Bolt, with phase order and exact UNWIND/MERGE shapes pinned to the cookbook.
- [`knowledge-policies.skill.md`](skills/knowledge-policies.skill.md), [`decay-tuning.skill.md`](skills/decay-tuning.skill.md), [`promotion-policies.skill.md`](skills/promotion-policies.skill.md) — full DDL surface for decay/promotion, with disambiguated bundle-vs-binding language.
- [`managed-embeddings.skill.md`](skills/managed-embeddings.skill.md), [`vector-search.skill.md`](skills/vector-search.skill.md), [`rag-procedures.skill.md`](skills/rag-procedures.skill.md) — search and RAG surfaces.

Knowledge-policy user guides (`docs/user-guides/knowledge-layer-policies.md`, `decay-profiles.md`, `promotion-policies.md`) rewritten in the same precise vocabulary: bundles are inert parameter packages, bindings carry the `FOR` target, decay is pure time math evaluated on read, `ON ACCESS` belongs to promotion only, `LAST_ACCESSED` decay only reads access metadata.

## Migration scripts — Neo4j and Qdrant

New `scripts/migration/` directory with runnable migrations in three languages each:

- `scripts/migration/neo4j/{migrate.py,migrate.go,migrate.mjs}` — Bolt → Bolt, schema first (constraints + indexes including fulltext and vector), then nodes via `UnwindSimpleMergeBatch`, then edges via `UnwindMultiMatchCreateBatch`. Keyset-paginated by `elementId`. Preserves source IDs on `_neo4j_id` and original labels on `_neo4j_labels` for an operator-driven label promotion pass.
- `scripts/migration/qdrant/{migrate.py,migrate.go,migrate.mjs}` — gRPC → gRPC (Node uses REST since there's no maintained JS gRPC client). Replicates collection vector configs, scrolls and upserts points in batches, verifies counts. Idempotent on re-run.

All Go scripts vet and build clean against `github.com/qdrant/go-client v1.18.1` and `github.com/neo4j/neo4j-go-driver/v5 v5.28.4`.

## Documentation site — consolidated navigation, fixed search routing

Rewrote `mkdocs.yml`:

- Top-level navigation collapsed from 14 groups to 8: Home, Getting Started, Concepts & Guides, Skills (Agent-Ready), Migrating, API Reference, Operating, Building, Changelog. Sub-grouped within each (e.g. Concepts & Guides → Cypher / Search & RAG / Knowledge Layer / Temporal & Ledger / Topology & Multi-DB / Heimdall / Plugins).
- **Every published `.md` file is registered in `nav:`.** This is the fix for the bug where clicking a search result loaded the page content but left the sidebar empty — Material renders the page on direct hit, but a page outside `nav:` has no anchor for the sidebar to highlight.
- `use_directory_urls: true` made explicit; `site_url` produces a `<link rel="canonical">` on every built page.
- Theme features tightened: `navigation.instant.progress`, `navigation.tracking`, `navigation.path`, `navigation.indexes`, `navigation.prune`, `search.share`, `toc.integrate`. The combination keeps URL ↔ sidebar ↔ content synchronized when navigating from a search result.
- New top-level entries link the Skills directory and the Migrating section (Neo4j + Qdrant) so they're discoverable from the global nav, not just deep-linked from the README.

Root README cross-links the cookbook, the skills directory, and the migration scripts in the relevant feature and "why switch from" sections.

## Plan landed

The single-pass wire-contract pinning above was tracked through completion. Default values were reconciled against `LoadDefaults()` truth (the original draft had `AsyncWritesEnabled` and `PersistSearchIndexes` defaults inverted; both are now correct). Regression tests live in `pkg/cypher/commit_failure_message_contract_test.go`, `pkg/storage/conflict_message_contract_test.go`, and `pkg/config/defaults_consumer_contract_test.go`.
