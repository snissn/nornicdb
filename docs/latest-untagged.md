# Latest (Untagged) Changelog

Changes that have landed on `main` since the last tagged release. Promoted into a versioned section of `CHANGELOG.md` when the next tag cuts.

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

`docs/plans/consumer-pinned-error-contract-plan.md` — single-pass plan documenting the wire-contract pinning above. Default values reconciled against `LoadDefaults()` truth (the original draft had `AsyncWritesEnabled` and `PersistSearchIndexes` defaults inverted; both are now correct).
