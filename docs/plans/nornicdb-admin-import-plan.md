# `nornicdb-admin database import` Plan

**Status:** Proposed (single-pass)

## Scope

Ship a `nornicdb-admin` CLI binary whose `database import` subcommand mirrors `neo4j-admin database import` as closely as the license and trademark allow. Same file formats, same flag names where they don't collide with the Neo4j brand, same operational model: offline import into a stopped or empty target database, single-pass index population at the end.

This plan covers:

1. The CLI surface (`cmd/nornicdb-admin/`).
2. The Neo4j-compatible CSV header dialect for nodes and relationships.
3. The internal storage write path that bypasses incremental index maintenance.
4. The post-import index/constraint build phase.
5. Validation, recovery, and exit codes.

The use case driving this plan: shipping pre-built knowledge packs (4,000–50,000+ nodes with 384-dim embeddings) that today take ~13.5 hours per pack through the online Bolt path because the HNSW index is updated incrementally per batch. A bulk import that writes raw store entries and builds the index once at the end runs the same data in minutes.

For background and the alternatives we rejected, see the bottom of this plan.

## Non-goals

- Bulk *online* import. The existing online path (Bolt + `apoc.periodic.iterate`-style batched MERGE) stays as-is. This plan is strictly the offline, fast path.
- A graphical or web import wizard.
- Streaming import from a running source database. The existing `scripts/migration/neo4j/migrate.go` Bolt-to-Bolt migrator continues to handle that case.
- Cypher-driven import. `LOAD CSV` is a separate online surface and not part of this plan.

## Phase 1 — `nornicdb-admin` binary skeleton

1. Add `cmd/nornicdb-admin/` alongside the existing `cmd/nornicdb/`. Same Cobra style as the main binary so flag conventions match.

2. Subcommand layout (mirroring `neo4j-admin`'s shape):

   ```
   nornicdb-admin
   nornicdb-admin database
   nornicdb-admin database import full     <db-name> [flags]
   nornicdb-admin database import incremental <db-name> [flags]
   nornicdb-admin database info            <db-name>
   nornicdb-admin server status
   ```

   `database import full` and `database import incremental` map 1:1 onto Neo4j's `full` and `incremental` modes. The other subcommands are stubs we add as needed; only `import` is in scope for this plan.

3. Add a `make build-admin` target that produces `bin/nornicdb-admin{,.exe}` for every platform we already cross-compile for (`make cross-all` + admin). The binary embeds the same storage engine as `nornicdb` so it can open the data directory directly.

4. Acceptance: `nornicdb-admin --help` and `nornicdb-admin database import --help` print structured help that lists every flag.

## Phase 2 — File format compatibility

The import format is the documented Neo4j CSV format with one minor relabel: the trademark "Neo4j" is replaced with "NornicDB" or "nornic" wherever it appears in flag names or examples. The CSV header dialect itself is unchanged so files produced for `neo4j-admin import` work with `nornicdb-admin database import` byte-for-byte.

### Node CSV header dialect

```
:ID(GroupName),name:string,age:int,:LABEL
n01,Alice,34,Person
n02,Acme,,Company
```

- `:ID(<group>)` — primary key column; the `(<group>)` is optional and lets you have multiple ID namespaces in the same import.
- `:LABEL` — pipe-separated list of labels (`Person|User`).
- `:IGNORE` — column to skip.
- Property columns: `name`, `name:string`, `age:int`, `score:float`, `created:datetime`, `tags:string[]`. Same primitive types, list types, and array delimiters as Neo4j: `:int`, `:long`, `:float`, `:double`, `:boolean`, `:byte`, `:short`, `:string`, `:char`, `:date`, `:datetime`, `:localdatetime`, `:time`, `:localtime`, `:duration`, `:point`, plus `[]` array suffix.
- Quote, escape, and array-delimiter rules: identical to Neo4j defaults.

Two NornicDB extensions for our content-pack use case:

- `:EMBEDDING(<index-name>)` — float-array column that bypasses the property store and writes directly into the named vector index. Vector dimension is inferred from the first row and validated thereafter.
- `:NAMED_EMBEDDING(<key>)` — float-array column written into `Node.NamedEmbeddings[<key>]` (the path the Qdrant gRPC layer already uses).

Both extensions are explicit and unambiguous; standard CSV files without them work exactly as `neo4j-admin import` would interpret them.

### Relationship CSV header dialect

```
:START_ID,:END_ID,:TYPE,since:date
n01,n02,WORKS_AT,2018-06-01
```

- `:START_ID(<group>)`, `:END_ID(<group>)`, `:TYPE` — required.
- Same property column rules as nodes.

### Multi-file imports

The same `--nodes` / `--relationships` flag form Neo4j uses:

```
nornicdb-admin database import full mydb \
  --nodes=Person=people.csv \
  --nodes=Company=companies.csv \
  --relationships=WORKS_AT=works_at.csv \
  --relationships=KNOWS=knows.csv
```

Multi-CSV-per-target is supported through repetition of the flag, identical to Neo4j.

### Flag set (Phase 2 scope)

```
nornicdb-admin database import full <db-name> [flags]

Required:
  --nodes=[<labels>=]<file.csv>[,<file.csv>...]   Node CSV files (repeatable)
  --relationships=[<type>=]<file.csv>[...]         Relationship CSV files (repeatable)

Common:
  --data-dir=<path>                                Target data directory (default: ./data)
  --delimiter=<char>                               Field delimiter (default: ,)
  --array-delimiter=<char>                         Array element delimiter (default: ;)
  --quote=<char>                                   Quote character (default: ")
  --id-type=string|integer                         ID column type (default: string)
  --skip-bad-relationships=true|false              Skip rels referencing missing nodes (default: false)
  --skip-duplicate-nodes=true|false                Skip duplicate :ID rows (default: false)
  --report-file=<path>                             Write a JSON report after import
  --verbose                                        Verbose logging

Index/constraint control:
  --build-indexes=true|false                       Build schema indexes after import (default: true)
  --constraints-file=<path>                        Cypher file with CREATE CONSTRAINT/INDEX statements run AFTER data load
```

This is the **shape** Neo4j uses, with one rename: `--input-encoding` and `--ignore-empty-strings` are not in Phase 2 (they can come in Phase 5 if a user asks).

## Phase 3 — Internal write path

This is where the speedup comes from. The reason `CREATE` over Bolt with a vector index is slow is that each commit replays through the executor and triggers an incremental HNSW insertion. The reason a one-shot `CREATE VECTOR INDEX` over the same data is ~100× faster is that it walks the storage engine in a tight loop and pushes the whole batch through the HNSW builder in one pass.

The import path in `nornicdb-admin database import` writes through the same lower-level surface as the post-hoc index build:

1. **Open the target data directory directly** through `pkg/nornicdb` library mode. No Bolt, no HTTP, no transaction layer round-trips.

2. **Refuse to start if the database is online.** Phase 1 doesn't try to coordinate with a running server. If the target data dir has a lock file held by `nornicdb serve`, the import refuses with a clear error — same as `neo4j-admin import` requires the database to be stopped.

3. **Disable WAL fsync per row** for the duration of the import. Wrap the import in a single deferred-fsync window: every row is written to the WAL but `fsync` is called once at the end of the import, after all data is durable in the LSM tree. If the import process crashes, the partially-written WAL is replayed on next startup and either completes or rolls back atomically — same crash-safety guarantee as a normal write, at a fraction of the per-row cost.

4. **Stream nodes through `BadgerEngine.PutNodeBatch`** (already exists in `pkg/storage`). For each node:
   - Resolve labels and properties.
   - Write the node entry directly via the namespaced engine.
   - **If a column is `:EMBEDDING(<idx>)`**: append `(node_id, vector)` to a per-index buffer. Do NOT update the HNSW index yet.
   - **If a column is `:NAMED_EMBEDDING(<key>)`**: write directly to `Node.NamedEmbeddings[<key>]` like the Qdrant ingestion path does.

5. **Stream relationships** the same way: resolve `:START_ID`/`:END_ID` against the in-memory ID-to-internal-ID map built during the node phase, then write edge entries directly.

6. **Skip incremental index maintenance.** The storage engine's per-write index callbacks (`OnNodeCreated` → `searchService.IndexNode(node)`) are bypassed for the duration of the import. The engine writes raw entries; index population is a separate Phase 4 step.

7. **Single-pass HNSW build at the end.** For each `:EMBEDDING(<idx>)` column encountered, after all nodes are written:
   - Look up the existing vector index config (or create one with dimensions inferred from the column).
   - Call the same `pkg/search` one-shot population path that `CREATE VECTOR INDEX` uses today (your benchmark shows ~20s for 32k vectors).

8. **Build BM25 indexes** with the same single-pass approach against the now-populated node store.

9. **Apply constraints from `--constraints-file`.** Run the Cypher file with the storage engine open in normal mode. Any constraint violation aborts the import and records the offending row in `--report-file`.

10. **Single fsync, then close.**

The net write amplification matches Neo4j's `database import full` model: one WAL write per row, one schema-index write at the end, zero incremental HNSW rebuilds.

### Library reuse

We don't write a new storage engine for this. Every primitive we need is already in `pkg/storage` and `pkg/search`:

- `BadgerEngine.CreateNode` / `CreateEdge` (or their batch siblings) — direct write path.
- `pkg/search/Service.IndexNode` is what gets bypassed; the corresponding one-shot `BuildIndex(nodes []*Node)` path is what `CREATE VECTOR INDEX` already uses.
- `pkg/cypher` constraint parsing for the `--constraints-file` step.

What's new is the CSV reader, the column-spec parser, and the orchestration. The hot path is unchanged.

## Phase 4 — Index and constraint build phase

After all rows are written, the import runs:

1. For each `:EMBEDDING(<name>)` column seen during ingest:
   - Create the vector index if it didn't exist (dimensions from the first row, similarity function from a `--vector-similarity` flag, default `cosine`).
   - Run the one-shot HNSW build over the buffered `(node_id, vector)` pairs.

2. For each `:NAMED_EMBEDDING(<key>)` column:
   - Add to `search.Service`'s named-embedding index. Single-pass build.

3. Apply the constraints file (`--constraints-file=<path>`):
   - Parse and execute every `CREATE CONSTRAINT` / `CREATE INDEX` statement.
   - Constraints validate against the now-loaded data; violations fail the import.

4. Build the BM25 fulltext index for any newly-created fulltext index entries.

5. Write a JSON report to `--report-file` summarising rows imported, indexes built, durations.

The output of Phase 4 is a data directory that's byte-equivalent to one produced by online ingestion of the same content, modulo timestamp metadata.

## Phase 5 — Validation, recovery, exit codes

- **Pre-flight checks:**
  - Refuse to run if the target data dir is locked by `nornicdb serve`.
  - Refuse `database import full` if the target database has any user data (use `incremental` for that case, gated by an explicit flag).
  - Validate that `--nodes` / `--relationships` files exist and parse the headers before opening the engine.

- **Failure modes:**
  - CSV parse error: report row, file, column index; exit code 2.
  - Bad relationship reference (no source/target node found): record in report; if `--skip-bad-relationships=false` (the default), exit code 3; if `true`, count and continue.
  - Duplicate `:ID`: same pattern, exit code 4.
  - Constraint violation during the post-load step: report row, constraint, conflicting node; exit code 5.
  - Crash mid-import: the WAL replay on next `nornicdb serve` start either completes the partial transactions or rolls them back. The user re-runs `nornicdb-admin database import full` against the same empty database to retry.

- **Exit code 0:** every row imported, every index built, every constraint satisfied. Report file written.

## Phase 6 — Cypher procedure helpers (post-import)

Two small additions to the running server, scoped to make the bulk-load UX cleaner for *online* loads that still want the deferred-population behavior:

1. **`CALL db.awaitIndex(indexName, timeoutSeconds)`** — block until the named index reports `ONLINE`. Same shape as Neo4j's `db.awaitIndex`. Useful for the existing online drop-and-recreate workaround so loaders can wait for the rebuild without polling.

2. **`SHOW INDEXES` already exposes population state.** Confirm it reports `populationProgress` and lifecycle state (`POPULATING` / `ONLINE` / `FAILED`) consistent with Neo4j's surface so client-side fallback logic can detect a populating index.

These two helpers are independent of the offline `nornicdb-admin database import` work; they're useful for online loaders that prefer to keep using Bolt.

## Phase 7 — Documentation

1. **`docs/operations/admin-tool.md`** — operator-facing user guide for `nornicdb-admin`, with the import workflow, file formats, and recovery procedure.

2. **`docs/operations/cli-commands.md`** — add the `nornicdb-admin database import` table and a pointer to the user guide.

3. **`docs/user-guides/data-import-export.md`** — add an "Offline bulk import" section linking to the admin tool, alongside the existing online `LOAD CSV` and Bolt-driven import patterns.

4. **Format reference** in `docs/operations/admin-tool.md` covering:
   - `:ID`, `:START_ID`, `:END_ID`, `:LABEL`, `:TYPE`, `:IGNORE`
   - Property type suffixes (`:int`, `:string`, `:datetime`, `:string[]`, etc.)
   - The two NornicDB extensions: `:EMBEDDING(<idx>)`, `:NAMED_EMBEDDING(<key>)`

5. **Migration notes** for users coming from `neo4j-admin database import`: note the trademark-related rename of the binary (`nornicdb-admin`) and that CSV header syntax is unchanged.

## Phase 8 — Cross-cutting

- **Packaging:** the `nornicdb-admin` binary ships in the same release artifacts as `nornicdb`. Update the [native-package-distribution-plan](native-package-distribution-plan.md) Phase 1 to bundle both binaries.
- **Codesigning:** macOS Developer ID and Windows Authenticode cover both binaries.
- **Help links:** `nornicdb-admin database import --help` references `docs/operations/admin-tool.md`.

## Background: why this plan

### The benchmark that prompted the request

| Operation | Time | Notes |
|---|---|---|
| Insert 250 nodes (no vector index) | ~1s | Raw write speed is fine. |
| Insert 250 nodes (with vector index) | ~45 min | Per-batch HNSW rebuild dominates. |
| One-shot `CREATE VECTOR INDEX` over 32k nodes | ~20s | Single-pass build is fast. |

A 4,500-node content pack via 18 batches over Bolt: ~13.5 hours. The same 4,500 nodes loaded via a Neo4j-style admin import: minutes. The asymmetry exists because the HNSW builder already has a fast batch-build path; Bolt-driven ingestion just doesn't expose it.

### Why we picked the admin-tool path over the alternatives

- **`SET INDEX POPULATION OFF` session mode** would require designing visibility/correctness semantics from scratch (Neo4j has no analog) and would muddle the executor's invariants. Rejected.
- **`CREATE VECTOR INDEX OPTIONS {build: 'deferred'}`** is implicit in Neo4j (every `CREATE INDEX` is async-populated), and we get the same effect today by dropping and recreating. The piece worth adding is the `db.awaitIndex` helper (Phase 6 here).
- **`nornicdb-admin database import`** matches what Neo4j users already know. The format, the lifecycle, and the operational model carry over. It also gives us the "click install, wait a minute" UX without introducing a new session mode that has no precedent.

### What we explicitly are not changing

- Online ingest performance through Bolt is unchanged. Loaders that need online ingest continue to use the drop-and-recreate workaround documented in [Data Import/Export](../user-guides/data-import-export.md).
- The `MERGE`/`SET` UNIQUE-violation issue described in the user's report is filed as a separate bug; this plan does not address it.

## Acceptance Criteria

- [ ] `nornicdb-admin database import full <db>` accepts every flag and CSV header form documented in this plan.
- [ ] A 4,500-node, 384-dim-vector pack imports in <2 minutes on the same hardware where it currently takes 13.5 hours.
- [ ] A 50,000-node pack imports in <10 minutes.
- [ ] Output data directory is byte-equivalent (modulo timestamps) to one produced by the online path.
- [ ] Pre-built Neo4j-format CSV files (no NornicDB extensions) import without modification.
- [ ] CSV files using `:EMBEDDING(<idx>)` produce a populated vector index without a separate `CREATE VECTOR INDEX` step.
- [ ] All exit codes documented in Phase 5 are reachable in tests.
- [ ] `make build-admin` produces a working `bin/nornicdb-admin` for darwin/linux/windows.

## Related Documentation

- [Native Package Distribution Plan](native-package-distribution-plan.md) — admin binary ships alongside `nornicdb`.
- [Data Import/Export](../user-guides/data-import-export.md) — current online import patterns.
- [Vector Search](../user-guides/vector-search.md) — the index that benefits from one-shot construction.
- [Hot-Path Cypher Cookbook](../performance/hot-path-query-cookbook.md) — what the online ingest path looks like today.
- [Existing Neo4j → NornicDB migrator](https://github.com/orneryd/nornicdb/blob/main/scripts/migration/neo4j/migrate.go) — Bolt-to-Bolt complement to this CLI.
