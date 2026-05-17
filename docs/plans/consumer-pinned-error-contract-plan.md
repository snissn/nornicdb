# Consumer-Pinned Error Contract — Regression Tests & Remediation Plan

**Status:** Proposed (single-pass)
**Date:** 2026-05-16
**Scope:** Lock down NornicDB behaviors that downstream Bolt consumers pin as a typed contract, so refactors can't silently turn safe retries into projection failures or re-introduce known-bad shapes. Owner: NornicDB storage + cypher + bolt + config teams.

---

## 1. Why this plan exists

A real downstream consumer (a code-graph indexer driving NornicDB over Bolt with `neo4j-go-driver/v5`) treats a small set of NornicDB error strings, defaults, and concurrency invariants as a **typed contract**. The classifier that decides "retry" vs "projection failure" matches on substrings inside `error.Error()`. That makes any of the following changes silently contract-breaking:

- Reformatting `commit failed: constraint violation:...` to `commit failure: unique violation:...`.
- Wrapping the `conflict: <id> changed after transaction start` shape in a new outer error.
- Returning a different code class (`Neo.TransientError.*` instead of `Neo.ClientError.Transaction.TransactionCommitFailed`) for the same race.
- Changing the namespaced-engine prefix handling on the `RefreshUniqueConstraintValuesForEngine` rebuild path.
- Flipping the parsed `Database.AsyncWritesEnabled`, `Database.PersistSearchIndexes`, `Auth.Enabled`, `Server.BoltPort`, or `Server.HTTPPort` defaults.

Some of these are already pinned by accident (existing call sites and existing tests). None of them are pinned **on purpose**, and that is the gap this plan closes. The remediation work is mostly **regression tests** plus narrow comments at the source-of-truth lines that name what the contract is and what would break a consumer.

This plan does not change behavior. Each step is a guard around behavior that already exists. Where current code already handles a previously-buggy shape (e.g. `EnsureNodeIDDatabasePrefixForEngine` in `pkg/storage/constraint_validation.go:71`), the test pins that fix so a later refactor cannot quietly regress it.

---

## 2. The compensations consumers carry today

The following are real workarounds consumers ship because of NornicDB behaviors that historically misfired. Every one of them is a candidate to **either retire on the consumer side** (if NornicDB now guarantees the behavior) **or harden in NornicDB** (so the consumer can stop carrying the workaround).

### 2.1 Bounded retry on commit-time UNIQUE for MERGE-only groups

| Aspect       | Detail                                                                 |
|--------------|------------------------------------------------------------------------|
| Behavior     | `MERGE (n {uid: ...}) SET ...` under concurrent writers can produce commit-time UNIQUE on `v1.0.45+` because two transactions both index-probe the uid as absent and both attempt CREATE; the loser fails commit. |
| Consumer comp| Retry the whole group with backoff — but only if **every statement in the group is MERGE-shaped**, otherwise re-running CREATE/DELETE/SET-only could double-apply effects. |
| Source-of-truth in NornicDB | Error string assembled in `pkg/cypher/transaction.go:181` (`commit failed: %w`) and `pkg/storage/badger_transaction.go:1404` (`constraint violation: %w`). |
| Status       | Working as designed in NornicDB (the storage check is correct), but the wire format is a contract. A reformat would break the consumer's classifier. |
| Remediation  | Pin the post-`v1.0.45` `commit failed: ...` / `constraint violation: ...` / `Neo.ClientError.Transaction.TransactionCommitFailed` shape with a regression test. |

### 2.2 Bounded retry on optimistic write conflict

| Aspect       | Detail                                                                 |
|--------------|------------------------------------------------------------------------|
| Behavior     | Concurrent edits on the same node or edge surface as `<wrapper>: conflict: node <id> changed after transaction start` / `... edge <id> changed after transaction start`. |
| Consumer comp| Classify any error matching `conflict:` AND `changed after transaction start` as transient and retry. |
| Source-of-truth in NornicDB | `pkg/storage/badger_transaction.go:1829`/`:1843`/`:1857`/`:1871`/`:1942` build the strings. `ErrConflict` is the wrapper. |
| Status       | Working as designed. |
| Remediation  | Lock the canonical message shapes with a regression test that asserts the substrings consumers match on and that `errors.Is(err, ErrConflict)` succeeds. |

### 2.3 Constraint refresh under namespaced engine

| Aspect       | Detail                                                                 |
|--------------|------------------------------------------------------------------------|
| Historical bug | `DROP CONSTRAINT` + `CREATE CONSTRAINT` against a populated DB left the unique-value cache holding *unprefixed* node IDs (because `RefreshUniqueConstraintValuesForEngine` scanned through the `NamespacedEngine.toUserNode` path), while transactional writes pass *prefixed* IDs. Subsequent `MATCH (n {prop}) SET ...` on a pre-existing node falsely commit-failed with UNIQUE. |
| Consumer comp| Never re-issue `CREATE CONSTRAINT` on a populated database. Bootstrap DDL runs exactly once on an empty DB. |
| Status       | **Already fixed in current source.** `pkg/storage/constraint_validation.go:71` calls `EnsureNodeIDDatabasePrefixForEngine` to re-prefix node IDs before registering them, so the cache and transactional writes use the same key space. |
| Gap          | The fix has no regression guard. The closest test, `TestRefreshUniqueConstraintValuesForEngine_RebuildsCache` in `pkg/storage/constraint_validation_extra_test.go:27`, drives the **bare** engine with already-prefixed IDs — it would still pass even if `EnsureNodeIDDatabasePrefixForEngine` got removed. |
| Remediation  | New regression test through a `NamespacedEngine` that drops+recreates the constraint on a populated DB and then runs `CheckUniqueConstraint` on the matched node's storage-prefixed ID to prove no false UNIQUE surfaces. |

### 2.4 MERGE idempotency under retry

| Aspect       | Detail                                                                 |
|--------------|------------------------------------------------------------------------|
| Behavior     | `MERGE (n {uid: ...}) SET ...` re-executed after a commit-time UNIQUE failure must match the now-committed node and apply the SET cleanly. |
| Consumer comp| Rely on this to retry without de-duping by uid. |
| Status       | Working today. No regression guard at the integration level. |
| Remediation  | New cypher-level regression test that runs two concurrent MERGE writers on the same uid, asserts one commits and the other gets the documented commit-time UNIQUE shape, then re-runs the loser and asserts success with the loser's `SET` applied. |

### 2.5 UNWIND batch throughput at consumer-pinned label caps

| Aspect       | Detail                                                                 |
|--------------|------------------------------------------------------------------------|
| Behavior     | High-cardinality canonical writes (Variable=100, Struct=50, Function=15, K8sResource=1 rows per UNWIND batch) drain inside a 30s canonical-write timeout. |
| Consumer comp| Per-label batch caps were tuned to the smallest values that finish under 30s on million-entity inputs. |
| Status       | Currently good, but no benchmark gate. |
| Remediation  | A benchmark + threshold gate that runs an UNWIND-MERGE shape at each cap and fails if median run time exceeds a recorded ceiling. Not a hard CI gate (numbers drift); a `make bench-consumer-contract` lane that publishes JSON for review. Out of scope for this single-pass plan; tracked separately. |

### 2.6 Default flags consumers depend on

The defaults below are the **actual** post-`LoadDefaults()` values (verified against current source). Any flip to one of these is a deliberate, coordinated change.

| Field                                | Default | Why consumer pins it                                           |
|--------------------------------------|---------|----------------------------------------------------------------|
| `Database.AsyncWritesEnabled`        | `true`  | Async writes are on by default for throughput. Consumers that need synchronous commit set `NORNICDB_ASYNC_WRITES_ENABLED=false` explicitly. |
| `Database.PersistSearchIndexes`      | `false` | Search-index persistence is opt-in (experimental). Consumers that need restart durability set `NORNICDB_PERSIST_SEARCH_INDEXES=true`. |
| `Auth.Enabled`                       | `false` | Auth is off by default. Consumers running locally rely on this; production deployments enable via YAML or `NORNICDB_AUTH=...`. |
| `Server.BoltPort`                    | `7687`  | Match Neo4j default — consumers reuse Neo4j tooling out of the box. |
| `Server.HTTPPort`                    | `7474`  | Match Neo4j default. |

> **Correction note (single-pass):** earlier drafts of this plan asserted `AsyncWritesEnabled=false` and `PersistSearchIndexes=true`. The code defaults are the opposite. The contract is "the consumer can rely on these specific values not flipping silently" — which value each one is gets locked here against the actual code, not against a stale memory.

---

## 3. Regression test set

All tests live in `pkg/...` so they run inside the standard `go test ./...` gate. None of them require the Bolt server or the docker stack.

### 3.1 `pkg/storage/constraint_validation_namespaced_test.go` — new

Pins the fix at `constraint_validation.go:71`. Removing `EnsureNodeIDDatabasePrefixForEngine` from the rebuild path would re-introduce the documented "false UNIQUE on MATCH/SET against a pre-existing node" failure mode that downstream Bolt consumers pin as a contract.

Test wiring:

1. Create a `MemoryEngine`, wrap it with `NewNamespacedEngine(inner, "tenant_a")`.
2. Add a UNIQUE constraint on `:T(uid)` and create a node before triggering rebuild.
3. Call `RefreshUniqueConstraintValuesForEngine(ns, schema)`.
4. Assert `schema.CheckUniqueConstraint("T", "uid", "abc-123", storageID)` returns nil where `storageID = EnsureNodeIDDatabasePrefixForEngine(ns, "n-1")`.
5. Assert a different storage ID claiming the same value still produces a violation.

### 3.2 `pkg/storage/conflict_message_contract_test.go` — new

Locks the on-the-wire shape of optimistic-conflict errors at `pkg/storage/badger_transaction.go:1829`/`:1843`/`:1857`/`:1871`/`:1942`. Asserts the four canonical shapes contain the substrings consumers match on and that `errors.Is(err, ErrConflict)` succeeds for each.

### 3.3 `pkg/cypher/commit_failure_message_contract_test.go` — new

Locks the post-v1.0.45 `commit failed: constraint violation:...` wrapping at `pkg/cypher/transaction.go:181` + `pkg/storage/badger_transaction.go:1404`. Builds the wrapped error from the fixed format strings and asserts substring presence plus `errors.Is` chain preservation.

### 3.4 `pkg/cypher/merge_idempotency_under_concurrent_test.go` — new

Cross-contract test for §2.1 + §2.4. Two writers MERGE on the same uid; exactly one commits, the other returns the v1.0.45 commit-failed shape; re-executing the loser succeeds and the loser's `SET` is visible on read. Uses explicit barriers (`sync.WaitGroup` + ready channel), not `time.Sleep`.

### 3.5 `pkg/config/defaults_consumer_contract_test.go` — new

Pins the defaults from §2.6 using the real config entry point `config.LoadDefaults()`. Reads typed fields directly — there is no `DefaultFor` accessor.

```go
package config

import "testing"

// TestDefaults_ConsumerContract pins the parsed defaults downstream Bolt
// consumers depend on. Flipping any of these is a deliberate, coordinated
// change — see docs/plans/consumer-pinned-error-contract-plan.md §2.6.
func TestDefaults_ConsumerContract(t *testing.T) {
    c := LoadDefaults()

    if got, want := c.Database.AsyncWritesEnabled, true; got != want {
        t.Errorf("Database.AsyncWritesEnabled default changed: got %v want %v", got, want)
    }
    if got, want := c.Database.PersistSearchIndexes, false; got != want {
        t.Errorf("Database.PersistSearchIndexes default changed: got %v want %v", got, want)
    }
    if got, want := c.Auth.Enabled, false; got != want {
        t.Errorf("Auth.Enabled default changed: got %v want %v", got, want)
    }
    if got, want := c.Server.BoltPort, 7687; got != want {
        t.Errorf("Server.BoltPort default changed: got %v want %v", got, want)
    }
    if got, want := c.Server.HTTPPort, 7474; got != want {
        t.Errorf("Server.HTTPPort default changed: got %v want %v", got, want)
    }
}
```

### 3.6 Benchmark gate (deferred)

Out of scope for this single-pass plan. Tracked as a follow-up: `make bench-consumer-contract` lane that records median wall clock for UNWIND-MERGE at consumer-pinned label caps and publishes JSON for review.

---

## 4. Source-of-truth annotations

Add a one-line comment at each of the following sites linking to this plan, so future refactors trip over the contract before they break it. No behavior change.

| File:line                                              | Comment to add                                                                 |
|--------------------------------------------------------|---------------------------------------------------------------------------------|
| `pkg/cypher/transaction.go:181`                        | `// Wire contract: substring "commit failed" is matched by downstream Bolt classifiers. See docs/plans/consumer-pinned-error-contract-plan.md §2.1.` |
| `pkg/storage/badger_transaction.go:1404`               | `// Wire contract: prefix "constraint violation:" is matched downstream. See docs/plans/consumer-pinned-error-contract-plan.md §2.1.` |
| `pkg/storage/badger_transaction.go:1829`               | `// Wire contract: substrings "conflict:" and "changed after transaction start" are matched downstream as transient. See docs/plans/consumer-pinned-error-contract-plan.md §2.2.` |
| `pkg/storage/constraint_validation.go:71`              | `// Pinned by consumer-pinned-error-contract-plan.md §2.3 + TestRefreshUniqueConstraint_KeepsPrefixedIDs_UnderNamespacedEngine. Removing EnsureNodeIDDatabasePrefixForEngine re-introduces a known false-UNIQUE failure mode for namespaced engines.` |
| `pkg/storage/badger_constraint_validation.go:279`,`:343`,`:659`,`:686` | `// Wire contract: "Node with ... already exists" / "Relationship with ... already exists" are matched downstream. See docs/plans/consumer-pinned-error-contract-plan.md §2.1.` |

These are 1-line comments — they do not add explanatory prose. The plan holds the prose; the comments hold the link.

---

## 5. Rollout order

This is a single-pass plan: every step listed here lands in one coordinated change, with the regression tests gating the source annotations.

1. Land the §3.1, §3.2, §3.3, §3.4, §3.5 regression tests.
2. Add the §4 source-of-truth comments at the exact file:line cites.
3. Add a `CHANGELOG.md` bullet under "Internal":
   *"Pinned consumer-visible error contract and namespaced-engine constraint refresh behavior with explicit regression tests."*
4. File one follow-up issue: "Migrate consumer-pinned wire contract from substring matching to stable Neo4j error codes" (out of scope for this plan).

---

## 6. Out of scope

- Changing any error string, default, or retry semantics. This plan **pins** existing behavior. Behavior changes go through their own ADR.
- Replacing the substring-match wire contract with a typed code surface. That is a real follow-up (NornicDB returns Neo4j-shaped error codes today; consumers would prefer a stable code over a stable message), but it requires a deprecation window and consumer-side migration. Out of scope here.
- The §2.5 / §3.6 benchmark gate. Tracked separately.
- Vendoring a NornicDB-specific Bolt driver for downstream consumers. Consumers explicitly use the upstream `neo4j-go-driver/v5` and that contract is the point.

---

## 7. Done criteria

- [ ] §3.1–§3.5 land green on `go test ./pkg/storage/... ./pkg/cypher/... ./pkg/config/... -race -count=1`.
- [ ] §4 annotations land at the exact file:line cites above.
- [ ] CHANGELOG bullet added.
- [ ] One follow-up issue filed for the typed-code-surface migration.
