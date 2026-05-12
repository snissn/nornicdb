# Knowledge-Layer Scoring and Visibility Plan

**Status:** Proposed
**Date:** April 15, 2026
**Scope:** Replace hardcoded Ebbinghaus memory-tier decay behavior with a generic, profile-and-policy-driven decay and scoring system that can support existing, proposed, or future decay models, while expressing promotive declarative tiers through separate promotion profile and policy subsystems, supporting MVCC-aware score-from selection for both nodes and edges, implementing efficient deindexing for visibility-suppressed nodes and edges, persisting `ON ACCESS` mutation state in a separate accessMeta index so that nodes and edges remain read-only during policy evaluation, evaluating scoring before query visibility so that invisible entities are suppressed from queries unless accessed through `reveal()`, and resolving promotion policies before decay profiles.

> **Observability.** Decay / promotion / suppression, access-flush batches,
> and on-access mutations are instrumented under the
> `nornicdb_knowledge_policy_*` prefix. See
> [`docs/observability/knowledge-policy-metrics.md`](../observability/knowledge-policy-metrics.md)
> for the full instrument reference and operational runbook, and
> [`docs/plans/knowledge-policy-observability-plan.md`](knowledge-policy-observability-plan.md)
> for the implementation plan that wires this subsystem into the OTEL layer.
> Key alerts:
>
> - `access_flush_buffer_fullness > 0.9` sustained → raise
>   `Memory.AccessFlushBufferSize` or lower `Memory.DecayInterval`.
> - `suppressions_total{reason}` skew from `below_threshold` to
>   `score_floor` → floor policy is dominating; review binding configuration.
> - `decay_score` histogram flat → half-life mis-tuned.

---

## 1. Objective

Implement a flexible decay and scoring architecture in NornicDB where retention behavior is resolved from policies rather than hardcoded cognitive tiers.

The system must support:

- no-decay entities and properties
- configurable decay half-lives and thresholds
- node-, edge-, and property-level decay behavior
- named policy presets for operator convenience
- separate promotion policies that declaratively model tier-like score boosts by referencing promotion profiles, without changing the existing Cypher scoring API
- declarative MVCC-aware score-from selection through decay profile options
- future decay models without requiring new engine enums or switch statements
- efficient visibility suppression of whole nodes and whole edges
- asynchronous removal of suppressed nodes and suppressed edges from indexing
- property-level decay effects that can exclude properties from vectorization or retrieval surfaces without suppressing, moving, or deleting those properties from storage

Nodes and edges must be treated as first-class decay targets. A node or edge must be able to decay, be scored, be suppressed from retrieval, be removed from indexing, and be promoted using the same policy-driven machinery.

Properties are not suppression targets. Properties may receive decay scores and vectorization-exclusion behavior, but they remain stored in place and remain directly queryable through Cypher.

This plan is intentionally model-agnostic. It is not tied to any one research paper or taxonomy. Although inspired by this research paper which called out NornicDB specifically. [https://arxiv.org/pdf/2604.11364](https://arxiv.org/pdf/2604.11364)

---

## 2. Problem Statement

NornicDB currently has memory-decay behavior that depends on fixed tier names and fixed decay assumptions. That makes the system harder to evolve because retention logic is embedded in runtime code rather than expressed declaratively.

That creates six engineering problems:

1. Adding new retention behavior requires code changes instead of policy changes.
2. The engine assumes a closed set of decay categories.
3. Decay is primarily entity-wide instead of being expressible at node, edge, and property scope.
4. Operators cannot declare retention semantics through the same schema-oriented mechanisms they already use elsewhere.
5. Under MVCC, decay scoring needs an explicit start-time anchor unless the policy states whether score age begins at entity creation time or at the latest visible version time.
6. Suppressed nodes and edges must be removed from indexing efficiently, without expensive full-index scans, while property-level decay behavior must not be confused with whole-entity suppression.

The system should instead treat decay behavior as configurable retention profiles, promotion behavior as separate configurable scoring profiles and policies, score start time as an explicit profile decision, and deindex cleanup as a dedicated deindex workflow for nodes and edges only.

---

## 3. Design Principles

1. Retention behavior must be data-driven, not hardcoded into a fixed enum.
2. Decay and scoring must be resolvable at node, edge, and property scope.
3. `NO DECAY` must be directly expressible in policy definitions.
4. Decay half-life, decay function, visibility threshold, and score floor must be configurable independently.
5. Promotion tiers must be expressible declaratively through separate promotion profile and promotion policy subsystems rather than through hardcoded runtime categories.
6. Score start time must be declaratively expressible through decay profile options using `CREATED`, `VERSION`, or `CUSTOM`.
7. Nodes and edges must be handled symmetrically by the policy system. Edge decay must not be a second-class or special-case feature.
8. Suppression behavior applies only to whole nodes and whole edges, never to individual properties.
9. Property-level decay may influence vectorization, ranking, filtering, reranking, and summarization, but it must not move, suppress, or delete stored property values.
   9a. Properties that participate in structural indexes (lookup indexes, range indexes, and composite indexes) are immune to decay scoring, decay hiding, and property-level exclusion. Fulltext indexes and vector indexes are retrieval-surface indexes and do not confer property immunity. Indexed properties must remain stable and always visible because they are relied upon for aggregation, joining, and lookup.
10. Suppressed nodes and edges must be removed from indexing using exact-key deindexing rather than discovery by scanning secondary indexes.
11. Runtime paths must not silently fall back to legacy tier assumptions.
12. Named presets may exist for convenience, but the engine must operate on resolved profiles and policies.
13. The architecture must be flexible enough to support any current or future decay model.

---

## 4. Target Architecture

### 4.1 Decay Profile Layer

Decay profiles are the mechanism that decides whether decay applies, at what rate, at what scope, and from which score start time decay age is measured. Decay profiles are the only decay authoring surface — there is no separate decay policy concept.

Required behavior:

- resolve effective decay profile from configuration and profile definitions
- support node-, edge-, and property-level targeting
- allow `NO DECAY` and rate-based decay without relying on fixed tier names
- permit named presets but not require them
- support multiple decay functions over time
- support score start-time selection through profile options
- resolve suppression eligibility for whole nodes and whole edges
- resolve property-level vectorization-exclusion behavior without treating properties as suppression targets
- reject or ignore property-level decay rules that target properties participating in structural indexes (lookup, range, and composite indexes); indexed properties are immune to decay scoring and hiding; fulltext and vector indexes do not confer immunity
- enforce at most one decay profile per unique target as a hard constraint

Suggested fit in NornicDB:

- shared profile resolver used by recall, recalc, suppression pass, and ranking paths
- config-defined presets for operator convenience
- schema-backed decay profiles as the main control surface
- diagnostics that explain why a given node or edge resolved to a given decay profile and score start time

### 4.2 Promotion Layer

Promotion behavior is split into two object types: profiles and policies.

Promotion profiles are named parameter bundles (multiplier, score floor, score cap, scope). They contain no logic and cannot be targeted to entities directly. They are referenced by name inside promotion policy APPLY blocks.

Promotion policies contain logic — `FOR` targets, `APPLY` blocks, `WHEN` predicates, and optional `ON ACCESS` mutation blocks. Policies bind profiles to specific node labels, edge types, and property paths. Promotion policies are resolved first, before decay profile resolution. `WHEN` predicates are evaluated before `ON ACCESS` mutations — if the entity is visibility-suppressed (below the visibility threshold), `ON ACCESS` mutations do not execute. This prevents suppressed entities from accumulating access state they should not have. The promotion adjustments are applied to the base decay score to produce the final score without changing the existing Cypher scoring API.

Required behavior:

- resolve applicable promotion profiles through promotion policy evaluation
- support node-, edge-, and property-level targeting
- allow promotion profiles to declare score multipliers, caps, and floors
- when multiple WHEN predicates match within a policy, the profile with the highest effective multiplier wins deterministically
- keep promotion profiles separately authored, shown, and retrieved from promotion policies
- support optional `ON ACCESS` mutation blocks that execute when the target is accessed during scoring resolution, but only after `WHEN` predicates have been evaluated and only if the entity passes the suppression gate (is not visibility-suppressed); `ON ACCESS` mutations write exclusively to a separate accessMeta index keyed to the target node or edge, never to the node or edge itself
- enforce at most one promotion policy per unique target as a hard constraint

Suggested fit in NornicDB:

- a dedicated promotion subsystem with its own catalog and DDL for both profiles and policies
- a separate accessMeta index that stores `ON ACCESS` mutation state per target node or edge as `map[string]interface{}`, serialized in msgpack alongside other data files for performance
- ON ACCESS mutations are accumulated in-process and flushed asynchronously; ON ACCESS blocks are syntactic sugar for declaring which counters and timestamps the accumulator tracks — they are not executed as literal Cypher statements on every read
- ON ACCESS SET expressions may optionally include a `WITH KALMAN` modifier that routes the raw measurement through a per-property Kalman filter before storing the value in accessMeta; this is a behavioral signal smoother — it stabilizes derived metrics (confidence scores, cross-session access rates, agreement ratios) where the input stream is noisy, not raw monotonic counters or timestamps which should use plain `SET`; the Kalman filter algorithm is the same fast scalar Kalman from the imu-f flight controller (`docs/plans/kalman.md`), inlined for zero-allocation hot-path performance; three modes are supported: `WITH KALMAN` (auto mode — R auto-adjusted by a per-property variance tracker, Q defaults to 0.0001), `WITH KALMAN{q: <val>, r: <val>}` (manual mode — fixed Q and R), and `WITH KALMAN{q: <val>}` (hybrid — user-set Q, R auto-adjusted); Kalman filter state and variance tracker state are persisted as a typed `KalmanFilters map[string]*KalmanPropertyState` field on `AccessMetaEntry`, keyed by property name; when `WHEN` predicates or `policy()` read a Kalman-filtered property, they see the filter's optimal estimate, not the raw measurement; query context variables from `X-Query-*` request headers are available inside `ON ACCESS` blocks and `WHEN` predicates as `$_<key>` (e.g., `X-Query-Session` → `$_session`, `X-Query-Agent` → `$_agent`, `X-Query-Tenant` → `$_tenant`), enabling session-aware gating, provenance tracking, and cross-session reinforcement before measurements reach the filter; see §7.4 for design constraints and usage guidance
- the hot path (read time) buffers ON ACCESS increments in per-P (per-processor) sharded accumulators using `sync.Pool` for P-local affinity, targeting zero cross-core contention regardless of graph topology; each shard holds per-entity deltas (counters, timestamps, and custom keys), not absolute values; no msgpack, no Badger write, no allocation on the read path; see the [implementation plan](./knowledge-layer-persistence-implementation.md) §3.1 for the full `AccessAccumulator` design, including the `entityDelta` struct, `sync.Pool`-based P-local shard selection, and super-node contention analysis
- the cold path (flush) is a background goroutine that drains the per-P shards on a configurable interval (default: 2s); it locks each shard briefly, swaps out the delta map, aggregates across all shards, and applies a single batched Badger write that merges the deltas into the persisted accessMeta entries; this is the only path that does msgpack round-trips; see the [implementation plan](./knowledge-layer-persistence-implementation.md) §3.3 for flush goroutine details
- P-local sharding eliminates cross-core contention; the flush goroutine is the sole writer to Badger accessMeta keys, eliminating write contention
- timestamp fields (`lastAccessedAt`, `lastTraversedAt`) use max-wins semantics in the per-entity delta struct; the flush writes the latest value, not an accumulation
- access counts are eventually consistent with a bounded lag of one flush interval; WHEN predicates that read `n.accessCount` see `persisted + buffered delta` by reading through the accumulator across all P-local shards, not Badger
- shared runtime scoring that first resolves the promotion policy, then resolves the decay profile, then applies promotion adjustments to the base decay score
- property reads within `ON ACCESS` blocks and `WHEN` predicates resolve from accessMeta first, falling back to the node or edge's stored properties
- diagnostics that explain which promotion policy matched, which profile was selected, and how it affected the final score

### 4.3 Authoring Subsystem Layer

The authoring subsystem is the surface for declaring decay profiles and promotion profiles and policies.

Required behavior:

- allow operators to declare decay profiles in Cypher
- allow operators to declare promotion profiles and promotion policies in Cypher
- validate definitions at creation time where applicable
- expose profiles and policies through introspection and admin APIs
- enforce one decay profile and one promotion policy per unique target
- support property-targeted rules in addition to node and edge targets

Suggested fit in NornicDB:

- introduce a dedicated decay profile subsystem with its own catalog and DDL
- introduce a dedicated promotion subsystem with its own catalog and DDL for both profiles and policies
- borrow authoring, validation, and introspection patterns from the constraint subsystem without making decay or promotion rules first-class constraints
- express property-level retention and promotion as inline entries inside profile or policy bodies
- add retention-specific and promotion-specific resolution rules alongside existing schema rules

### 4.4 Runtime Resolution Layer

The runtime resolution layer converts configuration and profiles into effective decay behavior and final score for a node, edge, or property. Scoring happens before query visibility — a node or edge must be scored before it becomes visible to the query.

Required behavior:

- evaluate the matching promotion policy first during recall, reinforcement, recalc, suppression pass, and ranking; evaluate `WHEN` predicates to determine the entity's promotion tier and whether it is suppressed; execute `ON ACCESS` mutations only if the entity passes the suppression gate (is not visibility-suppressed after promotion and decay resolution)
- resolve decay profile second during recall, reinforcement, recalc, suppression pass, and ranking
- resolve score start time from decay profile during score evaluation
- compute the final score from promotion and decay resolution before determining query visibility
- suppress nodes, edges, and properties from query results when their final score renders them invisible, unless the caller uses `reveal()` to bypass scoring-driven visibility
- support explicit overrides and inheritance
- allow property-level state without forcing entity-wide decay
- resolve inline property entries from the active decay profile before falling back to entity defaults
- expose final decay score through native Cypher functions without changing Neo4j-compatible node or relationship result shapes
- expose raw stored entities through `reveal()` without decay-driven visibility filtering or property hiding
- avoid duplicated logic across CLI, DB, and API code paths

Suggested fit in NornicDB:

- one shared resolver used by DB runtime, CLI decay tools, Cypher procedures, and background maintenance
- one explanation format returned by diagnostics and admin endpoints
- one shared scorer that evaluates promotion first, then computes base score from decay profile, then applies promotion adjustments to produce the final score
- one shared MVCC-aware score-start resolver that interprets `CREATED`, `VERSION`, and `CUSTOM`
- one `reveal()` bypass path that returns the raw stored entity, skipping scoring-driven visibility and property hiding

### 4.5 MVCC Interaction Layer

MVCC version resolution and decay scoring are separate concerns, but scoring gates query visibility. MVCC determines which version of an entity exists at the transaction snapshot. Scoring then determines whether that version is visible to the query.

Required behavior:

- resolve the visible node, edge, or property version using the transaction snapshot
- evaluate promotion policy and decay profile on the resolved version before exposing the entity to the query
- evaluate the base decay score using the score start time resolved from decay profile
- suppress entities whose final score falls below the visibility threshold from query results, search hits, and traversal paths
- support `CREATED`, where decay age begins at the entity's original creation timestamp
- support `VERSION`, where decay age begins at the latest visible version timestamp under MVCC
- allow `reveal()` to bypass scoring-driven visibility and return the MVCC-resolved version without suppression
- never require new stored versions solely because a derived score changed over time

Suggested fit in NornicDB:

- version resolution remains owned by MVCC
- score start-time choice remains owned by decay profile
- the shared scorer consumes both the visible node or edge version and the profile-resolved score start time
- query visibility is determined after scoring: MVCC resolves the version, scoring determines whether it appears

### 4.6 Visibility Suppression and Deindex Layer

The visibility suppression and deindex layer is the mechanism that removes suppressed whole nodes and whole edges from indexing in the most performant way possible.

Required behavior:

- suppress only whole nodes and whole edges
- never suppress, move, or delete individual properties because of decay profile
- mark suppressed nodes and edges in primary storage immediately
- remove suppressed nodes and edges from indexing asynchronously
- avoid discovering stale index entries by scanning entire secondary indexes
- support a configurable background cleanup cadence, defaulting to nightly but configurable in seconds
- ensure suppressed nodes and edges are skipped efficiently during retrieval
- allow property-level vectorization exclusion without storage relocation or Cypher inaccessibility

Suggested fit in NornicDB:

- maintain a per-node and per-edge index-entry catalog that stores the exact secondary-index keys written for that entity
- when a node or edge becomes suppressed, enqueue a deindex work item referencing that entity and its index-entry catalog
- have the background deindex job drain deindex work items and perform blind batched deletes against index keys
- keep read-time suppressed checks cheap so suppressed entities are skipped even before asynchronous deindex completes
- treat physical space reclamation as separate storage maintenance rather than part of logical suppression/deindex semantics

---

## 5. Logical Resolution Model

Because decay scores are derived rather than stored on fields, this section describes runtime resolution artifacts and schema objects, not a stored score data model.

### 5.1 Schema Objects

#### DecayProfile

Database object used to define reusable decay parameter bundles. Profiles contain no logic — they declare configuration values only.

Minimum fields:

- profile id
- profile name
- half-life definition in seconds
- scoring function or strategy id
- score start time: `CREATED`, `VERSION`, or `CUSTOM`
- custom score-from property path, if `CUSTOM`
- visibility threshold override for node or edge suppression eligibility
- minimum score floor
- scope type: node, edge, property
- enabled or disabled

#### PromotionProfile

Database object used to define reusable promotive scoring parameter bundles. Profiles contain no logic — they declare configuration values only.

Minimum fields:

- profile id
- profile name
- score multiplier
- optional score floor override
- optional score cap override
- scope type: node, edge, property
- enabled or disabled

#### PolicyBackedDecayRule

Logical rule compiled from decay profile definitions and used by the resolver.

Minimum fields:

- contract name
- policy name
- entity target: label or edge type
- property path, if any
- rule kind: no-decay, policy, rate, threshold, floor, function
- referenced policy name, if any
- inline rule order for deterministic precedence
- original expression text for diagnostics

#### PolicyBackedPromotionRule

Logical rule compiled from promotion policy definitions and used by the resolver.

Minimum fields:

- contract name
- policy name
- entity target: label or edge type
- property path, if any
- rule kind: promotion-profile, multiplier, floor, cap
- referenced policy name, if any
- predicate expression
- inline rule order for deterministic precedence
- original expression text for diagnostics

#### AccessMeta

Persistent metadata index that stores `ON ACCESS` mutation state separately from the node or edge it describes. Each entry is a `map[string]interface{}` keyed to a target node or edge identifier. AccessMeta entries are serialized in msgpack alongside other data files for performance.

Nodes and edges are read-only during `ON ACCESS` evaluation. All writes within an `ON ACCESS` block mutate the target's accessMeta entry, never the target's stored properties. All reads within `ON ACCESS` blocks and `WHEN` predicates resolve from the target's accessMeta entry first, falling back to the target's stored properties when the key is not present in accessMeta. The `stored(entity)` qualifier may be used inside WHEN predicates and ON ACCESS blocks to force a read from stored node or edge properties, bypassing accessMeta-first resolution. `stored()` is the escape hatch for properties managed by external processes and is not a general Cypher function.

AccessMeta has a fast-path fixed-layout struct for the most common fields (`accessCount int64`, `lastAccessedAt int64`, `traversalCount int64`, `lastTraversedAt int64`). Only custom keys fall back to the `map[string]interface{}` overflow map. The fixed-layout struct serializes to a known-size byte slice with no reflection. msgpack is used only for the overflow map of custom keys. Pre-allocated per-entity byte buffers in the flush goroutine are reused across iterations (sync.Pool or ring buffer).

All integer values in accessMeta are normalized to `int64` and all floating-point values to `float64` at deserialization time. This normalization ensures that Cypher arithmetic in ON ACCESS and WHEN blocks operates on consistent types — `coalesce(n.accessCount, 0) + 1` always works because both operands are `int64`. Boolean values remain `bool`. String values remain `string`. `time.Time` is stored as `int64` (UnixNano) and converted on read.

ON ACCESS mutations are not executed as literal Cypher writes on every read. They are accumulated in-process via a sharded counter ring and flushed asynchronously to Badger in batches. See section 4.2 for the accumulator design.

Minimum fields:

- target id
- target scope: node or edge
- metadata map: `map[string]interface{}`
- last accessed at
- last mutated at
- mutation count

#### AccessMeta Lifecycle

When a node or edge is deleted (tombstoned in MVCC), its accessMeta entry is enqueued for deletion in the same transaction. The accessMeta key is deleted immediately from the in-process accumulator and enqueued as a deindex work item alongside any index-entry catalog cleanup.

When a node or edge is suppressed, its accessMeta entry is retained — suppressed entities are still accessible via `reveal()`, and `policy()` on a revealed entity should still return its access history. The accessMeta entry is only deleted when the entity is physically reclaimed by the compliance retention lifecycle.

When MVCC version pruning removes all versions of an entity, its accessMeta entry is eligible for deletion. The `PruneMVCCVersions` function should check for orphaned accessMeta entries and delete them.

AccessMeta keys use a dedicated prefix (`prefixAccessMeta`) so that orphan detection is a prefix scan bounded to accessMeta, not a full database scan.

AccessMeta is included in MVCC snapshot isolation. Updates are atomic but the snapshot is always as of the transaction time.

#### IndexEntryCatalog

Persistent catalog of exact index entries created for a node or edge.

Minimum fields:

- target id
- target scope: node or edge
- index entry key list or catalog reference
- index family identifiers, if partitioned
- last indexed version, if tracked
- suppressed boolean or state marker, if duplicated for cleanup convenience

#### DeindexWorkItem

Persistent background work item used to deindex a visibility-suppressed node or edge.

Minimum fields:

- work item id
- target id
- target scope: node or edge
- suppression state
- enqueued at
- next attempt at
- retry count
- cleanup status
- index catalog reference or direct key reference

### 5.2 Derived Runtime Artifacts

#### ScoringResolution

Derived resolution result produced by the shared resolver for a requested node, edge, or property.

Minimum fields:

- target id
- target scope
- resolved decay profile id
- resolved score start time
- resolution source chain
- applied decay profile names
- applied decay profile entries
- applied promotion policy name
- applied promotion profile name selected by the policy
- effective rate
- effective threshold
- effective multiplier
- base score
- final score
- no-decay boolean
- suppression-eligible boolean for node or edge targets only

#### DecayResolutionMeta

Derived metadata emitted at read time for Cypher and unified search surfaces.

Minimum fields:

- entity id
- entity scope: node or edge
- entity decay score, if applicable
- score start time
- per-property resolved score map
- optional per-property explanation payload

### 5.3 Design Rule

- derived scores are not persisted into node, edge, or property payloads
- the shared resolver is the source of truth for node-, edge-, and property-level scoring
- Cypher functions and unified search metadata project derived scores outward without mutating stored graph data
- the existing Cypher scoring API remains unchanged; resolved promotion policies affect the returned score through the shared scorer rather than through new function signatures
- the score start time is resolved from decay profile and used by the shared scorer without changing the existing Cypher scoring API
- whole-node and whole-edge suppression state is persisted
- property suppression state is not persisted because properties are not suppression targets
- property-level decay may exclude properties from vectorization or retrieval surfaces but must not move or delete stored property values
- `ON ACCESS` mutation state is persisted in a separate accessMeta index keyed per target node or edge, not on the node or edge itself
- accessMeta entries are `map[string]interface{}` serialized in msgpack alongside other data files for performance
- nodes and edges are read-only during `ON ACCESS` evaluation; all writes target the accessMeta index
- property reads within `ON ACCESS` blocks and `WHEN` predicates resolve from accessMeta first, falling back to stored node or edge properties
- the `policy()` Cypher function projects accessMeta outward without implying that access-tracking metadata is stored on the node or edge

---

## 6. Query and Resolution Semantics

### 6.1 Resolution Rules

Scoring happens before query visibility. When a query touches a node or edge, the engine must resolve and apply promotion and decay scoring before deciding whether the entity is visible to the query. An entity whose final score falls below the visibility threshold or whose decay profile renders it invisible must not appear in `MATCH` results, `WHERE` evaluation, or search hits unless the caller explicitly uses `reveal(entity)` to bypass scoring-driven visibility.

The resolution order is: promotion first, then decay, then score-start resolution, then visibility determination.

Every scoring-aware read or maintenance operation should resolve the promotion policy first, in this order:

1. property-level promotion policy entries that match the target
2. entity-level promotion policy entries that match the target
3. edge-type or label-targeted promotion policy
4. wildcard-targeted promotion policy (`FOR (n:*)` or `FOR ()-[r:*]-()`)
5. configured default promotion behavior, if any

Then every scoring-aware operation should resolve the decay profile in this order:

6. explicit no-decay rule
7. property-level inline rule inside the applicable decay profile
8. entity-level rule inside the applicable decay profile
9. edge-type or label-targeted decay profile
10. wildcard-targeted decay profile (`FOR (n:*)` or `FOR ()-[r:*]-()`)
11. configured default decay profile

Then every score-aware read should resolve the score start time from the resolved decay profile:

12. `CREATED`, if the resolved decay profile declares `CREATED`
13. `VERSION`, if the resolved decay profile declares `VERSION`
14. `CUSTOM`, if the resolved decay profile declares `CUSTOM` with a `scoreFromProperty` path; the property is resolved from accessMeta first, falling back to stored node or edge properties; if the resolved value is null or unparsable, log a warning and fall back to entity creation time
15. configured default score start time, if no explicit profile value applies

Then the engine computes the final score and determines visibility:

16. compute the base decay score from the resolved decay profile and score start time
17. apply the resolved promotion policy adjustments to produce the final score
18. if the final score falls below the visibility threshold, the entity is invisible to the query unless accessed through `reveal()`; `ON ACCESS` mutations do not execute for suppressed entities
19. if property-level decay excludes a property from retrieval surfaces, that property is hidden from the query result unless accessed through `reveal()`
20. properties that participate in structural indexes (lookup, range, and composite indexes) are never subject to steps 18 or 19 — they are immune to decay scoring, decay hiding, and property-level exclusion regardless of any matching decay profile or promotion policy; fulltext and vector indexes do not confer this immunity

If no promotion policy matches, the target should resolve with a neutral promotion effect.

If no decay profile matches, the engine should either treat the target as non-decaying or use an explicit configured default decay profile, but it must not silently assume any legacy tier.

If no score start time matches, the engine should use an explicit configured default. The recommended default is `VERSION`.

#### Compiled Binding Tables and Lazy Scoring

The resolution cascade above is the logical model. The implementation pre-flattens it at DDL time using a three-tier optimization strategy.

**Tier 1 — Compile-time profile binding table.** When a decay profile or promotion policy is created, altered, or dropped, the schema manager builds a direct lookup table: `map[string]*compiledBinding` keyed by label or edge type. Each `compiledBinding` holds the resolved decay profile pointer, the resolved promotion policy pointer, the visibility threshold, the score-start mode, and the decay function pointer. Wildcard entries are expanded into per-label/per-type entries at compile time. Resolution at query time is a single map lookup — no cascade. The table is rebuilt on any DDL change, which is rare. For multi-label nodes, the table keys on sorted label sets, not individual labels.

**Tier 2 — Suppressed-bit fast path.** Suppressed entities already have a persisted marker in primary storage. The read path checks the suppressed bit before any profile resolution. If suppressed and the query does not use `reveal()`, skip immediately. Cost: one byte check. This eliminates full resolution for the entire suppressed population.

**Tier 3 — Amortized score computation.** For non-suppressed entities with exponential decay, the score is a pure function of `(now - scoreFrom, halfLife)`. Pre-compute a score threshold timestamp: `thresholdAge = -halfLife * ln(visibilityThreshold) / ln(2)`. At read time, compare `now - scoreFrom > thresholdAge` using integer subtraction on UnixNano values — no `math.Exp()` needed for the visibility check. Only compute the precise float64 score when the entity survives visibility and is projected into results (lazy scoring). This reduces the hot path to one integer comparison per entity. The `thresholdAge` is computed once at compile time per decay profile and stored as `int64` nanoseconds in the compiled binding.

For `ORDER BY decayScore(n)`, the scorer can use a monotonic proxy: `scoreFromTime.UnixNano()` itself is monotonically related to the decay score (newer = higher score) for a fixed half-life and function. Sorting by `scoreFromTime DESC` is equivalent to sorting by `decayScore ASC` without computing any exponentials. The precise score is only needed if the caller mixes `decayScore()` with other expressions in ORDER BY.

#### Multi-Label Node Resolution

If a node has multiple labels (e.g., `:SessionRecord:MemoryEpisode`) and separate decay profiles exist for both labels, the following rules apply:

- When a `CREATE DECAY PROFILE ... FOR (n:LabelA)` is issued, the schema manager checks whether any existing node in the database has both `:LabelA` and another label that already has a targeted binding. If so, the CREATE fails with: "Conflict: nodes with labels [:LabelA, :LabelB] would match two decay profiles. Create a dedicated profile for the multi-label combination or drop one of the conflicting profiles."
- If the operator explicitly wants multi-label handling, they create a profile targeting the multi-label combination: `FOR (n:SessionRecord:MemoryEpisode)`. A multi-label target takes precedence over any single-label target.
- At query time, if a multi-label node somehow matches multiple bindings (e.g., a label was added after profile creation), resolution picks the binding with the most specific (most labels) target. If two bindings have equal specificity, the resolver returns an error logged as a diagnostic warning. The node is treated as non-decaying until the conflict is resolved.
- The compiled binding table handles this by keying on sorted label sets, not individual labels.

### 6.2 MVCC Score Start-Time Semantics

The engine should support three profile-declared score start times:

- `CREATED`
- `VERSION`
- `CUSTOM`

These semantics must apply equally to nodes and edges.

#### `CREATED`

`CREATED` means the decay age is measured from the entity's original creation timestamp.

Semantics:

- MVCC determines which node or edge version is visible at the transaction snapshot
- the scorer uses the original creation timestamp as the start of decay age
- later updates do not reset decay age
- `CREATED` is the durable, age-from-origin option

#### `VERSION`

`VERSION` means the decay age is measured from the latest visible version timestamp under MVCC.

Semantics:

- MVCC still determines which node or edge version is visible at the transaction snapshot
- the scorer uses the latest visible version timestamp as the start of decay age
- updates reset decay age for the visible target
- `VERSION` is the freshness-from-last-change option

#### `CUSTOM`

`CUSTOM` means the decay age is measured from a user-specified property value on the entity.

Semantics:

- MVCC still determines which node or edge version is visible at the transaction snapshot
- the scorer reads the property path declared in the decay profile's `scoreFromProperty` option using accessMeta-first resolution: the property is resolved from the target's accessMeta entry first, falling back to the target's stored node or edge properties only when the key is not present in accessMeta
- the property value must be a timestamp; if the resolved value is missing, null, or not parsable as a timestamp, the scorer should log a warning and fall back to the entity's original creation time
- `CUSTOM` is the operator-defined, domain-specific option

#### Rule

Visibility is always snapshot-based. Only the decay-age start time changes.

The scoring timestamp ("now") is the transaction's MVCC snapshot timestamp for query paths, or the maintenance cycle start time for background paths. The scorer does not call `time.Now()`. The scorer receives the snapshot timestamp from the transaction context. This ensures deterministic, repeatable scoring within a transaction: the same entity queried twice in the same transaction returns the same score. The snapshot timestamp is already available in the MVCC read path (`MVCCVersion.CommitTimestamp`). It is passed through to the scorer as `scoringTime`. For background maintenance (recalc, suppression pass), `scoringTime` is `time.Now()` at the start of the maintenance cycle, frozen for the duration of the batch.

The system must not create new stored versions solely because a derived score changed.

### 6.3 Property-Level and Edge-Level Semantics

Property-level decay is required for mixed-longevity entities.

Examples:

- a `Profile` node may keep `name` and `tenantId` permanently while decaying `lastConversationSummary`
- a `Task` edge may keep identity and timestamps permanently while decaying a transient confidence field
- a `Document` node may keep canonical content permanently while decaying ranking hints or ephemeral summaries
- a `CO_ACCESSED` edge may decay as a whole, even if neither endpoint node decays at the same rate

Edge-level decay should support at least these outcomes:

- lowering ranking weight for an edge during retrieval or traversal
- suppression or hiding of an edge while preserving endpoint nodes
- edge-specific decay independent of the decay profile of connected nodes

Property decay should support at least these outcomes:

- lower ranking weight for the property during retrieval
- exclusion of the property from vectorization or vector-backed retrieval if policy says so; when the `AccessFlusher` detects that a property's score has crossed below its visibility threshold during a flush cycle, it writes an explicit `nil` for that property key in the entity's `AccessMetaEntry.Overflow` map and re-queues the node for embedding via `InvalidateManagedEmbeddings` + `AddToPendingEmbeddings`; the embed worker applies accessMeta-first projection over node properties before building embed text — any property that resolves to explicit `nil` after projection is excluded from the embed text; when a property's score rises above the threshold (e.g., via promotion), the flusher removes the `nil` key from the overflow map and re-queues the node, and the next embed cycle includes the property again; no separate background scorer or persisted suppression list is needed — the overflow map `nil` convention is the suppression signal
- explicit supersession or replacement behavior in retrieval logic, if configured

Properties that participate in structural indexes (lookup indexes, range indexes, and composite indexes) are immune to property-level decay scoring, decay hiding, and vectorization exclusion. These properties must remain stable and always visible to queries because index-backed operations depend on their values being present and consistent. Fulltext indexes and vector indexes are retrieval-surface indexes and do not confer property immunity — property-level decay may exclude a property from a vector index or fulltext search without breaking aggregation or joins. If a decay profile or promotion policy contains a property-level rule that targets a property participating in a structural index, the engine should reject the rule at creation time with a validation error.

Property-level promotion should support at least these outcomes:

- higher ranking weight for the property during retrieval
- tier-like score boosts for reinforced or validated properties
- score floor or cap adjustments without changing the parent entity's stored fields

Property-level scores should only influence retrieval when the property is directly involved in matching, ranking, reranking, filtering, projection, summarization, vectorization, or vector-backed retrieval. A decayed or promoted property should not silently degrade or improve the score of the entire entity by default.

Edge decay should not be inferred from node decay by default. An edge must be able to decay on its own policy terms even if both endpoint nodes are non-decaying.

Properties are not suppression targets. A property with a low score for vectorization may be excluded from vectorization outputs or vector-backed retrieval, but it remains stored in place and directly queryable in Cypher.

### 6.4 Suppression Semantics

Visibility suppression applies only to whole nodes and whole edges.

When a node or edge crosses suppression eligibility:

- the node or edge may be marked suppressed in primary storage
- the node or edge should be skipped by retrieval and ranking paths as efficiently as possible
- the node or edge must be removed from secondary indexing asynchronously
- the system must not scan secondary indexes to discover which entries to remove
- the system should use the target's stored index-entry catalog to perform direct key deletion

Property-level decay must not cause property suppression, property movement, or property deletion from storage.

A node or edge with at least one property-level `NO DECAY` rule is not suppressible. The `NO DECAY` property acts as a suppression anchor: the parent entity's content still decays, but the entity remains visible because the anchored property has a permanent score of 1.0. This prevents structural metadata (tenant IDs, session IDs, bi-temporal timestamps) from being hidden by entity-level decay. Property-level `NO DECAY` on an entity whose binding already has `decayEnabled: false` or entity-level `NO DECAY` is redundant and should be omitted.

If a node remains indexed, its properties remain indexable under ordinary indexing rules. Property-level decay affects retrieval and vectorization behavior, not whether the property exists in storage. Properties that participate in structural indexes (lookup, range, and composite indexes) are entirely immune to decay scoring, decay hiding, and vectorization exclusion — they must remain stable and always visible for aggregation, joining, and lookup. Fulltext indexes and vector indexes are retrieval-surface indexes and do not confer this immunity.

### 6.5 Decay Function Semantics

The engine should support multiple decay function identifiers over time.

Initial supported scoring modes can include:

- `exponential`
- `linear`
- `step`
- `none`

The engine should resolve these as runtime scoring behavior, not as special categories.

These scoring modes should be accepted both:

- from resolved decay profile and constraint configuration, and
- from an explicit Cypher options object on decay scoring functions.

Cypher may override the profile-resolved scoring mode for the scope of that scoring expression only. Unified retrieval should not expose that override surface and should remain profile-resolved.

### 6.6 Promotion and Decay Resolution Order

Promotion policies are evaluated first. The promotion policy for the target is resolved and its `WHEN` predicates are evaluated before decay profile resolution begins. `WHEN` predicates determine which promotion profile applies and what tier the entity is in.

After promotion resolution, the decay profile is resolved and the base decay score is computed. The promotion adjustments are then applied to the base decay score to produce the final score. The final score determines query visibility.

`ON ACCESS` mutations execute **after** the final score and visibility determination. If the entity's final score falls below the visibility threshold (i.e., the entity is suppressed), `ON ACCESS` mutations do **not** execute. This prevents suppressed entities from accumulating access state — a suppressed entity should not record "accesses" that only occurred because the scorer was evaluating it, not because a user or query actually retrieved it. `ON ACCESS` mutations reflect genuine access by a visible entity, not internal scoring housekeeping.

The evaluation order is:

1. Resolve `WHEN` predicates from the matching promotion policy (determines promotion tier)
2. Resolve the decay profile and compute the base decay score
3. Apply promotion adjustments to produce the final score
4. Determine visibility: is the final score above the visibility threshold?
5. **Only if visible:** execute `ON ACCESS` mutations (increment access counts, set timestamps, etc.); for mutations marked `WITH KALMAN`, the raw measurement (a derived behavioral metric such as a confidence score or cross-session access rate) is run through the per-property Kalman filter before storing — the filter's optimal estimate `x` replaces the raw value, smoothing noise in the behavioral signal while tracking genuine trends; session-aware gating via query context variables (e.g., `$_session`, `$_agent` from `X-Query-*` headers) should be applied in the ON ACCESS expression itself before the measurement reaches the filter, to prevent same-session sycophancy loops from biasing the estimate
6. Flush `ON ACCESS` deltas (including updated Kalman filter state and variance tracker state) to accessMeta asynchronously

The normative formula for final score computation is:

```
promotedScore = baseDecayScore × promotionMultiplier
flooredScore  = max(promotedScore, promotionFloor)
cappedScore   = min(flooredScore, promotionCap)
finalScore    = max(cappedScore, decayFloor)
```

Where:

- `baseDecayScore` is the output of the decay function (e.g., `exp(-t * ln(2) / halfLife)`)
- `promotionMultiplier`, `promotionFloor`, `promotionCap` come from the matched promotion profile (defaults: 1.0, 0.0, 1.0)
- `decayFloor` comes from the decay profile's `DECAY FLOOR` directive (default: 0.0)

Order of operations: multiply → floor → cap → decay floor. The decay floor is applied last because it is a hard minimum from the decay profile, independent of promotion.

If no promotion policy matches, `promotionMultiplier = 1.0`, `promotionFloor = 0.0`, `promotionCap = 1.0`, and the formula reduces to `max(baseDecayScore, decayFloor)`.

When multiple `WHEN` predicates match within the same promotion policy, the profile with the highest effective multiplier wins. This is deterministic and does not require an explicit composition directive.

### 6.7 Explainability

For any entity or property, the system should be able to explain:

- whether decay applies
- which decay profile was selected
- which promotion policy matched and which profile was selected
- which score start time was selected
- which decay profile and inline rule selected it
- which promotion policy entry and WHEN predicate selected the profile
- what rate, threshold, floor, and multiplier are active
- whether decay age was measured from `CREATED`, `VERSION`, or `CUSTOM` and which property path was used if `CUSTOM`, whether the value was resolved from accessMeta or stored properties, and whether a fallback to entity creation time occurred due to a null or unparsable value
- why a node or edge was suppressed or not suppressed
- why a node or edge was deindexed or pending deindex
- why a property was excluded from vectorization or retrieval surfaces without being suppressed
- whether a property is immune to decay because it participates in a structural index (lookup, range, or composite)

### 6.8 Native Cypher Access

The decay subsystem should expose scoring through native Cypher functions so callers can inspect resolved scores without altering Neo4j-compatible node or relationship structures.

Proposed functions:

- `decayScore(entity)` returns the effective scalar decay score for a node or edge
- `decayScore(entity, { scoringMode: 'linear' })` returns the effective scalar decay score for a node or edge using the requested scoring mode
- `decayScore(entity, { property: 'summary' })` returns the effective scalar decay score for a specific property on that node or edge
- `decayScore(entity, { property: 'summary', scoringMode: 'step' })` returns the effective scalar decay score for a specific property using the requested scoring mode
- `decay(entity)` returns a structured decay object for the node or edge
- `decay(entity, { scoringMode: 'linear' })` returns a structured decay object for the node or edge using the requested scoring mode
- `decay(entity, { property: 'summary' })` returns a structured decay object for the requested property
- `decay(entity, { property: 'summary', scoringMode: 'step' })` returns a structured decay object for the requested property using the requested scoring mode

The options-object shape avoids ambiguous string overloads. `property` and `scoringMode` are named keys rather than positional string arguments.

The structured `decay(...)` result should always expose a Cypher-accessible `.score` field so callers can write concise expressions without needing a second helper function when they want richer metadata.

Suggested fields on `decay(...)` results:

- `score`
- `policy`
- `scope`
- `function`
- `visibilityThreshold`
- `floor`
- `applies`
- `reason`
- `scoreFrom`

The `decay(...)` object is a derived value. It should not imply that score metadata is being persisted back onto the node, edge, or property itself.

If a caller invokes `decayScore(...)` or `decay(...)` for a target with no matching policy, the function should return the non-decaying/default result rather than failing. The default scalar should be `1.0`, and the structured form should report a neutral non-decaying result.

The existing Cypher scoring API remains unchanged. The score returned by `decayScore(...)` and `decay(...).score` is the final resolved score after applying the decay profile, the profile-declared score start time, and the matching promotion policy.

The promotion policy subsystem should expose accessMeta through a native Cypher function so callers can inspect access-tracking state without altering Neo4j-compatible node or relationship structures.

Proposed function:

- `policy(entity)` returns the accessMeta map for the node or edge as a structured Cypher object

There is no correlated `policyScore()` scalar function. Unlike `decay()` / `decayScore()`, the accessMeta map is a general-purpose key-value store with no single canonical scalar to extract. Callers access individual keys through standard Cypher property access on the returned map, for example `policy(n).accessCount` or `policy(r).traversalCount`.

Suggested fields on `policy(...)` results:

- all keys present in the target's accessMeta entry, projected as a Cypher map
- `targetId`: the target node or edge identifier
- `targetScope`: `node` or `edge`
- `lastAccessedAt`: timestamp of the most recent node access
- `lastMutatedAt`: timestamp of the most recent `ON ACCESS` mutation
- `mutationCount`: total number of `ON ACCESS` mutations applied

Field names match the `AccessMetaEntry` struct fields defined in the [implementation plan](./knowledge-layer-persistence-implementation.md) §1.1 (`pkg/knowledgepolicy/access_meta.go`).

The `policy(...)` object is a derived value read from the accessMeta index. It does not imply that access-tracking metadata is stored on the node or edge itself.

If a caller invokes `policy(...)` for a target with no accessMeta entry, the function should return an empty map with only the `targetId` and `targetScope` fields rather than failing.

The scoring subsystem should expose a bypass function so callers can retrieve the raw stored entity without decay-driven visibility filtering or property hiding.

Proposed function:

- `reveal(entity)` returns the raw stored node or edge as it exists in primary storage, bypassing all scoring-driven visibility suppression and property-level decay hiding

`reveal()` is a plan-level visibility bypass marker, not a runtime function. It does not disable scoring — the entity still has a resolved score. It disables the visibility gate that would otherwise hide the entity or its properties from the query result. `reveal()` is the only mechanism to access entities that are invisible due to scoring. It does not affect `decayScore()`, `decay()`, or `policy()` — those functions still return the resolved values.

When the query planner detects `reveal(variable)` anywhere in the query (RETURN, WITH, WHERE, or ORDER BY), it marks that variable's binding as **visibility-bypassed** during plan compilation. A visibility-bypassed binding skips scoring-driven suppression at MATCH time. The entity is always materialized. Its score is still computed (so `decayScore()` and `decay()` return correct values), but the visibility gate is disabled for that binding. This is equivalent to the planner rewriting `MATCH (m:MemoryEpisode) RETURN reveal(m)` into a plan where `m`'s scan does not apply the visibility filter.

If `reveal()` is used on one variable but not another in the same query, only the revealed variable bypasses visibility. Example: `MATCH (m:MemoryEpisode)-[:EVIDENCES]->(k:KnowledgeFact) RETURN reveal(m), k` — `m` bypasses visibility, `k` does not.

`reveal()` with no downstream usage in the query is a no-op (standard dead-code elimination). `reveal()` wrapping an already-visible entity is a no-op at runtime.

When `reveal()` is used, the returned entity includes all stored properties, including any that would normally be hidden by property-level decay exclusion. The entity appears in query results regardless of its final score.

`reveal()` works on both nodes and edges. It should be usable in `RETURN`, `WITH`, `WHERE`, and any other Cypher clause that accepts an entity expression.

If decay is not enabled or the entity is not subject to any scoring-driven visibility suppression, `reveal()` is a no-op and returns the entity unchanged.

Suppressed properties do not exist as a concept. Properties remain directly queryable in Cypher even when property-level decay excludes them from vectorization or vector-backed retrieval.

Example usage:

```cypher
MATCH (n:SessionRecord)
RETURN n, decayScore(n) AS entityDecayScore
```

```cypher
MATCH (n:SessionRecord)
RETURN n.summary, decayScore(n, {property: 'summary'}) AS summaryDecayScore
```

```cypher
MATCH ()-[r:CO_ACCESSED]-()
RETURN r, decayScore(r) AS edgeDecayScore
```

```cypher
MATCH ()-[r:CO_ACCESSED]-()
RETURN r.signalScore, decayScore(r, {property: 'signalScore'}) AS signalScoreDecay
```

```cypher
MATCH (n:SessionRecord)
RETURN n.summary, n.summary AS stillDirectlyQueryableInCypher
```

```cypher
MATCH (n:SessionRecord)
RETURN n, policy(n) AS accessMeta
```

```cypher
MATCH (n:SessionRecord)
WHERE policy(n).accessCount >= 5
RETURN n, policy(n).accessCount AS accessCount, policy(n).lastMutatedAt AS lastAccessed
```

```cypher
MATCH ()-[r:CO_ACCESSED]-()
RETURN r, policy(r).traversalCount AS traversals, decay(r) AS decayMeta
```

```cypher
// Retrieve a node that may be invisible due to scoring
MATCH (n:SessionRecord {id: $id})
RETURN reveal(n) AS rawNode, decayScore(n) AS score
```

```cypher
// Retrieve all suppressed or hidden nodes with their scores for diagnostics
MATCH (n:SessionRecord)
RETURN reveal(n) AS rawNode, decay(n) AS decayMeta, policy(n) AS accessMeta
```

```cypher
// Bypass property-level hiding to see all stored properties
MATCH ()-[r:CO_ACCESSED]-()
RETURN reveal(r) AS rawEdge, reveal(r).signalScore AS rawSignal
```

Compatibility rule:

- `RETURN n` remains Neo4j-compatible and does not automatically inject decay metadata into the node; however, `n` is subject to scoring-driven visibility — if the entity's score renders it invisible, it will not appear in results unless accessed through `reveal(n)`
- `RETURN r` remains Neo4j-compatible and does not automatically inject decay metadata into the edge; same visibility rules apply
- `RETURN reveal(n)` or `RETURN reveal(r)` bypasses scoring-driven visibility and property hiding, returning the raw stored entity
- callers opt in by returning `decayScore(...)`, `decay(...)`, `policy(...)`, or `reveal(...)` explicitly as additional columns
- property-level scores are therefore visible to Cypher without changing Bolt node or relationship structures
- missing decay profile should behave like ordinary metadata lookup in Cypher: no error, neutral score

Implementation rule:

- Cypher scoring functions should call the same shared runtime scorer used by unified retrieval scoring
- Cypher options objects should be validated against the accepted keys `property` and `scoringMode`
- supported Cypher `scoringMode` values remain: `exponential`, `linear`, `step`, `none`
- unified retrieval should call the same scorer but should not accept a caller-supplied `scoringMode`

### 6.9 Unified Search Metadata

The unified search service should follow the same derived-on-read model as native Cypher.

It should not persist node-, edge-, or property-level decay scores into stored entity fields. Instead, when requested, it should add resolved scoring metadata into a separate response `meta` structure.

Unified retrieval scoring should use the same scorer as Cypher scoring functions, but it should remain profile-and-policy-resolved and should not expose the Cypher-only `scoringMode` override.

The shape should be a keyed object rather than an array of single-entry maps.

Preferred shape:

```json
{
  "scores": {
    "node-id-12": {
      "decay": 0.82,
      "properties": {
        "property1": { "decay": 0.44 },
        "property2": { "decay": 0.91 }
      }
    },
    "edge-id-77": {
      "decay": 0.63,
      "properties": {
        "signalScore": { "decay": 0.28 }
      }
    }
  }
}
```

Suggested conventions:

- top-level key by entity id
- entity-level score at `scores[id].decay`
- property-level scores nested at `scores[id].properties[propertyKey].decay`
- optional richer metadata can be added later beside `decay`, such as `policy`, `reason`, `scope`, or `scoreFrom`
- if no policy applies, `decay` should be reported as `1.0` unless an explicit configured default policy says otherwise

Suggested retrieval scoring inputs:

- options object with optional `property` when scoring needs to target a specific property
- options object may later grow additional explicit keys without breaking call-site semantics
- retrieval callers should not provide `scoringMode`; mode selection comes from the resolved decay profile

The existing unified search metadata shape remains unchanged. Promotion-policy effects and score-start-time effects are reflected in the resolved score value rather than through a new response field, though richer metadata may optionally expose the selected `scoreFrom`.

Suppressed nodes and edges should be excluded from unified retrieval as soon as possible. Property-level exclusions should affect vectorization and vector-backed retrieval only, while stored properties remain directly queryable in Cypher.

When vector search (e.g., `db.retrieve`, `db.index.vector.queryNodes`) returns candidates that are subsequently suppressed by decay visibility, the caller may receive fewer results than the requested LIMIT. To address this, the vector search layer should chunk results based on the LIMIT value and continue pulling additional chunks until the original limit is satisfied or the index is exhausted. This ensures that decay-filtered vector search returns the expected number of visible results.

---

## 7. Policy Subsystem Design

### 7.1 Policy Statements

Decay behavior should be authored through a dedicated decay profile subsystem, not through first-class constraints. Decay profiles are the only decay authoring surface — there is no separate decay policy concept.

Promotion behavior should be authored through dedicated promotion profile and promotion policy subsystems, not through first-class constraints.

`NO DECAY`, `DECAY HALF LIFE`, `DECAY VISIBILITY THRESHOLD`, `DECAY PROFILE`, and `DECAY FLOOR` are decay directives inside decay profile APPLY blocks. They are not standalone constraint types. Decay function and scope are declared through `OPTIONS { ... }` on the profile definition itself, not as inline APPLY-block directives.

`APPLY PROFILE` is the promotion directive inside promotion policy APPLY blocks. It is not a standalone constraint type. Multiplier, score floor, and score cap are declared through `OPTIONS { ... }` on the profile definition itself, not as inline APPLY-block directives.

Score start time is declared through decay profile `OPTIONS { ... }`, not through new standalone syntax.

Suggested decay DDL surface:

- `CREATE DECAY PROFILE`
- `ALTER DECAY PROFILE`
- `DROP DECAY PROFILE`
- `SHOW DECAY PROFILES`

Suggested promotion DDL surface:

- `CREATE PROMOTION PROFILE`
- `ALTER PROMOTION PROFILE`
- `DROP PROMOTION PROFILE`
- `SHOW PROMOTION PROFILES`
- `CREATE PROMOTION POLICY`
- `ALTER PROMOTION POLICY`
- `DROP PROMOTION POLICY`
- `SHOW PROMOTION POLICIES`

These are NornicDB extensions, not Neo4j compatibility targets.

### 7.2 Valid Targets

Decay profiles and promotion policies should be valid on:

- node labels
- edge types
- wildcard `*` meaning all node labels or all edge types
- inline property paths on nodes within a profile or policy body
- inline property paths on edges within a profile or policy body

The wildcard `*` target applies the profile or policy to every node label or every edge type in the database. A wildcard target is specified using `FOR (n:*)` for nodes or `FOR ()-[r:*]-()` for edges. Wildcard targeting is only valid on `CREATE` statements for decay profiles and promotion policies — it cannot be used inline within APPLY blocks or on ALTER statements.

A label-specific or edge-type-specific profile or policy takes precedence over a wildcard-targeted one. The wildcard acts as the default for any target that does not have its own explicit profile or policy.

Edges are first-class targets and must support the same lifecycle as nodes, including creation, alteration, introspection, resolution, scoring, and suppression behavior.

Properties are valid scoring targets, but not suppression targets.

Properties that participate in structural indexes (lookup indexes, range indexes, and composite indexes) are not valid decay or promotion targets. Fulltext indexes and vector indexes are retrieval-surface indexes and do not confer property immunity. If an inline property-level rule in a decay profile or promotion policy targets a property participating in a structural index, the engine should reject the rule at creation time with a validation error. This constraint ensures that structurally indexed properties remain stable and always visible for index-backed operations.

There can be at most one decay profile and one promotion policy per unique target. Competing or overlapping definitions for the same target are a hard constraint violation. A wildcard target does not conflict with a label-specific or edge-type-specific target — the specific target wins. Two wildcard-scoped definitions for the same scope (node or edge) do conflict. If an operator needs different decay or promotion behavior for a different label or edge type, they must create a separate targeted profile or policy.

### 7.3 Profile Semantics

Promotion profiles are named parameter bundles. They contain no logic — no `FOR` targets, no `APPLY` blocks, no `WHEN` predicates, no `ON ACCESS` mutations. A promotion profile declares configuration values: multiplier, score floor, score cap, and scope. Promotion profiles cannot be targeted to entities directly — they are only referenced by name inside promotion policy APPLY blocks via `APPLY PROFILE 'name'`.

Decay profiles are always targeted. Every decay profile must include `FOR` and `APPLY` clauses that bind the profile to a specific node label, edge type, or wildcard target. A decay profile may reference a named parameter bundle via `DECAY PROFILE '<bundle_name>'` inside its APPLY block, or it may declare decay directives inline (e.g., `DECAY HALF LIFE`, `DECAY VISIBILITY THRESHOLD`, `NO DECAY`).

Parameter bundles are reusable configuration objects created with `CREATE DECAY PROFILE <name> OPTIONS { ... }` (no `FOR` clause). They declare configuration values only — half-life, scoring function, score start time, visibility threshold, score floor, and scope. Parameter bundles are not directly targeted to entities; they exist to be referenced by name inside targeted decay profiles. A parameter bundle without a `FOR` clause is inert until referenced.

Internally, parameter bundles and targeted bindings are stored in separate maps in the schema manager: `decayProfileBundles map[string]*DecayProfileBundle` and `decayProfileBindings map[string]*DecayProfileBinding`. This avoids runtime type-switching.

There is no separate decay policy concept — decay profiles are the only decay authoring surface.

Profiles may be altered, dropped, and introspected independently. Dropping a promotion profile that is still referenced by an active promotion policy should produce a validation error. Dropping a decay profile parameter bundle that is still referenced by an active targeted binding should produce a validation error.

If decay is globally disabled, decay profiles still exist in schema but are operationally inert until the subsystem is enabled.

If promotion is globally disabled, promotion profiles and promotion policies still exist in schema but are operationally inert until the subsystem is enabled.

### 7.4 Policy Semantics

Policies exist only on the promotion side. There is no separate decay policy concept.

Promotion policies contain logic. They declare a target via `FOR`, contain an `APPLY` block, and may include `WHEN` predicates, `ON ACCESS` mutation blocks, and inline property-level rules. Promotion policies bind promotion profiles to specific node labels, edge types, and property paths through `WHEN` predicates and `APPLY PROFILE` references.

Promotion policies may include an `ON ACCESS` block that executes mutations when the target entity is accessed and passes the suppression gate. `WHEN` predicates are evaluated first to determine the entity's promotion tier. The decay profile is then resolved to compute the final score and visibility. `ON ACCESS` mutations execute only if the entity's final score is above the visibility threshold — suppressed entities do not accumulate access state. This means `WHEN` predicates cannot depend on access counts from the current evaluation cycle; they read the persisted + buffered accessMeta state from prior accesses.

`ON ACCESS` mutations are applied exclusively to a separate accessMeta index, never to the target node or edge itself. The target node or edge is read-only during `ON ACCESS` evaluation. The accessMeta index stores a `map[string]interface{}` per target, keyed to the target node or edge identifier, and is serialized in msgpack alongside other data files for performance.

Property resolution within `ON ACCESS` blocks and `WHEN` predicates uses accessMeta-first semantics: a property read such as `n.accessCount` resolves from the target's accessMeta entry first, and falls back to the target's stored node or edge properties only when the key is not present in accessMeta. All writes such as `SET n.accessCount = ...` mutate the accessMeta entry for the target, not the target's stored properties. This means `ON ACCESS` Cypher syntax is unchanged — `n.propertyKey` and `r.propertyKey` work as expected — but the storage destination is the accessMeta index.

#### WITH KALMAN Behavioral Signal Smoother

##### Design Constraints

`WITH KALMAN` is a behavioral signal smoother, not a truth estimator. Three constraints govern its use:

1. **Kalman stabilizes behavioral signals, not truth.** The filter is appropriate for derived metrics — access rates, confidence scores from LLM evaluations, traversal velocity — where the underlying signal is noisy and the system is estimating a latent behavioral state. It must not be used to estimate ground-truth values. Labels that represent canonical facts (e.g., `:KnowledgeFact`, `:WisdomDirective`, or any label the operator designates as ground truth) should either not have a promotion policy with `WITH KALMAN` mutations, or should use `NO DECAY` on their decay profile so they are never subject to behavioral scoring. The truth-immunity is enforced generically by `FOR` targeting — if a label has no promotion policy with `WITH KALMAN` mutations, no Kalman filtering occurs. If a label has `NO DECAY`, it is truth-immune at the scoring layer. No label is hardcoded as special. Truth-immune labels are always visible (`score = 1.0` via `NO DECAY`) but must never receive promotion multipliers that boost them above other nodes — their relevance comes from semantic matching at retrieval time, not from access-pattern promotion. A truth-immune label should have no promotion policy, or at most a promotion policy with `multiplier: 1.0` and no score floor override. This prevents the canonical graph layer from drowning out episodic content that has earned legitimate behavioral reinforcement through cross-session access patterns.

2. **Session-aware gating before filtering.** LLM-driven access noise is not Gaussian — it is adversarial, bursty, and self-reinforcing within a session. A Kalman filter will smooth transient spikes, but it cannot correct systematic bias from a sycophancy loop where the same agent repeatedly accesses the same entity within a single session. To address this, `ON ACCESS` blocks and `WHEN` predicates have access to **query context variables** projected from request headers. The gating is expressed in the `ON ACCESS` block itself, not hardcoded in the engine:

   ```cypher
   ON ACCESS {
     -- Only increment if this is a new session (cross-session reinforcement)
     WITH KALMAN SET n.crossSessionAccessRate =
       CASE WHEN n._lastSessionId <> $_session
         THEN coalesce(n.crossSessionAccessRate, 0) + 1
         ELSE n.crossSessionAccessRate
       END
     SET n._lastSessionId = $_session
     SET n._lastAgentId = $_agent
     SET n.lastAccessedAt = timestamp()
   }
   ```

   ##### Query Context Variable Passthrough

   The engine projects all `X-Query-*` HTTP headers (and Bolt metadata keys with prefix `query_`) into the query evaluation context as `$_<key>` variables. The header name is lowercased and the `X-Query-` prefix is stripped. Examples:

   | Request Header / Bolt Metadata | Available As | Purpose |
   |-------------------------------|-------------|---------|
   | `X-Query-Session` / `query_session` | `$_session` | Same-session deduplication |
   | `X-Query-Agent` / `query_agent` | `$_agent` | Agent-aware gating |
   | `X-Query-Tenant` / `query_tenant` | `$_tenant` | Multi-tenant isolation |
   | `X-Query-Experiment` / `query_experiment` | `$_experiment` | A/B testing, experiment tracking |
   | Any `X-Query-<Key>` | `$_<key>` | User-defined context |

   Query context variables are:
   - **Read-only** — available inside `ON ACCESS` blocks, `WHEN` predicates, and general Cypher queries, but not writable.
   - **Not stored** — they exist for the duration of the query evaluation. They are ephemeral request context, not graph state.
   - **Generic** — the engine does not decide which provenance matters. Operators define their own gating logic using whatever context they send. `$_session` and `$_agent` are conventions, not special cases.
   - **Default to empty string** — if a header is not provided in the request, the corresponding variable resolves to `""`. This means same-session gating expressions evaluate conservatively (an empty `$_session` always differs from a previously stored session ID).

   This generic passthrough keeps the engine unopinionated about what context matters for gating. The operator decides what to send in headers and what to gate on in their `ON ACCESS` expressions.

3. **Multi-signal modeling, not scalar counters.** Kalman filtering a raw monotonic counter (e.g., `accessCount += 1`) is a misuse of the filter — the counter has no noise to smooth and no latent state to estimate. `WITH KALMAN` is designed for derived metrics where the input signal has genuine measurement noise: confidence scores, agreement ratios, relevance assessments, access rates, and other quantities where the observed value fluctuates around a latent true state. Raw counters and timestamps should use plain `SET` without `WITH KALMAN`.

##### Filter Semantics

Individual `SET` expressions within an `ON ACCESS` block may be prefixed with a `WITH KALMAN` modifier. When present, the raw measurement value (the RHS expression result) is run through a per-property Kalman filter before being written to accessMeta. The stored value is the filter's optimal state estimate — a smoother that dampens noisy measurements while tracking genuine trends in the behavioral signal.

The Kalman filter algorithm is the same fast scalar Kalman from the imu-f flight controller (`docs/plans/kalman.md`): velocity-based state projection, setpoint error boosting, and gain-limited measurement update. It is inlined in the accumulator for zero-allocation hot-path performance (~20 lines of pure scalar math).

Three modes are supported:

- `WITH KALMAN` — auto mode. R (measurement noise) is continuously adjusted by a per-property variance tracker using a sliding window (default window size: 32). This is a direct port of `update_kalman_covariance` from the flight controller. Q (process noise) defaults to `0.0001` (0.1 × 0.001 scaling). R is seeded at `88.0` and adapts from there. As measurement noise decreases, R decreases and the filter becomes more responsive. As noise increases, R increases and the filter dampens harder.
- `WITH KALMAN{q: <val>, r: <val>}` — manual mode. Fixed Q and R, no variance tracker. For operators who know their signal characteristics.
- `WITH KALMAN{q: <val>}` — hybrid mode. User-set Q, R auto-adjusted by the variance tracker.

Additional optional parameters: `varianceScale` (default: 10.0) and `windowSize` (default: 32). See the [implementation plan](./knowledge-layer-persistence-implementation.md) §1.3 and §3.3.1 for the full DDL parsing rules, default values, and Kalman accumulator internals.

Kalman filter state is persisted as a typed `KalmanFilters map[string]*KalmanPropertyState` field on `AccessMetaEntry`, keyed by property name (e.g., `"confidenceScore"`, `"crossSessionAccessRate"`). Each `KalmanPropertyState` contains the filtered value, the full `KalmanFilterState` (gain, covariance, observations), and the `VarianceTrackerState` (auto mode only, sliding window + variance). This is a type-safe struct field — not stringly-typed keys in the overflow map. The `policy()` Cypher function exposes `kalmanFilters` as part of the accessMeta result for diagnostics (e.g., `policy(n).kalmanFilters.confidenceScore.filteredValue`, `policy(n).kalmanFilters.confidenceScore.filter.k` for gain).

When `WHEN` predicates or `policy()` read a Kalman-filtered property, they see the filter's optimal estimate (`x`), not the raw measurement. The raw measurement is not stored — only the filtered value. This is the smoother-only design: all measurements are accepted, but the stored value is always the Kalman's best estimate.

##### Why Kalman Works for Behavioral Signals

The filter's anti-noise properties come from the flight controller algorithm:

- **Velocity-based prediction** (`x += x - lastX`) projects the state forward. A genuine trend in the behavioral signal continues; a hallucinated spike deviates from the trajectory.
- **Gain limiting** (`K = P / (P + R)`) — when R is high (noisy measurements), K is low, and a single wild measurement barely moves the estimate. The filter trusts its prediction over a single outlier.
- **Auto-R via variance tracking** (auto mode) — the `VarianceTracker` computes `R = sqrt(sample_variance) * VARIANCE_SCALE` over a sliding window. As the measurement stream becomes noisier, R increases and the filter dampens harder. A single outlier in a stable stream has minimal effect.

This is analogous to filtering noisy gyro readings on a flight controller — transient spikes should not corrupt the estimated state. However, unlike gyro noise, LLM-driven access patterns can exhibit systematic bias (sycophancy loops). The Kalman filter alone cannot correct bias — it can only smooth variance. Bias correction is handled by the session-aware gating layer (constraint 2 above), which deduplicates same-session accesses before the measurement reaches the filter. The two layers work together: gating removes bias, Kalman removes variance.

##### Access History for Diagnostics

The variance tracker's sliding window (default: 32 samples) provides a built-in access history for each Kalman-filtered property. This window is persisted as part of the `KalmanPropertyState.Variance` field on `AccessMetaEntry.KalmanFilters` and is accessible through `policy()` for diagnostics (e.g., `policy(n).kalmanFilters.confidenceScore.variance.w` for the raw window values). Operators can inspect the last N measurements, the current variance, and the filter's covariance to understand how the behavioral signal has evolved over time.

`SET` expressions without `WITH KALMAN` behave exactly as before — plain delta accumulation with no filtering.

There can be at most one promotion policy per unique target. Competing or overlapping promotion policies for the same target are a hard constraint violation.

Property-level promotion rules should be authored inline within the same promotion policy body that declares the entity-level promotion behavior for that label or edge type.

Property-level retention rules should be authored inline within the same decay profile body that declares the entity-level defaults for that label or edge type.

When a targeted decay profile's APPLY block contains `DECAY VISIBILITY THRESHOLD`, that value overrides the `visibilityThreshold` declared in the referenced parameter bundle's OPTIONS. When omitted, the parameter bundle's value is inherited. The resolution precedence is: APPLY block override → referenced profile OPTIONS → configured system default.

Nested `FOR ... APPLY` entries should remain invalid inside a profile or policy body. If operators need different decay or promotion behavior for a different label or edge type, they must create a separate targeted profile or policy.

When property-level retention or promotion rules exist, the runtime should make the resolved score available through `decayScore(entity, {property: propertyKey})` and `decay(entity, {property: propertyKey})` even if the underlying Bolt result only returns the base node or edge structure.

The same shared scorer should also back retrieval scoring, using the scoring mode resolved from the decay profile, the score start time resolved from the decay profile, and the promotion adjustments resolved from the promotion policy rather than a caller override.

Property-level rules may exclude properties from vectorization or vector-backed retrieval, but they do not suppress, move, or delete properties from storage.

### 7.5 Sample Policies in Cypher

#### Node-level default policy with inline property rules

```cypher
CREATE DECAY PROFILE session_record_retention
FOR (n:SessionRecord)
APPLY {
  DECAY PROFILE 'working_memory'
  DECAY VISIBILITY THRESHOLD 0.10
  n.summary DECAY PROFILE 'session_summary'
  n.lastConversationSummary DECAY HALF LIFE 2592000
  n.tenantId NO DECAY
}
```

#### Edge-level default policy

```cypher
CREATE DECAY PROFILE coaccess_edge_retention
FOR ()-[r:CO_ACCESSED]-()
APPLY {
  DECAY PROFILE 'edge_working_memory'
  DECAY VISIBILITY THRESHOLD 0.15
  r.externalId NO DECAY
}
```

#### Edge-level custom rate and property rules

```cypher
CREATE DECAY PROFILE coaccess_retention
FOR ()-[r:CO_ACCESSED]-()
APPLY {
  DECAY HALF LIFE 1209600
  r.signalScore DECAY HALF LIFE 1209600
  r.signalScore DECAY FLOOR 0.15
  r.externalId NO DECAY
}
```

#### Edge-level no-decay

```cypher
CREATE DECAY PROFILE canonical_link_retention
FOR ()-[r:CANONICAL_LINK]-()
APPLY {
  NO DECAY
  r.externalId NO DECAY
  r.sourceSystem NO DECAY
}
```

#### Property-only override inside an edge block

```cypher
CREATE DECAY PROFILE review_link_retention
FOR ()-[r:REVIEWED_WITH]-()
APPLY {
  DECAY HALF LIFE 604800
  r.confidence DECAY HALF LIFE 86400
  r.confidence DECAY FLOOR 0.25
}
```

#### Node-level promotion tiers with promotion policy

```cypher
CREATE PROMOTION POLICY session_record_tiering
FOR (n:SessionRecord)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
    SET n.lastAccessedAt = timestamp()
  }

  WHEN n.accessCount >= 3
    APPLY PROFILE 'reinforced_tier'

  WHEN n.accessCount >= 5 AND n.sourceAgreement >= 0.95
    APPLY PROFILE 'canonical_tier'

}
```

#### Node-level promotion with Kalman-filtered behavioral signals (anti-sycophancy)

```cypher
CREATE PROMOTION POLICY episodic_recall_quality
FOR (n:MemoryEpisode)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
    SET n.lastAccessedAt = timestamp()

    -- Kalman-filter derived behavioral metrics, not raw counters.
    -- confidenceScore is an LLM-evaluated assessment — noisy, not monotonic.
    WITH KALMAN{q: 0.05, r: 50.0} SET n.confidenceScore = $evaluatedConfidence

    -- Cross-session access rate: gated by session provenance before filtering.
    -- Only increments when a NEW session accesses this entity, then Kalman-smoothed.
    WITH KALMAN SET n.crossSessionAccessRate =
      CASE WHEN n._lastSessionId <> $_session
        THEN coalesce(n.crossSessionAccessRate, 0) + 1
        ELSE n.crossSessionAccessRate
      END
    SET n._lastSessionId = $_session
    SET n._lastAgentId = $_agent
  }

  WHEN n.accessCount >= 5 AND n.confidenceScore >= 0.8
    APPLY PROFILE 'high_confidence_tier'

  WHEN n.accessCount >= 3
    APPLY PROFILE 'reinforced_tier'

}
```

In this example, `n.confidenceScore` is a value written by an LLM during retrieval evaluation — a noisy behavioral signal, not a monotonic counter. Without `WITH KALMAN`, a single hallucinated confidence of 0.99 on a mediocre memory would immediately promote it to `high_confidence_tier`. With `WITH KALMAN{q: 0.05, r: 50.0}`, the Kalman filter dampens the spike — the stored value moves slowly toward 0.99 only if subsequent evaluations consistently agree. A single outlier barely moves the estimate.

The `crossSessionAccessRate` demonstrates session-aware gating: the `CASE WHEN n._lastSessionId <> $_session` expression ensures that repeated accesses from the same session (sycophancy loops) do not inflate the rate. Only genuinely new sessions contribute measurements to the Kalman filter, which then smooths cross-session access velocity.

Raw counters (`accessCount`) and timestamps (`lastAccessedAt`) use plain `SET` — they are monotonic and have no noise to smooth.

#### Edge-level promotion tiers

```cypher
CREATE PROMOTION POLICY coaccess_signal_tiering
FOR ()-[r:CO_ACCESSED]-()
APPLY {
  ON ACCESS {
    SET r.traversalCount = coalesce(r.traversalCount, 0) + 1
    SET r.lastTraversedAt = timestamp()

    -- Kalman-smooth the cross-session support signal (noisy, session-gated)
    WITH KALMAN SET r.crossSessionSupport =
      CASE WHEN r._lastSessionId <> $_session
        THEN coalesce(r.crossSessionSupport, 0) + 1
        ELSE r.crossSessionSupport
      END
    SET r._lastSessionId = $_session
  }

  WHEN r.traversalCount >= 10
    APPLY PROFILE 'reinforced_edge_tier'

  WHEN r.traversalCount >= 50 AND r.crossSessionSupport >= 3
    APPLY PROFILE 'canonical_edge_tier'

}
```

#### Edge property-level promotion tiers

```cypher
CREATE PROMOTION POLICY coaccess_signal_property_tiering
FOR ()-[r:CO_ACCESSED]-()
APPLY {
  ON ACCESS {
    SET r.signalAccessCount = coalesce(r.signalAccessCount, 0) + 1
  }

  r.signalScore WHEN r.signalAccessCount >= 10
    APPLY PROFILE 'reinforced_signal_tier'

  r.signalScore WHEN r.signalAccessCount >= 50 AND r.crossSessionSupport >= 3
    APPLY PROFILE 'canonical_signal_tier'

}
```

#### Property-level vectorization exclusion without suppression

```cypher
CREATE DECAY PROFILE session_vectorization_rules
FOR (n:SessionRecord)
APPLY {
  n.summary DECAY PROFILE 'session_summary'
  n.lastConversationSummary DECAY HALF LIFE 2592000
}
```

#### Wildcard decay profile for all nodes

```cypher
CREATE DECAY PROFILE default_node_retention
FOR (n:*)
APPLY {
  DECAY PROFILE 'working_memory'
  DECAY VISIBILITY THRESHOLD 0.05
}
```

#### Wildcard decay profile for all edges

```cypher
CREATE DECAY PROFILE default_edge_retention
FOR ()-[r:*]-()
APPLY {
  DECAY PROFILE 'edge_working_memory'
  DECAY VISIBILITY THRESHOLD 0.10
}
```

#### Wildcard promotion policy for all nodes

```cypher
CREATE PROMOTION POLICY default_node_promotion
FOR (n:*)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
  }

  WHEN n.reinforcementCount >= 3
    APPLY PROFILE 'reinforced_tier'
}
```

Wildcard targets use `*` in place of a label or edge type to match every node label or every edge type. A label-specific or edge-type-specific profile or policy always takes precedence over a wildcard-targeted one. Wildcards are only valid on `CREATE` statements.

In the promotion policy examples above, `ON ACCESS` SET statements such as `SET n.accessCount = coalesce(n.accessCount, 0) + 1` write exclusively to the accessMeta index for the target node or edge, not to the node or edge itself. The `coalesce(n.accessCount, 0)` read resolves from accessMeta first, falling back to the node's stored properties only when the key is absent from accessMeta. This keeps nodes and edges read-only during policy evaluation while preserving familiar Cypher property syntax.

When `WITH KALMAN` is present on a `SET` expression, the raw RHS expression result is run through a per-property Kalman filter before storage. The filter's optimal estimate replaces the raw value, providing behavioral signal smoothing. The `WITH KALMAN` modifier is optional and per-expression, so a single `ON ACCESS` block can mix filtered and unfiltered mutations. The correct pattern is: `WITH KALMAN` on derived behavioral metrics (confidence scores, cross-session rates, agreement ratios) and plain `SET` on raw counters and timestamps. Raw monotonic counters have no noise to smooth and no latent state to estimate — Kalman filtering them is a misuse of the filter. Query context variables from `X-Query-*` request headers (e.g., `$_session`, `$_agent`, `$_tenant`) are available inside `ON ACCESS` blocks for session-aware gating before measurements reach the filter.

In this model, edges can decay just like nodes can. They can also have independent promotion policies and property-level overrides. Properties can be excluded from vectorization or vector-backed retrieval by profile, but they are never suppressed from storage.

### 7.6 Profile and Policy DDL

Decay profiles should be first-class database objects in a dedicated subsystem, not ordinary graph nodes and not first-class constraints.

Promotion profiles and promotion policies should be first-class database objects in a dedicated subsystem, not ordinary graph nodes and not first-class constraints. Profiles are parameter bundles; policies contain logic and reference profiles.

Operators should be able to define decay profiles independently and bind them to targets via `FOR` selectors in the profile statement itself.

Operators should be able to define promotion profiles independently. Promotion profiles are referenced by name inside promotion policy APPLY blocks — they are not directly targeted to entities.

Operators should be able to define promotion policies independently and bind them to targets via `FOR` selectors in the policy statement itself.

Example decay profile bootstrap with declarative score start time:

```cypher
CREATE DECAY PROFILE durable_fact
OPTIONS {
  decayEnabled: false,
  visibilityThreshold: 0.0,
  scope: 'NODE',
  function: 'none',
  scoreFrom: 'CREATED'
}

CREATE DECAY PROFILE durable_edge
OPTIONS {
  decayEnabled: false,
  visibilityThreshold: 0.0,
  scope: 'EDGE',
  function: 'none',
  scoreFrom: 'CREATED'
}

CREATE DECAY PROFILE session_summary
OPTIONS {
  halfLifeSeconds: 1209600,
  visibilityThreshold: 0.10,
  scope: 'PROPERTY',
  function: 'exponential',
  scoreFrom: 'VERSION'
}

CREATE DECAY PROFILE working_memory
OPTIONS {
  halfLifeSeconds: 604800,
  visibilityThreshold: 0.05,
  scope: 'NODE',
  function: 'exponential',
  scoreFrom: 'VERSION'
}

CREATE DECAY PROFILE edge_working_memory
OPTIONS {
  halfLifeSeconds: 604800,
  visibilityThreshold: 0.05,
  scope: 'EDGE',
  function: 'exponential',
  scoreFrom: 'VERSION'
}

CREATE DECAY PROFILE event_anchored
OPTIONS {
  halfLifeSeconds: 2592000,
  visibilityThreshold: 0.05,
  scope: 'NODE',
  function: 'exponential',
  scoreFrom: 'CUSTOM',
  scoreFromProperty: 'eventTimestamp'
}
```

Example promotion profile bootstrap:

```cypher
CREATE PROMOTION PROFILE reinforced_tier
OPTIONS {
  scope: 'NODE',
  multiplier: 1.20,
  scoreFloor: 0.0,
  scoreCap: 1.0
}

CREATE PROMOTION PROFILE reinforced_edge_tier
OPTIONS {
  scope: 'EDGE',
  multiplier: 1.20,
  scoreFloor: 0.0,
  scoreCap: 1.0
}

CREATE PROMOTION PROFILE canonical_tier
OPTIONS {
  scope: 'NODE',
  multiplier: 1.35,
  scoreFloor: 0.85,
  scoreCap: 1.0
}

CREATE PROMOTION PROFILE canonical_edge_tier
OPTIONS {
  scope: 'EDGE',
  multiplier: 1.35,
  scoreFloor: 0.85,
  scoreCap: 1.0
}

CREATE PROMOTION PROFILE reinforced_signal_tier
OPTIONS {
  scope: 'PROPERTY',
  multiplier: 1.15,
  scoreFloor: 0.0,
  scoreCap: 1.0
}

CREATE PROMOTION PROFILE canonical_signal_tier
OPTIONS {
  scope: 'PROPERTY',
  multiplier: 1.30,
  scoreFloor: 0.0,
  scoreCap: 1.0
}
```

Suggested follow-on DDL:

```cypher
SHOW DECAY PROFILES
```

```cypher
SHOW PROMOTION PROFILES
```

```cypher
SHOW PROMOTION POLICIES
```

```cypher
ALTER DECAY PROFILE working_memory
SET OPTIONS {
  halfLifeSeconds: 432000,
  visibilityThreshold: 0.08,
  scoreFrom: 'VERSION'
}
```

```cypher
ALTER DECAY PROFILE durable_fact
SET OPTIONS {
  decayEnabled: false,
  function: 'none',
  scoreFrom: 'CREATED'
}
```

```cypher
ALTER PROMOTION PROFILE canonical_tier
SET OPTIONS {
  multiplier: 1.30,
  scoreFloor: 0.80
}
```

```cypher
DROP DECAY PROFILE session_summary
```

```cypher
DROP PROMOTION PROFILE canonical_tier
```

---

## 8. Cypher, Search, and Storage Changes

### Suggested Cypher additions

- native scalar function: `decayScore(entity[, options])`
- native structured function: `decay(entity[, options])`
- native structured function: `policy(entity)` — returns the accessMeta map for the target node or edge; no correlated `policyScore()` scalar function
- native bypass function: `reveal(entity)` — returns the raw stored node or edge, bypassing scoring-driven visibility suppression and property-level decay hiding
- `decayScore(...)`, `decay(...)`, and `reveal(...)` should work for nodes and edges
- `policy(...)` should work for nodes and edges
- `decay(...).score` should be the canonical Cypher-visible field for downstream sorting, filtering, and projection
- `policy(...)` keys are accessed through standard Cypher map property access, for example `policy(n).accessCount`
- `decayScore(...)` and `decay(...)` derive scores from the shared resolver rather than reading persisted property-level score fields
- `policy(...)` reads from the accessMeta index rather than from stored node or edge properties
- `decayScore(...)` and `decay(...)` accept an explicit Cypher options object with keys such as `property` and `scoringMode`

### Suggested storage rules

- decay eligibility and rate are resolved from decay profiles and their compiled policy entries, not a baked-in tier enum
- promotion tier effects are resolved from promotion policies and the profiles they reference, not a baked-in tier enum
- score start time is resolved from decay profile options, not from runtime defaults alone
- node-level and edge-level decay are both first-class resolved behaviors
- property-level decay scores are derived on demand and are not written into the entity's stored property map
- suppressed nodes and suppressed edges are persisted as suppressed state in primary storage
- suppressed nodes and suppressed edges are removed from secondary indexing asynchronously
- each node and edge should maintain an exact index-entry catalog to support direct deindex deletes
- deindex cleanup should use batched blind deletes against known index keys rather than scanning indexes to discover stale entries
- the deindex cleanup job should default to nightly execution but be configurable in seconds
- properties are never suppressed, moved, or deleted because of decay profile
- property-level decay may exclude properties from vectorization or vector-backed retrieval, but stored properties remain directly queryable in Cypher via `reveal()`
- properties that participate in structural indexes (lookup, range, and composite indexes) are immune to decay scoring, decay hiding, and property-level exclusion; they remain stable and always visible for aggregation, joining, and lookup; fulltext and vector indexes do not confer immunity
- property-level decay or promotion rules targeting indexed properties must be rejected at creation time
- scoring is evaluated before query visibility; promotion policies are resolved first, then decay profiles, and the final score determines whether the entity appears in query results
- entities whose final score renders them invisible are suppressed from `MATCH`, `WHERE`, search hits, and traversal paths unless accessed through `reveal()`
- `reveal()` bypasses scoring-driven visibility and property hiding but does not disable scoring itself
- accessMeta entries are persisted in a separate index keyed to the target node or edge, serialized in msgpack alongside other data files for performance
- `ON ACCESS` mutations write exclusively to the accessMeta index; nodes and edges are read-only during `ON ACCESS` evaluation
- property reads within `ON ACCESS` blocks and `WHEN` predicates resolve from accessMeta first, falling back to stored node or edge properties
- temporary caches of resolved scores are allowed as implementation detail, but they are not the source of truth
- profile and policy resolution artifacts should be diagnosable without mutating the underlying entity
- no-decay profiles should be enforced consistently across recall, suppression pass, and maintenance paths
- derived score changes must not create new stored versions solely because time advanced

### Suggested search response behavior

- unified search may return node-, edge-, and property-level decay metadata additively in a separate `meta` section
- the `meta` section should mirror the same resolved scores available through `decayScore()` and `decay()`
- search hits themselves remain standard result entities plus ordinary ranking fields
- unified retrieval scoring should call the same shared scorer implementation as Cypher, but without exposing the Cypher-only `scoringMode` override
- suppressed nodes and edges must be skipped efficiently during retrieval, even before background deindexing fully drains
- property-level decay exclusions should apply to vectorization and vector-backed retrieval, not to storage visibility or direct Cypher access

---

## 9. Implementation Workstreams

### Workstream A: Profile and Policy Model

Deliverables:

- define the decay profile schema model
- define the promotion profile and promotion policy schema models
- define supported decay functions and thresholds
- define supported promotion multipliers
- define supported score start-time values `CREATED`, `VERSION`, and `CUSTOM`
- define explainable resolution output
- define whole-node and whole-edge suppression semantics
- define non-suppression property exclusion semantics for vectorization and retrieval
- define indexed-property immunity: properties in structural indexes (lookup, range, and composite) are immune to decay scoring, hiding, and exclusion; fulltext and vector indexes do not confer immunity
- define the native `decayScore()` and `decay()` Cypher function contracts, including explicit `property` and `scoringMode` options
- define the native `policy()` Cypher function contract for accessMeta retrieval
- define the native `reveal()` Cypher function contract for bypassing scoring-driven visibility and property hiding
- define scoring-before-visibility semantics: promotion first, then decay, then visibility determination
- define the accessMeta schema model: `map[string]interface{}` keyed per target node or edge, serialized in msgpack
- define accessMeta-first property resolution semantics for `ON ACCESS` and `WHEN` evaluation
- define the derived search metadata contract for node-, edge-, and property-level scores

### Workstream B: Policy Authoring and Compilation

Deliverables:

- implement decay profile compilation for decay-aware entries
- implement promotion policy compilation for promotion-aware entries
- compile `scoreFrom` from decay profile `OPTIONS { ... }`
- support node-, edge-, and property-targeted policies
- validate creation-time behavior and introspection
- reject property-level decay or promotion rules that target properties participating in structural indexes (lookup, range, and composite) at creation time

### Workstream C: Shared Resolver

Deliverables:

- introduce a shared decay profile resolver
- introduce a shared promotion policy resolver
- support configurable decay half-lives and named presets
- support configurable promotion multipliers and named presets
- support `CREATED`, `VERSION`, and `CUSTOM` score-start resolution from decay profile
- define precedence and conflict rules for overlapping inline block entries
- expose an explainable resolution trace for any effective policy
- make resolved node-, edge-, and property-level scores available to native Cypher functions
- make the same resolved scores available to unified search metadata without persisting them into entity fields
- centralize profile-and-policy-resolved scoring so Cypher and unified retrieval call the same scorer
- keep Cypher-only scoring-mode override handling at the function surface while leaving unified retrieval profile-and-policy-resolved
- make accessMeta entries available to the native `policy()` Cypher function
- implement accessMeta-first property resolution for `ON ACCESS` reads and `WHEN` predicate evaluation

### Workstream D: Runtime Integration

Deliverables:

- route recall, recalc, suppression pass, ranking, and stats paths through the shared resolver
- remove hardcoded tier branching from runtime code
- support property-level decay behavior
- support edge-level decay behavior
- support property-level promotion behavior
- support edge-level promotion behavior
- support MVCC-aware decay-age evaluation using profile-declared `CREATED`, `VERSION`, or `CUSTOM`
- implement scoring-before-visibility: resolve promotion first, then decay, then determine query visibility before exposing entities to the query
- implement scoring-driven visibility suppression for nodes, edges, and properties in `MATCH`, `WHERE`, search, and traversal paths
- implement `reveal()` bypass path that returns raw stored entities without scoring-driven visibility filtering or property hiding
- support whole-node and whole-edge suppression marking
- support fast suppressed-entity skipping in read paths
- implement accessMeta index storage with msgpack serialization alongside other data files
- route `ON ACCESS` mutation writes to the accessMeta index, keeping nodes and edges read-only during policy evaluation
- implement accessMeta-first read resolution for property access within `ON ACCESS` blocks and `WHEN` predicates

### Workstream E: Archival and Deindex Infrastructure

Deliverables:

- implement per-node and per-edge exact index-entry catalogs
- implement persistent deindex work items for deindex cleanup
- implement configurable deindex cleanup scheduling, default nightly and configurable in seconds
- perform asynchronous batched deindex deletes for suppressed nodes and edges
- ensure cleanup does not scan entire indexes to discover stale entries
- keep physical storage reclamation separate from logical suppression and deindex behavior

### Workstream F: UI and Tooling

Deliverables:

- show effective decay profile and matching promotion policy in browser, diagnostics, and admin outputs
- show effective score start time in diagnostics and admin outputs
- show suppressed vs deindexed status for nodes and edges in diagnostics and admin outputs
- let operators inspect policies and resolution traces
- add diagnostics for why a node or edge decayed, promoted, was suppressed, deindexed, or did not
- add diagnostics for why a property was excluded from vectorization or retrieval without being suppressed

---

## 10. Implementation Sequence

1. Define the decay profile schema model and resolution precedence.
2. Define the promotion profile and promotion policy schema models and resolution precedence.
3. Define the decay profile `scoreFrom` option with supported values `CREATED`, `VERSION`, and `CUSTOM`.
4. Centralize decay resolution in a shared helper used by recall, recalc, suppression pass, ranking, and stats paths.
5. Add configurable per-profile half-lives, decay rates, named presets, and function identifiers.
6. Add configurable per-profile promotion multipliers and named presets.
7. Define and implement schema-backed decay profile entries for nodes and edges in the dedicated decay profile subsystem.
8. Define and implement schema-backed promotion policy entries for nodes and edges in the dedicated promotion subsystem.
9. Extend profile parsing and compiled metadata to support property-targeted retention entries.
10. Extend policy parsing and compiled metadata to support property-targeted promotion entries.
11. Add MVCC-aware decay-age start resolution using the profile-declared `scoreFrom`.
12. Implement whole-node and whole-edge suppression state transitions.
13. Implement per-entity index-entry catalogs for nodes and edges.
14. Implement persistent deindex work items and configurable deindex scheduling.
15. Add asynchronous batched deindex cleanup for suppressed nodes and edges.
16. Implement scoring-before-visibility: promotion first, then decay, then visibility determination before exposing entities to the query.
17. Implement the accessMeta index with msgpack serialization, `ON ACCESS` mutation routing, and accessMeta-first read resolution.
18. Add native Cypher functions `decayScore()` and `decay()` for nodes and edges with an explicit options object for `property` and optional `scoringMode`.
19. Add native Cypher function `policy()` for accessMeta retrieval on nodes and edges.
20. Add native Cypher function `reveal()` for bypassing scoring-driven visibility and property hiding on nodes and edges.
21. Migrate runtime logic away from fixed tier assumptions.
22. Bind unified retrieval scoring to the same shared scorer and profile-and-policy-resolved scoring configuration.
23. Expose policy and resolution information in Cypher, search metadata, and UI surfaces.
24. Add regression tests for node-level, edge-level, and property-level resolution, score start-time selection, scoring-before-visibility, `reveal()` bypass, accessMeta mutation and resolution, suppression/deindex behavior, and suppression skipping.
25. Add benchmark and evaluation coverage for profile and policy resolution overhead, scoring-before-visibility overhead, accessMeta read/write throughput, and correctness.

---

## 11. Testing Plan

### Must-have regression cases

- no-decay nodes are skipped by recalc and suppression paths
- no-decay edges are skipped by recalc and suppression paths
- effective decay rate comes from resolved decay profile rather than hardcoded tier
- edge-level decay rules can age an edge independently of its endpoint nodes
- property-level inline decay rules can age one property without decaying the parent entity
- property-level inline promotion rules can boost one property without boosting the parent entity
- property-level vectorization exclusion does not suppress, move, or delete the stored property
- properties excluded from vectorization remain directly queryable in Cypher
- property-level decay or promotion rules targeting indexed properties are rejected at creation time with a validation error
- indexed properties remain visible in query results regardless of entity-level decay score
- indexed properties are never hidden by property-level decay exclusion
- indexed properties are never excluded from vectorization by property-level decay rules
- creating a structural index (lookup, range, or composite) on a property that already has a decay or promotion rule produces a validation error or warning
- conflicting profiles or policies for the same target are rejected at creation time
- removing or changing a decay profile changes future resolution without corrupting stored history
- removing or changing a promotion policy or profile changes future resolution without corrupting stored history
- node-level, edge-level, and inline property rules all resolve correctly
- explain output identifies the exact decay profile entry and exact promotion policy entry with the selected profile
- `decayScore(n)` and `decayScore(r)` return the same resolved score used by runtime profile and policy evaluation
- `decay(n).score` and `decay(r).score` are Cypher-accessible and stable for projection and ordering
- returning `n` or `r` alone does not alter Neo4j-compatible result shape
- `decayScore(...)` returns `1.0` rather than an error when no decay profile or configured default applies
- unified search `meta` returns node, edge, and property decay scores in a separate keyed structure without mutating the hit payload
- unified retrieval scoring returns the same score family as the equivalent Cypher call when both use the same resolved profile and policy
- decay profiles, promotion profiles, and promotion policies are shown and retrieved separately through their respective subsystems
- `scoreFrom: 'CREATED'` measures decay age from original entity creation time
- `scoreFrom: 'VERSION'` measures decay age from the latest visible version time
- `scoreFrom: 'CUSTOM'` with `scoreFromProperty` measures decay age from the specified property value
- `scoreFrom: 'CUSTOM'` resolves the `scoreFromProperty` from accessMeta first, falling back to stored node or edge properties
- `scoreFrom: 'CUSTOM'` logs a warning and falls back to entity creation time when the resolved value is missing, null, or unparsable as a timestamp
- changing only derived score as time advances does not create a new stored version
- suppressed nodes and suppressed edges are skipped by retrieval paths
- suppressed nodes and suppressed edges are removed from indexing by the background cleanup process
- deindex cleanup uses exact-key deindexing and does not require full index scans
- deindex cleanup is idempotent and retry-safe
- properties are never suppressed as part of deindex cleanup
- `ON ACCESS` SET mutations write to the accessMeta index and do not mutate the target node or edge
- `ON ACCESS` and `WHEN` property reads resolve from accessMeta first, falling back to stored node or edge properties
- `policy(n)` returns the accessMeta map for a node with all keys written by `ON ACCESS` mutations
- `policy(r)` returns the accessMeta map for an edge with all keys written by `ON ACCESS` mutations
- `policy(...)` returns an empty map with only `targetId` and `targetScope` when no accessMeta entry exists
- accessMeta entries survive node or edge reads without being lost or corrupted
- accessMeta entries are correctly serialized and deserialized via msgpack across restarts
- scoring is evaluated before query visibility: a node whose final score falls below the visibility threshold does not appear in `MATCH` results
- scoring is evaluated before query visibility: a property excluded by decay does not appear on returned nodes unless accessed through `reveal()`
- promotion policies are resolved before decay profiles during scoring evaluation
- `reveal(n)` returns the raw stored node including all properties regardless of scoring-driven visibility
- `reveal(r)` returns the raw stored edge including all properties regardless of scoring-driven visibility
- `reveal()` does not alter the values returned by `decayScore()`, `decay()`, or `policy()` for the same entity
- `reveal()` is a no-op when decay is not enabled or the entity has no scoring-driven visibility suppression
- entities invisible due to scoring are accessible only through `reveal()` and not through ordinary `MATCH`

### Benchmark targets

- decay profile resolution overhead
- promotion policy resolution overhead
- score start-time resolution overhead
- edge-level decay selectivity and maintenance cost
- property-level decay selectivity and maintenance cost
- property-level promotion selectivity and ranking cost
- suppression pass throughput under mixed node and edge profile workloads
- deindex throughput for suppressed nodes and edges
- recall and ranking overhead with resolved node and edge profile and policy checks
- accessMeta index read and write throughput under concurrent `ON ACCESS` mutation workloads
- accessMeta-first property resolution overhead compared to direct node or edge property reads
- accessMeta msgpack serialization and deserialization throughput
- scoring-before-visibility overhead per entity during `MATCH` and search execution
- `reveal()` bypass overhead compared to ordinary scored entity access

---

## 12. Acceptance Criteria

The plan is complete when:

- no runtime path depends on a hardcoded tier enum to decide whether something decays or promotes
- operators can define decay semantics through config and decay profiles
- operators can define promotive declarative tiers through config, promotion profiles, and promotion policies
- operators can define property-level retention inline in decay profile bodies
- operators can define property-level promotion inline in promotion policy bodies
- operators can declaratively choose `CREATED`, `VERSION`, or `CUSTOM` score start time through decay profile options
- node-, edge-, and property-level decay are all supported
- node-, edge-, and property-level promotion are all supported
- edges can decay just like nodes can
- only whole nodes and whole edges can be suppressed
- suppressed nodes and suppressed edges are removed from indexing asynchronously and efficiently
- background deindex cleanup defaults to nightly execution and is configurable in seconds
- properties are never suppressed, moved, or deleted because of decay profile
- property-level decay can exclude properties from vectorization or vector-backed retrieval while preserving storage and direct Cypher access
- properties that participate in general indexes are immune to decay scoring, decay hiding, and property-level exclusion
- property-level decay or promotion rules targeting indexed properties are rejected at creation time
- explainable profile and policy resolution is available for diagnostics
- native Cypher functions expose resolved node, edge, and property scores without mutating Neo4j-compatible payloads
- unified search exposes the same resolved scores additively through response metadata rather than persisted fields
- Cypher scoring and unified retrieval scoring share the same scorer implementation, but only Cypher exposes an explicit `scoringMode` override
- targets without a matching decay profile or promotion policy resolve to a neutral score of `1.0` instead of producing Cypher errors
- decay profiles, promotion profiles, and promotion policies are authored, shown, and retrieved through separate subsystems
- new decay models can be expressed as decay profiles and new promotion tier models can be expressed as promotion profiles and policies without new engine categories
- the existing Cypher scoring API remains unchanged
- MVCC visibility remains snapshot-based while decay-age start time is declaratively selected by decay profile
- `ON ACCESS` mutation handlers write exclusively to a separate accessMeta index; nodes and edges are read-only during policy evaluation
- accessMeta entries are stored as `map[string]interface{}` keyed per target node or edge, serialized in msgpack alongside other data files
- property reads within `ON ACCESS` blocks and `WHEN` predicates resolve from accessMeta first, falling back to stored node or edge properties
- the native `policy()` Cypher function exposes accessMeta for any node or edge without mutating Neo4j-compatible payloads
- there is no correlated `policyScore()` scalar function; accessMeta is a general-purpose map, not a single score
- targets without an accessMeta entry return an empty map from `policy()` rather than producing a Cypher error
- scoring is evaluated before query visibility: promotion first, then decay, then visibility determination
- entities whose final score renders them invisible are suppressed from `MATCH`, `WHERE`, search hits, and traversal paths
- `reveal()` is the only mechanism to access scoring-invisible entities and decay-hidden properties in Cypher
- `reveal()` bypasses visibility suppression and property hiding without disabling scoring itself

---

## 13. Deliverables

- a profile-and-policy-driven decay and scoring specification
- schema and Cypher/search updates for profile-aware decay behavior and policy-aware promotion behavior
- a shared decay profile resolver with config-backed defaults and compiled profile entries
- a shared promotion policy resolver with config-backed defaults and compiled policy entries
- dedicated decay profile subsystem support for inline property-level retention rules, with creation-time validation rejecting rules that target indexed properties
- dedicated promotion policy subsystem support for inline property-level promotion rules, with creation-time validation rejecting rules that target indexed properties
- declarative decay profile support for `scoreFrom: 'CREATED' | 'VERSION' | 'CUSTOM'`
- explicit node and edge targeting support throughout profile and policy resolution and scoring
- whole-node and whole-edge suppression strategy with asynchronous deindex cleanup
- exact index-entry catalog support for nodes and edges
- deindex work item infrastructure for background cleanup
- native Cypher function support for `decayScore()` and `decay()`
- native Cypher function support for `policy()` to expose accessMeta on nodes and edges
- native Cypher function support for `reveal()` to bypass scoring-driven visibility and property hiding on nodes and edges
- scoring-before-visibility runtime implementation: promotion first, then decay, then visibility determination before query exposure
- accessMeta index implementation with msgpack serialization, `ON ACCESS` mutation routing, and accessMeta-first property read resolution
- shared runtime scorer support for Cypher and unified retrieval, with Cypher-only `scoringMode` override support and profile-and-policy-resolved search scoring
- unified search metadata support for additive node-, edge-, and property-level decay scores
- regression tests covering node-, edge-, and property-level semantics, scoring-before-visibility, and `reveal()` bypass
- user-facing documentation for decay profile authoring
- user-facing documentation for promotion profile and promotion policy authoring
- user-facing documentation for suppression and deindex behavior

---

## 14. Notes

This plan is intentionally implementation-oriented. The main architectural shift is to stop using fixed categories as permanent engine concepts and instead operate on resolved profiles and policies.

Named presets may remain in documentation for bootstrapping a memory decay model or promotive tier model for operator convenience, but the engine should ultimately care only about effective decay profile, effective promotion policy with its selected profile, and profile-resolved score start time.

Property-level decay and promotion may affect vectorization and retrieval behavior, but properties remain stored in place and directly queryable in Cypher via `reveal()`. Archival is reserved for whole nodes and whole edges.

There is no migration path from the existing tier system (`TierEpisodic`, `TierSemantic`, `TierProcedural` with hardcoded lambdas, `Node.DecayScore`, `Node.LastAccessed`, `Node.AccessCount` stored on nodes, replication codec sending `DecayScore`). This is a 1.1.0 version bump signifying an incompatible break with the older experimental memory model.

Updated summary: added a dedicated suppression/deindex layer, made suppression apply only to whole nodes and edges, added exact index-entry catalogs plus deindex work items for async cleanup, made the cleanup job nightly by default but configurable in seconds, and clarified that properties are never suppressed and only get excluded from vectorization/retrieval surfaces while remaining in storage and Cypher-visible. Added a separate accessMeta index for `ON ACCESS` mutation state, making nodes and edges read-only during policy evaluation. AccessMeta entries are `map[string]interface{}` keyed per target, serialized in msgpack alongside other data files. Property reads in `ON ACCESS` and `WHEN` blocks resolve from accessMeta first, falling back to stored node or edge properties. Added the native `policy()` Cypher function to expose accessMeta, with no correlated `policyScore()` scalar. Changed resolution order to promotion first, then decay. Scoring now happens before query visibility — entities whose final score renders them invisible are suppressed from `MATCH`, `WHERE`, search hits, and traversal paths. Added the native `reveal()` Cypher function to bypass scoring-driven visibility suppression and property-level decay hiding, returning the raw stored entity. Added indexed-property immunity: properties that participate in general indexes (lookup, range, composite, or any index used for aggregation and joining) are immune to decay scoring, decay hiding, and property-level exclusion; rules targeting indexed properties are rejected at creation time. Added Appendix A with a complete worked example implementing a Ebbinghaus-Roynard model informed by the four-layer decomposition proposed in arXiv:2604.11364v1.

---

## Appendix A: Ebbinghaus-Roynard Model — Four-Layer Example

This appendix demonstrates a complete decay and promotion configuration that implements a Modified Ebbinghaus forgetting-curve model, informed by the four-layer decomposition proposed in Roynard (2026), (Ebbinghaus-Roynard) "The Missing Knowledge Layer in Cognitive Architectures for AI Agents" ([arXiv:2604.11364v1](https://arxiv.org/pdf/2604.11364)).

### A.1 Motivation

The paper identifies a category error in existing memory systems: applying uniform cognitive decay to all content types. It calls out NornicDB specifically for "applying storage-level decay to permanent facts" and Signet for "uniform 0.95 days decay to all content types." The paper proposes a four-layer decomposition where each layer has distinct persistence semantics:

| Layer            | Content type                    | Persistence semantic                         | Decay behavior                                                            |
| ---------------- | ------------------------------- | -------------------------------------------- | ------------------------------------------------------------------------- |
| **Knowledge**    | Facts, claims, entities         | Supersession, not forgetting                 | No decay; facts are replaced by newer evidence, never forgotten by time   |
| **Memory**       | Experiences, episodes, sessions | Ebbinghaus-style forgetting                  | Exponential decay; consolidation on repeated access promotes to Knowledge |
| **Wisdom**       | Behavioral directives, patterns | Evidence-gated revision with stability tiers | No time-based decay; stability tiers gate revision, not forgetting        |
| **Intelligence** | The model itself                | Frozen in weights                            | Outside the scope of this system                                          |

The standard Ebbinghaus model treats all content uniformly: `score = e^(-t / halfLife)`. The modified model applies different decay semantics per layer while using promotion policies to model consolidation — the process by which repeatedly accessed Memory content is promoted to Knowledge-tier durability.

### A.2 Layer Labels

This example uses NornicDB node labels to represent the four-layer decomposition. The [Canonical Graph + Mutation Log Guide](../user-guides/canonical-graph-ledger.md) defines a reusable pattern for versioned facts using generic placeholder labels (`:FactKey`, `:FactVersion`) and structural edges (`:CURRENT`, `:SUPERSEDES`, `:HAS_VERSION`). Those labels are generic — operators replace them with domain-specific labels when adopting the pattern. In this model, `:KnowledgeFact` is the domain label for durable facts in the Knowledge layer. Operators structure `:KnowledgeFact` nodes using the CGL pattern (stable identity keys, immutable versioned snapshots, `:CURRENT` pointers, temporal no-overlap constraints) as described in the ledger guide. Supersession is modeled exclusively through `:SUPERSEDES` edges, not as a property on the node.

- `:KnowledgeFact` — durable facts, claims, entities (Knowledge layer)
- `:MemoryEpisode` — ephemeral experiences, session records, observations (Memory layer)
- `:WisdomDirective` — behavioral patterns, learned rules, procedural knowledge (Wisdom layer)
- `:EVIDENCES` — edge type linking Memory episodes to Knowledge facts they support or contradict
- `:CONSOLIDATES_TO` — edge type linking Memory episodes to Knowledge facts created through consolidation
- `:DERIVED_FROM` — edge type linking Wisdom directives to the Knowledge or Memory entities they were derived from
- `:SUPERSEDES` — edge type linking a newer `:KnowledgeFact` to the older fact it supersedes (Knowledge-layer supersession provenance)
- `:REVISES` — edge type linking a newer `:WisdomDirective` to the older directive it revises (Wisdom-layer revision provenance)

#### Layer semantics mapping

The paper requires four categorically different persistence mechanisms. This section maps each to concrete NornicDB mechanics:

| Layer            | Persistence mechanism                                                   | NornicDB implementation                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| ---------------- | ----------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Knowledge**    | Supersession + provenance; shared; no decay                             | `:KnowledgeFact` with `decayEnabled: false`. Supersession modeled through `:SUPERSEDES` edges and the Canonical Graph + Mutation Log (see [canonical-graph-ledger.md](../user-guides/canonical-graph-ledger.md)). Old facts are never deleted — they are linked to successors via `:SUPERSEDES` with provenance properties (`superseded_at`, `superseded_by_agent`, `evidence_source`). Facts are shared across agents: any agent querying the knowledge base sees the same facts.                                                                                             |
| **Memory**       | Ebbinghaus decay + bi-temporal event sourcing; per-agent; consolidation | `:MemoryEpisode` with exponential decay and bi-temporal timestamps. Each episode carries four timestamps following the Graphiti bi-temporal model: `system_created_at` (when NornicDB ingested it), `system_expired_at` (when it was superseded or archived), `valid_from` (when the real-world event occurred), `valid_to` (when the real-world event ceased to be true). Memory is scoped per agent via `agentId` (or `tenantId` at the tenant boundary). Each memory operation produces an immutable WAL event. Consolidation into Knowledge is promotion-driven (see A.5). |
| **Wisdom**       | Evidence-gated revision; no time-based decay; multi-source              | `:WisdomDirective` with `decayEnabled: false` and stability-tier promotion. Revision is an explicit graph operation: when evidence gates are met for revision, a new `:WisdomDirective` is created and linked to the old one via `:REVISES` with provenance (`revised_at`, `revision_reason`, `evidence_sources`). The old directive is marked `status: 'retired'`. Stability tiers (provisional → established → canonical) gate how much counter-evidence is required for revision, preventing sycophantic promotion.                                                         |
| **Intelligence** | Ephemeral; inference-time only                                          | Outside the scope of the persistence layer. Intelligence is the model itself and the runtime query context. It leaves no trace in storage; its effects persist only through writes to the other three layers.                                                                                                                                                                                                                                                                                                                                                                  |

#### Recency vs. decay — the organizing principle

The paper identifies a key design litmus: recency is a **query-time** heuristic (the Intelligence layer can boost recent results when recency matters), while decay is a **storage-level** mechanism (the Memory layer applies Ebbinghaus forgetting to experiential content). This system implements the distinction correctly: Knowledge facts are never subject to storage-level decay, but a query can still `ORDER BY` a fact's creation time or version time to prefer recent facts. Decay scoring via `decayScore()` operates at the storage level; recency boosting via `ORDER BY` or search ranking operates at query time. These are independent concerns.

### A.3 Decay Profiles

#### Knowledge layer — no decay

Knowledge facts are permanent. They are superseded by newer evidence through graph operations (creating a new fact node and marking the old one as superseded), not by time-based decay. The Ebbinghaus curve does not apply here. See the [Canonical Graph + Mutation Log Guide](../user-guides/canonical-graph-ledger.md) for the definitive model of versioned facts, validity windows, and append-only mutation logging that governs knowledge-layer persistence.

```cypher
CREATE DECAY PROFILE knowledge_fact_retention
OPTIONS {
  decayEnabled: false,
  visibilityThreshold: 0.0,
  scope: 'NODE',
  function: 'none',
  scoreFrom: 'CREATED'
}
```

```cypher
CREATE DECAY PROFILE knowledge_fact_retention_binding
FOR (n:KnowledgeFact)
APPLY {
  DECAY PROFILE 'knowledge_fact_retention'
  n.sourceId NO DECAY
  n.tenantId NO DECAY
  n.claim NO DECAY
  n.confidence NO DECAY
  n.assertedBy NO DECAY
  n.evidenceSource NO DECAY
}
```

Provenance is first-class: every `:KnowledgeFact` carries `sourceId` (where the fact came from), `assertedBy` (which agent or process asserted it), `evidenceSource` (the evidence chain), and `confidence` (extraction confidence). Supersession creates a `:SUPERSEDES` edge linking the new fact to the old, preserving both for historical queries. This matches the paper's requirement that "old claims are never deleted but linked to their successors." See the [Canonical Graph + Mutation Log Guide](../user-guides/canonical-graph-ledger.md) for the full `FactVersion` model with temporal validity windows.

#### Memory layer — Ebbinghaus exponential decay

Memory episodes decay according to the Ebbinghaus forgetting curve. The half-life is 7 days (604800 seconds). Episodes that are not accessed or consolidated within this window lose score and eventually become invisible to queries. Suppressed episodes are deindexed but remain accessible through `reveal()`.

```cypher
CREATE DECAY PROFILE memory_episode_retention
OPTIONS {
  halfLifeSeconds: 604800,
  visibilityThreshold: 0.10,
  scope: 'NODE',
  function: 'exponential',
  scoreFrom: 'VERSION'
}
```

```cypher
CREATE DECAY PROFILE memory_episode_retention_binding
FOR (n:MemoryEpisode)
APPLY {
  DECAY PROFILE 'memory_episode_retention'
  DECAY VISIBILITY THRESHOLD 0.10
  n.tenantId NO DECAY
  n.agentId NO DECAY
  n.sessionId NO DECAY
  n.system_created_at NO DECAY
  n.system_expired_at NO DECAY
  n.valid_from NO DECAY
  n.valid_to NO DECAY
  n.summary DECAY HALF LIFE 1209600
  n.summary DECAY FLOOR 0.10
  n.ephemeralContext DECAY HALF LIFE 86400
}
```

#### Bi-temporal event sourcing

The paper requires bi-temporal timestamps following the Graphiti model. Every `:MemoryEpisode` carries four timestamps:

- `system_created_at` — when NornicDB ingested the episode (system time)
- `system_expired_at` — when the episode was superseded or archived (system time; null if current)
- `valid_from` — when the real-world event occurred (real-world time)
- `valid_to` — when the real-world event ceased to be true (real-world time; null if still true)

This distinction between "when did we learn this" and "when was this actually true" is essential for resolving temporal conflicts. All four timestamps are declared `NO DECAY` — they are structural metadata, not decaying content. The decay score is computed from `scoreFrom: 'VERSION'` (the latest version timestamp), not from the bi-temporal timestamps, which serve temporal queries (`AS OF`, `WHEN`) rather than decay scoring.

Memory is scoped per agent via `agentId`. An agent querying its own memory sees only its own episodes. Knowledge is shared; Memory is not. This matches the paper's ownership model.

#### Memory episode summaries — property-level decay

Episode summaries decay more slowly than the episode itself, reflecting that a summary may retain value even as the raw experience fades. The ephemeral context (working state, scratch data) decays aggressively at a 1-day half-life.

#### Wisdom layer — no time-based decay

Wisdom directives do not decay over time. They have stability tiers managed through promotion policies. A directive is revised only when evidence gates are met, not when time passes. This directly addresses the paper's observation that "knowing how" and "knowing that" are architecturally distinct.

```cypher
CREATE DECAY PROFILE wisdom_directive_retention
OPTIONS {
  decayEnabled: false,
  visibilityThreshold: 0.0,
  scope: 'NODE',
  function: 'none',
  scoreFrom: 'CREATED'
}
```

```cypher
CREATE DECAY PROFILE wisdom_directive_retention_binding
FOR (n:WisdomDirective)
APPLY {
  DECAY PROFILE 'wisdom_directive_retention'
  n.tenantId NO DECAY
  n.directive NO DECAY
  n.stabilityTier NO DECAY
  n.revisionLog NO DECAY
  n.status NO DECAY
  n.derivedFrom NO DECAY
}
```

#### Wisdom revision mechanics

Wisdom does not decay, but it does get revised. The paper distinguishes this from supersession: Knowledge supersession preserves both old and new claims indefinitely because both are factual statements. Wisdom revision retires the old directive because the old behavioral pattern is no longer recommended.

When evidence gates are met for revision (e.g., a canonical directive is contradicted by overwhelming counter-evidence), the operation is:

1. Create a new `:WisdomDirective` with the revised behavioral pattern, `status: 'active'`.
2. Set `status: 'retired'` on the old directive.
3. Create a `:REVISES` edge from the new directive to the old, carrying `revised_at`, `revision_reason`, and `evidence_sources`.
4. The old directive remains queryable (it is not deleted or archived) but its `status: 'retired'` signals that it is no longer the current recommendation.

This is an explicit graph operation performed by the consolidation process or the calling agent — not by the decay/promotion subsystem. The promotion policy's stability tiers determine **how much evidence is required** before revision is permitted, but they do not perform the revision itself. This separation prevents sycophantic models from promoting agreeable-but-incorrect patterns: revision requires structured evidence (corroboration count, session span, contradiction absence), not approval.

```cypher
// Example revision edge
CREATE DECAY PROFILE revision_edge_retention
FOR ()-[r:REVISES]-()
APPLY {
  NO DECAY
  r.revised_at NO DECAY
  r.revision_reason NO DECAY
  r.evidence_sources NO DECAY
}
```

#### Evidence edges — moderate decay

Evidence edges linking Memory episodes to Knowledge facts decay at a moderate rate. Old evidence relationships lose weight over time, but the Knowledge fact they support does not decay. This models the intuition that the relevance of a specific piece of supporting evidence fades, even though the conclusion it supported remains durable.

```cypher
CREATE DECAY PROFILE evidence_edge_retention
OPTIONS {
  halfLifeSeconds: 2592000,
  visibilityThreshold: 0.10,
  scope: 'EDGE',
  function: 'exponential',
  scoreFrom: 'CREATED'
}
```

```cypher
CREATE DECAY PROFILE evidence_edge_retention_binding
FOR ()-[r:EVIDENCES]-()
APPLY {
  DECAY PROFILE 'evidence_edge_retention'
  DECAY VISIBILITY THRESHOLD 0.10
  r.sourceId NO DECAY
}
```

#### Consolidation and provenance edges — no decay

Consolidation edges, supersession edges, and derivation edges are permanent records of the provenance chain between layers. They do not decay because provenance is itself a durable claim. The paper requires that "old claims are never deleted but linked to their successors" — these edges implement that invariant.

```cypher
CREATE DECAY PROFILE consolidation_edge_retention
FOR ()-[r:CONSOLIDATES_TO]-()
APPLY {
  NO DECAY
  r.consolidatedAt NO DECAY
  r.sourceId NO DECAY
  r.consolidatedByAgent NO DECAY
}
```

```cypher
CREATE DECAY PROFILE supersession_edge_retention
FOR ()-[r:SUPERSEDES]-()
APPLY {
  NO DECAY
  r.superseded_at NO DECAY
  r.superseded_by_agent NO DECAY
  r.evidence_source NO DECAY
}
```

```cypher
CREATE DECAY PROFILE derivation_edge_retention
FOR ()-[r:DERIVED_FROM]-()
APPLY {
  NO DECAY
  r.derived_at NO DECAY
  r.derivation_method NO DECAY
}
```

### A.4 Promotion Profiles

#### Memory consolidation tiers

These profiles model the Ebbinghaus spaced-repetition effect: repeated access strengthens the memory trace. The `consolidation_candidate` tier identifies episodes that have been accessed enough times to be candidates for consolidation into durable Knowledge facts.

```cypher
CREATE PROMOTION PROFILE memory_reinforced
OPTIONS {
  scope: 'NODE',
  multiplier: 1.25,
  scoreFloor: 0.0,
  scoreCap: 1.0
}

CREATE PROMOTION PROFILE consolidation_candidate
OPTIONS {
  scope: 'NODE',
  multiplier: 1.50,
  scoreFloor: 0.80,
  scoreCap: 1.0
}
```

#### Wisdom stability tiers

Wisdom directives use stability tiers rather than time-based decay. A directive that has been validated by multiple independent evidence sources is harder to revise — it requires stronger counter-evidence. This is the "evidence-gated revision" the paper describes.

```cypher
CREATE PROMOTION PROFILE wisdom_provisional
OPTIONS {
  scope: 'NODE',
  multiplier: 1.0,
  scoreFloor: 0.0,
  scoreCap: 1.0
}

CREATE PROMOTION PROFILE wisdom_established
OPTIONS {
  scope: 'NODE',
  multiplier: 1.0,
  scoreFloor: 0.50,
  scoreCap: 1.0
}

CREATE PROMOTION PROFILE wisdom_canonical
OPTIONS {
  scope: 'NODE',
  multiplier: 1.0,
  scoreFloor: 0.90,
  scoreCap: 1.0
}
```

### A.5 Promotion Policies

#### Memory episode consolidation policy

This policy implements the core Ebbinghaus modification: repeated access within the decay window promotes a Memory episode through reinforcement tiers. Episodes that reach `consolidation_candidate` status are flagged for the consolidation process, which creates a corresponding `:KnowledgeFact` node and a `:CONSOLIDATES_TO` edge.

**Architectural boundary: truth-promotion is an application-layer concern.** The promotion policy determines *when an entity is a candidate for consolidation* (via `WHEN` predicates and promotion tiers). It does **not** perform the consolidation itself — creating a `:KnowledgeFact` node, linking it with a `:CONSOLIDATES_TO` edge, and deciding what constitutes "ground truth" is a workload-specific decision that belongs in the application layer, not in the engine's decay/promotion subsystem. NornicDB provides the primitives (decay profiles, promotion policies, Kalman-smoothed behavioral signals, query context variables) but leaves the truth-promotion logic to the calling application. The Heimdall plugin (`pkg/heimdall/plugin.go`) serves as the reference implementation for how to build a consolidation pipeline on top of these primitives — it reads `policy(n).crossSessionAccessRate`, checks agreement thresholds, and performs the graph operations to promote episodic content to the canonical knowledge layer. This separation prevents the engine from encoding workload-specific opinions about what constitutes truth, while giving every deployment a documented pattern to follow or customize.

The `ON ACCESS` block tracks access count and last access time in the accessMeta index. The node itself is read-only — `n.accessCount` writes to accessMeta, and `n.accessCount` reads resolve from accessMeta first. Raw counters and timestamps use plain `SET`. The `WITH KALMAN` modifier is used on the derived `crossSessionAccessRate` — a noisy behavioral signal that benefits from smoothing and session-aware gating.

```cypher
CREATE PROMOTION POLICY memory_episode_consolidation
FOR (n:MemoryEpisode)
APPLY {
  ON ACCESS {
    SET n.accessCount = coalesce(n.accessCount, 0) + 1
    SET n.lastAccessedAt = timestamp()
    SET n.accessIntervals = coalesce(n.accessIntervals, '') + ',' + toString(timestamp())

    -- Kalman-smooth cross-session access rate (session-gated, noisy behavioral signal)
    WITH KALMAN SET n.crossSessionAccessRate =
      CASE WHEN n._lastSessionId <> $_session
        THEN coalesce(n.crossSessionAccessRate, 0) + 1
        ELSE n.crossSessionAccessRate
      END
    SET n._lastSessionId = $_session
  }

  WHEN n.accessCount >= 3
    APPLY PROFILE 'memory_reinforced'

  WHEN n.accessCount >= 5 AND n.sourceAgreement >= 0.80
    APPLY PROFILE 'consolidation_candidate'
}
```

The `consolidation_candidate` profile sets a score floor of `0.80`, which means even as the Ebbinghaus curve pulls the base score down, the promoted floor keeps the episode visible. This models the spaced-repetition finding that sufficiently rehearsed content resists forgetting.

#### Wisdom directive stability policy

Wisdom directives are not subject to time-based decay, but they have stability tiers that gate revision. A provisional directive can be revised by any contradicting evidence. An established directive requires multiple independent sources. A canonical directive requires overwhelming counter-evidence and is flagged for human review before revision.

```cypher
CREATE PROMOTION POLICY wisdom_directive_stability
FOR (n:WisdomDirective)
APPLY {
  ON ACCESS {
    SET n.evaluationCount = coalesce(n.evaluationCount, 0) + 1
    SET n.lastEvaluatedAt = timestamp()
  }

  WHEN n.evidenceCount < 3
    APPLY PROFILE 'wisdom_provisional'

  WHEN n.evidenceCount >= 3 AND n.contradictionRate < 0.20
    APPLY PROFILE 'wisdom_established'

  WHEN n.evidenceCount >= 10 AND n.contradictionRate < 0.05 AND n.crossSessionSupport >= 3
    APPLY PROFILE 'wisdom_canonical'
}
```

Note: `n.evidenceCount`, `n.contradictionRate`, and `n.crossSessionSupport` in the `WHEN` predicates resolve from accessMeta first. If they are not present in accessMeta, they fall back to stored node properties. This allows external processes (e.g., a consolidation job) to write these values to either the node or to accessMeta depending on whether they should be durable or transient.

#### Evidence edge traversal policy

Evidence edges track traversal count to support ranking of evidence chains. More frequently traversed evidence links carry higher weight in retrieval even as their base decay score drops.

```cypher
CREATE PROMOTION PROFILE reinforced_evidence
OPTIONS {
  scope: 'EDGE',
  multiplier: 1.20,
  scoreFloor: 0.0,
  scoreCap: 1.0
}
```

```cypher
CREATE PROMOTION POLICY evidence_traversal_tiering
FOR ()-[r:EVIDENCES]-()
APPLY {
  ON ACCESS {
    SET r.traversalCount = coalesce(r.traversalCount, 0) + 1
    SET r.lastTraversedAt = timestamp()
  }

  WHEN r.traversalCount >= 5
    APPLY PROFILE 'reinforced_evidence'
}
```

### A.6 Resolution Walkthrough

This section walks through the complete resolution sequence for a semantic search query that uses NornicDB's vector search to find content related to a user question, traverses the graph across all three layers, and returns decay-filtered results. This demonstrates how scoring-before-visibility naturally integrates with semantic retrieval.

#### Scenario

A user asks: _"What are the best practices for fine-tuning efficiency?"_

The system needs to:

1. Semantically search for related Knowledge facts, Memory episodes, and Wisdom directives
2. Score all touched entities through promotion and decay
3. Suppress invisible results (decayed Memory episodes below threshold)
4. Return ranked results across all three layers with scoring metadata

#### Query 1: Semantic retrieval with graph traversal and decay filtering

This query uses `db.retrieve` to find semantically related nodes across all layers, then traverses the graph to find connected evidence chains and directives, returning everything with decay scores.

```cypher
// Semantic search finds related content across all layers
CALL db.retrieve({query: 'best practices for fine-tuning efficiency', limit: 20}) YIELD node, score AS searchScore

// Separate results by layer
WITH node, searchScore
WHERE node:KnowledgeFact OR node:MemoryEpisode OR node:WisdomDirective

// Traverse from each hit to find connected evidence and directives
OPTIONAL MATCH (node)-[e:EVIDENCES]->(k:KnowledgeFact)
OPTIONAL MATCH (node)<-[e2:EVIDENCES]-(m:MemoryEpisode)
OPTIONAL MATCH (w:WisdomDirective)-[d:DERIVED_FROM]->(node)

// Return with decay metadata — invisible entities are already filtered out
RETURN node,
       labels(node) AS layer,
       searchScore,
       decayScore(node) AS decayFinalScore,
       decay(node) AS decayMeta,
       policy(node) AS accessMeta,
       collect(DISTINCT k) AS linkedFacts,
       collect(DISTINCT m) AS linkedEpisodes,
       collect(DISTINCT w) AS linkedDirectives
ORDER BY searchScore * decayScore(node) DESC
```

In this query, scoring-before-visibility means:

- The 20 candidates returned by `db.retrieve` have already been scored. Memory episodes that crossed below the visibility threshold are not in the result set — they were suppressed before the `YIELD`.
- The `OPTIONAL MATCH` traversals also respect visibility. A `:MemoryEpisode` connected by an `:EVIDENCES` edge will not appear in `linkedEpisodes` if its decay score renders it invisible.
- The `ORDER BY` combines semantic relevance (`searchScore`) with decay freshness (`decayScore(node)`) for a unified ranking that balances meaning and recency.
- Knowledge facts and Wisdom directives always appear because they have no decay. Memory episodes appear only if they are above threshold.

#### Query 2: Vector search with explicit embedding and reranking

This query uses `db.index.vector.queryNodes` with a string query for explicit vector search against a specific index, with full decay metadata. Reranking operates on already-scored, already-materialized results — the cross-encoder evaluates semantic relevance only and does not take promotion or decay scores into account. Decay-weighted ranking is applied after reranking by the caller.

```cypher
// Vector search against knowledge facts — queryNodes accepts a string directly
CALL db.index.vector.queryNodes('knowledge_fact_idx', 15, 'best practices for fine-tuning efficiency') YIELD node AS fact, score AS vecScore

// Traverse to find supporting episodes and derived directives
OPTIONAL MATCH (m:MemoryEpisode)-[e:EVIDENCES]->(fact)
OPTIONAL MATCH (w:WisdomDirective)-[:DERIVED_FROM]->(fact)

// Collect candidates for reranking
WITH fact, vecScore,
     collect(m) AS episodes,
     collect(w) AS directives

// Return with decay and policy metadata
RETURN fact,
       fact.claim AS claim,
       vecScore,
       decayScore(fact) AS factDecayScore,
       [ep IN episodes | {
         episode: ep,
         decayScore: decayScore(ep),
         accessCount: policy(ep).accessCount,
         lastAccessed: policy(ep).lastMutatedAt
       }] AS episodeDetails,
       [dir IN directives | {
         directive: dir,
         stabilityTier: policy(dir).evidenceCount,
         evaluationCount: policy(dir).evaluationCount
       }] AS directiveDetails
ORDER BY vecScore DESC
```

In this query:

- `db.index.vector.queryNodes` accepts a string directly — NornicDB handles embedding inline. It returns only visible Knowledge facts — facts are always visible because they have `decayEnabled: false`, but if the index were over `:MemoryEpisode` nodes, decayed episodes below threshold would be suppressed from the vector search results.
- The `OPTIONAL MATCH` for `:MemoryEpisode` respects visibility. An episode accessed once 20 days ago (score `0.100`, below threshold) does not appear in the `episodes` collection.
- The list comprehension `[ep IN episodes | {...}]` extracts decay and policy metadata per episode, showing how `decayScore()` and `policy()` compose naturally with vector search results.
- `policy(ep).accessCount` reads from accessMeta. If the episode was accessed 4 times, this value was incremented by `ON ACCESS` mutations during prior evaluations, not stored on the episode node.

#### Query 3: Hybrid search with reranking and cross-layer aggregation

This query uses `db.retrieve` for hybrid search (vector + BM25 + RRF), then `db.rerank` for cross-encoder reranking, then applies decay-weighted final scores as a separate step. The reranker is a pure semantic relevance pass — it scores query-to-content similarity using the cross-encoder model and has no knowledge of promotion policies, decay profiles, or accessMeta. Policy-based scoring (promotion and decay) happens before the results are materialized into the candidate list, determining visibility. Reranking happens after materialization, reordering only the visible candidates by semantic fit. The caller then combines the rerank score with the decay score for the final ranking.

```cypher
// Hybrid search: vector + BM25 + RRF fusion
CALL db.retrieve({query: 'LoRA fine-tuning efficiency tradeoffs', limit: 30}) YIELD node, score AS retrievalScore

// Or, optionally...
CALL db.index.vector.queryNodes('knowledge_fact_idx', 15, 'best practices for fine-tuning efficiency') YIELD node, score AS retrievalScore

// Rerank with cross-encoder
WITH collect({id: id(node), content: coalesce(node.content, node.claim, node.summary, toString(node)), score: retrievalScore}) AS candidates
CALL db.rerank({query: 'LoRA fine-tuning efficiency tradeoffs', candidates: candidates, rerankTopK: 10}) YIELD id, final_score AS rerankScore

// Resolve the reranked nodes
MATCH (n) WHERE id(n) = id

// Decay-weighted final ranking
WITH n, rerankScore, decayScore(n) AS decayFinal,
     rerankScore * decayScore(n) AS combinedScore

// Return with full metadata per layer
RETURN n,
       labels(n) AS layer,
       rerankScore,
       decayFinal,
       combinedScore,
       decay(n) AS decayMeta,
       policy(n) AS accessMeta,
       CASE
         WHEN n:KnowledgeFact THEN 'permanent — supersession only'
         WHEN n:MemoryEpisode THEN 'ebbinghaus — ' + toString(decayFinal)
         WHEN n:WisdomDirective THEN 'stability-gated — tier ' + coalesce(toString(policy(n).evidenceCount), 'unknown')
         ELSE 'unclassified'
       END AS retentionBehavior
ORDER BY combinedScore DESC
```

In this query:

- `db.retrieve` performs hybrid search (vector + BM25 + RRF fusion). Scoring-before-visibility applies here: promotion and decay are resolved per candidate before `YIELD`. Memory episodes below the visibility threshold are suppressed — the 30 candidates are all visible, already-scored entities.
- `db.rerank` applies cross-encoder reranking to the top 10. The reranker is a pure semantic relevance evaluator — it receives the text content of each candidate and the query string, and produces a relevance score. It has no access to promotion policies, decay profiles, accessMeta, or any policy-based scoring. It sees only visible, materialized content.
- The `combinedScore = rerankScore * decayScore(n)` is computed by the caller after reranking, blending the reranker's semantic relevance with the policy system's decay freshness. This is the correct composition point: the reranker handles meaning, the policy system handles durability, and the caller decides how to weight them.
- A Knowledge fact always contributes `decayFinal = 1.0`. A 3-day-old Memory episode accessed 4 times contributes `decayFinal = 0.936` (base `0.749` × reinforced multiplier `1.25`). A 10-day-old episode accessed once contributes `decayFinal = 0.317`.
- The `CASE` expression demonstrates how the layer decomposition is visible in query results — each layer's retention behavior is self-describing.

#### Query 4: Diagnostics — reveal invisible entities

When an operator needs to inspect what the decay subsystem has hidden — for consolidation review, audit, or debugging — `reveal()` bypasses visibility.

```cypher
// Find ALL memory episodes for a session, including invisible ones
MATCH (m:MemoryEpisode {sessionId: $sessionId})
RETURN reveal(m) AS rawEpisode,
       decay(m) AS decayMeta,
       policy(m) AS accessMeta,
       decayScore(m) AS score,
       CASE
         WHEN decayScore(m) >= 0.80 THEN 'visible — strong'
         WHEN decayScore(m) >= 0.10 THEN 'visible — fading'
         ELSE 'invisible — below visibility threshold'
       END AS visibilityStatus
ORDER BY decayScore(m) ASC
```

```cypher
// Consolidation review: find episodes that are consolidation candidates
MATCH (m:MemoryEpisode)
WHERE policy(reveal(m)).accessCount >= 5
RETURN reveal(m) AS rawEpisode,
       policy(m).accessCount AS accessCount,
       policy(m).sourceAgreement AS sourceAgreement,
       decayScore(m) AS currentScore,
       decay(m).scoreFrom AS scoreAnchor
```

```cypher
// Full cross-layer audit: semantic search with reveal to include invisible results
CALL db.index.vector.embed('fine-tuning efficiency') YIELD embedding
// this is for explicit control and an exmple of how we use either an embedding array or a string in the same call.
CALL db.index.vector.queryNodes('memory_episode_idx', 50, embedding) YIELD node, score AS vecScore
RETURN reveal(node) AS rawNode,
       vecScore,
       decayScore(node) AS decayFinal,
       decay(node) AS decayMeta,
       policy(node) AS accessMeta
ORDER BY vecScore DESC
```

Note: `reveal()` in the last query ensures that even Memory episodes whose decay score has dropped below the visibility threshold are returned from the vector search. Without `reveal()`, those episodes would be suppressed by scoring-before-visibility and would not appear in the `YIELD`.

#### Step-by-step resolution (Query 1)

This walkthrough traces the resolution of Query 1 for a representative set of results.

**1. Semantic retrieval**

`db.retrieve` performs hybrid search (vector + BM25 + RRF). Before yielding each candidate, the engine resolves promotion and decay for every touched node:

**2. Promotion policy WHEN resolution (first, per candidate)**

`WHEN` predicates are evaluated using the persisted + buffered accessMeta state from prior accesses. `ON ACCESS` mutations have not yet executed for this evaluation cycle.

- `k:KnowledgeFact {claim: 'LoRA achieves 95% of full fine-tuning quality'}` — no promotion policy matches. Neutral promotion effect.
- `m1:MemoryEpisode` (4 accesses, 3 days old) — `memory_episode_consolidation` matches. `WHEN` predicates evaluated against current accessMeta: `accessCount = 4`. `accessCount >= 3` → `memory_reinforced` selected (multiplier `1.25`). `accessCount >= 5` → not met (only 4). Highest matching: `memory_reinforced`.
- `m2:MemoryEpisode` (1 access, 10 days old) — `memory_episode_consolidation` matches. `WHEN` predicates evaluated: `accessCount = 1`. No `WHEN` predicate matches. Neutral promotion.
- `m3:MemoryEpisode` (0 accesses, 20 days old) — `memory_episode_consolidation` matches. `WHEN` predicates evaluated: `accessCount = 0`. No `WHEN` predicate matches. Neutral promotion.
- `e1:EVIDENCES` (traversed 6 times) — `evidence_traversal_tiering` matches. `WHEN` predicates evaluated: `traversalCount = 6`. `traversalCount >= 5` → `reinforced_evidence` selected (multiplier `1.20`).
- `w1:WisdomDirective` (8 evidence sources, 0.10 contradiction rate) — `wisdom_directive_stability` matches. `WHEN` predicates evaluated: `evidenceCount = 8`, `contradictionRate = 0.10`. `evidenceCount >= 3 AND contradictionRate < 0.20` → `wisdom_established` selected (floor `0.50`).

**3. Decay profile resolution (second, per candidate)**

- `k`: `knowledge_fact_retention_binding` matches. `decayEnabled: false`. Base score: `1.0`.
- `m1`: `memory_episode_retention_binding` matches. `function: 'exponential'`, `halfLifeSeconds: 604800`, `scoreFrom: 'VERSION'`. Base score: `e^(-259200 × ln(2) / 604800) = 0.749`.
- `m2`: same profile. Base score: `e^(-864000 × ln(2) / 604800) = 0.317`.
- `m3`: same profile. Base score: `e^(-1728000 × ln(2) / 604800) = 0.100`.
- `e1`: `evidence_edge_retention_binding` matches. `function: 'exponential'`, `halfLifeSeconds: 2592000`, `scoreFrom: 'CREATED'`. Base score: `e^(-1296000 × ln(2) / 2592000) = 0.707`.
- `w1`: `wisdom_directive_retention_binding` matches. `decayEnabled: false`. Base score: `1.0`.

**4. Final score computation**

| Entity                                | Base    | Promotion                                   | Final                                      | Visible?             |
| ------------------------------------- | ------- | ------------------------------------------- | ------------------------------------------ | -------------------- |
| `k` (KnowledgeFact)                   | `1.000` | none                                        | `1.000`                                    | yes — no decay       |
| `m1` (MemoryEpisode, 3d, 5 accesses)  | `0.749` | `consolidation_candidate` ×1.50, floor 0.80 | `max(0.749 × 1.50, 0.80) = 1.000` (capped) | yes — consolidated   |
| `m2` (MemoryEpisode, 10d, 2 accesses) | `0.317` | none                                        | `0.317`                                    | yes — fading         |
| `m3` (MemoryEpisode, 20d, 1 access)   | `0.100` | none                                        | `0.100`                                    | yes — near threshold |
| `e1` (EVIDENCES, 15d, 7 traversals)   | `0.707` | `reinforced_evidence` ×1.20                 | `0.849`                                    | yes                  |
| `w1` (WisdomDirective, established)   | `1.000` | `wisdom_established` floor 0.50             | `1.000`                                    | yes — no decay       |

Note: `m1` is now a `consolidation_candidate`. Its promotion floor of `0.80` means it resists forgetting even as the Ebbinghaus curve continues to pull the base score down. After 30 days, the base score would be `e^(-2592000 × ln(2) / 604800) = 0.045`, but the promotion floor holds the final score at `0.80`. This is the spaced-repetition effect: sufficiently rehearsed content resists the forgetting curve.

**5. Visibility determination**

- `m3` has a final score of `0.100`, at the visibility threshold of `0.10` — still visible but low-ranked in the `ORDER BY searchScore * decayScore(node) DESC`.
- At 21 days, `m3`'s base score would drop to `e^(-1814400 × ln(2) / 604800) = 0.088`, final `0.088`, below the visibility threshold of `0.10` — suppressed. It would no longer appear in `db.retrieve` results or `MATCH` traversals.
- `m1` remains visible indefinitely because its promotion floor of `0.80` overrides the Ebbinghaus curve. To find `m3` after it becomes suppressed, the operator would use `reveal()`.

**5a. ON ACCESS execution (visible entities only)**

`ON ACCESS` mutations execute only for entities that passed the visibility gate in step 5. Suppressed entities do not accumulate access state.

- `k`: no promotion policy → no `ON ACCESS` block. No mutation.
- `m1`: visible. `ON ACCESS` executes: `n.accessCount` incremented to `5` in accessMeta, `n.lastAccessedAt` set.
- `m2`: visible. `ON ACCESS` executes: `n.accessCount` incremented to `2` in accessMeta.
- `m3`: visible (score `0.100` ≥ threshold `0.10`). `ON ACCESS` executes: `n.accessCount` set to `1` in accessMeta.
- `e1`: visible. `ON ACCESS` executes: `r.traversalCount` incremented to `7` in accessMeta.
- `w1`: visible (no decay). `ON ACCESS` executes: `n.evaluationCount` incremented in accessMeta.

Note: if `m3` were below the visibility threshold (e.g., at 21 days), its `ON ACCESS` block would **not** execute. The entity would remain at `accessCount = 0` — it cannot accumulate access state while suppressed.

**6. Combined ranking**

The `ORDER BY searchScore * decayScore(node) DESC` in Query 1 produces a unified ranking:

| Entity                | Search Score | Decay Score | Combined | Layer                        |
| --------------------- | ------------ | ----------- | -------- | ---------------------------- |
| `k` (KnowledgeFact)   | `0.92`       | `1.000`     | `0.920`  | Knowledge                    |
| `m1` (consolidated)   | `0.88`       | `1.000`     | `0.880`  | Memory → Knowledge candidate |
| `w1` (established)    | `0.75`       | `1.000`     | `0.750`  | Wisdom                       |
| `e1` (reinforced)     | `0.71`       | `0.849`     | `0.603`  | Evidence edge                |
| `m2` (fading)         | `0.65`       | `0.317`     | `0.206`  | Memory                       |
| `m3` (near threshold) | `0.60`       | `0.100`     | `0.060`  | Memory                       |

The Knowledge fact ranks highest because it is both semantically relevant and permanently durable. The consolidated episode ranks second because its promotion floor keeps it strong. The Wisdom directive ranks third. Fading episodes rank last — their semantic relevance is discounted by their decay scores, naturally pushing stale content down the ranking without removing it entirely until it crosses the visibility threshold.

### A.7 Addressing the Paper's Critique

This configuration directly addresses the paper's identified problems:

| Paper critique                                                                                 | How the modified model addresses it                                                                                                                                                                                                                   |
| ---------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| "NornicDB applying storage-level decay to permanent facts"                                     | `:KnowledgeFact` nodes use `decayEnabled: false`. Facts are never subject to time-based decay. Supersession is modeled through `:SUPERSEDES` edges with full provenance, not decay.                                                                   |
| "Systems either forget what they should remember or remember what they should forget"          | The four-layer label scheme applies categorically different persistence semantics per content type. Knowledge: supersession. Memory: Ebbinghaus decay. Wisdom: evidence-gated revision.                                                               |
| "Signet applies uniform 0.95 days decay to all content types"                                  | Each layer has its own decay profile. Memory episodes use Ebbinghaus exponential. Knowledge and Wisdom use no-decay. Evidence edges use a longer half-life.                                                                                           |
| "CoALA does not distinguish the persistence semantics of semantic memory from episodic memory" | `:KnowledgeFact` (semantic) and `:MemoryEpisode` (episodic) have fundamentally different decay profiles, promotion policies, ownership scope, and visibility behavior.                                                                                |
| "Knowing how and knowing that are architecturally distinct"                                    | `:WisdomDirective` (knowing how) uses stability-tier promotion with evidence-gated revision via `:REVISES` edges. `:KnowledgeFact` (knowing that) uses supersession via `:SUPERSEDES` edges. Different update mechanics, different provenance models. |
| "Consolidation as a first-class operation"                                                     | The `consolidation_candidate` promotion tier flags Memory episodes for consolidation into Knowledge facts via `:CONSOLIDATES_TO` edges. Consolidation is expressed through promotion policies, not hardcoded runtime logic.                           |
| "Knowledge is shared across agents"                                                            | `:KnowledgeFact` nodes carry no `agentId` — they are shared. `:MemoryEpisode` nodes carry `agentId` for per-agent scoping. The ownership boundary is at the label level.                                                                              |
| "Bi-temporal event sourcing" with four timestamps per memory                                   | `:MemoryEpisode` carries `system_created_at`, `system_expired_at`, `valid_from`, `valid_to` — the Graphiti bi-temporal model. Enables "when did we learn this" vs. "when was this true" queries.                                                      |
| "Recency is a query-time heuristic, decay is a storage-level mechanism"                        | Decay scoring via `decayScore()` is storage-level (applied before visibility). Recency boosting via `ORDER BY` is query-time. Knowledge facts get zero decay but can still be sorted by recency.                                                      |
| "Sycophantic models could promote agreeable-but-incorrect patterns"                            | Wisdom stability tiers gate revision on structured evidence (corroboration count, contradiction absence, cross-session support), not on approval. Prevents sycophantic promotion.                                                                     |
| "Projections never silently become the truth they summarize"                                   | `:CONSOLIDATES_TO` and `:SUPERSEDES` edges preserve the provenance chain. The original Memory episode remains accessible via `reveal()`. Derivation is always explicit and auditable.                                                                 |

### A.8 Key Design Decisions

1. **Decay applies only to the Memory layer.** Knowledge and Wisdom do not decay over time. This is the core departure from uniform Ebbinghaus. It directly resolves the paper's central critique: "a paper's findings do not become less true after 69 days."

2. **Each layer has a different update mechanism.** Knowledge uses supersession (`:SUPERSEDES` edges). Memory uses Ebbinghaus decay. Wisdom uses evidence-gated revision (`:REVISES` edges). Intelligence is ephemeral. These are the four persistence mechanisms the paper identifies as the minimum decomposition.

3. **Consolidation is promotion-driven.** Repeated access promotes a Memory episode through reinforcement tiers until it reaches `consolidation_candidate` status. An external consolidation process then creates a `:KnowledgeFact` and a `:CONSOLIDATES_TO` edge. The policy subsystem identifies candidates; the graph operation performs the consolidation.

4. **Wisdom uses stability tiers, not decay.** A Wisdom directive's durability comes from evidence accumulation, not from time-based score. The promotion policy selects the stability tier; the tier determines how much counter-evidence is needed for revision. Revision is an explicit graph operation that creates a `:REVISES` edge and retires the old directive — it is not a gradual fade.

5. **Evidence edges decay but facts do not.** The relevance of a specific piece of evidence fades over time, but the conclusion it supports persists. This mirrors the paper's observation: "what decays is the agent's attentional relevance to the information (a memory concern, not a knowledge concern)."

6. **Memory is bi-temporal and per-agent.** Each `:MemoryEpisode` carries four timestamps (system-created, system-expired, valid-from, valid-to) following the Graphiti bi-temporal model. Memory is scoped per agent via `agentId`. Knowledge is shared — any agent sees the same facts. This matches the paper's ownership model.

7. **Recency is query-time; decay is storage-level.** The paper's organizing principle is implemented directly: `decayScore()` is a storage-level mechanism applied before visibility. `ORDER BY n.system_created_at DESC` is a query-time heuristic. A Knowledge fact with `decayEnabled: false` can still be sorted by recency without being subjected to storage-level decay.

8. **Anti-sycophancy by design.** Wisdom stability tiers gate revision on structured evidence (corroboration count, contradiction absence, cross-session support), not on user approval. This directly addresses the paper's reference to [7] showing RLHF-trained models affirm user behavior 50% more than humans.

9. **Provenance is explicit and auditable.** Supersession creates `:SUPERSEDES` edges. Consolidation creates `:CONSOLIDATES_TO` edges. Revision creates `:REVISES` edges. Derivation creates `:DERIVED_FROM` edges. "Projections never silently become the truth they summarize" — every derivation chain is preserved and queryable.

10. **accessMeta tracks access patterns without mutating nodes.** All access counters, timestamps, and interval tracking live in the accessMeta index. The nodes and edges themselves are read-only during policy evaluation. This preserves MVCC integrity and avoids creating new stored versions solely because access state changed.

11. **`reveal()` enables diagnostics and consolidation.** Invisible Memory episodes remain accessible through `reveal()` for consolidation review, audit, and diagnostics. Nothing is truly lost — it is scored out of default visibility.

12. **Indexed properties are immune.** Properties like `tenantId`, `sessionId`, and `sourceId` that participate in general indexes are declared `NO DECAY` and are immune to decay scoring and hiding, ensuring aggregation and joining remain stable.
