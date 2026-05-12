# Knowledge Policy × OTEL Observability Integration Plan

Status: drafted
Owner: observability + knowledge-policy working group
Related: `docs/architecture/adr/0001-observability.md`, `docs/plans/knowledge-layer-persistence-plan.md`

## Motivation

The knowledge-policy subsystem (decay, promotion, visibility suppression, on-access mutations) is fully instrumented *internally* — every evaluation produces a `ScoringResolution` and every flush drains an accumulator with per-entity access counts — but none of that state is exported. Operators have no way to answer questions like:

- How much of the working set is currently suppressed, and why (threshold vs. score-floor vs. on-access)?
- Are access-flush batches healthy, or is the buffer filling faster than the timer can drain it?
- Is a schema-change reconcile actually touching the expected number of entities?
- What is the decay-score distribution for each entity kind, and does it look bimodal (healthy) or flat (misconfigured half-life)?

The OTEL layer is well-established: `pkg/observability.Provider` plugs in a TracerProvider, a MeterProvider backed by a Prometheus registry, and already owns closed-enum subsystem catalogs (`cypher`, `storage`, `bolt`, `auth`, etc.). The BSP self-metrics pattern shows how to attach observability to a pipeline without inverting ownership. The two surfaces have no cross-references today; there are no architectural blockers, just absent wiring.

## Subsystem Identity

- Register a new closed-enum subsystem `knowledge_policy` in `pkg/observability/metrics.go` by appending the string to the `allowedSubsystems` slice.
- New file `pkg/observability/catalog_knowledge_policy.go` following the shape of `catalog_cypher.go`:
  - `KnowledgePolicyMetrics` struct holding pre-bound instruments.
  - Constructor `NewKnowledgePolicyMetrics(reg *Registry, tenantLabelsEnabled bool) *KnowledgePolicyMetrics`.
  - Pre-bind hot-path fires (scored counter, decay-score histogram) so call sites do a straight `m.ObserveScore(...)` with zero per-call label allocation.

### Instrument Catalog

| Name | OTel type | Labels | Fires when | Interpretation |
|---|---|---|---|---|
| `nornicdb_knowledge_policy_scored_total` | Int64Counter | `entity_kind{node,edge,property}`, `result{visible,suppressed,no_decay}` | End of `Scorer.score()` before return, plus early-return paths | Total scoring evaluations; `suppressed / visible` ratio = working-set attrition |
| `nornicdb_knowledge_policy_decay_score` | Float64Histogram (buckets 0.0, 0.1, …, 1.0), **sampled 1/32** | `entity_kind` | Same site as scored_total | Post-decay score distribution; bimodal = healthy, flat = misconfigured half-life |
| `nornicdb_knowledge_policy_suppressions_total` | Int64Counter | `entity_kind`, `reason{below_threshold,score_floor,on_access,explicit_flag,rule_cap}` | Scorer suppression path, read-path filter flag path, on-access suppression | Why suppressions happen — critical for tuning |
| `nornicdb_knowledge_policy_access_flush_batch_rows` | Float64Histogram (RowCountBuckets) | — | `AccessFlusher.flush()` after `DrainAll()` | Batch pressure; p99 ≈ maxBufferSize indicates backpressure |
| `nornicdb_knowledge_policy_access_flush_duration_seconds` | Float64Histogram (LatencyBucketsSeconds) | — | Wrap flush body; record on return | Flush cost; correlates with storage write p99 |
| `nornicdb_knowledge_policy_access_flush_buffer_fullness` | GaugeFunc | — | Passive scrape reads `len(accumulator.buffer) / maxBufferSize` | Tripwire for flush-interval tuning |
| `nornicdb_knowledge_policy_on_access_mutations_total` | Int64Counter | `result{applied,skipped_no_policy,error}` | `applyOnAccessMutations` + `evaluatePropertySuppression` | On-access policy workload |
| `nornicdb_knowledge_policy_deindex_enqueued_total` | Int64Counter | `entity_kind{node,edge}` | `EnqueueDeindexIfSuppressed` caller after `becameSuppressed=true` | Downstream search-index cost generator |
| `nornicdb_knowledge_policy_read_filter_dropped_total` | Int64Counter | `entity_kind{node,edge}` | `filterNodeByDecay` / `filterEdgeByDecay` return-true path | Read-path suppression rate |
| `nornicdb_knowledge_policy_reconcile_total` | Int64Counter | `trigger{schema_change,startup,manual}` | `ReconcileDecaySuppressionWithChanges` callers | Schema-driven churn |

All labels are closed enums and bounded. No user-defined label or property-key axis; see "Cardinality discipline" below.

## Where the Scorer Gets Its Meter

Split approach along the control-flow boundary:

- **Constructor injection for `AccessFlusher` and `Scorer`** — functional option `WithMetrics(m *observability.KnowledgePolicyMetrics)` on both. Wire through `pkg/nornicdb/db.go` the same way `cypherMetrics` flows into `exec.SetCypherMetrics` in `main.go`.
- **Package-level `atomic.Pointer` for the Badger read-path filter** — mirror `bsp_self_metrics.go`'s `bspMetricsRefs` idiom. New file `pkg/observability/knowledgepolicy_metrics_ref.go` holds `kpMetricsRefs atomic.Pointer[KnowledgePolicyMetrics]` with `SetKnowledgePolicyMetrics` / `GetKnowledgePolicyMetrics` accessors. `pkg/storage/badger_decay_filter.go` calls the getter on each filter invocation and no-ops on nil.

Trade-off: atomic-pointer is harder to unit-test (tests must set/restore the global) and a forgotten `SetKnowledgePolicyMetrics()` produces silent no-ops. Mitigated by a `provider_test.go` assertion that the pointer is non-nil after `Provider.Start()` when `Memory.DecayEnabled` is true.

## Tracing

Spans only at coarse-grained boundaries — scoring is too hot.

- `nornicdb.knowledge_policy.flush` — wraps `AccessFlusher.Flush()`. Attributes: `batch.row_count`, `batch.suppressed_count`, `batch.deindex_enqueued_count`, `batch.property_suppression_changes`. Downstream storage spans (`nornicdb.storage.Put`) nest beneath, producing one coherent flush trace.
- `nornicdb.knowledge_policy.reconcile` — wraps `ReconcileDecaySuppressionWithChanges`. Attributes: `trigger`, `changes_count`, `tokens_invalidated`.
- **No per-scoring-call span.** Metric-only. Optional `Observability.DebugSpans` flag enables per-evaluation spans for investigation; off by default.

## Cardinality Discipline

Tenant-label scoping respects `cfg.Observability.Metrics.TenantLabelsEnabled` exactly like the existing catalogs — pass `tenantLabelsEnabled` into `NewKnowledgePolicyMetrics` and omit the `database` labelname when false.

**Explicit exclusions:**
- Graph-schema `label` (e.g., `Product`, `User`) — unbounded from user DDL. NOT a metric label. Route to exemplar trace attributes.
- Property keys — same reasoning.

If per-label audit is ever needed, use structured logs or trace attributes — not Prometheus labels.

## Wiring Sequence

Edits in this order:

1. `pkg/observability/metrics.go` — append `"knowledge_policy"` to `allowedSubsystems`.
2. `pkg/observability/catalog_knowledge_policy.go` — new file, constructor + struct + methods.
3. `pkg/observability/knowledgepolicy_metrics_ref.go` — new file, `atomic.Pointer` accessors.
4. `cmd/nornicdb/main.go` — construct `kpMetrics` alongside `NewCypherMetrics` / `NewStorageMetrics`; call `SetKnowledgePolicyMetrics`; hand to `db.AttachKnowledgePolicyMetrics(kpMetrics)`.
5. `pkg/nornicdb/db.go` — attach handle to `accessFlusher` and `BadgerEngine` scorer factory inside the `config.Memory.DecayEnabled` block.
6. `pkg/knowledgepolicy/scorer.go` — record counter + histogram just before return in `score()`; early-return paths get `result="no_decay"`.
7. `pkg/knowledgepolicy/access_flusher.go` — span, duration, batch-row histogram, on-access counter, deindex counter fires.
8. `pkg/knowledgepolicy/on_access_runtime.go` — increment `on_access_mutations_total{result=…}`.
9. `pkg/storage/badger_decay_filter.go` — increment `read_filter_dropped_total{entity_kind}` via the global pointer on the suppress-true path.
10. `pkg/nornicdb/db.go` reconcile path — counter + span.

## Testing Plan

- `pkg/knowledgepolicy/metrics_test.go` (new) — table-driven test: known `CompiledBinding` + access-meta inputs fed through `Scorer.score`; assert counter deltas via Prometheus testutil, histogram buckets via `reg.Gather()`. Mirror `pkg/cypher/executor_spans_test.go`'s `tracetest.NewInMemoryExporter` for span assertions.
- `pkg/knowledgepolicy/access_flusher_metrics_test.go` (new) — fake `AccessMetaStore`; assert `access_flush_batch_rows` records exactly one sample per `Flush()` call, including zero-batch short-circuit.
- `pkg/observability/catalog_knowledge_policy_test.go` (new) — constructor smoke test against a fresh `*Registry`; `AssertCardinalityCeiling` for suppressions (3 entity_kind × 5 reason = 15 series cap).
- Extend existing knowledge-policy e2e in `pkg/nornicdb/` — seed a node, tick past half-life, force flush, scrape via `promtest.CollectAndCount`, assert presence of new series.

## Documentation

- `docs/observability/knowledge-policy-metrics.md` (new) — instrument reference: name, type, labels, fires-when, interpretation, recommended alert thresholds.
- `docs/architecture/adr/0001-observability.md` — append `knowledge_policy` to the subsystem list; cross-link new metrics doc.
- `docs/plans/knowledge-layer-persistence-plan.md` — new "Observability" section linking the metrics doc and calling out runbook thresholds (e.g., "`buffer_fullness` > 0.9 sustained → raise `AccessFlushBufferSize` or lower `DecayInterval`").

## Rollout Considerations

- **Feature flag.** Gate subsystem registration on `cfg.Observability.Metrics.KnowledgePolicyEnabled`, defaulting to `true` when `Memory.DecayEnabled` is true and `false` otherwise. Runtime toggling NOT supported — metric registration is one-shot.
- **Default dashboard.** Five series: `scored_total` rate, `suppressions_total` rate by reason, `decay_score` p50/p95, `access_flush_duration_seconds` p99, `deindex_enqueued_total` rate.
- **Debug dashboard.** `buffer_fullness` gauge, `reconcile_total`, per-reason suppression breakdown.
- **Experimental.** `read_filter_dropped_total` flagged experimental for first two releases — most likely to need label-axis revision once real-world cardinality is observed.

## Risks / Open Questions

- **Scoring hot-path cost.** `Scorer.score()` runs per node returned from any Cypher match when decay is enabled. A pre-bound `counter.Add(1)` is ~15 ns; a `histogram.Observe` is ~50 ns. At 1M-row scans that's 65 ms of metric overhead. **Mitigation:** sample the histogram 1/32 and document it.
- **Tenant-label propagation from `AccessFlusher`.** The flusher is shared across namespaces; per-tenant sub-aggregation doesn't exist. Verdict: flush-level metrics are NOT tenant-scoped, only scoring metrics are. Documented asymmetry.
- **Init-order gap.** `be.SetDecayEnabled(true)` runs before `main.go` constructs `kpMetrics`. Early reads may miss metrics. **Recommend:** move metrics construction before `db.Open` so the ref is set first.
- **`result="no_decay"` dominance.** The early-return path for entities without any binding may dominate `scored_total` and obscure the `suppressed/visible` ratio. Consider a separate `unscored_total` counter if the noise is bad.
- **`reason` enum closure.** On-access suppression has sub-reasons (cap hit, floor hit, rule-based). Pre-register all five anticipated reasons now (`below_threshold`, `score_floor`, `on_access`, `explicit_flag`, `rule_cap`) to avoid a breaking enum extension.

## Critical Files

- `pkg/observability/metrics.go`
- `pkg/observability/catalog_knowledge_policy.go` (new)
- `pkg/observability/knowledgepolicy_metrics_ref.go` (new)
- `pkg/knowledgepolicy/scorer.go`
- `pkg/knowledgepolicy/access_flusher.go`
- `pkg/knowledgepolicy/on_access_runtime.go`
- `pkg/storage/badger_decay_filter.go`
- `pkg/nornicdb/db.go`
- `cmd/nornicdb/main.go`
