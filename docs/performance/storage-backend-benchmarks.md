# Storage Backend Benchmark Matrix

This matrix is the baseline gate for the TreeDB backend work. It exists so
backend-selection, transaction, durability, metrics, and TreeDB implementation
PRs compare against the same Badger evidence instead of using one-off commands.

## Canonical Command

```bash
TMPDIR=/mnt/fast4tb/tmp \
GOWORK=off \
GOMODCACHE=/mnt/fast4tb/tmp/gomod \
GOCACHE=/mnt/fast4tb/tmp/gocache \
GOPATH=/mnt/fast4tb/tmp/go \
CGO_ENABLED=0 \
BENCH_COUNT=10 \
scripts/benchmark_storage_backends.sh
```

The script writes these artifacts under
`benchmarks/storage-backends/<timestamp>/`:

- `context.txt`: git, Go, CPU, environment, timeout, count, and regex context.
- `gates.txt`: TreeDB and Badger regression budgets.
- `storage.txt`: raw `go test ./pkg/storage -bench` output.
- `cypher.txt`: raw `go test ./pkg/cypher -bench` output.
- `summary.tsv`: compact report with benchmark name, `ns/op`, derived
  ops/sec, `B/op`, `allocs/op`, and `MB/s` when Go reports it.

By default it uses:

- `GOWORK=off`, so unrelated local workspaces do not affect the run.
- `CGO_ENABLED=0`, so storage benchmarks do not require local llama.cpp
  libraries. CI still hydrates llama libraries for full builds.
- `/mnt/fast4tb/tmp` as `TMPDIR`, `GOMODCACHE`, `GOCACHE`, and `GOPATH` when
  present, to keep temporary benchmark data and Go caches off small
  root-backed filesystems.

For smoke runs, keep the same matrix but shorten the timer:

```bash
TMPDIR=/mnt/fast4tb/tmp GOWORK=off GOMODCACHE=/mnt/fast4tb/tmp/gomod \
GOCACHE=/mnt/fast4tb/tmp/gocache GOPATH=/mnt/fast4tb/tmp/go CGO_ENABLED=0 \
BENCH_COUNT=1 BENCHTIME=1x scripts/benchmark_storage_backends.sh
```

## Default Matrix

The default storage regex keeps the existing Badger/WAL regression gates and
adds persistent Badger, TreeDB wrapper, namespaced TreeDB, and direct TreeDB
equivalent rows:

| Backend surface | Benchmark families |
| --- | --- |
| Existing Badger gates | `BenchmarkBadgerEngine_*`, `BenchmarkTransaction_*`, `BenchmarkWAL_*`, `BenchmarkWALEngine_CreateNode` |
| Persistent Badger direct | `BenchmarkPersistentBadgerEngine_*` |
| Persistent Badger namespaced/server-chain | `BenchmarkNamespacedPersistentBadgerEngine_*` |
| TreeDB wrapper | `BenchmarkTreeDBEngine_*` |
| TreeDB namespaced/server-chain | `BenchmarkNamespacedTreeDBEngine_*` |
| Direct TreeDB equivalent | `BenchmarkDirectTreeDB_Graph*Equivalent` |

The default storage workloads include node create/get, bulk node create, edge
create, bulk edge create, transaction create, label lookup, batch get, and
outgoing adjacency reads where the backend surface supports the operation.

The default Cypher regex keeps the previous memory-backed query-shape gates and
adds `BenchmarkBackendCypherMatrix`, which runs the same representative Cypher
workloads against persistent namespaced Badger and persistent namespaced
TreeDB:

| Cypher workload | Shape |
| --- | --- |
| `BareCreateBatch100` | `UNWIND` plus `CREATE` for 100 property nodes |
| `LabelCountRead256` | `MATCH (n:BenchNode) RETURN count(n)` |
| `RelationshipCount255` | `MATCH ()-[r:BENCH_LINK]->() RETURN count(r)` |
| `ShortestPath64` | bounded `shortestPath` traversal over a 64-node chain |

## Required Comparison

Every PR that touches storage, transactions, WAL, backend configuration,
capability discovery, query-visible graph writes, or graph-read hot paths must
record:

- baseline commit and candidate commit;
- exact command and environment;
- hardware/context from `context.txt`;
- fixture shape and timer boundary;
- `ns/op`, derived ops/sec, `B/op`, and `allocs/op`;
- `benchstat` output when comparing two runs;
- a short decision on whether any Badger regression is material.

The TreeDB tracker regression budget is:

- Badger storage and Cypher hot paths: no material regression, targeting <=3%
  runtime/throughput regression.
- Badger allocations: no new steady-state alloc/op in hot paths unless the PR
  proves the allocation is correctness-required and minimized.
- Badger bytes/op: <=5% increase unless explicitly accepted.

TreeDB-specific gates begin once the TreeDB engine exists:

- primitive persisted writes: >=80% of direct equivalent TreeDB throughput;
- bulk/batch writes: >=85% of direct equivalent TreeDB throughput;
- hot-path allocation overhead: <=2 allocs/op over direct equivalent TreeDB
  operations, or a profiled blocker must be linked before closeout.

Use throughput from `summary.tsv` for TreeDB direct-equivalent ratios:

- primitive persisted writes: `TreeDBEngine_CreateNode` and
  `NamespacedTreeDBEngine_CreateNode` versus
  `DirectTreeDB_GraphCreateNodeEquivalent`;
- bulk writes: `TreeDBEngine_BulkCreateNodes`,
  `NamespacedTreeDBEngine_BulkCreateNodes`,
  `TreeDBEngine_BulkCreateEdges`, and
  `NamespacedTreeDBEngine_BulkCreateEdges` versus their direct equivalents;
- allocation overhead: compare `allocs/op` for wrapper/namespaced rows against
  the closest `DirectTreeDB_Graph*Equivalent` row.

Use `benchstat` on raw `storage.txt` and `cypher.txt` files for Badger
candidate-vs-baseline comparisons. The target is <=3% runtime regression, no
new steady-state alloc/op, and <=5% B/op unless the PR documents why the
increase is required.

## Focused Reviewer Commands

Storage matrix listing:

```bash
TMPDIR=/mnt/fast4tb/tmp GOWORK=off GOMODCACHE=/mnt/fast4tb/tmp/gomod \
GOCACHE=/mnt/fast4tb/tmp/gocache GOPATH=/mnt/fast4tb/tmp/go CGO_ENABLED=0 \
go test ./pkg/storage -run '^$' \
  -list '^(BenchmarkPersistentBadgerEngine_|BenchmarkNamespacedPersistentBadgerEngine_|BenchmarkTreeDBEngine_|BenchmarkNamespacedTreeDBEngine_|BenchmarkDirectTreeDB_)'
```

Focused storage smoke:

```bash
TMPDIR=/mnt/fast4tb/tmp GOWORK=off GOMODCACHE=/mnt/fast4tb/tmp/gomod \
GOCACHE=/mnt/fast4tb/tmp/gocache GOPATH=/mnt/fast4tb/tmp/go CGO_ENABLED=0 \
go test ./pkg/storage -run '^$' \
  -bench '^(BenchmarkPersistentBadgerEngine_|BenchmarkNamespacedPersistentBadgerEngine_|BenchmarkTreeDBEngine_|BenchmarkNamespacedTreeDBEngine_|BenchmarkDirectTreeDB_)' \
  -benchmem -benchtime=1x -count=1 -timeout=30m
```

Focused Cypher smoke:

```bash
TMPDIR=/mnt/fast4tb/tmp GOWORK=off GOMODCACHE=/mnt/fast4tb/tmp/gomod \
GOCACHE=/mnt/fast4tb/tmp/gocache GOPATH=/mnt/fast4tb/tmp/go CGO_ENABLED=0 \
go test ./pkg/cypher -run '^$' -bench '^BenchmarkBackendCypherMatrix$' \
  -benchmem -benchtime=1x -count=1 -timeout=30m
```

Full reviewer run:

```bash
TMPDIR=/mnt/fast4tb/tmp GOWORK=off GOMODCACHE=/mnt/fast4tb/tmp/gomod \
GOCACHE=/mnt/fast4tb/tmp/gocache GOPATH=/mnt/fast4tb/tmp/go CGO_ENABLED=0 \
BENCH_COUNT=10 scripts/benchmark_storage_backends.sh
```

## TreeDB Substrate Target

The TreeDB backend implementation should target the public module path
`github.com/snissn/gomap/TreeDB`. The expected substrate APIs are:

- `treedb.Open(opts)` with `Options.Dir`, `CommandWAL`, `ReadOnly`, chunk,
  sync, and pager knobs;
- raw KV reads and writes through `Get`, `GetVersioned`, `Set`, `SetSync`,
  `Delete`, `DeleteSync`, iterators, snapshots, and batches;
- conditional writes through `NewConditionalTxn`, `SetWithRevision`,
  `DeleteWithRevision`, `Commit`, and `CommitSync`;
- entry revision metadata through `EntryRevision` and versioned reads;
- durability/maintenance boundaries through `CommitSync`, `Checkpoint`,
  `CompactIndex`, and `VacuumIndexOnline`.

The first TreeDB engine PR should treat TreeDB as a raw KV substrate under
NornicDB's existing graph key layout. TreeDB collections or alternate graph
layouts require separate evidence that they preserve NornicDB semantics and
improve the measured hot path.

## Initial Local Baseline Sample

This local sample verifies the baseline harness on the current checkout. It is
not a replacement for the full `BENCH_COUNT=10` baseline required before
mergeability of later storage PRs.

Context:

- Commit: `d7cddad903e5a5e1746971f65541563cd595b77b`
- Host: Linux amd64, Intel Core i5-11400F
- Go: `go1.26.4` downloaded through the Go toolchain mechanism
- Environment: `CGO_ENABLED=0 GOWORK=off`
- Command: `go test ./pkg/storage -run '^$' -bench '^BenchmarkBadgerEngine_CreateNode$' -benchmem -count=1`

| Benchmark | ns/op | ops/sec | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| `BenchmarkBadgerEngine_CreateNode-12` | 30,733 | 32,538 | 10,165 | 193 |

## Local Validation Notes

The ambient workspace at this host contains unrelated modules and stale paths,
so use `GOWORK=off` for this matrix. With CGO enabled, local storage tests link
against `lib/llama/libllama_linux_amd64`, which is not present on this machine;
the benchmark script disables CGO for storage-backend evidence. GitHub CI
hydrates llama libraries for full builds through
`scripts/hydrate-llama-cpu-libs.sh`.

A broad exploratory MVCC benchmark regex was also tested and is not part of the
default matrix because several retained-version sub-benchmarks currently fail
with `not found` or prune-retention assertions. Those benchmarks should be fixed
or invoked explicitly before they become required gates.
