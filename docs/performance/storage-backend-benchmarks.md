# Storage Backend Benchmark Matrix

This matrix is the baseline gate for the TreeDB backend work. It exists so
backend-selection, transaction, durability, metrics, and TreeDB implementation
PRs compare against the same Badger evidence instead of using one-off commands.

## Canonical Command

```bash
BENCH_COUNT=10 scripts/benchmark_storage_backends.sh
```

The script writes `context.txt`, `storage.txt`, and `cypher.txt` under
`benchmarks/storage-backends/<timestamp>/`. By default it uses:

- `GOWORK=off`, so unrelated local workspaces do not affect the run.
- `CGO_ENABLED=0`, so storage benchmarks do not require local llama.cpp
  libraries. CI still hydrates llama libraries for full builds.
- `/mnt/fast4tb/tmp` as `TMPDIR` when present, to keep temporary benchmark data
  off small root-backed filesystems.

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
