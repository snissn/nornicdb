#!/usr/bin/env bash
#
# Run the storage-backend benchmark matrix used by the TreeDB backend work.
#
# The defaults intentionally disable CGO and the ambient go.work. That keeps
# storage-only evidence independent of local llama.cpp artifacts and unrelated
# workspace modules while still allowing Go's toolchain download to honor
# go.mod.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
cd "$REPO_ROOT"

OUT_DIR="${1:-benchmarks/storage-backends/$(date -u +%Y%m%dT%H%M%SZ)}"
COUNT="${BENCH_COUNT:-3}"
TIMEOUT="${BENCH_TIMEOUT:-30m}"
BENCHTIME="${BENCHTIME:-}"
RUN_STORAGE="${RUN_STORAGE:-1}"
RUN_CYPHER="${RUN_CYPHER:-1}"

STORAGE_BENCH_RE="${STORAGE_BENCH_RE:-^BenchmarkBadgerEngine_(CreateNode|GetNode|BulkCreateNodes)$|^BenchmarkTransaction_(CommitNodes|RollbackNodes)$|^BenchmarkWAL_(Append|AppendWithSync)$|^BenchmarkWALEngine_CreateNode$|^BenchmarkPersistentBadgerEngine_(CreateNode|GetNode|BulkCreateNodes|CreateEdge|BulkCreateEdges|TxnCreateNode|GetNodesByLabel|BatchGetNodes|GetOutgoingEdges)$|^BenchmarkNamespacedPersistentBadgerEngine_(CreateNode|GetNode|BulkCreateNodes|CreateEdge|BulkCreateEdges|TxnCreateNode|GetNodesByLabel|GetOutgoingEdges)$|^BenchmarkTreeDBEngine_(CreateNode|GetNode|BulkCreateNodes|CreateEdge|BulkCreateEdges|TxnCreateNode|GetNodesByLabel|BatchGetNodes|GetOutgoingEdges)$|^BenchmarkNamespacedTreeDBEngine_(CreateNode|GetNode|BulkCreateNodes|CreateEdge|BulkCreateEdges|TxnCreateNode|GetNodesByLabel|GetOutgoingEdges)$|^BenchmarkDirectTreeDB_(GraphCreateNodeEquivalent|GraphBulkCreateNodesEquivalent|GraphGetNodeEquivalent|GraphCreateEdgeEquivalent|GraphBulkCreateEdgesEquivalent|GraphGetNodesByLabelEquivalent|GraphGetOutgoingEdgesEquivalent)$}"
CYPHER_BENCH_RE="${CYPHER_BENCH_RE:-^BenchmarkSeedBareCreate$|^BenchmarkUnwindMergeBatch|^BenchmarkMatchRelationships_CountAll$|^BenchmarkFastPath_WithLimit$|^BenchmarkBackendCypherMatrix$}"

mkdir -p "$OUT_DIR"

export CGO_ENABLED="${CGO_ENABLED:-0}"
export GOWORK="${GOWORK:-off}"
if [ -d /mnt/fast4tb/tmp ]; then
	export TMPDIR="${TMPDIR:-/mnt/fast4tb/tmp}"
	export GOMODCACHE="${GOMODCACHE:-/mnt/fast4tb/tmp/gomod}"
	export GOCACHE="${GOCACHE:-/mnt/fast4tb/tmp/gocache}"
	export GOPATH="${GOPATH:-/mnt/fast4tb/tmp/go}"
fi

BENCHTIME_ARG=()
if [ -n "$BENCHTIME" ]; then
	BENCHTIME_ARG=(-benchtime "$BENCHTIME")
fi

{
	echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "git_head=$(git rev-parse HEAD)"
	echo "git_branch=$(git branch --show-current)"
	echo "go_version=$(go version)"
	echo "go_env_cgo=${CGO_ENABLED}"
	echo "go_env_gowork=${GOWORK}"
	echo "go_env_tmpdir=${TMPDIR:-}"
	echo "go_env_gomodcache=${GOMODCACHE:-}"
	echo "go_env_gocache=${GOCACHE:-}"
	echo "go_env_gopath=${GOPATH:-}"
	echo "bench_count=${COUNT}"
	echo "bench_timeout=${TIMEOUT}"
	echo "bench_time=${BENCHTIME:-go test default}"
	echo "cpu=$(awk -F: '/model name/ {gsub(/^ +/, "", $2); print $2; exit}' /proc/cpuinfo 2>/dev/null || true)"
	echo "kernel=$(uname -a)"
	echo "storage_bench_re=${STORAGE_BENCH_RE}"
	echo "cypher_bench_re=${CYPHER_BENCH_RE}"
} > "$OUT_DIR/context.txt"

cat > "$OUT_DIR/gates.txt" <<'GATES'
TreeDB primitive persisted writes: >=80% of direct equivalent TreeDB throughput.
TreeDB bulk writes: >=85% of direct equivalent TreeDB throughput.
TreeDB wrapper overhead: <=2 allocs/op over direct equivalent TreeDB operations where structurally possible.
Badger runtime regression target: <=3%.
Badger allocations target: no new steady-state alloc/op.
Badger bytes target: <=5% B/op increase unless justified.
GATES
cat "$OUT_DIR/gates.txt"

if [ "$RUN_STORAGE" = "1" ]; then
	go test ./pkg/storage \
		-run '^$' \
		-bench "$STORAGE_BENCH_RE" \
		-benchmem \
		"${BENCHTIME_ARG[@]}" \
		-count "$COUNT" \
		-timeout "$TIMEOUT" | tee "$OUT_DIR/storage.txt"
fi

if [ "$RUN_CYPHER" = "1" ]; then
	go test ./pkg/cypher \
		-run '^$' \
		-bench "$CYPHER_BENCH_RE" \
		-benchmem \
		"${BENCHTIME_ARG[@]}" \
		-count "$COUNT" \
		-timeout "$TIMEOUT" | tee "$OUT_DIR/cypher.txt"
fi

{
	echo -e "suite\tbenchmark\tns_per_op\tops_per_sec\tbytes_per_op\tallocs_per_op\tmb_per_sec"
	for result in storage cypher; do
		file="$OUT_DIR/${result}.txt"
		if [ ! -f "$file" ]; then
			continue
		fi
		awk -v suite="$result" '
			/^Benchmark/ {
				ns = ""; bytes = ""; allocs = ""; mb = "";
				for (i = 1; i <= NF; i++) {
					if ($i == "ns/op") {
						ns = $(i - 1)
					} else if ($i == "B/op") {
						bytes = $(i - 1)
					} else if ($i == "allocs/op") {
						allocs = $(i - 1)
					} else if ($i == "MB/s") {
						mb = $(i - 1)
					}
				}
				if (ns != "") {
					ops = 1000000000 / ns
					printf "%s\t%s\t%s\t%.2f\t%s\t%s\t%s\n", suite, $1, ns, ops, bytes, allocs, mb
				}
			}
		' "$file"
	done
} > "$OUT_DIR/summary.tsv"

echo "wrote benchmark artifacts to $OUT_DIR"
echo "summary: $OUT_DIR/summary.tsv"
