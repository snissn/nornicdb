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
RUN_STORAGE="${RUN_STORAGE:-1}"
RUN_CYPHER="${RUN_CYPHER:-1}"

STORAGE_BENCH_RE="${STORAGE_BENCH_RE:-^BenchmarkBadgerEngine_(CreateNode|GetNode|BulkCreateNodes)$|^BenchmarkTransaction_(CommitNodes|RollbackNodes)$|^BenchmarkWAL_(Append|AppendWithSync)$|^BenchmarkWALEngine_CreateNode$}"
CYPHER_BENCH_RE="${CYPHER_BENCH_RE:-^BenchmarkSeedBareCreate$|^BenchmarkUnwindMergeBatch|^BenchmarkMatchRelationships_CountAll$|^BenchmarkFastPath_WithLimit$}"

mkdir -p "$OUT_DIR"

export CGO_ENABLED="${CGO_ENABLED:-0}"
export GOWORK="${GOWORK:-off}"
if [ -d /mnt/fast4tb/tmp ]; then
	export TMPDIR="${TMPDIR:-/mnt/fast4tb/tmp}"
fi

{
	echo "timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "git_head=$(git rev-parse HEAD)"
	echo "git_branch=$(git branch --show-current)"
	echo "go_version=$(go version)"
	echo "go_env_cgo=${CGO_ENABLED}"
	echo "go_env_gowork=${GOWORK}"
	echo "bench_count=${COUNT}"
	echo "bench_timeout=${TIMEOUT}"
	echo "cpu=$(awk -F: '/model name/ {gsub(/^ +/, "", $2); print $2; exit}' /proc/cpuinfo 2>/dev/null || true)"
	echo "kernel=$(uname -a)"
	echo "storage_bench_re=${STORAGE_BENCH_RE}"
	echo "cypher_bench_re=${CYPHER_BENCH_RE}"
} > "$OUT_DIR/context.txt"

if [ "$RUN_STORAGE" = "1" ]; then
	go test ./pkg/storage \
		-run '^$' \
		-bench "$STORAGE_BENCH_RE" \
		-benchmem \
		-count "$COUNT" \
		-timeout "$TIMEOUT" | tee "$OUT_DIR/storage.txt"
fi

if [ "$RUN_CYPHER" = "1" ]; then
	go test ./pkg/cypher \
		-run '^$' \
		-bench "$CYPHER_BENCH_RE" \
		-benchmem \
		-count "$COUNT" \
		-timeout "$TIMEOUT" | tee "$OUT_DIR/cypher.txt"
fi

echo "wrote benchmark artifacts to $OUT_DIR"
