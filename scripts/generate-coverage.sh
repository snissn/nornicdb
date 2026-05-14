#!/usr/bin/env bash
#
# Per-package coverage generator.
#
# Modes:
#   generate-coverage.sh [raw_out] [out]
#       Default mode (no extra args): discover every coverable package
#       under ./pkg/..., run them, and produce filtered coverage.
#
#   generate-coverage.sh --append raw_out group_name pkg1 pkg2 ...
#       CI-driven mode: append coverage for the listed packages into
#       raw_out under the named group. Lets the workflow split the
#       coverage run into chunks (matching the test-step grouping)
#       so a failing or hung group surfaces with a name attached
#       instead of silently killing the whole job.
#
#   generate-coverage.sh --filter raw_out out
#       Final filtering step after one or more --append calls. Emits
#       the cleaned coverage profile that goveralls consumes.
#
# Exclusions (kept in EXCLUDE_RE) drop packages that are not meaningful
# for line-coverage scoring: arch-/GPU-gated stubs, gqlgen output,
# protobuf-generated stubs, ANTLR scaffolding. Same regex used in both
# default and --append modes so the two paths agree on what counts.

set -euo pipefail

EXCLUDE_RE='github.com/orneryd/nornicdb/pkg/cypher/(fn|testutil)$|github.com/orneryd/nornicdb/pkg/nornicgrpc/gen$|github.com/orneryd/nornicdb/pkg/localllm$|github.com/orneryd/nornicdb/pkg/gpu($|/)|github.com/orneryd/nornicdb/pkg/graphql/generated$'

run_pkg_with_retry() {
	local pkg="$1"
	local prof="$2"
	local attempts=3

	for attempt in $(seq 1 "$attempts"); do
		if go test -p 1 -parallel 4 -timeout 30m -coverprofile="$prof" "$pkg"; then
			return 0
		fi
		if [ "$attempt" -lt "$attempts" ]; then
			echo "retrying coverage test for $pkg (attempt $((attempt + 1))/$attempts)" >&2
			sleep 1
		fi
	done

	echo "coverage test failed after $attempts attempts: $pkg" >&2
	return 1
}

# expand_patterns turns a list of go-style package patterns (./pkg/foo,
# ./pkg/foo/...) into the underlying real packages, applying the
# coverage exclusion regex along the way. We expand-then-filter so the
# CI workflow can pass wildcards without having to also know the
# exclusion list.
expand_patterns() {
	local patterns=("$@")
	if [ "${#patterns[@]}" -eq 0 ]; then
		return 0
	fi
	go list "${patterns[@]}" | grep -Ev "$EXCLUDE_RE" || true
}

append_group() {
	local raw_out="$1"
	local group="$2"
	shift 2
	local patterns=("$@")

	# Resolve patterns to a concrete package list before logging so
	# the CI run shows exactly what was covered in this group.
	local pkgs
	pkgs="$(expand_patterns "${patterns[@]}")"

	if [ -z "$pkgs" ]; then
		echo "coverage group '$group': no packages match patterns: ${patterns[*]}" >&2
		return 0
	fi

	# Lazily write the mode header on first append. The workflow
	# truncates the file before the first call, so "exists but empty"
	# is the on-disk shape we have to detect alongside "missing".
	if [ ! -s "$raw_out" ]; then
		echo "mode: set" > "$raw_out"
	fi

	local tmp_dir
	tmp_dir="$(mktemp -d)"
	trap 'rm -rf "$tmp_dir"' RETURN

	local i=0
	local start_ts
	start_ts="$(date +%s)"
	while IFS= read -r pkg; do
		[ -n "$pkg" ] || continue
		i=$((i + 1))
		local prof="$tmp_dir/$i.cover"
		run_pkg_with_retry "$pkg" "$prof"
		# Skip the per-package mode line; append statement blocks.
		tail -n +2 "$prof" >> "$raw_out"
	done <<< "$pkgs"
	local end_ts
	end_ts="$(date +%s)"
	echo "coverage group '$group' ok ($((end_ts - start_ts))s, $i packages)"
}

filter_only() {
	local raw_out="$1"
	local out="$2"
	bash scripts/filter-generated-coverage.sh "$raw_out" "$out"
}

case "${1:-}" in
	--append)
		shift
		if [ "$#" -lt 3 ]; then
			echo "usage: $0 --append raw_out group_name pkg1 [pkg2 ...]" >&2
			exit 2
		fi
		append_group "$@"
		exit 0
		;;
	--filter)
		shift
		if [ "$#" -ne 2 ]; then
			echo "usage: $0 --filter raw_out out" >&2
			exit 2
		fi
		filter_only "$@"
		exit 0
		;;
esac

# Default mode — discover and run everything in one shot. Kept so
# `bash scripts/generate-coverage.sh` continues to work for local
# pre-PR runs without needing to know the group names.
RAW_OUT="${1:-coverage.raw.out}"
OUT="${2:-coverage.out}"
: > "$RAW_OUT"
append_group "$RAW_OUT" all ./pkg/...
filter_only "$RAW_OUT" "$OUT"
