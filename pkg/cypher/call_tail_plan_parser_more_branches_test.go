package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestParseCallTailConstrainedMaxDepthPlan_InvalidShapes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_tail_constrained_more_cov"))
	ctx := context.Background()

	tests := []string{
		"WITH x RETURN x",
		"MATCH p = (node)-[:REL*1..4]->(x)",
		"MATCH p = (node)-[:REL*1..4]->(x) RETURN elementId(node) AS nodeID, score, max(length(p)) AS maxDepth",
		"MATCH (node)-[:REL*1..4]->(x) WHERE any(r IN relationships(p) WHERE r.weight >= 1) AND any(n IN nodes(p) WHERE n.category IN ['a']) RETURN elementId(node) AS nodeID, score, max(length(p)) AS maxDepth",
		"MATCH p = (node)-[:REL*1..4]->(x) WHERE any(r IN relationships(p) WHERE r.weight >= 1) RETURN elementId(node) AS nodeID, score, max(length(p)) AS maxDepth",
		"MATCH p = (node)-[:REL*1..4]->(x) WHERE any(r IN relationships(q) WHERE r.weight >= 1) AND any(n IN nodes(q) WHERE n.category IN ['a']) RETURN elementId(node) AS nodeID, score, max(length(p)) AS maxDepth",
		"MATCH p = (node)-[:REL*1..4]->(x) WHERE any(r IN relationships(p) WHERE r.weight >= 1) AND any(n IN nodes(p) WHERE n.category IN ['a']) RETURN node.id AS nodeID, score, max(length(p)) AS maxDepth",
		"MATCH p = (node)-[:REL*1..4]->(x) WHERE any(r IN relationships(p) WHERE r.weight >= 1) AND any(n IN nodes(p) WHERE n.category IN ['a']) RETURN elementId(node) AS nodeID, rank, max(length(p)) AS maxDepth",
		"MATCH p = (node)-[:REL*1..4]->(x) WHERE any(r IN relationships(p) WHERE r.weight >= 1) AND any(n IN nodes(p) WHERE n.category IN ['a']) RETURN elementId(node) AS nodeID, score, max(length(p))",
	}

	for _, tail := range tests {
		plan, ok := exec.parseCallTailConstrainedMaxDepthPlan(ctx, tail)
		require.False(t, ok, tail)
		require.Nil(t, plan, tail)
	}
}

func TestParseCallTailFrontierReachablePlan_InvalidShapes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_tail_frontier_more_cov"))
	ctx := context.Background()

	tests := []string{
		"RETURN 1",
		"MATCH (node)-[:REL*1..4]->(x) RETURN node",
		"MATCH (node)-[:REL*1..4]->(x) WITH node, score, length(shortestPath((node)-[:REL*1..4]->(x))) AS d RETURN node",
		"MATCH (node)-[:REL*1..4]->(x) WITH node, score, d WITH node, score, min(d) AS nearest, count(*) AS reachable RETURN elementId(node) AS nodeID, score, nearest, reachable",
		"MATCH (node)-[:REL*1..4]->(x) WITH node, score, length(shortestPath((node)-[:REL*1..4]->(x))) AS d WITH node, score, min(d) AS nearest, count(n) AS reachable RETURN elementId(node) AS nodeID, score, nearest, reachable",
		"MATCH (node)-[:REL*1..4]->(x) WITH node, score, length(shortestPath((node)-[:REL*1..4]->(x))) AS d WITH node, score, min(d) AS nearest, count(*) AS reachable RETURN node.id AS nodeID, score, nearest, reachable",
	}

	for _, tail := range tests {
		plan, ok := exec.parseCallTailFrontierReachablePlan(ctx, tail)
		require.False(t, ok, tail)
		require.Nil(t, plan, tail)
	}
}

func TestParseCallTailBranchingPathCountPlan_InvalidShapes(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "call_tail_branching_more_cov"))
	ctx := context.Background()

	tests := []string{
		"MATCH p = (node)-[:REL*1..4]->(x) RETURN p",
		"MATCH p = (node)-[:REL*1..4]->(x) WHERE node.id = 'x' WITH node, score, p, length(p) AS d WITH node, score, collect(p)[0..3] AS paths RETURN elementId(node) AS nodeID, score, size(paths) AS pathCount",
		"MATCH p = (node)-[:REL*1..4]->(x) WHERE ALL(n IN nodes(p) WHERE size(labels(n)) > 0) WITH node, score, p, length(p) AS d WITH node, score, collect(p) AS paths RETURN elementId(node) AS nodeID, score, size(paths) AS pathCount",
		"MATCH p = (node)-[:REL*1..4]->(x) WHERE ALL(n IN nodes(p) WHERE size(labels(n)) > 0) WITH node, score, p, length(p) AS d WITH node, score, collect(p)[0..3] AS paths RETURN node.id AS nodeID, score, size(paths) AS pathCount",
	}

	for _, tail := range tests {
		plan, ok := exec.parseCallTailBranchingPathCountPlan(ctx, tail)
		require.False(t, ok, tail)
		require.Nil(t, plan, tail)
	}
}
