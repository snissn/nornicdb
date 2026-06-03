package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestUnwindParserHelpers_AdditionalBranches(t *testing.T) {
	assign, ok := parseUnwindSimpleMergeMatchAssignments("id: row.id, name: row.name")
	require.True(t, ok)
	require.Len(t, assign, 2)
	require.Equal(t, "id", assign[0].prop)
	require.Equal(t, "row.id", assign[0].expr)

	_, ok = parseUnwindSimpleMergeMatchAssignments("id row.id")
	require.False(t, ok)

	setAssign, ok := parseUnwindSimpleSetAssignments("n.k = row.k, n += row.props", "n")
	require.True(t, ok)
	require.Len(t, setAssign, 2)
	require.Equal(t, "k", setAssign[0].prop)
	require.True(t, setAssign[1].mergeMap)

	_, ok = parseUnwindSimpleSetAssignments("m.k = row.k", "n")
	require.False(t, ok)
	_, ok = parseUnwindSimpleSetAssignments("n +=", "n")
	require.False(t, ok)

	varName, labels, matchAssign, anyLabel, ok := parseUnwindNodePatternClauseInternal("MATCH (n:LabelA|LabelB {id: row.id})", "MATCH", true)
	require.True(t, ok)
	require.Equal(t, "n", varName)
	require.Equal(t, []string{"LabelA", "LabelB"}, labels)
	require.True(t, anyLabel)
	require.Len(t, matchAssign, 1)

	_, _, _, _, ok = parseUnwindNodePatternClauseInternal("MATCH (n:LabelA|LabelB {id: row.id})", "MATCH", false)
	require.False(t, ok)

	withPlan, ok := parseUnwindWithClause("WITH n, row.score AS score")
	require.True(t, ok)
	require.Len(t, withPlan.assignments, 1)
	require.Equal(t, "score", withPlan.assignments[0].alias)
	require.Equal(t, "row.score", withPlan.assignments[0].expr)

	_, ok = parseUnwindWithClause("WITH n, row.score")
	require.False(t, ok)

	wherePlan, ok := parseUnwindWhereClause("WHERE score > 0")
	require.True(t, ok)
	require.Equal(t, "score > 0", wherePlan.clause)
	_, ok = parseUnwindWhereClause("WHERE   ")
	require.False(t, ok)

	clauses, ok := splitUnwindMergeChainClauses("MERGE (n:Node {id: row.id}) SET n.k = row.k")
	require.True(t, ok)
	require.Len(t, clauses, 2)
	_, ok = splitUnwindMergeChainClauses("BAD (n)")
	require.False(t, ok)

	rel, ok := parseUnwindMergeRelationshipClause("MERGE (a)-[r:REL]->(b)")
	require.True(t, ok)
	require.Equal(t, "a", rel.fromVar)
	require.Equal(t, "b", rel.toVar)
	require.Equal(t, "r", rel.relVar)
	require.Equal(t, "REL", rel.relType)

	_, ok = parseUnwindMergeRelationshipClause("MERGE (a)-[:REL]->(b)-[:X]->(c)")
	require.False(t, ok)

	stages, ok := splitUnwindCompoundMutationStages(
		"MERGE (a:Node {id: row.id}) WITH $rows AS rows UNWIND rows AS row MERGE (b:Node {id: row.id})",
		"rows",
		"row",
	)
	require.True(t, ok)
	require.Len(t, stages, 2)

	_, ok = splitUnwindCompoundMutationStages("MERGE (a:Node {id: row.id})", "rows", "")
	require.False(t, ok)
}

func TestSplitUnwindCompoundMutationStages_AdditionalRejections(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "with_clause_not_param",
			query: "MERGE (a:Node {id: row.id}) WITH rows AS rows UNWIND rows AS row MERGE (b:Node {id: row.id})",
		},
		{
			name:  "different_param_name",
			query: "MERGE (a:Node {id: row.id}) WITH $items AS rows UNWIND rows AS row MERGE (b:Node {id: row.id})",
		},
		{
			name:  "missing_first_as",
			query: "MERGE (a:Node {id: row.id}) WITH $rows rows UNWIND rows AS row MERGE (b:Node {id: row.id})",
		},
		{
			name:  "missing_unwind_keyword",
			query: "MERGE (a:Node {id: row.id}) WITH $rows AS rows WITH rows AS row MERGE (b:Node {id: row.id})",
		},
		{
			name:  "alias_mismatch",
			query: "MERGE (a:Node {id: row.id}) WITH $rows AS rows UNWIND other AS row MERGE (b:Node {id: row.id})",
		},
		{
			name:  "missing_second_as",
			query: "MERGE (a:Node {id: row.id}) WITH $rows AS rows UNWIND rows row MERGE (b:Node {id: row.id})",
		},
		{
			name:  "stage_var_mismatch",
			query: "MERGE (a:Node {id: row.id}) WITH $rows AS rows UNWIND rows AS item MERGE (b:Node {id: row.id})",
		},
		{
			name:  "empty_stage_before_with",
			query: "WITH $rows AS rows UNWIND rows AS row MERGE (b:Node {id: row.id})",
		},
		{
			name:  "empty_last_stage",
			query: "MERGE (a:Node {id: row.id}) WITH $rows AS rows UNWIND rows AS row",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stages, ok := splitUnwindCompoundMutationStages(tt.query, "rows", "row")
			require.False(t, ok)
			require.Nil(t, stages)
		})
	}
}

func TestUnwindLookupSetHasUniqueAnchor_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "unwind_lookup_unique_anchor_cov")
	exec := NewStorageExecutor(store)
	ctx := context.Background()

	_, err := exec.Execute(ctx, "CREATE CONSTRAINT uniq_user_id IF NOT EXISTS FOR (u:User) REQUIRE u.id IS UNIQUE", nil)
	require.NoError(t, err)

	schema := store.GetSchema()
	require.NotNil(t, schema)

	require.True(t, unwindLookupSetHasUniqueAnchor(schema, unwindMergeChainLookupPlan{
		labels:           []string{"User"},
		matchAssignments: []unwindSimpleSetAssignment{{prop: "id", expr: "row.id"}},
	}))

	require.False(t, unwindLookupSetHasUniqueAnchor(schema, unwindMergeChainLookupPlan{
		labels:           []string{"User"},
		matchAssignments: []unwindSimpleSetAssignment{{prop: "name", expr: "row.name"}},
	}))

	require.False(t, unwindLookupSetHasUniqueAnchor(schema, unwindMergeChainLookupPlan{
		anyLabel:         true,
		labels:           []string{"User", "Admin"},
		matchAssignments: []unwindSimpleSetAssignment{{prop: "id", expr: "row.id"}},
	}))
}

func TestParseUnwindMergeChainPattern_AdditionalRejections(t *testing.T) {
	plan := parseUnwindMergeChainPattern("MERGE (n:Node {id: row.id}) CALL db.labels()")
	require.False(t, plan.supported)

	plan = parseUnwindMergeChainPattern("MERGE (a:Node {id: row.id}) MERGE (a)-[:REL]->(b)")
	require.False(t, plan.supported)

	// Relationship SET requires a relationship variable.
	plan = parseUnwindMergeChainPattern("MERGE (a:Node {id: row.a}) MERGE (b:Node {id: row.b}) MERGE (a)-[:REL]->(b) SET r.k = row.k")
	require.False(t, plan.supported)

	// Supported mixed lookup + merge + rel chain.
	plan = parseUnwindMergeChainPattern(`
MATCH (u:User {id: row.user_id})
MERGE (p:Post {id: row.post_id})
SET p.title = row.title
MERGE (u)-[r:AUTHORED]->(p)
SET r.source = row.source
`)
	require.True(t, plan.supported)
	require.GreaterOrEqual(t, len(plan.steps), 3)
}
