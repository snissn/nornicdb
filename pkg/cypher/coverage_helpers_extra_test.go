package cypher

import (
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"
)

func TestAggregateExpressionHelpers(t *testing.T) {
	cases := []struct {
		expr        string
		isAggregate bool
		identity    interface{}
	}{
		{expr: "collect(n)", isAggregate: true, identity: []interface{}{}},
		{expr: " COUNT (*)", isAggregate: true, identity: int64(0)},
		{expr: "sum (n.score)", isAggregate: true, identity: int64(0)},
		{expr: "avg(n.score)", isAggregate: true, identity: nil},
		{expr: "n.name", isAggregate: false, identity: nil},
	}
	for _, tc := range cases {
		t.Run(tc.expr, func(t *testing.T) {
			require.Equal(t, tc.isAggregate, isAggregateExpression(tc.expr))
			require.Equal(t, tc.identity, aggregateIdentity(tc.expr))
		})
	}

	require.True(t, rowHasAggregate([]returnItem{{expr: "n.name"}, {expr: "max(n.age)"}}))
	require.False(t, rowHasAggregate([]returnItem{{expr: "n.name"}, {expr: "n.age"}}))
}

func TestSmallCypherHelperBranches(t *testing.T) {
	require.Equal(t, []string{"Memory", "Evidence", "Session"}, splitCSVLabels(" Memory, Evidence ,, Session "))
	require.Empty(t, splitCSVLabels(" , , "))

	policy := minimalPolicyMap("node-1")
	require.Equal(t, "node-1", policy["targetId"])
	require.Equal(t, string(knowledgepolicy.ScopeNode), policy["targetScope"])

	writeQueries := []string{
		"CREATE (n)",
		"match (n) merge (m)",
		"MATCH (n) DELETE n",
		"MATCH (n) SET n.name = 'a'",
		"MATCH (n) REMOVE n.name",
	}
	for _, query := range writeQueries {
		require.True(t, looksLikeWriteQuery(query), query)
	}
	require.False(t, looksLikeWriteQuery("MATCH (n) RETURN n"))
}

func TestSliceAndNumericCoercionHelpers(t *testing.T) {
	input := []map[string]interface{}{{"a": 1}, {"b": "two"}}
	converted := toAnySlice(input)
	require.Len(t, converted, 2)
	require.Equal(t, map[string]interface{}{"a": 1}, converted[0])
	require.Equal(t, []interface{}{1, "two"}, toAnySlice([]interface{}{1, "two"}))
	require.Nil(t, toAnySlice("not a slice"))

	intCases := []interface{}{int(1), int8(2), int16(3), int32(4), int64(5), uint(6), uint8(7), uint16(8), uint32(9), uint64(10)}
	for i, v := range intCases {
		got, ok := coerceInt64(v)
		require.True(t, ok)
		require.Equal(t, int64(i+1), got)
	}
	_, ok := coerceInt64("1")
	require.False(t, ok)

	gotFloat, ok := coerceFloat64(float32(1.5))
	require.True(t, ok)
	require.InDelta(t, 1.5, gotFloat, 0.00001)
	gotFloat, ok = coerceFloat64(int64(2))
	require.True(t, ok)
	require.Equal(t, float64(2), gotFloat)
	_, ok = coerceFloat64("2")
	require.False(t, ok)
}

func TestQueryPatternIdentifierHelpers(t *testing.T) {
	require.Equal(t, []string{"a", "b_2", "c"}, extractNodeVariables("MATCH (a:Person)-[:KNOWS]->(b_2 {id: 1}), (c)"))
	require.Equal(t, []string{"_anon"}, extractNodeVariables("MATCH (_anon)"))
	require.Empty(t, extractNodeVariables("MATCH (:Label)-[]->()"))

	name, next, ok := scanIdentifierToken("abc_123 rest", 0)
	require.True(t, ok)
	require.Equal(t, "abc_123", name)
	require.Equal(t, len("abc_123"), next)

	_, _, ok = scanIdentifierToken("1abc", 0)
	require.False(t, ok)
	_, _, ok = scanIdentifierToken("abc", -1)
	require.False(t, ok)
}

func TestRedactSilentErrorListenerSyntaxError(t *testing.T) {
	listener := &redactSilentErrorListener{}
	require.False(t, listener.hadError)
	listener.SyntaxError(nil, nil, 0, 0, "boom", nil)
	require.True(t, listener.hadError)
}

func TestNormalizeInterfaceMap(t *testing.T) {
	got := normalizeInterfaceMap(map[interface{}]interface{}{1: "one", "two": 2, true: "yes"})
	want := map[string]interface{}{"1": "one", "two": 2, "true": "yes"}
	require.True(t, reflect.DeepEqual(want, got), "got %#v", got)
}
