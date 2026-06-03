package cypher

import "testing"

import "github.com/stretchr/testify/require"

func TestCompoundQueryShapeMatcher_Branches(t *testing.T) {
	q1 := "MATCH (a:A), (b:B) WITH a LIMIT 3 CREATE (a)-[r:REL]->(b) DELETE r"
	m, ok := matchCompoundCreateDeleteRelShape(q1)
	require.True(t, ok)
	require.Equal(t, shapeKindCompoundCreateDeleteRel, m.Kind)
	require.Equal(t, "A", m.Captures.String("label1"))
	require.Equal(t, "3", m.Probe.CapturedFields["limit"])

	q2 := "MATCH (a:A {id:1}), (b:B {id:2}) CREATE (a)-[r:REL]->(b) DELETE r"
	m, ok = matchCompoundPropCreateDeleteRelShape(q2)
	require.True(t, ok)
	require.Equal(t, shapeKindCompoundPropCreateDeleteRel, m.Kind)
	require.Equal(t, "id", m.Captures.String("prop1"))

	q3 := "MATCH (a:A {id:1}), (b:B {id:2}) CREATE (a)-[r:REL]->(b) WITH r DELETE r RETURN count(r)"
	m, ok = matchCompoundPropCreateDeleteReturnCountRelShape(q3)
	require.True(t, ok)
	require.Equal(t, shapeKindCompoundPropCreateDeleteReturnCountRel, m.Kind)

	all, ok := matchCompoundQueryShape(q3)
	require.True(t, ok)
	require.Equal(t, shapeKindCompoundPropCreateDeleteReturnCountRel, all.Kind)

	all, ok = matchCompoundQueryShape("RETURN 1")
	require.False(t, ok)
	require.Equal(t, shapeKindUnknown, all.Kind)
	require.False(t, all.Probe.Matched)
}

func TestCompoundQueryShapeMatcher_HelperParsers(t *testing.T) {
	left, right, ok := splitTopLevelCommaShape("(a:A {x:'1,2'}), (b:B)")
	require.True(t, ok)
	require.Equal(t, "(a:A {x:'1,2'})", left)
	require.Equal(t, "(b:B)", right)

	_, _, ok = splitTopLevelCommaShape("(a:A)")
	require.False(t, ok)

	w, ok := firstClauseWord("   foo bar")
	require.True(t, ok)
	require.Equal(t, "foo", w)
	_, ok = firstClauseWord("   ")
	require.False(t, ok)

	n, ok := parseLabeledNodePattern("(n:User {id: 1})")
	require.True(t, ok)
	require.Equal(t, "User", n.label)
	require.Equal(t, "id", n.propKey)
	require.Equal(t, "1", n.propValue)

	_, ok = parseLabeledNodePattern("(n)")
	require.False(t, ok)

	k, v, ok := parseSinglePropertyAssignment("id: 1")
	require.True(t, ok)
	require.Equal(t, "id", k)
	require.Equal(t, "1", v)
	_, _, ok = parseSinglePropertyAssignment("bad")
	require.False(t, ok)

	cp, ok := parseCreateRelationshipClause("CREATE (a)-[r:REL]->(b)")
	require.True(t, ok)
	require.Equal(t, "r", cp.relVar)
	require.Equal(t, "REL", cp.relType)
	_, ok = parseCreateRelationshipClause("CREATE (a)-[:REL]->(b)")
	require.False(t, ok)

	name, rest, ok := parseBareNodeReference("(node) tail")
	require.True(t, ok)
	require.Equal(t, "node", name)
	require.Equal(t, " tail", rest)
	_, _, ok = parseBareNodeReference("()")
	require.False(t, ok)

	id, trailing, ok := parseIdentifierToken("abc_1 tail")
	require.True(t, ok)
	require.Equal(t, "abc_1", id)
	require.Equal(t, " tail", trailing)
	_, _, ok = parseIdentifierToken("1abc")
	require.False(t, ok)

	inside, rest, ok := extractParenSection("(a(b))x")
	require.True(t, ok)
	require.Equal(t, "a(b)", inside)
	require.Equal(t, "x", rest)
	_, _, ok = extractParenSection("(a")
	require.False(t, ok)

	inside, rest, ok = extractBracketSection("[a['x']]y")
	require.True(t, ok)
	require.Equal(t, "a['x']", inside)
	require.Equal(t, "y", rest)
	_, _, ok = extractBracketSection("[a")
	require.False(t, ok)
}

func TestCompoundQueryShapeMatcher_Rejections(t *testing.T) {
	m, ok := matchCompoundCreateDeleteRelShape("WITH x RETURN x")
	require.False(t, ok)
	require.Contains(t, m.Probe.RejectReason, "missing leading MATCH")

	m, ok = matchCompoundCreateDeleteRelShape("MATCH (a:A), (b:B) WITH a LIMIT x CREATE (a)-[r:REL]->(b) DELETE r")
	require.False(t, ok)
	require.Contains(t, m.Probe.RejectReason, "invalid LIMIT")

	m, ok = matchCompoundPropCreateDeleteRelShape("MATCH (a:A), (b:B) CREATE (a)-[r:REL]->(b)")
	require.False(t, ok)
	require.Contains(t, m.Probe.RejectReason, "shape not found")

	m, ok = matchCompoundPropCreateDeleteReturnCountRelShape("MATCH (a:A {id:1}), (b:B {id:2}) CREATE (a)-[r:REL]->(b) WITH z DELETE r RETURN count(r)")
	require.False(t, ok)
	require.Contains(t, m.Probe.RejectReason, "variable mismatch")
}
