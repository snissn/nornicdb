package cypher

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// tryFastPathMatchVectorCosine handles a strict vector-search shape:
//
//	MATCH (n:Label)
//	RETURN ..., vector.similarity.cosine(n.<prop>, <query>) AS <scoreAlias>, ...
//	ORDER BY <scoreAlias|same_expression> DESC
//	LIMIT <k>
//
// It requires a matching VECTOR index on (Label, prop) using cosine similarity.
// If any requirement is not met, it cleanly falls back to normal Cypher execution.
func (e *StorageExecutor) tryFastPathMatchVectorCosine(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, bool) {
	trimmed := strings.TrimSpace(cypher)
	if !strings.HasPrefix(strings.TrimSpace(upperQuery), "MATCH") {
		return nil, false
	}

	// Keep this shape intentionally narrow to avoid semantic drift.
	for _, kw := range []string{
		"WHERE", "SKIP", "WITH", "UNWIND", "OPTIONAL MATCH", "CALL",
		"CREATE", "MERGE", "DELETE", "DETACH DELETE", "SET", "REMOVE", "UNION",
	} {
		if findKeywordIndex(trimmed, kw) > 0 {
			return nil, false
		}
	}

	returnIdx := findKeywordIndex(trimmed, "RETURN")
	orderIdx := findKeywordIndex(trimmed, "ORDER BY")
	limitIdx := findKeywordIndex(trimmed, "LIMIT")
	if returnIdx <= 0 || orderIdx <= returnIdx || limitIdx <= orderIdx {
		return nil, false
	}

	matchPart := strings.TrimSpace(trimmed[len("MATCH"):returnIdx])
	varName, labels, ok := parseSimpleMatchSingleNodePattern(matchPart)
	if !ok || len(labels) != 1 {
		return nil, false
	}
	label := labels[0]

	returnPart := strings.TrimSpace(trimmed[returnIdx+len("RETURN") : orderIdx])
	returnItems := e.parseReturnItems(returnPart)
	if len(returnItems) == 0 {
		return nil, false
	}

	cosineIdx, vectorProp, queryExpr, scoreRef, ok := parseCosineReturnShape(returnItems, varName)
	if !ok {
		return nil, false
	}

	orderPart := strings.TrimSpace(trimmed[orderIdx+len("ORDER BY") : limitIdx])
	if !isDescendingOrderByScore(orderPart, scoreRef) {
		return nil, false
	}

	limitPart := strings.TrimSpace(trimmed[limitIdx+len("LIMIT"):])
	limit, ok := parseFastPathLimit(ctx, limitPart)
	if !ok || limit < 0 {
		return nil, false
	}
	if limit == 0 {
		columns := make([]string, len(returnItems))
		for i, item := range returnItems {
			if item.alias != "" {
				columns[i] = item.alias
			} else {
				columns[i] = item.expr
			}
		}
		e.markCosineVectorIndexFastPathUsed()
		return &ExecuteResult{Columns: columns, Rows: [][]interface{}{}, Stats: &QueryStats{}}, true
	}

	indexName, hasIndex := findCosineVectorIndexName(e.storage.GetSchema(), label, vectorProp)
	if !hasIndex {
		return nil, false
	}

	callQuery := fmt.Sprintf(
		"CALL db.index.vector.queryNodes('%s', %d, %s) YIELD node, score",
		strings.ReplaceAll(indexName, "'", "''"),
		limit,
		queryExpr,
	)
	callRes, err := e.callDbIndexVectorQueryNodes(ctx, callQuery)
	if err != nil {
		// Preserve existing behavior on query-shape mismatches/invalid params.
		return nil, false
	}

	columns := make([]string, len(returnItems))
	for i, item := range returnItems {
		if item.alias != "" {
			columns[i] = item.alias
		} else {
			columns[i] = item.expr
		}
	}

	rows := make([][]interface{}, 0, len(callRes.Rows))
	for _, hit := range callRes.Rows {
		if len(hit) < 2 {
			continue
		}
		node, ok := hit[0].(*storage.Node)
		if !ok || node == nil {
			continue
		}
		score := hit[1]

		nodeCtx := map[string]*storage.Node{varName: node}
		row := make([]interface{}, len(returnItems))
		for i, item := range returnItems {
			if i == cosineIdx {
				row[i] = score
				continue
			}
			row[i] = e.evaluateExpressionWithContext(ctx, item.expr, nodeCtx, nil)
		}
		rows = append(rows, row)
	}

	e.markCosineVectorIndexFastPathUsed()
	return &ExecuteResult{Columns: columns, Rows: rows, Stats: &QueryStats{}}, true
}

func parseCosineReturnShape(items []returnItem, varName string) (cosineIdx int, vectorProp string, queryExpr string, scoreRef string, ok bool) {
	cosineIdx = -1
	for i, item := range items {
		expr := strings.TrimSpace(item.expr)
		if !matchFuncStartAndSuffix(expr, "vector.similarity.cosine") {
			continue
		}
		inner := extractFuncArgs(expr, "vector.similarity.cosine")
		args := splitTopLevelComma(inner)
		if len(args) != 2 {
			return -1, "", "", "", false
		}

		left := strings.TrimSpace(args[0])
		right := strings.TrimSpace(args[1])

		leftVar, leftProp, leftIsVarProp := parseVarPropertyRef(left)
		rightVar, rightProp, rightIsVarProp := parseVarPropertyRef(right)

		var property string
		var query string
		switch {
		case leftIsVarProp && leftVar == varName && !rightIsVarProp:
			property = leftProp
			query = right
		case rightIsVarProp && rightVar == varName && !leftIsVarProp:
			property = rightProp
			query = left
		default:
			return -1, "", "", "", false
		}

		if strings.Contains(query, varName+".") || strings.EqualFold(strings.TrimSpace(query), varName) {
			return -1, "", "", "", false
		}
		if cosineIdx >= 0 {
			// Only one cosine expression is supported by this strict fast path.
			return -1, "", "", "", false
		}

		cosineIdx = i
		vectorProp = property
		queryExpr = query
		if item.alias != "" {
			scoreRef = item.alias
		} else {
			scoreRef = item.expr
		}
	}

	if cosineIdx < 0 || strings.TrimSpace(vectorProp) == "" || strings.TrimSpace(queryExpr) == "" {
		return -1, "", "", "", false
	}
	return cosineIdx, vectorProp, queryExpr, scoreRef, true
}

func parseVarPropertyRef(expr string) (string, string, bool) {
	expr = strings.TrimSpace(expr)
	parts := strings.Split(expr, ".")
	if len(parts) != 2 {
		return "", "", false
	}
	v := strings.TrimSpace(parts[0])
	p := strings.TrimSpace(parts[1])
	if v == "" || p == "" || strings.Contains(v, " ") || strings.Contains(p, " ") {
		return "", "", false
	}
	return v, p, true
}

func isDescendingOrderByScore(orderPart string, scoreRef string) bool {
	terms := splitTopLevelComma(orderPart)
	if len(terms) != 1 {
		return false
	}
	fields := strings.Fields(strings.TrimSpace(terms[0]))
	if len(fields) == 0 || len(fields) > 2 {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(fields[0]), strings.TrimSpace(scoreRef)) {
		return false
	}
	if len(fields) == 1 {
		return false
	}
	return strings.EqualFold(fields[1], "DESC")
}

func parseFastPathLimit(ctx context.Context, limitPart string) (int, bool) {
	fields := strings.Fields(strings.TrimSpace(limitPart))
	if len(fields) == 0 {
		return 0, false
	}
	tok := fields[0]
	if strings.HasPrefix(tok, "$") {
		params := getParamsFromContext(ctx)
		if params == nil {
			return 0, false
		}
		v, ok := params[strings.TrimPrefix(tok, "$")]
		if !ok {
			return 0, false
		}
		switch t := v.(type) {
		case int:
			return t, true
		case int64:
			return int(t), true
		case float64:
			return int(t), true
		default:
			return 0, false
		}
	}
	limit, err := strconv.Atoi(tok)
	if err != nil {
		return 0, false
	}
	return limit, true
}

func findCosineVectorIndexName(schema *storage.SchemaManager, label string, property string) (string, bool) {
	if schema == nil {
		return "", false
	}
	indexes := schema.GetIndexes()
	matches := make([]string, 0, 2)
	for _, raw := range indexes {
		idx, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		typ, _ := idx["type"].(string)
		if !strings.EqualFold(strings.TrimSpace(typ), "VECTOR") {
			continue
		}
		idxLabel, _ := idx["label"].(string)
		if !strings.EqualFold(strings.TrimSpace(idxLabel), strings.TrimSpace(label)) {
			continue
		}
		idxProp, _ := idx["property"].(string)
		if !strings.EqualFold(strings.TrimSpace(idxProp), strings.TrimSpace(property)) {
			continue
		}

		similarity := "cosine"
		if simRaw, ok := idx["similarityFunc"].(string); ok && strings.TrimSpace(simRaw) != "" {
			similarity = strings.TrimSpace(simRaw)
		}
		if !strings.EqualFold(similarity, "cosine") {
			continue
		}

		if name, ok := idx["name"].(string); ok && strings.TrimSpace(name) != "" {
			matches = append(matches, name)
		}
	}
	if len(matches) == 0 {
		return "", false
	}
	sort.Strings(matches)
	return matches[0], true
}
