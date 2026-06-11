package cypher

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type parsedSimpleMatchRelationshipPattern struct {
	leftVar     string
	leftLabels  []string
	rightVar    string
	rightLabels []string
	relVar      string
	relType     string
	isReverse   bool
}

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
	orderDesc, ok := parseOrderByScoreDirection(orderPart, scoreRef)
	if !ok {
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

	nodeScores, ok := e.fetchCosineNodeScores(ctx, indexName, limit, queryExpr, orderDesc)
	if !ok {
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

	rows := make([][]interface{}, 0, len(nodeScores))
	for _, hit := range nodeScores {
		node := hit.node
		score := hit.score
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

// tryFastPathMatchWithVectorCosineProjection handles Graphiti-style projection shape:
//
//	MATCH (n:Label)
//	WITH [DISTINCT] n, vector.similarity.cosine(n.<prop>, <query>) AS <scoreAlias>
//	[WHERE <scoreAlias> <op> <number|$param>]
//	RETURN ...
//	ORDER BY <scoreAlias> DESC|ASC
//	LIMIT <k>
func (e *StorageExecutor) tryFastPathMatchWithVectorCosineProjection(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, bool) {
	trimmed := strings.TrimSpace(cypher)
	if !strings.HasPrefix(strings.TrimSpace(upperQuery), "MATCH") {
		return nil, false
	}
	for _, kw := range []string{
		"SKIP", "UNWIND", "OPTIONAL MATCH", "CALL",
		"CREATE", "MERGE", "DELETE", "DETACH DELETE", "SET", "REMOVE", "UNION",
	} {
		if findKeywordIndex(trimmed, kw) > 0 {
			return nil, false
		}
	}

	withIdx := findKeywordIndex(trimmed, "WITH")
	returnIdx := findKeywordIndex(trimmed, "RETURN")
	orderIdx := findKeywordIndex(trimmed, "ORDER BY")
	limitIdx := findKeywordIndex(trimmed, "LIMIT")
	if withIdx <= 0 || returnIdx <= withIdx || orderIdx <= returnIdx || limitIdx <= orderIdx {
		return nil, false
	}
	preWithSegment := strings.TrimSpace(trimmed[len("MATCH"):withIdx])
	preWhereClause := ""
	if preWhereIdx := findKeywordIndex(preWithSegment, "WHERE"); preWhereIdx > 0 {
		preWhereClause = strings.TrimSpace(preWithSegment[preWhereIdx+len("WHERE"):])
		preWithSegment = strings.TrimSpace(preWithSegment[:preWhereIdx])
		if preWhereClause == "" {
			return nil, false
		}
	}

	matchPart := preWithSegment
	varName, labels, ok := parseSimpleMatchSingleNodePattern(matchPart)
	if !ok || len(labels) != 1 {
		return nil, false
	}
	label := labels[0]

	withToReturn := strings.TrimSpace(trimmed[withIdx+len("WITH") : returnIdx])
	postWhereClause := ""
	withProjectionRaw := withToReturn
	if postWhereIdx := findKeywordIndex(withToReturn, "WHERE"); postWhereIdx > 0 {
		postWhereClause = strings.TrimSpace(withToReturn[postWhereIdx+len("WHERE"):])
		withProjectionRaw = strings.TrimSpace(withToReturn[:postWhereIdx])
		if postWhereClause == "" {
			return nil, false
		}
	}

	withProjectionEnd := returnIdx
	_ = withProjectionEnd
	withProjection := strings.TrimSpace(withProjectionRaw)
	withProjection = trimOptionalDistinctPrefix(withProjection)
	withItems := e.parseReturnItems(withProjection)
	if len(withItems) < 2 {
		return nil, false
	}
	if !withProjectionContainsVariable(withItems, varName) {
		return nil, false
	}

	_, vectorProp, queryExpr, scoreRef, ok := parseCosineReturnShape(withItems, varName)
	if !ok {
		return nil, false
	}
	if scoreRef == "" {
		return nil, false
	}

	scoreOp := ""
	scoreCmp := float64(0)
	if postWhereClause != "" {
		op, cmp, ok := e.parseScoreComparisonPredicate(ctx, postWhereClause, scoreRef)
		if !ok {
			return nil, false
		}
		scoreOp = op
		scoreCmp = cmp
	}

	returnPart := strings.TrimSpace(trimmed[returnIdx+len("RETURN") : orderIdx])
	returnItems := e.parseReturnItems(returnPart)
	if len(returnItems) == 0 {
		return nil, false
	}

	orderPart := strings.TrimSpace(trimmed[orderIdx+len("ORDER BY") : limitIdx])
	orderDesc, ok := parseOrderByScoreDirection(orderPart, scoreRef)
	if !ok {
		return nil, false
	}

	limitPart := strings.TrimSpace(trimmed[limitIdx+len("LIMIT"):])
	limit, ok := parseFastPathLimit(ctx, limitPart)
	if !ok || limit < 0 {
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
	if limit == 0 {
		e.markCosineVectorIndexFastPathUsed()
		return &ExecuteResult{Columns: columns, Rows: [][]interface{}{}, Stats: &QueryStats{}}, true
	}

	indexName, hasIndex := findCosineVectorIndexName(e.storage.GetSchema(), label, vectorProp)
	if !hasIndex {
		return nil, false
	}

	candidateLimit := chooseVectorCandidateLimit(limit, preWhereClause != "" || scoreOp != "")
	nodeScores, ok := e.fetchCosineNodeScores(ctx, indexName, candidateLimit, queryExpr, orderDesc)
	if !ok {
		return nil, false
	}

	rows := make([][]interface{}, 0, len(nodeScores))
	for _, hit := range nodeScores {
		if preWhereClause != "" && !e.evaluateWhere(ctx, hit.node, varName, preWhereClause) {
			continue
		}
		if scoreOp != "" && !compareScore(hit.score, scoreOp, scoreCmp) {
			continue
		}
		nodeCtx := map[string]*storage.Node{varName: hit.node}
		row := make([]interface{}, len(returnItems))
		for i, item := range returnItems {
			expr := strings.TrimSpace(item.expr)
			if strings.EqualFold(expr, scoreRef) {
				row[i] = hit.score
				continue
			}
			row[i] = e.evaluateExpressionWithContext(ctx, item.expr, nodeCtx, nil)
		}
		rows = append(rows, row)
		if len(rows) >= limit {
			break
		}
	}

	e.markCosineVectorIndexFastPathUsed()
	return &ExecuteResult{Columns: columns, Rows: rows, Stats: &QueryStats{}}, true
}

// tryFastPathMatchRelationshipVectorCosine handles relationship direct-return shape:
//
//	MATCH ()-[e:TYPE]->()
//	RETURN ..., vector.similarity.cosine(e.<prop>, <query>) AS <scoreAlias>, ...
//	ORDER BY <scoreAlias> DESC|ASC
//	LIMIT <k>
func (e *StorageExecutor) tryFastPathMatchRelationshipVectorCosine(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, bool) {
	trimmed := strings.TrimSpace(cypher)
	if !strings.HasPrefix(strings.TrimSpace(upperQuery), "MATCH") {
		return nil, false
	}
	for _, kw := range []string{
		"SKIP", "WITH", "UNWIND", "OPTIONAL MATCH", "CALL",
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

	matchSegment := strings.TrimSpace(trimmed[len("MATCH"):returnIdx])
	preWhereClause := ""
	if preWhereIdx := findKeywordIndex(matchSegment, "WHERE"); preWhereIdx > 0 {
		preWhereClause = strings.TrimSpace(matchSegment[preWhereIdx+len("WHERE"):])
		matchSegment = strings.TrimSpace(matchSegment[:preWhereIdx])
		if preWhereClause == "" {
			return nil, false
		}
	}

	pattern, ok := e.parseSimpleMatchRelationshipPattern(matchSegment)
	if !ok || strings.TrimSpace(pattern.relVar) == "" || strings.TrimSpace(pattern.relType) == "" {
		return nil, false
	}

	returnPart := strings.TrimSpace(trimmed[returnIdx+len("RETURN") : orderIdx])
	returnItems := e.parseReturnItems(returnPart)
	if len(returnItems) == 0 {
		return nil, false
	}

	cosineIdx, vectorProp, queryExpr, scoreRef, ok := parseCosineReturnShape(returnItems, pattern.relVar)
	if !ok {
		return nil, false
	}

	orderPart := strings.TrimSpace(trimmed[orderIdx+len("ORDER BY") : limitIdx])
	orderDesc, ok := parseOrderByScoreDirection(orderPart, scoreRef)
	if !ok {
		return nil, false
	}

	limitPart := strings.TrimSpace(trimmed[limitIdx+len("LIMIT"):])
	limit, ok := parseFastPathLimit(ctx, limitPart)
	if !ok || limit < 0 {
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
	if limit == 0 {
		e.markCosineVectorIndexFastPathUsed()
		return &ExecuteResult{Columns: columns, Rows: [][]interface{}{}, Stats: &QueryStats{}}, true
	}

	indexName, hasIndex := findCosineVectorIndexName(e.storage.GetSchema(), pattern.relType, vectorProp)
	if !hasIndex {
		return nil, false
	}

	candidateLimit := chooseVectorCandidateLimit(limit, preWhereClause != "")
	edgeScores, ok := e.fetchCosineRelationshipScores(ctx, indexName, candidateLimit, queryExpr, orderDesc)
	if !ok {
		return nil, false
	}

	rows := make([][]interface{}, 0, len(edgeScores))
	for _, hit := range edgeScores {
		nodeCtx, edgeCtx, ok := e.buildRelationshipContext(pattern, hit.edge)
		if !ok {
			continue
		}
		if preWhereClause != "" && !evaluateExpressionBoolWithContext(e, ctx, preWhereClause, nodeCtx, edgeCtx) {
			continue
		}

		row := make([]interface{}, len(returnItems))
		for i, item := range returnItems {
			if i == cosineIdx {
				row[i] = hit.score
				continue
			}
			row[i] = e.evaluateExpressionWithContext(ctx, item.expr, nodeCtx, edgeCtx)
		}
		rows = append(rows, row)
		if len(rows) >= limit {
			break
		}
	}

	e.markCosineVectorIndexFastPathUsed()
	return &ExecuteResult{Columns: columns, Rows: rows, Stats: &QueryStats{}}, true
}

// tryFastPathMatchWithRelationshipVectorCosineProjection handles Graphiti relationship shape:
//
//	MATCH (n)-[e:TYPE]->(m) [WHERE ...]
//	WITH [DISTINCT] e[, n, m], vector.similarity.cosine(e.<prop>, <query>) AS <scoreAlias>
//	[WHERE <scoreAlias> <op> <number|$param>]
//	RETURN ...
//	ORDER BY <scoreAlias> DESC|ASC
//	LIMIT <k>
func (e *StorageExecutor) tryFastPathMatchWithRelationshipVectorCosineProjection(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, bool) {
	trimmed := strings.TrimSpace(cypher)
	if !strings.HasPrefix(strings.TrimSpace(upperQuery), "MATCH") {
		return nil, false
	}
	for _, kw := range []string{
		"SKIP", "UNWIND", "OPTIONAL MATCH", "CALL",
		"CREATE", "MERGE", "DELETE", "DETACH DELETE", "SET", "REMOVE", "UNION",
	} {
		if findKeywordIndex(trimmed, kw) > 0 {
			return nil, false
		}
	}

	withIdx := findKeywordIndex(trimmed, "WITH")
	returnIdx := findKeywordIndex(trimmed, "RETURN")
	orderIdx := findKeywordIndex(trimmed, "ORDER BY")
	limitIdx := findKeywordIndex(trimmed, "LIMIT")
	if withIdx <= 0 || returnIdx <= withIdx || orderIdx <= returnIdx || limitIdx <= orderIdx {
		return nil, false
	}

	preWithSegment := strings.TrimSpace(trimmed[len("MATCH"):withIdx])
	preWhereClause := ""
	if preWhereIdx := findKeywordIndex(preWithSegment, "WHERE"); preWhereIdx > 0 {
		preWhereClause = strings.TrimSpace(preWithSegment[preWhereIdx+len("WHERE"):])
		preWithSegment = strings.TrimSpace(preWithSegment[:preWhereIdx])
		if preWhereClause == "" {
			return nil, false
		}
	}

	pattern, ok := e.parseSimpleMatchRelationshipPattern(preWithSegment)
	if !ok || strings.TrimSpace(pattern.relVar) == "" || strings.TrimSpace(pattern.relType) == "" {
		return nil, false
	}

	withToReturn := strings.TrimSpace(trimmed[withIdx+len("WITH") : returnIdx])
	postWhereClause := ""
	withProjectionRaw := withToReturn
	if postWhereIdx := findKeywordIndex(withToReturn, "WHERE"); postWhereIdx > 0 {
		postWhereClause = strings.TrimSpace(withToReturn[postWhereIdx+len("WHERE"):])
		withProjectionRaw = strings.TrimSpace(withToReturn[:postWhereIdx])
		if postWhereClause == "" {
			return nil, false
		}
	}

	withProjection := trimOptionalDistinctPrefix(strings.TrimSpace(withProjectionRaw))
	withItems := e.parseReturnItems(withProjection)
	if len(withItems) < 2 || !withProjectionContainsVariable(withItems, pattern.relVar) {
		return nil, false
	}

	_, vectorProp, queryExpr, scoreRef, ok := parseCosineReturnShape(withItems, pattern.relVar)
	if !ok || scoreRef == "" {
		return nil, false
	}

	scoreOp := ""
	scoreCmp := float64(0)
	if postWhereClause != "" {
		op, cmp, ok := e.parseScoreComparisonPredicate(ctx, postWhereClause, scoreRef)
		if !ok {
			return nil, false
		}
		scoreOp = op
		scoreCmp = cmp
	}

	returnPart := strings.TrimSpace(trimmed[returnIdx+len("RETURN") : orderIdx])
	returnItems := e.parseReturnItems(returnPart)
	if len(returnItems) == 0 {
		return nil, false
	}

	orderPart := strings.TrimSpace(trimmed[orderIdx+len("ORDER BY") : limitIdx])
	orderDesc, ok := parseOrderByScoreDirection(orderPart, scoreRef)
	if !ok {
		return nil, false
	}

	limitPart := strings.TrimSpace(trimmed[limitIdx+len("LIMIT"):])
	limit, ok := parseFastPathLimit(ctx, limitPart)
	if !ok || limit < 0 {
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
	if limit == 0 {
		e.markCosineVectorIndexFastPathUsed()
		return &ExecuteResult{Columns: columns, Rows: [][]interface{}{}, Stats: &QueryStats{}}, true
	}

	indexName, hasIndex := findCosineVectorIndexName(e.storage.GetSchema(), pattern.relType, vectorProp)
	if !hasIndex {
		return nil, false
	}

	candidateLimit := chooseVectorCandidateLimit(limit, preWhereClause != "" || scoreOp != "")
	edgeScores, ok := e.fetchCosineRelationshipScores(ctx, indexName, candidateLimit, queryExpr, orderDesc)
	if !ok {
		return nil, false
	}

	rows := make([][]interface{}, 0, len(edgeScores))
	for _, hit := range edgeScores {
		nodeCtx, edgeCtx, ok := e.buildRelationshipContext(pattern, hit.edge)
		if !ok {
			continue
		}
		if preWhereClause != "" && !evaluateExpressionBoolWithContext(e, ctx, preWhereClause, nodeCtx, edgeCtx) {
			continue
		}
		if scoreOp != "" && !compareScore(hit.score, scoreOp, scoreCmp) {
			continue
		}

		row := make([]interface{}, len(returnItems))
		for i, item := range returnItems {
			expr := strings.TrimSpace(item.expr)
			if strings.EqualFold(expr, scoreRef) {
				row[i] = hit.score
				continue
			}
			row[i] = e.evaluateExpressionWithContext(ctx, item.expr, nodeCtx, edgeCtx)
		}
		rows = append(rows, row)
		if len(rows) >= limit {
			break
		}
	}

	e.markCosineVectorIndexFastPathUsed()
	return &ExecuteResult{Columns: columns, Rows: rows, Stats: &QueryStats{}}, true
}

type vectorNodeScore struct {
	node  *storage.Node
	score float64
}

type vectorEdgeScore struct {
	edge  *storage.Edge
	score float64
}

func (e *StorageExecutor) tryFastPathAnyMatchVectorCosine(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, bool) {
	if result, handled := e.tryFastPathMatchVectorCosine(ctx, cypher, upperQuery); handled {
		return result, true
	}
	if result, handled := e.tryFastPathMatchWithVectorCosineProjection(ctx, cypher, upperQuery); handled {
		return result, true
	}
	if result, handled := e.tryFastPathMatchRelationshipVectorCosine(ctx, cypher, upperQuery); handled {
		return result, true
	}
	if result, handled := e.tryFastPathMatchWithRelationshipVectorCosineProjection(ctx, cypher, upperQuery); handled {
		return result, true
	}
	return nil, false
}

func (e *StorageExecutor) fetchCosineNodeScores(ctx context.Context, indexName string, limit int, queryExpr string, orderDesc bool) ([]vectorNodeScore, bool) {
	callQueryExpr := queryExpr
	negateOutputScore := false
	if !orderDesc {
		negatedExpr, ok := e.buildNegatedCosineQueryExpr(ctx, queryExpr)
		if !ok {
			return nil, false
		}
		callQueryExpr = negatedExpr
		negateOutputScore = true
	}

	callQuery := fmt.Sprintf(
		"CALL db.index.vector.queryNodes('%s', %d, %s) YIELD node, score",
		strings.ReplaceAll(indexName, "'", "''"),
		limit,
		callQueryExpr,
	)
	callRes, err := e.callDbIndexVectorQueryNodes(ctx, callQuery)
	if err != nil {
		return nil, false
	}

	out := make([]vectorNodeScore, 0, len(callRes.Rows))
	for _, hit := range callRes.Rows {
		if len(hit) < 2 {
			continue
		}
		node, ok := hit[0].(*storage.Node)
		if !ok || node == nil {
			continue
		}
		score, ok := toFloat64(hit[1])
		if !ok {
			return nil, false
		}
		if negateOutputScore {
			score = -score
		}
		out = append(out, vectorNodeScore{node: node, score: score})
	}
	return out, true
}

func (e *StorageExecutor) fetchCosineRelationshipScores(ctx context.Context, indexName string, limit int, queryExpr string, orderDesc bool) ([]vectorEdgeScore, bool) {
	vec, ok := e.resolveCosineQueryVector(ctx, queryExpr)
	if !ok || len(vec) == 0 {
		return nil, false
	}
	if !orderDesc {
		for i := range vec {
			vec[i] = -vec[i]
		}
	}
	callQueryExpr := formatInlineFloat32Vector(vec)
	negateOutputScore := !orderDesc

	callQuery := fmt.Sprintf(
		"CALL db.index.vector.queryRelationships('%s', %d, %s) YIELD relationship, score",
		strings.ReplaceAll(indexName, "'", "''"),
		limit,
		callQueryExpr,
	)
	callRes, err := e.callDbIndexVectorQueryRelationships(ctx, callQuery)
	if err != nil {
		return nil, false
	}

	out := make([]vectorEdgeScore, 0, len(callRes.Rows))
	for _, row := range callRes.Rows {
		if len(row) < 2 {
			continue
		}
		relMap, ok := row[0].(map[string]interface{})
		if !ok || relMap == nil {
			continue
		}
		edgeID := stringOr(firstPresent(relMap, "_id", "_edgeId"), "")
		if strings.TrimSpace(edgeID) == "" {
			continue
		}
		edge := &storage.Edge{
			ID:        storage.EdgeID(edgeID),
			Type:      stringOr(firstPresent(relMap, "_type", "type"), ""),
			StartNode: storage.NodeID(stringOr(firstPresent(relMap, "_start", "startNode"), "")),
			EndNode:   storage.NodeID(stringOr(firstPresent(relMap, "_end", "endNode"), "")),
		}
		if props, ok := relMap["properties"].(map[string]interface{}); ok && props != nil {
			edge.Properties = cloneStringAnyMap(props)
		} else {
			edge.Properties = map[string]interface{}{}
		}
		if edge.Type == "" || edge.StartNode == "" || edge.EndNode == "" {
			continue
		}
		score, ok := toFloat64(row[1])
		if !ok {
			return nil, false
		}
		if negateOutputScore {
			score = -score
		}
		out = append(out, vectorEdgeScore{edge: edge, score: score})
	}
	return out, true
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

func parseOrderByScoreDirection(orderPart string, scoreRef string) (bool, bool) {
	terms := splitTopLevelComma(orderPart)
	if len(terms) != 1 {
		return false, false
	}
	fields := strings.Fields(strings.TrimSpace(terms[0]))
	if len(fields) == 0 || len(fields) > 2 {
		return false, false
	}
	if !strings.EqualFold(strings.TrimSpace(fields[0]), strings.TrimSpace(scoreRef)) {
		return false, false
	}
	if len(fields) == 1 {
		return true, true
	}
	if strings.EqualFold(fields[1], "DESC") {
		return true, true
	}
	if strings.EqualFold(fields[1], "ASC") {
		return false, true
	}
	return false, false
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

func (e *StorageExecutor) buildNegatedCosineQueryExpr(ctx context.Context, queryExpr string) (string, bool) {
	vector, ok := e.resolveCosineQueryVector(ctx, queryExpr)
	if !ok || len(vector) == 0 {
		return "", false
	}
	vector = append([]float32(nil), vector...)
	if len(vector) == 0 {
		return "", false
	}
	for i := range vector {
		vector[i] = -vector[i]
	}
	return formatInlineFloat32Vector(vector), true
}

func (e *StorageExecutor) resolveCosineQueryVector(ctx context.Context, queryExpr string) ([]float32, bool) {
	value := e.parseValue(ctx, strings.TrimSpace(queryExpr))
	switch v := value.(type) {
	case []float32:
		return append([]float32(nil), v...), len(v) > 0
	case []float64:
		out := make([]float32, len(v))
		for i, val := range v {
			out[i] = float32(val)
		}
		return out, len(out) > 0
	case []interface{}:
		out := toFloat32Slice(v)
		return out, len(out) > 0
	case string:
		if strings.TrimSpace(v) == "" || e.embedder == nil {
			return nil, false
		}
		embedded, err := e.embedVectorQueryText(ctx, v)
		if err != nil {
			return nil, false
		}
		return embedded, len(embedded) > 0
	default:
		out := toFloat32Slice(value)
		return out, len(out) > 0
	}
}

func formatInlineFloat32Vector(vec []float32) string {
	if len(vec) == 0 {
		return "[]"
	}
	var b strings.Builder
	b.Grow(len(vec) * 8)
	b.WriteByte('[')
	for i, v := range vec {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(v), 'f', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}

func (e *StorageExecutor) parseSimpleMatchRelationshipPattern(pattern string) (parsedSimpleMatchRelationshipPattern, bool) {
	sourceContent, relContent, targetContent, isReverse, remainder, err := e.parseCreateRelPatternWithVars(pattern)
	if err != nil || strings.TrimSpace(remainder) != "" {
		return parsedSimpleMatchRelationshipPattern{}, false
	}
	relVar, relType, relPropsStr, err := parseCreateRelationshipContent(relContent)
	if err != nil || strings.TrimSpace(relPropsStr) != "" || strings.TrimSpace(relType) == "" {
		return parsedSimpleMatchRelationshipPattern{}, false
	}

	leftVar, leftLabels, ok := parseSimpleEndpointRef(sourceContent)
	if !ok {
		return parsedSimpleMatchRelationshipPattern{}, false
	}
	rightVar, rightLabels, ok := parseSimpleEndpointRef(targetContent)
	if !ok {
		return parsedSimpleMatchRelationshipPattern{}, false
	}

	return parsedSimpleMatchRelationshipPattern{
		leftVar:     leftVar,
		leftLabels:  leftLabels,
		rightVar:    rightVar,
		rightLabels: rightLabels,
		relVar:      strings.TrimSpace(relVar),
		relType:     strings.TrimSpace(relType),
		isReverse:   isReverse,
	}, true
}

func parseSimpleEndpointRef(content string) (string, []string, bool) {
	content = strings.TrimSpace(content)
	if content == "" {
		return "", nil, true
	}
	if strings.Contains(content, "{") || strings.Contains(content, "}") || strings.Contains(content, "-") {
		return "", nil, false
	}
	parts := strings.Split(content, ":")
	v := strings.TrimSpace(parts[0])
	labels := make([]string, 0, len(parts)-1)
	for _, p := range parts[1:] {
		lbl := strings.TrimSpace(p)
		if lbl == "" {
			return "", nil, false
		}
		labels = append(labels, lbl)
	}
	return v, labels, true
}

func (e *StorageExecutor) buildRelationshipContext(pattern parsedSimpleMatchRelationshipPattern, edge *storage.Edge) (map[string]*storage.Node, map[string]*storage.Edge, bool) {
	if edge == nil {
		return nil, nil, false
	}
	startID := storage.EnsureNodeIDDatabasePrefixForEngine(e.storage, edge.StartNode)
	endID := storage.EnsureNodeIDDatabasePrefixForEngine(e.storage, edge.EndNode)

	startNode, err := e.storage.GetNode(startID)
	if err != nil || startNode == nil {
		return nil, nil, false
	}
	endNode, err := e.storage.GetNode(endID)
	if err != nil || endNode == nil {
		return nil, nil, false
	}

	var leftNode, rightNode *storage.Node
	if pattern.isReverse {
		leftNode = endNode
		rightNode = startNode
	} else {
		leftNode = startNode
		rightNode = endNode
	}

	if len(pattern.leftLabels) > 0 && !nodeHasAnyLabel(leftNode, pattern.leftLabels) {
		return nil, nil, false
	}
	if len(pattern.rightLabels) > 0 && !nodeHasAnyLabel(rightNode, pattern.rightLabels) {
		return nil, nil, false
	}

	nodeCtx := map[string]*storage.Node{}
	if pattern.leftVar != "" {
		nodeCtx[pattern.leftVar] = leftNode
	}
	if pattern.rightVar != "" {
		nodeCtx[pattern.rightVar] = rightNode
	}
	edgeCtx := map[string]*storage.Edge{}
	if pattern.relVar != "" {
		edgeCtx[pattern.relVar] = edge
	}
	return nodeCtx, edgeCtx, true
}

func evaluateExpressionBoolWithContext(e *StorageExecutor, ctx context.Context, expr string, nodeCtx map[string]*storage.Node, edgeCtx map[string]*storage.Edge) bool {
	raw := e.evaluateExpressionWithContext(ctx, expr, nodeCtx, edgeCtx)
	if b, ok := raw.(bool); ok {
		return b
	}
	if b, ok := toBool(raw); ok {
		return b
	}
	return false
}

func trimOptionalDistinctPrefix(withClause string) string {
	trimmed := strings.TrimSpace(withClause)
	if len(trimmed) < len("DISTINCT ")+1 {
		return trimmed
	}
	if strings.EqualFold(trimmed[:len("DISTINCT")], "DISTINCT") {
		rest := strings.TrimSpace(trimmed[len("DISTINCT"):])
		if rest != "" {
			return rest
		}
	}
	return trimmed
}

func withProjectionContainsVariable(items []returnItem, variable string) bool {
	for _, item := range items {
		if strings.EqualFold(strings.TrimSpace(item.expr), variable) {
			return true
		}
	}
	return false
}

func (e *StorageExecutor) parseScoreComparisonPredicate(ctx context.Context, whereClause string, scoreAlias string) (string, float64, bool) {
	clause := strings.TrimSpace(whereClause)
	if clause == "" {
		return "", 0, false
	}
	for _, kw := range []string{" AND ", " OR ", " NOT "} {
		if topLevelKeywordIndex(clause, kw) >= 0 {
			return "", 0, false
		}
	}

	op := ""
	opIdx := -1
	for _, candidate := range []string{">=", "<=", ">", "<"} {
		if idx := strings.Index(clause, candidate); idx > 0 {
			op = candidate
			opIdx = idx
			break
		}
	}
	if opIdx <= 0 {
		return "", 0, false
	}

	left := strings.TrimSpace(clause[:opIdx])
	right := strings.TrimSpace(clause[opIdx+len(op):])
	if left == "" || right == "" {
		return "", 0, false
	}

	normalizedOp := op
	cmpExpr := ""
	if strings.EqualFold(left, scoreAlias) {
		cmpExpr = right
	} else if strings.EqualFold(right, scoreAlias) {
		cmpExpr = left
		switch op {
		case ">":
			normalizedOp = "<"
		case ">=":
			normalizedOp = "<="
		case "<":
			normalizedOp = ">"
		case "<=":
			normalizedOp = ">="
		default:
			return "", 0, false
		}
	} else {
		return "", 0, false
	}

	raw := e.parseValue(ctx, cmpExpr)
	f, ok := toFloat64(raw)
	if !ok {
		return "", 0, false
	}
	return normalizedOp, f, true
}

func compareScore(score float64, op string, target float64) bool {
	switch op {
	case ">":
		return score > target
	case ">=":
		return score >= target
	case "<":
		return score < target
	case "<=":
		return score <= target
	default:
		return false
	}
}

func chooseVectorCandidateLimit(limit int, filtered bool) int {
	if limit <= 0 {
		return 0
	}
	if !filtered {
		return limit
	}
	over := limit * 20
	if over < 200 {
		over = 200
	}
	if over > 5000 {
		over = 5000
	}
	return over
}
