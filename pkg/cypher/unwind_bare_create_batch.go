package cypher

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) tryUnwindBareCreateDirectBatch(
	ctx context.Context, cypher string,
) (*ExecuteResult, error, bool) {
	if wal, _ := e.resolveWALAndDatabase(); wal != nil {
		return nil, nil, false
	}
	trimmed := strings.TrimSpace(cypher)
	if !startsWithKeywordFold(trimmed, "UNWIND") || findKeywordIndex(trimmed, "UNWIND") != 0 {
		return nil, nil, false
	}
	variable, items, restQuery, ok := e.parseParameterizedUnwindBatch(ctx, trimmed)
	if !ok || !startsWithKeywordFold(strings.TrimSpace(restQuery), "CREATE") {
		return nil, nil, false
	}
	result, handled, err := e.executeUnwindBareCreateBatch(ctx, variable, items, restQuery)
	if !handled {
		return nil, nil, false
	}
	return result, err, true
}

func (e *StorageExecutor) parseParameterizedUnwindBatch(
	ctx context.Context, cypher string,
) (string, []interface{}, string, bool) {
	afterUnwind := cypher[len("UNWIND"):]
	asRelIdx := findKeywordNotInBrackets(afterUnwind, " AS ")
	if asRelIdx == -1 {
		return "", nil, "", false
	}
	listExpr := strings.TrimSpace(afterUnwind[:asRelIdx])
	if !strings.HasPrefix(listExpr, "$") {
		return "", nil, "", false
	}
	paramName := strings.TrimSpace(strings.TrimPrefix(listExpr, "$"))
	if paramName == "" || strings.IndexAny(paramName, " \t\r\n") >= 0 {
		return "", nil, "", false
	}
	params := getParamsFromContext(ctx)
	if params == nil {
		return "", nil, "", false
	}
	list, exists := params[paramName]
	if !exists {
		return "", nil, "", false
	}

	remainderStart := len("UNWIND") + asRelIdx + len("AS")
	for remainderStart < len(cypher) && isASCIISpace(cypher[remainderStart]) {
		remainderStart++
	}
	remainder := strings.TrimSpace(cypher[remainderStart:])
	spaceIdx := strings.IndexAny(remainder, " \t\r\n")
	if spaceIdx <= 0 {
		return "", nil, "", false
	}
	variable := strings.TrimSpace(remainder[:spaceIdx])
	if !isSimpleIdentifier(variable) {
		return "", nil, "", false
	}
	restQuery := strings.TrimSpace(remainder[spaceIdx:])
	return variable, unwindItemsFromList(list), restQuery, true
}

func unwindItemsFromList(list interface{}) []interface{} {
	switch v := list.(type) {
	case nil:
		// UNWIND null produces no rows (Neo4j compatible).
		return []interface{}{}
	case []interface{}:
		return v
	case []string:
		items := make([]interface{}, len(v))
		for i, s := range v {
			items[i] = s
		}
		return items
	case []int64:
		items := make([]interface{}, len(v))
		for i, n := range v {
			items[i] = n
		}
		return items
	case []float64:
		items := make([]interface{}, len(v))
		for i, n := range v {
			items[i] = n
		}
		return items
	case []map[string]interface{}:
		items := make([]interface{}, len(v))
		for i := range v {
			items[i] = v[i]
		}
		return items
	default:
		rv := reflect.ValueOf(list)
		if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
			items := make([]interface{}, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				items[i] = rv.Index(i).Interface()
			}
			return items
		}
		// Single value gets wrapped in a list.
		return []interface{}{list}
	}
}

// executeUnwindBareCreateBatch handles simple bulk creates of the form:
//
//	UNWIND $rows AS row
//	CREATE (:Label {k: row.field, fixed: 1})
//
// It parses the CREATE body once, materializes storage nodes for every row,
// and sends them through the storage bulk-create path in the current execution
// context. More complex mutation shapes deliberately fall back to the general
// executor.
func (e *StorageExecutor) executeUnwindBareCreateBatch(
	ctx context.Context, unwindVar string, items []interface{}, restQuery string,
) (*ExecuteResult, bool, error) {
	trimmed := strings.TrimSpace(restQuery)
	if !startsWithKeywordFold(trimmed, "CREATE") {
		return nil, false, nil
	}

	mutationPart := trimmed
	returnPart := ""
	countAlias := ""
	if returnIdx := findKeywordIndexInContext(trimmed, "RETURN"); returnIdx >= 0 {
		mutationPart = strings.TrimSpace(trimmed[:returnIdx])
		returnPart = strings.TrimSpace(trimmed[returnIdx:])
		alias, ok := parseUnwindBatchCountReturn(returnPart)
		if !ok {
			return nil, false, nil
		}
		countAlias = alias
	}
	if mutationPart == "" {
		return nil, false, nil
	}
	for _, keyword := range []string{"MATCH", "OPTIONAL MATCH", "MERGE", "SET", "DELETE", "DETACH", "REMOVE", "WITH", "CALL", "UNWIND", "FOREACH", "LOAD"} {
		if findKeywordIndexInContext(mutationPart, keyword) >= 0 {
			return nil, false, nil
		}
	}

	specs, ok := e.parseUnwindBareCreateNodeSpecs(mutationPart, unwindVar)
	if !ok || len(specs) == 0 {
		return nil, false, nil
	}

	rows := make([]map[string]interface{}, 0, len(items))
	for _, item := range items {
		row, ok := toStringAnyMap(item)
		if !ok {
			return nil, false, nil
		}
		rows = append(rows, row)
	}

	nodes := make([]*storage.Node, 0, len(rows)*len(specs))
	for _, row := range rows {
		for _, spec := range specs {
			nodes = append(nodes, &storage.Node{
				ID:         storage.NodeID(e.generateID()),
				Labels:     []string{spec.label},
				Properties: buildPropsFromSpec(row, spec.rowFieldRefs, spec.literals),
			})
		}
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	if len(nodes) > 0 {
		store := e.getStorage(ctx)
		if err := store.BulkCreateNodes(nodes); err != nil {
			return nil, true, fmt.Errorf("BulkCreateNodes: %w", err)
		}
		for _, node := range nodes {
			e.notifyNodeMutated(string(node.ID))
			addOptimisticNodeID(result, node.ID)
		}
		result.Stats.NodesCreated += len(nodes)
	}

	e.markUnwindMultiMatchCreateBatchUsed()
	if countAlias != "" {
		result.Columns = []string{countAlias}
		result.Rows = [][]interface{}{{int64(len(rows))}}
	}
	return result, true, nil
}

func (e *StorageExecutor) parseUnwindBareCreateNodeSpecs(mutationPart, unwindVar string) ([]createNodeSpec, bool) {
	createClauses := SplitByCreate(mutationPart)
	if len(createClauses) == 0 {
		return nil, false
	}
	specs := make([]createNodeSpec, 0, len(createClauses))
	for _, clause := range createClauses {
		clause = strings.TrimSpace(clause)
		if clause == "" {
			continue
		}
		patterns := e.splitCreatePatterns(clause)
		for _, pattern := range patterns {
			pattern = strings.TrimSpace(pattern)
			if pattern == "" {
				continue
			}
			if containsOutsideStrings(pattern, "->") ||
				containsOutsideStrings(pattern, "<-") ||
				containsOutsideStrings(pattern, "]-") ||
				containsOutsideStrings(pattern, "-[") {
				return nil, false
			}
			spec, ok := parseSimpleCreateNode(pattern, unwindVar)
			if !ok || !isValidIdentifier(spec.label) || containsReservedKeyword(spec.label) {
				return nil, false
			}
			specs = append(specs, spec)
		}
	}
	return specs, len(specs) > 0
}
