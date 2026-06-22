package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) evaluateExpressionWithContextFullOperators(
	ctx context.Context,
	expr string,
	lowerExpr string,
	nodes map[string]*storage.Node,
	rels map[string]*storage.Edge,
	paths map[string]*PathResult,
	allPathEdges []*storage.Edge,
	allPathNodes []*storage.Node,
	pathLength int,
) interface{} {
	// Boolean/Comparison Operators (must be before property access)
	// ========================================

	// NOT expr
	if hasPrefixFoldASCII(expr, "not ") {
		inner := strings.TrimSpace(expr[4:])
		result := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		if b, ok := result.(bool); ok {
			return !b
		}
		return nil
	}

	// BETWEEN must be checked before AND (because BETWEEN x AND y uses AND)
	if betweenLeft, betweenRight, ok := splitByOperatorWithOptions(expr, " BETWEEN ", true, true); ok {
		value := e.evaluateExpressionWithContext(ctx, betweenLeft, nodes, rels)
		if minPart, maxPart, ok := splitByOperatorWithOptions(betweenRight, " AND ", true, true); ok {
			minVal := e.evaluateExpressionWithContext(ctx, minPart, nodes, rels)
			maxVal := e.evaluateExpressionWithContext(ctx, maxPart, nodes, rels)
			return (e.compareGreater(value, minVal) || e.compareEqual(value, minVal)) &&
				(e.compareLess(value, maxVal) || e.compareEqual(value, maxVal))
		}
	}

	// AND operator
	if left, right, ok := splitByOperatorWithOptions(expr, " AND ", true, false); ok {
		if e.evaluateExpressionWithContext(ctx, left, nodes, rels) != true {
			return false
		}
		return e.evaluateExpressionWithContext(ctx, right, nodes, rels) == true
	}

	// OR operator
	if left, right, ok := splitByOperatorWithOptions(expr, " OR ", true, false); ok {
		if e.evaluateExpressionWithContext(ctx, left, nodes, rels) == true {
			return true
		}
		return e.evaluateExpressionWithContext(ctx, right, nodes, rels) == true
	}

	// XOR operator
	if left, right, ok := splitByOperatorWithOptions(expr, " XOR ", true, false); ok {
		leftResult := e.evaluateExpressionWithContext(ctx, left, nodes, rels) == true
		rightResult := e.evaluateExpressionWithContext(ctx, right, nodes, rels) == true
		return leftResult != rightResult
	}

	// ========================================
	// Null Predicates (IS NULL, IS NOT NULL)
	// ========================================
	if hasSuffixFoldASCII(expr, " is null") {
		inner := strings.TrimSpace(expr[:len(expr)-8])
		result := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		return result == nil
	}
	if hasSuffixFoldASCII(expr, " is not null") {
		inner := strings.TrimSpace(expr[:len(expr)-12])
		result := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		return result != nil
	}

	// ========================================
	// String Predicates (STARTS WITH, ENDS WITH, CONTAINS)
	// ========================================
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, " STARTS WITH ", true, true); ok {
		leftStr, ok1 := e.evaluateStringPredicateOperand(ctx, leftExpr, nodes, rels)
		rightStr, ok2 := e.evaluateStringPredicateOperand(ctx, rightExpr, nodes, rels)
		if ok1 && ok2 {
			return strings.HasPrefix(leftStr, rightStr)
		}
		return false
	}
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, " ENDS WITH ", true, true); ok {
		leftStr, ok1 := e.evaluateStringPredicateOperand(ctx, leftExpr, nodes, rels)
		rightStr, ok2 := e.evaluateStringPredicateOperand(ctx, rightExpr, nodes, rels)
		if ok1 && ok2 {
			return strings.HasSuffix(leftStr, rightStr)
		}
		return false
	}
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, " CONTAINS ", true, true); ok {
		leftStr, ok1 := e.evaluateStringPredicateOperand(ctx, leftExpr, nodes, rels)
		rightStr, ok2 := e.evaluateStringPredicateOperand(ctx, rightExpr, nodes, rels)
		if ok1 && ok2 {
			return strings.Contains(leftStr, rightStr)
		}
		return false
	}

	// ========================================
	// IN Operator (value IN list)
	// ========================================
	// NOT IN must be checked before IN (because "NOT IN" contains " IN ")
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, " NOT IN ", true, true); ok {
		value := e.evaluateExpressionWithContext(ctx, leftExpr, nodes, rels)
		listVal := e.evaluateExpressionWithContext(ctx, rightExpr, nodes, rels)

		// Convert to []interface{} if needed
		var list []interface{}
		switch v := listVal.(type) {
		case []interface{}:
			list = v
		case []string:
			list = make([]interface{}, len(v))
			for i, s := range v {
				list[i] = s
			}
		case []int64:
			list = make([]interface{}, len(v))
			for i, n := range v {
				list[i] = n
			}
		default:
			// Not a list, return true (value is not in non-list)
			return true
		}

		// Check if value is NOT in the list
		for _, item := range list {
			if e.compareEqual(value, item) {
				return false
			}
		}
		return true
	}
	if leftExpr, rightExpr, ok := splitByOperatorWithOptions(expr, " IN ", true, true); ok {
		value := e.evaluateExpressionWithContext(ctx, leftExpr, nodes, rels)
		listVal := e.evaluateExpressionWithContext(ctx, rightExpr, nodes, rels)

		// Convert to []interface{} if needed
		var list []interface{}
		switch v := listVal.(type) {
		case []interface{}:
			list = v
		case []string:
			list = make([]interface{}, len(v))
			for i, s := range v {
				list[i] = s
			}
		case []int64:
			list = make([]interface{}, len(v))
			for i, n := range v {
				list[i] = n
			}
		default:
			// Not a list, return false
			return false
		}

		// Check if value is in the list
		for _, item := range list {
			if e.compareEqual(value, item) {
				return true
			}
		}
		return false
	}

	// Comparison operators (=, <>, <, >, <=, >=)
	if result := e.evaluateComparisonExpr(ctx, expr, nodes, rels); result != nil {
		return result
	}

	// Arithmetic operators (*, /, %, -, +)
	// NOTE: Arithmetic is checked BEFORE string concatenation to support date/duration arithmetic
	if result := e.evaluateArithmeticExpr(ctx, expr, nodes, rels); result != nil {
		return result
		// If arithmetic returned nil, fall through to string concatenation for + operator
	}

	// ========================================
	// String Concatenation (+ operator)
	// ========================================
	// Only check for concatenation if + is outside of string literals
	// This is a fallback when arithmetic didn't apply (e.g., string + string)
	if e.hasConcatOperator(expr) {
		return e.evaluateStringConcatWithContext(ctx, expr, nodes, rels)
	}

	// Unary minus
	if strings.HasPrefix(expr, "-") && len(expr) > 1 {
		inner := strings.TrimSpace(expr[1:])
		result := e.evaluateExpressionWithContext(ctx, inner, nodes, rels)
		switch v := result.(type) {
		case int64:
			return -v
		case float64:
			return -v
		case int:
			return -v
		}
	}

	// ========================================

	if lowerExpr == "" {
		lowerExpr = strings.ToLower(expr)
	}
	return e.evaluateExpressionWithContextFullPropsLiterals(ctx, expr, lowerExpr, nodes, rels, paths, allPathEdges, allPathNodes, pathLength)
}

func (e *StorageExecutor) evaluateStringPredicateOperand(ctx context.Context, expr string, nodes map[string]*storage.Node, rels map[string]*storage.Edge) (string, bool) {
	if isWholeCypherQuotedString(expr) {
		return decodeCypherQuotedString(expr)
	}
	value := e.evaluateExpressionWithContext(ctx, expr, nodes, rels)
	str, ok := value.(string)
	return str, ok
}
