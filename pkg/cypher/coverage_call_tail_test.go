package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// All tests in this file exercise the *pure* callTail* extractors and the
// compileCallTailValue* predicate compilers without needing a query
// pipeline. The compilers return closures so we drive them directly with
// canned `values` and `params` maps.

func freshExecutorForCallTail(t *testing.T) *StorageExecutor {
	t.Helper()
	base := storage.NewMemoryEngine()
	ns := storage.NewNamespacedEngine(base, "call-tail-coverage")
	return NewStorageExecutor(ns)
}

// ============================================================================
// callTailPropertyValue — Node / Edge / map / nil-ptr / unsupported.
// ============================================================================

func TestCallTailPropertyValue_AllShapes(t *testing.T) {
	t.Run("nil node returns false", func(t *testing.T) {
		v, ok := callTailPropertyValue((*storage.Node)(nil), "name")
		require.False(t, ok)
		require.Nil(t, v)
	})

	t.Run("node property hit", func(t *testing.T) {
		n := &storage.Node{Properties: map[string]any{"name": "Alice"}}
		v, ok := callTailPropertyValue(n, "name")
		require.True(t, ok)
		require.Equal(t, "Alice", v)
	})

	t.Run("node property miss", func(t *testing.T) {
		n := &storage.Node{Properties: map[string]any{}}
		v, ok := callTailPropertyValue(n, "missing")
		require.False(t, ok)
		require.Nil(t, v)
	})

	t.Run("nil edge returns false", func(t *testing.T) {
		v, ok := callTailPropertyValue((*storage.Edge)(nil), "since")
		require.False(t, ok)
		require.Nil(t, v)
	})

	t.Run("edge property hit", func(t *testing.T) {
		edge := &storage.Edge{Properties: map[string]any{"since": int64(2024)}}
		v, ok := callTailPropertyValue(edge, "since")
		require.True(t, ok)
		require.Equal(t, int64(2024), v)
	})

	t.Run("map with nested properties bag finds property", func(t *testing.T) {
		raw := map[string]interface{}{
			"properties": map[string]interface{}{"name": "Bob"},
			"name":       "shadowed",
		}
		v, ok := callTailPropertyValue(raw, "name")
		require.True(t, ok)
		require.Equal(t, "Bob", v, "nested 'properties' bag wins over top-level key")
	})

	t.Run("map with top-level property", func(t *testing.T) {
		raw := map[string]interface{}{"name": "Carol"}
		v, ok := callTailPropertyValue(raw, "name")
		require.True(t, ok)
		require.Equal(t, "Carol", v)
	})

	t.Run("map missing property returns false", func(t *testing.T) {
		v, ok := callTailPropertyValue(map[string]interface{}{}, "nope")
		require.False(t, ok)
		require.Nil(t, v)
	})

	t.Run("unsupported type returns false", func(t *testing.T) {
		v, ok := callTailPropertyValue(42, "name")
		require.False(t, ok)
		require.Nil(t, v)
	})
}

// ============================================================================
// callTailPropertiesValue — Node / Edge / map / nil / unsupported.
// ============================================================================

func TestCallTailPropertiesValue_AllShapes(t *testing.T) {
	t.Run("nil node returns false", func(t *testing.T) {
		v, ok := callTailPropertiesValue((*storage.Node)(nil))
		require.False(t, ok)
		require.Nil(t, v)
	})

	t.Run("node copies properties map", func(t *testing.T) {
		original := map[string]any{"name": "Alice", "age": int64(30)}
		n := &storage.Node{Properties: original}
		v, ok := callTailPropertiesValue(n)
		require.True(t, ok)
		props := v.(map[string]interface{})
		require.Equal(t, "Alice", props["name"])
		// Mutation on the returned copy must NOT affect the original node.
		props["name"] = "Bob"
		require.Equal(t, "Alice", n.Properties["name"], "Properties must be cloned, not aliased")
	})

	t.Run("nil edge returns false", func(t *testing.T) {
		v, ok := callTailPropertiesValue((*storage.Edge)(nil))
		require.False(t, ok)
		require.Nil(t, v)
	})

	t.Run("edge copies properties map", func(t *testing.T) {
		edge := &storage.Edge{Properties: map[string]any{"since": int64(2024)}}
		v, ok := callTailPropertiesValue(edge)
		require.True(t, ok)
		props := v.(map[string]interface{})
		require.Equal(t, int64(2024), props["since"])
		props["since"] = int64(9999)
		require.Equal(t, int64(2024), edge.Properties["since"], "Edge props must be cloned")
	})

	t.Run("map with nested properties bag returns the bag clone", func(t *testing.T) {
		raw := map[string]interface{}{
			"properties": map[string]interface{}{"name": "Bob"},
			"_meta":      "internal",
		}
		v, ok := callTailPropertiesValue(raw)
		require.True(t, ok)
		props := v.(map[string]interface{})
		require.Equal(t, "Bob", props["name"])
		require.NotContains(t, props, "_meta")
	})

	t.Run("map without nested bag filters underscore keys", func(t *testing.T) {
		raw := map[string]interface{}{
			"name":     "Carol",
			"_id":      "skip-me",
			"_private": true,
		}
		v, ok := callTailPropertiesValue(raw)
		require.True(t, ok)
		props := v.(map[string]interface{})
		require.Equal(t, "Carol", props["name"])
		require.NotContains(t, props, "_id")
		require.NotContains(t, props, "_private")
	})

	t.Run("unsupported type returns false", func(t *testing.T) {
		v, ok := callTailPropertiesValue("scalar")
		require.False(t, ok)
		require.Nil(t, v)
	})
}

// ============================================================================
// callTailTypeValue — Edge / map / nil / unsupported.
// ============================================================================

func TestCallTailTypeValue_AllShapes(t *testing.T) {
	t.Run("nil edge returns false", func(t *testing.T) {
		v, ok := callTailTypeValue((*storage.Edge)(nil))
		require.False(t, ok)
		require.Nil(t, v)
	})

	t.Run("edge type", func(t *testing.T) {
		edge := &storage.Edge{Type: "KNOWS"}
		v, ok := callTailTypeValue(edge)
		require.True(t, ok)
		require.Equal(t, "KNOWS", v)
	})

	t.Run("map with type key", func(t *testing.T) {
		v, ok := callTailTypeValue(map[string]interface{}{"type": "KNOWS"})
		require.True(t, ok)
		require.Equal(t, "KNOWS", v)
	})

	t.Run("map with _type key (fallback)", func(t *testing.T) {
		v, ok := callTailTypeValue(map[string]interface{}{"_type": "FOLLOWS"})
		require.True(t, ok)
		require.Equal(t, "FOLLOWS", v)
	})

	t.Run("map without type or _type returns false", func(t *testing.T) {
		v, ok := callTailTypeValue(map[string]interface{}{"other": "x"})
		require.False(t, ok)
		require.Nil(t, v)
	})

	t.Run("non-edge non-map returns false", func(t *testing.T) {
		v, ok := callTailTypeValue(42)
		require.False(t, ok)
		require.Nil(t, v)
	})
}

// ============================================================================
// callTailIDValue — Node / Edge / map / nil / unsupported.
// ============================================================================

func TestCallTailIDValue_AllShapes(t *testing.T) {
	t.Run("nil node returns false", func(t *testing.T) {
		v, ok := callTailIDValue((*storage.Node)(nil))
		require.False(t, ok)
		require.Nil(t, v)
	})

	t.Run("node id stringified", func(t *testing.T) {
		v, ok := callTailIDValue(&storage.Node{ID: "u1"})
		require.True(t, ok)
		require.Equal(t, "u1", v)
	})

	t.Run("nil edge returns false", func(t *testing.T) {
		v, ok := callTailIDValue((*storage.Edge)(nil))
		require.False(t, ok)
		require.Nil(t, v)
	})

	t.Run("edge id stringified", func(t *testing.T) {
		v, ok := callTailIDValue(&storage.Edge{ID: "r1"})
		require.True(t, ok)
		require.Equal(t, "r1", v)
	})

	t.Run("map id key", func(t *testing.T) {
		v, ok := callTailIDValue(map[string]interface{}{"id": "x"})
		require.True(t, ok)
		require.Equal(t, "x", v)
	})

	t.Run("map _id fallback", func(t *testing.T) {
		v, ok := callTailIDValue(map[string]interface{}{"_id": "y"})
		require.True(t, ok)
		require.Equal(t, "y", v)
	})

	t.Run("map missing returns false", func(t *testing.T) {
		v, ok := callTailIDValue(map[string]interface{}{"other": "z"})
		require.False(t, ok)
		require.Nil(t, v)
	})

	t.Run("unsupported type", func(t *testing.T) {
		v, ok := callTailIDValue(true)
		require.False(t, ok)
		require.Nil(t, v)
	})
}

// ============================================================================
// callTailLabelsValue — Node / map / nil / unsupported.
// ============================================================================

func TestCallTailLabelsValue_AllShapes(t *testing.T) {
	t.Run("nil node returns false", func(t *testing.T) {
		v, ok := callTailLabelsValue((*storage.Node)(nil))
		require.False(t, ok)
		require.Nil(t, v)
	})

	t.Run("node labels are []interface{}", func(t *testing.T) {
		v, ok := callTailLabelsValue(&storage.Node{Labels: []string{"Person", "User"}})
		require.True(t, ok)
		require.Equal(t, []interface{}{"Person", "User"}, v)
	})

	t.Run("map with labels key passes through", func(t *testing.T) {
		raw := map[string]interface{}{"labels": []interface{}{"Foo"}}
		v, ok := callTailLabelsValue(raw)
		require.True(t, ok)
		require.Equal(t, []interface{}{"Foo"}, v)
	})

	t.Run("map without labels key returns false", func(t *testing.T) {
		v, ok := callTailLabelsValue(map[string]interface{}{"x": 1})
		require.False(t, ok)
		require.Nil(t, v)
	})

	t.Run("unsupported type", func(t *testing.T) {
		v, ok := callTailLabelsValue("scalar")
		require.False(t, ok)
		require.Nil(t, v)
	})
}

// ============================================================================
// cloneStringInterfaceMap — independent copy semantics.
// ============================================================================

func TestCloneStringInterfaceMap_IsIndependentCopy(t *testing.T) {
	original := map[string]interface{}{"a": 1, "b": "x"}
	clone := cloneStringInterfaceMap(original)
	require.Equal(t, original, clone)
	clone["a"] = 999
	require.Equal(t, 1, original["a"], "mutating the clone must not affect the original")
}

// ============================================================================
// compileCallTailValueNullPredicate — IS NULL / IS NOT NULL.
// ============================================================================

func TestCompileCallTailValueNullPredicate(t *testing.T) {
	e := freshExecutorForCallTail(t)
	ctx := context.Background()

	t.Run("IS NOT NULL: value present and non-nil", func(t *testing.T) {
		pred, ok := e.compileCallTailValueNullPredicate("x IS NOT NULL", " IS NOT NULL", true)
		require.True(t, ok)
		require.True(t, pred(map[string]interface{}{"x": "value"}, nil))
		require.False(t, pred(map[string]interface{}{"x": nil}, nil))
		require.False(t, pred(map[string]interface{}{}, nil), "missing value is not 'NOT NULL'")
	})

	t.Run("IS NULL: value missing or nil", func(t *testing.T) {
		pred, ok := e.compileCallTailValueNullPredicate("x IS NULL", " IS NULL", false)
		require.True(t, ok)
		require.True(t, pred(map[string]interface{}{"x": nil}, nil))
		require.True(t, pred(map[string]interface{}{}, nil))
		require.False(t, pred(map[string]interface{}{"x": "value"}, nil))
	})

	t.Run("operator not present returns ok=false", func(t *testing.T) {
		_, ok := e.compileCallTailValueNullPredicate("x = 1", " IS NULL", false)
		require.False(t, ok)
	})

	// Reuse via the top-level compiler
	t.Run("tryCompileCallTailValueWhere dispatches IS NOT NULL", func(t *testing.T) {
		pred, ok := e.tryCompileCallTailValueWhere(ctx, "x IS NOT NULL")
		require.True(t, ok)
		require.True(t, pred(map[string]interface{}{"x": "present"}, nil))
		require.False(t, pred(map[string]interface{}{"x": nil}, nil))
	})
}

// ============================================================================
// compileCallTailValueStringPredicate — STARTS WITH / ENDS WITH / CONTAINS.
// ============================================================================

func TestCompileCallTailValueStringPredicate(t *testing.T) {
	e := freshExecutorForCallTail(t)
	ctx := context.Background()

	t.Run("STARTS WITH", func(t *testing.T) {
		pred, ok := e.tryCompileCallTailValueWhere(ctx, `name STARTS WITH "Al"`)
		require.True(t, ok)
		require.True(t, pred(map[string]interface{}{"name": "Alice"}, nil))
		require.False(t, pred(map[string]interface{}{"name": "Bob"}, nil))
	})

	t.Run("ENDS WITH", func(t *testing.T) {
		pred, ok := e.tryCompileCallTailValueWhere(ctx, `name ENDS WITH "ce"`)
		require.True(t, ok)
		require.True(t, pred(map[string]interface{}{"name": "Alice"}, nil))
	})

	t.Run("CONTAINS", func(t *testing.T) {
		pred, ok := e.tryCompileCallTailValueWhere(ctx, `name CONTAINS "li"`)
		require.True(t, ok)
		require.True(t, pred(map[string]interface{}{"name": "Alice"}, nil))
	})

	t.Run("non-string operand returns false", func(t *testing.T) {
		pred, ok := e.tryCompileCallTailValueWhere(ctx, `name STARTS WITH "Al"`)
		require.True(t, ok)
		require.False(t, pred(map[string]interface{}{"name": 42}, nil),
			"non-string left operand cannot match a string predicate")
	})

	t.Run("operator absent returns false", func(t *testing.T) {
		_, ok := e.compileCallTailValueStringPredicate("a = b", " STARTS WITH ")
		require.False(t, ok)
	})
}

// ============================================================================
// compileCallTailValueInPredicate — IN / NOT IN with literal and resolved
// list operands.
// ============================================================================

func TestCompileCallTailValueInPredicate_LiteralAndResolvedList(t *testing.T) {
	e := freshExecutorForCallTail(t)
	ctx := context.Background()

	t.Run("literal IN list", func(t *testing.T) {
		pred, ok := e.tryCompileCallTailValueWhere(ctx, "n IN [1, 2, 3]")
		require.True(t, ok)
		require.True(t, pred(map[string]interface{}{"n": int64(2)}, nil))
		require.False(t, pred(map[string]interface{}{"n": int64(99)}, nil))
	})

	t.Run("literal NOT IN list", func(t *testing.T) {
		pred, ok := e.tryCompileCallTailValueWhere(ctx, "n NOT IN [1, 2, 3]")
		require.True(t, ok)
		require.False(t, pred(map[string]interface{}{"n": int64(2)}, nil))
		require.True(t, pred(map[string]interface{}{"n": int64(99)}, nil))
	})

	t.Run("resolved list from values map", func(t *testing.T) {
		pred, ok := e.tryCompileCallTailValueWhere(ctx, "needle IN haystack")
		require.True(t, ok)
		got := pred(map[string]interface{}{
			"needle":   int64(7),
			"haystack": []interface{}{int64(1), int64(7), int64(9)},
		}, nil)
		require.True(t, got)
	})

	t.Run("operator absent → not an IN predicate", func(t *testing.T) {
		_, ok := e.compileCallTailValueInPredicate("a = b", " IN ", false)
		require.False(t, ok)
	})
}

// ============================================================================
// compileCallTailValueComparisonPredicate — every operator + miss.
// ============================================================================

func TestCompileCallTailValueComparisonPredicate(t *testing.T) {
	e := freshExecutorForCallTail(t)
	ctx := context.Background()

	cases := []struct {
		name string
		expr string
		row  map[string]interface{}
		want bool
	}{
		{"= true", "x = 5", map[string]interface{}{"x": int64(5)}, true},
		{"= false", "x = 5", map[string]interface{}{"x": int64(6)}, false},
		{"<> true", "x <> 5", map[string]interface{}{"x": int64(6)}, true},
		{"!= true (mapped to <>)", "x != 5", map[string]interface{}{"x": int64(6)}, true},
		{"< true", "x < 5", map[string]interface{}{"x": int64(3)}, true},
		{"<= true (equal)", "x <= 5", map[string]interface{}{"x": int64(5)}, true},
		{"> true", "x > 5", map[string]interface{}{"x": int64(6)}, true},
		{">= true (equal)", "x >= 5", map[string]interface{}{"x": int64(5)}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pred, ok := e.tryCompileCallTailValueWhere(ctx, tc.expr)
			require.True(t, ok)
			require.Equal(t, tc.want, pred(tc.row, nil))
		})
	}

	t.Run("no comparison operator returns ok=false", func(t *testing.T) {
		_, ok := e.compileCallTailValueComparisonPredicate("x")
		require.False(t, ok)
	})
}

// ============================================================================
// tryCompileCallTailValueWhere — top-level AND/OR/NOT composition.
// ============================================================================

func TestTryCompileCallTailValueWhere_AndOrNot(t *testing.T) {
	e := freshExecutorForCallTail(t)
	ctx := context.Background()

	t.Run("empty clause is always true", func(t *testing.T) {
		pred, ok := e.tryCompileCallTailValueWhere(ctx, "")
		require.True(t, ok)
		require.True(t, pred(nil, nil))
	})

	t.Run("AND composition", func(t *testing.T) {
		pred, ok := e.tryCompileCallTailValueWhere(ctx, "x = 1 AND y = 2")
		require.True(t, ok)
		require.True(t, pred(map[string]interface{}{"x": int64(1), "y": int64(2)}, nil))
		require.False(t, pred(map[string]interface{}{"x": int64(1), "y": int64(3)}, nil))
	})

	t.Run("OR composition", func(t *testing.T) {
		pred, ok := e.tryCompileCallTailValueWhere(ctx, "x = 1 OR y = 2")
		require.True(t, ok)
		require.True(t, pred(map[string]interface{}{"x": int64(1), "y": int64(0)}, nil))
		require.True(t, pred(map[string]interface{}{"x": int64(0), "y": int64(2)}, nil))
		require.False(t, pred(map[string]interface{}{"x": int64(0), "y": int64(0)}, nil))
	})

	t.Run("NOT composition", func(t *testing.T) {
		pred, ok := e.tryCompileCallTailValueWhere(ctx, "NOT x = 1")
		require.True(t, ok)
		require.False(t, pred(map[string]interface{}{"x": int64(1)}, nil))
		require.True(t, pred(map[string]interface{}{"x": int64(2)}, nil))
	})

	t.Run("unparseable clause returns ok=false", func(t *testing.T) {
		_, ok := e.tryCompileCallTailValueWhere(ctx, "definitely not a where clause")
		require.False(t, ok)
	})
}

// ============================================================================
// compileCallTailValueResolver — literal, parameter, direct, and map-lookup
// resolution paths.
// ============================================================================

func TestCompileCallTailValueResolver_AllPaths(t *testing.T) {
	e := freshExecutorForCallTail(t)

	t.Run("empty expression returns ok=false", func(t *testing.T) {
		_, ok := e.compileCallTailValueResolver("")
		require.False(t, ok)
	})

	t.Run("integer literal", func(t *testing.T) {
		r, ok := e.compileCallTailValueResolver("42")
		require.True(t, ok)
		v, ok := r(nil, nil)
		require.True(t, ok)
		require.Equal(t, int64(42), v)
	})

	t.Run("parameter reference $name", func(t *testing.T) {
		r, ok := e.compileCallTailValueResolver("$id")
		require.True(t, ok)
		params := map[string]interface{}{"id": "abc"}
		v, ok := r(nil, params)
		require.True(t, ok)
		require.Equal(t, "abc", v)
	})

	t.Run("parameter reference with nil params returns ok=false", func(t *testing.T) {
		r, ok := e.compileCallTailValueResolver("$id")
		require.True(t, ok)
		v, ok := r(nil, nil)
		require.False(t, ok)
		require.Nil(t, v)
	})

	t.Run("empty parameter name returns ok=false", func(t *testing.T) {
		_, ok := e.compileCallTailValueResolver("$")
		require.False(t, ok)
	})

	t.Run("bare identifier short-circuits via direct resolver to values map", func(t *testing.T) {
		// Bare identifiers are intercepted by compileCallTailDirectValueResolver
		// and return a closure that only consults `values` (params is ignored
		// when the direct resolver claims the expression).
		r, ok := e.compileCallTailValueResolver("x")
		require.True(t, ok)
		v, ok := r(map[string]interface{}{"x": "v"}, nil)
		require.True(t, ok)
		require.Equal(t, "v", v)
		// And missing from values returns ok=false, not a params lookup.
		_, ok = r(map[string]interface{}{}, map[string]interface{}{"x": "from-params"})
		require.False(t, ok, "direct resolver shortcuts before params lookup")
	})

}
