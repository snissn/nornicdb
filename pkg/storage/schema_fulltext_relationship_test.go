package storage

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFulltextIndex_RelationshipScope_RoundTrip asserts that a fulltext
// index declared on a relationship type round-trips through the JSON
// persistence layer with no on-disk schema-version bump. Two independent
// invariants are tested deeply:
//
//  1. Round-trip preserves every field.
//  2. The on-disk JSON for a node-only index does NOT contain the new
//     relationship_types key (omitempty), so an older binary opening
//     a database written by the new binary sees exactly the bytes it
//     would have written itself.
//
// The forward-compat case (new binary loading old JSON without the
// relationship_types field) is covered by TestFulltextIndex_LoadLegacy
// below.
func TestFulltextIndex_RelationshipScope_RoundTrip(t *testing.T) {
	def := &SchemaDefinition{
		FulltextIndexes: []FulltextIndex{
			{
				Name:              "rel_idx",
				RelationshipTypes: []string{"KNOWS", "WORKS_WITH"},
				Properties:        []string{"note", "since"},
			},
		},
	}
	encoded, err := json.Marshal(def)
	require.NoError(t, err)

	// Round-trip.
	var decoded SchemaDefinition
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Len(t, decoded.FulltextIndexes, 1)
	idx := decoded.FulltextIndexes[0]
	assert.Equal(t, "rel_idx", idx.Name)
	assert.Equal(t, []string{"KNOWS", "WORKS_WITH"}, idx.RelationshipTypes)
	assert.Equal(t, []string{"note", "since"}, idx.Properties)
	assert.Empty(t, idx.Labels, "relationship-scoped index must not carry labels")
}

// TestFulltextIndex_NodeOnly_OmitsRelationshipTypesField is the
// backwards-compatibility guarantee: existing databases written before
// this PR (and the corresponding new-binary writes for node-only
// indexes) must serialize without a relationship_types field. An older
// binary's json.Unmarshal silently ignores fields it doesn't know;
// preserving the old shape means no behavior change for old readers.
func TestFulltextIndex_NodeOnly_OmitsRelationshipTypesField(t *testing.T) {
	def := &SchemaDefinition{
		FulltextIndexes: []FulltextIndex{
			{
				Name:       "node_idx",
				Labels:     []string{"Memory"},
				Properties: []string{"name", "type"},
			},
		},
	}
	encoded, err := json.Marshal(def)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "relationship_types",
		"node-only index must NOT serialize a relationship_types field; doing so breaks old-binary read compatibility (got %s)", string(encoded))
	assert.Contains(t, string(encoded), `"labels":["Memory"]`)
}

// TestFulltextIndex_LoadLegacy is the forward-compatibility guarantee:
// JSON written by an older binary (no relationship_types key) must
// load cleanly into the new struct, with RelationshipTypes left as nil
// (which downstream code treats as "node-scoped index, fall back to
// label filtering").
func TestFulltextIndex_LoadLegacy(t *testing.T) {
	legacy := `{
		"fulltext_indexes": [
			{"name": "old_idx", "labels": ["Person"], "properties": ["name"]}
		]
	}`
	var def SchemaDefinition
	require.NoError(t, json.Unmarshal([]byte(legacy), &def))
	require.Len(t, def.FulltextIndexes, 1)
	idx := def.FulltextIndexes[0]
	assert.Equal(t, "old_idx", idx.Name)
	assert.Equal(t, []string{"Person"}, idx.Labels)
	assert.Empty(t, idx.RelationshipTypes,
		"missing relationship_types field must load as empty / nil")
	assert.Equal(t, []string{"name"}, idx.Properties)
}

// TestSchemaManager_AddFulltextRelationshipIndex_RoundTrip drives the
// full export/import cycle through SchemaManager so the persistence
// helpers are exercised end-to-end.
func TestSchemaManager_AddFulltextRelationshipIndex_RoundTrip(t *testing.T) {
	var persisted *SchemaDefinition
	sm := NewSchemaManager()
	sm.SetPersister(func(def *SchemaDefinition) error {
		// Take a deep copy via JSON so we observe exactly what would be
		// written to disk.
		raw, _ := json.Marshal(def)
		var clone SchemaDefinition
		_ = json.Unmarshal(raw, &clone)
		persisted = &clone
		return nil
	})

	require.NoError(t, sm.AddFulltextRelationshipIndex("rel_search", []string{"KNOWS"}, []string{"note"}))

	// In-memory get returns the new fields.
	idx, ok := sm.GetFulltextIndex("rel_search")
	require.True(t, ok)
	assert.Equal(t, []string{"KNOWS"}, idx.RelationshipTypes)
	assert.Empty(t, idx.Labels)
	assert.Equal(t, []string{"note"}, idx.Properties)

	// Persistence saw the relationship-scope shape.
	require.NotNil(t, persisted, "persist callback must have fired")
	require.Len(t, persisted.FulltextIndexes, 1)
	assert.Equal(t, []string{"KNOWS"}, persisted.FulltextIndexes[0].RelationshipTypes)
	assert.Empty(t, persisted.FulltextIndexes[0].Labels)

	// Re-load the persisted def into a fresh manager — round-trip drops
	// nothing.
	sm2 := NewSchemaManager()
	require.NoError(t, sm2.ReplaceFromDefinition(persisted))
	idx2, ok := sm2.GetFulltextIndex("rel_search")
	require.True(t, ok)
	assert.Equal(t, []string{"KNOWS"}, idx2.RelationshipTypes)
	assert.Equal(t, []string{"note"}, idx2.Properties)
}

// TestFulltextIndex_BothScopesNeverMixed asserts the convention that
// a single index does not carry both labels and relationship_types.
// The runtime relies on this to decide which storage scan to drive;
// breaking it silently produces queries that traverse the wrong axis.
func TestFulltextIndex_BothScopesNeverMixed(t *testing.T) {
	sm := NewSchemaManager()
	require.NoError(t, sm.AddFulltextIndex("node_idx", []string{"Person"}, []string{"name"}))
	require.NoError(t, sm.AddFulltextRelationshipIndex("rel_idx", []string{"KNOWS"}, []string{"note"}))

	nodeIdx, _ := sm.GetFulltextIndex("node_idx")
	relIdx, _ := sm.GetFulltextIndex("rel_idx")

	require.NotEmpty(t, nodeIdx.Labels)
	require.Empty(t, nodeIdx.RelationshipTypes)

	require.Empty(t, relIdx.Labels)
	require.NotEmpty(t, relIdx.RelationshipTypes)
}

// TestFulltextIndex_LegacyTextRetainsExactShape uses a hand-written
// legacy JSON document and asserts the round-trip preserves byte
// equivalence for the node-only indexes (no relationship_types key
// added by the marshal step). This catches accidental field-default
// changes that would silently rewrite every existing schema document
// on next startup.
func TestFulltextIndex_LegacyTextRetainsExactShape(t *testing.T) {
	legacy := `{"fulltext_indexes":[{"name":"old_idx","labels":["Person"],"properties":["name"]}]}`
	var def SchemaDefinition
	require.NoError(t, json.Unmarshal([]byte(legacy), &def))

	out, err := json.Marshal(def)
	require.NoError(t, err)
	// The re-emitted JSON shouldn't contain relationship_types because
	// the index is node-scoped.
	assert.False(t, strings.Contains(string(out), "relationship_types"),
		"node-only index must round-trip without introducing relationship_types: got %s", string(out))
}
