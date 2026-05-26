// Package cypher - Pre-compiled regex patterns for performance.
//
// This file contains all regex patterns used in hot paths, pre-compiled at package init time.
// Moving regex compilation from function calls to package initialization provides 5-10x
// performance improvements for operations that use these patterns repeatedly.
//
// Performance Impact:
//   - Schema DDL operations: 5-10x faster (9 patterns)
//   - APOC path operations: 8-15x faster (8 patterns)
//   - Duration parsing: 3-5x faster (2 patterns)
package cypher

import (
	"regexp"
	"sync"
)

// =============================================================================
// Schema DDL Patterns (CREATE CONSTRAINT, CREATE INDEX)
// =============================================================================

const (
	ddlIdentifierToken = "(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)"
	ddlVariableToken   = "(?:`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)"
	ddlOptionsTail     = "(?:\\s+OPTIONS\\s+\\{.*\\})?"
)

var (
	// Constraint patterns - CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (var:Label) REQUIRE var.prop IS UNIQUE
	constraintNamedForRequire   = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+REQUIRE\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+UNIQUE` + ddlOptionsTail + `\s*$`)
	constraintUnnamedForRequire = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+REQUIRE\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+UNIQUE` + ddlOptionsTail + `\s*$`)
	constraintOnAssert          = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+ON\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+ASSERT\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+UNIQUE` + ddlOptionsTail + `\s*$`)

	// EXISTS / NOT NULL constraints
	constraintNamedForRequireNotNull   = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+REQUIRE\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+NOT\s+NULL` + ddlOptionsTail + `\s*$`)
	constraintUnnamedForRequireNotNull = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+REQUIRE\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+NOT\s+NULL` + ddlOptionsTail + `\s*$`)
	constraintOnAssertExists           = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+ON\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+ASSERT\s+exists\s*\(\s*(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s*\)` + ddlOptionsTail + `\s*$`)
	constraintOnAssertNotNull          = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+ON\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+ASSERT\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+NOT\s+NULL` + ddlOptionsTail + `\s*$`)

	// NODE KEY constraints
	constraintNamedForRequireNodeKey   = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+REQUIRE\s+\(([^)]+)\)\s+IS\s+NODE\s+KEY` + ddlOptionsTail + `\s*$`)
	constraintUnnamedForRequireNodeKey = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+REQUIRE\s+\(([^)]+)\)\s+IS\s+NODE\s+KEY` + ddlOptionsTail + `\s*$`)
	constraintOnAssertNodeKey          = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+ON\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+ASSERT\s+\(([^)]+)\)\s+IS\s+NODE\s+KEY` + ddlOptionsTail + `\s*$`)

	// Temporal no-overlap constraints (NornicDB extension)
	constraintNamedForRequireTemporal   = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+REQUIRE\s+\(([^)]+)\)\s+IS\s+TEMPORAL(?:\s+NO\s+OVERLAP)?` + ddlOptionsTail + `\s*$`)
	constraintUnnamedForRequireTemporal = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+REQUIRE\s+\(([^)]+)\)\s+IS\s+TEMPORAL(?:\s+NO\s+OVERLAP)?` + ddlOptionsTail + `\s*$`)

	// Property type constraints
	constraintNamedForRequireType   = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+REQUIRE\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+(?:::|TYPED)\s*([A-Z]+(?:\s+[A-Z]+)?)` + ddlOptionsTail + `\s*$`)
	constraintUnnamedForRequireType = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+REQUIRE\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+(?:::|TYPED)\s*([A-Z]+(?:\s+[A-Z]+)?)` + ddlOptionsTail + `\s*$`)
	constraintOnAssertType          = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+ON\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+ASSERT\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+(?:::|TYPED)\s*([A-Z]+(?:\s+[A-Z]+)?)` + ddlOptionsTail + `\s*$`)

	// =========================================================================
	// Relationship constraint patterns - CREATE CONSTRAINT ... FOR ()-[r:TYPE]-() REQUIRE ...
	// =========================================================================

	// Relationship UNIQUE constraints (single property)
	constraintRelNamedForRequire   = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+UNIQUE` + ddlOptionsTail + `\s*$`)
	constraintRelUnnamedForRequire = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+UNIQUE` + ddlOptionsTail + `\s*$`)

	// Relationship EXISTS / NOT NULL constraints
	constraintRelNamedForRequireNotNull   = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+NOT\s+NULL` + ddlOptionsTail + `\s*$`)
	constraintRelUnnamedForRequireNotNull = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+NOT\s+NULL` + ddlOptionsTail + `\s*$`)

	// Relationship property type constraints
	constraintRelNamedForRequireType   = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+(?:::|TYPED)\s*([A-Z]+(?:\s+[A-Z]+)?)` + ddlOptionsTail + `\s*$`)
	constraintRelUnnamedForRequireType = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+(?:::|TYPED)\s*([A-Z]+(?:\s+[A-Z]+)?)` + ddlOptionsTail + `\s*$`)

	// Relationship KEY constraints (composite properties)
	constraintRelNamedForRequireRelKey   = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s+\(([^)]+)\)\s+IS\s+RELATIONSHIP\s+KEY` + ddlOptionsTail + `\s*$`)
	constraintRelUnnamedForRequireRelKey = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s+\(([^)]+)\)\s+IS\s+RELATIONSHIP\s+KEY` + ddlOptionsTail + `\s*$`)

	// Relationship composite UNIQUE constraints (parenthesized multi-property)
	constraintRelNamedForRequireCompositeUnique   = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s+\(([^)]+)\)\s+IS\s+UNIQUE` + ddlOptionsTail + `\s*$`)
	constraintRelUnnamedForRequireCompositeUnique = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s+\(([^)]+)\)\s+IS\s+UNIQUE` + ddlOptionsTail + `\s*$`)

	// Relationship single-property KEY constraint (non-composite form)
	constraintRelNamedForRequireSingleRelKey   = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+RELATIONSHIP\s+KEY` + ddlOptionsTail + `\s*$`)
	constraintRelUnnamedForRequireSingleRelKey = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IS\s+RELATIONSHIP\s+KEY` + ddlOptionsTail + `\s*$`)

	// Relationship temporal no-overlap constraints (NornicDB extension)
	constraintRelNamedForRequireTemporal   = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s+\(([^)]+)\)\s+IS\s+TEMPORAL(?:\s+NO\s+OVERLAP)?` + ddlOptionsTail + `\s*$`)
	constraintRelUnnamedForRequireTemporal = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s+\(([^)]+)\)\s+IS\s+TEMPORAL(?:\s+NO\s+OVERLAP)?` + ddlOptionsTail + `\s*$`)

	// Node domain/enum constraints (NornicDB extension)
	// CREATE CONSTRAINT name FOR (n:Label) REQUIRE n.prop IN ['val1', 'val2']
	constraintNodeNamedForRequireDomain   = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+REQUIRE\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IN\s+\[([^\]]*)\]` + ddlOptionsTail + `\s*$`)
	constraintNodeUnnamedForRequireDomain = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+REQUIRE\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IN\s+\[([^\]]*)\]` + ddlOptionsTail + `\s*$`)

	// Relationship domain/enum constraints (NornicDB extension)
	// CREATE CONSTRAINT name FOR ()-[r:TYPE]-() REQUIRE r.prop IN ['val1', 'val2']
	constraintRelNamedForRequireDomain   = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IN\s+\[([^\]]*)\]` + ddlOptionsTail + `\s*$`)
	constraintRelUnnamedForRequireDomain = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s+(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s+IN\s+\[([^\]]*)\]` + ddlOptionsTail + `\s*$`)

	// Cardinality constraints (NornicDB extension) — outgoing direction
	// CREATE CONSTRAINT name [IF NOT EXISTS] FOR ()-[r:TYPE]->() REQUIRE MAX COUNT N
	constraintCardinalityOutNamedForRequire   = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*->\s*\(\s*\)\s+REQUIRE\s+MAX\s+COUNT\s+(\d+)` + ddlOptionsTail + `\s*$`)
	constraintCardinalityOutUnnamedForRequire = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*->\s*\(\s*\)\s+REQUIRE\s+MAX\s+COUNT\s+(\d+)` + ddlOptionsTail + `\s*$`)

	// Cardinality constraints (NornicDB extension) — incoming direction
	// CREATE CONSTRAINT name [IF NOT EXISTS] FOR ()<-[r:TYPE]-() REQUIRE MAX COUNT N
	constraintCardinalityInNamedForRequire   = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*<-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s+MAX\s+COUNT\s+(\d+)` + ddlOptionsTail + `\s*$`)
	constraintCardinalityInUnnamedForRequire = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*<-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*\(\s*\)\s+REQUIRE\s+MAX\s+COUNT\s+(\d+)` + ddlOptionsTail + `\s*$`)

	// Relationship endpoint policy constraints (NornicDB extension)
	// CREATE CONSTRAINT name [IF NOT EXISTS] FOR (:SrcLabel)-[r:TYPE]->(:TgtLabel) REQUIRE ALLOWED
	constraintPolicyAllowedNamedForRequire      = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*->\s*\(\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+REQUIRE\s+ALLOWED` + ddlOptionsTail + `\s*$`)
	constraintPolicyAllowedUnnamedForRequire    = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*->\s*\(\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+REQUIRE\s+ALLOWED` + ddlOptionsTail + `\s*$`)
	constraintPolicyDisallowedNamedForRequire   = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*->\s*\(\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+REQUIRE\s+DISALLOWED` + ddlOptionsTail + `\s*$`)
	constraintPolicyDisallowedUnnamedForRequire = regexp.MustCompile(`(?is)^\s*CREATE\s+CONSTRAINT(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s*-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*->\s*\(\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+REQUIRE\s+DISALLOWED` + ddlOptionsTail + `\s*$`)

	// DROP CONSTRAINT
	dropConstraintPattern = regexp.MustCompile(`(?is)^\s*DROP\s+CONSTRAINT\s+(?:IF\s+EXISTS\s+)?(` + ddlIdentifierToken + `)(?:\s+IF\s+EXISTS)?\s*$`)

	// Index patterns - CREATE INDEX [name] [IF NOT EXISTS] FOR (var:Label) ON (var.prop)
	indexNamedFor   = regexp.MustCompile(`(?is)^\s*CREATE\s+INDEX\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+ON\s+\(([^)]+)\)` + ddlOptionsTail + `\s*$`)
	indexUnnamedFor = regexp.MustCompile(`(?is)^\s*CREATE\s+INDEX(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+ON\s+\(([^)]+)\)` + ddlOptionsTail + `\s*$`)

	// Fulltext index pattern - CREATE FULLTEXT INDEX name FOR (var:Label) ON EACH [props]
	fulltextIndexPattern = regexp.MustCompile(`(?is)^\s*CREATE\s+FULLTEXT\s+INDEX\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+ON\s+EACH\s+\[([^\]]+)\]` + ddlOptionsTail + `\s*$`)

	// Fulltext relationship index pattern -
	//   CREATE FULLTEXT INDEX name [IF NOT EXISTS] FOR ()-[var:Type]-() ON EACH [props]
	// or  ()-[var:Type]->()  /  ()<-[var:Type]-()  — Neo4j accepts any
	// direction marker because the index is direction-agnostic. The
	// surrounding empty node patterns (`()`) are mandatory.
	fulltextRelIndexPattern = regexp.MustCompile(`(?is)^\s*CREATE\s+FULLTEXT\s+INDEX\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*\)\s*<?-\s*\[\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\]\s*-\s*>?\s*\(\s*\)\s+ON\s+EACH\s+\[([^\]]+)\]` + ddlOptionsTail + `\s*$`)

	// Vector index patterns
	vectorIndexPattern      = regexp.MustCompile(`(?is)^\s*CREATE\s+VECTOR\s+INDEX\s+(` + ddlIdentifierToken + `)(?:\s+IF\s+NOT\s+EXISTS)?\s+FOR\s+\(\s*(` + ddlVariableToken + `)\s*:\s*(` + ddlIdentifierToken + `)\s*\)\s+ON\s+\(\s*(` + ddlVariableToken + `)\s*\.\s*(` + ddlIdentifierToken + `)\s*\)`)
	vectorDimensionsPattern = regexp.MustCompile(`vector\.dimensions[:\s]+(\d+)`)
	vectorSimilarityPattern = regexp.MustCompile(`vector\.similarity_function[:\s]+['"]?(\w+)['"]?`)
)

// =============================================================================
// APOC Configuration Patterns
// =============================================================================

var (
	// APOC path configuration
	apocMaxLevelPattern    = regexp.MustCompile(`maxLevel\s*:\s*(\d+)`)
	apocMinLevelPattern    = regexp.MustCompile(`minLevel\s*:\s*(\d+)`)
	apocLimitPattern       = regexp.MustCompile(`limit\s*:\s*(\d+)`)
	apocRelFilterPattern   = regexp.MustCompile(`relationshipFilter\s*:\s*['"]([^'"]+)['"]`)
	apocLabelFilterPattern = regexp.MustCompile(`labelFilter\s*:\s*['"]([^'"]+)['"]`)

	// APOC node ID extraction
	apocNodeIdBracePattern = regexp.MustCompile(`\{[^}]*id\s*:\s*['"]([^'"]+)['"]`)
	apocWhereIdPattern     = regexp.MustCompile(`WHERE\s+\w+\.id\s*=\s*['"]([^'"]+)['"]`)

	// Fulltext search phrase extraction
	fulltextPhrasePattern = regexp.MustCompile(`"([^"]+)"`)
)

// =============================================================================
// Duration Parsing Patterns
// =============================================================================

var (
	// ISO 8601 duration parsing - P[n]Y[n]M[n]DT[n]H[n]M[n]S
	durationDatePartPattern = regexp.MustCompile(`(\d+)([YMD])`)
	durationTimePartPattern = regexp.MustCompile(`(\d+(?:\.\d+)?)([HMS])`)
)

// =============================================================================
// Query Analysis Patterns (EXPLAIN/PROFILE)
// =============================================================================

var (
	// Variable-length path pattern: [*], [*1..3], [*..5]
	varLengthPathPattern = regexp.MustCompile(`\[\*\d*\.?\.\d*\]`)

	// Label extraction pattern: (n:Label) or (:Label)
	labelExtractPattern = regexp.MustCompile(`\(\w*:(\w+)`)

	// Aggregation function detection
	aggregationPattern = regexp.MustCompile(`(?i)(COUNT|SUM|AVG|MIN|MAX|COLLECT)\s*\(`)

	// LIMIT/SKIP extraction
	limitPattern = regexp.MustCompile(`(?i)LIMIT\s+(\d+)`)
	skipPattern  = regexp.MustCompile(`(?i)SKIP\s+(\d+)`)

	// CALL procedure extraction
	callProcedurePattern = regexp.MustCompile(`(?i)CALL\s+([\w.]+)`)
)

// =============================================================================
// Parameter Substitution Pattern
// =============================================================================

var (
	// Parameter reference: $name or $name123
	parameterPattern = regexp.MustCompile(`\$([a-zA-Z_][a-zA-Z0-9_]*)`)
)

// =============================================================================
// Aggregation Function Property Patterns
// =============================================================================

var (
	// Aggregation with property: COUNT(n.prop), SUM(n.prop), etc.
	countPropPattern   = regexp.MustCompile(`(?i)COUNT\((\w+)\.(\w+)\)`)
	sumPropPattern     = regexp.MustCompile(`(?i)SUM\((\w+)\.(\w+)\)`)
	avgPropPattern     = regexp.MustCompile(`(?i)AVG\((\w+)\.(\w+)\)`)
	minPropPattern     = regexp.MustCompile(`(?i)MIN\((\w+)\.(\w+)\)`)
	maxPropPattern     = regexp.MustCompile(`(?i)MAX\((\w+)\.(\w+)\)`)
	collectPropPattern = regexp.MustCompile(`(?i)COLLECT\((\w+)(?:\.(\w+))?\)`)

	// Aliases for compatibility (used in match.go)
	countPropRe = countPropPattern
	sumRe       = sumPropPattern
	avgRe       = avgPropPattern
	minRe       = minPropPattern
	maxRe       = maxPropPattern
	collectRe   = collectPropPattern

	// DISTINCT variants
	countDistinctPropPattern   = regexp.MustCompile(`(?i)COUNT\(\s*DISTINCT\s+(\w+)\.(\w+)\)`)
	collectDistinctPropPattern = regexp.MustCompile(`(?i)COLLECT\(\s*DISTINCT\s+(\w+)\.(\w+)\)`)

	// Aliases for DISTINCT variants
	countDistinctPropRe   = countDistinctPropPattern
	collectDistinctPropRe = collectDistinctPropPattern
)

// =============================================================================
// Path and Traversal Patterns
// =============================================================================

var (
	// ID extraction: id(n)
	idFunctionPattern = regexp.MustCompile(`id\((\w+)\)`)
)

// =============================================================================
// CREATE Statement Patterns (pre-compiled for hot path optimization)
// =============================================================================

var (
	// MATCH keyword split pattern
	matchKeywordPattern = regexp.MustCompile(`(?i)\bMATCH\s+`)

	// CREATE keyword split pattern
	createKeywordPattern = regexp.MustCompile(`(?i)\bCREATE\s+`)

	// Forward relationship pattern: (a)-[r:TYPE {props}]->(b)
	// Captures full node content including inline definitions like (c:Label {props})
	relForwardPattern = regexp.MustCompile(`\(([^)]+)\)\s*-\[(\w*)(?::(\w+))?(?:\s*(\{[^}]*\}))?\]\s*->\s*\(([^)]+)\)`)

	// Reverse relationship pattern: (a)<-[r:TYPE {props}]-(b)
	// Captures full node content including inline definitions like (c:Label {props})
	relReversePattern = regexp.MustCompile(`\(([^)]+)\)\s*<-\[(\w*)(?::(\w+))?(?:\s*(\{[^}]*\}))?\]\s*-\s*\(([^)]+)\)`)

	// Node variable extraction: (varName:Label) or (varName)
	nodeVarPattern = regexp.MustCompile(`\((\w+)(?::\w+)?`)

	// Node variable with label: (varName:Label) or (varName)
	nodeVarLabelPattern = regexp.MustCompile(`\((\w+)(?::(\w+))?`)

	// Relationship variable extraction: [r:TYPE] or [r] or [:TYPE]
	relVarTypePattern = regexp.MustCompile(`\[(\w+)(?::(\w+))?\]`)
)

// =============================================================================
// Cache/Label Extraction Patterns
// =============================================================================

var (
	// Label extraction: :Label, (:Label), :Label:AnotherLabel
	// Used in cache.go for query analysis
	labelRegex = regexp.MustCompile(`:([A-Z][A-Za-z0-9_]*)`)
)

// =============================================================================
// Index Hint Patterns (USING INDEX, USING SCAN, USING JOIN)
// =============================================================================

var (
	// USING INDEX n:Label(property) or USING INDEX n:Label(prop1, prop2)
	indexHintPattern = regexp.MustCompile(`(?i)USING\s+INDEX\s+(\w+):(\w+)\s*\(\s*([^)]+)\s*\)`)

	// USING SCAN n:Label
	scanHintPattern = regexp.MustCompile(`(?i)USING\s+SCAN\s+(\w+):(\w+)`)

	// USING JOIN ON n
	joinHintPattern = regexp.MustCompile(`(?i)USING\s+JOIN\s+ON\s+(\w+)`)
)

// =============================================================================
// Dynamic Regex Cache (for user-provided patterns like =~ comparison)
// =============================================================================

// regexCache provides thread-safe caching of compiled regex patterns.
// Used for dynamic patterns like Cypher's =~ regex comparison operator.
var regexCache sync.Map // map[string]*regexp.Regexp

// GetCachedRegex returns a compiled regex for the pattern, using cache if available.
// This avoids re-compiling the same pattern on every =~ comparison.
func GetCachedRegex(pattern string) (*regexp.Regexp, error) {
	// Check cache first
	if cached, ok := regexCache.Load(pattern); ok {
		return cached.(*regexp.Regexp), nil
	}

	// Compile and cache
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	// Store in cache (another goroutine might have stored it already, that's fine)
	regexCache.Store(pattern, re)
	return re, nil
}
