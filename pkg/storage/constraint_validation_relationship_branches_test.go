package storage

import (
	"errors"
	"strings"
	"testing"
)

type allEdgesOverrideEngine struct {
	Engine
	edges []*Edge
	err   error
}

func (e *allEdgesOverrideEngine) AllEdges() ([]*Edge, error) {
	if e.err != nil {
		return nil, e.err
	}
	return e.edges, nil
}

func TestValidateRelationshipConstraintOnCreationForEngine_Branches(t *testing.T) {
	t.Run("all-edges scan error is propagated", func(t *testing.T) {
		eng := &allEdgesOverrideEngine{
			Engine: NewMemoryEngine(),
			err:    errors.New("scan boom"),
		}
		t.Cleanup(func() { _ = eng.Engine.Close() })

		err := validateRelationshipConstraintOnCreationForEngine(eng, Constraint{
			Type:       ConstraintUnique,
			EntityType: ConstraintEntityRelationship,
			Label:      "REL",
			Properties: []string{"k"},
		})
		if err == nil || !strings.Contains(err.Error(), "scanning edges: scan boom") {
			t.Fatalf("expected scan error, got: %v", err)
		}
	})

	t.Run("property type constraint is accepted in this path", func(t *testing.T) {
		eng := &allEdgesOverrideEngine{
			Engine: NewMemoryEngine(),
			edges: []*Edge{
				{ID: "test:e1", Type: "REL", Properties: map[string]interface{}{"k": 1}},
			},
		}
		t.Cleanup(func() { _ = eng.Engine.Close() })

		err := validateRelationshipConstraintOnCreationForEngine(eng, Constraint{
			Type:       ConstraintPropertyType,
			EntityType: ConstraintEntityRelationship,
			Label:      "REL",
			Properties: []string{"k"},
		})
		if err != nil {
			t.Fatalf("expected nil error, got: %v", err)
		}
	})

	t.Run("relationship key first enforces existence", func(t *testing.T) {
		eng := &allEdgesOverrideEngine{
			Engine: NewMemoryEngine(),
			edges: []*Edge{
				{ID: "test:e1", Type: "REL", Properties: map[string]interface{}{"k1": "x"}},
			},
		}
		t.Cleanup(func() { _ = eng.Engine.Close() })

		err := validateRelationshipConstraintOnCreationForEngine(eng, Constraint{
			Type:       ConstraintRelationshipKey,
			EntityType: ConstraintEntityRelationship,
			Label:      "REL",
			Properties: []string{"k1", "k2"},
		})
		if err == nil || !strings.Contains(err.Error(), "missing required property") {
			t.Fatalf("expected existence violation, got: %v", err)
		}
	})

	t.Run("unsupported relationship constraint type returns explicit error", func(t *testing.T) {
		eng := &allEdgesOverrideEngine{
			Engine: NewMemoryEngine(),
			edges: []*Edge{
				{ID: "test:e1", Type: "REL", Properties: map[string]interface{}{"k": "x"}},
			},
		}
		t.Cleanup(func() { _ = eng.Engine.Close() })

		err := validateRelationshipConstraintOnCreationForEngine(eng, Constraint{
			Type:       ConstraintType("nonexistent"),
			EntityType: ConstraintEntityRelationship,
			Label:      "REL",
			Properties: []string{"k"},
		})
		if err == nil || !strings.Contains(err.Error(), "unsupported relationship constraint type") {
			t.Fatalf("expected unsupported-type error, got: %v", err)
		}
	})
}
