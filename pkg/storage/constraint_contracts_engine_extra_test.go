package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type ccErrScanNodesEngine struct{ Engine }

func (e *ccErrScanNodesEngine) GetNodesByLabel(label string) ([]*Node, error) {
	return nil, errors.New("scan nodes failed")
}

type ccErrScanEdgesEngine struct{ Engine }

func (e *ccErrScanEdgesEngine) GetEdgesByType(edgeType string) ([]*Edge, error) {
	return nil, errors.New("scan edges failed")
}

func TestValidateConstraintContractOnCreationForEngine_Branches(t *testing.T) {
	eng := NewMemoryEngine()
	_, err := eng.CreateNode(&Node{ID: "test:n1", Labels: []string{"Person"}, Properties: map[string]any{"team": "core"}})
	require.NoError(t, err)
	_, err = eng.CreateNode(&Node{ID: "test:n2", Labels: []string{"Person"}, Properties: map[string]any{"team": "core"}})
	require.NoError(t, err)
	err = eng.CreateEdge(&Edge{ID: "test:e1", StartNode: "test:n1", EndNode: "test:n2", Type: "KNOWS", Properties: map[string]any{"kind": "peer"}})
	require.NoError(t, err)

	nodeContract := ConstraintContract{
		Name:              "node_rule",
		TargetEntityType:  string(ConstraintEntityNode),
		TargetLabelOrType: "Person",
		Entries:           []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "n.team IN ['core']"}},
	}
	require.NoError(t, ValidateConstraintContractOnCreationForEngine(eng, nodeContract))

	edgeContract := ConstraintContract{
		Name:              "edge_rule",
		TargetEntityType:  string(ConstraintEntityRelationship),
		TargetLabelOrType: "KNOWS",
		Entries:           []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanRelationship, Expression: "startNode(r) <> endNode(r)"}},
	}
	require.NoError(t, ValidateConstraintContractOnCreationForEngine(eng, edgeContract))

	violatingNode := ConstraintContract{
		Name:              "node_fail",
		TargetEntityType:  string(ConstraintEntityNode),
		TargetLabelOrType: "Person",
		Entries:           []ConstraintContractEntry{{Kind: ConstraintContractKindBooleanNode, Expression: "n.team IN ['ops']"}},
	}
	err = ValidateConstraintContractOnCreationForEngine(eng, violatingNode)
	require.Error(t, err)
	require.Contains(t, err.Error(), "violated")

	unsupported := ConstraintContract{Name: "bad", TargetEntityType: "INVALID", TargetLabelOrType: "X"}
	err = ValidateConstraintContractOnCreationForEngine(eng, unsupported)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported constraint contract target entity type")

	err = ValidateConstraintContractOnCreationForEngine(&ccErrScanNodesEngine{Engine: eng}, nodeContract)
	require.Error(t, err)
	require.Contains(t, err.Error(), "scanning nodes")

	err = ValidateConstraintContractOnCreationForEngine(&ccErrScanEdgesEngine{Engine: eng}, edgeContract)
	require.Error(t, err)
	require.Contains(t, err.Error(), "scanning relationships")
}

func TestAddConstraintContractBundle_Branches(t *testing.T) {
	sm := NewSchemaManager()
	persistCalls := 0
	sm.persist = func(def *SchemaDefinition) error {
		persistCalls++
		if persistCalls == 2 {
			return errors.New("persist failed")
		}
		return nil
	}

	base := ConstraintContract{Name: "c1", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Person"}
	require.NoError(t, sm.AddConstraintContractBundle(base, nil, nil, false))

	// ifNotExists idempotent branch.
	require.NoError(t, sm.AddConstraintContractBundle(base, nil, nil, true))

	// duplicate error branch.
	err := sm.AddConstraintContractBundle(base, nil, nil, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already exists")

	// conflict with existing constraint name branch.
	sm.constraints["c_conflict"] = Constraint{Name: "c_conflict", Type: ConstraintUnique, Label: "Person", Properties: []string{"name"}}
	err = sm.AddConstraintContractBundle(ConstraintContract{Name: "c_conflict", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Person"}, nil, nil, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "conflicts with an existing constraint name")

	// persist failure rolls back insertion.
	contractPersistFail := ConstraintContract{Name: "c2", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Person"}
	err = sm.AddConstraintContractBundle(contractPersistFail, nil, nil, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "persist failed")
	_, exists := sm.constraintContracts["c2"]
	require.False(t, exists)

	// compiledConstraints path: addConstraintLocked failure should rollback.
	err = sm.AddConstraintContractBundle(
		ConstraintContract{Name: "c3", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Person"},
		[]Constraint{
			{Name: "dup_schema", Type: ConstraintUnique, Label: "Person", Properties: []string{"email"}},
			{Name: "dup_schema", Type: ConstraintUnique, Label: "Person", Properties: []string{"email"}},
		},
		nil,
		false,
	)
	require.Error(t, err)
	_, exists = sm.constraintContracts["c3"]
	require.False(t, exists)

	// compiledTypes path: addPropertyTypeConstraintValueLocked failure should rollback.
	err = sm.AddConstraintContractBundle(
		ConstraintContract{Name: "c4", TargetEntityType: string(ConstraintEntityNode), TargetLabelOrType: "Person"},
		nil,
		[]PropertyTypeConstraint{
			{Name: "ptype_dup", Label: "Person", Property: "age", ExpectedType: PropertyTypeInteger},
			{Name: "ptype_dup", Label: "Person", Property: "age", ExpectedType: PropertyTypeInteger},
		},
		false,
	)
	require.Error(t, err)
	_, exists = sm.constraintContracts["c4"]
	require.False(t, exists)
}
